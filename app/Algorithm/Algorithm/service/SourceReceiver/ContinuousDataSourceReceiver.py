import asyncio
import bisect
import io
import logging
import math
import struct
import time
from asyncio import Queue
from collections import deque
from dataclasses import dataclass
from typing import Union

import numpy as np

from Algorithm.common.utils.seed import seed_everything_for_stage
from Algorithm.method.model.AlgorithmObject import (
    AlgorithmCalibrationObject,
    AlgorithmContinuousDataObject,
    AlgorithmDeviceObject,
)
from Algorithm.service.exception.AlgorithmSourceException import AlgorithmSourceReceiverIsTurnedOffException
from Algorithm.service.interface.SourceReceiverInterface import SourceReceiverInterface
from Common.converter.BaseDataClassMessageConverter import BaseDataClassMessageConverter
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel
from Common.model.CommonMessageModel import DevicePackageModel, EventPackageModel, DataPackageModel, \
    ImpedancePackageModel, ControlPackageModel, InformationPackageModel


@dataclass
class SingleEvent:
    event_position: int = None
    event_data: float = None

    def __lt__(self, other):
        # 比较两个SingleEvent对象的position属性
        return self.event_position < other.event_position

    def __le__(self, other):
        # 比较两个SingleEvent对象的position属性
        return self.event_position <= other.event_position


@dataclass
class CalibrationChunkBufferModel:
    total_chunk_number: int
    total_payload_size: int
    chunk_payload_dict: dict[int, bytes]
    first_chunk_wallclock: float
    last_chunk_wallclock: float


class ContinuousDataSourceReceiver(SourceReceiverInterface):
    """
    连续数据源接收器。

    这是算法侧最重要的数据入口之一。
    它把 ProcessHub 转发来的各种包，整理成算法更容易消费的对象：
    1. DevicePackage -> AlgorithmDeviceObject；
    2. calibration 阶段的 DataPackage(bytes) -> AlgorithmCalibrationObject；
    3. online 阶段的 DataPackage(连续数值) -> AlgorithmContinuousDataObject；
    4. EventPackage -> 写入最后一行 trigger 通道；
    5. ControlPackage(end_flag) -> 结束标记。
    """
    __CALIBRATION_CHUNK_MAGIC = b'CAL1'
    __CALIBRATION_CHUNK_HEADER_FORMAT = '>4sIII'
    __CALIBRATION_CHUNK_HEADER_SIZE = struct.calcsize(__CALIBRATION_CHUNK_HEADER_FORMAT)
    __DEFAULT_DEVICE_INFORMATION_WAIT_TIMEOUT_SECONDS = 0.0
    # 决赛正式流程里，算法侧可能在“所有组准备完成后”继续等待很久才真正开赛，
    # 因此不能对“尚未开始发送的 calibration 对象”做整体超时。
    # 真正需要保护的是：一旦开始收到 calibration chunk，后续分块长时间收不齐。
    __DEFAULT_CALIBRATION_OBJECT_WAIT_TIMEOUT_SECONDS = 0.0
    __DEFAULT_CALIBRATION_CHUNK_ASSEMBLY_TIMEOUT_SECONDS = 15.0
    __MIN_CALIBRATION_CHUNK_ASSEMBLY_TIMEOUT_SECONDS = 15.0
    __CALIBRATION_CHUNK_TIMEOUT_SECONDS_PER_CHUNK = 0.75
    __RETURN_TO_CALIBRATION_MARKER_KEY = '__return_to_calibration__'
    __CONSUMER_PHASE_CALIBRATION = 'calibration'
    __CONSUMER_PHASE_ONLINE = 'online'

    def __init__(self):
        # 配置信息
        self.__source_label: str = ""
        # 设置多少个点一个数据包,如果为0，则不进行分块
        self.__chunk_size: int = 0
        self.__device_information_wait_timeout_seconds: float = (
            self.__DEFAULT_DEVICE_INFORMATION_WAIT_TIMEOUT_SECONDS
        )
        self.__calibration_object_wait_timeout_seconds: float = (
            self.__DEFAULT_CALIBRATION_OBJECT_WAIT_TIMEOUT_SECONDS
        )
        self.__calibration_chunk_assembly_timeout_seconds: float = (
            self.__DEFAULT_CALIBRATION_CHUNK_ASSEMBLY_TIMEOUT_SECONDS
        )
        self.__algorithm_device_object: AlgorithmDeviceObject = AlgorithmDeviceObject()
        self.__incoming_channel_number: int = 0
        self.__incoming_channel_label_list: list[str] = []
        self.__required_channel_label_list: list[str] = []
        self.__required_channel_index_list: Union[list[int], None] = None
        self.__data_queue = Queue[AlgorithmContinuousDataObject]()  # 供算法 run() 消费的在线数据队列
        self.__calibration_queue = Queue[AlgorithmCalibrationObject]()  # 供算法 calibrate() 消费的校准数据队列
        self.__data_deque = deque[AlgorithmContinuousDataObject]()  # 在线数据镜像，用于补写 event
        self.__event_list = list[SingleEvent]()

        self.__base_data_class_message_converter = BaseDataClassMessageConverter()

        # 临时变量
        self.__device_information_write_event = asyncio.Event()
        self.__current_add_data_subject_id = None
        self.__current_add_data_block_id = None
        self.__current_data_cache = deque()
        # 缓存数据记录位置（绝对采样点位置）。
        # online 数据切换到下一批次时，Collector 的 data_position / event_position 仍会继续递增，
        # 不能在算法侧重新从 0 编号，否则 trigger 无法再插入到对应数据包里。
        self.__current_data_cache_position: Union[int, None] = None

        # 读取数据记录位
        self.__used_data_position = 0

        self.__model_class_for_operate_func_dict = {
            ControlPackageModel: self.__control_model_process,
            DevicePackageModel: self.__device_model_process,
            EventPackageModel: self.__event_model_process,
            DataPackageModel: self.__data_model_process,
            ImpedancePackageModel: self.__impedance_model_process,
            InformationPackageModel: self.__information_model_process,
        }

        self.__finish_flag = False
        self.__calibration_chunk_buffer_model: Union[CalibrationChunkBufferModel, None] = None
        self.__calibration_chunk_timeout_task: Union[asyncio.Task, None] = None
        self.__pending_message_model_deque = deque[AlgorithmDataMessageModel]()
        self.__deferred_message_count = 0
        self.__active_consumer_phase: Union[str, None] = None
        self.__receiver_failure_exception: Union[Exception, None] = None
        self.__calibration_chunk_session_serial: int = 0

        self.__logger = logging.getLogger("algorithmLogger")

    def get_source_label(self) -> str:
        return self.__source_label

    def get_used_data_position(self) -> int:
        return self.__used_data_position

    async def get_data(self) -> AlgorithmContinuousDataObject:
        self.__active_consumer_phase = self.__CONSUMER_PHASE_ONLINE
        await asyncio.sleep(0)  # 允许协程切换
        self.__raise_if_receiver_failed()
        if self.__finish_flag:
            raise AlgorithmSourceReceiverIsTurnedOffException("数据源已关闭")
        # 在线阶段从 data_queue 取下一包。
        algorithm_data_object = await self.__data_queue.get()
        self.__raise_if_receiver_failed()
        # data_deque 只镜像“真实在线数据包”。
        # 某些阶段切换 marker 只负责唤醒 run() 返回，不参与 event 回填，因此不会进入 data_deque。
        if self.__data_deque and self.__data_deque[0] is algorithm_data_object:
            self.__data_deque.popleft()
        self.__logger.debug(
            f"获取数据，data queue size{self.__data_queue.qsize()} data deque size {len(self.__data_deque)}")
        dequeue_wallclock = time.time()
        setattr(algorithm_data_object, '_receiver_dequeue_wallclock', dequeue_wallclock)
        enqueue_wallclock = getattr(algorithm_data_object, '_receiver_enqueue_wallclock', None)
        queue_wait_ms = None
        if enqueue_wallclock is not None:
            queue_wait_ms = max(0.0, (dequeue_wallclock - float(enqueue_wallclock)) * 1000.0)
            setattr(algorithm_data_object, '_receiver_queue_wait_ms', queue_wait_ms)
        if (
            algorithm_data_object.data is not None
            and algorithm_data_object.start_position is not None
        ):
            self.__logger.debug(
                "算法侧在线数据包出队: source=%s stage=%s start_position=%s end_position=%s sample_count=%s "
                "queue_size_after_get=%s queue_wait_ms=%s trigger_summary=%s",
                self.__source_label,
                self.__format_stage_for_log(algorithm_data_object.other_information),
                algorithm_data_object.start_position,
                algorithm_data_object.start_position + algorithm_data_object.data.shape[1],
                algorithm_data_object.data.shape[1],
                self.__data_queue.qsize(),
                f"{queue_wait_ms:.3f}" if queue_wait_ms is not None else "unknown",
                self.__summarize_trigger_channel(algorithm_data_object.data),
            )

        # 记录使用数据位置
        if algorithm_data_object.data is not None and algorithm_data_object.start_position is not None:
            self.__used_data_position = algorithm_data_object.start_position + algorithm_data_object.data.shape[1]

        # 一旦拿到 finish_flag，说明这个 source 已经结束。
        self.__finish_flag = algorithm_data_object.finish_flag
        return algorithm_data_object

    async def get_device(self) -> AlgorithmDeviceObject:
        self.__raise_if_receiver_failed()
        try:
            if self.__device_information_wait_timeout_seconds > 0:
                await asyncio.wait_for(
                    self.__device_information_write_event.wait(),
                    timeout=self.__device_information_wait_timeout_seconds,
                )
            else:
                await self.__device_information_write_event.wait()
        except asyncio.TimeoutError:
            await self.__mark_receiver_failed(
                RuntimeError(
                    "等待设备信息超时，"
                    f"{self.__device_information_wait_timeout_seconds:.1f}s 内未收到 DevicePackage"
                )
            )
        self.__raise_if_receiver_failed()
        self.__logger.debug(f"获取设备信息{self.__algorithm_device_object}")
        return self.__algorithm_device_object

    async def get_calibration(self) -> AlgorithmCalibrationObject:
        self.__active_consumer_phase = self.__CONSUMER_PHASE_CALIBRATION
        await asyncio.sleep(0)
        self.__raise_if_receiver_failed()
        calibration_object = await self.__calibration_queue.get()
        self.__raise_if_receiver_failed()
        if not calibration_object.finish_flag:
            stage_seed = seed_everything_for_stage(
                calibration_object.subject_id,
                calibration_object.exp_name,
                calibration_object.exp_task,
                calibration_object.session_id,
            )
            calibration_object.stage_seed = stage_seed
            self.__logger.info(
                "calibration stage random state reset: source=%s stage=%s/%s/%s/%s seed=%s",
                self.__source_label,
                calibration_object.subject_id,
                calibration_object.exp_name,
                calibration_object.exp_task,
                calibration_object.session_id,
                stage_seed,
            )
        return calibration_object

    async def set_message_model(self, message_model: AlgorithmDataMessageModel):
        self.__raise_if_receiver_failed()
        await self.__dispatch_message_model(message_model)

    def set_source_label(self, source_label: str):
        self.__source_label = source_label

    def set_configuration(self, configuration: dict[str, Union[str, dict]]):
        self.__chunk_size = configuration['chunk_size'] \
            if configuration is not None and 'chunk_size' in configuration else 0
        if configuration is not None and 'device_information_wait_timeout_seconds' in configuration:
            timeout_seconds = float(
                configuration.get(
                    'device_information_wait_timeout_seconds',
                    self.__DEFAULT_DEVICE_INFORMATION_WAIT_TIMEOUT_SECONDS,
                ) or self.__DEFAULT_DEVICE_INFORMATION_WAIT_TIMEOUT_SECONDS
            )
            self.__device_information_wait_timeout_seconds = max(0.0, timeout_seconds)
        if configuration is not None and 'calibration_object_wait_timeout_seconds' in configuration:
            timeout_seconds = float(
                configuration.get(
                    'calibration_object_wait_timeout_seconds',
                    self.__DEFAULT_CALIBRATION_OBJECT_WAIT_TIMEOUT_SECONDS,
                ) or self.__DEFAULT_CALIBRATION_OBJECT_WAIT_TIMEOUT_SECONDS
            )
            self.__calibration_object_wait_timeout_seconds = max(0.0, timeout_seconds)
        if configuration is not None and 'calibration_chunk_assembly_timeout_seconds' in configuration:
            timeout_seconds = float(
                configuration.get(
                    'calibration_chunk_assembly_timeout_seconds',
                    self.__DEFAULT_CALIBRATION_CHUNK_ASSEMBLY_TIMEOUT_SECONDS,
                ) or self.__DEFAULT_CALIBRATION_CHUNK_ASSEMBLY_TIMEOUT_SECONDS
            )
            self.__calibration_chunk_assembly_timeout_seconds = max(0.0, timeout_seconds)

    def set_required_channel_labels(self, channel_label_list: list[str]):
        # 这里保存的是算法声明的目标通道顺序。设备信息到达后，会基于真实设备标签做一次映射校验。
        self.__required_channel_label_list = list(channel_label_list or [])
        self.__required_channel_index_list = None
        if self.__incoming_channel_label_list:
            self.__apply_required_channel_mapping()

    async def __dispatch_message_model(
        self,
        message_model: AlgorithmDataMessageModel,
        allow_defer: bool = True,
        allow_flush_pending: bool = True,
    ):
        self.__raise_if_receiver_failed()
        package = message_model.package
        if allow_defer and self.__should_defer_message_package(package):
            self.__pending_message_model_deque.append(message_model)
            self.__deferred_message_count += 1
            if (self.__deferred_message_count <= 5
                    or self.__deferred_message_count % 200 == 0
                    or isinstance(package, (DevicePackageModel, ControlPackageModel))):
                self.__logger.warning(
                    "校准分块未收齐，暂存后续消息: source=%s package=%s pending=%s deferred_count=%s",
                    message_model.source_label,
                    type(package).__name__,
                    len(self.__pending_message_model_deque),
                    self.__deferred_message_count,
                )
            return

        func = self.__model_class_for_operate_func_dict[type(package)]
        self.__logger.debug(f"收到消息内容{type(package).__name__}")
        if asyncio.iscoroutinefunction(func):
            await func(package)
        else:
            func(package)

        if allow_flush_pending and self.__calibration_chunk_buffer_model is None:
            await self.__flush_pending_message_model_deque()

    async def __flush_pending_message_model_deque(self) -> None:
        if len(self.__pending_message_model_deque) == 0:
            return
        self.__logger.info(
            "校准分块已收齐，开始回放暂存消息: count=%s",
            len(self.__pending_message_model_deque),
        )
        self.__deferred_message_count = 0
        while self.__calibration_chunk_buffer_model is None and len(self.__pending_message_model_deque) > 0:
            pending_message_model = self.__pending_message_model_deque.popleft()
            await self.__dispatch_message_model(
                pending_message_model,
                allow_defer=True,
                allow_flush_pending=False,
            )

    def __should_defer_message_package(self, package) -> bool:
        if self.__calibration_chunk_buffer_model is None:
            return False
        if isinstance(package, (DevicePackageModel, ControlPackageModel)):
            return False
        return not (
            isinstance(package, DataPackageModel)
            and isinstance(package.data, (bytes, bytearray))
        )

    async def __control_model_process(self, control_model: ControlPackageModel):
        # 收到 end_flag 后：
        # 1. 先把残余缓存凑成最后一包在线数据；
        # 2. 再向在线队列和校准队列各塞一个结束标记。
        if control_model.end_flag:
            if self.__calibration_chunk_buffer_model is not None:
                await self.__mark_receiver_failed(
                    RuntimeError("收到 end_flag 时校准分块仍未接收完整")
                )
                raise self.__receiver_failure_exception
            # 判断数据缓冲区是否还有数据,如果有，则生成AlgorithmDataObject
            if len(self.__current_data_cache) > 0:
                # 获取数据
                new_data_list = [self.__current_data_cache.popleft() for _ in range(len(self.__current_data_cache))]
                # 生成 AlgorithmDataObject
                algorithm_data_object = self.__create_algorithm_data_object(
                    new_data_list,
                    self.__current_data_cache_position,
                )
                await self.__data_queue.put(algorithm_data_object)
                self.__data_deque.append(algorithm_data_object)
                self.__current_data_cache_position += algorithm_data_object.data.shape[1]
            # 发送终止标记位
            current_subject_id = (
                self.__current_add_data_subject_id
                or (self.__algorithm_device_object.other_information or {}).get('subject_id')
            )
            finish_algorithm_data_object = AlgorithmContinuousDataObject(data=None,
                                                                         subject_id=current_subject_id,
                                                                         other_information=dict(
                                                                             self.__algorithm_device_object.other_information or {}
                                                                         ),
                                                                         finish_flag=control_model.end_flag)
            await self.__data_queue.put(finish_algorithm_data_object)
            self.__data_deque.append(finish_algorithm_data_object)
            await self.__calibration_queue.put(AlgorithmCalibrationObject(finish_flag=True))
        return

    async def __data_model_process(self, data_model: DataPackageModel):
        # DataPackage 有两种完全不同的含义：
        # 1. calibration 阶段：data 是 bytes，里面其实是 npz；
        # 2. online 阶段：data 是连续采样点。
        new_data = data_model.data
        current_other_information = self.__algorithm_device_object.other_information or {}
        current_stream_role = current_other_information.get('stream_role', 'online')
        if isinstance(new_data, (bytes, bytearray)):
            calibration_chunk_bytes = bytes(new_data)
            if current_stream_role == 'calibration':
                # calibration 阶段直接解码后放入 calibration_queue，不进入在线缓存。
                await self.__process_calibration_chunk_bytes(
                    calibration_chunk_bytes=calibration_chunk_bytes,
                    current_stream_role=current_stream_role,
                )
                return
            # ProcessHub 当前会把 calibration 私有源和 online 共享源统一映射到同一个算法 source_label。
            # 阶段切换时如果 shared topic 上的 online device info 先到，而 private topic 上的 calibration
            # chunk 后到，算法侧当前 stream_role 已经是 online；这里需要按协议头兼容迟到的校准分块。
            if self.__looks_like_calibration_chunk(calibration_chunk_bytes):
                await self.__process_calibration_chunk_bytes(
                    calibration_chunk_bytes=calibration_chunk_bytes,
                    current_stream_role=current_stream_role,
                )
                return
            raise TypeError("在线阶段不支持bytes类型DataPackage")

        if current_stream_role == 'calibration':
            if self.__active_consumer_phase == self.__CONSUMER_PHASE_ONLINE:
                current_other_information = dict(self.__algorithm_device_object.other_information or {})
                self.__logger.warning(
                    "当前stream_role=calibration但算法已进入online消费阶段，按online数据兼容处理: "
                    "source=%s stage=%s/%s/%s/%s data_position=%s",
                    self.__source_label,
                    current_other_information.get('subject_id'),
                    current_other_information.get('exp_name'),
                    current_other_information.get('exp_task'),
                    current_other_information.get('session_id'),
                    data_model.data_position,
                )
            else:
                raise TypeError("校准阶段仅支持以bytes形式发送DataPackage")

        if isinstance(new_data, np.ndarray):
            new_data = new_data.tolist()
        incoming_data_position = self.__coerce_optional_int(data_model.data_position)
        if len(self.__current_data_cache) == 0 and incoming_data_position is not None:
            self.__current_data_cache_position = incoming_data_position
        # online 阶段则把连续采样点写入缓存，凑够 chunk_size 后再生成一个在线数据包。
        self.__current_data_cache.extend(new_data)
        channel_number = self.__incoming_channel_number
        if self.__chunk_size == 0 or self.__chunk_size is None:
            package_length = len(new_data)
        else:
            package_length = self.__chunk_size * channel_number
        # 循环判断缓存队列是否达到一个数据包
        while len(self.__current_data_cache) >= package_length:
            new_data_list = [self.__current_data_cache.popleft() for _ in range(package_length)]
            # 生成 AlgorithmDataObject
            algorithm_data_object = self.__create_algorithm_data_object(
                new_data_list,
                self.__current_data_cache_position,
            )
            # 插入用于保存数据的异步队列
            enqueue_wallclock = time.time()
            setattr(algorithm_data_object, '_receiver_enqueue_wallclock', enqueue_wallclock)
            await self.__data_queue.put(algorithm_data_object)
            # 同时插入用于检索和修改内部对象
            self.__data_deque.append(algorithm_data_object)
            self.__logger.debug(
                "算法侧在线数据包入队: source=%s stage=%s start_position=%s end_position=%s sample_count=%s "
                "data_queue_size=%s trigger_summary=%s",
                self.__source_label,
                self.__format_stage_for_log(algorithm_data_object.other_information),
                algorithm_data_object.start_position,
                algorithm_data_object.start_position + algorithm_data_object.data.shape[1],
                algorithm_data_object.data.shape[1],
                self.__data_queue.qsize(),
                self.__summarize_trigger_channel(algorithm_data_object.data),
            )
            # 需要注意有可能有数据包不满的情况（如终止过一次）
            self.__current_data_cache_position = self.__current_data_cache_position + algorithm_data_object.data.shape[
                1]

    async def __device_model_process(self, device_model: DevicePackageModel):
        # DevicePackage 决定了后续如何解释 DataPackage。
        incoming_other_information = dict(device_model.other_information or {})
        incoming_stream_role = incoming_other_information.get('stream_role', 'online')
        if incoming_stream_role != 'calibration' and self.__calibration_chunk_buffer_model is not None:
            await self.__mark_receiver_failed(
                RuntimeError("校准数据分块尚未接收完整，不能切换到非calibration阶段")
            )
            raise self.__receiver_failure_exception

        previous_other_information = dict(self.__algorithm_device_object.other_information or {})
        previous_stream_role = previous_other_information.get('stream_role', 'online')
        previous_subject_id = previous_other_information.get('subject_id') or self.__current_add_data_subject_id
        incoming_subject_id = incoming_other_information.get('subject_id') or self.__current_add_data_subject_id
        previous_stage_signature = (
            previous_subject_id,
            previous_other_information.get('exp_name'),
            previous_other_information.get('exp_task'),
            previous_other_information.get('session_id'),
        )
        incoming_stage_signature = (
            incoming_subject_id,
            incoming_other_information.get('exp_name'),
            incoming_other_information.get('exp_task'),
            incoming_other_information.get('session_id'),
        )
        if incoming_subject_id is not None:
            # device 包可能先于 InformationPackage 到达，尤其是在切到新 subject 时。
            # 这里要尽早同步 subject_id，避免阶段切换 marker 仍然带着上一位被试。
            self.__current_add_data_subject_id = incoming_subject_id

        self.__incoming_channel_number = device_model.channel_number
        self.__incoming_channel_label_list = list(device_model.channel_label or [])

        # 再根据算法声明的 required_channel_labels 做通道筛选和重排。
        self.__algorithm_device_object = AlgorithmDeviceObject(
            data_type=device_model.data_type.name,
            channel_number=device_model.channel_number,
            sample_rate=device_model.sample_rate,
            channel_label=list(device_model.channel_label or []),
            other_information=incoming_other_information,
        )
        self.__apply_required_channel_mapping()
        self.__device_information_write_event.set()

        # Collector 的实际节奏是“当前阶段 online 结束后，先到 calibration device info，再到 calibration bytes”。
        # 这里不能只在“切到不同 stage”时注入 marker。
        # 指定阶段重跑时，stage_signature 可能保持不变（例如 left_vs_rest -> left_vs_rest），
        # 但算法依然必须先从 run() 退回 calibrate()；否则会一直卡在上一轮 online 等待。
        if (
            incoming_stream_role == 'calibration'
            and previous_stream_role != 'calibration'
            and any(value is not None for value in previous_stage_signature)
        ):
            dropped_packet_count = self.__reset_online_runtime_buffers_for_calibration_transition()
            if self.__active_consumer_phase == self.__CONSUMER_PHASE_ONLINE:
                marker_other_information = dict(incoming_other_information)
                marker_other_information[self.__RETURN_TO_CALIBRATION_MARKER_KEY] = True
                await self.__data_queue.put(
                    AlgorithmContinuousDataObject(
                        data=None,
                        subject_id=incoming_subject_id,
                        other_information=marker_other_information,
                        finish_flag=False,
                    )
                )
                self.__logger.info(
                    "检测到下一阶段 calibration device info，重置online缓冲并注入 stage switch marker: previous_stage=%s next_stage=%s dropped_packet_count=%s data_queue_size=%s consumer_phase=%s",
                    previous_stage_signature,
                    incoming_stage_signature,
                    dropped_packet_count,
                    self.__data_queue.qsize(),
                    self.__active_consumer_phase,
                )
            else:
                self.__logger.info(
                    "检测到下一阶段 calibration device info，重置online缓冲但不注入 marker: previous_stage=%s next_stage=%s dropped_packet_count=%s consumer_phase=%s",
                    previous_stage_signature,
                    incoming_stage_signature,
                    dropped_packet_count,
                    self.__active_consumer_phase,
                )

    def __reset_online_runtime_buffers_for_calibration_transition(self) -> int:
        dropped_packet_count = 0
        while not self.__data_queue.empty():
            try:
                self.__data_queue.get_nowait()
                dropped_packet_count += 1
            except asyncio.QueueEmpty:
                break
        self.__data_deque.clear()
        self.__event_list.clear()
        self.__current_data_cache.clear()
        self.__current_data_cache_position = None
        self.__used_data_position = 0
        return dropped_packet_count

    @classmethod
    def is_return_to_calibration_marker(
        cls,
        algorithm_data_object: AlgorithmContinuousDataObject,
    ) -> bool:
        if algorithm_data_object is None:
            return False
        if algorithm_data_object.finish_flag:
            return False
        if algorithm_data_object.data is not None:
            return False
        other_information = algorithm_data_object.other_information or {}
        return bool(other_information.get(cls.__RETURN_TO_CALIBRATION_MARKER_KEY))

    def __event_model_process(self, event_model: EventPackageModel):
        # Event 先拆成 SingleEvent；
        # 如果能找到对应在线数据包，就直接写进去；否则先缓存。
        single_event_list = [
            SingleEvent(event_position=int(event_model.event_position[i]), event_data=float(event_model.event_data[i]))
            for i in range(len(event_model.event_position))]

        for single_event in single_event_list:
            bisect.insort_left(self.__event_list, single_event)

        if len(self.__data_deque) == 0:
            return

        for single_event_index in range(len(self.__event_list) - 1, -1, -1):

            if self.__event_list[single_event_index].event_position \
                    > self.__data_deque[-1].start_position + self.__data_deque[-1].data.shape[1]:
                # 如果event的位置大于最后一个数据包中所有元素的位置，则跳过
                continue

            elif self.__event_list[single_event_index].event_position < self.__data_deque[0].start_position:
                # 如果event的位置小于第一个数据包中所有元素的位置，则删除之前所有事件记录，并退出（因为数据包已经被使用了）
                self.__event_list[:] = self.__event_list[single_event_index + 1:]
                return
            else:
                # 遍历数据包中的所有元素，找到第一个小于等于event的位置，且包内数据结尾大于等于event位置的包，然后插入event
                for data_index in range(len(self.__data_deque) - 1, -1, -1):
                    if self.__data_deque[data_index].start_position \
                            <= self.__event_list[single_event_index].event_position \
                            < self.__data_deque[data_index].start_position + \
                            self.__data_deque[data_index].data.shape[1]:
                        self.__insert_event_to_algorithm_data_object(
                            self.__data_deque[data_index],
                            self.__event_list[single_event_index])
                        if self.__event_list[single_event_index].event_data in {101.0, 241.0, 242.0}:
                            self.__logger.debug(
                                "算法侧事件回填到在线数据包: source=%s stage=%s event_position=%s event_data=%s "
                                "packet_start_position=%s packet_end_position=%s pending_packet_count=%s",
                                self.__source_label,
                                self.__format_stage_for_log(self.__data_deque[data_index].other_information),
                                self.__event_list[single_event_index].event_position,
                                int(self.__event_list[single_event_index].event_data),
                                self.__data_deque[data_index].start_position,
                                self.__data_deque[data_index].start_position + self.__data_deque[data_index].data.shape[1],
                                len(self.__data_deque),
                            )
                        self.__event_list.pop(single_event_index)
                        break

    def __impedance_model_process(self, impedance_model: ImpedancePackageModel):
        # 阻抗信息暂不处理
        return

    def __information_model_process(self, information_model: InformationPackageModel):
        self.__current_add_data_subject_id = information_model.subject_id
        self.__current_add_data_block_id = information_model.block_id

    def __create_algorithm_data_object(self, new_data_list: list[float], start_position: Union[int, None]) \
            -> AlgorithmContinuousDataObject:
        # 在线阶段最核心的数据变换：
        # 一维序列 -> [sample, channel] -> 转置成 [channel, sample]
        # -> 选择所需通道 -> 最后一行补 trigger 通道。
        incoming_channel_number = self.__incoming_channel_number
        if incoming_channel_number <= 0:
            raise ValueError("尚未收到有效设备通道信息，无法解码在线数据")
        if start_position is None:
            raise ValueError("在线数据缺少绝对起始位置，无法对齐 trigger")
        sample_number = len(new_data_list) / incoming_channel_number
        if not (isinstance(sample_number, int)
                or (math.floor(sample_number) == sample_number and not math.isnan(sample_number))):
            raise Exception("sample_number is not a integer")
        sample_number = int(sample_number)
        data = np.array(new_data_list)
        # 数据传入时是按照先通道再采样点，所以需要重塑
        data = data.reshape(sample_number, incoming_channel_number)
        data = data.T
        data = self.__select_required_channels(data)
        output_channel_number = data.shape[0]
        zero_row = np.zeros((1, data.shape[1]))
        padded_data = np.concatenate((data, zero_row), axis=0)

        # 判断是否有事件在当前数据包中
        event_index = len(self.__event_list) - 1
        while event_index >= 0 and self.__event_list[event_index].event_position >= start_position:
            # 如果当前事件的position大于等于start_position且小于start_position+data.shape[1]，则插入并从event_list中删除它
            if start_position <= self.__event_list[event_index].event_position < start_position + data.shape[1]:
                # 计算对应的event位置，并插入数据
                relative_position = self.__event_list[event_index].event_position - start_position
                padded_data[output_channel_number, relative_position] = self.__event_list[event_index].event_data
                # 从列表中删除指定位置的事件
                del self.__event_list[event_index]

            event_index = event_index - 1

        return AlgorithmContinuousDataObject(
            start_position=start_position,
            data=padded_data,
            subject_id=(
                self.__current_add_data_subject_id
                or (self.__algorithm_device_object.other_information or {}).get('subject_id')
            ),
            other_information=dict(self.__algorithm_device_object.other_information or {}),
            finish_flag=False)

    @staticmethod
    def __coerce_optional_int(value) -> Union[int, None]:
        if value is None:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def __insert_event_to_algorithm_data_object(
            self, algorithm_data_object: AlgorithmContinuousDataObject, single_event: SingleEvent) \
            -> AlgorithmContinuousDataObject:
        # 计算相对位置
        relative_position = single_event.event_position - algorithm_data_object.start_position
        if relative_position < 0 or relative_position >= algorithm_data_object.data.shape[1]:
            raise ValueError("event_position is out of range")
        else:
            algorithm_data_object.data[self.__algorithm_device_object.channel_number, relative_position] = \
                single_event.event_data
        return algorithm_data_object

    @staticmethod
    def __format_stage_for_log(other_information: dict | None) -> str:
        stage_information = dict(other_information or {})
        return "%s|%s|%s|%s|%s" % (
            stage_information.get('subject_id'),
            stage_information.get('exp_name'),
            stage_information.get('exp_task'),
            stage_information.get('session_id'),
            stage_information.get('stream_role'),
        )

    @staticmethod
    def __summarize_trigger_channel(data: np.ndarray | None) -> str:
        if data is None or getattr(data, 'shape', None) is None or data.shape[0] <= 0:
            return "none"
        trigger_row = data[-1]
        non_zero_index_array = np.nonzero(trigger_row)[0]
        if non_zero_index_array.size == 0:
            return "none"
        summary_item_list = []
        for relative_position in non_zero_index_array[:8]:
            summary_item_list.append(
                f"{int(relative_position)}:{int(trigger_row[int(relative_position)])}"
            )
        if non_zero_index_array.size > 8:
            summary_item_list.append(f"...(+{int(non_zero_index_array.size - 8)})")
        return ",".join(summary_item_list)

    def __decode_calibration_object(self, calibration_bytes: bytes) -> AlgorithmCalibrationObject:
        # calibration bytes 实际来自 np.savez 压缩后的二进制内容。
        with np.load(io.BytesIO(calibration_bytes), allow_pickle=False) as calibration_npz_file:
            subject_id = str(calibration_npz_file['subject_id'].item())
            exp_name = str(calibration_npz_file['exp_name'].item())
            exp_task = str(calibration_npz_file['exp_task'].item())
            session_id = str(calibration_npz_file['session_id'].item())
            session_data = np.asarray(calibration_npz_file['data'], dtype=np.float32)
            if session_data.ndim == 3:
                session_data = self.__select_required_channels(session_data)
            session_data_dict = {
                session_id: {
                    'data': session_data,
                    'label': np.asarray(calibration_npz_file['label'], dtype=np.int64),
                }
            }

        return AlgorithmCalibrationObject(
            subject_id=subject_id,
            exp_name=exp_name,
            exp_task=exp_task,
            session_id=session_id,
            session_data_dict=session_data_dict,
            finish_flag=False,
        )

    @classmethod
    def __looks_like_calibration_chunk(cls, calibration_chunk_bytes: bytes) -> bool:
        if len(calibration_chunk_bytes) < cls.__CALIBRATION_CHUNK_HEADER_SIZE:
            return False
        return calibration_chunk_bytes[:len(cls.__CALIBRATION_CHUNK_MAGIC)] == cls.__CALIBRATION_CHUNK_MAGIC

    async def __process_calibration_chunk_bytes(
        self,
        calibration_chunk_bytes: bytes,
        current_stream_role: str,
    ) -> None:
        if current_stream_role != 'calibration':
            current_other_information = dict(self.__algorithm_device_object.other_information or {})
            self.__logger.warning(
                "当前stream_role=%s 但收到校准分块，按迟到calibration chunk兼容处理: source=%s active_consumer_phase=%s stage=%s/%s/%s/%s pending=%s",
                current_stream_role,
                self.__source_label,
                self.__active_consumer_phase,
                current_other_information.get('subject_id'),
                current_other_information.get('exp_name'),
                current_other_information.get('exp_task'),
                current_other_information.get('session_id'),
                len(self.__pending_message_model_deque),
            )
        try:
            calibration_object = self.__append_calibration_chunk_and_try_decode(calibration_chunk_bytes)
        except Exception:
            self.__cancel_calibration_chunk_timeout_task()
            raise
        if calibration_object is not None:
            await self.__calibration_queue.put(calibration_object)

    def __append_calibration_chunk_and_try_decode(
        self,
        calibration_chunk_bytes: bytes,
    ) -> Union[AlgorithmCalibrationObject, None]:
        if len(calibration_chunk_bytes) < self.__CALIBRATION_CHUNK_HEADER_SIZE:
            raise ValueError("校准分块长度小于协议头长度")

        chunk_header = calibration_chunk_bytes[:self.__CALIBRATION_CHUNK_HEADER_SIZE]
        chunk_payload = calibration_chunk_bytes[self.__CALIBRATION_CHUNK_HEADER_SIZE:]
        chunk_magic, total_chunk_number, chunk_index, total_payload_size = struct.unpack(
            self.__CALIBRATION_CHUNK_HEADER_FORMAT,
            chunk_header,
        )
        if chunk_magic != self.__CALIBRATION_CHUNK_MAGIC:
            raise ValueError("校准分块协议头标记不正确")
        if total_chunk_number <= 0:
            raise ValueError(f"校准分块总数非法: {total_chunk_number}")
        if chunk_index < 0 or chunk_index >= total_chunk_number:
            raise ValueError(f"校准分块序号越界: {chunk_index}/{total_chunk_number}")
        if total_payload_size < 0:
            raise ValueError(f"校准负载总长度非法: {total_payload_size}")

        if self.__calibration_chunk_buffer_model is None:
            now_wallclock = time.time()
            self.__calibration_chunk_session_serial += 1
            self.__calibration_chunk_buffer_model = CalibrationChunkBufferModel(
                total_chunk_number=total_chunk_number,
                total_payload_size=total_payload_size,
                chunk_payload_dict={},
                first_chunk_wallclock=now_wallclock,
                last_chunk_wallclock=now_wallclock,
            )
        else:
            if self.__calibration_chunk_buffer_model.total_chunk_number != total_chunk_number:
                raise ValueError("校准分块总数与已缓存分块不一致")
            if self.__calibration_chunk_buffer_model.total_payload_size != total_payload_size:
                raise ValueError("校准负载总长度与已缓存分块不一致")

        if chunk_index in self.__calibration_chunk_buffer_model.chunk_payload_dict:
            self.__logger.warning(
                "收到重复的校准分块，忽略该块: source=%s chunk_index=%s total_chunk_number=%s buffered=%s",
                self.__source_label,
                chunk_index,
                total_chunk_number,
                len(self.__calibration_chunk_buffer_model.chunk_payload_dict),
            )
            self.__refresh_calibration_chunk_timeout_task()
            return None
        self.__calibration_chunk_buffer_model.chunk_payload_dict[chunk_index] = chunk_payload
        self.__calibration_chunk_buffer_model.last_chunk_wallclock = time.time()
        received_chunk_number = len(self.__calibration_chunk_buffer_model.chunk_payload_dict)
        if (
            received_chunk_number <= 3
            or received_chunk_number == total_chunk_number
            or received_chunk_number % 5 == 0
        ):
            missing_chunk_index_list = [
                index
                for index in range(total_chunk_number)
                if index not in self.__calibration_chunk_buffer_model.chunk_payload_dict
            ]
            self.__logger.info(
                "收到校准分块: source=%s chunk_index=%s total_chunk_number=%s received=%s missing_preview=%s chunk_payload_bytes=%s session_serial=%s",
                self.__source_label,
                chunk_index,
                total_chunk_number,
                received_chunk_number,
                missing_chunk_index_list[:8],
                len(chunk_payload),
                self.__calibration_chunk_session_serial,
            )

        if received_chunk_number != total_chunk_number:
            self.__refresh_calibration_chunk_timeout_task()
            return None

        calibration_payload = b''.join(
            self.__calibration_chunk_buffer_model.chunk_payload_dict[index]
            for index in range(total_chunk_number)
        )
        if len(calibration_payload) != total_payload_size:
            raise ValueError(
                f"校准分块重组长度不匹配，期望 {total_payload_size} 字节，实际 {len(calibration_payload)} 字节"
            )

        self.__cancel_calibration_chunk_timeout_task()
        completed_chunk_session_serial = self.__calibration_chunk_session_serial
        assembly_elapsed_ms = (
            self.__calibration_chunk_buffer_model.last_chunk_wallclock
            - self.__calibration_chunk_buffer_model.first_chunk_wallclock
        ) * 1000.0
        self.__calibration_chunk_buffer_model = None
        self.__logger.info(
            "校准分块组装完成: source=%s total_chunk_number=%s total_payload_size=%s assembly_elapsed_ms=%.3f session_serial=%s",
            self.__source_label,
            total_chunk_number,
            total_payload_size,
            assembly_elapsed_ms,
            completed_chunk_session_serial,
        )
        return self.__decode_calibration_object(calibration_payload)

    def __refresh_calibration_chunk_timeout_task(self) -> None:
        if self.__calibration_chunk_assembly_timeout_seconds <= 0:
            return
        self.__cancel_calibration_chunk_timeout_task()
        self.__calibration_chunk_timeout_task = asyncio.create_task(
            self.__wait_and_fail_on_calibration_chunk_timeout(
                self.__resolve_calibration_chunk_timeout_seconds()
            )
        )

    def __resolve_calibration_chunk_timeout_seconds(self) -> float:
        calibration_chunk_buffer_model = self.__calibration_chunk_buffer_model
        configured_timeout_seconds = max(
            0.0,
            float(self.__calibration_chunk_assembly_timeout_seconds or 0.0),
        )
        if calibration_chunk_buffer_model is None:
            return configured_timeout_seconds
        adaptive_timeout_seconds = max(
            self.__MIN_CALIBRATION_CHUNK_ASSEMBLY_TIMEOUT_SECONDS,
            float(calibration_chunk_buffer_model.total_chunk_number)
            * self.__CALIBRATION_CHUNK_TIMEOUT_SECONDS_PER_CHUNK,
        )
        return max(configured_timeout_seconds, adaptive_timeout_seconds)

    def __cancel_calibration_chunk_timeout_task(self) -> None:
        timeout_task = self.__calibration_chunk_timeout_task
        self.__calibration_chunk_timeout_task = None
        if timeout_task is not None and not timeout_task.done():
            timeout_task.cancel()

    async def __wait_and_fail_on_calibration_chunk_timeout(self, timeout_seconds: float) -> None:
        try:
            await asyncio.sleep(timeout_seconds)
            calibration_chunk_buffer_model = self.__calibration_chunk_buffer_model
            if calibration_chunk_buffer_model is None or self.__receiver_failure_exception is not None:
                return
            received_chunk_number = len(calibration_chunk_buffer_model.chunk_payload_dict)
            total_chunk_number = calibration_chunk_buffer_model.total_chunk_number
            current_other_information = dict(self.__algorithm_device_object.other_information or {})
            missing_chunk_index_list = [
                index
                for index in range(total_chunk_number)
                if index not in calibration_chunk_buffer_model.chunk_payload_dict
            ]
            await self.__mark_receiver_failed(
                RuntimeError(
                    "校准分块接收超时，等待 "
                    f"{timeout_seconds:.1f}s 仍未收齐: "
                    f"received={received_chunk_number}/{total_chunk_number}"
                )
            )
            self.__logger.error(
                "校准分块接收超时，触发本地失败保护: source=%s timeout_seconds=%.1f received=%s/%s "
                "stage=%s/%s/%s/%s pending=%s active_consumer_phase=%s missing_preview=%s session_serial=%s",
                self.__source_label,
                timeout_seconds,
                received_chunk_number,
                total_chunk_number,
                current_other_information.get('subject_id'),
                current_other_information.get('exp_name'),
                current_other_information.get('exp_task'),
                current_other_information.get('session_id'),
                len(self.__pending_message_model_deque),
                self.__active_consumer_phase,
                missing_chunk_index_list[:8],
                self.__calibration_chunk_session_serial,
            )
        except asyncio.CancelledError:
            return

    async def __mark_receiver_failed(self, exc: Exception) -> None:
        if self.__receiver_failure_exception is not None:
            return
        pending_message_count = len(self.__pending_message_model_deque)
        self.__receiver_failure_exception = exc
        self.__cancel_calibration_chunk_timeout_task()
        self.__calibration_chunk_buffer_model = None
        self.__pending_message_model_deque.clear()
        self.__deferred_message_count = 0
        self.__device_information_write_event.set()
        self.__logger.error(
            "数据源进入失败状态，停止继续接收并唤醒等待中的算法消费者: source=%s pending_cleared=%s exc=%s",
            self.__source_label,
            pending_message_count,
            exc,
        )
        await self.__data_queue.put(
            AlgorithmContinuousDataObject(
                data=None,
                subject_id=self.__current_add_data_subject_id,
                other_information=dict(self.__algorithm_device_object.other_information or {}),
                finish_flag=False,
            )
        )
        await self.__calibration_queue.put(AlgorithmCalibrationObject(finish_flag=False))

    def __raise_if_receiver_failed(self) -> None:
        if self.__receiver_failure_exception is not None:
            raise self.__receiver_failure_exception

    def __apply_required_channel_mapping(self) -> None:
        # 根据算法声明的通道名，在设备原始通道中找到对应下标。
        if len(self.__required_channel_label_list) == 0:
            raise ValueError(
                "算法未为数据源显式声明通道列表，正式运行不允许按原始全通道默认转发: "
                f"source_label={self.__source_label}"
            )

        normalized_available_label_to_index_dict = {}
        for index, channel_label in enumerate(self.__incoming_channel_label_list):
            normalized_channel_label = self.__normalize_channel_label(channel_label)
            if normalized_channel_label in normalized_available_label_to_index_dict:
                raise ValueError(f"设备通道名标准化后重复，无法唯一映射: {channel_label}")
            normalized_available_label_to_index_dict[normalized_channel_label] = index

        required_channel_index_list = []
        missing_channel_label_list = []
        for required_channel_label in self.__required_channel_label_list:
            normalized_required_channel_label = self.__normalize_channel_label(required_channel_label)
            if normalized_required_channel_label not in normalized_available_label_to_index_dict:
                missing_channel_label_list.append(required_channel_label)
                continue
            required_channel_index_list.append(
                normalized_available_label_to_index_dict[normalized_required_channel_label]
            )

        if missing_channel_label_list:
            raise ValueError(
                "算法声明的通道在设备信息中不存在，请检查选手端通道配置是否与裁判机设备标签一致: "
                f"source_label={self.__source_label} missing_channel_labels={missing_channel_label_list} "
                f"available_channel_labels={self.__incoming_channel_label_list}"
            )

        self.__required_channel_index_list = required_channel_index_list
        self.__algorithm_device_object.channel_number = len(required_channel_index_list)
        self.__algorithm_device_object.channel_label = list(self.__required_channel_label_list)
        self.__logger.info(
            "数据源 %s 通道映射完成: original_channel_number=%s selected_channel_number=%s selected_channel_label=%s",
            self.__source_label,
            self.__incoming_channel_number,
            self.__algorithm_device_object.channel_number,
            self.__algorithm_device_object.channel_label,
        )

    def __select_required_channels(self, data: np.ndarray) -> np.ndarray:
        if self.__required_channel_index_list is None:
            return data
        if data.ndim == 2:
            return data[self.__required_channel_index_list, :]
        if data.ndim == 3:
            return data[:, self.__required_channel_index_list, :]
        raise ValueError(f"不支持的数据维度: {data.ndim}")

    @staticmethod
    def __normalize_channel_label(channel_label: str) -> str:
        return ''.join(char for char in str(channel_label).upper() if char.isalnum())
