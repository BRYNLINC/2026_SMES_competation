import asyncio
import copy
import hashlib
import io
import json
import logging
import os
import socket
import struct
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import aiofiles
import grpc
import numpy as np
import yaml

from ApplicationFramework.api.interface.ComponentFrameworkOperatorInterface import ReceiveMessageOperatorInterface
from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from Collector.common.converter.ReceiverTransferModelToDataMessageModelConverter import \
    ReceiverTransferModelToDataMessageModelConverter
from Common.converter.CommonMessageConverter import CommonMessageConverter
from Common.model.CommonMessageModel import DataMessageModel, DataPackageModel
from Common.protobuf.CommonMessage_pb2 import DataMessage as DataMessage_pb2
from Collector.receiver.interface.ReceiverInterface import EEGReceiverInterface
from Collector.receiver.model.ReceiverTransferModel import DeviceTransferModel, TransferDataTypeEnum, \
    ReceiverTransferModel, InformationTransferModel, EventTransferModel, DataTransferModel , ControlTransferModel
from Collector.receiver.virtual_receiver.api.converter.VirtualReceiverCustomControlMessageConverter import \
    VirtualReceiverCustomControlMessageConverter
from Collector.receiver.virtual_receiver.api.message.VirtualReceiverMessageKeyEnum import (
    VirtualReceiverMessageKeyEnum)
from Collector.receiver.virtual_receiver.api.model.VirtualReceiverCustomControlModel import \
    VirtualReceiverCustomControlModel, CalibrationTrialCountControlPackageModel
from Collector.receiver.virtual_receiver.exception.VirtualReceiverException import VirtualReceiverFileNotFoundException
from Collector.receiver.virtual_receiver.model.DataFileModel import DataFileModel
from Collector.receiver.virtual_receiver.api.proto.VirtualReceiverCustomControl_pb2 import (
    VirtualReceiverCustomControlMessage as VirtualReceiverCustomControlMessage_pb2)

PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from tools.runtime_state_sqlite import (  # noqa: E402
    STATE_KEY_CURRENT_TRIAL,
    resolve_runtime_state_db_path,
    write_json_state,
)


@dataclass
class ParsedTrialModel:
    trigger_value: int
    eeg_data: np.ndarray
    original_start_position: int
    session_id: str
    file_path: str


@dataclass
class RuntimeTaskGroupModel:
    subject_id: str
    exp_name: str
    exp_task: str
    session_id: str
    data_file_model_list: list[DataFileModel]


@dataclass
class FileTaskDataModel:
    data_file_model: DataFileModel
    calibration_trial_model_list: list[ParsedTrialModel]


class VirtualReceiverImplement(EEGReceiverInterface):
    """
    虚拟 EEG 接收器。

    这个类不是连接真实采集硬件，而是从本地 `.dat` 文件回放数据。
    为了让本地调试更接近比赛流程，它不是简单逐文件直读，而是：
    1. 读取数据文件；
    2. 根据 trigger 切成 trial；
    3. 对每个 session 先发送 calibration 数据；
    4. 再发送 online 连续流；
    5. 最后发送 end_flag。
    """
    __TEXT_DAT_FORMAT = 'text'
    __BINARY_FLOAT32_DAT_FORMAT = 'binary_float32'
    __DEFAULT_TRIGGER_CHANNEL_LABEL_SET = {'TRIGGER', 'TRIG', 'STATUS', 'EVENT', 'MARKER', 'STI014'}
    __DEFAULT_AUX_CHANNEL_LABEL_SET = {'HEO', 'VEO', 'EKG', 'EMG'}
    __DEFAULT_EXP_TASK_ORDER = ('left_vs_rest', 'right_vs_rest')
    __VALID_TRIGGER_VALUE_SET = (1, 2, 3)
    __EXP_TASK_TRIGGER_VALUE_DICT = {
        'left_vs_rest': {1, 3},
        'right_vs_rest': {2, 3},
    }
    __TRIAL_START_TRIGGER = 101  # trial标记定义
    __TRIAL_END_TRIGGER = 241
    __BLOCK_START_TRIGGER = 242
    __BLOCK_END_TRIGGER = 243
    __TRIAL_DURATION_POINTS = 4000
    __HIDDEN_SCORE_SOURCE_LABEL = 'hidden_score'
    __HIDDEN_SCORE_TOPIC = 'private.hidden_score'
    __CALIBRATION_CHUNK_MAGIC = b'CAL1'
    __CALIBRATION_CHUNK_HEADER_FORMAT = '>4sIII'
    __CALIBRATION_CHUNK_HEADER_SIZE = struct.calcsize(__CALIBRATION_CHUNK_HEADER_FORMAT)
    # 正式链路会经过 Java/Kafka 桥接。
    # 这里不能把 chunk payload 贴着 1 MiB 上限发送，否则再套上 protobuf/DataMessage 外壳后
    # 容易超过默认消息大小，导致校准分块在正式环境里丢失或发送失败。
    __MAX_CALIBRATION_CHUNK_BYTES = 512 * 1024
    # 为了保证不同选手申请不同校准 trial 数量时，online 测试集保持一致：
    # 1. 每个 session 每个类别先固定保留前 10 个 trial 作为“校准候选池”；
    # 2. online 只使用这个候选池之后的 trial；
    # 3. 真正发给算法的 calibration trial 数量，再由选手申请值决定。
    __FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS = 10
    # ============================================================
    # 正式流程和 DEBUG 都先走同一个 __calibrate_trials_per_class 默认值。
    # 如果后续比赛规则调整，只需要优先检查这里。
    # ============================================================
    __DEFAULT_CALIBRATE_TRIALS_PER_CLASS = __FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS
    __DEFAULT_CALIBRATION_TRIAL_COUNT_WAIT_TIMEOUT_SECONDS = 300.0
    __RUNTIME_STAGE_EVENT_MESSAGE_KEY = 'runtime_stage_event'
    __RUNTIME_STAGE_CONTROL_MESSAGE_KEY = 'runtime_stage_control'
    __TRIAL_RELEASE_WAIT_WARNING_INTERVAL_SECONDS = 5.0
    __PREDICTION_TIMEOUT_SECONDS = 1.0
    __CALIBRATION_CHUNK_SEND_YIELD_INTERVAL = 1
    __CALIBRATION_CHUNK_SEND_YIELD_SECONDS = 0.005
    __CALIBRATION_PAYLOAD_SEND_RETRY_LIMIT = 5
    __CALIBRATION_PAYLOAD_SEND_RETRY_DELAY_SECONDS = 0.5

    def __init__(self):
        super().__init__()

        # 自定义控制 topic，外部可以通过它切换当前被试/当前 block。
        self.__virtual_receiver_command_control_topic: str = None
        self.__data_byte_width = 4  # 假设是数据是float32，每个浮点数占用4个字节
        self.__downsampling_factor: int = 1   # 降采样因子，默认为1为不降采样。采用N抽1的方式降采样。

        self.__amplifier_socket: socket = None
        self.__send_package_points: int = 0
        self.__online_replay_mode: str = 'realtime'
        self.__calibrate_trials_per_class: int = self.__DEFAULT_CALIBRATE_TRIALS_PER_CLASS
        self.__calibration_trial_count_wait_timeout_seconds: float = (
            self.__DEFAULT_CALIBRATION_TRIAL_COUNT_WAIT_TIMEOUT_SECONDS
        )
        self.__calibration_trials_per_class_by_team: dict[str, int] = {}
        self.__device_transfer_model: DeviceTransferModel = None
        self.__config_dict: dict[str, Union[str, dict]] = None
        self.__trigger_channel_label_set: set[str] = set(self.__DEFAULT_TRIGGER_CHANNEL_LABEL_SET)
        self.__aux_channel_label_set: set[str] = set(self.__DEFAULT_AUX_CHANNEL_LABEL_SET)
        self.__exp_task_order: list[str] = list(self.__DEFAULT_EXP_TASK_ORDER)

        self.__logger = logging.getLogger("collectorLogger")
        # 所有待回放的数据文件都会展开后存到这里。
        self.__data_files_model_list: list[DataFileModel] = list()

        # 当前应该从哪一个 DataFileModel 开始回放。
        self.__current_data_file_model_index: int = 0

        # 按 send_package_points 预估的一次读取字节数。
        self.__cache_bytes_number: int = 0
        self.__current_date_position = 0
        self.__last_trigger_value: int = 0
        self.__current_runtime_data_file_model: Union[DataFileModel, None] = None
        self.__current_runtime_stream_role: str = 'online'
        self.__last_sent_device_info_signature: Union[tuple[str, str, str, str], None] = None
        self.__parsed_trial_model_list_cache_dict: dict[str, list[ParsedTrialModel]] = dict()
        # 缓存被试者block信息
        self.__subject_block_dict: dict[str, int] = dict()
        # group_id / collector_component_id / team_id_list 是运行期确定性路由的基础字段。
        # 本轮改造后不再依赖 topic 名或 component_id 反推队伍归属，而是直接从 component_info 读取。
        self.__group_id: str = None
        self.__collector_component_id: str = None
        self.__team_id_list: list[str] = []
        # calibration_private_topic_by_team:
        #   每个赛队在校准阶段的私有 topic。
        #   同一 group 内可以给不同队伍发送不同数量的校准trial，但 online 仍然共享一份数据流。
        self.__calibration_private_topic_by_team: dict[str, str] = {}
        # runtime_stage_event_topic / runtime_stage_control_topic:
        #   Collector 与 RuntimeStageCoordinator 之间的事件 / 控制通道。
        #   前者上报“当前stage已经准备好”等事件，后者接收“允许进入online / 放行下一个trial”等控制。
        self.__runtime_stage_event_topic: str = None
        self.__runtime_stage_control_topic: str = None
        # 由于一个 Collector 需要同时直发多个 calibration 私有 topic，
        # 这里为每个 team 构造独立 message_key，避免复用 send_data 时相互覆盖订阅关系。
        self.__team_calibration_message_key_by_team: dict[str, str] = {}
        # 两类 asyncio.Event 分别实现两级门控：
        # 1. online_stage_release_event: 整个 session 的 online 是否允许开始；
        # 2. online_stage_complete_event: 当前 session 是否允许收尾并进入下一 stage；
        # 3. trial_release_event: 当前 session 的某个 trial 是否允许继续发送。
        self.__online_stage_release_event_dict: dict[str, asyncio.Event] = {}
        self.__online_stage_complete_event_dict: dict[str, asyncio.Event] = {}
        self.__trial_release_event_dict: dict[tuple[str, int], asyncio.Event] = {}
        self.__trial_release_payload_by_stage_trial_key: dict[tuple[str, int], dict] = {}
        # 校准 trial 申请值需要显式等所有队伍到齐后，才能开始真正切分 session。
        # 否则 Collector 会先按默认值切出校准集，后续再收到申请值时已经来不及回溯修正。
        self.__received_calibration_trial_count_team_id_set: set[str] = set()
        self.__calibration_trial_count_ready_event: asyncio.Event = asyncio.Event()

        self.__read_data_task: asyncio.Task = None
        self.__trial_post_send_task_set: set[asyncio.Task] = set()

        self.__send_flag_event: asyncio.Event = asyncio.Event()

        self.__shutdown_flag: bool = False
        ###############
        # debug入口使用：允许在单进程调试时覆盖Collector工作目录，避免依赖独立进程的cwd
        self.__workspace_path_override: str = None
        self.__config_path_override: str = None
        self.__debug_exp_name_filter: str = None
        self.__debug_subject_id_filter: str = None
        ###############

    async def initial(self, config_dict: dict[str, Union[str, dict]] = None) -> None:
        # 读取虚拟接收器自身配置。
        current_file_path = os.path.abspath(__file__)
        directory_path = os.path.dirname(current_file_path)
        receiver_config_file_name = 'VirtualReceiverConfig.yml'
        receiver_config_path = self.__config_path_override or os.path.join(directory_path, receiver_config_file_name)
        with open(receiver_config_path, 'r', encoding='utf-8') as f:
            self.__config_dict = yaml.safe_load(f)

        # custom control topic 优先级：
        # 1. 运行期 component_info.message（正式链路 / 中控生成配置）；
        # 2. 本地 VirtualReceiverConfig.yml（单组件调试）；
        # 3. 若两者都没有，再在 __load_runtime_routing() 里按 collector_component_id 做确定性回退。
        local_message_dict = self.__config_dict.get("message", dict()) or {}
        runtime_message_dict = (config_dict or {}).get("message", dict()) or {}
        self.__virtual_receiver_command_control_topic = (
            runtime_message_dict.get(VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value)
            or local_message_dict.get(VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value)
        )
        self.__logger.info(
            "初始化 VirtualReceiver custom control topic: runtime_topic=%s local_topic=%s resolved_topic=%s",
            runtime_message_dict.get(VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value),
            local_message_dict.get(VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value),
            self.__virtual_receiver_command_control_topic,
        )

        send_config_dict = self.__config_dict.get("send_config", dict())
        self.__send_package_points = send_config_dict.get("send_package_points", 0)
        self.__online_replay_mode = str(send_config_dict.get("online_replay_mode", "realtime") or "realtime").strip().lower()

        device_info_dict = self.__config_dict.get("device_info", dict())
        channel_label = list(device_info_dict.get("channel_label", dict()).keys())
        configured_channel_number = device_info_dict.get("channel_number", None)
        if channel_label and configured_channel_number not in (None, len(channel_label)):
            self.__logger.warning(
                "VirtualReceiverConfig channel_number=%s 与 channel_label 数量=%s 不一致，将按 channel_label 数量处理",
                configured_channel_number,
                len(channel_label),
            )
            configured_channel_number = len(channel_label)
        other_information = device_info_dict.get("other_information", dict()) or dict()
        trigger_channel_alias = other_information.get('trigger_channel_alias')
        if isinstance(trigger_channel_alias, list):
            self.__trigger_channel_label_set.update(self.__normalize_channel_label(label) for label in trigger_channel_alias)
        aux_channel_alias = other_information.get('aux_channel_alias')
        if isinstance(aux_channel_alias, list):
            self.__aux_channel_label_set.update(self.__normalize_channel_label(label) for label in aux_channel_alias)
        configured_exp_task_order = other_information.get('exp_task_order')
        if isinstance(configured_exp_task_order, list):
            normalized_exp_task_order = [
                str(exp_task).strip()
                for exp_task in configured_exp_task_order
                if str(exp_task).strip() in self.__EXP_TASK_TRIGGER_VALUE_DICT
            ]
            if normalized_exp_task_order:
                self.__exp_task_order = normalized_exp_task_order
        # 设备信息模板。
        # 真正发送时会根据当前 exp/session/stream_role 额外补充 other_information。
        self.__device_transfer_model = DeviceTransferModel(
            data_type=TransferDataTypeEnum.EEG,
            channel_number=configured_channel_number,
            sample_rate=device_info_dict.get("sample_rate", None),
            channel_label=channel_label,
            other_information=other_information
        )
        data_files_dict = self.__config_dict.get("data_files", dict())
        # 把配置文件里按 subject / exp_name 组织的路径展开成统一列表，后续方便顺序遍历。
        self.__data_files_model_list = [
            DataFileModel(
                subject_id,
                exp_name,
                exp_task,
                self.__resolve_session_id(file_path),
                file_path,
            )
            for subject_id, exp_files_dict in data_files_dict.items()
            if isinstance(exp_files_dict, dict)
            for exp_name, file_paths in exp_files_dict.items()
            if isinstance(file_paths, list)
            for file_path in file_paths
            for exp_task in self.__exp_task_order
        ]

        # 默认按“EEG 通道 + trigger 通道”估算缓存字节数。
        self.__cache_bytes_number = (
            self.__send_package_points *
            (self.__device_transfer_model.channel_number + 1) *
            self.__data_byte_width
        )

    async def startup(self) -> None:
        # 每次启动都重置运行时状态，避免把上一次 session 的信息带进来。
        self.__current_date_position = 0
        self.__last_trigger_value = 0
        self.__shutdown_flag = False
        self.__current_runtime_data_file_model = None
        self.__current_runtime_stream_role = 'online'
        self.__last_sent_device_info_signature = None
        self.__parsed_trial_model_list_cache_dict.clear()
        self.__subject_block_dict.clear()
        self.__online_stage_release_event_dict.clear()
        self.__online_stage_complete_event_dict.clear()
        self.__trial_release_event_dict.clear()
        self.__trial_release_payload_by_stage_trial_key.clear()
        self.__received_calibration_trial_count_team_id_set.clear()
        self.__calibration_trial_count_ready_event.clear()
        await self.__load_runtime_routing()

        # 绑定虚拟接收器的自定义控制消息。
        self.__logger.info(
            "绑定 VirtualReceiver custom control topic: topic=%s collector_component_id=%s group_id=%s",
            self.__virtual_receiver_command_control_topic,
            self.__collector_component_id,
            self.__group_id,
        )
        await self._component_framework.bind_message(
            MessageBindingModel(
                message_key=VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value,
                topic=self.__virtual_receiver_command_control_topic
            )
        )
        await self._component_framework.bind_message(
            MessageBindingModel(
                message_key=self.__HIDDEN_SCORE_SOURCE_LABEL,
                topic=self.__HIDDEN_SCORE_TOPIC,
            )
        )
        if self.__runtime_stage_event_topic is not None:
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=self.__RUNTIME_STAGE_EVENT_MESSAGE_KEY,
                    topic=self.__runtime_stage_event_topic,
                )
            )
        if self.__runtime_stage_control_topic is not None:
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=self.__RUNTIME_STAGE_CONTROL_MESSAGE_KEY,
                    topic=self.__runtime_stage_control_topic,
                )
            )
        for team_id, calibration_topic in self.__calibration_private_topic_by_team.items():
            message_key = self.__team_calibration_message_key_by_team[team_id]
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=message_key,
                    topic=calibration_topic,
                )
            )

        # 订阅管理message_key
        class ReceiveVirtualReceiverCustomControlMessageOperator(ReceiveMessageOperatorInterface):
            def __init__(self, virtual_receiver: VirtualReceiverImplement):
                self.__virtual_receiver: VirtualReceiverImplement = virtual_receiver

            async def receive_message(self, data: bytes) -> None:
                virtual_receiver_custom_control_model = VirtualReceiverCustomControlMessageConverter.protobuf_to_model(
                    VirtualReceiverCustomControlMessage_pb2.FromString(data)
                )
                await self.__virtual_receiver.custom_control(virtual_receiver_custom_control_model)

        await self._component_framework.subscribe_message(
            VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value,
            ReceiveVirtualReceiverCustomControlMessageOperator(virtual_receiver=self)
        )

        class ReceiveRuntimeStageControlMessageOperator(ReceiveMessageOperatorInterface):
            def __init__(self, virtual_receiver: VirtualReceiverImplement):
                self.__virtual_receiver = virtual_receiver

            async def receive_message(self, data: bytes) -> None:
                await self.__virtual_receiver._receive_runtime_stage_control_message(data)

        if self.__runtime_stage_control_topic is not None:
            await self._component_framework.subscribe_message(
                self.__RUNTIME_STAGE_CONTROL_MESSAGE_KEY,
                ReceiveRuntimeStageControlMessageOperator(virtual_receiver=self),
            )

        # 启动后台读取任务。
        # 注意：任务启动后会先等待 start_data_sending() 的 event。
        self.__read_data_task = asyncio.create_task(self.__read_data())
        # await self._receiver_transponder.send_data(ControlPackageModel())


    async def start_data_sending(self) -> None:
        # 真正打开“开始发数据”的开关。
        self.__send_flag_event.set()

    async def stop_data_sending(self) -> None:
        self.__send_flag_event.clear()

    async def send_device_info(self) -> None:
        # 设备信息除了通道配置，还会附带当前阶段信息，
        # 这样算法端能知道后续数据属于哪个 exp/session/stream_role。
        runtime_data_file_model = self.__resolve_runtime_data_file_model()
        device_transfer_model = copy.deepcopy(self.__device_transfer_model)
        if runtime_data_file_model is not None:
            device_transfer_model = self.__create_runtime_device_transfer_model(
                runtime_data_file_model=runtime_data_file_model,
                stream_role=self.__current_runtime_stream_role,
            )
        self.__logger.info(
            "发送共享 device info: subject_id=%s exp_name=%s exp_task=%s session_id=%s stream_role=%s",
            runtime_data_file_model.subject_id if runtime_data_file_model is not None else None,
            runtime_data_file_model.exp_name if runtime_data_file_model is not None else None,
            runtime_data_file_model.exp_task if runtime_data_file_model is not None else None,
            runtime_data_file_model.session_id if runtime_data_file_model is not None else None,
            self.__current_runtime_stream_role,
        )
        await self._receiver_transponder.send_data(ReceiverTransferModel(package=device_transfer_model))
        if runtime_data_file_model is not None:
            self.__last_sent_device_info_signature = (
                runtime_data_file_model.exp_name,
                runtime_data_file_model.exp_task,
                runtime_data_file_model.session_id,
                self.__current_runtime_stream_role,
            )

    async def send_impedance(self) -> None:
        pass

    async def shutdown(self) -> None:
        # 取消订阅管理message_key
        await self._component_framework.unsubscribe_message(
            VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value)
        if self.__runtime_stage_control_topic is not None:
            await self._component_framework.unsubscribe_message(
                self.__RUNTIME_STAGE_CONTROL_MESSAGE_KEY
            )
        # 防止后台读取任务卡在 wait() 上，先主动唤醒它。
        self.__send_flag_event.set()
        self.__shutdown_flag = True
        await self.__read_data_task
        if self.__trial_post_send_task_set:
            await asyncio.gather(*list(self.__trial_post_send_task_set), return_exceptions=True)

    async def custom_control(self, virtual_receiver_custom_control_model: VirtualReceiverCustomControlModel):
        # 当前自定义控制支持两种：
        # 1. 根据 subject_id + block_id 定位应从哪个数据文件开始播放；
        # 2. 在正式流程启动前覆盖每类校准trial数量。
        package = virtual_receiver_custom_control_model.package
        if isinstance(package, CalibrationTrialCountControlPackageModel):
            requested_trial_count = int(package.calibration_trial_count_per_class)
            if requested_trial_count < 0 or requested_trial_count > self.__FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS:
                raise ValueError(
                    f"算法申请的校准trial数量必须在 0~{self.__FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS} 之间，"
                    f"当前为 {requested_trial_count}"
                )
            team_id = str(package.team_id or "").strip()
            if not team_id:
                raise ValueError("校准trial申请必须显式携带 team_id")
            self.__calibration_trials_per_class_by_team[team_id] = requested_trial_count
            if self.__team_id_list and team_id not in self.__team_id_list:
                self.__logger.warning(
                    "收到未在当前group配置中的校准trial申请，已记录但不会计入ready集合: team_id=%s configured_team_id_list=%s",
                    team_id,
                    self.__team_id_list,
                )
            else:
                self.__received_calibration_trial_count_team_id_set.add(team_id)
                if all(
                    configured_team_id in self.__received_calibration_trial_count_team_id_set
                    for configured_team_id in self.__team_id_list
                ):
                    self.__calibration_trial_count_ready_event.set()
            self.__logger.info(
                "已更新每类校准trial数量: team_id=%s calibration_trials_per_class=%s received_team_id_list=%s pending_team_id_list=%s",
                team_id,
                requested_trial_count,
                sorted(self.__received_calibration_trial_count_team_id_set),
                [
                    configured_team_id
                    for configured_team_id in self.__team_id_list
                    if configured_team_id not in self.__received_calibration_trial_count_team_id_set
                ],
            )
            return

        information_package_model = package
        subject_id = information_package_model.subject_id
        block_id = int(information_package_model.block_id)
        block_index = 0
        visited_task_key_set: set[tuple[str, str, str, str]] = set()
        for index, data_file_model in enumerate(self.__data_files_model_list):
            task_key = (
                data_file_model.subject_id,
                data_file_model.exp_name,
                data_file_model.exp_task,
                data_file_model.session_id,
            )
            if task_key in visited_task_key_set:
                continue
            visited_task_key_set.add(task_key)
            if data_file_model.subject_id != subject_id:
                continue
            block_index += 1
            if block_index == block_id:
                self.__current_data_file_model_index = index
                return

    ###############
    # debug入口使用：允许在单进程调试时覆盖Collector工作目录，避免依赖独立进程的cwd
    def set_workspace_path_override(self, workspace_path: str) -> None:
        self.__workspace_path_override = workspace_path

    def set_config_path_override(self, config_path: str) -> None:
        self.__config_path_override = config_path

    def set_debug_data_file_filter(self, exp_name: str = None, subject_id: str = None) -> None:
        self.__debug_exp_name_filter = exp_name
        self.__debug_subject_id_filter = subject_id

    def set_debug_calibrate_trials_per_class(self, calibrate_trials_per_class: int = 0) -> None:
        if calibrate_trials_per_class is None:
            self.__calibrate_trials_per_class = self.__DEFAULT_CALIBRATE_TRIALS_PER_CLASS
            return
        resolved_trial_count = int(calibrate_trials_per_class)
        if resolved_trial_count < 0 or resolved_trial_count > self.__FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS:
            raise ValueError(
                f"校准trial数量必须在 0~{self.__FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS} 之间，"
                f"当前为 {resolved_trial_count}"
            )
        self.__calibrate_trials_per_class = resolved_trial_count
    ###############

    def __get_runtime_data_file_model_list(self) -> list[DataFileModel]:
        if self.__debug_exp_name_filter is not None or self.__debug_subject_id_filter is not None:
            return [
                data_file_model
                for data_file_model in self.__data_files_model_list
                if (self.__debug_exp_name_filter is None or data_file_model.exp_name == self.__debug_exp_name_filter)
                and (self.__debug_subject_id_filter is None or data_file_model.subject_id == self.__debug_subject_id_filter)
            ]
        return self.__data_files_model_list[self.__current_data_file_model_index:]

    def __resolve_runtime_exp_name(self) -> Union[str, None]:
        runtime_data_file_model = self.__resolve_runtime_data_file_model()
        if runtime_data_file_model is None:
            return None
        return runtime_data_file_model.exp_name

    def __resolve_runtime_data_file_model(self) -> Union[DataFileModel, None]:
        if self.__current_runtime_data_file_model is not None:
            return self.__current_runtime_data_file_model
        data_file_model_list = self.__get_runtime_data_file_model_list()
        if len(data_file_model_list) == 0:
            return None
        return data_file_model_list[0]

    async def __ensure_runtime_device_info_sent(
        self,
        data_file_model: DataFileModel,
        stream_role: str,
        force_resend: bool = False,
    ) -> None:
        # 这里的 force_resend 是 online/calibration 阶段切换的关键保护位。
        # 正式链路里，Collector 可能在真正开始发送前就先发过一次 shared online device info；
        # 但算法在校准期间又会收到 calibration device info，并把当前上下文切到 calibration。
        # 如果 online 放行后仍然只按 signature 去重，就会错过这次“切回 online”的显式声明，
        # 随后算法会把 online 浮点 EEG 误当成校准 bytes 包处理。
        self.__current_runtime_stream_role = stream_role
        runtime_signature = (
            data_file_model.exp_name,
            data_file_model.exp_task,
            data_file_model.session_id,
            stream_role,
        )
        self.__logger.info(
            "检查是否需要发送共享 device info: runtime_signature=%s last_signature=%s force_resend=%s",
            runtime_signature,
            self.__last_sent_device_info_signature,
            force_resend,
        )
        if force_resend or runtime_signature != self.__last_sent_device_info_signature:
            await self.send_device_info()

    async def __load_runtime_routing(self) -> None:
        # 所有路由信息均从 component_info 显式读取。
        # 这样后续即使接入 UI / 外部控制，也只需要修改配置生成器和 component_info，
        # 不需要再改 Collector 内部的推导逻辑。
        component_model = await self._component_framework.get_component_model()
        component_info = component_model.component_info or {}
        self.__group_id = component_info.get('group_id')
        self.__collector_component_id = component_info.get('collector_component_id') or component_model.component_id
        if not self.__virtual_receiver_command_control_topic and self.__collector_component_id:
            self.__virtual_receiver_command_control_topic = (
                f"{self.__collector_component_id}."
                f"{VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value}"
            )
        self.__team_id_list = list(component_info.get('team_id_list') or [])
        self.__calibration_trial_count_wait_timeout_seconds = float(
            component_info.get(
                'calibration_trial_count_wait_timeout_seconds',
                self.__DEFAULT_CALIBRATION_TRIAL_COUNT_WAIT_TIMEOUT_SECONDS,
            ) or self.__DEFAULT_CALIBRATION_TRIAL_COUNT_WAIT_TIMEOUT_SECONDS
        )
        self.__calibration_private_topic_by_team = dict(component_info.get('calibration_private_topic_by_team') or {})
        self.__runtime_stage_event_topic = component_info.get('runtime_stage_event_topic')
        self.__runtime_stage_control_topic = component_info.get('runtime_stage_control_topic')
        self.__team_calibration_message_key_by_team = {
            team_id: f"calibration_private_{team_id}"
            for team_id in self.__team_id_list
            if team_id in self.__calibration_private_topic_by_team
        }
        for team_id in self.__team_id_list:
            self.__calibration_trials_per_class_by_team.setdefault(
                team_id,
                self.__DEFAULT_CALIBRATE_TRIALS_PER_CLASS,
            )
        if len(self.__team_id_list) == 0:
            self.__calibration_trial_count_ready_event.set()
        self.__logger.info(
            "加载 Collector 运行期路由完成: group_id=%s collector_component_id=%s team_id_list=%s custom_control_topic=%s calibration_private_topic_by_team=%s calibration_trial_count_wait_timeout_seconds=%s",
            self.__group_id,
            self.__collector_component_id,
            self.__team_id_list,
            self.__virtual_receiver_command_control_topic,
            self.__calibration_private_topic_by_team,
            self.__calibration_trial_count_wait_timeout_seconds,
        )

    async def __send_calibration_payload_to_team(
        self,
        team_id: str,
        runtime_data_file_model: DataFileModel,
        calibration_chunk_list: list[bytes],
        calibration_delivery_id: str,
    ) -> None:
        calibration_message_key = self.__team_calibration_message_key_by_team.get(team_id)
        if calibration_message_key is None:
            raise ValueError(f"未配置赛队 {team_id} 的私有校准topic")
        self.__logger.info(
            "发送私有 calibration device info: team_id=%s exp_name=%s exp_task=%s session_id=%s delivery_id=%s",
            team_id,
            runtime_data_file_model.exp_name,
            runtime_data_file_model.exp_task,
            runtime_data_file_model.session_id,
            calibration_delivery_id,
        )
        calibration_device_transfer_model = self.__create_runtime_device_transfer_model(
            runtime_data_file_model=runtime_data_file_model,
            stream_role='calibration',
        )
        calibration_device_transfer_model.other_information['calibration_delivery_id'] = (
            calibration_delivery_id
        )
        await self.__send_direct_receiver_transfer_model(
            calibration_message_key,
            ReceiverTransferModel(
                package=calibration_device_transfer_model
            ),
        )
        for chunk_index, calibration_chunk in enumerate(calibration_chunk_list, start=1):
            await self.__send_direct_receiver_transfer_model(
                calibration_message_key,
                ReceiverTransferModel(
                    package=DataTransferModel(
                        data_position=self.__current_date_position,
                        data=calibration_chunk,
                    ),
                ),
            )
            if (
                len(calibration_chunk_list) > 1
                and chunk_index % self.__CALIBRATION_CHUNK_SEND_YIELD_INTERVAL == 0
            ):
                await asyncio.sleep(self.__CALIBRATION_CHUNK_SEND_YIELD_SECONDS)

    async def __send_calibration_payload_to_team_with_retry(
        self,
        team_id: str,
        runtime_data_file_model: DataFileModel,
        calibration_chunk_list: list[bytes],
        calibration_delivery_id: str,
    ) -> None:
        retryable_exception_types = (
            asyncio.TimeoutError,
            asyncio.InvalidStateError,
            ConnectionError,
            OSError,
            grpc.aio.AioRpcError,
        )
        for attempt in range(1, self.__CALIBRATION_PAYLOAD_SEND_RETRY_LIMIT + 1):
            try:
                await self.__send_calibration_payload_to_team(
                    team_id=team_id,
                    runtime_data_file_model=runtime_data_file_model,
                    calibration_chunk_list=calibration_chunk_list,
                    calibration_delivery_id=calibration_delivery_id,
                )
                if attempt > 1:
                    self.__logger.info(
                        "私有校准包完整重发成功: team_id=%s stage=%s/%s/%s delivery_id=%s attempt=%s/%s",
                        team_id,
                        runtime_data_file_model.exp_name,
                        runtime_data_file_model.exp_task,
                        runtime_data_file_model.session_id,
                        calibration_delivery_id,
                        attempt,
                        self.__CALIBRATION_PAYLOAD_SEND_RETRY_LIMIT,
                    )
                return
            except retryable_exception_types as exc:
                self.__logger.warning(
                    "私有校准包发送失败: team_id=%s stage=%s/%s/%s attempt=%s/%s error_type=%s error=%s",
                    team_id,
                    runtime_data_file_model.exp_name,
                    runtime_data_file_model.exp_task,
                    runtime_data_file_model.session_id,
                    attempt,
                    self.__CALIBRATION_PAYLOAD_SEND_RETRY_LIMIT,
                    type(exc).__name__,
                    exc,
                )
                if attempt >= self.__CALIBRATION_PAYLOAD_SEND_RETRY_LIMIT:
                    raise
                await asyncio.sleep(self.__CALIBRATION_PAYLOAD_SEND_RETRY_DELAY_SECONDS * attempt)

    async def __send_direct_receiver_transfer_model(
        self,
        message_key: str,
        receiver_transfer_model: ReceiverTransferModel,
    ) -> None:
        await self._component_framework.send_message(
            message_key,
            CommonMessageConverter.model_to_protobuf(
                ReceiverTransferModelToDataMessageModelConverter.convert(receiver_transfer_model)
            ).SerializeToString(),
        )

    @staticmethod
    def __build_stage_context(runtime_task_group_model: RuntimeTaskGroupModel) -> dict:
        return {
            'subject_id': runtime_task_group_model.subject_id,
            'exp_name': runtime_task_group_model.exp_name,
            'exp_task': runtime_task_group_model.exp_task,
            'session_id': runtime_task_group_model.session_id,
        }

    @classmethod
    def __build_stage_key(cls, runtime_task_group_model: RuntimeTaskGroupModel) -> str:
        stage_context = cls.__build_stage_context(runtime_task_group_model)
        return (
            f"{stage_context['subject_id']}|{stage_context['exp_name']}|"
            f"{stage_context['exp_task']}|{stage_context['session_id']}"
        )

    @staticmethod
    def __build_stage_key_from_context(stage_context: dict) -> str:
        return (
            f"{stage_context.get('subject_id')}|{stage_context.get('exp_name')}|"
            f"{stage_context.get('exp_task')}|{stage_context.get('session_id')}"
        )

    def __resolve_requested_calibration_trial_count(self, team_id: Union[str, None]) -> int:
        if team_id is None:
            return self.__DEFAULT_CALIBRATE_TRIALS_PER_CLASS
        return self.__calibration_trials_per_class_by_team.get(
            team_id,
            self.__DEFAULT_CALIBRATE_TRIALS_PER_CLASS,
        )

    async def __wait_until_online_stage_allowed(self, stage_key: str) -> None:
        # stage gate:
        # Collector 发送完 calibration 私有数据后，会阻塞在这里，
        # 直到 RuntimeStageCoordinator 确认“同组全部赛队都已完成校准”。
        stage_release_event = self.__online_stage_release_event_dict.setdefault(stage_key, asyncio.Event())
        await stage_release_event.wait()

    async def __wait_until_online_stage_completed(self, stage_key: str) -> None:
        # stage completion gate:
        # 当前 stage 的最后一个 online trial 发完后，仍需等待协调器确认
        # “同组全部队伍都已经进入该 stage 最后一个 trial 的终态”，
        # 才能安全进入下一 stage 的 calibration。
        stage_complete_event = self.__online_stage_complete_event_dict.setdefault(stage_key, asyncio.Event())
        await stage_complete_event.wait()

    async def __wait_until_calibration_trial_count_ready(self) -> None:
        if len(self.__team_id_list) == 0:
            return
        pending_team_id_list = [
            team_id
            for team_id in self.__team_id_list
            if team_id not in self.__received_calibration_trial_count_team_id_set
        ]
        if len(pending_team_id_list) == 0:
            return
        self.__logger.info(
            "等待全部赛队校准trial申请值到齐后再开始切分数据: configured_team_id_list=%s pending_team_id_list=%s timeout_seconds=%s",
            self.__team_id_list,
            pending_team_id_list,
            self.__calibration_trial_count_wait_timeout_seconds,
        )
        if self.__calibration_trial_count_wait_timeout_seconds <= 0:
            await self.__calibration_trial_count_ready_event.wait()
        else:
            try:
                await asyncio.wait_for(
                    self.__calibration_trial_count_ready_event.wait(),
                    timeout=self.__calibration_trial_count_wait_timeout_seconds,
                )
            except asyncio.TimeoutError:
                self.__logger.warning(
                    "等待校准trial申请值超时，按已收到申请值和默认值继续切分数据: configured_team_id_list=%s received_team_id_list=%s pending_team_id_list=%s timeout_seconds=%s default_trials_per_class=%s",
                    self.__team_id_list,
                    sorted(self.__received_calibration_trial_count_team_id_set),
                    [
                        team_id
                        for team_id in self.__team_id_list
                        if team_id not in self.__received_calibration_trial_count_team_id_set
                    ],
                    self.__calibration_trial_count_wait_timeout_seconds,
                    self.__DEFAULT_CALIBRATE_TRIALS_PER_CLASS,
                )
                return
        self.__logger.info(
            "全部赛队校准trial申请值已到齐，开始切分数据: configured_team_id_list=%s received_team_id_list=%s",
            self.__team_id_list,
            sorted(self.__received_calibration_trial_count_team_id_set),
        )

    async def __wait_until_trial_released(self, stage_key: str, trial_id: int) -> None:
        # trial gate:
        # 同一 group 内必须在“上一个 trial 的所有队伍都进入终态”后，
        # 才允许 Collector 发送下一个 online trial。
        trial_release_event = self.__trial_release_event_dict.setdefault((stage_key, int(trial_id)), asyncio.Event())
        while True:
            try:
                await asyncio.wait_for(
                    trial_release_event.wait(),
                    timeout=self.__TRIAL_RELEASE_WAIT_WARNING_INTERVAL_SECONDS,
                )
                return
            except asyncio.TimeoutError:
                self.__logger.warning(
                    "等待 online trial 放行超时，继续等待: group_id=%s stage_key=%s trial_id=%s "
                    "wait_interval_seconds=%s release_payload_known=%s current_data_position=%s",
                    self.__group_id,
                    stage_key,
                    int(trial_id),
                    self.__TRIAL_RELEASE_WAIT_WARNING_INTERVAL_SECONDS,
                    (stage_key, int(trial_id)) in self.__trial_release_payload_by_stage_trial_key,
                    self.__current_date_position,
                )

    async def __send_runtime_stage_event(self, payload: dict) -> None:
        if self.__runtime_stage_event_topic is None:
            self.__logger.warning("未配置 runtime stage event topic，跳过发送: %s", payload)
            return
        runtime_stage_event_payload = dict(payload)
        runtime_stage_event_payload.setdefault('event_id', str(uuid.uuid4()))
        self.__logger.debug(
            "发送 runtime stage event: event_id=%s event_type=%s group_id=%s stage_context=%s send_mode=single_shot",
            runtime_stage_event_payload.get('event_id'),
            runtime_stage_event_payload.get('event_type'),
            runtime_stage_event_payload.get('group_id'),
            runtime_stage_event_payload.get('stage_context'),
        )
        await self._component_framework.send_message(
            self.__RUNTIME_STAGE_EVENT_MESSAGE_KEY,
            CommonMessageConverter.model_to_protobuf(
                DataMessageModel(
                    package=DataPackageModel(
                        data_position=0.0,
                        data=json.dumps(runtime_stage_event_payload, ensure_ascii=False),
                    )
                )
            ).SerializeToString(),
        )
        self.__logger.debug(
            "runtime stage event 发送成功: event_id=%s event_type=%s group_id=%s send_mode=single_shot",
            runtime_stage_event_payload.get('event_id'),
            runtime_stage_event_payload.get('event_type'),
            runtime_stage_event_payload.get('group_id'),
        )

    def __schedule_trial_post_send_task(self, coro, *, task_name: str) -> None:
        if self.__shutdown_flag:
            return
        task = asyncio.create_task(coro, name=task_name)
        self.__trial_post_send_task_set.add(task)

        def _cleanup(completed_task: asyncio.Task) -> None:
            self.__trial_post_send_task_set.discard(completed_task)
            try:
                completed_task.result()
            except asyncio.CancelledError:
                return
            except Exception:
                self.__logger.exception("trial post-send 后台任务执行失败: task_name=%s", task_name)

        task.add_done_callback(_cleanup)

    async def _receive_runtime_stage_control_message(self, data: bytes) -> None:
        # RuntimeStageCoordinator 发给 Collector 的所有控制都走这里。
        # 当前只实现三种控制：
        # 1. allow_online_stage: 当前 session 可以从 calibration 切到 online；
        # 2. release_trial: 当前 session 的指定 trial 可以开始发送；
        # 3. complete_online_stage: 当前 session 的最后一个 online trial 已被全组处理完成。
        payload = None
        try:
            data_message_model = CommonMessageConverter.protobuf_to_model(
                DataMessage_pb2.FromString(data)
            )
            package = data_message_model.package
            if not isinstance(package, DataPackageModel):
                return
            payload = self.__parse_json_payload(package.data)
            if not isinstance(payload, dict):
                return
            if payload.get('group_id') != self.__group_id:
                return
            collector_component_id = payload.get('collector_component_id')
            if collector_component_id not in (None, '', self.__collector_component_id):
                return
            stage_context = payload.get('stage_context') or {}
            stage_key = self.__build_stage_key_from_context(stage_context)
            control_type = payload.get('control_type')
            self.__logger.debug(
                "收到 runtime stage control: control_id=%s control_type=%s group_id=%s stage_key=%s payload=%s",
                payload.get('control_id'),
                control_type,
                self.__group_id,
                stage_key,
                payload,
            )
            if control_type == 'allow_online_stage':
                self.__online_stage_release_event_dict.setdefault(stage_key, asyncio.Event()).set()
                self.__logger.info("已放开 online stage: stage_key=%s", stage_key)
            elif control_type == 'complete_online_stage':
                self.__online_stage_complete_event_dict.setdefault(stage_key, asyncio.Event()).set()
                self.__logger.info(
                    "当前 online stage 已完成，可切到下一 stage: stage_key=%s final_trial_id=%s",
                    stage_key,
                    payload.get('final_trial_id'),
                )
            elif control_type == 'release_trial':
                trial_id = int(payload.get('trial_id'))
                self.__trial_release_payload_by_stage_trial_key[(stage_key, trial_id)] = dict(payload)
                self.__trial_release_event_dict.setdefault((stage_key, trial_id), asyncio.Event()).set()
                self.__logger.debug("已放行 online trial: stage_key=%s trial_id=%s", stage_key, trial_id)
            else:
                self.__logger.warning("收到未知 runtime stage control: %s", payload)
        except Exception:
            self.__logger.exception("处理 runtime stage control 失败: payload=%s", payload)

    @staticmethod
    def __parse_json_payload(raw_data):
        if isinstance(raw_data, (bytes, bytearray)):
            try:
                raw_data = raw_data.decode('utf-8')
            except UnicodeDecodeError:
                return None
        if not isinstance(raw_data, str):
            return None
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return None

    def __create_runtime_device_transfer_model(
        self,
        runtime_data_file_model: DataFileModel,
        stream_role: str,
    ) -> DeviceTransferModel:
        device_transfer_model = copy.deepcopy(self.__device_transfer_model)
        if device_transfer_model.other_information is None:
            device_transfer_model.other_information = {}
        device_transfer_model.other_information['subject_id'] = runtime_data_file_model.subject_id
        device_transfer_model.other_information['exp_name'] = runtime_data_file_model.exp_name
        device_transfer_model.other_information['exp_task'] = runtime_data_file_model.exp_task
        device_transfer_model.other_information['session_id'] = runtime_data_file_model.session_id
        device_transfer_model.other_information['stream_role'] = stream_role
        if stream_role == 'online':
            device_transfer_model.other_information['online_replay_mode'] = self.__online_replay_mode
        return device_transfer_model

    async def __read_data(self) -> None:
        # 等待中控发来 start_data_sending 指令。
        await self.__send_flag_event.wait()
        await self.__wait_until_calibration_trial_count_ready()
        workspace_path = self.__workspace_path_override or os.getcwd()

        # RuntimeTaskGroup 可以理解成：
        # “某个被试在某个 exp_name/exp_task/session 下的一整段流程”。
        runtime_task_group_model_list = self.__get_runtime_task_group_model_list()
        for runtime_task_group_model in runtime_task_group_model_list:
            if self.__shutdown_flag:
                return
            try:
                team_file_task_data_model_list_dict = await self.__load_team_file_task_data_model_list_dict(
                    runtime_task_group_model=runtime_task_group_model,
                    workspace_path=workspace_path,
                )
                _, online_trial_model_list = await self.__load_file_task_data_model_list(
                    runtime_task_group_model=runtime_task_group_model,
                    workspace_path=workspace_path,
                    requested_trial_count=0,
                    log_context='shared_online_reference',
                )
                if len(team_file_task_data_model_list_dict) == 0 and len(online_trial_model_list) == 0:
                    self.__logger.warning(
                        "任务 %s/%s/%s 没有可发送的trial，已跳过",
                        runtime_task_group_model.exp_name,
                        runtime_task_group_model.exp_task,
                        runtime_task_group_model.session_id,
                    )
                    continue
                # 对每个 task group，内部会先发送 calibration，再发送 online。
                await self.__send_runtime_task_group(
                    runtime_task_group_model=runtime_task_group_model,
                    team_file_task_data_model_list_dict=team_file_task_data_model_list_dict,
                    online_trial_model_list=online_trial_model_list,
                )
            except Exception as exc:
                self.__logger.exception("read data error: %s", exc)
                self.__publish_current_trial_error_state(runtime_task_group_model, exc)
                self.__shutdown_flag = True
                return
        # 所有任务都结束后，发送 end_flag，通知算法端数据源结束。
        self.__publish_current_trial_finished_state()
        await self._receiver_transponder.send_data(ControlTransferModel(end_flag=True))

    def __get_runtime_task_group_model_list(self) -> list[RuntimeTaskGroupModel]:
        # 这一步是在做“运行期任务分组”。
        # 目标不是简单返回文件列表，而是把多个文件整理成算法真正感知到的阶段：
        # 1. subject_id     决定是哪个被试；
        # 2. exp_name       决定是哪种范式；
        # 3. exp_task       决定当前二分类任务；
        # 4. session_id     决定本轮校准+在线推理阶段。
        # 后面算法端的 calibrate()/run() 都是按这个四元组切换阶段的。
        runtime_data_file_model_list = self.__get_runtime_data_file_model_list()
        runtime_task_group_model_dict: dict[tuple[str, str, str, str], RuntimeTaskGroupModel] = {}
        runtime_task_group_model_list: list[RuntimeTaskGroupModel] = []
        for data_file_model in runtime_data_file_model_list:
            # session 不再跨文件聚合到同一个阶段，而是作为独立阶段显式发送给算法端。
            task_key = (
                data_file_model.subject_id,
                data_file_model.exp_name,
                data_file_model.exp_task,
                data_file_model.session_id,
            )
            if task_key not in runtime_task_group_model_dict:
                runtime_task_group_model_dict[task_key] = RuntimeTaskGroupModel(
                    subject_id=data_file_model.subject_id,
                    exp_name=data_file_model.exp_name,
                    exp_task=data_file_model.exp_task,
                    session_id=data_file_model.session_id,
                    data_file_model_list=[],
                )
                runtime_task_group_model_list.append(runtime_task_group_model_dict[task_key])
            runtime_task_group_model_dict[task_key].data_file_model_list.append(data_file_model)
        return runtime_task_group_model_list

    async def __load_file_task_data_model_list(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        workspace_path: str,
        requested_trial_count: int,
        log_context: str = 'team_calibration',
    ) -> tuple[list[FileTaskDataModel], list[ParsedTrialModel]]:
        # 这个函数做两件事：
        # 1. 把当前 session 关联的每个 data_file 读取并切成 trial；
        # 2. 再从这些 trial 里划分出“校准部分”和“在线部分”。
        #
        # 返回值为什么是两个？
        # - file_task_data_model_list: 保留“每个文件里被抽为 calibration 的 trial”
        #   这样调试时还能追踪校准样本来自哪个原始文件。
        # - online_trial_model_list: 把本 session 剩余可用于在线推理的 trial 汇总后统一打乱。
        # 改为 session 级独立阶段后，这里只统计当前 session 内的校准trial。
        # 这里把“保留进入固定校准池的数量”和“实际下发给选手的数量”拆开统计。
        # 这样就能保证：
        # 1. 每个 session 每类别前 10 个 trial 永远从 online 测试集中排除；
        # 2. 真正发送多少校准 trial，只受选手申请值影响；
        # 3. 因此不同选手的 online 测试集保持一致。
        calibration_pool_counter_dict = {
            trigger_value: 0
            for trigger_value in self.__EXP_TASK_TRIGGER_VALUE_DICT[runtime_task_group_model.exp_task]
        }
        sent_calibration_counter_dict = {
            trigger_value: 0
            for trigger_value in self.__EXP_TASK_TRIGGER_VALUE_DICT[runtime_task_group_model.exp_task]
        }
        file_task_data_model_list: list[FileTaskDataModel] = []
        online_candidate_trial_model_list: list[ParsedTrialModel] = []
        for data_file_model in runtime_task_group_model.data_file_model_list:
            parsed_trial_model_list = await self.__load_parsed_trial_model_list(
                data_file_model=data_file_model,
                workspace_path=workspace_path,
            )
            calibration_trial_model_list, online_trial_model_list = self.__split_calibration_and_online_trial_model_list(
                parsed_trial_model_list=parsed_trial_model_list,
                data_file_model=data_file_model,
                calibration_pool_counter_dict=calibration_pool_counter_dict,
                sent_calibration_counter_dict=sent_calibration_counter_dict,
                requested_trial_count=requested_trial_count,
            )
            if calibration_trial_model_list or online_trial_model_list:
                file_task_data_model_list.append(
                    FileTaskDataModel(
                        data_file_model=data_file_model,
                        calibration_trial_model_list=calibration_trial_model_list,
                    )
                )
            online_candidate_trial_model_list.extend(online_trial_model_list)

        online_trial_model_list = self.__shuffle_session_trial_model_list(
            trial_model_list=online_candidate_trial_model_list,
            runtime_task_group_model=runtime_task_group_model,
        )
        if log_context == 'shared_online_reference':
            self.__logger.info(
                "任务 %s/%s/%s 共享online准备完成: 固定校准池计数=%s online_trial_count=%s",
                runtime_task_group_model.exp_name,
                runtime_task_group_model.exp_task,
                runtime_task_group_model.session_id,
                calibration_pool_counter_dict,
                len(online_trial_model_list),
            )
        else:
            self.__logger.info(
                "任务 %s/%s/%s 校准数据准备完成: 固定校准池计数=%s 实际下发校准trial计数(每类)=%s online_trial_count=%s requested_trial_count_per_class=%s",
                runtime_task_group_model.exp_name,
                runtime_task_group_model.exp_task,
                runtime_task_group_model.session_id,
                calibration_pool_counter_dict,
                sent_calibration_counter_dict,
                len(online_trial_model_list),
                requested_trial_count,
            )
        # 到这里，当前 session 的材料已经准备完：
        # - 校准 trial: 按文件挂在 file_task_data_model_list 里；
        # - online trial: 合并后打乱，准备按实时流方式发送。
        return file_task_data_model_list, online_trial_model_list

    async def __load_team_file_task_data_model_list_dict(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        workspace_path: str,
    ) -> dict[str, list[FileTaskDataModel]]:
        # 针对每个赛队分别准备校准材料。
        # 注意这里不会复制 online 材料，online 仍然只保留一份 group 共享数据。
        if not self.__team_id_list:
            return {}
        team_file_task_data_model_list_dict: dict[str, list[FileTaskDataModel]] = {}
        for team_id in self.__team_id_list:
            requested_trial_count = self.__resolve_requested_calibration_trial_count(team_id)
            file_task_data_model_list, _ = await self.__load_file_task_data_model_list(
                runtime_task_group_model=runtime_task_group_model,
                workspace_path=workspace_path,
                requested_trial_count=requested_trial_count,
            )
            team_file_task_data_model_list_dict[team_id] = file_task_data_model_list
        return team_file_task_data_model_list_dict

    async def __load_parsed_trial_model_list(
        self,
        data_file_model: DataFileModel,
        workspace_path: str,
    ) -> list[ParsedTrialModel]:
        # 这个函数是“单个 .dat 文件 -> ParsedTrialModel 列表”的主入口。
        # 1. 定位文件；
        # 2. 读取 metadata；
        # 3. 判断文件是文本还是二进制；
        # 4. 解析通道映射；
        # 5. 读成二维 sample_matrix；
        # 6. 再根据 trigger 切成一个个 trial。
        data_file_path = os.path.join(workspace_path, data_file_model.file_path)
        if data_file_path in self.__parsed_trial_model_list_cache_dict:
            # 缓存命中时直接复用，避免同一个文件被不同 exp_task 重复解析。
            return self.__parsed_trial_model_list_cache_dict[data_file_path]

        if not os.path.exists(data_file_path):
            try:
                raise FileNotFoundError(f"{data_file_path} not found")
            except FileNotFoundError as e:
                raise VirtualReceiverFileNotFoundException(f"{data_file_path} not found") from e

        self.__logger.info(f"开始读取{data_file_path}数据")
        metadata_dict = self.__load_metadata(data_file_path)
        data_file_format = self.__detect_data_file_format(data_file_path, metadata_dict)
        file_total_channel_number = self.__resolve_file_total_channel_number(metadata_dict)
        eeg_channel_index_list, trigger_channel_index = self.__resolve_data_channel_mapping(
            metadata_dict=metadata_dict,
            file_total_channel_number=file_total_channel_number,
            data_file_path=data_file_path,
        )
        sample_matrix = await self.__load_sample_matrix(
            data_file_path=data_file_path,
            data_file_format=data_file_format,
            total_channel_number=file_total_channel_number,
        )
        parsed_trial_model_list = self.__extract_trial_model_list(
            sample_matrix=sample_matrix,
            eeg_channel_index_list=eeg_channel_index_list,
            trigger_channel_index=trigger_channel_index,
            session_id=data_file_model.session_id,
            file_path=data_file_model.file_path,
        )
        # 这里缓存的是“已经按 trigger 切好的 trial 列表”。
        # 后续 left_vs_rest / right_vs_rest 只是再按 trigger_value 做筛选，
        # 所以没有必要重复读盘。
        self.__parsed_trial_model_list_cache_dict[data_file_path] = parsed_trial_model_list
        self.__logger.info(f"{data_file_path}数据读取完毕")
        return parsed_trial_model_list

    def __split_calibration_and_online_trial_model_list(
        self,
        parsed_trial_model_list: list[ParsedTrialModel],
        data_file_model: DataFileModel,
        calibration_pool_counter_dict: dict[int, int],
        sent_calibration_counter_dict: dict[int, int],
        requested_trial_count: int,
    ) -> tuple[list[ParsedTrialModel], list[ParsedTrialModel]]:
        # 这里是“抽校准样本”的关键逻辑。
        # 修改说明：
        # 1. 先根据 exp_task 过滤掉不属于当前二分类任务的 trigger；
        # 2. 每类别先固定保留前 10 个 trial 进入“校准候选池”；
        # 3. online 仅使用候选池之后的 trial，这样不同选手拿到的测试集一致；
        # 4. 真正发给算法的 calibration trial，再从候选池里按申请数量截取。
        #
        # 注意：这里故意先抽 calibration，后面才 shuffle online，
        # 这样可以避免校准集被随机化策略污染。
        allowed_trigger_value_set = self.__EXP_TASK_TRIGGER_VALUE_DICT[data_file_model.exp_task]
        calibration_trial_model_list: list[ParsedTrialModel] = []
        online_candidate_trial_model_list: list[ParsedTrialModel] = []

        for trial_model in parsed_trial_model_list:
            if trial_model.trigger_value not in allowed_trigger_value_set:
                continue
            trigger_value = trial_model.trigger_value
            if calibration_pool_counter_dict[trigger_value] < self.__FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS:
                calibration_pool_counter_dict[trigger_value] += 1
                if sent_calibration_counter_dict[trigger_value] < requested_trial_count:
                    # 校准候选池中的前 requested N 个，才真正发送给选手算法。
                    calibration_trial_model_list.append(trial_model)
                    sent_calibration_counter_dict[trigger_value] += 1
                continue

            online_candidate_trial_model_list.append(trial_model)

        return calibration_trial_model_list, online_candidate_trial_model_list

    async def __send_runtime_task_group(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        team_file_task_data_model_list_dict: dict[str, list[FileTaskDataModel]],
        online_trial_model_list: list[ParsedTrialModel],
    ) -> None:
        # 这是整个虚拟接收器里最值得反复读的一个函数。
        # 它定义了“一个 session 到底是怎么发出去的”：
        # 1. 先切到 calibration 角色并发一包 npz；
        # 2. 再切回 online 角色；
        # 3. 发送 block 信息；
        # 4. 发送 block_start 事件；
        # 5. 逐 trial 发送在线 EEG；
        # 6. 最后发送 block_end 事件。
        #
        # 换句话说，这里决定了算法端看到的数据节奏。
        if len(team_file_task_data_model_list_dict) == 0 and len(online_trial_model_list) == 0:
            return

        runtime_data_file_model = (
            next(iter(team_file_task_data_model_list_dict.values()))[0].data_file_model
            if len(team_file_task_data_model_list_dict) > 0 and len(next(iter(team_file_task_data_model_list_dict.values()))) > 0
            else runtime_task_group_model.data_file_model_list[0]
        )
        self.__current_runtime_data_file_model = runtime_data_file_model

        stage_key = self.__build_stage_key(runtime_task_group_model)
        for team_id, file_task_data_model_list in team_file_task_data_model_list_dict.items():
            # calibration 数据按 team 单独序列化、分块并发送到私有 topic。
            # 这样可以保证：
            # 1. 每个队伍拿到的是自己申请数量的校准trial；
            # 2. 不同队伍即使申请数量不同，也不会互相污染；
            # 3. online 共享测试集的边界仍保持一致。
            calibration_data_dict = self.__build_calibration_data_dict(
                runtime_task_group_model=runtime_task_group_model,
                file_task_data_model_list=file_task_data_model_list,
            )
            calibration_payload = self.__serialize_calibration_session_data(
                runtime_task_group_model=runtime_task_group_model,
                calibration_data_dict=calibration_data_dict,
            )
            calibration_chunk_list = self.__split_calibration_payload_to_chunk_list(calibration_payload)
            calibration_delivery_id = uuid.uuid4().hex
            requested_trials_per_class = self.__resolve_requested_calibration_trial_count(team_id)
            actual_calibration_total_trial_count = int(calibration_data_dict['label'].shape[0])
            actual_calibration_trial_count_by_label = self.__summarize_binary_label_count(
                calibration_data_dict['label']
            )
            self.__logger.info(
                "发送校准数据 %s/%s/%s team_id=%s delivery_id=%s payload_bytes=%s chunk_count=%s max_chunk_payload_bytes=%s requested_trials_per_class=%s actual_total_trials=%s actual_trial_count_by_label=%s",
                runtime_task_group_model.exp_name,
                runtime_task_group_model.exp_task,
                runtime_task_group_model.session_id,
                team_id,
                calibration_delivery_id,
                len(calibration_payload),
                len(calibration_chunk_list),
                self.__MAX_CALIBRATION_CHUNK_BYTES,
                requested_trials_per_class,
                actual_calibration_total_trial_count,
                actual_calibration_trial_count_by_label,
            )
            await self.__send_calibration_payload_to_team_with_retry(
                team_id=team_id,
                runtime_data_file_model=runtime_data_file_model,
                calibration_chunk_list=calibration_chunk_list,
                calibration_delivery_id=calibration_delivery_id,
            )
        await self.__send_runtime_stage_event(
            {
                'event_type': 'collector_stage_prepared',
                'group_id': self.__group_id,
                'collector_component_id': self.__collector_component_id,
                'stage_context': self.__build_stage_context(runtime_task_group_model),
                'online_trial_count': len(online_trial_model_list),
            }
        )
        # 只有当协调器确认“全部赛队都完成当前 stage 校准”后，
        # Collector 才会真正继续下面的 online 发送。
        await self.__wait_until_online_stage_allowed(stage_key)

        # 校准阶段结束后切回 online，后续恢复为事件 + 连续EEG 的发送方式。
        # 这里必须强制重发一次 online device info。
        # 原因是 collector 在正式开始前可能已经发送过同一个 session 的 online device info，
        # 如果这里只按 signature 去重，就会误以为“online device info 已经发过”，
        # 从而跳过重发；但算法端在校准阶段已经收到过 calibration device info，
        # 此时它的当前 stream_role 实际仍然是 calibration。
        await self.__ensure_runtime_device_info_sent(
            runtime_data_file_model,
            'online',
            force_resend=True,
        )
        self.__subject_block_dict[runtime_task_group_model.subject_id] = (
            self.__subject_block_dict.get(runtime_task_group_model.subject_id, 0) + 1
        )
        block_id = str(self.__subject_block_dict[runtime_task_group_model.subject_id])
        await self._receiver_transponder.send_data(
            ReceiverTransferModel(
                package=InformationTransferModel(
                    subject_id=runtime_task_group_model.subject_id,
                    exp_name=runtime_task_group_model.exp_name,
                    block_id=block_id,
                )
            )
        )

        block_start_position = self.__current_date_position
        # block_start 是在线阶段的边界标记。
        # 算法本身通常只关心 trial_start/trial_end，但 challenge / 编排层仍可能使用 block 边界。
        await self._receiver_transponder.send_data(
            self.__create_event_receiver_transfer_model(
                event_position=block_start_position,
                event_data=self.__BLOCK_START_TRIGGER,
            )
        )

        for trial_index, trial_model in enumerate(online_trial_model_list, start=1):
            await asyncio.sleep(0)
            await self.__send_flag_event.wait()
            if self.__shutdown_flag:
                return
            # 每个 online trial 发送前都要经过 group 级 barrier。
            release_wait_start_wallclock = time.time()
            await self.__wait_until_trial_released(stage_key, trial_index)
            release_granted_wallclock = time.time()
            trial_release_payload = self.__trial_release_payload_by_stage_trial_key.get((stage_key, int(trial_index)), {})
            release_wallclock = self.__safe_positive_float(trial_release_payload.get('release_wallclock'))
            release_interval_seconds = self.__safe_positive_float(trial_release_payload.get('trial_release_interval_seconds')) or 1.3
            self.__logger.debug(
                "online trial 放行已生效，准备发送数据: group_id=%s stage_key=%s subject_id=%s exp_name=%s exp_task=%s "
                "session_id=%s block_id=%s trial_id=%s release_wait_ms=%.3f release_wallclock=%.6f current_data_position=%s",
                self.__group_id,
                stage_key,
                runtime_task_group_model.subject_id,
                runtime_task_group_model.exp_name,
                runtime_task_group_model.exp_task,
                runtime_task_group_model.session_id,
                block_id,
                trial_index,
                (release_granted_wallclock - release_wait_start_wallclock) * 1000.0,
                release_wallclock or -1.0,
                self.__current_date_position,
            )
            self.__publish_current_trial_state(
                runtime_task_group_model=runtime_task_group_model,
                block_id=block_id,
                trial_id=str(trial_index),
                trial_model=trial_model,
                release_wallclock=release_wallclock,
            )
            await self.__send_hidden_score_message(
                runtime_task_group_model=runtime_task_group_model,
                block_id=block_id,
                trial_id=str(trial_index),
                trial_model=trial_model,
            )
            await self.__send_trial_data(
                trial_model=trial_model,
                runtime_task_group_model=runtime_task_group_model,
                block_id=block_id,
                trial_id=str(trial_index),
                stage_key=stage_key,
                release_granted_wallclock=release_granted_wallclock,
                release_wallclock=release_wallclock,
                release_interval_seconds=release_interval_seconds,
            )

        if not self.__shutdown_flag and self.__current_date_position > block_start_position:
            await self._receiver_transponder.send_data(
                self.__create_event_receiver_transfer_model(
                    event_position=self.__current_date_position - 1,
                    event_data=self.__BLOCK_END_TRIGGER,
                )
            )
        if online_trial_model_list and not self.__shutdown_flag:
            await self.__wait_until_online_stage_completed(stage_key)

    def __build_calibration_data_dict(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        file_task_data_model_list: list[FileTaskDataModel],
    ) -> dict[str, np.ndarray]:
        # 这个函数把分散在多个文件里的 calibration trial
        # 重新整理成算法训练喜欢的标准张量格式：
        # data.shape  = [trial, channel, point]
        # label.shape = [trial]
        #
        # label 不是直接沿用原始 trigger，而是按 exp_task 映射成二分类标签：
        # - left_vs_rest  : trigger 1 -> 1, trigger 3 -> 0
        # - right_vs_rest : trigger 2 -> 1, trigger 3 -> 0
        calibration_data_dict = {
            'data': [],
            'label': [],
        }
        for file_task_data_model in file_task_data_model_list:
            for trial_model in file_task_data_model.calibration_trial_model_list:
                calibration_data_dict['data'].append(trial_model.eeg_data.astype(np.float32))
                calibration_data_dict['label'].append(
                    self.__map_exp_task_label(
                        exp_task=runtime_task_group_model.exp_task,
                        raw_trigger_value=trial_model.trigger_value,
                    )
                )

        channel_number = self.__device_transfer_model.channel_number
        if calibration_data_dict['data']:
            calibration_data_dict['data'] = np.stack(calibration_data_dict['data']).astype(np.float32)
            calibration_data_dict['label'] = np.asarray(calibration_data_dict['label'], dtype=np.int64)
        else:
            # 即使没有校准样本，也强制返回形状合法的空数组。
            # 这样算法端就可以统一写成 “先读 data/label，再判断 trial 数”。
            calibration_data_dict['data'] = np.empty(
                (0, channel_number, self.__TRIAL_DURATION_POINTS),
                dtype=np.float32,
            )
            calibration_data_dict['label'] = np.empty((0,), dtype=np.int64)
        return calibration_data_dict

    def __serialize_calibration_session_data(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        calibration_data_dict: dict[str, np.ndarray],
    ) -> bytes:
        # 为什么这里不用 protobuf，而是直接打 npz？
        # 因为 calibration 是“整批 trial 训练数据”，天然更像一个文件块，
        # 用 np.savez_compressed 打包更简单，也便于算法端一次性还原 numpy 数组。
        npz_payload_dict = {
            'subject_id': np.array(runtime_task_group_model.subject_id),
            'exp_name': np.array(runtime_task_group_model.exp_name),
            'exp_task': np.array(runtime_task_group_model.exp_task),
            'session_id': np.array(runtime_task_group_model.session_id),
            'data': calibration_data_dict['data'],
            'label': calibration_data_dict['label'],
        }

        buffer = io.BytesIO()
        np.savez_compressed(buffer, **npz_payload_dict)
        return buffer.getvalue()

    @classmethod
    def __split_calibration_payload_to_chunk_list(cls, calibration_payload: bytes) -> list[bytes]:
        if not isinstance(calibration_payload, (bytes, bytearray)):
            raise TypeError("calibration payload 必须是 bytes")

        payload_bytes = bytes(calibration_payload)
        payload_size = len(payload_bytes)
        if payload_size == 0:
            chunk_payload_size = 0
            total_chunk_number = 1
        else:
            chunk_payload_size = cls.__MAX_CALIBRATION_CHUNK_BYTES
            total_chunk_number = (payload_size + chunk_payload_size - 1) // chunk_payload_size

        calibration_chunk_list: list[bytes] = []
        for chunk_index in range(total_chunk_number):
            chunk_start = chunk_index * chunk_payload_size
            chunk_end = chunk_start + chunk_payload_size
            chunk_payload = payload_bytes[chunk_start:chunk_end]
            chunk_header = struct.pack(
                cls.__CALIBRATION_CHUNK_HEADER_FORMAT,
                cls.__CALIBRATION_CHUNK_MAGIC,
                total_chunk_number,
                chunk_index,
                payload_size,
            )
            calibration_chunk_list.append(chunk_header + chunk_payload)
        return calibration_chunk_list

    async def __load_sample_matrix(
        self,
        data_file_path: str,
        data_file_format: str,
        total_channel_number: int,
    ) -> np.ndarray:
        if data_file_format == self.__BINARY_FLOAT32_DAT_FORMAT:
            async with aiofiles.open(data_file_path, 'rb') as file:
                data_bytes = await file.read()
            return self.__decode_binary_sample_matrix(data_bytes, total_channel_number)

        async with aiofiles.open(data_file_path, 'r', encoding='utf-8') as file:
            data_text = await file.read()
        return self.__decode_text_sample_matrix(data_text, total_channel_number)

    def __decode_binary_sample_matrix(self, data_bytes: bytes, total_channel_number: int) -> np.ndarray:
        data_array = np.frombuffer(data_bytes, dtype=np.float32)
        if len(data_array) % total_channel_number != 0:
            raise ValueError(
                f"二进制DAT数据无法按 {total_channel_number} 列整除，"
                f"float32元素数={len(data_array)}"
            )
        return data_array.reshape(-1, total_channel_number)

    def __decode_text_sample_matrix(self, data_text: str, total_channel_number: int) -> np.ndarray:
        line_list = [
            line.strip().replace(',', ' ')
            for line in data_text.splitlines()
            if line.strip()
        ]
        if len(line_list) == 0:
            raise ValueError("文本DAT读取失败，未解析出任何非空数据行")

        flat_data = np.fromstring('\n'.join(line_list), sep=' ', dtype=np.float32)
        if flat_data.size == 0:
            raise ValueError("文本DAT读取失败，未解析出任何数值")
        if flat_data.size % len(line_list) != 0:
            raise ValueError(
                f"文本DAT行列不规则，{len(line_list)} 行共解析出 {flat_data.size} 个数值"
            )

        resolved_total_channel_number = flat_data.size // len(line_list)
        if resolved_total_channel_number != total_channel_number:
            raise ValueError(
                f"文本DAT列数不匹配，配置期望 {total_channel_number} 列，实际读取到 {resolved_total_channel_number} 列"
            )
        return flat_data.reshape(len(line_list), total_channel_number)

    def __extract_trial_model_list(
        self,
        sample_matrix: np.ndarray,
        eeg_channel_index_list: list[int],
        trigger_channel_index: int,
        session_id: str,
        file_path: str,
    ) -> list[ParsedTrialModel]:
        # sample_matrix 的原始形状是 [time, channel]。
        # 这里会先把它整理成框架更喜欢的形式：
        # - EEG 部分转成 [channel, time]
        # - trigger 单独拿出来
        # 然后根据 trigger 的“0 -> 非0”跳变位置切 trial。
        #
        # 当前实现里，一个完整 trial 固定取 4000 点，
        # 所以只有起点，没有再去搜索结束 trigger。
        if sample_matrix.ndim != 2:
            raise ValueError(f"DAT解析结果必须是二维矩阵，实际维度={sample_matrix.ndim}")
        if len(eeg_channel_index_list) != self.__device_transfer_model.channel_number:
            raise ValueError(
                f"EEG通道映射数量不匹配，配置期望 {self.__device_transfer_model.channel_number} 个，"
                f"实际映射到 {len(eeg_channel_index_list)} 个"
            )
        if trigger_channel_index < 0 or trigger_channel_index >= sample_matrix.shape[1]:
            raise ValueError(f"trigger通道索引越界: {trigger_channel_index}")

        eeg_data_array = sample_matrix[:, eeg_channel_index_list].T
        trigger_array = np.rint(sample_matrix[:, trigger_channel_index]).astype(np.int32).reshape(1, -1)
        data_array = np.concatenate((eeg_data_array, trigger_array), axis=0)
        if self.__downsampling_factor is not None and self.__downsampling_factor != 1:
            # 降采样时会特别处理最后一行 trigger，尽量保留非零事件。
            data_array = self.__downsample(data_array, self.__downsampling_factor)

        eeg_data_array = data_array[:-1, :].copy()
        trigger_array = data_array[-1, :].astype(np.int32)
        valid_trigger_array = np.where(
            np.isin(trigger_array, self.__VALID_TRIGGER_VALUE_SET),
            trigger_array,
            0,
        )
        previous_trigger_array = np.concatenate(([0], valid_trigger_array[:-1]))
        trial_start_position_array = np.where(
            (valid_trigger_array != 0) & (previous_trigger_array == 0)
        )[0]
        # 上面这句等价于：
        # “只认一个 trial 开头第一次出现的有效 trigger”。
        # 如果 trigger 连续持续多个点，只取最开始那个点作为起点。

        parsed_trial_model_list: list[ParsedTrialModel] = []
        incomplete_trial_number = 0
        for trial_start_position in trial_start_position_array.tolist():
            trial_end_position = trial_start_position + self.__TRIAL_DURATION_POINTS
            if trial_end_position > eeg_data_array.shape[1]:
                # 长度不够 4000 点的 trial 直接丢弃，避免后续模型输入 shape 不一致。
                incomplete_trial_number += 1
                continue
            parsed_trial_model_list.append(
                ParsedTrialModel(
                    trigger_value=int(valid_trigger_array[trial_start_position]),
                    eeg_data=eeg_data_array[:, trial_start_position:trial_end_position].copy(),
                    original_start_position=trial_start_position,
                    session_id=session_id,
                    file_path=file_path,
                )
            )

        if incomplete_trial_number > 0:
            self.__logger.warning("存在 %s 个不完整trial，已在预处理中丢弃", incomplete_trial_number)
        return parsed_trial_model_list

    def __shuffle_trial_model_list(
        self,
        trial_model_list: list[ParsedTrialModel],
        data_file_model: DataFileModel,
    ) -> list[ParsedTrialModel]:
        if len(trial_model_list) == 0:
            return []

        # IMPORTANT: 为防止选手利用固定trial顺序作弊，这里必须在trial维度对筛选后的trial做打乱。
        # 这里使用与文件上下文绑定的稳定随机种子，既能打乱trial顺序，也保证同一输入数据的调试结果可复现。
        shuffle_rng = np.random.default_rng(self.__build_trial_shuffle_seed(data_file_model))
        shuffled_trial_index_array = shuffle_rng.permutation(len(trial_model_list))
        return [trial_model_list[index] for index in shuffled_trial_index_array.tolist()]

    def __shuffle_session_trial_model_list(
        self,
        trial_model_list: list[ParsedTrialModel],
        runtime_task_group_model: RuntimeTaskGroupModel,
    ) -> list[ParsedTrialModel]:
        if len(trial_model_list) == 0:
            return []

        # IMPORTANT: 校准trial抽取完成后，将当前session剩余的所有trial汇总后统一打乱，
        # 避免不同run之间仍保留固定顺序，降低选手利用run边界作弊的可能性。
        shuffle_seed_source = (
            f"{runtime_task_group_model.subject_id}|{runtime_task_group_model.exp_name}|"
            f"{runtime_task_group_model.exp_task}|{runtime_task_group_model.session_id}"
        )
        shuffle_seed = int.from_bytes(hashlib.sha256(shuffle_seed_source.encode('utf-8')).digest()[:8], 'big')
        shuffle_rng = np.random.default_rng(shuffle_seed)
        shuffled_trial_index_array = shuffle_rng.permutation(len(trial_model_list))
        return [trial_model_list[index] for index in shuffled_trial_index_array.tolist()]

    @staticmethod
    def __build_trial_shuffle_seed(data_file_model: DataFileModel) -> int:
        seed_source = (
            f"{data_file_model.subject_id}|{data_file_model.exp_name}|"
            f"{data_file_model.exp_task}|{data_file_model.file_path}"
        )
        return int.from_bytes(hashlib.sha256(seed_source.encode('utf-8')).digest()[:8], 'big')

    @classmethod
    def __map_exp_task_label(cls, exp_task: str, raw_trigger_value: int) -> int:
        if exp_task == 'left_vs_rest':
            return 1 if raw_trigger_value == 1 else 0
        if exp_task == 'right_vs_rest':
            return 1 if raw_trigger_value == 2 else 0
        raise ValueError(f"未知实验任务类型: {exp_task}")

    @staticmethod
    def __summarize_binary_label_count(label_array: np.ndarray) -> dict[str, int]:
        if not isinstance(label_array, np.ndarray) or label_array.size == 0:
            return {'0': 0, '1': 0}
        unique_label_array, unique_count_array = np.unique(label_array.astype(np.int64), return_counts=True)
        label_count_dict = {'0': 0, '1': 0}
        for unique_label, unique_count in zip(unique_label_array.tolist(), unique_count_array.tolist()):
            label_count_dict[str(int(unique_label))] = int(unique_count)
        return label_count_dict

    @staticmethod
    def __resolve_session_id(file_path: str) -> str:
        normalized_path = str(file_path).replace('\\', '/')
        for path_part in normalized_path.split('/'):
            if path_part.lower().startswith('session'):
                return path_part
        return 'session_unknown'

    async def __send_hidden_score_message(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        block_id: str,
        trial_id: str,
        trial_model: ParsedTrialModel,
    ) -> None:
        payload = {
            'subject_id': runtime_task_group_model.subject_id,
            'exp_name': runtime_task_group_model.exp_name,
            'exp_task': runtime_task_group_model.exp_task,
            'session_id': runtime_task_group_model.session_id,
            'block_id': str(block_id),
            'trial_id': str(trial_id),
            'raw_trigger_value': int(trial_model.trigger_value),
            'true_label': str(
                self.__map_exp_task_label(
                    exp_task=runtime_task_group_model.exp_task,
                    raw_trigger_value=trial_model.trigger_value,
                )
            ),
        }
        await self._component_framework.send_message(
            self.__HIDDEN_SCORE_SOURCE_LABEL,
            CommonMessageConverter.model_to_protobuf(
                DataMessageModel(
                    package=DataPackageModel(
                        data_position=0.0,
                        data=json.dumps(payload, ensure_ascii=False),
                    )
                )
            ).SerializeToString()
        )

    def __publish_current_trial_state(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        block_id: str,
        trial_id: str,
        trial_model: ParsedTrialModel,
        release_wallclock: float | None = None,
    ) -> None:
        dispatch_wallclock = time.time()
        effective_release_wallclock = release_wallclock if release_wallclock is not None else dispatch_wallclock
        payload = {
            'group_id': self.__group_id,
            'collector_component_id': self.__collector_component_id,
            'subject_id': runtime_task_group_model.subject_id,
            'exp_name': runtime_task_group_model.exp_name,
            'exp_task': runtime_task_group_model.exp_task,
            'session_id': runtime_task_group_model.session_id,
            'block_id': str(block_id),
            'trial_id': str(trial_id),
            'true_label': str(
                self.__map_exp_task_label(
                    exp_task=runtime_task_group_model.exp_task,
                    raw_trigger_value=trial_model.trigger_value,
                )
            ),
            'raw_trigger_value': int(trial_model.trigger_value),
            'release_wallclock': effective_release_wallclock,
            'dispatch_wallclock': dispatch_wallclock,
            'prediction_deadline_wallclock': None,
            'cycle_end_wallclock': None,
            'status': 'running',
        }
        self.__write_live_state_json('current_trial.json', payload)

    @staticmethod
    def __resolve_live_state_root_dir() -> Path:
        return Path(__file__).resolve().parents[5] / 'results' / 'live'

    def __write_live_state_json(self, relative_file_path: str, payload: dict) -> None:
        if str(relative_file_path) == 'current_trial.json':
            try:
                write_json_state(
                    resolve_runtime_state_db_path(PROJECT_ROOT),
                    STATE_KEY_CURRENT_TRIAL,
                    payload,
                )
            except OSError:
                self.__logger.exception("写入 SQLite current_trial 状态失败")

    def __update_current_trial_timing_state(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        block_id: str,
        trial_id: str,
        trial_model: ParsedTrialModel,
        trial_sent_wallclock: float,
        next_release_target_wallclock: float,
        release_wallclock: float | None,
        dispatch_wallclock: float,
    ) -> None:
        payload = {
            'group_id': self.__group_id,
            'collector_component_id': self.__collector_component_id,
            'subject_id': runtime_task_group_model.subject_id,
            'exp_name': runtime_task_group_model.exp_name,
            'exp_task': runtime_task_group_model.exp_task,
            'session_id': runtime_task_group_model.session_id,
            'block_id': str(block_id),
            'trial_id': str(trial_id),
            'true_label': str(
                self.__map_exp_task_label(
                    exp_task=runtime_task_group_model.exp_task,
                    raw_trigger_value=trial_model.trigger_value,
                )
            ),
            'raw_trigger_value': int(trial_model.trigger_value),
            'release_wallclock': release_wallclock if release_wallclock is not None else dispatch_wallclock,
            'dispatch_wallclock': dispatch_wallclock,
            'prediction_deadline_wallclock': trial_sent_wallclock + self.__PREDICTION_TIMEOUT_SECONDS,
            'cycle_end_wallclock': next_release_target_wallclock,
            'trial_sent_wallclock': trial_sent_wallclock,
            'next_release_target_wallclock': next_release_target_wallclock,
            'status': 'running',
        }
        self.__write_live_state_json('current_trial.json', payload)

    def __publish_current_trial_finished_state(self) -> None:
        self.__write_live_state_json(
            'current_trial.json',
            {
                'group_id': self.__group_id,
                'collector_component_id': self.__collector_component_id,
                'status': 'finished',
                'updated_at': time.time(),
            }
        )

    def __publish_current_trial_error_state(
        self,
        runtime_task_group_model: RuntimeTaskGroupModel,
        error: BaseException,
    ) -> None:
        error_message = str(error).strip() or repr(error)
        self.__write_live_state_json(
            'current_trial.json',
            {
                'group_id': self.__group_id,
                'collector_component_id': self.__collector_component_id,
                'subject_id': runtime_task_group_model.subject_id,
                'exp_name': runtime_task_group_model.exp_name,
                'exp_task': runtime_task_group_model.exp_task,
                'session_id': runtime_task_group_model.session_id,
                'status': 'error',
                'error_type': type(error).__name__,
                'error_message': error_message,
                'recovery_advice': '检查裁判端消息桥接与网络后，从当前阶段重新开始比赛。',
                'updated_at': time.time(),
            }
        )

    async def __send_trial_data(
        self,
        trial_model: ParsedTrialModel,
        runtime_task_group_model: RuntimeTaskGroupModel,
        block_id: str,
        trial_id: str,
        stage_key: str,
        release_granted_wallclock: float | None = None,
        release_wallclock: float | None = None,
        release_interval_seconds: float = 1.3,
    ) -> None:
        # 这里把“一个完整 online trial”拆回实时流：
        # 1. 先发 TRIAL_START 事件；
        # 2. 再按 send_package_points 分块发送 EEG 数据；
        # 3. 最后发 TRIAL_END 事件。
        #
        # 算法端并不会直接收到 ParsedTrialModel，而是收到这些离散的数据块，
        # 然后在 ContinuousDataSourceReceiver/AlgorithmImplement.run() 中重新拼回 trial。
        trial_dispatch_start_wallclock = time.time()
        trial_start_position = self.__current_date_position
        trial_point_number = trial_model.eeg_data.shape[1]
        chunk_count = (trial_point_number + self.__send_package_points - 1) // self.__send_package_points
        self.__logger.debug(
            "开始发送 online trial 数据: group_id=%s stage_key=%s subject_id=%s exp_name=%s exp_task=%s "
            "session_id=%s block_id=%s trial_id=%s trial_start_position=%s trial_points=%s send_package_points=%s "
            "chunk_count=%s release_to_dispatch_start_ms=%.3f",
            self.__group_id,
            stage_key,
            runtime_task_group_model.subject_id,
            runtime_task_group_model.exp_name,
            runtime_task_group_model.exp_task,
            runtime_task_group_model.session_id,
            block_id,
            trial_id,
            trial_start_position,
            trial_point_number,
            self.__send_package_points,
            chunk_count,
            (trial_dispatch_start_wallclock - release_granted_wallclock) * 1000.0
            if release_granted_wallclock is not None else -1.0,
        )
        await self._receiver_transponder.send_data(
            self.__create_event_receiver_transfer_model(
                event_position=trial_start_position,
                event_data=self.__TRIAL_START_TRIGGER,
            )
        )
        await asyncio.sleep(0)
        for chunk_start_index in range(0, trial_point_number, self.__send_package_points):
            # chunk_data_array 形状是 [channel, chunk_point]。
            # 发送前转成 [time, channel] 再拉平成一维，
            # 因为下游 DataTransferModel 约定 data 是按“时间优先”展开的连续数组。
            chunk_data_array = trial_model.eeg_data[
                :,
                chunk_start_index:chunk_start_index + self.__send_package_points,
            ]
            transfer_data = chunk_data_array.T.reshape(-1).astype(np.float32)
            await self._receiver_transponder.send_data(
                ReceiverTransferModel(
                    package=DataTransferModel(
                        data_position=self.__current_date_position,
                        data=transfer_data,
                    )
                )
            )
            self.__current_date_position += chunk_data_array.shape[1]
            await asyncio.sleep(0)

        await self._receiver_transponder.send_data(
            self.__create_event_receiver_transfer_model(
                event_position=trial_start_position + trial_point_number,
                event_data=self.__TRIAL_END_TRIGGER,
            )
        )
        trial_dispatch_end_wallclock = time.time()
        next_release_target_wallclock = trial_dispatch_end_wallclock + release_interval_seconds
        await asyncio.sleep(0)
        self.__logger.debug(
            "online trial 数据发送完成: group_id=%s stage_key=%s subject_id=%s exp_name=%s exp_task=%s "
            "session_id=%s block_id=%s trial_id=%s trial_start_position=%s trial_end_position=%s chunk_count=%s "
            "dispatch_duration_ms=%.3f release_to_trial_end_sent_ms=%.3f release_wallclock=%.6f next_data_position=%s",
            self.__group_id,
            stage_key,
            runtime_task_group_model.subject_id,
            runtime_task_group_model.exp_name,
            runtime_task_group_model.exp_task,
            runtime_task_group_model.session_id,
            block_id,
            trial_id,
            trial_start_position,
            trial_start_position + trial_point_number,
            chunk_count,
            (trial_dispatch_end_wallclock - trial_dispatch_start_wallclock) * 1000.0,
            (trial_dispatch_end_wallclock - release_granted_wallclock) * 1000.0
            if release_granted_wallclock is not None else -1.0,
            release_wallclock or -1.0,
            self.__current_date_position,
        )
        self.__update_current_trial_timing_state(
            runtime_task_group_model=runtime_task_group_model,
            block_id=block_id,
            trial_id=trial_id,
            trial_model=trial_model,
            trial_sent_wallclock=trial_dispatch_end_wallclock,
            next_release_target_wallclock=next_release_target_wallclock,
            release_wallclock=release_wallclock,
            dispatch_wallclock=trial_dispatch_start_wallclock,
        )
        self.__schedule_trial_post_send_task(
            self.__send_trial_post_send_updates(
                runtime_task_group_model=runtime_task_group_model,
                block_id=block_id,
                trial_id=trial_id,
                trial_model=trial_model,
                trial_start_position=trial_start_position,
                trial_point_number=trial_point_number,
                stage_key=stage_key,
                trial_dispatch_start_wallclock=trial_dispatch_start_wallclock,
                trial_dispatch_end_wallclock=trial_dispatch_end_wallclock,
                release_granted_wallclock=release_granted_wallclock,
                release_wallclock=release_wallclock,
                release_interval_seconds=release_interval_seconds,
                next_release_target_wallclock=next_release_target_wallclock,
            ),
            task_name=f"trial-post-send-{stage_key}-{trial_id}",
        )

    async def __send_trial_post_send_updates(
        self,
        *,
        runtime_task_group_model: RuntimeTaskGroupModel,
        block_id: str,
        trial_id: str,
        trial_model: ParsedTrialModel,
        trial_start_position: int,
        trial_point_number: int,
        stage_key: str,
        trial_dispatch_start_wallclock: float,
        trial_dispatch_end_wallclock: float,
        release_granted_wallclock: float | None,
        release_wallclock: float | None,
        release_interval_seconds: float,
        next_release_target_wallclock: float,
    ) -> None:
        # team_trial_sent 只是协调器观测发送完成的辅助事件，
        # 放到后台尾处理，避免一个 trial 的附加消息阻塞下一个 trial 的放行等待。
        await self.__send_runtime_stage_event(
            {
                'event_type': 'team_trial_sent',
                'group_id': self.__group_id,
                'collector_component_id': self.__collector_component_id,
                'stage_context': self.__build_stage_context(runtime_task_group_model),
                'trial_context': {
                    'block_id': str(block_id),
                    'trial_id': int(trial_id),
                    'trial_start_position': int(trial_start_position),
                    'trial_end_position': int(trial_start_position + trial_point_number),
                },
                'trial_sent_wallclock': trial_dispatch_end_wallclock,
                'release_wallclock': release_wallclock,
                'dispatch_wallclock': trial_dispatch_start_wallclock,
                'next_release_target_wallclock': next_release_target_wallclock,
                'trial_release_interval_seconds': release_interval_seconds,
                'release_to_trial_end_sent_ms': (
                    (trial_dispatch_end_wallclock - release_granted_wallclock) * 1000.0
                    if release_granted_wallclock is not None else None
                ),
            }
        )

    @staticmethod
    def __safe_positive_float(value) -> float | None:
        try:
            parsed_value = float(value)
        except (TypeError, ValueError):
            return None
        if parsed_value <= 0:
            return None
        return parsed_value

    def __resolve_file_total_channel_number(self, metadata_dict: dict[str, str]) -> int:
        expected_total_channel_number = self.__device_transfer_model.channel_number + 1
        metadata_channel_number = metadata_dict.get('channels')
        if metadata_channel_number is None:
            return expected_total_channel_number

        try:
            total_channel_number = int(metadata_channel_number)
        except ValueError:
            self.__logger.warning("无法解析 metadata channels=%s，将继续使用配置列数", metadata_channel_number)
            return expected_total_channel_number

        return total_channel_number

    def __resolve_file_total_sample_number(
        self,
        metadata_dict: dict[str, str],
        data_file_path: str,
        data_file_format: str,
        file_total_channel_number: int,
    ) -> Union[int, None]:
        timepoints_text = metadata_dict.get('timepoints')
        trials_text = metadata_dict.get('trials', '1')
        if timepoints_text is not None:
            try:
                return int(timepoints_text) * int(trials_text)
            except ValueError:
                self.__logger.warning(
                    "%s metadata timepoints/trials 解析失败，将尝试根据文件内容推断总点数",
                    data_file_path,
                )

        if data_file_format == self.__BINARY_FLOAT32_DAT_FORMAT:
            file_size_bytes = os.path.getsize(data_file_path)
            bytes_per_sample = file_total_channel_number * self.__data_byte_width
            if bytes_per_sample == 0:
                return None
            return file_size_bytes // bytes_per_sample

        return None

    def __resolve_data_channel_mapping(
        self,
        metadata_dict: dict[str, str],
        file_total_channel_number: int,
        data_file_path: str,
    ) -> tuple[list[int], int]:
        # 这个函数解决的是“文件里的列”和“算法想要的通道顺序”之间的映射问题。
        #
        # 返回值是两个核心结果：
        # 1. eeg_channel_index_list: 应该从文件哪几列取 EEG，并且取出的顺序是什么；
        # 2. trigger_channel_index : trigger 在哪一列。
        #
        # 只要这一步做对，后面无论原始文件列顺序怎么排，都能抽出一致的 EEG trial。
        file_channel_label_list = self.__parse_metadata_channel_labels(metadata_dict)
        desired_eeg_channel_label_list = self.__device_transfer_model.channel_label

        if file_channel_label_list and len(file_channel_label_list) != file_total_channel_number:
            raise ValueError(
                f"{data_file_path} metadata 中 channel_labels 数量={len(file_channel_label_list)}，"
                f"与 channels={file_total_channel_number} 不一致"
            )

        if not file_channel_label_list:
            # 没 metadata 时只能退化为“经验规则”：
            # 前 N 列视作 EEG，最后 1 列视作 trigger。
            # 这也是为什么 metadata 缺失时风险更高。
            if file_total_channel_number < self.__device_transfer_model.channel_number + 1:
                raise ValueError(
                    f"{data_file_path} 列数不足，至少需要 {self.__device_transfer_model.channel_number + 1} 列，"
                    f"实际只有 {file_total_channel_number} 列"
                )
            if file_total_channel_number > self.__device_transfer_model.channel_number + 1:
                self.__logger.warning(
                    "%s 未提供 channel_labels，将默认取前 %s 列作为EEG，最后1列作为trigger，中间多余列将被忽略",
                    data_file_path,
                    self.__device_transfer_model.channel_number,
                )
            eeg_channel_index_list = list(range(self.__device_transfer_model.channel_number))
            trigger_channel_index = file_total_channel_number - 1
            return eeg_channel_index_list, trigger_channel_index

        normalized_file_label_to_index_dict = {
            self.__normalize_channel_label(channel_label): index
            for index, channel_label in enumerate(file_channel_label_list)
        }
        # 这里先把文件中的通道标签归一化成“只保留字母数字的大写形式”，
        # 目的是兼容 C3 / c3 / C-3 这类轻微格式差异。

        eeg_channel_index_list: list[int] = []
        missing_channel_label_list: list[str] = []
        for channel_label in desired_eeg_channel_label_list:
            normalized_label = self.__normalize_channel_label(channel_label)
            if normalized_label not in normalized_file_label_to_index_dict:
                missing_channel_label_list.append(channel_label)
                continue
            eeg_channel_index_list.append(normalized_file_label_to_index_dict[normalized_label])

        if missing_channel_label_list:
            raise ValueError(
                f"{data_file_path} 中缺少配置要求的EEG通道: {missing_channel_label_list}"
            )

        trigger_channel_index = self.__find_trigger_channel_index(file_channel_label_list)
        if trigger_channel_index is None:
            raise ValueError(f"{data_file_path} 中未找到trigger通道，请检查 metadata channel_labels")

        overlap_index_set = set(eeg_channel_index_list) & {trigger_channel_index}
        if overlap_index_set:
            raise ValueError(f"{data_file_path} 的EEG通道映射与trigger通道重叠: {overlap_index_set}")

        aux_channel_index_list = [
            index
            for index, channel_label in enumerate(file_channel_label_list)
            if self.__normalize_channel_label(channel_label) in self.__aux_channel_label_set
        ]
        # ignored_channel_number 用来帮助你发现“文件里还有一些框架没用到的列”，
        # 比如 EOG / EMG / 其他辅助信号。
        ignored_channel_number = file_total_channel_number - len(eeg_channel_index_list) - 1
        if ignored_channel_number != len(aux_channel_index_list):
            self.__logger.warning(
                "%s 中存在 %s 个非EEG非trigger列，其中识别出的辅助通道数为 %s；将按EEG映射和trigger映射继续处理",
                data_file_path,
                ignored_channel_number,
                len(aux_channel_index_list),
            )
        return eeg_channel_index_list, trigger_channel_index

    def __load_metadata(self, data_file_path: str) -> dict[str, str]:
        # metadata 文件是同名的 *_meta.txt。
        # 这里不做复杂语法解析，只支持最简单的 key=value。
        # 因此如果你想加新字段，最安全的做法就是继续沿用 key=value 每行一项。
        metadata_path = f"{os.path.splitext(data_file_path)[0]}_meta.txt"
        if not os.path.exists(metadata_path):
            return {}

        metadata_dict: dict[str, str] = {}
        with open(metadata_path, 'r', encoding='utf-8') as metadata_file:
            for line in metadata_file:
                stripped_line = line.strip()
                if not stripped_line or '=' not in stripped_line:
                    continue
                key, value = stripped_line.split('=', 1)
                metadata_dict[key.strip()] = value.strip()
        return metadata_dict

    def __detect_data_file_format(self, data_file_path: str, metadata_dict: dict[str, str]) -> str:
        # 文件格式判断优先级：
        # 1. 先信 metadata；
        # 2. metadata 没说，再抽样检查前 512 字节像不像文本。
        #
        # 这样做的原因是：比赛数据有时会混合文本/二进制存储，
        # 不能只靠扩展名猜。
        for key in ('storage_format', 'data_format', 'file_format'):
            format_text = metadata_dict.get(key)
            if format_text is None:
                continue
            normalized_format = format_text.strip().lower()
            if 'binary' in normalized_format and 'float32' in normalized_format:
                return self.__BINARY_FLOAT32_DAT_FORMAT
            if normalized_format in {'text', 'txt', 'ascii'}:
                return self.__TEXT_DAT_FORMAT

        with open(data_file_path, 'rb') as data_file:
            sample_bytes = data_file.read(512)

        if not sample_bytes:
            return self.__BINARY_FLOAT32_DAT_FORMAT

        printable_byte_number = sum(
            1 for value in sample_bytes
            if value in (9, 10, 13, 32) or 48 <= value <= 57 or value in (43, 44, 45, 46, 69, 101)
        )
        if b'\x00' not in sample_bytes and printable_byte_number / len(sample_bytes) > 0.95:
            return self.__TEXT_DAT_FORMAT

        return self.__BINARY_FLOAT32_DAT_FORMAT

    def __parse_metadata_channel_labels(self, metadata_dict: dict[str, str]) -> list[str]:
        # 这里只负责“把字符串拆成列表”，不负责校验数量与合法性。
        # 真正的校验在 __resolve_data_channel_mapping() 里完成。
        channel_labels_text = metadata_dict.get('channel_labels')
        if channel_labels_text is None:
            return []
        return [channel_label.strip() for channel_label in channel_labels_text.split(',') if channel_label.strip()]

    def __find_trigger_channel_index(self, file_channel_label_list: list[str]) -> Union[int, None]:
        for index, channel_label in enumerate(file_channel_label_list):
            normalized_label = self.__normalize_channel_label(channel_label)
            if normalized_label in self.__trigger_channel_label_set:
                return index
        return None

    @staticmethod
    def __create_event_receiver_transfer_model(event_position: int, event_data: int) -> ReceiverTransferModel:
        return ReceiverTransferModel(
            package=EventTransferModel(
                event_position=[event_position],
                event_data=[str(int(event_data))],
            )
        )

    @staticmethod
    def __normalize_channel_label(channel_label: str) -> str:
        return ''.join(char for char in str(channel_label).upper() if char.isalnum())

    @staticmethod
    def __downsample(data_array: np.ndarray, downsampling_factor: int) -> np.ndarray:
        """
        对输入数据进行整体降采样，以直接抽取的方式进行，抽取trigger时保留第一位非0元素
        :param data_array: 输入数据，行表示导联，列表示样本点，最后一行为trigger通道
        :param downsampling_factor: 整数降采样因子
        :return:
        """
        new_data_array = np.delete(data_array, -1, axis=0)
        downsampled_data_array = new_data_array[:, ::downsampling_factor]
        downsampled_trigger_array = np.zeros([1, downsampled_data_array.shape[1]])
        trigger_array = data_array[-1, :]
        trigger_index = np.where(trigger_array != 0)[0]
        new_trigger_index = trigger_index // downsampling_factor
        for i in range(len(new_trigger_index) - 1, -1, -1):
            downsampled_trigger_array[0, new_trigger_index[i]] = trigger_array[trigger_index[i]]

        downsampled_total_array = np.concatenate((downsampled_data_array, downsampled_trigger_array), axis=0)
        return downsampled_total_array
