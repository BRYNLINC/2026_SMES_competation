import asyncio
import logging
import time
from typing import Union

import numpy

from Collector.api.message.MessageKeyEnum import MessageKeyEnum
from Collector.api.model.ExternalTriggerModel import ExternalTriggerModel
from Collector.datasender.interface.DataSenderInterface import DataSenderInterface
from Common.converter.CommonMessageConverter import CommonMessageConverter
from Common.model.CommonMessageModel import (DataMessageModel, DataPackageModel, EventPackageModel, DevicePackageModel,
                                             ImpedancePackageModel, InformationPackageModel)

from Common.model.CommonMessageModel import ControlPackageModel


class TimingDataSender(DataSenderInterface):
    """
    计时数据发送器
    数据包发送时先记录当前数据点数和时间，当到达指定时间后才发送数据包

    可以把它理解成“实时节拍器”：
    1. DevicePackage 到来时，记住采样率和通道数；
    2. DataPackage 到来时，计算它理论上应该在哪个时刻发出；
    3. 如果还没到点，就 sleep；
    4. 到点后再通过组件框架发出去。
    """

    def __init__(self):
        super().__init__()
        self.__cached_external_trigger_list: list[ExternalTriggerModel] = list[ExternalTriggerModel]()
        self.__start_data_sending_time: float = 0.0
        self.__sample_rate: float = 0.0
        self.__channel_number: int = 0
        self.__current_stream_role: str = 'online'
        self.__current_online_replay_mode: str = 'burst' # 决赛使用busrt模式
        self.__sending_flag: bool = False
        self.__logger = logging.getLogger("collectorLogger")

    async def initial(self, config_dict: dict[str, Union[str, dict]] = None) -> None:
        pass

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        # 清理缓存内容
        self.__cached_external_trigger_list.clear()

    async def start_data_sending(self):
        if self.__sending_flag:
            return
        else:
            self.__sending_flag = True
            # 记录“开始发送”的基准时间。
            self.__start_data_sending_time = time.perf_counter() # 记录起始时间

    async def stop_data_sending(self):
        self.__sending_flag = False

    async def send_data(self, data_message_model: DataMessageModel) -> None:
        # 只处理框架约定的这几类包。
        if not isinstance(data_message_model.package,
                          Union[
                              DevicePackageModel,
                              EventPackageModel,
                              DataPackageModel,
                              ImpedancePackageModel,
                              InformationPackageModel,
                              ControlPackageModel
                          ]):
            return
        # 如果还没正式 start_data_sending，则 Data/Event 这种流式包先不发；
        # 但设备信息、控制包仍允许通过。
        if isinstance(data_message_model.package, Union[DataPackageModel, EventPackageModel]) and not self.__sending_flag:
            return

        # 外部 trigger 会先被缓存，等下一包 DataPackage 到来时转成 EventPackage 发出去。
        match data_message_model.package:
            case DevicePackageModel():
                await self.__device_package_func(data_message_model)
            case DataPackageModel():
                await self.__data_package_func(data_message_model)
            case _:
                await self.__default_func(data_message_model)

    async def receiver_external_trigger(self, external_trigger_model: ExternalTriggerModel) -> None:
        # 收到外部trigger，缓存入list，待下一次数据发送时发出
        self.__cached_external_trigger_list.append(external_trigger_model)

    async def __device_package_func(self, data_message_model: DataMessageModel):
        device_package_model: DevicePackageModel = data_message_model.package
        # 后续所有实时调度都依赖采样率和通道数，因此先缓存起来。
        self.__sample_rate = device_package_model.sample_rate
        self.__channel_number = device_package_model.channel_number
        other_information = device_package_model.other_information or {}
        self.__current_stream_role = str(other_information.get('stream_role', 'online') or 'online').strip().lower()
        self.__current_online_replay_mode = str(
            other_information.get('online_replay_mode', 'realtime') or 'realtime'
        ).strip().lower()
        # 这里把“当前流的设备信息”发到 Collector 的 SEND_DATA topic。
        # Python 侧最终接收入口在：
        # app/ProcessHub/ProcessHub/bci_competition/orchestrator/bci_competition_orchestrator/BciCompetitionOrchestrator.py
        # 里的 ReceiveDataOperator.receive_message()。
        # 它会再把消息转成算法源数据，继续送往 Algorithm 进程。
        await self._component_framework.send_message(
            MessageKeyEnum.SEND_DATA.value,
            CommonMessageConverter.model_to_protobuf(data_message_model).SerializeToString()
        )

    async def __data_package_func(self, data_message_model: DataMessageModel):
        current_time = time.perf_counter()
        data_package_model: DataPackageModel = data_message_model.package
        # 收敛数据类型，方便后续统一计算长度与序列化。
        if isinstance(data_package_model.data, list):
            if len(data_package_model.data) > 0:
                first_data = data_package_model.data[0]
                if isinstance(first_data, float):
                    data_package_model.data = numpy.ndarray(data_package_model.data, dtype=numpy.float32)
                elif isinstance(first_data, int):
                    data_package_model.data = numpy.ndarray(data_package_model.data, dtype=numpy.int32)
        elif isinstance(data_package_model.data, numpy.ndarray):
            if data_package_model.data.dtype == numpy.float64:
                data_package_model.data = data_package_model.data.astype(numpy.float32)
            elif data_package_model.data.dtype == numpy.int64:
                data_package_model.data = data_package_model.data.astype(numpy.int32)

        this_data_position = data_package_model.data_position
        data_message_class = type(data_package_model.data)
        this_data_end_point = this_data_position + (len(data_package_model.data) / self.__channel_number) - 1 \
            if data_message_class in [list, numpy.ndarray] else this_data_position
        # online 阶段在 burst 模式下不再按真实采样时间等待，trial 被放行后尽快发完。
        if self.__current_stream_role == 'online' and self.__current_online_replay_mode == 'burst':
            wait_time = 0.0
            self.__logger.debug(
                "online burst 发送数据包: data_position=%s data_end_point=%s",
                this_data_position,
                this_data_end_point,
            )
        else:
            # 理论发送时间 = 数据包末尾采样点 / sample_rate。
            # 当前已过去时间 = now - start_time。
            # 两者相减就是还需等待多久。
            wait_time = (this_data_end_point / self.__sample_rate) - (current_time - self.__start_data_sending_time)
            self.__logger.debug(f"当前距离发送开始时间:{current_time - self.__start_data_sending_time},"
                                f"数据包末尾时间{this_data_end_point / self.__sample_rate},"
                                f"所需等待时间:{wait_time}")
            # 如果已经“发晚了”，就立刻发；否则按节奏等待。
            if wait_time > 0:
                await asyncio.sleep(wait_time)

        for external_trigger_index, external_trigger in enumerate(self.__cached_external_trigger_list):
            event_package_model = EventPackageModel(
                event_position=[this_data_position + external_trigger_index],
                event_data=[external_trigger.trigger]
            )
            # 外部 trigger 会先转成 EventPackage 再发到 SEND_DATA topic。
            # Python 侧最终也是由 BciCompetitionOrchestrator.ReceiveDataOperator.receive_message()
            # 接收，然后进入 ProcessHub 的统一编排流程。
            await self._component_framework.send_message(
                MessageKeyEnum.SEND_DATA.value,
                CommonMessageConverter.model_to_protobuf(event_package_model).SerializeToString()
            )
        # 触发事件发完后立刻清空缓存，避免重复发。
        self.__cached_external_trigger_list.clear()
        # 真正的连续 EEG 数据包也走同一条 SEND_DATA 链路。
        # 最终接收文件仍然是：
        # app/ProcessHub/ProcessHub/bci_competition/orchestrator/bci_competition_orchestrator/BciCompetitionOrchestrator.py
        await self._component_framework.send_message(
            MessageKeyEnum.SEND_DATA.value,
            CommonMessageConverter.model_to_protobuf(data_message_model).SerializeToString()
        )
        self.__logger.debug(f"发送数据包，当前距离发送开始时间:{time.perf_counter() - self.__start_data_sending_time}")

    async def __default_func(self, data_message_model: DataMessageModel):
        if isinstance(data_message_model.package, ControlPackageModel):
            self.__logger.info(f"发送control包:{data_message_model}")
        # Control / Information / Impedance 这些非连续流消息也统一发到 SEND_DATA topic。
        # Python 侧最终依然由 BciCompetitionOrchestrator.ReceiveDataOperator.receive_message()
        # 收到，再根据包类型决定如何转给后续流程。
        await self._component_framework.send_message(
            MessageKeyEnum.SEND_DATA.value,
            CommonMessageConverter.model_to_protobuf(data_message_model).SerializeToString()
        )




