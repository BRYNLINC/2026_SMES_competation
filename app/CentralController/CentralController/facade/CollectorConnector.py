import logging

from injector import inject

from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkInterface

from CentralController.facade.interface.SubsystemConnectorInterface import CollectorConnectorInterface
from Collector.api.converter.CollectorControlMessageConverter import CollectorControlMessageConverter
from Collector.api.message.MessageKeyEnum import MessageKeyEnum
from Collector.api.model.CollectorControlModel import CollectorControlModel, StartDataSendingControlModel, \
    StopDataSendingControlModel, SendDeviceInfoControlModel, SendImpedanceControlModel, ApplicationExitControlModel


class CollectorConnector(CollectorConnectorInterface):

    @inject
    def __init__(self, component_framework: ComponentFrameworkInterface):
        self.__logger = logging.getLogger('centralControllerLogger')
        self.__component_framework = component_framework

    async def start_data_sending(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = CollectorControlModel(
            package=StartDataSendingControlModel()
        )
        proto = CollectorControlMessageConverter.model_to_protobuf(send_model)
        # 这里发给 Collector 的命令控制 topic。
        # Python 侧接收入口在：
        # app/Collector/Collector/control/CommandController.py
        # 里的 ReceiveCommandControlMessageOperator.receive_message()。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())

    async def stop_data_sending(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = CollectorControlModel(
            package=StopDataSendingControlModel()
        )
        proto = CollectorControlMessageConverter.model_to_protobuf(send_model)
        # 最终由 Collector 的 CommandController.receive_message() 接收并调用 business_manager.stop_data_sending()。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())

    async def send_device_info(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = CollectorControlModel(
            package=SendDeviceInfoControlModel()
        )
        proto = CollectorControlMessageConverter.model_to_protobuf(send_model)
        # 最终由 Collector/control/CommandController.py 接收，触发 business_manager.send_device_info()。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())

    async def send_impedance(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = CollectorControlModel(
            package=SendImpedanceControlModel()
        )
        proto = CollectorControlMessageConverter.model_to_protobuf(send_model)
        # 最终由 Collector/control/CommandController.py 接收，触发 business_manager.send_impedance()。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())

    async def application_exit(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = CollectorControlModel(
            package=ApplicationExitControlModel()
        )
        proto = CollectorControlMessageConverter.model_to_protobuf(send_model)
        # 退出命令同样由 Collector/control/CommandController.py 接收，
        # 然后转成 CollectorEventEnum.APPLICATION_EXIT 事件。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())
