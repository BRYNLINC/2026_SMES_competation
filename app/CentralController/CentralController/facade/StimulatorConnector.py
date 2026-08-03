import logging

from injector import inject

from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkInterface

from CentralController.facade.interface.SubsystemConnectorInterface import StimulatorConnectorInterface
from Stimulator.api.converter.CommandControlMessageConverter import StimulationSystemCommandControlMessageConverter
from Stimulator.api.converter.RandomNumberSeedsMessageConverter import RandomNumberSeedsMessageConverter
from Stimulator.api.message.MessageKeyEnum import MessageKeyEnum

from Stimulator.api.model.CommandControlModel import StimulationControlModel, StartStimulationControlModel, \
    StopStimulationControlModel, QuitStimulationControlModel
from Stimulator.api.model.RandomNumberSeedsModel import RandomNumberSeedsModel


class StimulatorConnector(StimulatorConnectorInterface):

    @inject
    def __init__(self, component_framework: ComponentFrameworkInterface):
        self.__logger = logging.getLogger('centralControllerLogger')
        self.__component_framework = component_framework

    async def start_stimulation(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = StimulationControlModel(
            package=StartStimulationControlModel()
        )
        proto = StimulationSystemCommandControlMessageConverter.model_to_protobuf(send_model)
        # 这里发送给刺激器子系统。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())

    async def stop_stimulation(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = StimulationControlModel(
            package=StopStimulationControlModel()
        )
        proto = StimulationSystemCommandControlMessageConverter.model_to_protobuf(send_model)
        # Python 仓库内无明确接收实现，通常是外部刺激系统接收该 topic。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())

    async def send_random_number_seeds(self, random_number_seeds: float, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.RANDOM_NUMBER_SEEDS.value}"
        send_model = RandomNumberSeedsModel(
            seeds=random_number_seeds
        )
        proto = RandomNumberSeedsMessageConverter.model_to_protobuf(send_model)
        # 随机种子消息也发给外部刺激系统；仓库中未提供 Python 接收者实现。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())

    async def application_exit(self, component_id: str):
        message_key = f"{component_id}.{MessageKeyEnum.COMMAND_CONTROL.value}"
        send_model = StimulationControlModel(
            package=QuitStimulationControlModel()
        )
        proto = StimulationSystemCommandControlMessageConverter.model_to_protobuf(send_model)
        # 退出刺激系统的命令同样发往外部 stimulator 组件，Python 侧无接收文件。
        await self.__component_framework.send_message(message_key, proto.SerializeToString())
