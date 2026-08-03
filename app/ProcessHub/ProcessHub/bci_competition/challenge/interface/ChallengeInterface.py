from abc import abstractmethod, ABC
from typing import Union

from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel, AlgorithmReportMessageModel
from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkApplicationInterface
from Common.model.CommonMessageModel import ScorePackageModel
from ProcessHub.orchestrator.model.SourceModel import SourceModel


class ChallengeInterface(ABC):

    def __init__(self):
        self._component_framework: ComponentFrameworkApplicationInterface = None

    @abstractmethod
    async def receive_message(self, algorithm_data_message_model: AlgorithmDataMessageModel) \
            -> Union[AlgorithmDataMessageModel, None]:
        # 需要针对algorithm_data_message_model中不同来源，不同类型的数据包(DevicePackageModel,EventPackageModel
        # DataPackageModel,ImpedancePackageModel，InformationPackageModel，ControlpackageModel)进行预处理
        pass

    @abstractmethod
    async def receive_report(self, algorithm_report_message_model: AlgorithmReportMessageModel) -> None:
        """
        接收报告结果，并填满所有内容
        """
        pass

    @abstractmethod
    async def receive_algorithm_connector_closed_event(self, algorithm_address: str):
        """
        接收算法关闭事件，并要求补全所有试次的成绩列表，待get_score()被调用时返回所有试次的成绩（包含算法端未报告的）
        """
        pass

    @abstractmethod
    async def receive_algorithm_config(self, algorithm_config: dict[str, Union[str, dict]]):
        pass

    @abstractmethod
    async def get_score(self) -> list[ScorePackageModel]:
        # 获取成绩,需要返回截止至当前数据包，当前block成绩
        pass

    async def receive_timeout_trial(self, timeout_context: dict) -> None:
        return None

    @abstractmethod
    async def timeout_trigger(self, algorithm_data_message_model: AlgorithmDataMessageModel):
        pass

    @abstractmethod
    async def initial(self):
        # # 初始化配置信息
        # self._config.update(config_dict)
        # # 加载source配置信息
        # source_dict = config_dict.get('sources', dict[str, dict[str, str]]())
        # for source_label, source_config in source_dict.items():
        #     self._source_list.append(SourceModel(source_label, source_config.get('topic', None)))
        pass

    @abstractmethod
    async def startup(self):
        pass

    @abstractmethod
    async def shutdown(self):
        pass

    @abstractmethod
    async def get_to_algorithm_config(self) -> dict[str, Union[str, dict]]:
        pass
        # return {'challenge_to_algorithm_config': self._config.get(
        #     'challenge_to_algorithm_config', dict[str, Union[str, dict]]()
        # )
        # }

    @abstractmethod
    async def get_source_list(self) -> list[SourceModel]:
        pass
        # # 获取当前赛题的source配置信息
        # return self._source_list

    @abstractmethod
    async def get_to_strategy_config(self) -> dict[str, Union[str, dict]]:
        pass


    def set_component_framework(self, component_framework: ComponentFrameworkApplicationInterface):
        self._component_framework = component_framework
        
