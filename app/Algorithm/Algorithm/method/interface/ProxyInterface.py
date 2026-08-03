from abc import ABC, abstractmethod
from typing import Union

from Algorithm.method.interface.SourceReceiverReaderInterface import SourceReceiverReaderInterface
from Algorithm.method.model.AlgorithmObject import AlgorithmCalibrationProgressObject, AlgorithmResultObject


class ProxyInterface(ABC):

    @abstractmethod
    def get_source(self, source_label: str) -> SourceReceiverReaderInterface:
        pass

    @abstractmethod
    async def report(self, algorithm_result_object: AlgorithmResultObject):
        pass

    @abstractmethod
    def get_algorithm_config(self) -> dict[str, Union[str, dict]]:
        pass

    @abstractmethod
    def set_algorithm_config(self, algorithm_config_dict: dict[str, Union[str, dict]]):
        pass

    @abstractmethod
    def set_source_required_channel_labels(self, source_label: str, channel_label_list: list[str]):
        pass

    @abstractmethod
    async def report_calibration_progress(
        self,
        calibration_progress_object: AlgorithmCalibrationProgressObject,
    ):
        pass
