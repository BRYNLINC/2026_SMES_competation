from abc import ABC, abstractmethod

from Algorithm.method.model.AlgorithmObject import (
    AlgorithmCalibrationObject,
    AlgorithmContinuousDataObject,
    AlgorithmDeviceObject,
)


class SourceReceiverReaderInterface(ABC):
    """
    源接收器读取接口
    """

    @abstractmethod
    def get_source_label(self) -> str:
        """
        获取源标签
        :return:
        """
        pass

    @abstractmethod
    async def get_data(self) -> AlgorithmContinuousDataObject:
        """
        获取数据
        :return:
        """
        pass

    @abstractmethod
    async def get_device(self) -> AlgorithmDeviceObject:
        """
        获取设备信息
        :return:
        """
        pass

    @abstractmethod
    async def get_calibration(self) -> AlgorithmCalibrationObject:
        """
        获取校准数据
        :return:
        """
        pass

    @abstractmethod
    def set_required_channel_labels(self, channel_label_list: list[str]):
        """
        设置算法需要的通道列表，数据源应按该顺序输出数据。
        :param channel_label_list: 目标通道名列表
        :return:
        """
        pass
