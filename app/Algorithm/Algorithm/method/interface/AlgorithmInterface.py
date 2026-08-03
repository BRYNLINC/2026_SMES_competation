from abc import abstractmethod, ABC

from Algorithm.method.interface.ProxyInterface import ProxyInterface
from Algorithm.method.model.AlgorithmObject import CalibrationStageResultObject
import numpy as np


class AlgorithmInterface(ABC):

    def __init__(self):
        self._proxy: ProxyInterface = None
        self._end_flag = False

    @abstractmethod
    async def calibrate(self) -> CalibrationStageResultObject:
        """
        显式执行一个校准阶段。
        返回当前阶段的结构化结果：
        1. stage_context is None 且 calibration_ready=False 表示流程结束；
        2. stage_context 非空 且 calibration_ready=True 表示当前阶段可进入 online。
        """
        pass

    @abstractmethod
    async def run(self) -> bool:
        """
        执行当前已完成校准的online阶段。
        这是框架内部包装层，通常不建议参赛选手直接修改；
        选手真正应当实现的在线推理入口是 predict()。
        返回 True 表示后续可能仍有下一阶段，返回 False 表示数据源已结束。
        """
        pass

    @abstractmethod
    def predict(self, trial_data: np.ndarray) -> str:
        """
        对单个完整 trial 执行预测并返回结果字符串。
        推荐返回 JSON 字符串，如 {"predict_label": 0}。
        """
        pass

    def set_proxy(self, proxy: ProxyInterface):
        self._proxy = proxy

    def set_end_flag(self, end_flag: bool):
        self._end_flag = end_flag

    @abstractmethod
    def get_required_channel_labels(self) -> dict[str, list[str]]:
        """
        返回算法需要的通道列表，key为source_label，value为按目标顺序排列的通道名列表。
        算法必须显式声明所需通道，框架会据此做通道校验与重排，避免默认暴露全部通道。
        """
        pass
