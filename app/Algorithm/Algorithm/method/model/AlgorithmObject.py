from dataclasses import dataclass, field
from typing import Union, List
from numpy import ndarray


@dataclass
class AlgorithmContinuousDataObject:
    # 数据对象
    start_position: int = None  # 数据包内数据的起始位置
    data: ndarray = None    # 数据内容，每行表示一个导联，最后一行通道为trigger通道
    subject_id: str = None  # 当前数据包的subject_id
    other_information: dict = field(default_factory=dict)  # 当前数据包对应的阶段信息
    finish_flag: bool = None    # 数据结束标志位，当该标志位为true时，表示该数据源所有数据已经发送完毕


@dataclass
class AlgorithmDeviceObject:
    data_type: str = None   # 数据类型，目前可支持 UNKNOWN,EEG,EYETRACKING,MEG,MRI,ECOG,SPIKE,EMG,ECG,NIRS
    channel_number: int = None  # 当前数据包的通道数
    sample_rate: float = None   # 采样率
    channel_label: List[str] = None  # 通道标签
    other_information: dict = None  # 其他配置信息


@dataclass
class AlgorithmCalibrationObject:
    subject_id: str = None
    exp_name: str = None
    exp_task: str = None
    session_id: str = None
    session_data_dict: dict[str, dict[str, ndarray]] = field(default_factory=dict)
    finish_flag: bool = None
    # Stable seed selected by the framework for this stage.
    stage_seed: int = None


@dataclass
class StageContextObject:
    # 统一描述“当前阶段是谁、属于哪个session”。
    # 这个结构会被 MethodManager 序列化后发往 ProcessHub，
    # 再由 ProcessHub / RuntimeStageCoordinator 继续透传和聚合。
    subject_id: str = None
    exp_name: str = None
    exp_task: str = None
    session_id: str = None


@dataclass
class CalibrationStageResultObject:
    # calibrate() 的结构化返回值。
    # calibration_ready=True 表示当前 stage 的校准已结束，可以开始 online；
    # stage_context=None 且 calibration_ready=False 表示整个数据源已经没有后续阶段。
    stage_context: Union[StageContextObject, None] = None
    calibration_ready: bool = None


@dataclass
class AlgorithmCalibrationProgressObject:
    subject_id: str = None
    exp_name: str = None
    exp_task: str = None
    session_id: str = None
    status: str = None
    progress: float = None
    message: str = None
    current_epoch: int = None
    total_epoch: int = None


@dataclass
class AlgorithmResultObject:
    # 可支持字符串和二进制数据，如果为二进制数据，则可传输图片等数据，需在接收端或赛题端进行解码
    result: Union[None, bool, str, bytes, list[float], list[int], list[str]] = None

