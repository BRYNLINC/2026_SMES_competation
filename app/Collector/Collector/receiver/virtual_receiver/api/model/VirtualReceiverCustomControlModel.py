from dataclasses import dataclass
from typing import Union

from Common.model.CommonMessageModel import InformationPackageModel


@dataclass
class CalibrationTrialCountControlPackageModel:
    # 必须显式携带 team_id。
    # 这样 Collector 才能把“每队申请的校准trial数量”写入自己的 team->count 映射，
    # 避免再通过 topic / component_id 做推导。
    team_id: str = None
    calibration_trial_count_per_class: int = None


@dataclass
class VirtualReceiverCustomControlModel:
    package: Union[
        InformationPackageModel,
        CalibrationTrialCountControlPackageModel,
    ] = None

