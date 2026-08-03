from dataclasses import dataclass


@dataclass
class DataFileModel:
    subject_id: str = None # 被试名称
    exp_name: str = None  # 实验名称
    exp_task: str = None  # 实验任务类型，例如 left_vs_rest / right_vs_rest
    session_id: str = None  # session标识，例如 session1 / session2
    file_path: str = None # 路径
