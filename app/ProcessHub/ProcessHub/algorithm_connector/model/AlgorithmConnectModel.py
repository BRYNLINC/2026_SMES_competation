from dataclasses import dataclass


@dataclass
class AlgorithmConnectModel:
    address: str = None # 算法服务器地址，包含ip和端口，如：127.0.0.1:50051
    max_time_out: float = None
