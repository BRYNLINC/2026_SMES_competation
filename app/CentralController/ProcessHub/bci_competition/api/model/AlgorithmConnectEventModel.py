from dataclasses import dataclass
from typing import Union


@dataclass
class AlgorithmConnectClosedEventModel:
    address: str = None


@dataclass
class AlgorithmConnectEventModel:
    package: Union[
        AlgorithmConnectClosedEventModel,
    ] = None
