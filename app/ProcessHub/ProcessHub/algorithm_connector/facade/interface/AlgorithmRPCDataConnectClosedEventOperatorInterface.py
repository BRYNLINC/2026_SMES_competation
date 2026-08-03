from abc import abstractmethod, ABC


class AlgorithmRPCDataConnectClosedEventOperatorInterface(ABC):
    """
    AlgorithmRPCDataConnectClosedEventOperatorInterface
    """

    @abstractmethod
    async def on_closed(self, disconnect_reason: str = 'unknown') -> None:
        """
        触发关闭事件时执行的处理回调方法
        """
        pass
