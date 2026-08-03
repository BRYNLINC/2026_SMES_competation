import asyncio
import logging
import time
from typing import Union, Callable, Awaitable

from grpc.aio import AioRpcError

from Algorithm.api.converter.AlgorithmRPCMessageConverter import AlgorithmRPCMessageConverter
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel
from Algorithm.api.proto.AlgorithmRPCService_pb2_grpc import AlgorithmRPCDataConnectStub, AlgorithmRPCServiceControlStub
from ProcessHub.algorithm_connector.exception.ProcessHubAlgorithmConnectorException import \
    ProcessHubAlgorithmConnectorClosedException, ProcessHubAlgorithmConnectorTimeoutException
from ProcessHub.algorithm_connector.facade.interface.AlgorithmRPCDataConnectClosedEventOperatorInterface import \
    AlgorithmRPCDataConnectClosedEventOperatorInterface
from ProcessHub.common.enum.ServiceStatusEnum import ServiceStatusEnum
from ProcessHub.algorithm_connector.facade.AlgorithmRPCDataConnectClient import AlgorithmRPCDataConnectClient
from ProcessHub.algorithm_connector.facade.AlgorithmRPCServiceControlClient import AlgorithmRPCServiceControlClient
from ProcessHub.algorithm_connector.facade.GrpcClient import GrpcClient
from ProcessHub.algorithm_connector.interface.AlgorithmConnectorInterface import AlgorithmConnectorInterface, \
    ReceiveAlgorithmReportMessageOperatorInterface, DataConnectClosedEventOperatorInterface
from ProcessHub.algorithm_connector.model.AlgorithmConnectModel import AlgorithmConnectModel


class AlgorithmConnector(AlgorithmConnectorInterface):
    """
    RPC控制器
    被调用执行次序
    1、初始化实例__init__()
    2、注入接收结果处理器 set_receive_report_operator()
    3、注入关闭事件回调处理器 set_closed_event_operator()：可选
    4、注入算法断开事件处理器set_algorithm_disconnect_event_operator
    5、设置算法地址set_algorithm_address()
    6、启动startup()
    7、随时发送配置信息send_config()
    8、获取配置信息get_config():获取包括数据源的配置信息
    9、开始发送数据send_data()
    10、关闭shutdown()
    11、关闭远程算法系统remote_close_algorithm_system()：可选
    """

    def __init__(self,
                 algorithm_rpc_data_connect_client: AlgorithmRPCDataConnectClient,
                 algorithm_rpc_service_control_client: AlgorithmRPCServiceControlClient):
        self.__algorithm_rpc_data_connect_client = algorithm_rpc_data_connect_client
        self.__algorithm_rpc_service_control_client = algorithm_rpc_service_control_client
        self.__rpc_client: GrpcClient = None
        self.__logger = logging.getLogger("processHubLogger")
        self.__receive_report_operator: ReceiveAlgorithmReportMessageOperatorInterface = None
        self.__data_connect_closed_event_operator: DataConnectClosedEventOperatorInterface = None
        self.__message_converter = AlgorithmRPCMessageConverter()
        # service_status说明：RPC连接断开为STOPPED，RPC连接成功后但尚未启动数据连接为READY，数据连接启动为RUNNING，
        # 数据连接断开,但RPC连接保留为READY
        self.__service_status: ServiceStatusEnum = ServiceStatusEnum.STOPPED
        self.__algorithm_address: str = None
        self.__max_connection_timeout: float = None

    def set_receive_report_operator(self, receive_report_operator: ReceiveAlgorithmReportMessageOperatorInterface):
        self.__receive_report_operator = receive_report_operator

    def set_data_connect_closed_event_operator(
            self, data_connect_closed_event_operator: DataConnectClosedEventOperatorInterface):
        self.__data_connect_closed_event_operator = data_connect_closed_event_operator

    async def send_data(self, algorithm_data_message_model: AlgorithmDataMessageModel):
        await self.__algorithm_rpc_data_connect_client.send_data(
            self.__message_converter.model_to_protobuf(
                algorithm_data_message_model
            )
        )

    def is_transport_active(self) -> bool:
        return (
            self.__service_status is ServiceStatusEnum.RUNNING
            and self.__algorithm_rpc_data_connect_client.is_transport_active()
        )

    async def push_algorithm_config(self, config_dict: dict[str, Union[str, dict]]):
        await self.__algorithm_rpc_service_control_client.send_config(config_dict)

    async def pull_algorithm_config(self) -> dict[str, Union[str, dict]]:
        """
        拉取算法端配置信息
        """
        return await self.__algorithm_rpc_service_control_client.get_config()

    def get_algorithm_address(self) -> str:
        return self.__algorithm_address

    def get_max_connection_timeout(self) -> float:
        return self.__max_connection_timeout

    async def initial(self, algorithm_connect_model: AlgorithmConnectModel):
        self.__algorithm_address = algorithm_connect_model.address
        self.__max_connection_timeout = algorithm_connect_model.max_time_out

    async def startup(self):
        try:
            if self.__service_status not in [ServiceStatusEnum.STOPPED, ServiceStatusEnum.ERROR]:
                return
            self.__service_status = ServiceStatusEnum.STARTING

            self.__rpc_client = GrpcClient(self.__algorithm_address)

            # 先启动连接
            await self.__rpc_client.startup()

            # 再绑定并注入服务
            self.__algorithm_rpc_service_control_client.set_algorithm_rpc_service_control_stub(
                self.__rpc_client.get_stub_instance(AlgorithmRPCServiceControlStub)
            )

            self.__algorithm_rpc_data_connect_client.set_algorithm_rpc_data_connect_stub(
                self.__rpc_client.get_stub_instance(AlgorithmRPCDataConnectStub)
            )
            # 注入接收报告处理器
            self.__algorithm_rpc_data_connect_client.add_receive_report_operator(self.__receive_report_operator)

            class AlgorithmRPCDataConnectClosedEventOperator(AlgorithmRPCDataConnectClosedEventOperatorInterface):
                def __init__(self, outer: AlgorithmConnector):
                    self.__outer = outer

                async def on_closed(self, disconnect_reason: str = 'unknown') -> None:
                    await self.__outer.on_data_connect_closed(disconnect_reason=disconnect_reason)

            # 注入关闭事件处理器
            self.__algorithm_rpc_data_connect_client.add_connect_closed_event_operator(
                AlgorithmRPCDataConnectClosedEventOperator(self)
            )

            # 等待连接建立
            await self.__wait_for_connect()
            self.__logger.info(f"RPC连接建立，建立{self.__algorithm_address}算法端服务器数据连接")
            self.__service_status = ServiceStatusEnum.READY
        except Exception as e:
            self.__service_status = ServiceStatusEnum.ERROR
            if self.__rpc_client is not None:
                try:
                    await self.__rpc_client.shutdown()
                except Exception:
                    self.__logger.exception(f"关闭{self.__algorithm_address}失败的RPC连接时发生异常")
                finally:
                    self.__rpc_client = None
            raise e

    async def data_connect(self):
        if self.__service_status is not ServiceStatusEnum.READY:
            raise ProcessHubAlgorithmConnectorClosedException(
                f"cannot open algorithm data stream from state={self.__service_status}"
            )
        self.__logger.info(f"启动{self.__algorithm_address}算法端服务器数据连接")
        # 建立数据连接
        await self.__algorithm_rpc_data_connect_client.connect()

        self.__service_status = ServiceStatusEnum.RUNNING

    async def data_disconnect(self):
        if self.__service_status is not ServiceStatusEnum.RUNNING:
            return
        await self.__algorithm_rpc_data_connect_client.disconnect()
        self.__logger.info(f"停止{self.__algorithm_address}算法端服务器数据连接")
        self.__service_status = ServiceStatusEnum.READY

    async def shutdown(self):
        if self.__service_status not in [ServiceStatusEnum.RUNNING, ServiceStatusEnum.READY]:
            return
        previous_service_status = self.__service_status
        self.__service_status = ServiceStatusEnum.STOPPING
        try:
            if previous_service_status is ServiceStatusEnum.RUNNING:
                await self.__algorithm_rpc_data_connect_client.disconnect()
            if self.__rpc_client is not None:
                await self.__rpc_client.shutdown()
            self.__logger.info(f"RPC控制器关闭，已断开与{self.__algorithm_address}算法端服务器RPC连接")
        finally:
            self.__rpc_client = None
            self.__service_status = ServiceStatusEnum.STOPPED

    async def shutdown_and_close_algorithm_system(self) -> None:
        if self.__service_status not in [ServiceStatusEnum.RUNNING, ServiceStatusEnum.READY]:
            return
        self.__service_status = ServiceStatusEnum.STOPPING
        # 发送算法系统关闭请求
        self.__logger.info(f"向算法服务器{self.__algorithm_address}发送系统关闭请求")
        await self.__algorithm_rpc_service_control_client.shutdown()
        # 断开数据连接
        await self.__rpc_client.shutdown()
        self.__logger.info(f"RPC控制器关闭，已断开与{self.__algorithm_address}算法端服务器RPC连接")
        self.__service_status = ServiceStatusEnum.STOPPED

    async def __wait_for_connect(self):
        self.__logger.info(f"启动{self.__algorithm_address}算法端连接，最长等待时间{self.__max_connection_timeout}秒...")
        start_time = time.time()
        while True:
            try:
                service_status = await self.__algorithm_rpc_service_control_client.get_status()
                break
            except AioRpcError as e:
                if time.time() - start_time > self.__max_connection_timeout:
                    raise ProcessHubAlgorithmConnectorTimeoutException(
                        f"{self.__algorithm_address}算法端连接超时，请检查算法端是否正常运行"
                    ) from e
                await asyncio.sleep(1)
        self.__logger.info(f"{self.__algorithm_address}算法端连接成功")

    async def on_data_connect_closed(self, disconnect_reason: str = 'unknown'):
        # 执行数据连接关闭事件回调
        if self.__data_connect_closed_event_operator is not None:
            self.__logger.info(
                f"开始执行{self.__algorithm_address}算法端服务器数据连接关闭回调方法, reason={disconnect_reason}"
            )
            await self.__data_connect_closed_event_operator.on_closed(disconnect_reason=disconnect_reason)
        if self.__service_status is ServiceStatusEnum.RUNNING:
            self.__service_status = ServiceStatusEnum.READY
        self.__logger.info(
            f"已完成{self.__algorithm_address}算法端服务器数据关闭回调方法，RPC连接未关闭, reason={disconnect_reason}"
        )
