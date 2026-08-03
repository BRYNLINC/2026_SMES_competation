import asyncio
import contextlib
import logging
from asyncio import Queue, Task, Event, Lock

from grpc import RpcError
from grpc._cython.cygrpc import UsageError
from injector import inject

from Algorithm.api.converter.AlgorithmRPCMessageConverter import AlgorithmRPCMessageConverter
from Algorithm.api.proto.AlgorithmRPCService_pb2 import AlgorithmDataMessage as AlgorithmDataMessage_pb2
from ProcessHub.algorithm_connector.exception.ProcessHubAlgorithmConnectorException import \
    ProcessHubAlgorithmConnectorClosedException
from ProcessHub.algorithm_connector.facade.interface.AlgorithmRPCDataConnectClosedEventOperatorInterface import \
    AlgorithmRPCDataConnectClosedEventOperatorInterface
from ProcessHub.common.enum.ServiceStatusEnum import ServiceStatusEnum
from ProcessHub.algorithm_connector.interface.AlgorithmConnectorInterface import \
    ReceiveAlgorithmReportMessageOperatorInterface
from Algorithm.api.proto.AlgorithmRPCService_pb2_grpc import AlgorithmRPCDataConnectStub


class AlgorithmRPCDataConnectClient:
    @inject
    def __init__(self):
        self.__receive_report_operator: ReceiveAlgorithmReportMessageOperatorInterface = None
        self.__connect_closed_event_operator: AlgorithmRPCDataConnectClosedEventOperatorInterface = None
        self.__data_message_queue: Queue[AlgorithmDataMessage_pb2] = Queue()
        self.__algorithm_rpc_data_connect_stub: AlgorithmRPCDataConnectStub = None
        self.__send_end_flag = True
        # 仅依赖 service_status 会把“RPC channel 还在、双向数据流已被远端关闭”
        # 误判为在线。该标志在流的 finally 中立即置位，供握手和发送路径做健康检查。
        self.__transport_closed = True
        self.__receiver_status: ServiceStatusEnum = ServiceStatusEnum.STOPPED
        self.__sender_status: ServiceStatusEnum = ServiceStatusEnum.STOPPED
        self.__disconnect_event = Event()
        self.__logger = logging.getLogger("processHubLogger")
        self.__receive_report_task: Task = None
        self.__algorithm_rpc_message_converter: AlgorithmRPCMessageConverter = AlgorithmRPCMessageConverter()
        self.__lifecycle_lock = Lock()
        self.__session_generation = 0
        self.__active_session_generation = 0
        self.__disconnecting_session_generation = 0
        self.__disconnect_completion_event_by_session_generation: dict[int, Event] = {}
        self.__local_shutdown_session_generation_set: set[int] = set()

    async def connect(self):
        async with self.__lifecycle_lock:
            if self.__active_session_generation != 0:
                raise ProcessHubAlgorithmConnectorClosedException(
                    f"已有算法数据连接尚未完成清理: generation={self.__active_session_generation}"
                )
            self.__logger.info("发起数据连接")
            self.__reset_transport_state(reason='before_connect')
            self.__session_generation += 1
            session_generation = self.__session_generation
            self.__active_session_generation = session_generation
            session_queue = self.__data_message_queue
            session_disconnect_event = self.__disconnect_event
            self.__send_end_flag = False
            self.__transport_closed = False
            self.__receiver_status = ServiceStatusEnum.STARTING
            self.__sender_status = ServiceStatusEnum.STARTING

            async def connect_request_generator():
                if self.__active_session_generation == session_generation:
                    self.__sender_status = ServiceStatusEnum.RUNNING
                try:
                    while True:
                        message = await session_queue.get()
                        if (
                            self.__active_session_generation != session_generation
                            or (self.__send_end_flag and session_queue.qsize() == 0)
                        ):
                            session_disconnect_event.set()
                            break
                        self.__logger.debug(
                            f"send data {message.sourceLabel}-{type(message).__name__}-{message.WhichOneof('package')}")
                        yield message
                        session_queue.task_done()
                except asyncio.CancelledError:
                    self.__logger.info("数据发送任务被取消: generation=%s", session_generation)
                except Exception as e:
                    self.__logger.exception(f"数据接收连接异常：{e}")
                    raise e
                finally:
                    self.__logger.info("数据发送连接已断开: generation=%s", session_generation)
                    session_disconnect_event.set()
                    if self.__active_session_generation == session_generation:
                        self.__transport_closed = True
                    if self.__active_session_generation == session_generation:
                        self.__sender_status = ServiceStatusEnum.STOPPED
                    if self.__disconnecting_session_generation != session_generation:
                        await self.__disconnect_process(
                            disconnect_reason='request_stream_closed',
                            session_generation=session_generation,
                        )

            try:
                report_iterator = self.__algorithm_rpc_data_connect_stub.connect(connect_request_generator())
                self.__receive_report_task = asyncio.create_task(
                    self.__receive_report_function(report_iterator, session_generation)
                )
            except Exception:
                self.__active_session_generation = 0
                self.__reset_transport_state(reason=f'connect_failed:{session_generation}')
                raise
            self.__logger.info("启动结果接收任务: generation=%s", session_generation)

    async def disconnect(self):
        self.__logger.info("断开数据连接，原因=local_shutdown")
        session_generation = self.__active_session_generation
        if session_generation == 0:
            self.__logger.info("数据连接已处于断开状态")
            return
        self.__local_shutdown_session_generation_set.add(session_generation)
        try:
            await self.__disconnect_process(
                disconnect_reason='local_shutdown',
                session_generation=session_generation,
            )
        finally:
            self.__local_shutdown_session_generation_set.discard(session_generation)
        self.__logger.info("数据连接已断开")

    def add_receive_report_operator(self, receive_report_operator: ReceiveAlgorithmReportMessageOperatorInterface):
        self.__receive_report_operator = receive_report_operator

    def add_connect_closed_event_operator(self,
                                          connect_closed_event_operator:
                                          AlgorithmRPCDataConnectClosedEventOperatorInterface):
        self.__connect_closed_event_operator = connect_closed_event_operator

    def is_transport_active(self) -> bool:
        """Return whether the current bidirectional data stream is still usable.

        The control RPC channel can remain healthy after the data stream has been
        reset by the algorithm host. Callers use this check after the startup
        handshake and before forwarding the first payload of a stage.
        """
        receive_task = self.__receive_report_task
        return (
            self.__active_session_generation != 0
            and not self.__send_end_flag
            and not self.__transport_closed
            and receive_task is not None
            and not receive_task.done()
        )

    async def send_data(self, algorithm_data_message: AlgorithmDataMessage_pb2):
        if not self.is_transport_active():
            raise ProcessHubAlgorithmConnectorClosedException("发送数据时，连接已关闭")
        else:
            await self.__data_message_queue.put(algorithm_data_message)
            self.__logger.debug(f"{algorithm_data_message.sourceLabel}"
                                f"数据写入发送队列:{algorithm_data_message.WhichOneof('package')}")

    async def __receive_report_function(self, request_iterator, session_generation: int):
        if self.__active_session_generation != session_generation:
            return
        self.__receiver_status = ServiceStatusEnum.RUNNING
        report_stream_closed_normally = False
        close_callback_notified = False
        disconnect_reason = 'report_stream_closed'
        try:
            async for algorithm_report_message in request_iterator:
                await self.__receive_report_operator.receive_report(
                    self.__algorithm_rpc_message_converter.protobuf_to_model(algorithm_report_message)
                )
            report_stream_closed_normally = True
            disconnect_reason = 'report_stream_eof'
        except asyncio.CancelledError:
            disconnect_reason = 'report_stream_cancelled'
            self.__logger.info("结果接收任务取消")
        except UsageError:
            disconnect_reason = 'report_stream_usage_error'
            self.__logger.info(f"赛题端结果接收流已经关闭")
        except RpcError as rpc_error:  # 如果接收器出现异常，则关闭接收器
            disconnect_reason = self._format_rpc_disconnect_reason(rpc_error)
            self.__logger.exception(f"数据结果接收出现异常，关闭接收器{rpc_error}")
        except Exception as e:
            disconnect_reason = f'report_stream_exception:{type(e).__name__}'
            self.__logger.exception(f"数据结果接收连接异常：{e}")
            raise e
        finally:
            if self.__active_session_generation == session_generation:
                self.__transport_closed = True
            # self.__receiver_status = ServiceStatusEnum.STOPPED
            if report_stream_closed_normally:
                # 修改说明：
                # 这里单独补一条“结果流正常 EOF”日志，便于和 RpcError / 10054 这类异常断开区分。
                # 后续你排查实验是否正常结束时，只要看到这条，就能确定：
                # 1. 算法端结果流是按协议正常收尾的；
                # 2. 当前轮次不是因为连接异常被动断开的。
                self.__logger.info("接收结果报告流正常结束，原因=report_stream_eof")
            self.__logger.info(f"接收结果报告连接结束")
            is_active_session = self.__active_session_generation == session_generation
            is_local_shutdown = session_generation in self.__local_shutdown_session_generation_set
            if is_active_session and not is_local_shutdown and self.__connect_closed_event_operator is not None:
                try:
                    self.__logger.info("结果流关闭，先行上报断连事件: reason=%s", disconnect_reason)
                    await self.__connect_closed_event_operator.on_closed(disconnect_reason)
                    close_callback_notified = True
                except Exception:
                    self.__logger.exception("结果流关闭的提前断连回调失败")
            # 执行断开事件处理
            try:
                if self.__disconnecting_session_generation != session_generation:
                    await self.__disconnect_process(
                        disconnect_reason=disconnect_reason,
                        close_callback_notified=close_callback_notified,
                        session_generation=session_generation,
                    )
            except Exception:
                self.__logger.exception("report stream close callback failed")

    @staticmethod
    def _format_rpc_disconnect_reason(rpc_error: RpcError) -> str:
        try:
            status_code = rpc_error.code()
        except Exception:
            status_code = None
        code_name = getattr(status_code, 'name', None)
        if not code_name:
            code_name = str(status_code or 'unknown').rsplit('.', 1)[-1]
        try:
            detail = rpc_error.details()
        except Exception:
            detail = None
        normalized_detail = ' '.join(str(detail or '').split())
        reason = f"grpc_{str(code_name).strip().lower()}"
        if normalized_detail:
            reason = f"{reason}: {normalized_detail}"
        return reason[:512]

    def set_algorithm_rpc_data_connect_stub(self, algorithm_rpc_data_connect_stub: AlgorithmRPCDataConnectStub):
        self.__algorithm_rpc_data_connect_stub = algorithm_rpc_data_connect_stub

    async def __disconnect_process(
        self,
        disconnect_reason: str = 'unknown',
        close_callback_notified: bool = False,
        session_generation: int | None = None,
    ):
        current_task = asyncio.current_task()
        completion_event: Event | None = None
        should_notify_closed = False
        owns_cleanup = False
        async with self.__lifecycle_lock:
            target_generation = session_generation or self.__active_session_generation
            if target_generation == 0 or target_generation != self.__active_session_generation:
                self.__logger.info(
                    "忽略过期算法数据连接清理: reason=%s target_generation=%s active_generation=%s",
                    disconnect_reason,
                    target_generation,
                    self.__active_session_generation,
                )
                return
            if self.__disconnecting_session_generation == target_generation:
                if current_task is self.__receive_report_task:
                    return
                completion_event = self.__disconnect_completion_event_by_session_generation.get(
                    target_generation
                )
            else:
                owns_cleanup = True
                self.__disconnecting_session_generation = target_generation
                completion_event = Event()
                self.__disconnect_completion_event_by_session_generation[target_generation] = completion_event
                should_notify_closed = (
                    self.__receiver_status is ServiceStatusEnum.RUNNING
                    and self.__sender_status is ServiceStatusEnum.RUNNING
                    and self.__connect_closed_event_operator is not None
                    and not close_callback_notified
                    and target_generation not in self.__local_shutdown_session_generation_set
                )

        if not owns_cleanup:
            if completion_event is not None:
                await completion_event.wait()
            return

        self.__logger.info(
            "执行断开事件处理流程: reason=%s generation=%s receiver_status=%s "
            "sender_status=%s event_operator=%s close_callback_notified=%s",
            disconnect_reason,
            target_generation,
            self.__receiver_status,
            self.__sender_status,
            self.__connect_closed_event_operator,
            close_callback_notified,
        )
        try:
            await self.__stop_sender_process()
            await self.__stop_receiver_process(disconnect_reason=disconnect_reason)
            if should_notify_closed:
                self.__logger.info("执行算法数据连接关闭回调: reason=%s", disconnect_reason)
                await self.__connect_closed_event_operator.on_closed(disconnect_reason)
        finally:
            async with self.__lifecycle_lock:
                if self.__active_session_generation == target_generation:
                    self.__reset_transport_state(reason=f'after_disconnect:{disconnect_reason}')
                    self.__active_session_generation = 0
                self.__local_shutdown_session_generation_set.discard(target_generation)
                if self.__disconnecting_session_generation == target_generation:
                    self.__disconnecting_session_generation = 0
                completion_event = self.__disconnect_completion_event_by_session_generation.pop(
                    target_generation,
                    completion_event,
                )
                if completion_event is not None:
                    completion_event.set()

    async def __stop_sender_process(self):
        if self.__sender_status is not ServiceStatusEnum.RUNNING:
            return
        self.__sender_status = ServiceStatusEnum.STOPPING
        self.__logger.info("开始断开发送数据连接")
        # 置为结束标志位，并且额外生成一个数据包以触发结束操作
        self.__send_end_flag = True
        stop_response = AlgorithmDataMessage_pb2()
        self.__disconnect_event.clear()
        await self.__data_message_queue.put(stop_response)
        # 有界等待发送生成器消费结束包，避免远端异常时裁判进程永久卡住。
        try:
            await asyncio.wait_for(self.__disconnect_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            self.__logger.warning("等待数据发送流结束超时，继续关闭接收侧")
        self.__sender_status = ServiceStatusEnum.STOPPED

    async def __stop_receiver_process(self, disconnect_reason: str = 'unknown'):
        if self.__receiver_status is not ServiceStatusEnum.RUNNING:
            return
        self.__receiver_status = ServiceStatusEnum.STOPPING
        # 等待接收数据任务或超时任务完成，先等待接收数据任务
        if not self.__receive_report_task.done():
            current_task = asyncio.current_task()
            if self.__receive_report_task is current_task:
                self.__logger.info(
                    "结果接收流在当前协程内关闭，直接完成接收侧收尾，避免自等待: reason=%s",
                    disconnect_reason,
                )
                self.__receive_report_task = None
                self.__receiver_status = ServiceStatusEnum.STOPPED
                return
            # 创建一个未来的事件，等待20秒，如果20秒内数据接收停止，则正常退出，反之则强制取消数据接收任务
            timeout_task = asyncio.create_task(asyncio.sleep(20))
            done, pending = await asyncio.wait({self.__receive_report_task, timeout_task},
                                               return_when=asyncio.FIRST_COMPLETED)
            # 如果接收数据任务已经完成，就不再执行其他操作
            if self.__receive_report_task in done:
                self.__logger.info("数据接收任务已正常结束")
            else:
                # 如果超时任务先完成，说明接收数据任务需要被取消
                self.__logger.warning("数据接收任务超时，取消任务")
                self.__receive_report_task.cancel()
                # 等待接收数据任务被取消
                with contextlib.suppress(asyncio.CancelledError):
                    await self.__receive_report_task
            # 清理
            for task in pending:
                task.cancel()
        self.__receive_report_task = None
        self.__receiver_status = ServiceStatusEnum.STOPPED

    def __reset_transport_state(self, reason: str) -> None:
        pending_message_count = self.__data_message_queue.qsize()
        if pending_message_count > 0:
            self.__logger.warning(
                "重置算法数据连接客户端传输状态，丢弃遗留发送消息: reason=%s pending_message_count=%s",
                reason,
                pending_message_count,
            )
        self.__data_message_queue = Queue()
        self.__disconnect_event = Event()
        self.__send_end_flag = True
        self.__transport_closed = True
        self.__receiver_status = ServiceStatusEnum.STOPPED
        self.__sender_status = ServiceStatusEnum.STOPPED
        self.__receive_report_task = None
