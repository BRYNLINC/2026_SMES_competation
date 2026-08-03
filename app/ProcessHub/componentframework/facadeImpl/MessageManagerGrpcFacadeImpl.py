import asyncio
import logging
import grpc
from injector import inject
from asyncio import Queue
from componentframework.api.model.MessageModel import MessageModel
from componentframework.facadeImpl.grpc_connector import GrpcConnector
from componentframework.facade.RemoteProcedureCallFacade import RemoteProcedureCallFacade
from componentframework.facadeImpl.test_grpc import MessageManager_pb2, MessageManager_pb2_grpc
from componentframework.api.Enum.StatusEnum import StatusEnum
from componentframework.api.model.MessageOperateModel import AddListenerOnBindMessageModel


class MessageManagerGrpcFacadeImpl(RemoteProcedureCallFacade):
    @inject
    def __init__(self, grpc_connector_forwarder: GrpcConnector):
        super().__init__()
        self.send_stream = None
        self.component_pattern = None
        self.__report_message_queue = Queue()
        self.add_listener_on_bind_message_model = None
        self.stub = None
        self.__grpc_connector_forwarder = grpc_connector_forwarder
        self.service_id = None
        self.__logger = logging.getLogger("processHubLogger")
        self.__send_stream_lock = asyncio.Lock()
        self.__control_send_stream_lock = asyncio.Lock()
        self.__control_stream_message_key_set = {
            "runtime_stage_event",
            "runtime_stage_control",
            "runtime_stage_ui_control",
            "virtual_receiver_custom_control",
        }
        # 关键控制消息需要和共享 channel 完全隔离，否则前一条未结束的 stream-unary RPC
        # 仍可能占住底层连接状态，导致后一条 timeout 终态消息 write/done_writing 卡死。
        self.__single_shot_control_channel_options = [
            ('grpc.max_receive_message_length', 1089600010),
        ]
        self.__send_message_retry_limit = 2
        self.__send_message_write_timeout_seconds = 1.5
        self.__single_shot_control_response_timeout_seconds = 5.0
        self.__subscribe_retry_delay_seconds = 1.0

    async def bind_message(self, message_model: MessageModel):
        request = MessageManager_pb2.BindMessageRequest(
            messageKey=message_model.message_key,
            serviceID=message_model.component_id,
            topic=message_model.topic,
            componentPattern=self.component_pattern,
        )
        response = await self.stub.BindMessage(request)
        bind_message_model = MessageModel()
        bind_message_model.message_key = response.messageKey
        bind_message_model.service_id = response.serviceID
        bind_message_model.topic = response.topic
        self.service_id = response.serviceID
        return bind_message_model

    async def add_listener_on_bind_message(self, callback) -> None:
        request = MessageManager_pb2.AddListenerOnBindMessageRequest(request='request')
        subscribe_topic_response_stream = self.stub.AddListenerOnBindMessage(request)
        async for response in subscribe_topic_response_stream:
            print(asyncio.all_tasks())
            print(response)
            self.add_listener_on_bind_message_model = AddListenerOnBindMessageModel()
            self.add_listener_on_bind_message_model.message_key = response.messageKey
            self.add_listener_on_bind_message_model.component_id = response.serviceID
            self.add_listener_on_bind_message_model.topic = response.topic
            confirm_response = await callback.run(self.add_listener_on_bind_message_model)
            confirm_request = MessageManager_pb2.ConfirmBindMessageRequest(
                messageKey=confirm_response.message_key,
                serviceID=confirm_response.component_id,
                topic=confirm_response.topic,
            )
            self.stub.ConfirmBindMessage(confirm_request)
            await asyncio.sleep(0)

    async def get_topic_by_message_key(self, message_key: str, component_id: str = None) -> str:
        request = MessageManager_pb2.GetTopicByMessageKeyRequest(
            messageKey=message_key,
            serviceID=component_id,
        )
        response = await self.stub.GetTopicByMessageKey(request)
        return response.topic

    async def subscribe_topic(self, callback, message_key):
        # 9003 偶发重置时，订阅流不能直接退出，否则 ProcessHub 会永久丢失后续 trial/control 数据。
        # 这里改为在 gRPC 订阅流异常结束后自动重连，保证 runtime/event/data 订阅具备自恢复能力。
        while True:
            request = MessageManager_pb2.SubscribeTopicRequest(messageKey=message_key)
            try:
                subscribe_topic_response_stream = self.stub.SubscribeTopic(request)
                async for response in subscribe_topic_response_stream:
                    try:
                        await callback.run(response.response)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.__logger.exception(
                            "订阅消息回调处理失败，忽略该条消息并继续监听: message_key=%s",
                            message_key,
                        )
                self.__logger.warning(
                    "订阅流已结束，准备重连: message_key=%s retry_after_seconds=%s",
                    message_key,
                    self.__subscribe_retry_delay_seconds,
                )
            except asyncio.CancelledError:
                raise
            except grpc.aio.AioRpcError as exc:
                self.__logger.warning(
                    "订阅流异常断开，准备重连: message_key=%s retry_after_seconds=%s error=%s",
                    message_key,
                    self.__subscribe_retry_delay_seconds,
                    exc,
                )
            except Exception:
                self.__logger.exception(
                    "订阅流处理异常，准备重连: message_key=%s retry_after_seconds=%s",
                    message_key,
                    self.__subscribe_retry_delay_seconds,
                )
            await asyncio.sleep(self.__subscribe_retry_delay_seconds)

    async def send_message(self, message_key, value):
        request = MessageManager_pb2.SendMessageRequest(messageKey=message_key, value=value)
        if self.__should_use_control_send_stream(message_key):
            await self.__send_message_via_single_shot_control_rpc(message_key, request)
            return
        await self.__send_message_via_shared_stream(message_key, request)

    async def send_unary_message(self, message_key, message_model):
        request = MessageManager_pb2.SendResultRequest(messageKey=message_key, value=message_model)
        response = await self.stub.SendResult(request)
        if response:
            return StatusEnum.SUCCESS

    async def unsubscribe_source(self, message_key: str):
        request = MessageManager_pb2.UnsubscribeTopicRequest(request=self.service_id, messageKey=message_key)
        unsubscribe_source_result = self.stub.UnsubscribeTopic(request)
        if unsubscribe_source_result:
            return StatusEnum.SUCCESS

    async def cancel_add_listener_on_bind_message(self) -> StatusEnum:
        request = MessageManager_pb2.CancelAddListenerOnBindMessageRequest(request="request")
        cancel_add_listener_on_bind_message_result = self.stub.CancelAddListenerOnBindMessage(request)
        if cancel_add_listener_on_bind_message_result:
            return StatusEnum.SUCCESS

    async def startup(self, component_startup_configuration):
        self.__grpc_connector_forwarder.set_grpc_connector_address(
            component_startup_configuration.server_address,
            component_startup_configuration.server_port,
        )
        self.__grpc_connector_forwarder.connect()
        self.stub = MessageManager_pb2_grpc.MessageManagerServiceStub(
            self.__grpc_connector_forwarder.initial_stub()
        )
        self.component_pattern = component_startup_configuration.component_pattern.value
        self.send_stream = None

    def __create_send_stream(self, stream_name: str):
        self.__logger.debug("创建新的 SendMessage gRPC流: stream_name=%s", stream_name)
        return self.stub.SendMessage()

    def __create_single_shot_control_channel(self):
        return grpc.aio.insecure_channel(
            self.__grpc_connector_forwarder.address_port,
            options=self.__single_shot_control_channel_options,
        )

    def __reset_send_stream(self, reason: str) -> None:
        self.__logger.warning("重置 SendMessage gRPC流: reason=%s", reason)
        self.send_stream = None

    def __should_use_control_send_stream(self, message_key: str) -> bool:
        if message_key in self.__control_stream_message_key_set:
            return True
        if not isinstance(message_key, str):
            return False
        return message_key == "command_control" or message_key.endswith(".command_control")

    def __build_request_debug_summary(self, request) -> str:
        try:
            payload_size = len(request.value or b"")
        except Exception:
            payload_size = -1
        try:
            payload_preview = (request.value or b"")[:180].decode("utf-8", errors="ignore")
        except Exception:
            payload_preview = "<decode_failed>"
        payload_preview = payload_preview.replace("\n", "\\n").replace("\r", "\\r")
        return (
            f"message_key={request.messageKey} payload_size={payload_size} "
            f"payload_preview={payload_preview}"
        )

    async def __send_message_via_single_shot_control_rpc(self, message_key: str, request) -> None:
        # 关键控制消息不再复用长期存在的 stream-unary 写流。
        # 根因是：该类 RPC 在本工程里被长期复用后，done()/半关闭状态很容易污染下一条 timeout 终态消息。
        # 这里进一步改为“单消息单 channel + 单 RPC”，彻底隔离未结束控制调用之间的底层连接状态。
        request_debug_summary = self.__build_request_debug_summary(request)
        self.__logger.debug(
            "控制面发送准备进入发送锁: send_mode=single_shot_control transport=dedicated_channel %s",
            request_debug_summary,
        )
        async with self.__control_send_stream_lock:
            self.__logger.debug(
                "控制面发送已获得发送锁: send_mode=single_shot_control transport=dedicated_channel %s",
                request_debug_summary,
            )
            last_exception = None
            for attempt in range(1, self.__send_message_retry_limit + 1):
                self.__logger.debug(
                    "控制面发送开始新 attempt: attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel %s",
                    attempt,
                    self.__send_message_retry_limit,
                    request_debug_summary,
                )
                rpc_channel = self.__create_single_shot_control_channel()
                rpc_stub = MessageManager_pb2_grpc.MessageManagerServiceStub(rpc_channel)
                rpc_call = rpc_stub.SendMessage()
                should_close_channel = True
                try:
                    self.__logger.debug(
                        "控制面发送准备 write: attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel write_timeout_seconds=%s %s",
                        attempt,
                        self.__send_message_retry_limit,
                        self.__send_message_write_timeout_seconds,
                        request_debug_summary,
                    )
                    await asyncio.wait_for(
                        rpc_call.write(request),
                        timeout=self.__send_message_write_timeout_seconds,
                    )
                    self.__logger.debug(
                        "控制面发送 write 完成: attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel %s",
                        attempt,
                        self.__send_message_retry_limit,
                        request_debug_summary,
                    )
                    self.__logger.debug(
                        "控制面发送准备 done_writing: attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel write_timeout_seconds=%s %s",
                        attempt,
                        self.__send_message_retry_limit,
                        self.__send_message_write_timeout_seconds,
                        request_debug_summary,
                    )
                    await asyncio.wait_for(
                        rpc_call.done_writing(),
                        timeout=self.__send_message_write_timeout_seconds,
                    )
                    self.__logger.debug(
                        "控制面发送 done_writing 完成: attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel %s",
                        attempt,
                        self.__send_message_retry_limit,
                        request_debug_summary,
                    )
                    self.__start_single_shot_control_response_watch(
                        rpc_call=rpc_call,
                        message_key=message_key,
                        rpc_channel=rpc_channel,
                    )
                    self.__logger.debug(
                        "控制面发送已启动后台响应监控并准备返回: attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel %s",
                        attempt,
                        self.__send_message_retry_limit,
                        request_debug_summary,
                    )
                    should_close_channel = False
                    if attempt > 1:
                        self.__logger.debug(
                            "控制面单次发送重试成功: message_key=%s attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel",
                            message_key,
                            attempt,
                            self.__send_message_retry_limit,
                        )
                    return
                except (asyncio.TimeoutError, asyncio.InvalidStateError, grpc.aio.AioRpcError) as exc:
                    last_exception = exc
                    self.__logger.warning(
                        "控制面单次发送失败，准备重新发起独立RPC重试: attempt=%s/%s send_mode=single_shot_control transport=dedicated_channel error=%s %s",
                        attempt,
                        self.__send_message_retry_limit,
                        exc,
                        request_debug_summary,
                    )
                    rpc_call.cancel()
                    await self.__close_single_shot_control_channel(
                        rpc_channel=rpc_channel,
                        message_key=message_key,
                        close_reason="send_failed",
                    )
                    should_close_channel = False
                    if attempt < self.__send_message_retry_limit:
                        continue
                    raise
                finally:
                    if should_close_channel:
                        await self.__close_single_shot_control_channel(
                            rpc_channel=rpc_channel,
                            message_key=message_key,
                            close_reason="send_cleanup",
                        )
            if last_exception is not None:
                raise last_exception

    async def __send_message_via_shared_stream(self, message_key: str, request) -> None:
        async with self.__send_stream_lock:
            last_exception = None
            for attempt in range(1, self.__send_message_retry_limit + 1):
                send_stream = await self.__ensure_send_stream(message_key=message_key, attempt=attempt)
                try:
                    await asyncio.wait_for(
                        send_stream.write(request),
                        timeout=self.__send_message_write_timeout_seconds,
                    )
                    if attempt > 1:
                        self.__logger.debug(
                            "共享写流重试成功: message_key=%s attempt=%s/%s send_mode=shared_stream",
                            message_key,
                            attempt,
                            self.__send_message_retry_limit,
                        )
                    return
                except (asyncio.TimeoutError, asyncio.InvalidStateError, grpc.aio.AioRpcError) as exc:
                    last_exception = exc
                    self.__logger.warning(
                        "共享写流发送失败，准备重建写流后重试: message_key=%s attempt=%s/%s send_mode=shared_stream error=%s",
                        message_key,
                        attempt,
                        self.__send_message_retry_limit,
                        exc,
                    )
                    self.__reset_send_stream(
                        reason=(
                            f"shared_stream_send_failed message_key={message_key} "
                            f"attempt={attempt}/{self.__send_message_retry_limit}"
                        )
                    )
                    if attempt < self.__send_message_retry_limit:
                        continue
                    raise
            if last_exception is not None:
                raise last_exception

    async def __ensure_send_stream(self, message_key: str = None, attempt: int = 1):
        if self.send_stream is None:
            self.send_stream = self.__create_send_stream("shared_stream")
            return self.send_stream
        if self.send_stream.done():
            self.__logger.warning(
                "检测到 SendMessage gRPC流已结束，将重新创建: message_key=%s attempt=%s",
                message_key,
                attempt,
            )
            self.send_stream = self.__create_send_stream("shared_stream")
        return self.send_stream

    def __start_single_shot_control_response_watch(self, rpc_call, message_key: str, rpc_channel) -> None:
        response_watch_task = asyncio.create_task(
            self.__wait_single_shot_control_response(rpc_call, message_key, rpc_channel)
        )
        response_watch_task.add_done_callback(
            lambda task: self.__log_single_shot_control_response_watch(task, message_key)
        )

    async def __wait_single_shot_control_response(self, rpc_call, message_key: str, rpc_channel):
        try:
            response = await asyncio.wait_for(
                asyncio.shield(rpc_call),
                timeout=self.__single_shot_control_response_timeout_seconds,
            )
            self.__logger.debug(
                "控制面单次发送收到响应: message_key=%s send_mode=single_shot_control transport=dedicated_channel response=%s",
                message_key,
                getattr(response, "response", None),
            )
            return response
        except asyncio.TimeoutError:
            self.__logger.debug(
                "控制面单次发送响应超时，但消息已完成 write/done_writing，按已发送处理并关闭独立channel: message_key=%s send_mode=single_shot_control transport=dedicated_channel response_timeout_seconds=%s",
                message_key,
                self.__single_shot_control_response_timeout_seconds,
            )
            rpc_call.cancel()
            return None
        except grpc.aio.AioRpcError as exc:
            self.__logger.warning(
                "控制面单次发送后台响应阶段异常，关闭独立channel: message_key=%s send_mode=single_shot_control transport=dedicated_channel error=%s",
                message_key,
                exc,
            )
            return None
        finally:
            await self.__close_single_shot_control_channel(
                rpc_channel=rpc_channel,
                message_key=message_key,
                close_reason="response_watch_finished",
            )

    async def __close_single_shot_control_channel(self, rpc_channel, message_key: str, close_reason: str) -> None:
        try:
            await rpc_channel.close()
        except Exception as exc:
            self.__logger.warning(
                "关闭控制面独立channel失败: message_key=%s close_reason=%s error=%s",
                message_key,
                close_reason,
                exc,
            )

    def __log_single_shot_control_response_watch(self, task: asyncio.Task, message_key: str) -> None:
        if task.cancelled():
            return
        exception = task.exception()
        if exception is None:
            return
        self.__logger.exception(
            "控制面单次发送后台响应阶段失败: message_key=%s send_mode=single_shot_control",
            message_key,
            exc_info=exception,
        )
