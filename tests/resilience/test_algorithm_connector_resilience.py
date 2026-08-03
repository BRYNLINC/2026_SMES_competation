from __future__ import annotations

import sys
from pathlib import Path

import pytest
from grpc import StatusCode

try:
    import asyncio
    from grpc.aio import AioRpcError

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    PROCESS_HUB_APP_ROOT = PROJECT_ROOT / "app" / "ProcessHub"
    if str(PROCESS_HUB_APP_ROOT) not in sys.path:
        sys.path.insert(0, str(PROCESS_HUB_APP_ROOT))

    from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel
    from Common.model.CommonMessageModel import ControlPackageModel
    from ProcessHub.algorithm_connector.AlgorithmConnector import AlgorithmConnector
    from ProcessHub.algorithm_connector.exception.ProcessHubAlgorithmConnectorException import (
        ProcessHubAlgorithmConnectorClosedException,
        ProcessHubAlgorithmConnectorTimeoutException,
    )
    from ProcessHub.algorithm_connector.facade.AlgorithmRPCDataConnectClient import AlgorithmRPCDataConnectClient
    from ProcessHub.algorithm_connector.facade.AlgorithmRPCServiceControlClient import AlgorithmRPCServiceControlClient
    from ProcessHub.algorithm_connector.facade.GrpcClient import GrpcClient
    from ProcessHub.algorithm_connector.model.AlgorithmConnectModel import AlgorithmConnectModel
    from ProcessHub.common.enum.ServiceStatusEnum import ServiceStatusEnum
except Exception as exc:  # pragma: no cover - environment-dependent import gate
    asyncio = None
    AioRpcError = None
    AlgorithmDataMessageModel = None
    ControlPackageModel = None
    AlgorithmConnector = None
    ProcessHubAlgorithmConnectorClosedException = None
    ProcessHubAlgorithmConnectorTimeoutException = None
    AlgorithmRPCDataConnectClient = None
    AlgorithmRPCServiceControlClient = None
    GrpcClient = None
    AlgorithmConnectModel = None
    ServiceStatusEnum = None
    _ALGORITHM_CONNECTOR_IMPORT_ERROR = exc
else:
    _ALGORITHM_CONNECTOR_IMPORT_ERROR = None


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("algorithm_connector")]


if AlgorithmConnector is None or asyncio is None:
    pytestmark.append(
        pytest.mark.skip(
            reason=f"Algorithm connector import unavailable in current environment: {_ALGORITHM_CONNECTOR_IMPORT_ERROR!r}"
        )
    )


@pytest.mark.test_id("RES-ALG-00P")
@pytest.mark.priority("P0")
@pytest.mark.requirement("决赛平台不得要求不可修改的初赛 PYD 算法服务端接受新增的高频 keepalive ping")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/GrpcClient.py",
    function="GrpcClient.__init__",
)
def test_final_grpc_client_does_not_require_preliminary_pyd_keepalive_options() -> None:
    client = GrpcClient("127.0.0.1:9981")
    option_name_set = {
        option_name
        for option_name, _ in client._GrpcClient__channel_options  # type: ignore[attr-defined]
    }

    assert not any(
        option_name.startswith("grpc.keepalive")
        or option_name.startswith("grpc.http2.max_pings")
        for option_name in option_name_set
    )


class _FakeReceiveReportOperator:
    def __init__(self) -> None:
        self.received_reports = []

    async def receive_report(self, algorithm_report_message) -> None:
        self.received_reports.append(algorithm_report_message)


class _FakeClosedEventOperator:
    def __init__(self) -> None:
        self.disconnect_reason_list: list[str] = []

    async def on_closed(self, disconnect_reason: str = "unknown") -> None:
        self.disconnect_reason_list.append(disconnect_reason)


class _FakeRpcClient:
    def __init__(self, service_address: str) -> None:
        self.service_address = service_address
        self.startup_called = 0
        self.shutdown_called = 0
        self.requested_stub_classes: list[type] = []

    async def startup(self) -> None:
        self.startup_called += 1

    async def shutdown(self) -> None:
        self.shutdown_called += 1

    def get_stub_instance(self, stub_class: type):
        self.requested_stub_classes.append(stub_class)
        return object()


def _aio_rpc_error(message: str) -> AioRpcError:
    return AioRpcError(code=None, initial_metadata=None, trailing_metadata=None, details=message)


@pytest.mark.test_id("RES-ALG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("算法端在最大连接超时时间内无法响应 getStatus 时，连接器必须抛出超时异常并回滚为 ERROR")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/AlgorithmConnector.py",
    function="startup/__wait_for_connect",
)
@pytest.mark.asyncio
async def test_algorithm_connector_startup_times_out_when_remote_status_never_responds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_client = AlgorithmRPCServiceControlClient()
    data_client = AlgorithmRPCDataConnectClient()
    connector = AlgorithmConnector(data_client, control_client)
    connector.set_receive_report_operator(_FakeReceiveReportOperator())

    fake_rpc_client = _FakeRpcClient("127.0.0.1:9981")
    monkeypatch.setattr(
        "ProcessHub.algorithm_connector.AlgorithmConnector.GrpcClient",
        lambda service_address: fake_rpc_client,
    )

    async def _always_fail_get_status():
        raise _aio_rpc_error("connect failed")

    monkeypatch.setattr(control_client, "get_status", _always_fail_get_status)
    original_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda _: original_sleep(0))
    time_values = iter([0.0, 0.3, 0.8, 1.2, 1.5])
    monkeypatch.setattr(
        "ProcessHub.algorithm_connector.AlgorithmConnector.time.time",
        lambda: next(time_values),
    )

    await connector.initial(AlgorithmConnectModel(address="127.0.0.1:9981", max_time_out=1.0))

    with pytest.raises(ProcessHubAlgorithmConnectorTimeoutException):
        await connector.startup()

    assert connector._AlgorithmConnector__service_status is ServiceStatusEnum.ERROR
    assert fake_rpc_client.startup_called == 1
    assert fake_rpc_client.shutdown_called == 1


@pytest.mark.test_id("RES-ALG-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("数据连接断流后必须执行关闭回调，并把连接器状态从 RUNNING 收敛回 READY")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/AlgorithmConnector.py",
    function="on_data_connect_closed",
)
@pytest.mark.asyncio
async def test_algorithm_connector_on_data_connect_closed_notifies_callback_and_restores_ready_state() -> None:
    connector = AlgorithmConnector(AlgorithmRPCDataConnectClient(), AlgorithmRPCServiceControlClient())
    closed_operator = _FakeClosedEventOperator()
    connector.set_data_connect_closed_event_operator(closed_operator)
    connector._AlgorithmConnector__algorithm_address = "127.0.0.1:9981"
    connector._AlgorithmConnector__service_status = ServiceStatusEnum.RUNNING

    await connector.on_data_connect_closed(disconnect_reason="report_stream_closed")

    assert closed_operator.disconnect_reason_list == ["report_stream_closed"]
    assert connector._AlgorithmConnector__service_status is ServiceStatusEnum.READY


@pytest.mark.test_id("RES-ALG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("本地主动断开数据流后，DataConnectClient 必须复位发送队列、断开事件和收发状态，允许后续重连")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCDataConnectClient.py",
    function="disconnect/__disconnect_process",
)
@pytest.mark.asyncio
async def test_data_connect_client_disconnect_resets_transport_state_for_reconnect() -> None:
    client = AlgorithmRPCDataConnectClient()
    closed_operator = _FakeClosedEventOperator()
    client.add_connect_closed_event_operator(closed_operator)
    client._AlgorithmRPCDataConnectClient__sender_status = ServiceStatusEnum.RUNNING
    client._AlgorithmRPCDataConnectClient__receiver_status = ServiceStatusEnum.STOPPED
    client._AlgorithmRPCDataConnectClient__session_generation = 1
    client._AlgorithmRPCDataConnectClient__active_session_generation = 1
    old_queue = client._AlgorithmRPCDataConnectClient__data_message_queue
    old_event = client._AlgorithmRPCDataConnectClient__disconnect_event
    await client._AlgorithmRPCDataConnectClient__data_message_queue.put(object())

    async def _fake_stop_sender_process() -> None:
        client._AlgorithmRPCDataConnectClient__sender_status = ServiceStatusEnum.STOPPED

    async def _fake_stop_receiver_process(disconnect_reason: str = "unknown") -> None:
        client._AlgorithmRPCDataConnectClient__receiver_status = ServiceStatusEnum.STOPPED

    client._AlgorithmRPCDataConnectClient__stop_sender_process = _fake_stop_sender_process  # type: ignore[attr-defined]
    client._AlgorithmRPCDataConnectClient__stop_receiver_process = _fake_stop_receiver_process  # type: ignore[attr-defined]

    await asyncio.wait_for(client.disconnect(), timeout=1.0)

    assert closed_operator.disconnect_reason_list == []
    assert client._AlgorithmRPCDataConnectClient__sender_status is ServiceStatusEnum.STOPPED
    assert client._AlgorithmRPCDataConnectClient__receiver_status is ServiceStatusEnum.STOPPED
    assert client._AlgorithmRPCDataConnectClient__data_message_queue is not old_queue
    assert client._AlgorithmRPCDataConnectClient__disconnect_event is not old_event
    assert client._AlgorithmRPCDataConnectClient__data_message_queue.qsize() == 0


@pytest.mark.requirement("旧数据流 generation 的延迟清理不得重置或关闭已建立的新 generation")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCDataConnectClient.py",
    function="__disconnect_process",
)
async def test_stale_data_connect_cleanup_cannot_close_new_generation() -> None:
    client = AlgorithmRPCDataConnectClient()
    client._AlgorithmRPCDataConnectClient__session_generation = 2
    client._AlgorithmRPCDataConnectClient__active_session_generation = 2
    active_queue = client._AlgorithmRPCDataConnectClient__data_message_queue
    active_event = client._AlgorithmRPCDataConnectClient__disconnect_event

    await client._AlgorithmRPCDataConnectClient__disconnect_process(  # type: ignore[attr-defined]
        disconnect_reason="late_old_stream_cleanup",
        session_generation=1,
    )

    assert client._AlgorithmRPCDataConnectClient__active_session_generation == 2
    assert client._AlgorithmRPCDataConnectClient__data_message_queue is active_queue
    assert client._AlgorithmRPCDataConnectClient__disconnect_event is active_event


@pytest.mark.requirement("发送流和接收流同时结束时，同 generation 清理不得互相等待至超时")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCDataConnectClient.py",
    function="__disconnect_process",
)
async def test_concurrent_request_and_report_cleanup_does_not_deadlock() -> None:
    client = AlgorithmRPCDataConnectClient()
    client._AlgorithmRPCDataConnectClient__session_generation = 1
    client._AlgorithmRPCDataConnectClient__active_session_generation = 1
    primary_cleanup_waiting = asyncio.Event()
    receive_cleanup_finished = asyncio.Event()

    async def fake_stop_sender_process() -> None:
        return None

    async def fake_stop_receiver_process(disconnect_reason: str = "unknown") -> None:
        primary_cleanup_waiting.set()
        await receive_cleanup_finished.wait()

    client._AlgorithmRPCDataConnectClient__stop_sender_process = fake_stop_sender_process  # type: ignore[attr-defined]
    client._AlgorithmRPCDataConnectClient__stop_receiver_process = fake_stop_receiver_process  # type: ignore[attr-defined]

    async def run_receive_cleanup() -> None:
        await primary_cleanup_waiting.wait()
        client._AlgorithmRPCDataConnectClient__receive_report_task = asyncio.current_task()
        await client._AlgorithmRPCDataConnectClient__disconnect_process(  # type: ignore[attr-defined]
            disconnect_reason="report_stream_closed",
            session_generation=1,
        )
        receive_cleanup_finished.set()

    primary_cleanup_task = asyncio.create_task(
        client._AlgorithmRPCDataConnectClient__disconnect_process(  # type: ignore[attr-defined]
            disconnect_reason="request_stream_closed",
            session_generation=1,
        )
    )
    receive_cleanup_task = asyncio.create_task(run_receive_cleanup())

    await asyncio.wait_for(
        asyncio.gather(primary_cleanup_task, receive_cleanup_task),
        timeout=1.0,
    )

    assert client._AlgorithmRPCDataConnectClient__active_session_generation == 0
    assert client._AlgorithmRPCDataConnectClient__disconnecting_session_generation == 0


@pytest.mark.test_id("RES-ALG-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("数据连接关闭后若继续写入 trial 数据，客户端必须拒绝并抛出 closed 异常，防止晚到结果污染后续流程")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCDataConnectClient.py",
    function="send_data",
)
@pytest.mark.asyncio
async def test_data_connect_client_rejects_send_after_closed() -> None:
    client = AlgorithmRPCDataConnectClient()
    message_model = AlgorithmDataMessageModel(
        source_label="eeg_1",
        timestamp_ms=1,
        package=ControlPackageModel(end_flag=False),
    )
    protobuf_message = client._AlgorithmRPCDataConnectClient__algorithm_rpc_message_converter.model_to_protobuf(message_model)

    with pytest.raises(ProcessHubAlgorithmConnectorClosedException):
        await client.send_data(protobuf_message)


@pytest.mark.test_id("RES-ALG-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("算法连接器在 READY 状态执行数据连接后，应进入 RUNNING；执行 data_disconnect 后应回到 READY")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/AlgorithmConnector.py",
    function="data_connect/data_disconnect",
)
@pytest.mark.asyncio
async def test_algorithm_connector_data_connect_and_disconnect_transition_between_ready_and_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_client = AlgorithmRPCServiceControlClient()
    data_client = AlgorithmRPCDataConnectClient()
    connector = AlgorithmConnector(data_client, control_client)
    connector._AlgorithmConnector__service_status = ServiceStatusEnum.READY

    connect_called = {"count": 0}
    disconnect_called = {"count": 0}

    async def _fake_connect() -> None:
        connect_called["count"] += 1

    async def _fake_disconnect() -> None:
        disconnect_called["count"] += 1

    monkeypatch.setattr(data_client, "connect", _fake_connect)
    monkeypatch.setattr(data_client, "disconnect", _fake_disconnect)

    await connector.data_connect()
    assert connector._AlgorithmConnector__service_status is ServiceStatusEnum.RUNNING
    assert connect_called["count"] == 1

    await connector.data_disconnect()
    assert connector._AlgorithmConnector__service_status is ServiceStatusEnum.READY
    assert disconnect_called["count"] == 1


@pytest.mark.test_id("RES-ALG-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("控制 RPC 仍可用但双向数据流已关闭时，连接器不得继续报告 transport active")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/AlgorithmConnector.py",
    function="is_transport_active",
)
@pytest.mark.asyncio
async def test_algorithm_connector_transport_health_includes_data_stream_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_client = AlgorithmRPCDataConnectClient()
    connector = AlgorithmConnector(data_client, AlgorithmRPCServiceControlClient())
    connector._AlgorithmConnector__service_status = ServiceStatusEnum.RUNNING
    monkeypatch.setattr(data_client, "is_transport_active", lambda: True)

    assert connector.is_transport_active() is True

    monkeypatch.setattr(data_client, "is_transport_active", lambda: False)
    assert connector.is_transport_active() is False

    connector._AlgorithmConnector__service_status = ServiceStatusEnum.READY
    monkeypatch.setattr(data_client, "is_transport_active", lambda: True)
    assert connector.is_transport_active() is False


@pytest.mark.test_id("RES-ALG-07")
@pytest.mark.priority("P0")
@pytest.mark.requirement("算法主机重置连接时，断连原因必须保留 gRPC 状态码和 10054 详情")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCDataConnectClient.py",
    function="_format_rpc_disconnect_reason",
)
def test_rpc_disconnect_reason_preserves_status_and_remote_reset_detail() -> None:
    class FakeRpcError:
        @staticmethod
        def code():
            return StatusCode.UNAVAILABLE

        @staticmethod
        def details() -> str:
            return "Stream removed (Connection reset -- 10054)"

    reason = AlgorithmRPCDataConnectClient._format_rpc_disconnect_reason(FakeRpcError())

    assert reason == "grpc_unavailable: Stream removed (Connection reset -- 10054)"
