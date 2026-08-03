from __future__ import annotations

import sys
from pathlib import Path

import pytest

try:
    import yaml

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    PROCESS_HUB_APP_ROOT = PROJECT_ROOT / "app" / "ProcessHub"
    if str(PROCESS_HUB_APP_ROOT) not in sys.path:
        sys.path.insert(0, str(PROCESS_HUB_APP_ROOT))

    from Algorithm.api.converter.AlgorithmRPCMessageConverter import AlgorithmRPCMessageConverter
    from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmStatusEnum, AlgorithmStatusMessageModel
    from Common.protobuf.BaseDataClassMessage_pb2 import BooleanMessage, EmptyMessage, StringMessage
    from ProcessHub.algorithm_connector.facade.AlgorithmRPCServiceControlClient import AlgorithmRPCServiceControlClient
except Exception as exc:  # pragma: no cover - environment-dependent import gate
    yaml = None
    AlgorithmRPCMessageConverter = None
    AlgorithmStatusEnum = None
    AlgorithmStatusMessageModel = None
    BooleanMessage = None
    EmptyMessage = None
    StringMessage = None
    AlgorithmRPCServiceControlClient = None
    _ALGORITHM_RPC_CLIENT_IMPORT_ERROR = exc
else:
    _ALGORITHM_RPC_CLIENT_IMPORT_ERROR = None


pytestmark = [pytest.mark.algorithm_interface, pytest.mark.layer("algorithm_interface"), pytest.mark.category("algorithm_rpc_client")]


if AlgorithmRPCServiceControlClient is None:
    pytestmark.append(
        pytest.mark.skip(
            reason=f"Algorithm RPC client import unavailable in current environment: {_ALGORITHM_RPC_CLIENT_IMPORT_ERROR!r}"
        )
    )


class _FakeControlStub:
    def __init__(self) -> None:
        self.get_status_called = 0
        self.send_config_payloads: list[dict] = []
        self.shutdown_called = 0
        self.return_config = {
            "sources": {
                "eeg_1": None,
            },
            "challenge_to_algorithm_config": {
                "predict_timeout_seconds": 1.0,
                "requested_channel_count": 8,
            },
        }

    async def getStatus(self, request: EmptyMessage):
        self.get_status_called += 1
        return AlgorithmRPCMessageConverter.model_to_protobuf(
            AlgorithmStatusMessageModel(status=AlgorithmStatusEnum.READY)
        )

    async def sendConfig(self, request: StringMessage):
        self.send_config_payloads.append(yaml.safe_load(request.data))
        return EmptyMessage()

    async def getConfig(self, request: EmptyMessage):
        return StringMessage(data=yaml.safe_dump(self.return_config))

    async def shutdown(self, request: EmptyMessage):
        self.shutdown_called += 1
        return BooleanMessage(data=True)


@pytest.mark.test_id("ALG-CLIENT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("控制面 client 的 get_status 必须把 protobuf 状态反解为模型枚举")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCServiceControlClient.py",
    function="get_status",
)
@pytest.mark.asyncio
async def test_service_control_client_get_status_converts_protobuf_to_model() -> None:
    stub = _FakeControlStub()
    client = AlgorithmRPCServiceControlClient()
    client.set_algorithm_rpc_service_control_stub(stub)

    status_model = await client.get_status()

    assert stub.get_status_called == 1
    assert status_model.status is AlgorithmStatusEnum.READY


@pytest.mark.test_id("ALG-CLIENT-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("控制面 client 的 send_config 必须以 YAML 格式发送完整 challenge_to_algorithm_config")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCServiceControlClient.py",
    function="send_config",
)
@pytest.mark.asyncio
async def test_service_control_client_send_config_serializes_yaml_payload() -> None:
    stub = _FakeControlStub()
    client = AlgorithmRPCServiceControlClient()
    client.set_algorithm_rpc_service_control_stub(stub)
    payload = {
        "sources": {"eeg_1": None, "eeg_2": None},
        "challenge_to_algorithm_config": {
            "predict_timeout_seconds": 1.0,
            "calibration_trials_per_class_requested": 5,
        },
    }

    await client.send_config(payload)

    assert stub.send_config_payloads == [payload]


@pytest.mark.test_id("ALG-CLIENT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("控制面 client 的 get_config 必须把 YAML 配置反序列化为 dict，并保留 sources 主键")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCServiceControlClient.py",
    function="get_config",
)
@pytest.mark.asyncio
async def test_service_control_client_get_config_deserializes_yaml_response() -> None:
    stub = _FakeControlStub()
    client = AlgorithmRPCServiceControlClient()
    client.set_algorithm_rpc_service_control_stub(stub)

    payload = await client.get_config()

    assert payload == stub.return_config
    assert payload["sources"] == {"eeg_1": None}


@pytest.mark.test_id("ALG-CLIENT-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("控制面 client 的 shutdown 必须向远端发送关闭请求并返回布尔结果")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCServiceControlClient.py",
    function="shutdown",
)
@pytest.mark.asyncio
async def test_service_control_client_shutdown_returns_remote_boolean_result() -> None:
    stub = _FakeControlStub()
    client = AlgorithmRPCServiceControlClient()
    client.set_algorithm_rpc_service_control_stub(stub)

    result = await client.shutdown()

    assert result is True
    assert stub.shutdown_called == 1
