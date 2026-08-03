from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from tests.helpers import grpc_algorithm_client as client


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("grpc_algorithm_client")]


@pytest.mark.test_id("GRPC-HELPER-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("gRPC helper 不得在模块导入时导入 grpc，避免当前 Windows 网络栈异常导致测试收集失败")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="module_import_contract")
def test_grpc_helper_imports_without_loading_grpc_at_module_import_time() -> None:
    assert "grpc" not in client.__dict__
    assert callable(client.probe_grpc_availability)


@pytest.mark.test_id("GRPC-HELPER-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("算法 RPC endpoint 构造必须默认使用 9981，并校验端口范围")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="build_algorithm_endpoint")
def test_build_algorithm_endpoint_formats_ipv4_ipv6_and_rejects_invalid_ports() -> None:
    assert client.build_algorithm_endpoint() == "127.0.0.1:9981"
    assert client.build_algorithm_endpoint("192.168.1.10", 19081) == "192.168.1.10:19081"
    assert client.build_algorithm_endpoint("::1", 9981) == "[::1]:9981"

    with pytest.raises(ValueError, match="port out of range"):
        client.build_algorithm_endpoint("127.0.0.1", 70000)


@pytest.mark.test_id("GRPC-HELPER-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("控制面 RPC 契约必须覆盖 getStatus/sendConfig/getConfig/shutdown 的请求、响应和幂等属性")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="build_control_call_contract")
@pytest.mark.parametrize(
    ("method", "expected_request", "expected_response", "idempotent"),
    [
        ("getStatus", "EmptyMessage", "AlgorithmStatusMessage", True),
        ("sendConfig", "StringMessage(yaml_config)", "EmptyMessage", False),
        ("getConfig", "EmptyMessage", "StringMessage", True),
        ("shutdown", "EmptyMessage", "BooleanMessage", False),
    ],
)
def test_build_control_call_contract_covers_expected_control_methods(
    method: str,
    expected_request: str,
    expected_response: str,
    idempotent: bool,
) -> None:
    contract = client.build_control_call_contract(method, timeout_seconds=2.5)

    assert contract.method == method
    assert contract.timeout_seconds == 2.5
    assert contract.channel_kind == "unary"
    assert contract.expected_request == expected_request
    assert contract.expected_response == expected_response
    assert contract.idempotent is idempotent


@pytest.mark.test_id("GRPC-HELPER-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("流式 RPC 契约必须覆盖 connect/calibrate/predict，供后续真实 gRPC 测试复用")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="build_stream_call_contract")
@pytest.mark.parametrize("method", ["connect", "calibrate", "predict"])
def test_build_stream_call_contract_covers_algorithm_data_methods(method: str) -> None:
    contract = client.build_stream_call_contract(method)

    assert contract.method == method
    assert contract.channel_kind == "stream"
    assert contract.idempotent is False
    assert contract.expected_request.startswith("Algorithm")
    assert contract.expected_response.startswith("Algorithm")


@pytest.mark.test_id("GRPC-HELPER-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("gRPC 不可用摘要必须识别 WinError 10106 与 base_events，便于 CSV 报告定位系统网络栈问题")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="summarize_grpc_unavailable")
@pytest.mark.parametrize(
    "exc",
    [
        OSError("[WinError 10106] 无法加载或初始化请求的服务提供程序"),
        NameError("name 'base_events' is not defined"),
    ],
)
def test_summarize_grpc_unavailable_classifies_windows_network_stack_failures(exc: BaseException) -> None:
    summary = client.summarize_grpc_unavailable(exc)

    assert "system_network_stack_unavailable" in summary
    assert type(exc).__name__ in summary


@pytest.mark.test_id("GRPC-HELPER-06")
@pytest.mark.priority("P1")
@pytest.mark.requirement("probe_grpc_availability 必须把 import 失败转换为结构化不可用状态，而不是抛异常中断测试")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="probe_grpc_availability")
def test_probe_grpc_availability_returns_unavailable_on_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import_module(name: str):
        assert name == "grpc"
        raise OSError("[WinError 10106] provider failed")

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    availability = client.probe_grpc_availability()

    assert availability.available is False
    assert "system_network_stack_unavailable" in availability.reason


@pytest.mark.test_id("GRPC-HELPER-07")
@pytest.mark.priority("P2")
@pytest.mark.requirement("probe_grpc_availability 可记录 grpc 版本，便于现场算法接口问题追踪")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="probe_grpc_availability")
def test_probe_grpc_availability_records_version_when_import_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import_module(name: str):
        assert name == "grpc"
        return SimpleNamespace(__version__="1.99.0-test")

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    availability = client.probe_grpc_availability()

    assert availability.available is True
    assert availability.grpc_version == "1.99.0-test"
