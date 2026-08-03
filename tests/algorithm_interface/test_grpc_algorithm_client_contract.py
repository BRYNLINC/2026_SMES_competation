from __future__ import annotations

import pytest

from tests.helpers.grpc_algorithm_client import (
    build_algorithm_endpoint,
    build_control_call_contract,
    build_stream_call_contract,
    probe_grpc_availability,
)


pytestmark = [pytest.mark.algorithm_interface, pytest.mark.layer("algorithm_interface"), pytest.mark.category("grpc_algorithm_client_contract")]


@pytest.mark.test_id("ALG-GRPC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("算法接口测试层必须保留对 9981 endpoint 和控制面 RPC 契约的统一定义，避免多文件重复硬编码")
@pytest.mark.tested(
    file="tests/helpers/grpc_algorithm_client.py",
    function="build_algorithm_endpoint/build_control_call_contract",
)
def test_grpc_helper_defines_default_algorithm_control_contract() -> None:
    endpoint = build_algorithm_endpoint()
    get_status = build_control_call_contract("getStatus")
    send_config = build_control_call_contract("sendConfig")

    assert endpoint.endswith(":9981")
    assert get_status.expected_response == "AlgorithmStatusMessage"
    assert get_status.idempotent is True
    assert send_config.expected_request == "StringMessage(yaml_config)"
    assert send_config.idempotent is False


@pytest.mark.test_id("ALG-GRPC-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("算法数据流接口测试层必须约束 connect/calibrate/predict 为 stream 契约，便于后续恢复真实 gRPC 后直接接入")
@pytest.mark.tested(
    file="tests/helpers/grpc_algorithm_client.py",
    function="build_stream_call_contract",
)
def test_grpc_helper_defines_stream_contracts_for_algorithm_data_plane() -> None:
    connect = build_stream_call_contract("connect")
    calibrate = build_stream_call_contract("calibrate")
    predict = build_stream_call_contract("predict")

    assert {connect.method, calibrate.method, predict.method} == {"connect", "calibrate", "predict"}
    assert all(contract.channel_kind == "stream" for contract in (connect, calibrate, predict))


@pytest.mark.test_id("ALG-GRPC-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("当前环境若 grpc 不可用，算法接口层必须给出结构化原因，避免把系统问题误判成业务回归")
@pytest.mark.tested(
    file="tests/helpers/grpc_algorithm_client.py",
    function="probe_grpc_availability",
)
def test_grpc_availability_probe_returns_structured_status_for_algorithm_interface_layer() -> None:
    availability = probe_grpc_availability()

    assert isinstance(availability.available, bool)
    if availability.available:
        assert availability.grpc_version
    else:
        assert availability.reason
