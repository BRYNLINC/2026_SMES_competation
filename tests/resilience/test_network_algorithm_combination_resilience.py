from __future__ import annotations

import pytest

from tests.helpers.fake_algorithm_server import DeterministicFakeAlgorithmServer, build_profile
from tests.helpers.network_proxy import NetworkFaultProfile, should_disconnect


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("network_algorithm_combination")]


@pytest.mark.test_id("RES-NET-ALG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("网络断流与 fake algorithm disconnect_stream 组合时，必须保留网络阈值和算法断流原因，便于掉线重连矩阵复盘")
@pytest.mark.tested(
    file="tests/helpers/network_proxy.py;tests/helpers/fake_algorithm_server.py",
    function="should_disconnect/record_packet/snapshot",
)
def test_network_disconnect_and_algorithm_disconnect_stream_combination_is_observable() -> None:
    network_profile = NetworkFaultProfile(mode="disconnect_after_n", disconnect_after_packets=2)
    algorithm_server = DeterministicFakeAlgorithmServer(build_profile("disconnect_stream", after_message_count=3))

    assert should_disconnect(network_profile, packet_index=1) is False
    assert should_disconnect(network_profile, packet_index=2) is True
    assert algorithm_server.record_packet("control") == []
    assert algorithm_server.record_packet("data") == []
    algorithm_action = algorithm_server.record_packet("event")[0]

    assert algorithm_action.kind == "disconnect"
    assert algorithm_action.reason == "disconnect_stream_after_3_packets"
    assert algorithm_server.snapshot().disconnect_reasons == ["disconnect_stream_after_3_packets"]


@pytest.mark.test_id("RES-NET-ALG-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("固定网络延迟超过 1000ms 且算法 slow 时，组合结果必须归类为 timeout，不得被误判为正常结果")
@pytest.mark.tested(
    file="tests/helpers/network_proxy.py;tests/helpers/fake_algorithm_server.py",
    function="resolve_delay_ms/emit_prediction",
)
def test_network_delay_and_slow_algorithm_combination_resolves_to_timeout() -> None:
    network_profile = NetworkFaultProfile(mode="fixed_delay", fixed_delay_ms=1200)
    algorithm_server = DeterministicFakeAlgorithmServer(build_profile("slow", latency_ms=1101))

    network_delay_ms = max(0, int(network_profile.fixed_delay_ms))
    algorithm_action = algorithm_server.emit_prediction(report_source_position="trial_end", now_ms=1)[0]

    assert network_delay_ms > 1000
    assert algorithm_action.kind == "timeout"
    assert algorithm_action.accepted is False
    assert algorithm_action.reason == "predict_timeout_exceeded"


@pytest.mark.test_id("RES-NET-ALG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("网络正常但算法 duplicate_result 时，首个结果可接受、后续重复结果必须可识别并丢弃")
@pytest.mark.tested(
    file="tests/helpers/network_proxy.py;tests/helpers/fake_algorithm_server.py",
    function="emit_prediction",
)
def test_normal_network_with_duplicate_algorithm_keeps_only_first_result_accepted() -> None:
    algorithm_server = DeterministicFakeAlgorithmServer(build_profile("duplicate_result", duplicate_count=4))

    actions = algorithm_server.emit_prediction(report_source_position="trial_end", now_ms=2)

    assert [action.accepted for action in actions] == [True, False, False, False]
    assert [action.reason for action in actions] == [None, "duplicate_result", "duplicate_result", "duplicate_result"]
