from __future__ import annotations

import pytest

from tests.helpers.fake_algorithm_server import DeterministicFakeAlgorithmServer, build_profile


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("fake_algorithm_profile")]


@pytest.mark.test_id("RES-FAKE-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("slow、late_result、duplicate_result profile 必须稳定复现 timeout、晚到结果和重复结果矩阵")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="build_profile/emit_prediction",
)
def test_fake_algorithm_profiles_cover_timeout_late_result_and_duplicate_result_behaviour() -> None:
    slow_server = DeterministicFakeAlgorithmServer(build_profile("slow"))
    late_server = DeterministicFakeAlgorithmServer(build_profile("late_result", predict_label=1))
    duplicate_server = DeterministicFakeAlgorithmServer(build_profile("duplicate_result", duplicate_count=3))

    slow_action = slow_server.emit_prediction(report_source_position="trial_end", now_ms=100)[0]
    late_action = late_server.emit_prediction(report_source_position="trial_end", now_ms=200)[0]
    duplicate_actions = duplicate_server.emit_prediction(report_source_position="trial_end", now_ms=300)

    assert slow_action.kind == "timeout"
    assert slow_action.reason == "predict_timeout_exceeded"
    assert late_action.kind == "result"
    assert late_action.accepted is False
    assert late_action.reason == "late_result_after_timeout"
    assert [action.accepted for action in duplicate_actions] == [True, False, False]
    assert [action.reason for action in duplicate_actions] == [None, "duplicate_result", "duplicate_result"]


@pytest.mark.test_id("RES-FAKE-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("disconnect_stream profile 必须在指定消息数后断流，并保留断流原因供掉线重连矩阵复盘")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="record_packet/snapshot",
)
def test_fake_algorithm_disconnect_stream_profile_records_disconnect_reason_after_threshold() -> None:
    server = DeterministicFakeAlgorithmServer(build_profile("disconnect_stream", after_message_count=3))

    assert server.record_packet("control") == []
    assert server.record_packet("data") == []
    disconnect_action = server.record_packet("event")[0]
    snapshot = server.snapshot()

    assert disconnect_action.kind == "disconnect"
    assert disconnect_action.reason == "disconnect_stream_after_3_packets"
    assert snapshot.disconnect_reasons == ["disconnect_stream_after_3_packets"]
    assert snapshot.total_packet_count == 3


@pytest.mark.test_id("RES-FAKE-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("crash_on_connect、crash_on_calibration、crash_on_predict profile 必须在不同阶段给出明确退出码和异常标签")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="connect/record_packet",
)
def test_fake_algorithm_crash_profiles_are_stage_specific_and_observable() -> None:
    connect_server = DeterministicFakeAlgorithmServer(build_profile("crash_on_connect", exit_code=41))
    calibration_server = DeterministicFakeAlgorithmServer(build_profile("crash_on_calibration", exit_code=42))
    predict_server = DeterministicFakeAlgorithmServer(build_profile("crash_on_predict", exit_code=43))

    connect_action = connect_server.connect(["eeg_1"])[0]
    calibration_action = calibration_server.record_packet("calibration")[0]
    predict_action = predict_server.record_packet("event", {"event": "trial_end"})[0]

    assert connect_action.reason == "crash_on_connect"
    assert connect_server.snapshot().exit_code == 41
    assert calibration_action.reason == "crash_on_calibration"
    assert calibration_server.snapshot().exit_code == 42
    assert predict_action.reason == "crash_on_predict"
    assert predict_server.snapshot().exit_code == 43
