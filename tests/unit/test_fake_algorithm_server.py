from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.fake_algorithm_server import (
    DeterministicFakeAlgorithmServer,
    build_profile,
    describe_profile,
)


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("fake_algorithm_server")]


@pytest.mark.test_id("FAKE-ALG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("fake algorithm server 必须对 normal/slow/late_result profile 给出稳定且可复现的预测动作")
@pytest.mark.tested(file="tests/helpers/fake_algorithm_server.py", function="build_profile/emit_prediction")
def test_fake_algorithm_normal_slow_and_late_profiles_are_deterministic() -> None:
    normal_server = DeterministicFakeAlgorithmServer(build_profile("normal", predict_label=2, latency_ms=30))
    slow_server = DeterministicFakeAlgorithmServer(build_profile("slow"))
    late_server = DeterministicFakeAlgorithmServer(build_profile("late_result", predict_label=1))

    normal_action = normal_server.emit_prediction(report_source_position="trial_end", now_ms=100)[0]
    slow_action = slow_server.emit_prediction(report_source_position="trial_end", now_ms=200)[0]
    late_action = late_server.emit_prediction(report_source_position="trial_end", now_ms=300)[0]

    assert json.loads(normal_action.payload) == {
        "predict_label": 2,
        "report_source_position": "trial_end",
    }
    assert normal_action.accepted is True
    assert slow_action.kind == "timeout"
    assert slow_action.accepted is False
    assert late_action.kind == "result"
    assert late_action.reason == "late_result_after_timeout"
    assert late_server.snapshot().prediction_submit_times_ms == [300]


@pytest.mark.test_id("FAKE-ALG-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("fake algorithm server 必须记录 config、source_label 和各类 packet 计数，便于接口与掉线重连测试复用")
@pytest.mark.tested(file="tests/helpers/fake_algorithm_server.py", function="receive_config/receive_sources/record_packet/snapshot")
def test_fake_algorithm_server_records_observation_data_for_config_sources_and_packets() -> None:
    server = DeterministicFakeAlgorithmServer(build_profile("normal"))
    server.receive_config({"predict_timeout_seconds": 1.0})
    server.receive_sources(["eeg_1", "trigger"])
    server.record_packet("calibration")
    server.record_packet("control")
    server.record_packet("data")
    server.record_packet("event")

    snapshot = server.snapshot()

    assert snapshot.received_config == {"predict_timeout_seconds": 1.0}
    assert snapshot.source_labels == ["eeg_1", "trigger"]
    assert snapshot.calibration_packet_count == 1
    assert snapshot.control_packet_count == 1
    assert snapshot.data_packet_count == 1
    assert snapshot.event_packet_count == 1


@pytest.mark.test_id("FAKE-ALG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("disconnect_stream、crash_on_calibration、crash_on_predict profile 必须在对应阶段给出可观测断开原因和退出码")
@pytest.mark.tested(file="tests/helpers/fake_algorithm_server.py", function="connect/record_packet")
def test_fake_algorithm_disconnect_and_crash_profiles_emit_expected_control_actions() -> None:
    disconnect_server = DeterministicFakeAlgorithmServer(build_profile("disconnect_stream", after_message_count=2))
    calibration_crash_server = DeterministicFakeAlgorithmServer(build_profile("crash_on_calibration", exit_code=31))
    predict_crash_server = DeterministicFakeAlgorithmServer(build_profile("crash_on_predict", exit_code=32))

    assert disconnect_server.record_packet("data") == []
    disconnect_action = disconnect_server.record_packet("event")[0]
    calibration_crash_action = calibration_crash_server.record_packet("calibration")[0]
    predict_crash_action = predict_crash_server.record_packet("event", {"event": "trial_end"})[0]

    assert disconnect_action.kind == "disconnect"
    assert disconnect_server.snapshot().disconnect_reasons == ["disconnect_stream_after_2_packets"]
    assert calibration_crash_action.kind == "crash"
    assert calibration_crash_server.snapshot().exit_code == 31
    assert predict_crash_action.reason == "crash_on_predict"
    assert predict_crash_server.snapshot().exit_code == 32


@pytest.mark.test_id("FAKE-ALG-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("invalid_output 和 duplicate_result profile 必须输出稳定的非法载荷与重复计数，供接口防御测试复用")
@pytest.mark.tested(file="tests/helpers/fake_algorithm_server.py", function="emit_prediction")
def test_fake_algorithm_invalid_output_and_duplicate_profiles_are_repeatable() -> None:
    invalid_server = DeterministicFakeAlgorithmServer(build_profile("invalid_output", payload_type="unknown_fields"))
    duplicate_server = DeterministicFakeAlgorithmServer(build_profile("duplicate_result", duplicate_count=3))

    invalid_action = invalid_server.emit_prediction(report_source_position="trial_end")[0]
    duplicate_actions = duplicate_server.emit_prediction(report_source_position="trial_end")

    assert json.loads(invalid_action.payload) == {"exploit": "netcat", "predict_label": 0}
    assert len(duplicate_actions) == 3
    assert [action.accepted for action in duplicate_actions] == [True, False, False]
    assert [action.duplicate_index for action in duplicate_actions] == [0, 1, 2]


@pytest.mark.test_id("FAKE-ALG-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("malicious profile 只能在临时目录内执行受控动作，不能越过 sandbox")
@pytest.mark.tested(file="tests/helpers/fake_algorithm_server.py", function="emit_prediction")
def test_fake_algorithm_malicious_profile_is_constrained_to_workspace(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    server = DeterministicFakeAlgorithmServer(
        build_profile("malicious", workspace_root=workspace_root, malicious_action="write_marker")
    )

    actions = server.emit_prediction(report_source_position="trial_end")
    marker_path = workspace_root / "malicious_touch.txt"

    assert marker_path.exists()
    assert actions[0].kind == "malicious"
    assert actions[0].payload["touched_path"] == str(marker_path)
    assert actions[1].kind == "result"


@pytest.mark.test_id("FAKE-ALG-06")
@pytest.mark.priority("P2")
@pytest.mark.requirement("profile 描述文本必须稳定，便于 CSV 报告和失败工件复盘")
@pytest.mark.tested(file="tests/helpers/fake_algorithm_server.py", function="describe_profile")
def test_describe_profile_formats_repeatable_summary_text() -> None:
    assert describe_profile(build_profile("normal", predict_label=1, latency_ms=20)) == (
        "normal, label=1, latency_ms=20"
    )
    assert describe_profile(build_profile("disconnect_stream", after_message_count=4)) == (
        "disconnect_stream, after_message_count=4"
    )
