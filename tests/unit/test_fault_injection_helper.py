from __future__ import annotations

import pytest

from tests.helpers.fault_injection import (
    FaultInjectionEvent,
    build_fault_event,
    classify_fault_severity,
    summarize_fault_event,
)


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("fault_injection")]


@pytest.mark.test_id("FAULT-HELP-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("故障注入事件必须标准化 stage/action/team/trial/detail 字段，避免掉线重连矩阵记录格式漂移")
@pytest.mark.tested(file="tests/helpers/fault_injection.py", function="build_fault_event")
def test_build_fault_event_normalizes_fields_for_matrix_logging() -> None:
    event = build_fault_event(
        stage=" online_trial ",
        action=" disconnect ",
        team_id=" team_1 ",
        trial_id=" 7 ",
        detail=" late result window ",
    )

    assert event == FaultInjectionEvent(
        stage="online_trial",
        action="disconnect",
        team_id="team_1",
        trial_id="7",
        detail="late result window",
    )


@pytest.mark.test_id("FAULT-HELP-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("故障注入事件摘要必须包含 stage/team/action 和 trial，便于 CSV 与日志统一输出")
@pytest.mark.tested(file="tests/helpers/fault_injection.py", function="summarize_fault_event")
def test_summarize_fault_event_outputs_compact_loggable_text() -> None:
    event = FaultInjectionEvent(
        stage="calibration",
        action="disconnect",
        team_id="team_0",
        trial_id="2",
        detail="grpc stream closed",
    )

    assert summarize_fault_event(event) == (
        "stage=calibration, action=disconnect, team=team_0, trial=2, detail=grpc stream closed"
    )


@pytest.mark.test_id("FAULT-HELP-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("故障严重度分类必须把 kill/restart 和 online/task_switch 断连标记为高风险")
@pytest.mark.tested(file="tests/helpers/fault_injection.py", function="classify_fault_severity")
def test_classify_fault_severity_distinguishes_high_medium_and_low_risk_events() -> None:
    assert classify_fault_severity(FaultInjectionEvent("online_trial", "disconnect", "team_0", "3", None)) == "high"
    assert classify_fault_severity(FaultInjectionEvent("pre_match", "reconnect", "team_0", None, None)) == "medium"
    assert classify_fault_severity(FaultInjectionEvent("resume", "restart", "team_0", None, None)) == "high"
    assert classify_fault_severity(FaultInjectionEvent("observe", "log", "team_0", None, None)) == "low"
