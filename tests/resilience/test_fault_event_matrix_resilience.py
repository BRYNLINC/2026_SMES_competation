from __future__ import annotations

import pytest

from tests.helpers.fault_injection import build_fault_event, classify_fault_severity, summarize_fault_event


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("fault_event_matrix")]


@pytest.mark.test_id("RES-FAULT-MATRIX-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("选手掉线/重连矩阵必须能结构化记录 stage/action/team/trial/detail，并输出稳定日志摘要")
@pytest.mark.tested(file="tests/helpers/fault_injection.py", function="build_fault_event/summarize_fault_event/classify_fault_severity")
@pytest.mark.parametrize(
    ("stage", "action", "trial_id", "expected_severity"),
    [
        ("pre_match", "disconnect", None, "medium"),
        ("calibration_stream", "disconnect", None, "medium"),
        ("online_trial", "disconnect", "3", "high"),
        ("task_switch", "reconnect", None, "medium"),
        ("resume_start", "reconnect", None, "medium"),
    ],
)
def test_team_disconnect_reconnect_fault_event_matrix_is_loggable(
    stage: str,
    action: str,
    trial_id: str | None,
    expected_severity: str,
) -> None:
    event = build_fault_event(stage=stage, action=action, team_id="team_0", trial_id=trial_id, detail="matrix")

    summary = summarize_fault_event(event)

    assert f"stage={stage}" in summary
    assert "team=team_0" in summary
    assert "detail=matrix" in summary
    assert classify_fault_severity(event) == expected_severity


@pytest.mark.test_id("RES-FAULT-MATRIX-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("裁判机掉线和比赛重开事件必须被标为高风险，便于发布前人工复核")
@pytest.mark.tested(file="tests/helpers/fault_injection.py", function="classify_fault_severity")
@pytest.mark.parametrize(
    ("stage", "action"),
    [
        ("judgeweb", "kill_process"),
        ("collector", "kill_process"),
        ("runtime_stage_coordinator", "kill_process"),
        ("restart_from_stage", "restart"),
        ("clear_restart", "restart"),
    ],
)
def test_judge_component_dropout_and_restart_fault_events_are_high_risk(stage: str, action: str) -> None:
    event = build_fault_event(stage=stage, action=action, team_id="judge", detail="component dropout")

    assert classify_fault_severity(event) == "high"
