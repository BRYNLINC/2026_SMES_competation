from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FaultInjectionEvent:
    stage: str
    action: str
    team_id: str
    trial_id: str | None = None
    detail: str | None = None


def build_fault_event(
    *,
    stage: str,
    action: str,
    team_id: str,
    trial_id: str | None = None,
    detail: str | None = None,
) -> FaultInjectionEvent:
    return FaultInjectionEvent(
        stage=str(stage or "").strip(),
        action=str(action or "").strip(),
        team_id=str(team_id or "").strip(),
        trial_id=None if trial_id is None else str(trial_id).strip(),
        detail=None if detail is None else str(detail).strip(),
    )


def summarize_fault_event(event: FaultInjectionEvent) -> str:
    summary = f"stage={event.stage}, action={event.action}, team={event.team_id}"
    if event.trial_id:
        summary += f", trial={event.trial_id}"
    if event.detail:
        summary += f", detail={event.detail}"
    return summary


def classify_fault_severity(event: FaultInjectionEvent) -> str:
    stage_text = str(event.stage or "").strip().lower()
    action_text = str(event.action or "").strip().lower()
    if "restart" in action_text or "kill" in action_text:
        return "high"
    if "disconnect" in action_text and any(keyword in stage_text for keyword in ("online", "trial", "task_switch")):
        return "high"
    if "disconnect" in action_text or "reconnect" in action_text:
        return "medium"
    return "low"
