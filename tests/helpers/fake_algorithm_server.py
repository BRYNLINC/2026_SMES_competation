from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FakeAlgorithmProfile:
    name: str
    predict_label: int = 0
    latency_ms: int = 50
    still_send_after_timeout: bool = False
    exit_code: int = 1
    after_message_count: int = 0
    payload_type: str = "valid_json"
    duplicate_count: int = 2
    malicious_action: str | None = None
    workspace_root: Path | None = None
    predict_timeout_ms: int = 1000
    cpu_burn_ms: int = 0
    memory_blob_kb: int = 0


@dataclass
class FakeAlgorithmObservation:
    received_config: dict[str, Any] | None = None
    source_labels: list[str] = field(default_factory=list)
    calibration_packet_count: int = 0
    control_packet_count: int = 0
    data_packet_count: int = 0
    event_packet_count: int = 0
    prediction_submit_times_ms: list[int] = field(default_factory=list)
    disconnect_reasons: list[str] = field(default_factory=list)
    exit_code: int | None = None
    exception_text: str | None = None
    resource_hog_events: list[dict[str, int]] = field(default_factory=list)

    @property
    def total_packet_count(self) -> int:
        return (
            self.calibration_packet_count
            + self.control_packet_count
            + self.data_packet_count
            + self.event_packet_count
        )


@dataclass(frozen=True)
class FakeAlgorithmAction:
    kind: str
    payload: Any = None
    latency_ms: int = 0
    accepted: bool = True
    reason: str | None = None
    duplicate_index: int = 0


def build_profile(profile_name: str, **overrides: Any) -> FakeAlgorithmProfile:
    normalized_name = str(profile_name or "").strip().lower()
    default_map: dict[str, dict[str, Any]] = {
        "normal": {"predict_label": 0, "latency_ms": 50},
        "slow": {"predict_label": 0, "latency_ms": 1101},
        "late_result": {"predict_label": 0, "latency_ms": 1200, "still_send_after_timeout": True},
        "crash_on_connect": {"exit_code": 17},
        "crash_on_calibration": {"exit_code": 18},
        "crash_on_predict": {"exit_code": 19},
        "disconnect_stream": {"after_message_count": 1},
        "invalid_output": {"payload_type": "missing_predict_label"},
        "duplicate_result": {"duplicate_count": 2},
        "resource_hog": {"cpu_burn_ms": 1500, "memory_blob_kb": 2048, "latency_ms": 1500},
        "malicious": {"malicious_action": "write_marker"},
    }
    payload = default_map.get(normalized_name, {})
    payload = {**payload, **overrides}
    payload["name"] = normalized_name
    return FakeAlgorithmProfile(**payload)


def describe_profile(profile: FakeAlgorithmProfile) -> str:
    detail_parts = [profile.name]
    if profile.name in {"normal", "slow", "late_result"}:
        detail_parts.append(f"label={profile.predict_label}")
        detail_parts.append(f"latency_ms={profile.latency_ms}")
    if profile.name.startswith("crash_"):
        detail_parts.append(f"exit_code={profile.exit_code}")
    if profile.name == "disconnect_stream":
        detail_parts.append(f"after_message_count={profile.after_message_count}")
    if profile.name == "invalid_output":
        detail_parts.append(f"payload_type={profile.payload_type}")
    if profile.name == "duplicate_result":
        detail_parts.append(f"duplicate_count={profile.duplicate_count}")
    if profile.name == "resource_hog":
        detail_parts.append(f"cpu_burn_ms={profile.cpu_burn_ms}")
        detail_parts.append(f"memory_blob_kb={profile.memory_blob_kb}")
    if profile.name == "malicious":
        detail_parts.append(f"action={profile.malicious_action or 'none'}")
    return ", ".join(detail_parts)


class DeterministicFakeAlgorithmServer:
    def __init__(self, profile: FakeAlgorithmProfile):
        self.profile = profile
        self.observation = FakeAlgorithmObservation()

    def receive_config(self, config: dict[str, Any]) -> None:
        self.observation.received_config = json.loads(json.dumps(config))

    def receive_sources(self, source_labels: list[str]) -> None:
        self.observation.source_labels = [str(source_label) for source_label in source_labels]

    def connect(self, source_labels: list[str] | None = None) -> list[FakeAlgorithmAction]:
        if source_labels is not None:
            self.receive_sources(source_labels)
        if self.profile.name == "crash_on_connect":
            return [self._crash_action("crash_on_connect")]
        return []

    def record_packet(self, packet_kind: str, payload: dict[str, Any] | None = None) -> list[FakeAlgorithmAction]:
        normalized_kind = str(packet_kind or "").strip().lower()
        if normalized_kind == "calibration":
            self.observation.calibration_packet_count += 1
        elif normalized_kind == "control":
            self.observation.control_packet_count += 1
        elif normalized_kind == "data":
            self.observation.data_packet_count += 1
        else:
            self.observation.event_packet_count += 1

        if self.profile.name == "disconnect_stream":
            threshold = max(1, int(self.profile.after_message_count or 1))
            if self.observation.total_packet_count >= threshold:
                reason = f"disconnect_stream_after_{threshold}_packets"
                self.observation.disconnect_reasons.append(reason)
                return [FakeAlgorithmAction(kind="disconnect", accepted=False, reason=reason)]

        if self.profile.name == "crash_on_calibration" and normalized_kind == "calibration":
            return [self._crash_action("crash_on_calibration")]

        if self.profile.name == "crash_on_predict" and normalized_kind == "event":
            event_name = str((payload or {}).get("event") or "").strip().lower()
            if event_name == "trial_end":
                return [self._crash_action("crash_on_predict")]

        return []

    def emit_prediction(
        self,
        *,
        report_source_position: str,
        now_ms: int = 0,
    ) -> list[FakeAlgorithmAction]:
        self.observation.prediction_submit_times_ms.append(int(now_ms))

        if self.profile.name == "slow":
            return [
                FakeAlgorithmAction(
                    kind="timeout",
                    accepted=False,
                    latency_ms=int(self.profile.latency_ms),
                    reason="predict_timeout_exceeded",
                )
            ]

        if self.profile.name == "late_result":
            return [
                FakeAlgorithmAction(
                    kind="result",
                    payload=self._build_valid_payload(report_source_position),
                    latency_ms=int(self.profile.latency_ms),
                    accepted=False,
                    reason="late_result_after_timeout",
                )
            ]

        if self.profile.name == "crash_on_predict":
            return [self._crash_action("crash_on_predict")]

        if self.profile.name == "invalid_output":
            return [
                FakeAlgorithmAction(
                    kind="result",
                    payload=self._build_invalid_payload(),
                    latency_ms=int(self.profile.latency_ms),
                    accepted=True,
                    reason=f"invalid_output:{self.profile.payload_type}",
                )
            ]

        if self.profile.name == "duplicate_result":
            action_list: list[FakeAlgorithmAction] = []
            valid_payload = self._build_valid_payload(report_source_position)
            duplicate_total = max(2, int(self.profile.duplicate_count))
            for duplicate_index in range(duplicate_total):
                action_list.append(
                    FakeAlgorithmAction(
                        kind="result",
                        payload=valid_payload,
                        latency_ms=int(self.profile.latency_ms),
                        accepted=duplicate_index == 0,
                        reason=None if duplicate_index == 0 else "duplicate_result",
                        duplicate_index=duplicate_index,
                    )
                )
            return action_list

        if self.profile.name == "resource_hog":
            resource_hog_payload = {
                "cpu_burn_ms": max(0, int(self.profile.cpu_burn_ms)),
                "memory_blob_kb": max(0, int(self.profile.memory_blob_kb)),
            }
            self.observation.resource_hog_events.append(dict(resource_hog_payload))
            return [
                FakeAlgorithmAction(
                    kind="resource_hog",
                    payload=resource_hog_payload,
                    latency_ms=max(int(self.profile.latency_ms), int(self.profile.predict_timeout_ms)),
                    accepted=False,
                    reason="resource_hog_profile",
                )
            ]

        if self.profile.name == "malicious":
            return self._run_controlled_malicious_action(report_source_position)

        return [
            FakeAlgorithmAction(
                kind="result",
                payload=self._build_valid_payload(report_source_position),
                latency_ms=int(self.profile.latency_ms),
                accepted=True,
            )
        ]

    def snapshot(self) -> FakeAlgorithmObservation:
        return FakeAlgorithmObservation(
            received_config=json.loads(json.dumps(self.observation.received_config))
            if self.observation.received_config is not None
            else None,
            source_labels=list(self.observation.source_labels),
            calibration_packet_count=self.observation.calibration_packet_count,
            control_packet_count=self.observation.control_packet_count,
            data_packet_count=self.observation.data_packet_count,
            event_packet_count=self.observation.event_packet_count,
            prediction_submit_times_ms=list(self.observation.prediction_submit_times_ms),
            disconnect_reasons=list(self.observation.disconnect_reasons),
            exit_code=self.observation.exit_code,
            exception_text=self.observation.exception_text,
            resource_hog_events=list(self.observation.resource_hog_events),
        )

    def _build_valid_payload(self, report_source_position: str) -> str:
        return json.dumps(
            {
                "predict_label": int(self.profile.predict_label),
                "report_source_position": str(report_source_position),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    def _build_invalid_payload(self) -> Any:
        payload_type = str(self.profile.payload_type or "").strip().lower()
        if payload_type == "non_json_string":
            return "predict_label=0"
        if payload_type == "missing_predict_label":
            return json.dumps({"report_source_position": "trial_end"}, ensure_ascii=False, sort_keys=True)
        if payload_type == "unknown_fields":
            return json.dumps({"predict_label": 0, "exploit": "netcat"}, ensure_ascii=False, sort_keys=True)
        if payload_type == "oversized_payload":
            return "X" * 16384
        return None

    def _crash_action(self, reason: str) -> FakeAlgorithmAction:
        self.observation.exit_code = int(self.profile.exit_code)
        self.observation.exception_text = reason
        return FakeAlgorithmAction(
            kind="crash",
            accepted=False,
            reason=reason,
        )

    def _run_controlled_malicious_action(self, report_source_position: str) -> list[FakeAlgorithmAction]:
        workspace_root = Path(self.profile.workspace_root or ".").resolve()
        workspace_root.mkdir(parents=True, exist_ok=True)
        malicious_action = str(self.profile.malicious_action or "write_marker").strip().lower()

        if malicious_action == "write_marker":
            marker_path = workspace_root / "malicious_touch.txt"
            marker_path.write_text("sandbox-only", encoding="utf-8")
            detail_payload = {"touched_path": str(marker_path), "operation": malicious_action}
        elif malicious_action == "list_workspace":
            visible_items = sorted(path.name for path in workspace_root.iterdir())
            detail_payload = {"workspace_items": visible_items, "operation": malicious_action}
        elif malicious_action in {"read_hidden_score", "write_results", "network_access", "kill_process"}:
            detail_payload = {
                "operation": malicious_action,
                "blocked": True,
                "workspace_root": str(workspace_root),
            }
        else:
            detail_payload = {"operation": malicious_action, "status": "no_op"}

        return [
            FakeAlgorithmAction(
                kind="malicious",
                payload=detail_payload,
                accepted=False,
                reason="controlled_malicious_profile",
            ),
            FakeAlgorithmAction(
                kind="result",
                payload=self._build_valid_payload(report_source_position),
                latency_ms=int(self.profile.latency_ms),
                accepted=True,
            ),
        ]
