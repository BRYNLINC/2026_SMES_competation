from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.fake_algorithm_server import DeterministicFakeAlgorithmServer, build_profile


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("fake_algorithm_security")]


@pytest.mark.test_id("SEC-FAKE-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("malicious profile 的受控写入只能发生在指定 workspace 内，不能越界污染 sandbox 外路径")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="emit_prediction",
)
def test_fake_algorithm_malicious_write_marker_is_constrained_to_workspace(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    outside_marker = tmp_path / "outside.txt"
    outside_marker.write_text("safe", encoding="utf-8")
    server = DeterministicFakeAlgorithmServer(
        build_profile("malicious", workspace_root=workspace_root, malicious_action="write_marker")
    )

    actions = server.emit_prediction(report_source_position="trial_end")
    marker_path = workspace_root / "malicious_touch.txt"

    assert marker_path.exists()
    assert marker_path.read_text(encoding="utf-8") == "sandbox-only"
    assert outside_marker.read_text(encoding="utf-8") == "safe"
    assert actions[0].kind == "malicious"
    assert actions[0].accepted is False


@pytest.mark.test_id("SEC-FAKE-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("malicious profile 的 list_workspace 只能枚举 workspace 内条目，不能尝试外联或访问 sandbox 外目录")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="emit_prediction",
)
def test_fake_algorithm_malicious_list_workspace_only_exposes_workspace_entries(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True, exist_ok=True)
    (workspace_root / "a.txt").write_text("A", encoding="utf-8")
    (workspace_root / "b.txt").write_text("B", encoding="utf-8")
    server = DeterministicFakeAlgorithmServer(
        build_profile("malicious", workspace_root=workspace_root, malicious_action="list_workspace")
    )

    actions = server.emit_prediction(report_source_position="trial_end")
    malicious_action = actions[0]

    assert malicious_action.kind == "malicious"
    assert malicious_action.payload["workspace_items"] == ["a.txt", "b.txt"]
    assert "operation" in malicious_action.payload


@pytest.mark.test_id("SEC-FAKE-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("invalid_output profile 必须可稳定生成非 JSON、缺字段和超大 payload，供恶意输出防御测试复用")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="emit_prediction",
)
def test_fake_algorithm_invalid_output_profiles_generate_attack_shaped_payloads_without_side_effects() -> None:
    non_json_server = DeterministicFakeAlgorithmServer(build_profile("invalid_output", payload_type="non_json_string"))
    missing_field_server = DeterministicFakeAlgorithmServer(build_profile("invalid_output", payload_type="missing_predict_label"))
    oversized_server = DeterministicFakeAlgorithmServer(build_profile("invalid_output", payload_type="oversized_payload"))

    non_json_action = non_json_server.emit_prediction(report_source_position="trial_end")[0]
    missing_field_action = missing_field_server.emit_prediction(report_source_position="trial_end")[0]
    oversized_action = oversized_server.emit_prediction(report_source_position="trial_end")[0]

    assert non_json_action.payload == "predict_label=0"
    assert "predict_label" not in missing_field_action.payload
    assert len(oversized_action.payload) == 16384
    assert non_json_action.reason == "invalid_output:non_json_string"
    assert missing_field_action.reason == "invalid_output:missing_predict_label"
    assert oversized_action.reason == "invalid_output:oversized_payload"
