from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.fake_algorithm_server import DeterministicFakeAlgorithmServer, build_profile


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("malicious_algorithm_action_matrix")]


@pytest.mark.test_id("SEC-MAL-ACTION-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("恶意算法尝试读取隐藏真值、写 results、外联网络、杀进程时，fake profile 必须以受控 blocked 事件表达，不执行真实副作用")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="emit_prediction/_run_controlled_malicious_action",
)
@pytest.mark.parametrize(
    "malicious_action",
    ["read_hidden_score", "write_results", "network_access", "kill_process"],
)
def test_malicious_algorithm_controlled_actions_are_blocked_without_real_side_effects(
    tmp_path: Path,
    malicious_action: str,
) -> None:
    workspace_root = tmp_path / "workspace"
    protected_file = tmp_path / "protected_results.csv"
    protected_file.write_text("team_id,total_score\nteam_0,1\n", encoding="utf-8")
    server = DeterministicFakeAlgorithmServer(
        build_profile("malicious", workspace_root=workspace_root, malicious_action=malicious_action)
    )

    actions = server.emit_prediction(report_source_position="trial_end")
    malicious_event = actions[0]

    assert malicious_event.kind == "malicious"
    assert malicious_event.accepted is False
    assert malicious_event.payload["operation"] == malicious_action
    assert malicious_event.payload["blocked"] is True
    assert protected_file.read_text(encoding="utf-8") == "team_id,total_score\nteam_0,1\n"
    assert actions[1].kind == "result"
