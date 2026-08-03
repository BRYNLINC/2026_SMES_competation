from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.fake_algorithm_server import DeterministicFakeAlgorithmServer, build_profile


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHALLENGE_MI_FILE = (
    PROJECT_ROOT
    / "app"
    / "ProcessHub"
    / "ProcessHub"
    / "bci_competition"
    / "challenge"
    / "MI"
    / "ChallengeMI.py"
)


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("malicious_path_traversal")]


def _read_challenge_mi() -> str:
    return CHALLENGE_MI_FILE.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("SEC-PATH-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("恶意算法的 write_results/read_hidden_score/network_access/kill_process 必须被 fake profile 表达为 blocked 事件，不得真实执行副作用")
@pytest.mark.tested(file="tests/helpers/fake_algorithm_server.py", function="_run_controlled_malicious_action")
@pytest.mark.parametrize(
    "malicious_action",
    ["write_results", "read_hidden_score", "network_access", "kill_process"],
)
def test_malicious_profile_blocks_privileged_actions_as_data_events(
    tmp_path: Path,
    malicious_action: str,
) -> None:
    workspace_root = tmp_path / "workspace"
    outside_results = tmp_path / "outside_results" / "score.csv"
    outside_results.parent.mkdir(parents=True, exist_ok=True)
    outside_results.write_text("team_id,total_score\nteam_0,100\n", encoding="utf-8")

    server = DeterministicFakeAlgorithmServer(
        build_profile("malicious", workspace_root=workspace_root, malicious_action=malicious_action)
    )
    action_list = server.emit_prediction(report_source_position="trial_end")

    assert action_list[0].kind == "malicious"
    assert action_list[0].accepted is False
    assert action_list[0].payload["blocked"] is True
    assert action_list[0].payload["operation"] == malicious_action
    assert outside_results.read_text(encoding="utf-8") == "team_id,total_score\nteam_0,100\n"
    assert action_list[1].kind == "result"


@pytest.mark.test_id("SEC-PATH-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("ChallengeMI 结果目录解析只能使用框架解析出的 team_id，不得使用算法 result payload 中的路径字段作为目录片段")
@pytest.mark.tested(file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py", function="__resolve_result_dir/__persist_result_files")
def test_challenge_mi_result_paths_are_derived_from_framework_team_id_not_payload_paths() -> None:
    content = _read_challenge_mi()

    assert "return self.__resolve_results_root_dir() / self.__resolve_team_id()" in content
    assert "Path(record.get(" not in content
    assert "Path(package.result" not in content
    assert "../" not in content


@pytest.mark.test_id("SEC-PATH-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("ChallengeMI 清理旧结果只能 unlink 已知文件和 task_trials 内文件，不能递归删除 results 根目录或接受外部路径")
@pytest.mark.tested(file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py", function="__cleanup_legacy_result_files")
def test_challenge_mi_legacy_cleanup_does_not_use_recursive_delete_or_external_path_input() -> None:
    content = _read_challenge_mi()
    cleanup_body = content.split("def __cleanup_legacy_result_files", 1)[1].split("def ", 1)[0]

    assert "legacy_file_path = result_dir / legacy_file_name" in cleanup_body
    assert "task_trials_dir = result_dir / 'task_trials'" in cleanup_body
    assert "unlink()" in cleanup_body
    assert "shutil.rmtree" not in cleanup_body
    assert "os.remove" not in cleanup_body
