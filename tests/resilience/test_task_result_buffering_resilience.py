from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TASK_FILE = PROJECT_ROOT / "app" / "ProcessHub" / "ProcessHub" / "bci_competition" / "task" / "BCICompetitionTaskFinal.py"


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("task_result_buffering")]


def _read_task_file() -> str:
    return TASK_FILE.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("RES-BUFFER-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("同一 trial_end_position 的重复结果必须被忽略，避免 duplicate_result 污染当前或后续 trial")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__buffer_result_until_trial_ready",
)
def test_task_runtime_ignores_duplicate_buffered_results_for_same_trial_end_position() -> None:
    content = _read_task_file()

    assert "if normalized_trial_end_position in self.__buffered_result_by_end_position:" in content
    assert "忽略后到重复结果" in content
    assert "self.__buffered_result_by_end_position[normalized_trial_end_position] =" in content


@pytest.mark.test_id("RES-BUFFER-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("晚到结果若命中已 timeout 的 trial_end_position，必须被标记 discarded，不能回放进已终态 trial")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__consume_pending_trial_timing_for_result",
)
def test_task_runtime_discards_late_results_for_already_timed_out_trials() -> None:
    content = _read_task_file()

    assert "if trial_end_position is not None and trial_end_position in self.__timed_out_trial_end_position_set:" in content
    assert "self.__timed_out_trial_end_position_set.remove(trial_end_position)" in content
    assert "'timeout_discarded': True" in content


@pytest.mark.test_id("RES-BUFFER-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("早到结果必须先缓存，等 trial ready 后再回放；若已无 pending trial，则只能告警不能污染记分")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__buffer_result_until_trial_ready/__replay_buffered_result_after_trial_ready",
)
def test_task_runtime_buffers_early_results_and_warns_when_replay_target_is_missing() -> None:
    content = _read_task_file()

    assert "缓存早到算法结果，等待 trial ready 后回放" in content
    assert "buffered_result = self.__buffered_result_by_end_position.pop(int(trial_end_position), None)" in content
    assert 'remove_reason="buffered_result_replay"' in content
    assert "检测到缓存结果但未找到对应 pending trial，无法回放" in content


@pytest.mark.test_id("RES-BUFFER-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("跨 task 的 timeout 任务必须被丢弃，避免 task 切换后旧 trial 的晚到 timeout 串扰记分")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__handle_trial_timeout",
)
def test_task_runtime_drops_stale_timeout_tasks_from_previous_task() -> None:
    content = _read_task_file()

    assert "timeout_task_signature=%s current_task_signature=%s" in content
    assert "检测到过期 task 的 timeout 任务，已丢弃避免跨 task 记分" in content
    assert "task_signature != current_task_signature" in content
