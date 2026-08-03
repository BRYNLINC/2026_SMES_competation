from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TASK_FILE = PROJECT_ROOT / "app" / "ProcessHub" / "ProcessHub" / "bci_competition" / "task" / "BCICompetitionTaskFinal.py"


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("task_disconnect_reconnect")]


def _read_task_file() -> str:
    return TASK_FILE.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("RES-TASK-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("当前 task 掉线后必须把队伍 live 状态标记为 disconnected，并声明本 task 后续按 timeout 处理、下一个 task 再尝试恢复")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__mark_algorithm_disconnected",
)
def test_task_runtime_marks_disconnected_team_with_forfeit_and_recovery_advice() -> None:
    content = _read_task_file()

    assert "self.__algorithm_disconnected_for_current_task = True" in content
    assert "connection_status='disconnected'" in content
    assert "recovery_advice='当前 task 后续按 timeout 处理，下一个 task 再尝试恢复'" in content
    assert "forfeit_current_task=True" in content
    assert "forfeit_task_signature=self.__serialize_task_signature(self.__disconnected_task_signature)" in content


@pytest.mark.test_id("RES-TASK-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("进入新 task 时若上一 task 已掉线，系统必须先把旧 task 残留 pending trial 按 timeout 结算，再进入 reconnecting")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__maybe_handle_task_boundary_for_reconnect/__attempt_reconnect_for_new_task",
)
def test_task_runtime_reconnects_only_at_task_boundary_after_cleanup() -> None:
    content = _read_task_file()

    assert "await self.__attempt_reconnect_for_new_task(current_task_signature)" in content
    assert "await self.__force_timeout_pending_trials_for_task_signature(" in content
    assert 'reason="task_boundary_cleanup_before_reconnect"' in content
    assert "allow_stale_task_signature=True" in content
    assert "connection_status='reconnecting'" in content
    assert "recovery_advice='检测到新 task，正在尝试恢复连接'" in content
    assert "allow_current_task_join=True" in content


@pytest.mark.test_id("RES-TASK-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("掉线后同一 task 的后续 trial 必须允许立即强制 timeout，避免继续等待常规 1 秒截止")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__should_force_immediate_timeout_for_trial/__force_timeout_for_disconnected_trial",
)
def test_task_runtime_forces_immediate_timeout_for_disconnected_trials_in_same_task() -> None:
    content = _read_task_file()

    assert "if not self.__algorithm_disconnected_for_current_task:" in content
    assert "return task_signature == self.__disconnected_task_signature" in content
    assert 'cancel_reason="forced_timeout_after_disconnect"' in content
    assert "后续 trial 直接按 timeout 处理" in content
    assert "await self.__handle_trial_timeout(int(trial_end_position))" in content


@pytest.mark.test_id("RES-TASK-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("若算法始终未连上但输入流已结束，任务必须以 disconnected 收尾，而不是无限等待")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__maybe_finalize_unconnected_run_after_stream_end",
)
def test_task_runtime_finalizes_unconnected_run_after_stream_completion() -> None:
    content = _read_task_file()

    assert "finish_reason='algorithm never connected but stream completed'" in content
    assert "connection_status='disconnected'" in content
    assert "if self.__algorithm_connection_ready:" in content
    assert "if not self.__input_stream_finished:" in content


@pytest.mark.test_id("RES-TASK-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("平台先启动、算法后启动时，只允许短暂缓存校准私有流；online/predict 仍按平台实时计时和 timeout 处理")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskFinal.py",
    function="__should_buffer_startup_calibration_message/__replay_startup_unconnected_calibration_message_buffer/__establish_algorithm_runtime_session",
)
def test_task_runtime_buffers_only_startup_calibration_before_algorithm_connects() -> None:
    content = _read_task_file()

    assert "self.__startup_unconnected_calibration_message_buffer = deque()" in content
    assert "def __should_buffer_startup_calibration_message(" in content
    assert "algorithm_data_message_model.source_label != self.__CALIBRATION_PRIVATE_SOURCE_LABEL" in content
    assert "self.__buffer_startup_unconnected_calibration_message(algorithm_data_message_model)" in content
    assert "await self.__replay_startup_unconnected_calibration_message_buffer()" in content
    assert "self.__mark_startup_online_message_seen_if_needed(algorithm_data_message_model)" in content
    assert "算法尚未接入，缓存启动期校准消息等待回放" in content
    assert "启动期校准消息已回放到算法" in content
    assert "已回放启动期校准缓存并纳入当前 task" in content
    assert "算法接入前当前 task 已进入 online，丢弃启动期校准缓存并保持当前 task 按 timeout 处理" in content
    assert "await self.__update_trial_timing_context(algorithm_data_message_model)" in content
