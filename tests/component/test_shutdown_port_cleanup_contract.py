from __future__ import annotations

from pathlib import Path

import pytest

from tools import shutdown_judge_stack as sds


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("shutdown_contract")]


@pytest.mark.test_id("COMP-SHUT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("端口清理阶段应遍历所有监听端口 PID 并累计成功清理数")
@pytest.mark.tested(
    file="tools/shutdown_judge_stack.py",
    function="_kill_listening_port_owners",
)
def test_kill_listening_port_owners_aggregates_killed_pid_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sds,
        "_find_listening_port_pid_map",
        lambda port_list: {
            18080: [101, 102],
            5173: [201],
        },
    )

    kill_call_list: list[tuple[int, bool, str]] = []

    def fake_kill_pid_tree(pid_value: int, quiet: bool, reason: str) -> bool:
        kill_call_list.append((pid_value, quiet, reason))
        return pid_value != 102

    monkeypatch.setattr(sds, "_kill_pid_tree", fake_kill_pid_tree)

    killed_count = sds._kill_listening_port_owners([18080, 5173], quiet=True)

    assert killed_count == 2
    assert sorted(kill_call_list) == sorted([
        (101, True, "port:18080"),
        (102, True, "port:18080"),
        (201, True, "port:5173"),
    ])


@pytest.mark.test_id("COMP-SHUT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("等待端口释放超时时应返回 remaining_port_pid_map，释放成功时返回空映射")
@pytest.mark.tested(
    file="tools/shutdown_judge_stack.py",
    function="_wait_for_ports_released",
)
def test_wait_for_ports_released_reports_remaining_port_pid_map(monkeypatch: pytest.MonkeyPatch) -> None:
    state_sequence = iter(
        [
            {18080: [111]},
            {18080: [111]},
            {},
        ]
    )
    monkeypatch.setattr(sds, "_find_listening_port_pid_map", lambda port_list: next(state_sequence))
    monkeypatch.setattr(sds.time, "sleep", lambda seconds: None)
    clock_value = {"value": 0.0}

    def fake_time() -> float:
        clock_value["value"] += 0.2
        return clock_value["value"]

    monkeypatch.setattr(sds.time, "time", fake_time)

    clean_shutdown, remaining_port_pid_map = sds._wait_for_ports_released([18080], timeout_seconds=1.0, quiet=True)
    assert clean_shutdown is True
    assert remaining_port_pid_map == {}


@pytest.mark.test_id("COMP-SHUT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("manifest 记录的非 cmd 进程不应被误杀")
@pytest.mark.tested(
    file="tools/shutdown_judge_stack.py",
    function="_kill_recorded_processes",
)
def test_kill_recorded_processes_skips_non_cmd_processes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sds.write_process_manifest(
        tmp_path,
        [
            {"pid": 1001, "title": "[BCI Judge] JudgeWeb"},
            {"pid": 1002, "title": "[BCI Judge] Dashboard"},
        ],
    )
    monkeypatch.setattr(sds, "_query_process_name", lambda pid: "cmd.exe" if pid == 1001 else "python.exe")

    killed_pid_list: list[int] = []

    def fake_kill_pid_tree(pid_value: int, quiet: bool, reason: str) -> bool:
        killed_pid_list.append(pid_value)
        return True

    monkeypatch.setattr(sds, "_kill_pid_tree", fake_kill_pid_tree)

    killed_count = sds._kill_recorded_processes(tmp_path, quiet=True)

    assert killed_count == 1
    assert killed_pid_list == [1001]
