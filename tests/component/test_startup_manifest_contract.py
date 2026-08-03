from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import shutdown_judge_stack as sds
from tools import start_judge_stack as sjs


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("startup_manifest")]


@pytest.mark.test_id("COMP-START-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("launcher manifest 和 process manifest 应共享恢复信息与关键路径元数据")
@pytest.mark.tested(
    file="tools/start_judge_stack.py;tools/shutdown_judge_stack.py",
    function="write_launcher_manifest/write_process_manifest",
)
def test_launcher_manifest_and_process_manifest_share_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    control_root = tmp_path / "results" / "control"
    monkeypatch.setattr(sjs, "CONTROL_ROOT", control_root)
    monkeypatch.setattr(sds, "CONTROL_ROOT", control_root)
    monkeypatch.setattr(sjs, "load_processor_component_id_list", lambda: ["team_0.group_1", "team_1.group_1"])
    monkeypatch.setattr(sjs, "_resolve_process_manifest_path", lambda: control_root / "judge_process_manifest.json")

    applied_recovery = {
        "recovery_mode": "resume_ready",
        "stage": {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"},
    }
    sjs.write_launcher_manifest(
        match_start_mode="resume",
        python_executable="python.exe",
        java_executable="java.exe",
        npm_executable="npm.cmd",
        applied_recovery=applied_recovery,
    )
    sds.write_process_manifest(
        tmp_path,
        [
            {
                "pid": 1234,
                "title": "[BCI Judge] JudgeWeb",
                "cwd": str(tmp_path / "app" / "JudgeWeb"),
            }
        ],
        metadata={
            "match_start_mode": "resume",
            "applied_recovery": applied_recovery,
        },
    )

    launcher_manifest = json.loads((control_root / "launcher_manifest.json").read_text(encoding="utf-8"))
    process_manifest = json.loads((control_root / "judge_process_manifest.json").read_text(encoding="utf-8"))

    assert launcher_manifest["match_start_mode"] == "resume"
    assert launcher_manifest["processor_component_id_list"] == ["team_0.group_1", "team_1.group_1"]
    assert launcher_manifest["applied_recovery"] == applied_recovery
    assert process_manifest["metadata"]["match_start_mode"] == "resume"
    assert process_manifest["metadata"]["applied_recovery"] == applied_recovery
    assert process_manifest["processes"][0]["title"] == "[BCI Judge] JudgeWeb"


@pytest.mark.test_id("COMP-START-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("process manifest 路径解析应落在 results/control 下")
@pytest.mark.tested(
    file="tools/shutdown_judge_stack.py",
    function="_resolve_process_manifest_path",
)
def test_process_manifest_path_is_written_under_results_control(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sds, "PROJECT_ROOT", tmp_path)
    resolved = sds._resolve_process_manifest_path(tmp_path)

    assert resolved == tmp_path / "results" / "control" / "judge_process_manifest.json"


@pytest.mark.test_id("COMP-START-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("shutdown report 结构应包含清理计数、端口状态和 clean_shutdown 标志")
@pytest.mark.tested(
    file="tools/shutdown_judge_stack.py",
    function="shutdown_judge_runtime",
)
def test_shutdown_report_contains_expected_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sds, "_kill_recorded_processes", lambda project_root, quiet: 2)
    monkeypatch.setattr(sds, "_kill_title_window_processes", lambda quiet: True)
    monkeypatch.setattr(sds, "_kill_listening_port_owners", lambda port_list, quiet: 3)
    monkeypatch.setattr(sds, "_wait_for_ports_released", lambda port_list, timeout_seconds, quiet: (True, {}))

    report = sds.shutdown_judge_runtime(
        project_root=tmp_path,
        reason="test_shutdown",
        quiet=True,
        timeout_seconds=1.0,
    )

    assert report == {
        "reason": "test_shutdown",
        "manifest_kill_count": 2,
        "title_killed": True,
        "port_kill_count": 3,
        "clean_shutdown": True,
        "remaining_port_pid_map": {},
    }
