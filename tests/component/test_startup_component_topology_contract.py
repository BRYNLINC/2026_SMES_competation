from __future__ import annotations

from pathlib import Path

import pytest

from tools import start_judge_stack as sjs


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("startup_topology")]


@pytest.mark.test_id("COMP-TOPO-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("裁判机启动顺序必须满足核心组件拓扑：中心桥接、控制器、协调器、采集桥接、Collector、各队 ProcessHub、JudgeWeb、Dashboard")
@pytest.mark.tested(
    file="tools/start_judge_stack.py",
    function="main",
)
def test_main_launches_components_in_expected_topology_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launched: list[dict] = []
    written_manifests: list[dict] = []
    (tmp_path / "judge-dashboard").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(sjs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(sjs, "APP_ROOT", tmp_path / "app")
    monkeypatch.setattr(sjs, "PROCEED_ROOT", tmp_path / "proceed")
    monkeypatch.setattr(sjs, "DASHBOARD_ROOT", tmp_path / "judge-dashboard")
    monkeypatch.setattr(sjs, "RESULTS_ROOT", tmp_path / "results")
    monkeypatch.setattr(sjs, "CONTROL_ROOT", tmp_path / "results" / "control")
    monkeypatch.setattr(sjs, "RUNTIME_STAGE_LAUNCHER_CONFIG_PATH", tmp_path / "config" / "runtime.yml")
    monkeypatch.setattr(sjs, "ensure_runtime_directories", lambda: None)
    monkeypatch.setattr(
        sjs,
        "shutdown_judge_runtime",
        lambda **kwargs: {
            "clean_shutdown": True,
            "remaining_port_pid_map": {},
        },
    )
    monkeypatch.setattr(
        sjs,
        "prepare_results_root",
        lambda mode: {
            "recovery_mode": "clear_start",
            "stage": None,
            "collector_start_selector": None,
        },
    )
    monkeypatch.setattr(sjs, "write_launcher_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        sjs,
        "write_process_manifest",
        lambda project_root, process_row_list, metadata=None: written_manifests.append(
            {
                "process_count": len(process_row_list),
                "metadata": metadata,
            }
        ),
    )
    monkeypatch.setattr(sjs, "load_processor_component_id_list", lambda: ["team_0.group_1", "team_1.group_1"])
    monkeypatch.setattr(sjs, "normalize_executable_path", lambda raw: raw)
    monkeypatch.setattr(sjs, "resolve_npm_executable", lambda: "npm.cmd")
    monkeypatch.setattr(sjs, "build_process_command", lambda parts: " ".join(parts))
    monkeypatch.setattr(sjs.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(sjs, "wait_for_http_service", lambda *args, **kwargs: True)

    def _fake_start_component_window(title: str, cwd: Path, command: str, extra_env: dict | None = None) -> dict:
        row = {
            "title": title,
            "cwd": str(cwd),
            "command": command,
            "extra_env": extra_env or {},
            "pid": len(launched) + 1000,
            "mode": "window",
        }
        launched.append(row)
        return row

    monkeypatch.setattr(sjs, "start_component_window", _fake_start_component_window)

    exit_code = sjs.main()

    assert exit_code == 0
    assert [row["title"] for row in launched] == [
        "[BCI Judge] Central Java Controller",
        "[BCI Judge] CentralController Python",
        "[BCI Judge] RuntimeStageCoordinator Python",
        "[BCI Judge] Collector Java Bridge",
        "[BCI Judge] Task Java Bridge",
        "[BCI Judge] Collector Python",
        "[BCI Judge] ProcessHub team_0.group_1",
        "[BCI Judge] ProcessHub team_1.group_1",
        "[BCI Judge] JudgeWeb",
        "[BCI Judge] Judge Dashboard",
    ]
    assert launched[2]["extra_env"] == {"LAUNCHER_CONFIG_PATH": str(tmp_path / "config" / "runtime.yml")}
    assert launched[6]["extra_env"] == {"COMPONENT_ID": "team_0.group_1"}
    assert launched[7]["extra_env"] == {"COMPONENT_ID": "team_1.group_1"}
    assert written_manifests[-1]["process_count"] == 10
    assert written_manifests[-1]["metadata"]["match_start_mode"] == "clear"


@pytest.mark.test_id("COMP-TOPO-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("未检测到 npm 时，启动流程应跳过 dashboard，但其他裁判组件仍完整启动")
@pytest.mark.tested(
    file="tools/start_judge_stack.py",
    function="main",
)
def test_main_skips_dashboard_when_npm_is_not_available(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launched_titles: list[str] = []
    (tmp_path / "judge-dashboard").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(sjs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(sjs, "APP_ROOT", tmp_path / "app")
    monkeypatch.setattr(sjs, "PROCEED_ROOT", tmp_path / "proceed")
    monkeypatch.setattr(sjs, "DASHBOARD_ROOT", tmp_path / "judge-dashboard")
    monkeypatch.setattr(sjs, "RESULTS_ROOT", tmp_path / "results")
    monkeypatch.setattr(sjs, "CONTROL_ROOT", tmp_path / "results" / "control")
    monkeypatch.setattr(sjs, "ensure_runtime_directories", lambda: None)
    monkeypatch.setattr(sjs, "shutdown_judge_runtime", lambda **kwargs: {"clean_shutdown": True, "remaining_port_pid_map": {}})
    monkeypatch.setattr(
        sjs,
        "prepare_results_root",
        lambda mode: {"recovery_mode": "clear_start", "stage": None, "collector_start_selector": None},
    )
    monkeypatch.setattr(sjs, "write_launcher_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(sjs, "write_process_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(sjs, "load_processor_component_id_list", lambda: ["team_0.group_1"])
    monkeypatch.setattr(sjs, "resolve_npm_executable", lambda: None)
    monkeypatch.setattr(sjs, "build_process_command", lambda parts: " ".join(parts))
    monkeypatch.setattr(sjs, "normalize_executable_path", lambda raw: raw)
    monkeypatch.setattr(sjs.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(sjs, "wait_for_http_service", lambda *args, **kwargs: True)

    def _fake_start_component_window(title: str, cwd: Path, command: str, extra_env: dict | None = None) -> dict:
        launched_titles.append(title)
        return {"title": title, "pid": len(launched_titles) + 1000, "cwd": str(cwd), "command": command, "mode": "window"}

    monkeypatch.setattr(sjs, "start_component_window", _fake_start_component_window)

    exit_code = sjs.main()

    assert exit_code == 0
    assert "[BCI Judge] Judge Dashboard" not in launched_titles
    assert launched_titles[-1] == "[BCI Judge] JudgeWeb"


@pytest.mark.test_id("COMP-TOPO-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("预清理后若裁判侧端口仍未释放，启动流程必须立即失败，不得继续拉起任何组件")
@pytest.mark.tested(
    file="tools/start_judge_stack.py",
    function="main",
)
def test_main_aborts_without_launching_components_when_preflight_shutdown_is_not_clean(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launched = []

    monkeypatch.setattr(sjs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(sjs, "RESULTS_ROOT", tmp_path / "results")
    monkeypatch.setattr(sjs, "CONTROL_ROOT", tmp_path / "results" / "control")
    monkeypatch.setattr(sjs, "ensure_runtime_directories", lambda: None)
    monkeypatch.setattr(
        sjs,
        "shutdown_judge_runtime",
        lambda **kwargs: {
            "clean_shutdown": False,
            "remaining_port_pid_map": {18080: 4321},
        },
    )
    monkeypatch.setattr(sjs, "start_component_window", lambda *args, **kwargs: launched.append((args, kwargs)))

    exit_code = sjs.main()

    assert exit_code == 1
    assert launched == []
