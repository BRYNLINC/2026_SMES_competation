from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest
import yaml

from tests.helpers.heavy_runtime import (
    HEAVY_BASE_PORT,
    HEAVY_ALGORITHM_CPU_THREAD_COUNT,
    HEAVY_DEFAULT_MATCH_TIMEOUT_SECONDS,
    HEAVY_DEFAULT_TEAM_COUNT,
    HEAVY_DEFAULT_DATASET_SUBJECT_COUNT,
    HEAVY_MATCH_TIMEOUT_SECONDS,
    HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS,
    HEAVY_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS,
    HeavyEnvironment,
    HeavyProgressReporter,
    LocalJsonFetchResult,
    _collect_heavy_algorithm_process_exit_parts,
    _collect_heavy_current_trial_failure_parts,
    _collect_heavy_log_pattern_hits,
    _collect_heavy_runtime_failure_detail_parts,
    _collect_heavy_team_state_failure_parts,
    _build_heavy_dataset_spec,
    _assert_formal_runtime_state_bootstrap,
    _build_formal_start_readiness_failure_detail,
    _build_missing_runtime_state_failure_detail,
    _collect_real_heavy_source_sample_pairs,
    _collect_representative_headless_log_tails,
    _emit_heavy_console_text,
    _extract_runtime_stage_progress,
    _fetch_json_via_powershell,
    _format_local_json_fetch_detail,
    _ignore_heavy_workspace_dashboard_copy,
    _ignore_heavy_workspace_proceed_copy,
    _detect_heavy_runtime_failure,
    _raise_on_heavy_runtime_failure_detected,
    _resolve_heavy_match_progress_ratio,
    _resolve_float_env,
    _resolve_waiting_start_readiness_progress_ratio,
    _summarize_start_readiness_progress,
    _summarize_runtime_state_bootstrap_state,
    _summarize_team_state_snapshot,
    _summarize_heavy_runtime_progress,
    _trigger_formal_start_match,
    _wait_for_formal_start_readiness,
    dump_heavy_environment_snapshot,
    preserve_heavy_failure_artifacts,
    prepare_heavy_workspace,
    _prepare_team_algorithm_workspaces,
    _patch_headless_start_judge_stack,
    _patch_central_controller_component_monitor_for_heavy,
    assert_heavy_completion_outputs,
    build_heavy_team_scenarios,
    shutdown_heavy_environment,
    shutdown_and_preserve_heavy_failure_artifacts,
)
from tests.helpers.config_factory import build_team_config
from tests.helpers.process_runner import ManagedProcess
from tools.runtime_state_sqlite import (
    STATE_KEY_CURRENT_TRIAL,
    STATE_KEY_MATCH_CONTROL_STATUS,
    STATE_KEY_RUNTIME_STAGE_STATUS,
    ensure_runtime_state_schema,
    read_json_state,
    replace_team_subject_task_overview_rows,
    replace_team_task_overview_rows,
    replace_team_trial_record_rows,
    resolve_runtime_state_db_path,
    write_json_state,
    write_team_score_overview_row,
)
from tests import conftest as test_conftest


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("heavy_runtime")]


@pytest.mark.test_id("HEAVY-HELPER-00P")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy workspace must not copy generated Kafka runtime state from proceed/centrol/runtime")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_ignore_heavy_workspace_proceed_copy")
def test_heavy_workspace_proceed_copy_excludes_generated_runtime_only() -> None:
    ignored_names = _ignore_heavy_workspace_proceed_copy(
        str(Path("proceed") / "centrol"),
        ["runtime", "centrol.jar", "src", "patches", ".idea", "__pycache__"],
    )

    assert ignored_names == {"runtime", ".idea", "__pycache__"}


@pytest.mark.test_id("HEAVY-HELPER-00D")
@pytest.mark.priority("P0")
@pytest.mark.requirement("headless heavy workspace must not copy dashboard dependency caches")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_ignore_heavy_workspace_dashboard_copy")
def test_heavy_workspace_dashboard_copy_excludes_installed_dependencies() -> None:
    ignored_names = _ignore_heavy_workspace_dashboard_copy(
        str(Path("judge-dashboard")),
        ["node_modules", ".vite", "src", "package.json", "__pycache__"],
    )

    assert ignored_names == {"node_modules", ".vite", "__pycache__"}


@pytest.mark.test_id("HEAVY-HELPER-00A")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 进度输出必须同步写入 live_progress.json，避免现场只能依赖滚动控制台")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="HeavyProgressReporter.emit")
def test_heavy_progress_reporter_writes_live_progress_file(tmp_path: Path) -> None:
    progress_path = tmp_path / "latest" / "heavy" / "live_progress.json"
    reporter = HeavyProgressReporter(
        total_steps=6,
        estimated_seconds=120.0,
        started_monotonic=0.0,
        progress_output_path=progress_path,
    )

    import time
    from unittest.mock import patch

    with patch.object(time, "monotonic", return_value=30.0):
        reporter.emit(3, "start_algorithms", "starting 17 real Algorithm.main processes")

    payload = json.loads(progress_path.read_text(encoding="utf-8"))
    assert payload["step"] == 3
    assert payload["stage"] == "start_algorithms"
    assert payload["percent"] == 50
    assert "17 real Algorithm.main processes" in payload["detail"]


@pytest.mark.test_id("HEAVY-HELPER-00B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 工件目录被残留进程占用时必须在复制 app 前明确失败，不能静默保留半删除工作区")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="prepare_heavy_workspace")
def test_prepare_heavy_workspace_rejects_artifact_root_that_cannot_be_reset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    stale_app_root = artifact_root / "heavy_workspace" / "app"
    stale_app_root.mkdir(parents=True)
    (stale_app_root / "sentinel.txt").write_text("stale", encoding="utf-8")

    def fake_rmtree(path, ignore_errors=False, *args, **kwargs):
        if Path(path) in {artifact_root, artifact_root / "heavy_workspace"}:
            if ignore_errors:
                return None
            raise PermissionError("simulated stale heavy process")
        raise AssertionError(f"unexpected rmtree path: {path}")

    monkeypatch.setattr("tests.helpers.heavy_runtime.shutil.rmtree", fake_rmtree)

    with pytest.raises(RuntimeError, match="cannot reset heavy artifact root"):
        prepare_heavy_workspace(artifact_root, team_count=1)


@pytest.mark.test_id("HEAVY-HELPER-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 完赛判定必须校验 runtime_state.db、match_finished、17 队 finished 状态和总分 CSV")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="assert_heavy_completion_outputs")
def test_assert_heavy_completion_outputs_accepts_finished_seventeen_team_results(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    db_path = resolve_runtime_state_db_path(workspace_root)
    ensure_runtime_state_schema(db_path)
    write_json_state(
        db_path,
        STATE_KEY_MATCH_CONTROL_STATUS,
        {
            "match_finished": True,
            "finished_team_id_list": [f"team_{index}" for index in range(HEAVY_DEFAULT_TEAM_COUNT)],
        },
    )
    for index in range(HEAVY_DEFAULT_TEAM_COUNT):
        team_id = f"team_{index}"
        write_team_score_overview_row(
            db_path,
            {
                "team_id": team_id,
                "total_score": float(HEAVY_DEFAULT_TEAM_COUNT + 1 - index),
                "run_status": "finished",
                "updated_at": "2026-04-23T00:00:00",
                "observed_trial_count": 4,
                "configured_task_count": 2,
                "started_task_count": 2,
                "mean_accuracy_percent": 50,
                "avg_reaction_time_ms": 100,
                "started_task_names": "vme|vmi",
            },
        )
        replace_team_task_overview_rows(
            db_path,
            team_id,
            [
                {
                    "team_id": team_id,
                    "task_id": "S1_vme_session1",
                    "exp_name": "vme",
                    "exp_task": "left_vs_rest",
                    "task_status": "finished",
                    "observed_trial_count": 2,
                },
                {
                    "team_id": team_id,
                    "task_id": "S1_vmi_session2",
                    "exp_name": "vmi",
                    "exp_task": "right_vs_rest",
                    "task_status": "finished",
                    "observed_trial_count": 2,
                },
            ],
        )
        replace_team_subject_task_overview_rows(
            db_path,
            team_id,
            [
                {
                    "team_id": team_id,
                    "subject_id": "S1",
                    "task_id": "S1_vme_session1",
                    "exp_name": "vme",
                    "exp_task": "left_vs_rest",
                    "task_status": "finished",
                    "observed_trial_count": 2,
                },
                {
                    "team_id": team_id,
                    "subject_id": "S1",
                    "task_id": "S1_vmi_session2",
                    "exp_name": "vmi",
                    "exp_task": "right_vs_rest",
                    "task_status": "finished",
                    "observed_trial_count": 2,
                },
            ],
        )
        replace_team_trial_record_rows(
            db_path,
            team_id,
            _make_heavy_trial_rows(team_id, profile=_profile_for_index(index)),
        )
        write_json_state(db_path, f"team:{team_id}", {"team_id": team_id, "run_status": "finished"})
    write_json_state(db_path, STATE_KEY_RUNTIME_STAGE_STATUS, {"group_status_list": [{"group_id": "group_1"}]})
    _write_score_csv(results_root / "00_team_score_overview.csv")
    _write_heavy_profile_observations(results_root)

    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(
            HEAVY_DEFAULT_TEAM_COUNT,
            HEAVY_BASE_PORT,
            profiles=[
                "normal",
                "slow",
                "late_result",
                "disconnect_stream",
                "duplicate_result",
                "invalid_output",
                "resource_hog",
                "malicious",
                "normal",
                "slow",
                "late_result",
                "disconnect_stream",
                "duplicate_result",
                "invalid_output",
                "resource_hog",
                "malicious",
                "normal",
            ],
        ),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
        scenario_by_team_id=build_heavy_team_scenarios(HEAVY_DEFAULT_TEAM_COUNT),
    )

    assert_heavy_completion_outputs(environment, expected_team_count=HEAVY_DEFAULT_TEAM_COUNT)


@pytest.mark.test_id("HEAVY-HELPER-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 完赛判定必须拒绝未 finished 的结果，防止只启动不完赛也通过")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="assert_heavy_completion_outputs")
def test_assert_heavy_completion_outputs_rejects_unfinished_match(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    db_path = resolve_runtime_state_db_path(workspace_root)
    ensure_runtime_state_schema(db_path)
    write_json_state(db_path, STATE_KEY_MATCH_CONTROL_STATUS, {"match_finished": False})
    (results_root / "00_team_score_overview.csv").write_text("team_id,total_score\n", encoding="utf-8")
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    with pytest.raises(AssertionError, match="match_finished"):
        assert_heavy_completion_outputs(environment, expected_team_count=HEAVY_DEFAULT_TEAM_COUNT)


@pytest.mark.test_id("HEAVY-HELPER-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 若在较长冷启动宽限后仍未创建 runtime_state.db，应提前失败并带出 headless 关键 stderr 日志尾部，避免盲等整个 30 分钟")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_build_missing_runtime_state_failure_detail")
def test_build_missing_runtime_state_failure_detail_includes_manifest_and_headless_log_tails(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    headless_log_root = results_root / "control" / "headless_logs"
    headless_log_root.mkdir(parents=True, exist_ok=True)
    (headless_log_root / "BCI_Judge__Collector_Python.stderr.log").write_text(
        "line_1\nline_2\ncollector failed here\n",
        encoding="utf-8",
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
        scenario_by_team_id=build_heavy_team_scenarios(HEAVY_DEFAULT_TEAM_COUNT),
    )

    detail = _build_missing_runtime_state_failure_detail(environment)

    assert "runtime_state.db creation" in detail
    assert "launcher_manifest_exists=False" in detail
    assert "process_manifest_exists=False" in detail
    assert "BCI_Judge__Collector_Python.stderr.log tail:" in detail
    assert "collector failed here" in detail
    assert f"{int(HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS)}s" in detail
    assert "artifact_root=" in detail
    assert "runtime_state_db_path=" in detail


@pytest.mark.test_id("HEAVY-HELPER-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 需要的算法端口覆写与故障注入必须补丁到测试生成的独立 team workspace，而不能混入正式业务源码")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_prepare_team_algorithm_workspaces")
def test_prepare_team_algorithm_workspaces_moves_heavy_patches_to_test_workspace(tmp_path: Path) -> None:
    team_config_list = build_team_config(
        2,
        HEAVY_BASE_PORT,
        profiles=["slow", "malicious"],
    )
    workspace_by_team_id = _prepare_team_algorithm_workspaces(
        artifact_root=tmp_path / "artifacts",
        team_config_list=team_config_list,
        observation_root=tmp_path / "results" / "control" / "heavy_profile_observations",
    )

    team_0_config_manager = (
        workspace_by_team_id["team_0"]
        / "app"
        / "Algorithm"
        / "Algorithm"
        / "service"
        / "ConfigManager.py"
    ).read_text(encoding="utf-8")
    team_1_algorithm_implement = (
        workspace_by_team_id["team_1"]
        / "app"
        / "Algorithm"
        / "Algorithm"
        / "method"
        / "model_artifacts"
        / "baseline_example"
        / "AlgorithmImplement.py"
    ).read_text(encoding="utf-8")
    team_1_predict_worker_manager = (
        workspace_by_team_id["team_1"]
        / "app"
        / "Algorithm"
        / "Algorithm"
        / "method"
        / "worker"
        / "PredictWorkerManager.py"
    ).read_text(encoding="utf-8")
    source_config_manager = Path("app/Algorithm/Algorithm/service/ConfigManager.py").read_text(encoding="utf-8")
    source_algorithm_implement = Path(
        "app/Algorithm/Algorithm/method/model_artifacts/baseline_example/AlgorithmImplement.py"
    ).read_text(encoding="utf-8")
    source_predict_worker_manager = Path(
        "app/Algorithm/Algorithm/method/worker/PredictWorkerManager.py"
    ).read_text(encoding="utf-8")

    assert f"'rpc_address': '[::]:{HEAVY_BASE_PORT}'" in team_0_config_manager
    assert "'rpc_address': '[::]:9981'" in source_config_manager
    assert "self.__heavy_fault_profile = 'malicious'" in team_1_algorithm_implement
    assert "__maybe_apply_heavy_fault_profile" in team_1_algorithm_implement
    assert "def __use_heavy_inline_predict_mode(self) -> bool:" in team_1_algorithm_implement
    assert "heavy inline predict mode active, skip predict worker session sync" in team_1_algorithm_implement
    assert "await asyncio.wait_for(" in team_1_algorithm_implement
    assert "torch.set_num_threads(1)" in team_1_algorithm_implement
    assert "torch.set_num_interop_threads(1)" in team_1_algorithm_implement
    assert "heavy lifecycle:" in team_1_algorithm_implement
    assert "before_get_device" in team_1_algorithm_implement
    assert "after_get_calibration" in team_1_algorithm_implement
    assert "BCI_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS" in team_1_predict_worker_manager
    assert "async def ensure_worker_started(self) -> None:" in team_1_predict_worker_manager
    assert "__maybe_apply_heavy_fault_profile" not in source_algorithm_implement
    assert "def __use_heavy_inline_predict_mode(self) -> bool:" not in source_algorithm_implement
    assert "ensure_worker_started" not in source_predict_worker_manager
    assert "BCI_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS" not in source_predict_worker_manager


@pytest.mark.test_id("HEAVY-HELPER-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 的 headless 裁判栈启动必须通过测试层补丁注入到复制出的 start_judge_stack.py，而不是修改正式工具脚本")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_patch_headless_start_judge_stack")
def test_patch_headless_start_judge_stack_only_modifies_workspace_copy(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    (workspace_root / "tools").mkdir(parents=True, exist_ok=True)
    source_path = Path("tools/start_judge_stack.py")
    target_path = workspace_root / "tools" / "start_judge_stack.py"
    target_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")

    _patch_headless_start_judge_stack(workspace_root)

    patched_text = target_path.read_text(encoding="utf-8")
    source_text = source_path.read_text(encoding="utf-8")
    assert "def is_headless_mode_enabled()" in patched_text
    assert "def wait_for_local_tcp_port(host: str, port: int, timeout_seconds: float = 30.0) -> bool:" in patched_text
    assert "def wait_for_collector_runtime_ready(timeout_seconds: float = 90.0) -> bool:" in patched_text
    assert "central_controller_component_port_ready = wait_for_local_tcp_port('127.0.0.1', 9002, timeout_seconds=45.0)" in patched_text
    assert "collector_runtime_ready = wait_for_collector_runtime_ready(timeout_seconds=90.0)" in patched_text
    assert "collector runtime readiness timeout before ProcessHub startup" in patched_text
    assert "'mode': 'headless'" in patched_text
    assert "def is_headless_mode_enabled()" not in source_text
    assert "def wait_for_local_tcp_port(host: str, port: int, timeout_seconds: float = 30.0) -> bool:" not in source_text
    assert "def wait_for_collector_runtime_ready(timeout_seconds: float = 90.0) -> bool:" not in source_text


@pytest.mark.test_id("HEAVY-HELPER-05B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy headless 裁判栈启动不能通过 cmd /c 执行带引号的 Java 路径，避免 cmd 将 \\\" 当成字面命令导致 Central Java Controller 启动失败")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_patch_headless_start_judge_stack")
def test_patch_headless_start_judge_stack_runs_quoted_java_command_without_cmd_wrapper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_root = tmp_path / "workspace"
    (workspace_root / "tools").mkdir(parents=True, exist_ok=True)
    source_path = Path("tools/start_judge_stack.py")
    target_path = workspace_root / "tools" / "start_judge_stack.py"
    target_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")
    _patch_headless_start_judge_stack(workspace_root)

    spec = importlib.util.spec_from_file_location("patched_start_judge_stack_for_heavy_test", target_path)
    assert spec is not None
    assert spec.loader is not None
    patched_start_judge_stack = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(patched_start_judge_stack)

    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 12345

    def fake_popen(command: object, **kwargs: object) -> FakeProcess:
        captured["command"] = command
        captured["kwargs"] = kwargs
        return FakeProcess()

    monkeypatch.setenv("BCI_HEADLESS", "1")
    monkeypatch.setattr(patched_start_judge_stack.subprocess, "Popen", fake_popen)

    patched_start_judge_stack.start_component_window(
        title="[BCI Judge] Central Java Controller",
        cwd=workspace_root,
        command='"C:\\Program Files\\Common Files\\Oracle\\Java\\javapath\\java.EXE" -jar centrol.jar',
    )

    assert captured["command"] == [
        "C:\\Program Files\\Common Files\\Oracle\\Java\\javapath\\java.EXE",
        "-jar",
        "centrol.jar",
    ]


@pytest.mark.test_id("HEAVY-HELPER-05A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 对 CentralController 并发启动稳定性的补丁必须仅作用于复制出的 workspace，不得修改正式业务源码")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_patch_central_controller_component_monitor_for_heavy")
def test_patch_central_controller_component_monitor_only_modifies_workspace_copy(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    target_dir = workspace_root / "app" / "CentralController" / "CentralController" / "service"
    target_dir.mkdir(parents=True, exist_ok=True)
    source_path = Path("app/CentralController/CentralController/service/ComponentMonitor.py")
    target_path = target_dir / "ComponentMonitor.py"
    target_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")

    _patch_central_controller_component_monitor_for_heavy(workspace_root)

    patched_text = target_path.read_text(encoding="utf-8")
    source_text = source_path.read_text(encoding="utf-8")
    assert "component_model_item_list = list(component_model_dict.items())" in patched_text
    assert "for component_model_id, registered_component_information_model in component_model_item_list:" in patched_text
    assert "component_model_item_list = list(component_model_dict.items())" not in source_text


@pytest.mark.test_id("HEAVY-HELPER-06")
@pytest.mark.priority("P1")
@pytest.mark.requirement("17 队 heavy 的默认完赛超时和缺失 runtime_state 宽限必须提升，并在辅助常量中固定，避免仍沿用 9 队基线")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="HEAVY_MATCH_TIMEOUT_SECONDS/HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS")
def test_heavy_timeout_constants_are_tuned_for_seventeen_team_full_chain() -> None:
    assert HEAVY_DEFAULT_TEAM_COUNT == 17
    assert HEAVY_DEFAULT_DATASET_SUBJECT_COUNT is None
    assert HEAVY_DEFAULT_MATCH_TIMEOUT_SECONDS >= 28800.0
    assert HEAVY_MATCH_TIMEOUT_SECONDS >= HEAVY_DEFAULT_MATCH_TIMEOUT_SECONDS
    assert HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS >= 120.0
    assert HEAVY_ALGORITHM_CPU_THREAD_COUNT == 1
    assert HEAVY_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS >= 90.0


@pytest.mark.test_id("HEAVY-HELPER-06A")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 完赛等待上限必须允许通过环境变量加长，同时不能低于 17 队正式链路最低保护值")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_resolve_float_env")
def test_heavy_match_timeout_can_be_extended_by_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BCI_HEAVY_MATCH_TIMEOUT_SECONDS", "43200")
    assert _resolve_float_env("BCI_HEAVY_MATCH_TIMEOUT_SECONDS", 28800.0, minimum=10800.0) == 43200.0

    monkeypatch.setenv("BCI_HEAVY_MATCH_TIMEOUT_SECONDS", "60")
    assert _resolve_float_env("BCI_HEAVY_MATCH_TIMEOUT_SECONDS", 28800.0, minimum=10800.0) == 10800.0

    monkeypatch.setenv("BCI_HEAVY_MATCH_TIMEOUT_SECONDS", "bad")
    assert _resolve_float_env("BCI_HEAVY_MATCH_TIMEOUT_SECONDS", 28800.0, minimum=10800.0) == 28800.0


@pytest.mark.test_id("HEAVY-HELPER-26")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 启动 17 队算法进程时，必须只通过环境变量和复制件补丁收敛 CPU 并放宽 predict worker session sync 超时，不能直接修改源业务代码")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="start_heavy_algorithms")
def test_start_heavy_algorithms_injects_heavy_only_runtime_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    environment = prepare_heavy_workspace(tmp_path / "artifacts", team_count=2)
    captured_env_list: list[dict[str, str]] = []

    def _fake_start_python_module(
        python_executable: str,
        module: str,
        cwd: Path,
        artifact_dir: Path,
        env: dict[str, str] | None = None,
        name: str | None = None,
    ) -> ManagedProcess:
        captured_env_list.append(dict(env or {}))
        artifact_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = artifact_dir / f"{name}.stdout.log"
        stderr_path = artifact_dir / f"{name}.stderr.log"
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")

        class _FakeProcess:
            pid = 10001

            def poll(self):
                return None

            def terminate(self):
                return None

            def wait(self, timeout=None):
                return 0

        return ManagedProcess(
            name=name or module,
            process=_FakeProcess(),
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            cwd=cwd,
            command=[python_executable, "-m", module],
            started_at=0.0,
        )

    try:
        monkeypatch.setattr("tests.helpers.heavy_runtime.validate_heavy_python_runtime", lambda _: None)
        monkeypatch.setattr("tests.helpers.heavy_runtime.start_python_module", _fake_start_python_module)
        monkeypatch.setattr("tests.helpers.heavy_runtime.wait_for_port", lambda host, port, timeout=20.0: True)

        from tests.helpers.heavy_runtime import start_heavy_algorithms

        start_heavy_algorithms(environment, python_executable="python")

        assert len(captured_env_list) == 2
        for env in captured_env_list:
            assert env["OMP_NUM_THREADS"] == str(HEAVY_ALGORITHM_CPU_THREAD_COUNT)
            assert env["MKL_NUM_THREADS"] == str(HEAVY_ALGORITHM_CPU_THREAD_COUNT)
            assert env["OPENBLAS_NUM_THREADS"] == str(HEAVY_ALGORITHM_CPU_THREAD_COUNT)
            assert env["NUMEXPR_NUM_THREADS"] == str(HEAVY_ALGORITHM_CPU_THREAD_COUNT)
            assert env["BCI_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS"] == str(
                HEAVY_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS
            )
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-HELPER-26B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 算法端口启动等待时间必须覆盖真实 17 进程冷启动，避免 20 秒固定窗口误判失败")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="start_heavy_algorithms")
def test_start_heavy_algorithms_waits_one_hundred_twenty_seconds_for_algorithm_ports(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    environment = prepare_heavy_workspace(tmp_path / "artifacts", team_count=1)
    observed_timeout_list: list[float] = []

    def _fake_start_python_module(
        python_executable: str,
        module: str,
        cwd: Path,
        artifact_dir: Path,
        env: dict[str, str] | None = None,
        name: str | None = None,
    ) -> ManagedProcess:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = artifact_dir / f"{name}.stdout.log"
        stderr_path = artifact_dir / f"{name}.stderr.log"
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")

        class _FakeProcess:
            pid = 10003

            def poll(self):
                return None

            def terminate(self):
                return None

            def wait(self, timeout=None):
                return 0

        return ManagedProcess(
            name=name or module,
            process=_FakeProcess(),
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            cwd=cwd,
            command=[python_executable, "-m", module],
            started_at=0.0,
        )

    def _fake_wait_for_port(host: str, port: int, timeout: float = 10.0) -> bool:
        observed_timeout_list.append(timeout)
        return True

    try:
        monkeypatch.setattr("tests.helpers.heavy_runtime.validate_heavy_python_runtime", lambda _: None)
        monkeypatch.setattr("tests.helpers.heavy_runtime.start_python_module", _fake_start_python_module)
        monkeypatch.setattr("tests.helpers.heavy_runtime._is_port_listening_by_netstat", lambda port: False)
        monkeypatch.setattr("tests.helpers.heavy_runtime.wait_for_port", _fake_wait_for_port)

        from tests.helpers.heavy_runtime import start_heavy_algorithms

        start_heavy_algorithms(environment, python_executable="python")

        assert observed_timeout_list == [120.0]
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-HELPER-26A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 算法端口就绪判定必须接受 IPv6 wildcard 监听，不能仅因 127.0.0.1 connect 失败而误判未启动")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="start_heavy_algorithms")
def test_start_heavy_algorithms_accepts_netstat_listener_before_ipv4_connect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    environment = prepare_heavy_workspace(tmp_path / "artifacts", team_count=1)

    def _fake_start_python_module(
        python_executable: str,
        module: str,
        cwd: Path,
        artifact_dir: Path,
        env: dict[str, str] | None = None,
        name: str | None = None,
    ) -> ManagedProcess:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = artifact_dir / f"{name}.stdout.log"
        stderr_path = artifact_dir / f"{name}.stderr.log"
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")

        class _FakeProcess:
            pid = 10002

            def poll(self):
                return None

            def terminate(self):
                return None

            def wait(self, timeout=None):
                return 0

        return ManagedProcess(
            name=name or module,
            process=_FakeProcess(),
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            cwd=cwd,
            command=[python_executable, "-m", module],
            started_at=0.0,
        )

    try:
        monkeypatch.setattr("tests.helpers.heavy_runtime.validate_heavy_python_runtime", lambda _: None)
        monkeypatch.setattr("tests.helpers.heavy_runtime.start_python_module", _fake_start_python_module)
        monkeypatch.setattr("tests.helpers.heavy_runtime._is_port_listening_by_netstat", lambda port: port == HEAVY_BASE_PORT)
        monkeypatch.setattr("tests.helpers.heavy_runtime.wait_for_port", lambda host, port, timeout=20.0: False)

        from tests.helpers.heavy_runtime import start_heavy_algorithms

        process_list = start_heavy_algorithms(environment, python_executable="python")

        assert len(process_list) == 1
        assert process_list[0].name == "algorithm_team_0"
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-HELPER-26C")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 裁判栈 JudgeWeb 就绪等待时间必须覆盖真实重型启动耗时，避免 150 秒固定窗口误判失败")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="start_headless_judge_stack")
def test_start_headless_judge_stack_waits_three_hundred_sixty_seconds_for_judge_web(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    artifact_root = tmp_path / "artifacts"
    results_root = workspace_root / "results"
    observed_timeout_list: list[float] = []
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=artifact_root,
        results_root=results_root,
        team_config_list=[],
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    def _fake_start_python_module(
        python_executable: str,
        module: str,
        cwd: Path,
        artifact_dir: Path,
        env: dict[str, str] | None = None,
        name: str | None = None,
    ) -> ManagedProcess:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = artifact_dir / f"{name}.stdout.log"
        stderr_path = artifact_dir / f"{name}.stderr.log"
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")

        class _FakeProcess:
            pid = 10004

            def poll(self):
                return None

            def terminate(self):
                return None

            def wait(self, timeout=None):
                return 0

        return ManagedProcess(
            name=name or module,
            process=_FakeProcess(),
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            cwd=cwd,
            command=[python_executable, "-m", module],
            started_at=0.0,
        )

    def _fake_wait_for_http(url: str, timeout: float = 10.0) -> bool:
        observed_timeout_list.append(timeout)
        return True

    monkeypatch.setattr("tests.helpers.heavy_runtime.validate_heavy_python_runtime", lambda _: None)
    monkeypatch.setattr("tests.helpers.heavy_runtime.start_python_module", _fake_start_python_module)
    monkeypatch.setattr("tests.helpers.heavy_runtime.wait_for_http", _fake_wait_for_http)
    monkeypatch.setattr("tests.helpers.heavy_runtime._assert_formal_runtime_state_bootstrap", lambda _: None)
    monkeypatch.setattr("tests.helpers.heavy_runtime._trigger_formal_start_match", lambda _: None)

    from tests.helpers.heavy_runtime import start_headless_judge_stack

    process = start_headless_judge_stack(environment, python_executable="python")

    assert observed_timeout_list == [360.0]
    assert environment.judge_processes == [process]


@pytest.mark.test_id("HEAVY-HELPER-27")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 为 17 队并发打的 predict worker 预热/超时补丁必须只存在于复制出的 team workspace，源业务文件必须保持未修改")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_prepare_single_team_algorithm_workspace")
def test_heavy_predict_worker_patch_is_workspace_only(tmp_path: Path) -> None:
    environment = prepare_heavy_workspace(tmp_path / "artifacts", team_count=1)
    try:
        copied_predict_worker_manager = (
            environment.algorithm_workspace_by_team_id["team_0"]
            / "app"
            / "Algorithm"
            / "Algorithm"
            / "method"
            / "worker"
            / "PredictWorkerManager.py"
        ).read_text(encoding="utf-8")
        source_predict_worker_manager = Path(
            "app/Algorithm/Algorithm/method/worker/PredictWorkerManager.py"
        ).read_text(encoding="utf-8")

        assert "BCI_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS" in copied_predict_worker_manager
        assert "async def ensure_worker_started(self) -> None:" in copied_predict_worker_manager
        assert "BCI_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS" not in source_predict_worker_manager
        assert "async def ensure_worker_started(self) -> None:" not in source_predict_worker_manager
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-HELPER-27A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("17 队 heavy 只放大并发队伍数，不得把 VirtualReceiver 正式数据集按队伍数伪造扩容为 S1..S17")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="prepare_heavy_workspace/dump_heavy_environment_snapshot")
def test_prepare_heavy_workspace_keeps_formal_virtual_receiver_subject_set(tmp_path: Path) -> None:
    environment = prepare_heavy_workspace(tmp_path / "artifacts", team_count=HEAVY_DEFAULT_TEAM_COUNT)
    try:
        snapshot = dump_heavy_environment_snapshot(environment)
        source_virtual_receiver_payload = yaml.safe_load(
            Path("app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml").read_text(encoding="utf-8")
        ) or {}

        assert sorted(snapshot["virtual_receiver_config"]["data_files"].keys()) == sorted(
            (source_virtual_receiver_payload.get("data_files") or {}).keys()
        )
        assert "S17" not in snapshot["virtual_receiver_config"]["data_files"]
        assert len(snapshot["virtual_receiver_config"]["data_files"]) < HEAVY_DEFAULT_TEAM_COUNT
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-HELPER-28")
@pytest.mark.priority("P0")
@pytest.mark.requirement("pytest 的 python_executable 夹具必须跳过 WinError 10106 等坏解释器，并优先选择通过 heavy 依赖自检的解释器")
@pytest.mark.tested(file="tests/conftest.py", function="python_executable/_python_runtime_self_check")
def test_python_executable_fixture_prefers_runtime_healthy_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    checked_candidate_list: list[str] = []

    def _fake_self_check(candidate: str) -> bool:
        checked_candidate_list.append(candidate)
        return candidate == r"D:\anaconda3\python.exe"

    monkeypatch.setattr(test_conftest, "_python_runtime_self_check", _fake_self_check)
    monkeypatch.setenv("BCI_PYTHON_EXE", r"D:\broken\python.exe")
    monkeypatch.setattr(test_conftest.sys, "executable", r"D:\bad\sys_python.exe")
    monkeypatch.setenv("PATH", os.pathsep.join([r"D:\path_one", r"D:\path_two"]))

    resolved = test_conftest.python_executable.__wrapped__()

    assert resolved == str(Path(r"D:\anaconda3\python.exe").resolve())
    assert checked_candidate_list[:4] == [
        r"D:\broken\python.exe",
        r"D:\anaconda3\envs\BCI_competation_2026\python.exe",
        r"D:\anaconda3\envs\BCI_competition_2026\python.exe",
        r"D:\anaconda3\python.exe",
    ]


@pytest.mark.test_id("HEAVY-HELPER-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 等待进度摘要必须在 runtime_state.db 未生成时明确提示 runtime_state 路径、headless_logs 与 judge_stack_logs 排查路径，并指出正式链路中该库应在启动早期创建")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_summarize_heavy_runtime_progress")
def test_summarize_heavy_runtime_progress_mentions_log_locations_before_runtime_state_exists(tmp_path: Path) -> None:
    runtime_state_db_path = tmp_path / "results" / "runtime_state.db"

    summary = _summarize_heavy_runtime_progress(
        runtime_state_db_path=runtime_state_db_path,
        expected_team_count=HEAVY_DEFAULT_TEAM_COUNT,
        match_control_status={},
        expected_stage_count=14,
        expected_subject_count=5,
    )

    assert "runtime_state.db not created yet" in summary
    assert "runtime_state_path=" in summary
    assert "headless_logs=" in summary
    assert "judge_stack_logs" in summary
    assert "formal judge chain" in summary
    assert str(int(HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS)) in summary


@pytest.mark.test_id("HEAVY-HELPER-07A")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 进度抽取必须基于正式 runtime_stage_status 计算 stage 完成比和当前 active stage，而不是固定显示 5/6=83%")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_extract_runtime_stage_progress/_resolve_heavy_match_progress_ratio")
def test_extract_runtime_stage_progress_and_progress_ratio_follow_real_stage_completion(tmp_path: Path) -> None:
    db_path = tmp_path / "results" / "runtime_state.db"
    ensure_runtime_state_schema(db_path)
    write_json_state(
        db_path,
        STATE_KEY_RUNTIME_STAGE_STATUS,
        {
            "group_status_list": [
                {
                    "group_id": "group_1",
                    "stage_status_list": [
                        {
                            "stage_context": {
                                "subject_id": "S1",
                                "exp_name": "vme",
                                "exp_task": "left_vs_rest",
                                "session_id": "session1",
                            },
                            "online_trial_count": 40,
                            "completed_trial_count": 40,
                        },
                        {
                            "stage_context": {
                                "subject_id": "S1",
                                "exp_name": "vme",
                                "exp_task": "right_vs_rest",
                                "session_id": "session1",
                            },
                            "online_trial_count": 40,
                            "completed_trial_count": 10,
                        },
                    ],
                }
            ]
        },
    )

    progress_snapshot = _extract_runtime_stage_progress(read_json_state(db_path, STATE_KEY_RUNTIME_STAGE_STATUS))

    assert progress_snapshot["total_stage_count"] == 2
    assert progress_snapshot["completed_stage_count"] == 1
    assert progress_snapshot["active_stage_context"] == {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "right_vs_rest",
        "session_id": "session1",
    }
    assert progress_snapshot["active_completed_trial_count"] == 10
    assert progress_snapshot["active_online_trial_count"] == 40
    assert 0.62 <= float(progress_snapshot["stage_completion_ratio"]) <= 0.63
    assert _resolve_heavy_match_progress_ratio(db_path, expected_stage_count=40) > 0.30


@pytest.mark.test_id("HEAVY-HELPER-07B")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 在 waiting_start_readiness 阶段的进度百分比必须跟随 connected/group readiness 渐进增长，不能固定显示为 83%")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_resolve_waiting_start_readiness_progress_ratio")
def test_resolve_waiting_start_readiness_progress_ratio_stays_near_pre_start_window() -> None:
    ratio_empty = _resolve_waiting_start_readiness_progress_ratio(
        {
            "start_readiness": {
                "configured_team_id_list": [f"team_{index}" for index in range(17)],
                "connected_team_id_list": [],
                "pending_group_id_list": ["group_1"],
                "group_readiness_list": [{"group_id": "group_1", "collector_ready": False}],
            }
        }
    )
    ratio_connected = _resolve_waiting_start_readiness_progress_ratio(
        {
            "start_readiness": {
                "configured_team_id_list": [f"team_{index}" for index in range(17)],
                "connected_team_id_list": [f"team_{index}" for index in range(17)],
                "pending_group_id_list": ["group_1"],
                "group_readiness_list": [{"group_id": "group_1", "collector_ready": False}],
            }
        }
    )
    ratio_ready = _resolve_waiting_start_readiness_progress_ratio(
        {
            "start_readiness": {
                "configured_team_id_list": [f"team_{index}" for index in range(17)],
                "connected_team_id_list": [f"team_{index}" for index in range(17)],
                "pending_group_id_list": [],
                "group_readiness_list": [{"group_id": "group_1", "collector_ready": True}],
            }
        }
    )

    assert 0.251 <= ratio_empty < ratio_connected < ratio_ready < 0.280


@pytest.mark.test_id("HEAVY-HELPER-08")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 在 JudgeWeb ready 后必须快速观察到正式链路的 runtime_state 启动状态，否则应尽早按启动异常失败")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_assert_formal_runtime_state_bootstrap")
def test_assert_formal_runtime_state_bootstrap_accepts_early_match_control_status(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    db_path = resolve_runtime_state_db_path(workspace_root)
    ensure_runtime_state_schema(db_path)
    write_json_state(
        db_path,
        STATE_KEY_MATCH_CONTROL_STATUS,
        {
            "match_started": False,
            "match_finished": False,
            "updated_at": 1.0,
        },
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    _assert_formal_runtime_state_bootstrap(environment)


@pytest.mark.test_id("HEAVY-HELPER-09")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 的 runtime_state bootstrap 失败详情必须输出 SQLite 当前表计数和前若干 json_state keys，便于区分空库、半库和正式 bootstrap 缺失")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_summarize_runtime_state_bootstrap_state")
def test_summarize_runtime_state_bootstrap_state_reports_table_counts_and_keys(tmp_path: Path) -> None:
    db_path = tmp_path / "results" / "runtime_state.db"
    ensure_runtime_state_schema(db_path)
    write_json_state(db_path, "team:team_0", {"team_id": "team_0", "updated_at": 1.0})

    detail_lines = _summarize_runtime_state_bootstrap_state(db_path)

    assert any("runtime_state_json_state_count=1" in line for line in detail_lines)
    assert any("team:team_0" in line for line in detail_lines)
    assert any("runtime_state_team_score_count=0" in line for line in detail_lines)
    assert any("runtime_state_trial_record_count=0" in line for line in detail_lines)


@pytest.mark.test_id("HEAVY-HELPER-10")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 在正式链路 bootstrap 完成后必须主动调用 JudgeWeb 的 start-match 接口，并等待 match_started=true")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_trigger_formal_start_match")
def test_trigger_formal_start_match_accepts_success_payload_and_waits_for_match_started(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    db_path = resolve_runtime_state_db_path(workspace_root)
    ensure_runtime_state_schema(db_path)
    write_json_state(
        db_path,
        STATE_KEY_MATCH_CONTROL_STATUS,
        {"match_started": False, "match_finished": False, "updated_at": 1.0},
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    class _FakeResponse:
        def __init__(self, payload: dict, update_match_started: bool = False):
            self._payload = payload
            self._update_match_started = update_match_started

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            if self._update_match_started:
                write_json_state(
                    db_path,
                    STATE_KEY_MATCH_CONTROL_STATUS,
                    {"match_started": True, "match_finished": False, "started_at": 2.0, "updated_at": 2.0},
                )
            return json.dumps(self._payload).encode("utf-8")

    response_iter = iter(
        [
            _FakeResponse(
                {
                    "start_readiness": {
                        "ready": True,
                        "configured_team_id_list": [f"team_{index}" for index in range(HEAVY_DEFAULT_TEAM_COUNT)],
                        "connected_team_id_list": [f"team_{index}" for index in range(HEAVY_DEFAULT_TEAM_COUNT)],
                        "pending_group_id_list": [],
                    }
                }
            ),
            _FakeResponse({"ok": True}, update_match_started=True),
        ]
    )

    monkeypatch.setattr("urllib.request.urlopen", lambda request, timeout=10.0: next(response_iter))
    monkeypatch.setattr("time.sleep", lambda _: None)

    _trigger_formal_start_match(environment)


@pytest.mark.test_id("HEAVY-HELPER-11")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 在调用正式 start-match 前必须先等待 JudgeWeb 的 start_readiness.ready=true，避免过早请求导致 409")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_wait_for_formal_start_readiness")
def test_wait_for_formal_start_readiness_returns_when_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    environment = HeavyEnvironment(
        workspace_root=tmp_path / "workspace",
        artifact_root=tmp_path / "artifacts",
        results_root=tmp_path / "workspace" / "results",
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )
    payload_list = iter(
        [
            {"start_readiness": {"ready": False, "reason_list": ["pending"]}},
            {
                "start_readiness": {
                    "ready": True,
                    "configured_team_id_list": [f"team_{index}" for index in range(HEAVY_DEFAULT_TEAM_COUNT)],
                    "connected_team_id_list": [f"team_{index}" for index in range(HEAVY_DEFAULT_TEAM_COUNT)],
                    "pending_group_id_list": [],
                }
            },
        ]
    )

    class _FakeResponse:
        def __init__(self, payload: dict):
            self._payload = payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

    monkeypatch.setattr("urllib.request.urlopen", lambda request, timeout=10.0: _FakeResponse(next(payload_list)))
    monkeypatch.setattr("time.sleep", lambda _: None)

    readiness = _wait_for_formal_start_readiness(environment, "http://127.0.0.1:18080/api/v1/control/status")

    assert readiness["ready"] is True
    assert len(readiness["connected_team_id_list"]) == HEAVY_DEFAULT_TEAM_COUNT


@pytest.mark.test_id("HEAVY-HELPER-12")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 的 formal start readiness 超时详情必须包含最后一次 HTTP 访问来源、runtime_state 摘要、team 状态摘要和关键 headless 日志尾部，便于直接定位")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_build_formal_start_readiness_failure_detail")
def test_build_formal_start_readiness_failure_detail_includes_http_runtime_team_and_logs(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    headless_log_root = results_root / "control" / "headless_logs"
    headless_log_root.mkdir(parents=True, exist_ok=True)
    (headless_log_root / "BCI_Judge__JudgeWeb.stdout.log").write_text("GET /healthz\n", encoding="utf-8")
    (headless_log_root / "BCI_Judge__ProcessHub_team_0_group_1.stdout.log").write_text(
        "disconnect_reason=report_stream_closed\n",
        encoding="utf-8",
    )
    db_path = resolve_runtime_state_db_path(workspace_root)
    ensure_runtime_state_schema(db_path)
    write_json_state(db_path, STATE_KEY_MATCH_CONTROL_STATUS, {"match_started": False, "match_finished": False, "updated_at": 1.0})
    write_json_state(db_path, STATE_KEY_RUNTIME_STAGE_STATUS, {"group_status_list": [{"group_id": "group_1", "stage_status_list": []}]})
    write_json_state(
        db_path,
        "team:team_0",
        {
            "team_id": "team_0",
            "connection_status": "disconnected",
            "run_status": "ready",
            "calibration_ready": False,
            "last_disconnect_reason": "algorithm_data_connection_closed_before_task_finished: report_stream_closed",
            "updated_at": 2.0,
        },
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )
    fetch_result = LocalJsonFetchResult(
        payload={"start_readiness": {"ready": False, "pending_group_id_list": ["group_1"], "connected_team_id_list": []}},
        error="URLError: localhost unavailable",
        source="powershell",
        status_code=200,
    )

    detail = _build_formal_start_readiness_failure_detail(
        environment,
        "http://127.0.0.1:18080/api/v1/control/status",
        fetch_result,
    )

    assert "source=powershell" in detail
    assert "runtime_state_json_state_count=" in detail
    assert "team_state_disconnected_count=1" in detail
    assert "report_stream_closed" in detail
    assert "BCI_Judge__JudgeWeb.stdout.log tail:" in detail
    assert "BCI_Judge__ProcessHub_team_0_group_1.stdout.log tail:" in detail


@pytest.mark.test_id("HEAVY-HELPER-13")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 的 readiness 进行中进度日志必须带出 HTTP 来源、队伍连接数、pending group、runtime stage 计数和掉线原因采样")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_summarize_start_readiness_progress")
def test_summarize_start_readiness_progress_reports_http_and_team_state(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    db_path = resolve_runtime_state_db_path(workspace_root)
    ensure_runtime_state_schema(db_path)
    write_json_state(db_path, STATE_KEY_MATCH_CONTROL_STATUS, {"match_started": False, "match_finished": False, "updated_at": 1.0})
    write_json_state(
        db_path,
        STATE_KEY_RUNTIME_STAGE_STATUS,
        {"group_status_list": [{"group_id": "group_1", "stage_status_list": [{"collector_prepared": False}]}]},
    )
    write_json_state(
        db_path,
        "team:team_0",
        {
            "team_id": "team_0",
            "connection_status": "disconnected",
            "run_status": "ready",
            "calibration_ready": False,
            "last_disconnect_reason": "report_stream_closed",
            "updated_at": 1.0,
        },
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )
    fetch_result = LocalJsonFetchResult(
        payload={"start_readiness": {"ready": False, "connected_team_id_list": [], "configured_team_id_list": [f"team_{i}" for i in range(HEAVY_DEFAULT_TEAM_COUNT)], "pending_group_id_list": ["group_1"]}},
        error="URLError: test",
        source="urllib",
        status_code=None,
    )

    summary = _summarize_start_readiness_progress(
        environment,
        "http://127.0.0.1:18080/api/v1/control/status",
        fetch_result,
    )

    assert "http_source=urllib" in summary
    assert "http_error=URLError: test" in summary
    assert "pending_groups=['group_1']" in summary
    assert "runtime_stage_stage_count=1" in summary
    assert "disconnect_reason_sample=['report_stream_closed']" in summary


@pytest.mark.test_id("HEAVY-HELPER-14")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 的 team 状态摘要必须输出总数、掉线数和样本，便于区分全连通与大面积断连")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_summarize_team_state_snapshot")
def test_summarize_team_state_snapshot_reports_counts_and_sample(tmp_path: Path) -> None:
    db_path = tmp_path / "results" / "runtime_state.db"
    ensure_runtime_state_schema(db_path)
    write_json_state(db_path, "team:team_0", {"team_id": "team_0", "connection_status": "connected", "updated_at": 1.0})
    write_json_state(
        db_path,
        "team:team_1",
        {
            "team_id": "team_1",
            "connection_status": "disconnected",
            "last_disconnect_reason": "report_stream_closed",
            "updated_at": 2.0,
        },
    )

    detail_lines = _summarize_team_state_snapshot(db_path)

    assert any("team_state_count=2" in line for line in detail_lines)
    assert any("team_state_disconnected_count=1" in line for line in detail_lines)
    assert any("report_stream_closed" in line for line in detail_lines)


@pytest.mark.test_id("HEAVY-HELPER-15")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 的关键 headless 日志采样必须优先包含 JudgeWeb、RuntimeStageCoordinator、Collector 和首个 ProcessHub")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_collect_representative_headless_log_tails")
def test_collect_representative_headless_log_tails_prefers_key_logs(tmp_path: Path) -> None:
    headless_log_root = tmp_path / "results" / "control" / "headless_logs"
    headless_log_root.mkdir(parents=True, exist_ok=True)
    for file_name in (
        "BCI_Judge__JudgeWeb.stdout.log",
        "BCI_Judge__RuntimeStageCoordinator_Python.stderr.log",
        "BCI_Judge__Collector_Python.stdout.log",
        "BCI_Judge__ProcessHub_team_0_group_1.stderr.log",
    ):
        (headless_log_root / file_name).write_text(f"{file_name}\n", encoding="utf-8")

    detail_lines = _collect_representative_headless_log_tails(headless_log_root)

    assert any("BCI_Judge__JudgeWeb.stdout.log tail:" in line for line in detail_lines)
    assert any("BCI_Judge__RuntimeStageCoordinator_Python.stderr.log tail:" in line for line in detail_lines)
    assert any("BCI_Judge__Collector_Python.stdout.log tail:" in line for line in detail_lines)
    assert any("BCI_Judge__ProcessHub_team_0_group_1.stderr.log tail:" in line for line in detail_lines)


@pytest.mark.test_id("HEAVY-HELPER-16")
@pytest.mark.priority("P2")
@pytest.mark.requirement("heavy 的本地 HTTP 失败详情格式必须稳定输出 url/source/status/error/payload，方便直接附到 AssertionError")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_format_local_json_fetch_detail")
def test_format_local_json_fetch_detail_is_stable() -> None:
    detail = _format_local_json_fetch_detail(
        "http://127.0.0.1:18080/api/v1/control/status",
        LocalJsonFetchResult(
            payload={"start_readiness": {"ready": False}},
            error="URLError: boom",
            source="urllib",
            status_code=409,
        ),
    )

    assert "url=http://127.0.0.1:18080/api/v1/control/status" in detail
    assert "source=urllib" in detail
    assert "status=409" in detail
    assert "error=URLError: boom" in detail
    assert "payload={'start_readiness': {'ready': False}}" in detail


@pytest.mark.test_id("HEAVY-HELPER-17")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 的正式数据集必须复制仓库里已有的真实可解析 .dat 与对应 _meta.txt，并保留正式 subject/session/run 布局，不能再写伪造字节串")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_build_heavy_dataset_spec")
def test_build_heavy_dataset_spec_copies_real_dat_and_meta_files(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    generated_file_path_list: list[Path] = []
    source_pair_by_subject = _collect_real_heavy_source_sample_pairs(
        Path("app/Collector/Collector/receiver/virtual_receiver/data")
    )
    assert source_pair_by_subject
    first_subject_key = sorted(source_pair_by_subject.keys())[0]
    source_vme_dat_path, source_vme_meta_path = source_pair_by_subject[first_subject_key]["vme"][0]
    source_vmi_dat_path, source_vmi_meta_path = source_pair_by_subject[first_subject_key]["vmi"][0]
    dataset_spec = _build_heavy_dataset_spec(
        workspace_root,
        team_count=3,
        generated_file_path_list=generated_file_path_list,
    )

    discovered_subject_key_list = sorted(source_pair_by_subject.keys())
    assert sorted(dataset_spec["data_files"].keys()) == discovered_subject_key_list
    vme_entry = dataset_spec["data_files"][first_subject_key]["vme"][0]
    vmi_entry = dataset_spec["data_files"][first_subject_key]["vmi"][0]
    vme_path = Path(vme_entry["source_path"])
    vmi_path = Path(vmi_entry["source_path"])
    vme_meta_path = vme_path.with_name(f"{vme_path.stem}_meta.txt")
    vmi_meta_path = vmi_path.with_name(f"{vmi_path.stem}_meta.txt")

    assert vme_path.exists()
    assert vmi_path.exists()
    assert vme_meta_path.exists()
    assert vmi_meta_path.exists()
    assert vme_path.read_bytes() == source_vme_dat_path.read_bytes()
    assert vmi_path.read_bytes() == source_vmi_dat_path.read_bytes()

    source_vme_meta = source_vme_meta_path.read_text(encoding="utf-8")
    source_vmi_meta = source_vmi_meta_path.read_text(encoding="utf-8")
    assert "storage_format=binary_float32_le" in source_vme_meta
    assert "channel_labels=" in source_vme_meta
    vme_meta_text = vme_meta_path.read_text(encoding="utf-8")
    vmi_meta_text = vmi_meta_path.read_text(encoding="utf-8")
    assert "storage_format=binary_float32_le" in vme_meta_text
    assert "channel_labels=" in vme_meta_text
    assert "storage_format=binary_float32_le" in vmi_meta_text
    assert "channel_labels=" in vmi_meta_text
    assert vme_meta_text.splitlines()[0] == f"data_file={vme_path.name}"
    assert vmi_meta_text.splitlines()[0] == f"data_file={vmi_path.name}"
    expected_copied_file_count = sum(
        2 * (len(paradigm_pair_map["vme"]) + len(paradigm_pair_map["vmi"]))
        for paradigm_pair_map in source_pair_by_subject.values()
    )
    assert len(generated_file_path_list) == expected_copied_file_count
    assert all(path.exists() for path in generated_file_path_list)


@pytest.mark.test_id("HEAVY-HELPER-18")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 为正式数据集复制出的每个 subject/session/run 生成的 yaml_path 必须仍落在 Collector/receiver/virtual_receiver/data 下，以兼容正式链路解析")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_build_heavy_dataset_spec")
def test_build_heavy_dataset_spec_keeps_virtual_receiver_relative_yaml_paths(tmp_path: Path) -> None:
    dataset_spec = _build_heavy_dataset_spec(tmp_path / "workspace", team_count=2)

    for subject_id, paradigm_map in dataset_spec["data_files"].items():
        assert set(paradigm_map) == {"vme", "vmi"}
        for entry in paradigm_map["vme"] + paradigm_map["vmi"]:
            yaml_path = entry["yaml_path"]
            assert yaml_path.startswith("Collector/receiver/virtual_receiver/data/")
            assert f"/{subject_id}/" in yaml_path
            assert "/session" in yaml_path
            assert yaml_path.endswith(".dat")


@pytest.mark.test_id("HEAVY-HELPER-19")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 样本来源发现必须按目录动态扫描有效 subject，并保留每个 subject 下所有正式 session/run；未来样本替换时无需改测试代码")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_collect_real_heavy_source_sample_pairs/_build_heavy_dataset_spec")
def test_build_heavy_dataset_spec_discovers_dynamic_valid_subjects_without_subject_count_inflation(tmp_path: Path) -> None:
    source_root = tmp_path / "source_data"
    workspace_root = tmp_path / "workspace"

    _write_source_sample_pair(source_root, "zzz_subject", "session1", "session2", b"ZZZ_VME", b"ZZZ_VMI")
    _write_source_sample_pair(source_root, "aaa_subject", "session1", "session2", b"AAA_VME", b"AAA_VMI")
    _write_source_sample_pair(source_root, "broken_subject", "session1", "session2", b"BROKEN_VME", None)
    _write_source_sample_pair(
        source_root,
        "aaa_subject",
        "session1",
        "session2",
        b"AAA_VME_RUN2",
        b"AAA_VMI_RUN2",
        run_index=2,
    )

    dataset_spec = _build_heavy_dataset_spec(
        workspace_root,
        team_count=17,
        source_receiver_data_root=source_root,
    )

    assert sorted(dataset_spec["data_files"].keys()) == ["aaa_subject", "zzz_subject"]
    aaa_vme_path_list = [Path(entry["source_path"]) for entry in dataset_spec["data_files"]["aaa_subject"]["vme"]]
    aaa_vmi_path_list = [Path(entry["source_path"]) for entry in dataset_spec["data_files"]["aaa_subject"]["vmi"]]
    zzz_vme_path_list = [Path(entry["source_path"]) for entry in dataset_spec["data_files"]["zzz_subject"]["vme"]]
    zzz_vmi_path_list = [Path(entry["source_path"]) for entry in dataset_spec["data_files"]["zzz_subject"]["vmi"]]

    assert [path.read_bytes() for path in aaa_vme_path_list] == [b"AAA_VME", b"AAA_VME_RUN2"]
    assert [path.read_bytes() for path in aaa_vmi_path_list] == [b"AAA_VMI", b"AAA_VMI_RUN2"]
    assert [path.read_bytes() for path in zzz_vme_path_list] == [b"ZZZ_VME"]
    assert [path.read_bytes() for path in zzz_vmi_path_list] == [b"ZZZ_VMI"]


@pytest.mark.test_id("HEAVY-HELPER-20")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 若 ProcessHub 已记录 team error 或 calibration 前断连，应立即失败而不是继续等待")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_collect_heavy_team_state_failure_parts/_detect_heavy_runtime_failure")
def test_detect_heavy_runtime_failure_reports_team_state_error(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    db_path = resolve_runtime_state_db_path(workspace_root)
    ensure_runtime_state_schema(db_path)
    write_json_state(
        db_path,
        "team:team_0",
        {
            "team_id": "team_0",
            "run_status": "error",
            "connection_status": "error",
            "calibration_ready": False,
            "last_disconnect_reason": "algorithm_data_connection_closed_before_task_finished: report_stream_closed",
            "last_error_type": "<class 'TimeoutError'>",
            "last_error_message": "predict worker timed",
            "updated_at": 1.0,
        },
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    detail_parts = _collect_heavy_team_state_failure_parts(db_path)
    detail = _detect_heavy_runtime_failure(environment)

    assert any("team_state_failure_count=1" in item for item in detail_parts)
    assert detail is not None
    assert "team_state_failure_count=1" in detail
    assert "predict worker timed" in detail
    assert "algorithm_data_connection_closed_before_task_finished" in detail


@pytest.mark.test_id("HEAVY-HELPER-20B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 若 Collector 已写入 current_trial error，必须立即失败并报告阶段和错误，不能继续等待整场超时")
@pytest.mark.tested(
    file="tests/helpers/heavy_runtime.py",
    function="_collect_heavy_current_trial_failure_parts/_detect_heavy_runtime_failure",
)
def test_detect_heavy_runtime_failure_reports_collector_stage_distribution_error(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    db_path = resolve_runtime_state_db_path(workspace_root)
    write_json_state(
        db_path,
        STATE_KEY_CURRENT_TRIAL,
        {
            "status": "error",
            "group_id": "group_1",
            "collector_component_id": "collector_group_1",
            "subject_id": "qyq",
            "exp_name": "vme",
            "exp_task": "right_vs_rest",
            "session_id": "session2",
            "error_type": "TimeoutError",
            "error_message": "calibration_private_team_4",
            "updated_at": 1.0,
        },
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    detail_parts = _collect_heavy_current_trial_failure_parts(db_path)
    detail = _detect_heavy_runtime_failure(environment)

    assert any("collector_stage_distribution_failure" in item for item in detail_parts)
    assert detail is not None
    assert "qyq/vme/right_vs_rest/session2" in detail
    assert "TimeoutError" in detail
    assert "calibration_private_team_4" in detail


@pytest.mark.test_id("HEAVY-HELPER-21")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 若 headless 或 algorithm 日志已出现 PredictWorkerTimeoutError/terminal_run_status=error，应立即在控制台抛错")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_collect_heavy_log_pattern_hits/_raise_on_heavy_runtime_failure_detected")
def test_raise_on_heavy_runtime_failure_detected_raises_for_log_hits(tmp_path: Path, capfd: pytest.CaptureFixture[str]) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    headless_log_root = results_root / "control" / "headless_logs"
    algorithm_log_root = tmp_path / "artifacts" / "algorithm_logs"
    headless_log_root.mkdir(parents=True, exist_ok=True)
    algorithm_log_root.mkdir(parents=True, exist_ok=True)
    (headless_log_root / "BCI_Judge__ProcessHub_team_0_group_1.stdout.log").write_text(
        "ExceptionPackageModel(... PredictWorkerTimeoutError ...)\n"
        "terminal_run_status=error\n",
        encoding="utf-8",
    )
    (algorithm_log_root / "algorithm_team_0.stdout.log").write_text(
        "PredictWorkerTimeoutError: predict worker timed out after 10.0s\n",
        encoding="utf-8",
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    hit_parts = _collect_heavy_log_pattern_hits(
        algorithm_log_root,
        pattern_spec_list=(("predict_worker_timeout", "PredictWorkerTimeoutError"),),
        glob_pattern="*.log",
    )

    assert hit_parts
    with pytest.raises(AssertionError, match="heavy runtime failure detected before full-chain completion"):
        _raise_on_heavy_runtime_failure_detected(environment, phase="waiting_match_finish")
    captured = capfd.readouterr()
    merged = f"{captured.out}\n{captured.err}"
    assert "[heavy_error] phase=waiting_match_finish" in merged
    assert "PredictWorkerTimeoutError" in merged


@pytest.mark.test_id("HEAVY-HELPER-22")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 若算法子进程提前退出，也必须纳入失败详情，避免只看 runtime_state 导致长时间盲等")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_collect_heavy_algorithm_process_exit_parts/_collect_heavy_runtime_failure_detail_parts")
def test_collect_heavy_runtime_failure_detail_parts_reports_algorithm_process_early_exit(tmp_path: Path) -> None:
    stdout_path = tmp_path / "algorithm_0.stdout.log"
    stderr_path = tmp_path / "algorithm_0.stderr.log"
    stdout_path.write_text("", encoding="utf-8")
    stderr_path.write_text("", encoding="utf-8")

    class _FakeProcess:
        def poll(self) -> int:
            return 1

    managed_process = ManagedProcess(
        name="algorithm_team_0",
        process=_FakeProcess(),  # type: ignore[arg-type]
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        cwd=tmp_path,
        command=["python", "-m", "Algorithm.main"],
        started_at=1.0,
    )
    environment = HeavyEnvironment(
        workspace_root=tmp_path / "workspace",
        artifact_root=tmp_path / "artifacts",
        results_root=tmp_path / "workspace" / "results",
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[managed_process],
        judge_web_url="http://127.0.0.1:18080",
    )

    exit_parts = _collect_heavy_algorithm_process_exit_parts(environment)
    detail_parts = _collect_heavy_runtime_failure_detail_parts(
        environment,
        resolve_runtime_state_db_path(environment.workspace_root),
    )

    assert exit_parts
    assert any("algorithm_process_early_exit=" in item for item in exit_parts)
    assert any("algorithm_process_early_exit=" in item for item in detail_parts)


@pytest.mark.test_id("HEAVY-HELPER-25")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 控制台错误输出必须使用 ASCII-safe 形式，避免 Windows 默认控制台编码导致故障详情不可见")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="_emit_heavy_console_text")
def test_emit_heavy_console_text_is_ascii_safe(capfd: pytest.CaptureFixture[str]) -> None:
    _emit_heavy_console_text("PredictWorkerTimeoutError: \u7b97\u6cd5\u5f02\u5e38 \u03b1")

    captured = capfd.readouterr()
    merged = f"{captured.out}\n{captured.err}"
    assert "PredictWorkerTimeoutError:" in merged
    assert "\\u7b97\\u6cd5\\u5f02\\u5e38" in merged


@pytest.mark.test_id("HEAVY-HELPER-23")
@pytest.mark.priority("P0")
@pytest.mark.requirement("prepare_heavy_workspace 不能把仓库原始 org_data 整包复制进 heavy workspace；virtual_receiver/data 只能保留正式数据集中发现到的有效 subject 目录")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="prepare_heavy_workspace/_ignore_heavy_workspace_app_copy")
def test_prepare_heavy_workspace_excludes_bulk_receiver_data_and_org_data(tmp_path: Path) -> None:
    environment = prepare_heavy_workspace(tmp_path / "artifacts", team_count=2)
    try:
        receiver_data_root = environment.generated_receiver_data_root
        assert receiver_data_root is not None
        source_subject_key_list = sorted(
            _collect_real_heavy_source_sample_pairs(
                Path("app/Collector/Collector/receiver/virtual_receiver/data")
            ).keys()
        )
        assert sorted(path.name for path in receiver_data_root.iterdir() if path.is_dir()) == source_subject_key_list
        assert not (
            environment.workspace_root
            / "app"
            / "Collector"
            / "Collector"
            / "receiver"
            / "virtual_receiver"
            / "org_data"
        ).exists()
        assert environment.expected_subject_count == len(source_subject_key_list)
        assert environment.expected_stage_count == 40
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-HELPER-24")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 清理只能删除本次复制出来的数据文件，不能删除 virtual_receiver/data 目录本身、org_data 或任何未登记的原始文件")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="shutdown_heavy_environment")
def test_shutdown_heavy_environment_removes_only_tracked_generated_files(tmp_path: Path) -> None:
    receiver_root = tmp_path / "workspace" / "app" / "Collector" / "Collector" / "receiver" / "virtual_receiver"
    data_root = receiver_root / "data"
    session_root = data_root / "S1" / "session1"
    org_data_root = receiver_root / "org_data"
    session_root.mkdir(parents=True, exist_ok=True)
    org_data_root.mkdir(parents=True, exist_ok=True)

    copied_dat_path = session_root / "sub_S1_vme_run1.dat"
    copied_meta_path = session_root / "sub_S1_vme_run1_meta.txt"
    untracked_original_path = data_root / "keep_source.dat"
    org_data_path = org_data_root / "keep_org.dat"
    copied_dat_path.write_bytes(b"COPIED")
    copied_meta_path.write_text("data_file=sub_S1_vme_run1.dat\n", encoding="utf-8")
    untracked_original_path.write_bytes(b"ORIGINAL")
    org_data_path.write_bytes(b"ORG")

    environment = HeavyEnvironment(
        workspace_root=tmp_path / "workspace",
        artifact_root=tmp_path / "artifacts",
        results_root=tmp_path / "workspace" / "results",
        team_config_list=[],
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
        generated_receiver_data_root=data_root,
        generated_receiver_data_file_list=[copied_dat_path, copied_meta_path],
    )

    shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-HELPER-24B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 清理必须按 judge_process_manifest 回收启动器退出后仍存活的裁判进程，并拒绝结束 PID 已复用的进程")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="shutdown_heavy_environment")
def test_shutdown_heavy_environment_terminates_only_verified_manifest_processes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    results_root = workspace_root / "results"
    valid_cwd = workspace_root / "proceed" / "task"
    stale_cwd = workspace_root / "app" / "JudgeWeb"
    valid_cwd.mkdir(parents=True)
    stale_cwd.mkdir(parents=True)
    manifest_path = results_root / "control" / "judge_process_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "processes": [
                    {
                        "pid": 41001,
                        "started_at": 1000.0,
                        "cwd": str(valid_cwd),
                        "command": "java -jar task.jar",
                    },
                    {
                        "pid": 41002,
                        "started_at": 1000.0,
                        "cwd": str(stale_cwd),
                        "command": "python -m JudgeWeb.main",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    terminated_process_id_list: list[int] = []

    class _FakeProcess:
        def __init__(self, process_id: int, started_at: float, cwd: Path) -> None:
            self.pid = process_id
            self._started_at = started_at
            self._cwd = cwd

        def create_time(self) -> float:
            return self._started_at

        def cwd(self) -> str:
            return str(self._cwd)

        def children(self, recursive: bool = False) -> list:
            assert recursive is True
            return []

        def terminate(self) -> None:
            terminated_process_id_list.append(self.pid)

        def kill(self) -> None:
            raise AssertionError("graceful manifest cleanup should not need kill")

    process_by_id = {
        41001: _FakeProcess(41001, 1001.0, valid_cwd),
        41002: _FakeProcess(41002, 1060.0, stale_cwd),
    }
    monkeypatch.setattr(
        "tests.helpers.heavy_runtime.psutil.Process",
        lambda process_id: process_by_id[process_id],
    )
    monkeypatch.setattr(
        "tests.helpers.heavy_runtime.psutil.wait_procs",
        lambda process_list, timeout: (list(process_list), []),
    )
    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=tmp_path / "artifacts",
        results_root=results_root,
        team_config_list=[],
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    shutdown_heavy_environment(environment)

    assert terminated_process_id_list == [41001]


@pytest.mark.test_id("HEAVY-HELPER-24A")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 失败时必须把 real_full_chain 工件复制到 failed_test_artifacts，避免现场复盘时丢失 headless 日志和 workspace")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="preserve_heavy_failure_artifacts")
def test_preserve_heavy_failure_artifacts_copies_artifact_tree(tmp_path: Path) -> None:
    artifact_root = tmp_path / "latest" / "heavy" / "real_full_chain"
    failed_root = tmp_path / "latest" / "failed_test_artifacts"
    (artifact_root / "judge_stack_logs").mkdir(parents=True, exist_ok=True)
    (artifact_root / "judge_stack_logs" / "judge.log").write_text("judge-start\n", encoding="utf-8")
    environment = HeavyEnvironment(
        workspace_root=artifact_root / "heavy_workspace",
        artifact_root=artifact_root,
        results_root=artifact_root / "heavy_workspace" / "results",
        team_config_list=build_team_config(HEAVY_DEFAULT_TEAM_COUNT, HEAVY_BASE_PORT),
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    preserved_root = preserve_heavy_failure_artifacts(
        environment,
        test_label="HEAVY-REAL-01",
        failed_root=failed_root,
    )

    assert preserved_root == failed_root / "HEAVY-REAL-01"
    assert preserved_root.exists()
    assert (preserved_root / "judge_stack_logs" / "judge.log").read_text(encoding="utf-8") == "judge-start\n"


@pytest.mark.test_id("HEAVY-HELPER-24C")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 失败工件复制不得因 Kafka broker 运行时锁文件而整棵失败，且必须保留其他诊断日志")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="preserve_heavy_failure_artifacts")
def test_preserve_heavy_failure_artifacts_skips_kafka_runtime_lock(tmp_path: Path) -> None:
    artifact_root = tmp_path / "latest" / "heavy" / "real_full_chain"
    broker_root = artifact_root / "heavy_workspace" / "proceed" / "centrol" / "runtime" / "kafka" / "broker-test"
    broker_root.mkdir(parents=True, exist_ok=True)
    (broker_root / ".lock").write_text("locked\n", encoding="utf-8")
    (broker_root / "recovery-point-offset-checkpoint").write_text("diagnostic\n", encoding="utf-8")
    environment = HeavyEnvironment(
        workspace_root=artifact_root / "heavy_workspace",
        artifact_root=artifact_root,
        results_root=artifact_root / "heavy_workspace" / "results",
        team_config_list=[],
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )

    preserved_root = preserve_heavy_failure_artifacts(
        environment,
        test_label="HEAVY-REAL-01",
        failed_root=tmp_path / "latest" / "failed_test_artifacts",
    )

    preserved_broker_root = preserved_root / broker_root.relative_to(artifact_root)
    assert not (preserved_broker_root / ".lock").exists()
    assert (preserved_broker_root / "recovery-point-offset-checkpoint").read_text(encoding="utf-8") == "diagnostic\n"


@pytest.mark.test_id("HEAVY-HELPER-24D")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 失败现场必须先停止持锁进程再复制，避免复制中的文件锁和继续写入导致现场不完整")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="shutdown_and_preserve_heavy_failure_artifacts")
def test_shutdown_and_preserve_heavy_failure_artifacts_stops_before_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "latest" / "heavy" / "real_full_chain"
    artifact_root.mkdir(parents=True)
    environment = HeavyEnvironment(
        workspace_root=artifact_root / "heavy_workspace",
        artifact_root=artifact_root,
        results_root=artifact_root / "heavy_workspace" / "results",
        team_config_list=[],
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
    )
    event_list: list[str] = []

    monkeypatch.setitem(
        shutdown_and_preserve_heavy_failure_artifacts.__globals__,
        "shutdown_heavy_environment",
        lambda _environment: event_list.append("shutdown"),
    )
    original_preserve = preserve_heavy_failure_artifacts

    def record_preserve(*args, **kwargs):
        event_list.append("preserve")
        return original_preserve(*args, **kwargs)

    monkeypatch.setitem(
        shutdown_and_preserve_heavy_failure_artifacts.__globals__,
        "preserve_heavy_failure_artifacts",
        record_preserve,
    )

    preserved_root = shutdown_and_preserve_heavy_failure_artifacts(
        environment,
        test_label="HEAVY-REAL-01",
        failed_root=tmp_path / "latest" / "failed_test_artifacts",
    )

    assert event_list == ["shutdown", "preserve"]
    assert preserved_root == tmp_path / "latest" / "failed_test_artifacts" / "HEAVY-REAL-01"


def _profile_for_index(index: int) -> str:
    return build_heavy_team_scenarios(HEAVY_DEFAULT_TEAM_COUNT)[f"team_{index}"].algorithm_profile


def _write_source_sample_pair(
    source_root: Path,
    subject_name: str,
    vme_session_name: str,
    vmi_session_name: str,
    vme_bytes: bytes,
    vmi_bytes: bytes | None,
    run_index: int = 1,
) -> None:
    vme_session_root = source_root / subject_name / vme_session_name
    vmi_session_root = source_root / subject_name / vmi_session_name
    vme_session_root.mkdir(parents=True, exist_ok=True)
    vmi_session_root.mkdir(parents=True, exist_ok=True)
    vme_dat_path = vme_session_root / f"{subject_name}_vme_run{int(run_index)}.dat"
    vme_meta_path = vme_session_root / f"{subject_name}_vme_run{int(run_index)}_meta.txt"
    vme_dat_path.write_bytes(vme_bytes)
    vme_meta_path.write_text(
        "\n".join(
            [
                f"data_file={vme_dat_path.name}",
                "storage_format=binary_float32_le",
                "channel_labels=C3,C4,Cz",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    if vmi_bytes is None:
        return
    vmi_dat_path = vmi_session_root / f"{subject_name}_vmi_run{int(run_index)}.dat"
    vmi_meta_path = vmi_session_root / f"{subject_name}_vmi_run{int(run_index)}_meta.txt"
    vmi_dat_path.write_bytes(vmi_bytes)
    vmi_meta_path.write_text(
        "\n".join(
            [
                f"data_file={vmi_dat_path.name}",
                "storage_format=binary_float32_le",
                "channel_labels=C3,C4,Cz",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _make_heavy_trial_rows(team_id: str, profile: str) -> list[dict]:
    row_list = [
        _trial_row(team_id, 1, "S1_vme_session1", "vme", "session1", "1"),
        _trial_row(team_id, 2, "S1_vme_session1", "vme", "session1", "2"),
        _trial_row(team_id, 3, "S1_vmi_session2", "vmi", "session2", "1"),
        _trial_row(team_id, 4, "S1_vmi_session2", "vmi", "session2", "2"),
    ]
    if profile in {"slow", "late_result", "disconnect_stream", "resource_hog"}:
        row_list[0]["is_timeout"] = True
        row_list[0]["predict_time_ms"] = 1000.0
    if profile in {"invalid_output", "malicious"}:
        row_list[0]["is_invalid_output"] = True
        row_list[0]["predict_label"] = ""
    return row_list


def _trial_row(
    team_id: str,
    team_trial_index: int,
    task_id: str,
    exp_name: str,
    session_id: str,
    trial_id: str,
) -> dict:
    return {
        "team_id": team_id,
        "team_trial_index": team_trial_index,
        "task_trial_index": 1 if trial_id == "1" else 2,
        "task_id": task_id,
        "subject_id": "S1",
        "exp_name": exp_name,
        "exp_task": "left_vs_rest" if exp_name == "vme" else "right_vs_rest",
        "session_id": session_id,
        "block_id": "block_1",
        "trial_id": trial_id,
        "true_label": "0",
        "predict_label": "0",
        "is_correct": True,
        "trial_score": 1.0,
        "predict_time_ms": 10.0,
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": float(team_trial_index),
        "is_timeout": False,
        "report_position": "trial_end",
    }


def _write_score_csv(csv_path: Path) -> None:
    lines = [
        "team_id,total_score,run_status,observed_trial_count",
        *[
            f"team_{index},{float(HEAVY_DEFAULT_TEAM_COUNT + 1 - index)},finished,4"
            for index in range(HEAVY_DEFAULT_TEAM_COUNT)
        ],
    ]
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_heavy_profile_observations(results_root: Path) -> None:
    observation_root = results_root / "control" / "heavy_profile_observations"
    observation_root.mkdir(parents=True, exist_ok=True)
    for index in range(HEAVY_DEFAULT_TEAM_COUNT):
        team_id = f"team_{index}"
        profile = _profile_for_index(index)
        if profile == "normal":
            continue
        if profile == "malicious":
            line_list = [
                f'{{"team_id":"{team_id}","profile":"malicious","event":"malicious_action","operation":"{operation}","status":"blocked"}}'
                for operation in ("read_hidden_score", "write_results", "network_access", "kill_process")
            ]
        else:
            line_list = [f'{{"team_id":"{team_id}","profile":"{profile}","event":"profile_observed"}}']
        (observation_root / f"{team_id}.jsonl").write_text("\n".join(line_list) + "\n", encoding="utf-8")
