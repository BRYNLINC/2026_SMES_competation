from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import psutil
import yaml

from tests.helpers.result_assertions import (
    REQUIRED_SCOREBOARD_FIELDS,
    REQUIRED_TRIAL_FIELDS,
    assert_required_fields,
    assert_scoreboard_sorted,
    load_csv_rows,
)
from tests.helpers.config_factory import (
    build_team_config,
    patch_judge_web_config,
    write_central_controller_config,
    write_runtime_stage_config,
    write_virtual_receiver_config,
)
from tests.helpers.process_runner import (
    ManagedProcess,
    build_subprocess_env,
    _is_port_listening_by_netstat,
    start_python_module,
    terminate_tree,
    wait_for_http,
    wait_for_port,
)
from tests.helpers.project_paths import project_root
from tools.runtime_state_sqlite import (
    STATE_KEY_CURRENT_TRIAL,
    STATE_KEY_MATCH_CONTROL_STATUS,
    STATE_KEY_RUNTIME_STAGE_STATUS,
    TEAM_STATE_KEY_PREFIX,
    count_json_state_by_prefix,
    json_state_exists,
    list_json_state_by_prefix,
    load_team_score_overview_rows,
    load_team_subject_task_overview_rows,
    load_team_task_overview_rows,
    load_team_trial_record_rows,
    read_json_state,
    resolve_runtime_state_db_path,
)


PROJECT_ROOT = project_root()


def _resolve_float_env(name: str, default_value: float, minimum: float | None = None) -> float:
    raw_value = os.environ.get(name)
    if raw_value is None or str(raw_value).strip() == "":
        resolved_value = float(default_value)
    else:
        try:
            resolved_value = float(str(raw_value).strip())
        except ValueError:
            resolved_value = float(default_value)
    if minimum is not None:
        resolved_value = max(float(minimum), resolved_value)
    return resolved_value


HEAVY_DEFAULT_TEAM_COUNT = 17
HEAVY_BASE_PORT = 29981
HEAVY_PREPARE_ESTIMATED_SECONDS = 120.0
HEAVY_ESTIMATED_STAGE_SECONDS = 240.0
HEAVY_DEFAULT_MATCH_TIMEOUT_SECONDS = 28800.0
HEAVY_MATCH_TIMEOUT_SECONDS = _resolve_float_env(
    "BCI_HEAVY_MATCH_TIMEOUT_SECONDS",
    HEAVY_DEFAULT_MATCH_TIMEOUT_SECONDS,
    minimum=10800.0,
)
HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS = 120.0
HEAVY_PROGRESS_EMIT_INTERVAL_SECONDS = 15.0
HEAVY_READINESS_EMIT_INTERVAL_SECONDS = 10.0
HEAVY_PROGRESS_REPLACE_RETRY_COUNT = 8
HEAVY_PROGRESS_REPLACE_RETRY_DELAY_SECONDS = 0.1
HEAVY_ALGORITHM_CPU_THREAD_COUNT = 1
HEAVY_ALGORITHM_STARTUP_TIMEOUT_SECONDS = 120.0
HEAVY_JUDGE_WEB_STARTUP_TIMEOUT_SECONDS = 360.0
HEAVY_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS = 90.0
HEAVY_MANIFEST_PROCESS_START_TIME_TOLERANCE_SECONDS = 5.0
HEAVY_MANIFEST_PROCESS_TERMINATE_TIMEOUT_SECONDS = 10.0
HEAVY_DEFAULT_DATASET_SUBJECT_COUNT: int | None = None
HEAVY_PROFILE_LIST = [
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
]
assert len(HEAVY_PROFILE_LIST) >= HEAVY_DEFAULT_TEAM_COUNT


@dataclass(frozen=True)
class HeavyTeamScenario:
    team_id: str
    algorithm_profile: str
    expected_observation: str
    description: str


@dataclass
class HeavyEnvironment:
    workspace_root: Path
    artifact_root: Path
    results_root: Path
    team_config_list: list[dict]
    judge_processes: list[ManagedProcess]
    algorithm_processes: list[ManagedProcess]
    judge_web_url: str
    algorithm_workspace_by_team_id: dict[str, Path] = field(default_factory=dict)
    scenario_by_team_id: dict[str, HeavyTeamScenario] = field(default_factory=dict)
    expected_subject_count: int = 0
    expected_stage_count: int = 0
    generated_receiver_data_root: Path | None = None
    generated_receiver_data_file_list: list[Path] = field(default_factory=list)
    progress_start_monotonic: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class LocalJsonFetchResult:
    payload: dict | None
    error: str | None
    source: str
    status_code: int | None = None


def build_heavy_team_config(team_count: int = HEAVY_DEFAULT_TEAM_COUNT, base_port: int = HEAVY_BASE_PORT) -> list[dict]:
    return build_team_config(team_count, base_port, profiles=HEAVY_PROFILE_LIST[:team_count])


def build_heavy_team_scenarios(team_count: int = HEAVY_DEFAULT_TEAM_COUNT) -> dict[str, HeavyTeamScenario]:
    scenario_description_by_profile = {
        "normal": ("normal_completion", "正常队伍完整完成所有 subject/session/task/trial"),
        "slow": ("timeout_trial", "首个在线 trial 故意超过平台 predict timeout，随后恢复正常"),
        "late_result": ("late_timeout_trial", "首个在线 trial 模拟 timeout 后晚到结果，不得污染后续 trial"),
        "disconnect_stream": ("transient_stream_timeout", "首个在线 trial 模拟数据流中断/重连窗口，后续继续完赛"),
        "duplicate_result": ("deduplicated_trial", "首个在线 trial 重复 report，同一 trial 只能保留一条结果"),
        "invalid_output": ("invalid_output_trial", "首个在线 trial 返回非法输出，必须记为 invalid 且比赛继续"),
        "resource_hog": ("resource_timeout_trial", "首个在线 trial 模拟 CPU/内存占用并触发 timeout，不能拖垮其他队"),
        "malicious": ("malicious_blocked", "恶意读写结果/隐藏分/网络/杀进程动作只能记录为 blocked，不得执行副作用"),
    }
    scenario_by_team_id: dict[str, HeavyTeamScenario] = {}
    for team_index, profile_name in enumerate(HEAVY_PROFILE_LIST[:team_count]):
        expected_observation, description = scenario_description_by_profile[profile_name]
        team_id = f"team_{team_index}"
        scenario_by_team_id[team_id] = HeavyTeamScenario(
            team_id=team_id,
            algorithm_profile=profile_name,
            expected_observation=expected_observation,
            description=description,
        )
    return scenario_by_team_id


def prepare_heavy_workspace(
    artifact_root: Path,
    team_count: int = HEAVY_DEFAULT_TEAM_COUNT,
    *,
    source_receiver_data_root: Path | None = None,
) -> HeavyEnvironment:
    progress = HeavyProgressReporter(
        total_steps=6,
        estimated_seconds=HEAVY_PREPARE_ESTIMATED_SECONDS + HEAVY_MATCH_TIMEOUT_SECONDS,
    )
    progress.emit(1, "prepare_workspace", f"copying app/tools/proceed and generating {team_count}-team configs")
    artifact_root = Path(artifact_root)
    _reset_heavy_artifact_root(artifact_root)
    workspace_root = artifact_root / "heavy_workspace"
    shutil.copytree(PROJECT_ROOT / "app", workspace_root / "app", ignore=_ignore_heavy_workspace_app_copy)
    shutil.copytree(PROJECT_ROOT / "tools", workspace_root / "tools")
    shutil.copytree(
        PROJECT_ROOT / "proceed",
        workspace_root / "proceed",
        ignore=_ignore_heavy_workspace_proceed_copy,
    )
    if (PROJECT_ROOT / "judge-dashboard").exists():
        shutil.copytree(
            PROJECT_ROOT / "judge-dashboard",
            workspace_root / "judge-dashboard",
            ignore=_ignore_heavy_workspace_dashboard_copy,
        )
    (workspace_root / "results").mkdir(parents=True, exist_ok=True)

    generated_receiver_data_root = _resolve_heavy_receiver_data_root(workspace_root)
    generated_receiver_data_file_list: list[Path] = []
    dataset_spec = _build_heavy_dataset_spec(
        workspace_root,
        team_count,
        source_receiver_data_root=source_receiver_data_root,
        generated_file_path_list=generated_receiver_data_file_list,
    )
    team_config_list = build_heavy_team_config(team_count=team_count)
    scenario_by_team_id = build_heavy_team_scenarios(team_count=team_count)

    write_central_controller_config(workspace_root, team_config_list)
    write_runtime_stage_config(
        workspace_root,
        {"group_1": [team_config["team_id"] for team_config in team_config_list]},
    )
    write_virtual_receiver_config(workspace_root, dataset_spec)
    patch_judge_web_config(workspace_root, "127.0.0.1", 18080, True)
    _patch_headless_start_judge_stack(workspace_root)
    _patch_central_controller_component_monitor_for_heavy(workspace_root)
    algorithm_workspace_by_team_id = _prepare_team_algorithm_workspaces(
        artifact_root=artifact_root,
        team_config_list=team_config_list,
        observation_root=workspace_root / "results" / "control" / "heavy_profile_observations",
    )

    environment = HeavyEnvironment(
        workspace_root=workspace_root,
        artifact_root=artifact_root,
        results_root=workspace_root / "results",
        team_config_list=team_config_list,
        judge_processes=[],
        algorithm_processes=[],
        judge_web_url="http://127.0.0.1:18080",
        algorithm_workspace_by_team_id=algorithm_workspace_by_team_id,
        scenario_by_team_id=scenario_by_team_id,
        expected_subject_count=len(dataset_spec.get("data_files") or {}),
        expected_stage_count=_count_expected_heavy_stage_count(dataset_spec),
        generated_receiver_data_root=generated_receiver_data_root,
        generated_receiver_data_file_list=generated_receiver_data_file_list,
    )
    progress.emit(
        2,
        "workspace_ready",
        (
            f"team_count={team_count} "
            f"dataset_subject_count={len(dataset_spec.get('data_files') or {})} "
            f"expected_stage_count={environment.expected_stage_count} "
            f"results_root={environment.results_root} "
            f"profiles={','.join(item.algorithm_profile for item in scenario_by_team_id.values())}"
        ),
    )
    return environment


def _reset_heavy_artifact_root(artifact_root: Path) -> None:
    if artifact_root.exists():
        try:
            shutil.rmtree(artifact_root)
        except OSError as exc:
            raise RuntimeError(
                "cannot reset heavy artifact root: "
                f"path={artifact_root} error={type(exc).__name__}: {exc}. "
                "A stale heavy runtime process may still be using this directory; stop it before rerunning."
            ) from exc
    if artifact_root.exists():
        raise RuntimeError(
            "cannot reset heavy artifact root: "
            f"path still exists after removal: {artifact_root}. "
            "A stale heavy runtime process may still be using this directory; stop it before rerunning."
        )


def start_heavy_algorithms(environment: HeavyEnvironment, python_executable: str) -> list[ManagedProcess]:
    progress = HeavyProgressReporter.from_environment(environment)
    progress.emit(3, "start_algorithms", f"starting {len(environment.team_config_list)} real Algorithm.main processes")
    validate_heavy_python_runtime(python_executable)
    managed_processes: list[ManagedProcess] = []
    for team_config in environment.team_config_list:
        algorithm_port = int(team_config["algorithm_port"])
        team_id = str(team_config["team_id"])
        algorithm_workspace_root = environment.algorithm_workspace_by_team_id[team_id]
        algorithm_root = algorithm_workspace_root / "app" / "Algorithm"
        process = start_python_module(
            python_executable=python_executable,
            module="Algorithm.main",
            cwd=algorithm_root,
            artifact_dir=environment.artifact_root / "algorithm_logs",
            name=f"algorithm_{team_id}",
            env={
                "PYTHONPATH": str(algorithm_root.parent),
                "TEAM_ID": team_id,
                "PYTHONHASHSEED": "0",
                "OMP_NUM_THREADS": str(HEAVY_ALGORITHM_CPU_THREAD_COUNT),
                "MKL_NUM_THREADS": str(HEAVY_ALGORITHM_CPU_THREAD_COUNT),
                "OPENBLAS_NUM_THREADS": str(HEAVY_ALGORITHM_CPU_THREAD_COUNT),
                "NUMEXPR_NUM_THREADS": str(HEAVY_ALGORITHM_CPU_THREAD_COUNT),
                "BCI_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS": str(
                    HEAVY_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS
                ),
            },
        )
        managed_processes.append(process)
    for team_config in environment.team_config_list:
        algorithm_port = int(team_config["algorithm_port"])
        assert (
            _is_port_listening_by_netstat(algorithm_port)
            or wait_for_port(
                "127.0.0.1",
                algorithm_port,
                timeout=HEAVY_ALGORITHM_STARTUP_TIMEOUT_SECONDS,
            )
        ), (
            f"algorithm port not ready: {team_config['team_id']} {team_config['algorithm_port']}"
        )
        progress.emit(
            3,
            "algorithm_port_ready",
            f"{team_config['team_id']} port={team_config['algorithm_port']}",
        )
    environment.algorithm_processes = managed_processes
    return managed_processes


def start_headless_judge_stack(environment: HeavyEnvironment, python_executable: str) -> ManagedProcess:
    progress = HeavyProgressReporter.from_environment(environment)
    progress.emit(4, "start_judge_stack", "starting CentralController/Collector/RuntimeStageCoordinator/ProcessHubs/JudgeWeb")
    validate_heavy_python_runtime(python_executable)
    process = start_python_module(
        python_executable=python_executable,
        module="tools.start_judge_stack",
        cwd=environment.workspace_root,
        artifact_dir=environment.artifact_root / "judge_stack_logs",
        name="judge_stack_start",
        env={
            "PYTHONPATH": str(environment.workspace_root),
            "BCI_HEADLESS": "1",
            "BCI_MATCH_START_MODE": "clear",
            "PYTHONHASHSEED": "0",
        },
    )
    environment.judge_processes = [process]
    if not wait_for_http(
        f"{environment.judge_web_url}/healthz",
        timeout=HEAVY_JUDGE_WEB_STARTUP_TIMEOUT_SECONDS,
    ):
        failure_detail = _build_missing_runtime_state_failure_detail(environment)
        _emit_heavy_console_text("[heavy_error] phase=judge_web_startup summary=JudgeWeb not ready in heavy mode")
        _emit_heavy_console_text(f"[heavy_error_detail]\n{failure_detail}")
        raise AssertionError("JudgeWeb not ready in heavy mode.\n" + failure_detail)
    progress.emit(
        4,
        "judge_web_ready",
        (
            f"url={environment.judge_web_url} "
            f"headless_logs={environment.results_root / 'control' / 'headless_logs'}"
        ),
    )
    _assert_formal_runtime_state_bootstrap(environment)
    _trigger_formal_start_match(environment)
    return process


def _assert_formal_runtime_state_bootstrap(environment: HeavyEnvironment) -> None:
    progress = HeavyProgressReporter.from_environment(environment)
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    deadline = time.monotonic() + float(HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS)
    last_match_control_status: dict | None = None
    last_runtime_stage_status: dict | None = None
    while time.monotonic() < deadline:
        if runtime_state_db_path.exists():
            last_match_control_status = read_json_state(runtime_state_db_path, STATE_KEY_MATCH_CONTROL_STATUS) or {}
            last_runtime_stage_status = read_json_state(runtime_state_db_path, STATE_KEY_RUNTIME_STAGE_STATUS) or {}
            if isinstance(last_match_control_status, dict) and last_match_control_status:
                progress.emit(
                    5,
                    "runtime_state_bootstrapped",
                    (
                        f"runtime_state_db={runtime_state_db_path} "
                        f"match_started={bool(last_match_control_status.get('match_started'))} "
                        f"match_finished={bool(last_match_control_status.get('match_finished'))}"
                    ),
                    progress_ratio_override=0.25,
                )
                return
            if isinstance(last_runtime_stage_status, dict) and last_runtime_stage_status:
                progress.emit(
                    5,
                    "runtime_state_bootstrapped",
                    (
                        f"runtime_state_db={runtime_state_db_path} "
                        f"runtime_stage_groups={len(last_runtime_stage_status.get('group_status_list') or [])}"
                    ),
                    progress_ratio_override=0.25,
                )
                return
        _raise_on_heavy_runtime_failure_detected(environment, phase="runtime_state_bootstrap")
        time.sleep(1.0)
    raise AssertionError(
        "formal judge-chain bootstrap missing: runtime_state.db or early runtime status was not created "
        f"within {int(HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS)}s after JudgeWeb became ready.\n"
        + _build_missing_runtime_state_failure_detail(environment)
    )


def _trigger_formal_start_match(environment: HeavyEnvironment) -> None:
    progress = HeavyProgressReporter.from_environment(environment)
    control_status_url = f"{environment.judge_web_url}/api/v1/control/status"
    start_match_url = f"{environment.judge_web_url}/api/v1/control/start-match"
    readiness_payload = _wait_for_formal_start_readiness(environment, control_status_url)
    progress.emit(
        5,
        "start_readiness_ready",
        (
            f"control_status_url={control_status_url} "
            f"connected_teams={len(readiness_payload.get('connected_team_id_list') or [])}/"
            f"{len(readiness_payload.get('configured_team_id_list') or [])} "
            f"pending_groups={len(readiness_payload.get('pending_group_id_list') or [])}"
        ),
        progress_ratio_override=0.28,
    )
    fetch_result = _fetch_local_json(start_match_url, timeout_seconds=10.0, method="POST", json_payload={})
    if fetch_result.payload is None:
        raise AssertionError(
            "formal start-match request failed. "
            + _format_local_json_fetch_detail(start_match_url, fetch_result)
        )
    payload = fetch_result.payload
    if not bool(payload.get("ok")):
        raise AssertionError(
            "formal start-match request returned non-ok payload: "
            f"{payload}; {_format_local_json_fetch_detail(start_match_url, fetch_result)}"
        )
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        match_control_status = read_json_state(runtime_state_db_path, STATE_KEY_MATCH_CONTROL_STATUS) or {}
        if bool(match_control_status.get("match_started")):
            progress.emit(
                5,
                "match_started",
                (
                    f"start_match_url={start_match_url} "
                    f"started_at={match_control_status.get('started_at')}"
                ),
                progress_ratio_override=0.30,
            )
            return
        _raise_on_heavy_runtime_failure_detected(environment, phase="waiting_match_started")
        time.sleep(1.0)
    raise AssertionError(
        "formal start-match request was accepted but match_started did not become true within 30s. "
        f"last_status={read_json_state(runtime_state_db_path, STATE_KEY_MATCH_CONTROL_STATUS) or {}}"
    )


def _wait_for_formal_start_readiness(environment: HeavyEnvironment, control_status_url: str) -> dict:
    deadline = time.monotonic() + 180.0
    progress = HeavyProgressReporter.from_environment(environment)
    last_fetch_result = LocalJsonFetchResult(payload=None, error="not_requested", source="none", status_code=None)
    last_emit_monotonic = 0.0
    while time.monotonic() < deadline:
        last_fetch_result = _fetch_local_json(control_status_url, timeout_seconds=10.0)
        payload = last_fetch_result.payload or {}
        start_readiness = payload.get("start_readiness") or {}
        if bool(start_readiness.get("ready")):
            return start_readiness
        _raise_on_heavy_runtime_failure_detected(environment, phase="waiting_start_readiness")
        current_monotonic = time.monotonic()
        if current_monotonic - last_emit_monotonic >= HEAVY_READINESS_EMIT_INTERVAL_SECONDS:
            progress.emit(
                5,
                "waiting_start_readiness",
                _summarize_start_readiness_progress(environment, control_status_url, last_fetch_result),
                progress_ratio_override=_resolve_waiting_start_readiness_progress_ratio(payload),
            )
            last_emit_monotonic = current_monotonic
        time.sleep(1.0)
    raise AssertionError(
        "formal start readiness was not reached within 180s after JudgeWeb became ready.\n"
        + _build_formal_start_readiness_failure_detail(environment, control_status_url, last_fetch_result)
    )


def wait_for_heavy_completion(environment: HeavyEnvironment, timeout_seconds: float = HEAVY_MATCH_TIMEOUT_SECONDS) -> None:
    estimated_seconds = max(float(timeout_seconds), _estimate_heavy_match_seconds_from_environment(environment))
    progress = HeavyProgressReporter.from_environment(environment, estimated_seconds=estimated_seconds)
    deadline = time.time() + float(timeout_seconds)
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    last_status: dict | None = None
    last_emit_monotonic = 0.0
    missing_runtime_state_since_monotonic: float | None = None
    while time.time() < deadline:
        match_control_status = read_json_state(runtime_state_db_path, STATE_KEY_MATCH_CONTROL_STATUS) or {}
        last_status = match_control_status
        if bool(match_control_status.get("match_finished")):
            progress.emit(
                5,
                "match_finished",
                "runtime_state reports match_finished=true",
                progress_ratio_override=0.97,
            )
            assert_heavy_completion_outputs(environment, expected_team_count=len(environment.team_config_list))
            progress.emit(
                6,
                "result_validation_complete",
                (
                    f"runtime_state_db={runtime_state_db_path} "
                    f"score_csv={environment.results_root / '00_team_score_overview.csv'}"
                ),
                progress_ratio_override=1.0,
            )
            return
        if not runtime_state_db_path.exists():
            if missing_runtime_state_since_monotonic is None:
                missing_runtime_state_since_monotonic = time.monotonic()
            elif time.monotonic() - missing_runtime_state_since_monotonic >= HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS:
                raise AssertionError(_build_missing_runtime_state_failure_detail(environment))
        else:
            missing_runtime_state_since_monotonic = None
        _raise_on_heavy_runtime_failure_detected(environment, phase="waiting_match_finish")
        current_monotonic = time.monotonic()
        if current_monotonic - last_emit_monotonic >= HEAVY_PROGRESS_EMIT_INTERVAL_SECONDS:
            progress.emit(
                5,
                "waiting_match_finish",
                _summarize_heavy_runtime_progress(
                    runtime_state_db_path,
                    len(environment.team_config_list),
                    last_status,
                    expected_stage_count=environment.expected_stage_count,
                    expected_subject_count=environment.expected_subject_count,
                ),
                progress_ratio_override=_resolve_heavy_match_progress_ratio(
                    runtime_state_db_path,
                    expected_stage_count=environment.expected_stage_count,
                ),
            )
            last_emit_monotonic = current_monotonic
        time.sleep(2.0)
    failure_detail = _build_heavy_timeout_failure_detail(environment, timeout_seconds, last_status)
    _emit_heavy_console_text("[heavy_error] phase=waiting_match_finish summary=heavy full-chain timeout")
    _emit_heavy_console_text(f"[heavy_error_detail]\n{failure_detail}")
    raise AssertionError(failure_detail)


def validate_heavy_python_runtime(python_executable: str) -> None:
    env = build_subprocess_env()
    probe_code = (
        "import asyncio, socket; "
        "sock = socket.socket(); sock.close(); "
        "import yaml, grpc, numpy, injector; "
        "print('heavy_python_runtime_ok')"
    )
    result = subprocess.run(
        [python_executable, "-c", probe_code],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        env=env,
        check=False,
    )
    if result.returncode == 0:
        return
    detail = "\n".join(
        text.strip()
        for text in (result.stdout, result.stderr)
        if str(text or "").strip() != ""
    )
    _emit_heavy_console_text(
        "[heavy_warning] python preflight probe failed but heavy will continue with the selected runtime: "
        f"python={python_executable} detail={detail}"
    )


def read_launcher_manifest(environment: HeavyEnvironment) -> dict:
    manifest_path = environment.results_root / "control" / "launcher_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def read_process_manifest(environment: HeavyEnvironment) -> dict:
    manifest_path = environment.results_root / "control" / "judge_process_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _is_path_within_root(candidate_path: Path, root_path: Path) -> bool:
    try:
        candidate_path.resolve().relative_to(root_path.resolve())
    except (OSError, RuntimeError, ValueError):
        return False
    return True


def _resolve_manifest_process(
    process_record: dict,
    workspace_root: Path,
) -> tuple[psutil.Process | None, str]:
    try:
        process_id = int(process_record["pid"])
        recorded_started_at = float(process_record["started_at"])
        recorded_cwd = Path(str(process_record["cwd"]))
    except (KeyError, TypeError, ValueError):
        return None, "invalid_record"
    if process_id <= 0 or not _is_path_within_root(recorded_cwd, workspace_root):
        return None, "invalid_record"

    try:
        process = psutil.Process(process_id)
        live_started_at = float(process.create_time())
        live_cwd = Path(process.cwd())
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return None, "already_exited"
    except (psutil.AccessDenied, OSError):
        return None, "identity_access_denied"

    if abs(live_started_at - recorded_started_at) > HEAVY_MANIFEST_PROCESS_START_TIME_TOLERANCE_SECONDS:
        return None, "pid_reused"
    if not _is_path_within_root(live_cwd, workspace_root):
        return None, "cwd_mismatch"
    return process, "matched"


def _terminate_psutil_process_trees(root_process_list: list[psutil.Process]) -> list[int]:
    unique_process_by_id: dict[int, psutil.Process] = {}
    for root_process in root_process_list:
        try:
            child_process_list = root_process.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        except psutil.AccessDenied:
            child_process_list = []
        for candidate in child_process_list + [root_process]:
            unique_process_by_id[candidate.pid] = candidate

    wait_candidate_list: list[psutil.Process] = []
    blocked_process_id_list: list[int] = []
    for candidate in unique_process_by_id.values():
        try:
            candidate.terminate()
            wait_candidate_list.append(candidate)
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        except psutil.AccessDenied:
            blocked_process_id_list.append(candidate.pid)

    _, alive_process_list = psutil.wait_procs(
        wait_candidate_list,
        timeout=HEAVY_MANIFEST_PROCESS_TERMINATE_TIMEOUT_SECONDS,
    )
    kill_candidate_list: list[psutil.Process] = []
    for candidate in alive_process_list:
        try:
            candidate.kill()
            kill_candidate_list.append(candidate)
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        except psutil.AccessDenied:
            blocked_process_id_list.append(candidate.pid)
    _, surviving_process_list = psutil.wait_procs(kill_candidate_list, timeout=5.0)
    return sorted(
        set(blocked_process_id_list + [candidate.pid for candidate in surviving_process_list])
    )


def _terminate_manifest_judge_processes(
    environment: HeavyEnvironment,
    progress: HeavyProgressReporter,
) -> None:
    manifest_path = environment.results_root / "control" / "judge_process_manifest.json"
    if not manifest_path.exists():
        return
    try:
        manifest_payload = read_process_manifest(environment)
    except (OSError, json.JSONDecodeError) as exception:
        _emit_heavy_console_text(
            f"[heavy_warning] failed to read judge process manifest: "
            f"path={manifest_path} error={exception}"
        )
        return

    process_record_list = manifest_payload.get("processes")
    if not isinstance(process_record_list, list):
        _emit_heavy_console_text(
            f"[heavy_warning] invalid judge process manifest: path={manifest_path}"
        )
        return

    matched_count = 0
    already_exited_count = 0
    skipped_count = 0
    matched_process_list: list[psutil.Process] = []
    for process_record in reversed(process_record_list):
        if not isinstance(process_record, dict):
            skipped_count += 1
            continue
        process, resolution = _resolve_manifest_process(
            process_record,
            environment.workspace_root,
        )
        if resolution == "already_exited":
            already_exited_count += 1
            continue
        if process is None:
            skipped_count += 1
            continue
        matched_count += 1
        matched_process_list.append(process)

    survivor_process_id_list = _terminate_psutil_process_trees(matched_process_list)

    progress.emit(
        6,
        "cleanup_manifest_processes",
        (
            f"matched={matched_count} already_exited={already_exited_count} "
            f"skipped={skipped_count} survivors={sorted(set(survivor_process_id_list))}"
        ),
    )
    if survivor_process_id_list:
        _emit_heavy_console_text(
            "[heavy_warning] judge process cleanup left verified processes alive: "
            f"pids={sorted(set(survivor_process_id_list))}"
        )


def shutdown_heavy_environment(environment: HeavyEnvironment) -> None:
    progress = HeavyProgressReporter.from_environment(environment)
    progress.emit(
        6,
        "shutdown",
        (
            f"terminating judge stack and algorithm processes "
            f"artifacts={environment.artifact_root}"
        ),
    )
    try:
        try:
            for process in reversed(environment.judge_processes):
                terminate_tree(process, timeout=10.0)
        finally:
            try:
                _terminate_manifest_judge_processes(environment, progress)
            finally:
                for process in reversed(environment.algorithm_processes):
                    terminate_tree(process, timeout=10.0)
    finally:
        _cleanup_heavy_generated_receiver_data(environment, progress)


def preserve_heavy_failure_artifacts(
    environment: HeavyEnvironment,
    test_label: str,
    failed_root: Path | None = None,
) -> Path | None:
    source_root = Path(environment.artifact_root)
    if not source_root.exists():
        return None
    resolved_failed_root = (
        Path(failed_root)
        if failed_root is not None
        else PROJECT_ROOT / "tests" / "artifacts" / "latest" / "failed_test_artifacts"
    )
    safe_label = re.sub(r"[^A-Za-z0-9._-]+", "_", str(test_label or "heavy_failure")).strip("._")
    if not safe_label:
        safe_label = "heavy_failure"
    destination_root = resolved_failed_root / safe_label
    try:
        destination_root.parent.mkdir(parents=True, exist_ok=True)
        if destination_root.exists():
            shutil.rmtree(destination_root, ignore_errors=True)
        shutil.copytree(
            source_root,
            destination_root,
            ignore=_ignore_kafka_runtime_lock,
        )
    except OSError as exc:
        _emit_heavy_console_text(
            "[heavy_warning] failed to preserve heavy failure artifacts: "
            f"source={source_root} destination={destination_root} error={exc!r}"
        )
        return None
    _emit_heavy_console_text(
        "[heavy_artifact] preserved_failure_artifacts="
        f"{destination_root} source={source_root}"
    )
    return destination_root


def _ignore_kafka_runtime_lock(directory_path: str, entry_name_list: list[str]) -> set[str]:
    directory = Path(directory_path)
    if directory.parent.name.lower() != "kafka" or ".lock" not in entry_name_list:
        return set()
    return {".lock"}


def shutdown_and_preserve_heavy_failure_artifacts(
    environment: HeavyEnvironment,
    test_label: str,
    failed_root: Path | None = None,
) -> Path | None:
    try:
        shutdown_heavy_environment(environment)
    except Exception as exc:
        _emit_heavy_console_text(
            "[heavy_warning] failed to fully stop heavy environment before preserving artifacts: "
            f"error={exc!r}"
        )
    return preserve_heavy_failure_artifacts(
        environment,
        test_label=test_label,
        failed_root=failed_root,
    )


def assert_heavy_completion_outputs(
    environment: HeavyEnvironment,
    expected_team_count: int = HEAVY_DEFAULT_TEAM_COUNT,
) -> None:
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    match_control_status = read_json_state(runtime_state_db_path, STATE_KEY_MATCH_CONTROL_STATUS) or {}
    team_score_row_list = load_team_score_overview_rows(runtime_state_db_path)
    team_score_csv_path = environment.results_root / "00_team_score_overview.csv"
    expected_team_id_list = [str(team_config["team_id"]) for team_config in environment.team_config_list]

    assert runtime_state_db_path.exists(), f"runtime_state.db not found: {runtime_state_db_path}"
    assert bool(match_control_status.get("match_finished")) is True, match_control_status
    assert sorted(match_control_status.get("finished_team_id_list") or []) == sorted(expected_team_id_list), match_control_status
    assert team_score_csv_path.exists(), f"score overview csv not found: {team_score_csv_path}"
    assert len(team_score_row_list) == expected_team_count, team_score_row_list
    assert sorted(str(row.get("team_id") or "") for row in team_score_row_list) == sorted(expected_team_id_list)
    assert_scoreboard_sorted(team_score_row_list)
    for score_row in team_score_row_list:
        assert_required_fields(score_row, REQUIRED_SCOREBOARD_FIELDS)
    assert all(str(row.get("run_status") or "").lower() == "finished" for row in team_score_row_list), team_score_row_list
    _assert_heavy_csv_matches_runtime_scoreboard(team_score_csv_path, team_score_row_list)
    _assert_heavy_runtime_stage_state(runtime_state_db_path, expected_team_count)
    _assert_heavy_team_result_details(environment, runtime_state_db_path, expected_team_id_list)
    _assert_heavy_profile_observations(environment)


def _assert_heavy_csv_matches_runtime_scoreboard(team_score_csv_path: Path, team_score_row_list: list[dict]) -> None:
    csv_row_list = load_csv_rows(team_score_csv_path)
    assert len(csv_row_list) == len(team_score_row_list), csv_row_list
    csv_by_team_id = {str(row.get("team_id") or ""): row for row in csv_row_list}
    for runtime_row in team_score_row_list:
        team_id = str(runtime_row.get("team_id") or "")
        csv_row = csv_by_team_id.get(team_id)
        assert csv_row is not None, f"team missing from score csv: {team_id}"
        assert str(csv_row.get("run_status") or "").lower() == str(runtime_row.get("run_status") or "").lower()
        assert int(float(csv_row.get("observed_trial_count") or 0)) == int(float(runtime_row.get("observed_trial_count") or 0))


def _assert_heavy_runtime_stage_state(runtime_state_db_path: Path, expected_team_count: int) -> None:
    assert json_state_exists(runtime_state_db_path, STATE_KEY_RUNTIME_STAGE_STATUS), "runtime_stage_status json_state missing"
    runtime_stage_status = read_json_state(runtime_state_db_path, STATE_KEY_RUNTIME_STAGE_STATUS) or {}
    assert isinstance(runtime_stage_status.get("group_status_list") or [], list), runtime_stage_status
    assert count_json_state_by_prefix(runtime_state_db_path, TEAM_STATE_KEY_PREFIX) >= expected_team_count


def _assert_heavy_team_result_details(
    environment: HeavyEnvironment,
    runtime_state_db_path: Path,
    expected_team_id_list: list[str],
) -> None:
    for team_id in expected_team_id_list:
        trial_row_list = load_team_trial_record_rows(runtime_state_db_path, team_id)
        task_row_list = load_team_task_overview_rows(runtime_state_db_path, team_id)
        subject_task_row_list = load_team_subject_task_overview_rows(runtime_state_db_path, team_id)
        score_row = _find_score_row(runtime_state_db_path, team_id)
        scenario = environment.scenario_by_team_id.get(team_id)

        assert trial_row_list, f"heavy team has no trial rows: {team_id}"
        assert task_row_list, f"heavy team has no task overview rows: {team_id}"
        assert subject_task_row_list, f"heavy team has no subject-task overview rows: {team_id}"
        for trial_row in trial_row_list:
            assert_required_fields(trial_row, REQUIRED_TRIAL_FIELDS)
        assert int(float(score_row.get("observed_trial_count") or 0)) == len(trial_row_list), (
            team_id,
            score_row,
            len(trial_row_list),
        )
        assert _contains_task_family(task_row_list + subject_task_row_list + trial_row_list, "vme"), team_id
        assert _contains_task_family(task_row_list + subject_task_row_list + trial_row_list, "vmi"), team_id
        assert _trial_identity_count(trial_row_list) == len(trial_row_list), f"duplicate trial rows persisted for {team_id}"
        if scenario is not None:
            _assert_profile_result_expectation(team_id, scenario.algorithm_profile, trial_row_list)


def _assert_profile_result_expectation(team_id: str, profile_name: str, trial_row_list: list[dict]) -> None:
    normalized_profile = str(profile_name or "normal").strip().lower()
    if normalized_profile in {"slow", "late_result", "disconnect_stream", "resource_hog"}:
        assert any(_is_truthy(row.get("is_timeout")) for row in trial_row_list), (
            f"{team_id} profile={normalized_profile} did not produce a timeout trial",
            trial_row_list,
        )
    if normalized_profile in {"invalid_output", "malicious"}:
        assert any(_is_truthy(row.get("is_invalid_output")) for row in trial_row_list), (
            f"{team_id} profile={normalized_profile} did not produce an invalid-output trial",
            trial_row_list,
        )
    if normalized_profile == "duplicate_result":
        assert _trial_identity_count(trial_row_list) == len(trial_row_list), (
            f"{team_id} duplicate_result profile persisted duplicate trial rows",
            trial_row_list,
        )


def _assert_heavy_profile_observations(environment: HeavyEnvironment) -> None:
    observation_root = environment.results_root / "control" / "heavy_profile_observations"
    for team_id, scenario in environment.scenario_by_team_id.items():
        profile_name = scenario.algorithm_profile
        if profile_name == "normal":
            continue
        observation_path = observation_root / f"{team_id}.jsonl"
        assert observation_path.exists(), f"heavy profile observation missing: {team_id} profile={profile_name}"
        observation_list = [
            json.loads(line)
            for line in observation_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert any(item.get("profile") == profile_name for item in observation_list), observation_list
        if profile_name == "malicious":
            malicious_events = [item for item in observation_list if item.get("event") == "malicious_action"]
            assert malicious_events, observation_list
            assert all(item.get("status") == "blocked" for item in malicious_events), malicious_events
            assert {item.get("operation") for item in malicious_events} >= {
                "read_hidden_score",
                "write_results",
                "network_access",
                "kill_process",
            }


def _find_score_row(runtime_state_db_path: Path, team_id: str) -> dict:
    for row in load_team_score_overview_rows(runtime_state_db_path):
        if str(row.get("team_id") or "") == team_id:
            return row
    raise AssertionError(f"score row not found for {team_id}")


def _contains_task_family(row_list: list[dict], task_family: str) -> bool:
    needle = str(task_family).strip().lower()
    for row in row_list:
        values = [
            row.get("exp_name"),
            row.get("exp_task"),
            row.get("task_id"),
            row.get("started_task_names"),
        ]
        if any(needle in str(value or "").strip().lower() for value in values):
            return True
    return False


def _trial_identity_count(trial_row_list: list[dict]) -> int:
    return len(
        {
            (
                str(row.get("task_id") or ""),
                str(row.get("subject_id") or ""),
                str(row.get("session_id") or ""),
                str(row.get("trial_id") or ""),
            )
            for row in trial_row_list
        }
    )


def _is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


@dataclass
class HeavyProgressReporter:
    total_steps: int = 6
    estimated_seconds: float = HEAVY_MATCH_TIMEOUT_SECONDS
    started_monotonic: float = field(default_factory=time.monotonic)
    progress_output_path: Path | None = None

    @classmethod
    def from_environment(
        cls,
        environment: HeavyEnvironment,
        estimated_seconds: float = HEAVY_MATCH_TIMEOUT_SECONDS,
    ) -> "HeavyProgressReporter":
        return cls(
            total_steps=6,
            estimated_seconds=float(estimated_seconds),
            started_monotonic=float(environment.progress_start_monotonic),
            progress_output_path=environment.artifact_root.parent / "live_progress.json",
        )

    def emit(
        self,
        step: int,
        stage: str,
        detail: str,
        progress_ratio_override: float | None = None,
    ) -> None:
        elapsed_seconds = max(0.0, time.monotonic() - self.started_monotonic)
        progress_ratio = (
            float(progress_ratio_override)
            if progress_ratio_override is not None
            else float(step) / float(max(self.total_steps, 1))
        )
        progress_ratio = min(1.0, max(0.01, progress_ratio))
        base_percent = min(100.0, max(0.0, progress_ratio * 100.0))
        projected_total_seconds = max(float(self.estimated_seconds), elapsed_seconds / progress_ratio)
        remaining_seconds = max(0.0, projected_total_seconds - elapsed_seconds)
        print(
            "[heavy_progress] "
            f"step={step}/{self.total_steps} "
            f"percent={base_percent:.0f}% "
            f"elapsed={_format_duration(elapsed_seconds)} "
            f"eta={_format_duration(remaining_seconds)} "
            f"stage={stage} "
            f"detail={detail}",
            flush=True,
        )
        if self.progress_output_path is not None:
            self.progress_output_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.progress_output_path.with_name(f".{self.progress_output_path.name}.tmp")
            payload_text = json.dumps(
                {
                    "step": int(step),
                    "total_steps": int(self.total_steps),
                    "percent": round(base_percent),
                    "elapsed": _format_duration(elapsed_seconds),
                    "eta": _format_duration(remaining_seconds),
                    "stage": stage,
                    "detail": detail,
                    "updated_at": time.time(),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            last_permission_error: PermissionError | None = None
            for attempt_index in range(HEAVY_PROGRESS_REPLACE_RETRY_COUNT):
                temp_path.write_text(payload_text, encoding="utf-8")
                try:
                    os.replace(temp_path, self.progress_output_path)
                    break
                except PermissionError as exc:
                    last_permission_error = exc
                    time.sleep(HEAVY_PROGRESS_REPLACE_RETRY_DELAY_SECONDS * float(attempt_index + 1))
            else:
                fallback_path = self.progress_output_path.with_name(
                    f"{self.progress_output_path.stem}.{int(time.time() * 1000)}.json"
                )
                fallback_path.write_text(payload_text, encoding="utf-8")
                if last_permission_error is not None:
                    raise last_permission_error


def _summarize_heavy_runtime_progress(
    runtime_state_db_path: Path,
    expected_team_count: int,
    match_control_status: dict | None,
    expected_stage_count: int = 0,
    expected_subject_count: int = 0,
) -> str:
    if not runtime_state_db_path.exists():
        control_root = runtime_state_db_path.parent / "control"
        launcher_manifest_exists = (control_root / "launcher_manifest.json").exists()
        process_manifest_exists = (control_root / "judge_process_manifest.json").exists()
        headless_log_count = len(list((control_root / "headless_logs").glob("*.log"))) if (control_root / "headless_logs").exists() else 0
        return (
            "runtime_state.db not created yet; "
            f"runtime_state_path={runtime_state_db_path} "
            f"launcher_manifest={launcher_manifest_exists} "
            f"process_manifest={process_manifest_exists} "
            f"headless_logs={headless_log_count}. "
            "In the formal judge chain, RuntimeStageCoordinator should create runtime_state.db shortly after startup. "
            f"If this repeats for more than {int(HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS)} seconds after JudgeWeb is ready, "
            "inspect results/control/headless_logs and judge_stack_logs."
        )
    team_score_row_list = load_team_score_overview_rows(runtime_state_db_path)
    finished_count = sum(
        1
        for row in team_score_row_list
        if str(row.get("run_status") or "").strip().lower() == "finished"
    )
    observed_trial_count = sum(int(float(row.get("observed_trial_count") or 0)) for row in team_score_row_list)
    runtime_stage_exists = json_state_exists(runtime_state_db_path, STATE_KEY_RUNTIME_STAGE_STATUS)
    finished_team_id_list = (match_control_status or {}).get("finished_team_id_list") or []
    current_trial = read_json_state(runtime_state_db_path, "current_trial") or {}
    runtime_stage_status = read_json_state(runtime_state_db_path, STATE_KEY_RUNTIME_STAGE_STATUS) or {}
    stage_progress = _summarize_runtime_stage_progress(runtime_stage_status)
    return (
        f"teams_finished={finished_count}/{expected_team_count} "
        f"finished_team_ids={len(finished_team_id_list)}/{expected_team_count} "
        f"observed_trials={observed_trial_count} "
        f"expected_subjects={expected_subject_count or '<unknown>'} "
        f"expected_stages={expected_stage_count or '<unknown>'} "
        f"runtime_stage_status={runtime_stage_exists} "
        f"{stage_progress} "
        f"current_trial_subject={current_trial.get('subject_id')} "
        f"current_trial_exp={current_trial.get('exp_name')} "
        f"current_trial_task={current_trial.get('exp_task')} "
        f"current_trial_session={current_trial.get('session_id')} "
        f"current_trial_id={current_trial.get('trial_id')} "
        f"current_trial_status={current_trial.get('status')}"
    )


def _summarize_runtime_stage_progress(runtime_stage_status: dict | None) -> str:
    progress_snapshot = _extract_runtime_stage_progress(runtime_stage_status)
    if progress_snapshot["total_stage_count"] == 0:
        return "stages=0/0"
    active_stage_context = progress_snapshot["active_stage_context"]
    return (
        f"stages={progress_snapshot['completed_stage_count']}/{progress_snapshot['total_stage_count']} "
        f"active_stage={active_stage_context.get('subject_id')}/"
        f"{active_stage_context.get('exp_name')}/"
        f"{active_stage_context.get('exp_task')}/"
        f"{active_stage_context.get('session_id')} "
        f"stage_trials={progress_snapshot['active_completed_trial_count']}/{progress_snapshot['active_online_trial_count']}"
    )


def _extract_runtime_stage_progress(runtime_stage_status: dict | None) -> dict[str, object]:
    payload = runtime_stage_status or {}
    group_status_list = payload.get("group_status_list") or []
    stage_status_list: list[dict] = []
    for group_status in group_status_list:
        if not isinstance(group_status, dict):
            continue
        stage_status_list.extend(
            [stage_status for stage_status in (group_status.get("stage_status_list") or []) if isinstance(stage_status, dict)]
        )
    if not stage_status_list:
        return {
            "total_stage_count": 0,
            "completed_stage_count": 0,
            "active_stage_context": {},
            "active_completed_trial_count": 0,
            "active_online_trial_count": 0,
            "stage_completion_ratio": 0.0,
        }
    completed_stage_count = 0
    active_stage_status: dict | None = None
    for stage_status in stage_status_list:
        online_trial_count = int(stage_status.get("online_trial_count") or 0)
        completed_trial_count = int(stage_status.get("completed_trial_count") or 0)
        if online_trial_count > 0 and completed_trial_count >= online_trial_count:
            completed_stage_count += 1
            continue
        if active_stage_status is None:
            active_stage_status = stage_status
    if active_stage_status is None:
        active_stage_status = stage_status_list[-1]
    active_online_trial_count = int(active_stage_status.get("online_trial_count") or 0)
    active_completed_trial_count = int(active_stage_status.get("completed_trial_count") or 0)
    active_stage_fraction = (
        min(1.0, max(0.0, active_completed_trial_count / max(active_online_trial_count, 1)))
        if active_online_trial_count > 0
        else 0.0
    )
    stage_completion_ratio = min(
        1.0,
        max(0.0, (completed_stage_count + active_stage_fraction) / max(len(stage_status_list), 1)),
    )
    return {
        "total_stage_count": len(stage_status_list),
        "completed_stage_count": completed_stage_count,
        "active_stage_context": active_stage_status.get("stage_context") or {},
        "active_completed_trial_count": active_completed_trial_count,
        "active_online_trial_count": active_online_trial_count,
        "stage_completion_ratio": stage_completion_ratio,
    }


def _resolve_heavy_match_progress_ratio(
    runtime_state_db_path: Path,
    expected_stage_count: int = 0,
) -> float:
    if not runtime_state_db_path.exists():
        return 0.30
    runtime_stage_status = read_json_state(runtime_state_db_path, STATE_KEY_RUNTIME_STAGE_STATUS) or {}
    progress_snapshot = _extract_runtime_stage_progress(runtime_stage_status)
    stage_completion_ratio = float(progress_snapshot["stage_completion_ratio"] or 0.0)
    if expected_stage_count > 0:
        completed_stage_count = int(progress_snapshot["completed_stage_count"] or 0)
        active_online_trial_count = int(progress_snapshot["active_online_trial_count"] or 0)
        active_completed_trial_count = int(progress_snapshot["active_completed_trial_count"] or 0)
        active_stage_fraction = (
            min(1.0, max(0.0, active_completed_trial_count / max(active_online_trial_count, 1)))
            if active_online_trial_count > 0
            else 0.0
        )
        stage_completion_ratio = min(
            1.0,
            max(0.0, (completed_stage_count + active_stage_fraction) / max(int(expected_stage_count), 1)),
        )
    return min(0.96, max(0.30, 0.30 + stage_completion_ratio * 0.65))


def _count_expected_heavy_stage_count(dataset_spec: dict | None) -> int:
    data_files = (dataset_spec or {}).get("data_files") or {}
    stage_count = 0
    for subject_payload in data_files.values():
        for paradigm_entry_list in dict(subject_payload or {}).values():
            stage_count += len(list(paradigm_entry_list or []))
    return stage_count


def _estimate_heavy_match_seconds_from_environment(environment: HeavyEnvironment) -> float:
    stage_count = max(1, int(environment.expected_stage_count or 0))
    return max(
        HEAVY_MATCH_TIMEOUT_SECONDS,
        HEAVY_PREPARE_ESTIMATED_SECONDS + stage_count * HEAVY_ESTIMATED_STAGE_SECONDS,
    )


def _resolve_waiting_start_readiness_progress_ratio(fetch_payload: dict | None) -> float:
    readiness = (fetch_payload or {}).get("start_readiness") or {}
    configured_team_count = len(readiness.get("configured_team_id_list") or [])
    connected_team_count = len(readiness.get("connected_team_id_list") or [])
    group_readiness_list = readiness.get("group_readiness_list") or []
    pending_group_id_list = readiness.get("pending_group_id_list") or []
    connection_ratio = (
        min(1.0, max(0.0, connected_team_count / max(configured_team_count, 1)))
        if configured_team_count > 0
        else 0.0
    )
    if group_readiness_list:
        ready_group_count = sum(
            1 for group_readiness in group_readiness_list if bool((group_readiness or {}).get("collector_ready"))
        )
        group_ratio = min(1.0, max(0.0, ready_group_count / max(len(group_readiness_list), 1)))
    elif pending_group_id_list:
        group_ratio = 0.0
    else:
        group_ratio = 1.0 if connection_ratio >= 1.0 else 0.0
    readiness_ratio = min(1.0, max(0.0, connection_ratio * 0.75 + group_ratio * 0.25))
    return min(0.279, max(0.251, 0.25 + readiness_ratio * 0.029))


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    minutes, second = divmod(total_seconds, 60)
    hour, minute = divmod(minutes, 60)
    if hour > 0:
        return f"{hour:02d}:{minute:02d}:{second:02d}"
    return f"{minute:02d}:{second:02d}"


def _build_missing_runtime_state_failure_detail(environment: HeavyEnvironment) -> str:
    control_root = environment.results_root / "control"
    launcher_manifest_path = control_root / "launcher_manifest.json"
    process_manifest_path = control_root / "judge_process_manifest.json"
    headless_log_root = control_root / "headless_logs"
    runtime_state_db_path = environment.results_root / "runtime_state.db"
    detail_part_list = [
        (
            "heavy failed before runtime_state.db creation for more than "
            f"{int(HEAVY_MISSING_RUNTIME_STATE_GRACE_SECONDS)}s after judge stack startup."
        ),
        f"workspace_root={environment.workspace_root}",
        f"artifact_root={environment.artifact_root}",
        f"runtime_state_db_path={runtime_state_db_path}",
        f"runtime_state_db_exists={runtime_state_db_path.exists()}",
        f"launcher_manifest_exists={launcher_manifest_path.exists()}",
        f"process_manifest_exists={process_manifest_path.exists()}",
        f"headless_log_root_exists={headless_log_root.exists()}",
    ]
    if runtime_state_db_path.exists():
        detail_part_list.extend(_summarize_runtime_state_bootstrap_state(runtime_state_db_path))
    detail_part_list.extend(_collect_representative_headless_log_tails(headless_log_root))
    return "\n".join(detail_part_list)


def _build_heavy_timeout_failure_detail(
    environment: HeavyEnvironment,
    timeout_seconds: float,
    last_status: dict | None,
) -> str:
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    detail_part_list = [
        f"heavy full-chain match did not finish within {timeout_seconds:.1f}s",
        f"workspace_root={environment.workspace_root}",
        f"artifact_root={environment.artifact_root}",
        f"runtime_state_db_path={runtime_state_db_path}",
        f"last_match_control_status={last_status or {}}",
        (
            "progress_snapshot="
            + _summarize_heavy_runtime_progress(
                runtime_state_db_path,
                len(environment.team_config_list),
                last_status,
                expected_stage_count=environment.expected_stage_count,
                expected_subject_count=environment.expected_subject_count,
            )
        ),
    ]
    if runtime_state_db_path.exists():
        detail_part_list.extend(_summarize_runtime_state_bootstrap_state(runtime_state_db_path))
        detail_part_list.extend(_summarize_team_state_snapshot(runtime_state_db_path))
    detail_part_list.extend(
        _collect_representative_headless_log_tails(environment.results_root / "control" / "headless_logs")
    )
    return "\n".join(detail_part_list)


def _raise_on_heavy_runtime_failure_detected(environment: HeavyEnvironment, phase: str) -> None:
    failure_detail = _detect_heavy_runtime_failure(environment)
    if failure_detail is None:
        return
    _emit_heavy_console_text(f"[heavy_error] phase={phase} summary={failure_detail.splitlines()[0]}")
    _emit_heavy_console_text(f"[heavy_error_detail]\n{failure_detail}")
    raise AssertionError(failure_detail)


def _detect_heavy_runtime_failure(environment: HeavyEnvironment) -> str | None:
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    detail_part_list = _collect_heavy_runtime_failure_detail_parts(environment, runtime_state_db_path)
    if not detail_part_list:
        return None
    return "\n".join(
        [
            "heavy runtime failure detected before full-chain completion.",
            f"workspace_root={environment.workspace_root}",
            f"artifact_root={environment.artifact_root}",
            f"runtime_state_db_path={runtime_state_db_path}",
            *detail_part_list,
        ]
    )


def _collect_heavy_runtime_failure_detail_parts(
    environment: HeavyEnvironment,
    runtime_state_db_path: Path,
) -> list[str]:
    detail_part_list: list[str] = []
    detail_part_list.extend(_collect_heavy_current_trial_failure_parts(runtime_state_db_path))
    detail_part_list.extend(_collect_heavy_team_state_failure_parts(runtime_state_db_path))
    detail_part_list.extend(_collect_heavy_algorithm_process_exit_parts(environment))
    detail_part_list.extend(
        _collect_heavy_log_pattern_hits(
            environment.results_root / "control" / "headless_logs",
            pattern_spec_list=(
                ("processhub_exception_package", "ExceptionPackageModel("),
                ("team_run_finalized_error", "terminal_run_status=error"),
                ("team_run_startup_failed", "terminal_run_status=startup_failed"),
                ("runtime_stage_startup_failed", "startup_failed"),
                ("central_controller_concurrent_dict_iteration", "dictionary changed size during iteration"),
                (
                    "algorithm_disconnected_before_task_finished",
                    "algorithm_data_connection_closed_before_task_finished",
                ),
                ("algorithm_connector_closed_during_send", "algorithm_connector_closed_during_send"),
            ),
            glob_pattern="*.log",
        )
    )
    detail_part_list.extend(
        _collect_heavy_log_pattern_hits(
            environment.artifact_root / "algorithm_logs",
            pattern_spec_list=(
                ("predict_worker_timeout", "PredictWorkerTimeoutError"),
                ("predict_worker_timeout_message", "predict worker timed out after"),
                ("algorithm_runtime_exception", "[ERROR]算法执行发生异常"),
                ("algorithm_runtime_exception", "算法执行发生异常"),
                ("fortran_window_close", "forrtl: error (200)"),
            ),
            glob_pattern="*.log",
        )
    )
    return detail_part_list


def _collect_heavy_current_trial_failure_parts(runtime_state_db_path: Path) -> list[str]:
    current_trial = read_json_state(runtime_state_db_path, STATE_KEY_CURRENT_TRIAL) or {}
    status = str(current_trial.get("status") or "").strip().lower()
    if status not in {"error", "failed"}:
        return []
    stage_text = "/".join(
        str(current_trial.get(field_name) or "<unknown>")
        for field_name in ("subject_id", "exp_name", "exp_task", "session_id")
    )
    return [
        (
            "collector_stage_distribution_failure "
            f"status={status} "
            f"stage={stage_text} "
            f"error_type={current_trial.get('error_type') or '<unknown>'} "
            f"error_message={current_trial.get('error_message') or '<empty>'} "
            f"recovery_advice={current_trial.get('recovery_advice') or '<none>'}"
        )
    ]


def _collect_heavy_team_state_failure_parts(runtime_state_db_path: Path) -> list[str]:
    if not runtime_state_db_path.exists():
        return []
    team_state_list = list_json_state_by_prefix(runtime_state_db_path, TEAM_STATE_KEY_PREFIX)
    failed_team_state_list = [
        team_state
        for team_state in team_state_list
        if str(team_state.get("run_status") or "").strip().lower() in {"error", "startup_failed"}
        or str(team_state.get("connection_status") or "").strip().lower() == "error"
    ]
    if not failed_team_state_list:
        return []
    summarized_team_state_list = [
        {
            "team_id": team_state.get("team_id"),
            "run_status": team_state.get("run_status"),
            "connection_status": team_state.get("connection_status"),
            "calibration_ready": team_state.get("calibration_ready"),
            "last_disconnect_reason": team_state.get("last_disconnect_reason"),
            "last_error_type": team_state.get("last_error_type"),
            "last_error_message": team_state.get("last_error_message"),
        }
        for team_state in failed_team_state_list[:5]
    ]
    return [
        f"team_state_failure_count={len(failed_team_state_list)}",
        f"team_state_failure_sample={summarized_team_state_list}",
    ]


def _collect_heavy_algorithm_process_exit_parts(environment: HeavyEnvironment) -> list[str]:
    early_exit_part_list: list[str] = []
    for managed_process in environment.algorithm_processes:
        return_code = managed_process.process.poll()
        if return_code is None:
            continue
        early_exit_part_list.append(
            {
                "name": managed_process.name,
                "return_code": return_code,
                "stdout_path": str(managed_process.stdout_path),
                "stderr_path": str(managed_process.stderr_path),
            }
        )
    if not early_exit_part_list:
        return []
    return [f"algorithm_process_early_exit={early_exit_part_list[:5]}"]


def _collect_heavy_log_pattern_hits(
    log_root: Path,
    pattern_spec_list: tuple[tuple[str, str], ...],
    glob_pattern: str,
    max_hit_count: int = 8,
    tail_line_count: int = 160,
) -> list[str]:
    if not log_root.exists():
        return []
    hit_part_list: list[str] = []
    for log_path in sorted(log_root.glob(glob_pattern)):
        try:
            line_list = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError as exc:
            hit_part_list.append(f"log_read_error file={log_path} error={exc}")
            if len(hit_part_list) >= max_hit_count:
                break
            continue
        tail_line_list = line_list[-tail_line_count:]
        for label, pattern_text in pattern_spec_list:
            matched_line = next((line.strip() for line in reversed(tail_line_list) if pattern_text in line), None)
            if matched_line is None:
                continue
            hit_part_list.append(
                f"log_hit label={label} file={log_path} pattern={pattern_text} matched_line={matched_line}"
            )
            break
        if len(hit_part_list) >= max_hit_count:
            break
    if not hit_part_list:
        return []
    return hit_part_list


def _emit_heavy_console_text(text: str) -> None:
    message = str(text or "")
    try:
        os.write(2, (message + "\n").encode("ascii", "backslashreplace"))
        return
    except OSError:
        pass
    try:
        print(message.encode("ascii", "backslashreplace").decode("ascii"), file=sys.stderr, flush=True)
    except Exception:
        print("[heavy_error] failed to write console detail safely", file=sys.stderr, flush=True)


def _tail_text(file_path: Path, line_count: int = 20) -> str:
    try:
        line_list = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError as exc:
        return f"<failed to read {file_path.name}: {exc}>"
    if not line_list:
        return "<empty>"
    return "\n".join(line_list[-line_count:])


def _summarize_runtime_state_bootstrap_state(runtime_state_db_path: Path) -> list[str]:
    try:
        with sqlite3.connect(runtime_state_db_path, timeout=5.0) as connection:
            json_state_key_list = [
                str(row[0])
                for row in connection.execute(
                    "SELECT state_key FROM json_state ORDER BY state_key ASC LIMIT 20"
                ).fetchall()
            ]
            json_state_count = int(
                connection.execute("SELECT COUNT(1) FROM json_state").fetchone()[0]
            )
            team_score_count = int(
                connection.execute("SELECT COUNT(1) FROM team_score_overview").fetchone()[0]
            )
            team_overview_count = int(
                connection.execute("SELECT COUNT(1) FROM team_overview").fetchone()[0]
            )
            task_overview_count = int(
                connection.execute("SELECT COUNT(1) FROM task_overview").fetchone()[0]
            )
            subject_task_overview_count = int(
                connection.execute("SELECT COUNT(1) FROM subject_task_overview").fetchone()[0]
            )
            trial_record_count = int(
                connection.execute("SELECT COUNT(1) FROM trial_record").fetchone()[0]
            )
    except sqlite3.Error as exc:
        return [f"runtime_state_db_inspection_error={exc!r}"]
    return [
        f"runtime_state_json_state_count={json_state_count}",
        f"runtime_state_json_state_keys={json_state_key_list}",
        f"runtime_state_team_score_count={team_score_count}",
        f"runtime_state_team_overview_count={team_overview_count}",
        f"runtime_state_task_overview_count={task_overview_count}",
        f"runtime_state_subject_task_overview_count={subject_task_overview_count}",
        f"runtime_state_trial_record_count={trial_record_count}",
    ]


def _fetch_local_json(
    url: str,
    timeout_seconds: float = 10.0,
    method: str = "GET",
    json_payload: dict | None = None,
) -> LocalJsonFetchResult:
    normalized_method = str(method or "GET").strip().upper()
    body_text = json.dumps(json_payload or {}, ensure_ascii=False) if json_payload is not None else None
    urllib_error: str | None = None
    request: str | urllib.request.Request
    if normalized_method == "GET" and body_text is None:
        request = url
    else:
        request = urllib.request.Request(
            url,
            data=(body_text or "").encode("utf-8"),
            method=normalized_method,
        )
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return _build_json_fetch_result(
                raw_text=response.read().decode("utf-8", errors="ignore"),
                source="urllib",
                status_code=int(getattr(response, "status", 200)),
            )
    except urllib.error.HTTPError as exc:
        urllib_error = (
            f"HTTPError status={exc.code} detail="
            f"{exc.read().decode('utf-8', errors='ignore') or str(exc.reason or exc)}"
        )
    except (urllib.error.URLError, OSError, TimeoutError, ValueError) as exc:
        urllib_error = f"{type(exc).__name__}: {exc}"
    powershell_result = _fetch_json_via_powershell(
        url=url,
        timeout_seconds=timeout_seconds,
        method=normalized_method,
        body_text=body_text,
    )
    if powershell_result.payload is not None:
        return powershell_result
    combined_error = "; ".join(
        part
        for part in (
            f"urllib={urllib_error}" if urllib_error else None,
            f"powershell={powershell_result.error}" if powershell_result.error else None,
        )
        if part
    )
    return LocalJsonFetchResult(
        payload=None,
        error=combined_error or "both_local_http_strategies_failed",
        source="unavailable",
        status_code=powershell_result.status_code,
    )


def _fetch_json_via_powershell(
    url: str,
    timeout_seconds: float,
    method: str,
    body_text: str | None,
) -> LocalJsonFetchResult:
    escaped_url = _escape_powershell_single_quoted(url)
    escaped_method = _escape_powershell_single_quoted(method)
    escaped_body_text = _escape_powershell_single_quoted(body_text or "")
    command = (
        "$ProgressPreference='SilentlyContinue'; "
        "$ErrorActionPreference='Stop'; "
        "try { "
        f"$response = Invoke-WebRequest -UseBasicParsing -TimeoutSec {max(1, int(timeout_seconds))} "
        f"-Method '{escaped_method}' -Uri '{escaped_url}' "
        + (
            f"-ContentType 'application/json' -Body '{escaped_body_text}' "
            if body_text is not None
            else ""
        )
        + "; "
        "$wrapper = @{ ok = $true; status = [int]$response.StatusCode; content = [string]$response.Content } | ConvertTo-Json -Compress; "
        "Write-Output $wrapper; "
        "} catch { "
        "$statusCode = 0; "
        "if ($_.Exception.Response -and $_.Exception.Response.StatusCode) { $statusCode = [int]$_.Exception.Response.StatusCode }; "
        "$content = ''; "
        "if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $content = [string]$_.ErrorDetails.Message }; "
        "$wrapper = @{ ok = $false; status = $statusCode; message = [string]$_.Exception.Message; content = $content } | ConvertTo-Json -Compress; "
        "Write-Output $wrapper; "
        "}"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            env=build_subprocess_env(),
            timeout=max(5.0, float(timeout_seconds) + 2.0),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return LocalJsonFetchResult(
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            source="powershell",
            status_code=None,
        )
    wrapper_text = (result.stdout or "").strip()
    if wrapper_text == "":
        return LocalJsonFetchResult(
            payload=None,
            error=f"empty_stdout stderr={(result.stderr or '').strip()}",
            source="powershell",
            status_code=None,
        )
    try:
        wrapper_payload = json.loads(wrapper_text.splitlines()[-1])
    except json.JSONDecodeError as exc:
        return LocalJsonFetchResult(
            payload=None,
            error=f"wrapper_json_decode_error={exc}; stdout={wrapper_text}; stderr={(result.stderr or '').strip()}",
            source="powershell",
            status_code=None,
        )
    status_code = int(wrapper_payload.get("status") or 0) or None
    content_text = str(wrapper_payload.get("content") or "")
    if not bool(wrapper_payload.get("ok")):
        return LocalJsonFetchResult(
            payload=None,
            error=(
                f"status={status_code or 0} message={wrapper_payload.get('message') or ''} "
                f"detail={content_text}"
            ).strip(),
            source="powershell",
            status_code=status_code,
        )
    return _build_json_fetch_result(raw_text=content_text, source="powershell", status_code=status_code)


def _build_json_fetch_result(raw_text: str, source: str, status_code: int | None) -> LocalJsonFetchResult:
    try:
        payload = json.loads(raw_text or "{}")
    except json.JSONDecodeError as exc:
        return LocalJsonFetchResult(
            payload=None,
            error=f"json_decode_error={exc}; raw_text={raw_text}",
            source=source,
            status_code=status_code,
        )
    if not isinstance(payload, dict):
        return LocalJsonFetchResult(
            payload=None,
            error=f"payload_is_not_dict type={type(payload).__name__} raw_text={raw_text}",
            source=source,
            status_code=status_code,
        )
    return LocalJsonFetchResult(payload=payload, error=None, source=source, status_code=status_code)


def _escape_powershell_single_quoted(value: str) -> str:
    return str(value or "").replace("'", "''")


def _format_local_json_fetch_detail(url: str, fetch_result: LocalJsonFetchResult) -> str:
    return (
        f"url={url} source={fetch_result.source} status={fetch_result.status_code} "
        f"error={fetch_result.error} payload={fetch_result.payload}"
    )


def _summarize_start_readiness_progress(
    environment: HeavyEnvironment,
    control_status_url: str,
    fetch_result: LocalJsonFetchResult,
) -> str:
    readiness = (fetch_result.payload or {}).get("start_readiness") or {}
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    match_control_status = read_json_state(runtime_state_db_path, STATE_KEY_MATCH_CONTROL_STATUS) or {}
    runtime_stage_status = read_json_state(runtime_state_db_path, STATE_KEY_RUNTIME_STAGE_STATUS) or {}
    team_state_list = list_json_state_by_prefix(runtime_state_db_path, TEAM_STATE_KEY_PREFIX)
    disconnected_team_state_list = [
        team_state for team_state in team_state_list if str(team_state.get("connection_status") or "").lower() != "connected"
    ]
    disconnected_team_id_list = [
        str(team_state.get("team_id") or "")
        for team_state in disconnected_team_state_list
        if str(team_state.get("team_id") or "").strip()
    ]
    disconnect_reason_list = [
        str(team_state.get("last_disconnect_reason") or "").strip()
        for team_state in disconnected_team_state_list
        if str(team_state.get("last_disconnect_reason") or "").strip()
    ]
    stage_status_count = sum(
        len(group_status.get("stage_status_list") or [])
        for group_status in (runtime_stage_status.get("group_status_list") or [])
        if isinstance(group_status, dict)
    )
    return (
        f"url={control_status_url} "
        f"http_source={fetch_result.source} "
        f"http_status={fetch_result.status_code} "
        f"http_error={fetch_result.error or '<none>'} "
        f"ready={bool(readiness.get('ready'))} "
        f"connected={len(readiness.get('connected_team_id_list') or [])}/{len(readiness.get('configured_team_id_list') or environment.team_config_list)} "
        f"pending_groups={readiness.get('pending_group_id_list') or []} "
        f"pending_teams={readiness.get('pending_team_id_list') or []} "
        f"match_started={bool(match_control_status.get('match_started'))} "
        f"match_finished={bool(match_control_status.get('match_finished'))} "
        f"runtime_stage_groups={len(runtime_stage_status.get('group_status_list') or [])} "
        f"runtime_stage_stage_count={stage_status_count} "
        f"team_state_count={len(team_state_list)} "
        f"disconnected_sample={disconnected_team_id_list[:5]} "
        f"disconnect_reason_sample={disconnect_reason_list[:3]}"
    )


def _build_formal_start_readiness_failure_detail(
    environment: HeavyEnvironment,
    control_status_url: str,
    fetch_result: LocalJsonFetchResult,
) -> str:
    runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
    detail_part_list = [
        _format_local_json_fetch_detail(control_status_url, fetch_result),
        _summarize_start_readiness_progress(environment, control_status_url, fetch_result),
    ]
    if runtime_state_db_path.exists():
        detail_part_list.extend(_summarize_runtime_state_bootstrap_state(runtime_state_db_path))
        detail_part_list.extend(_summarize_team_state_snapshot(runtime_state_db_path))
    detail_part_list.extend(
        _collect_representative_headless_log_tails(environment.results_root / "control" / "headless_logs")
    )
    return "\n".join(detail_part_list)


def _summarize_team_state_snapshot(runtime_state_db_path: Path, max_team_count: int = 5) -> list[str]:
    team_state_list = list_json_state_by_prefix(runtime_state_db_path, TEAM_STATE_KEY_PREFIX)
    if not team_state_list:
        return ["team_state_snapshot=[]"]
    summarized_team_state_list = []
    for team_state in team_state_list[:max_team_count]:
        summarized_team_state_list.append(
            {
                "team_id": team_state.get("team_id"),
                "connection_status": team_state.get("connection_status"),
                "run_status": team_state.get("run_status"),
                "calibration_ready": team_state.get("calibration_ready"),
                "last_disconnect_reason": team_state.get("last_disconnect_reason"),
                "updated_at": team_state.get("updated_at"),
            }
        )
    disconnected_team_count = sum(
        1 for team_state in team_state_list if str(team_state.get("connection_status") or "").lower() != "connected"
    )
    return [
        f"team_state_count={len(team_state_list)}",
        f"team_state_disconnected_count={disconnected_team_count}",
        f"team_state_sample={summarized_team_state_list}",
    ]


def _collect_representative_headless_log_tails(headless_log_root: Path) -> list[str]:
    if not headless_log_root.exists():
        return []
    preferred_log_name_list = [
        "BCI_Judge__JudgeWeb.stdout.log",
        "BCI_Judge__JudgeWeb.stderr.log",
        "BCI_Judge__RuntimeStageCoordinator_Python.stdout.log",
        "BCI_Judge__RuntimeStageCoordinator_Python.stderr.log",
        "BCI_Judge__Collector_Python.stdout.log",
        "BCI_Judge__Collector_Python.stderr.log",
        "BCI_Judge__ProcessHub_team_0_group_1.stdout.log",
        "BCI_Judge__ProcessHub_team_0_group_1.stderr.log",
    ]
    detail_part_list: list[str] = []
    for log_name in preferred_log_name_list:
        log_path = headless_log_root / log_name
        if log_path.exists():
            detail_part_list.append(f"{log_path.name} tail:\n{_tail_text(log_path, line_count=20)}")
    return detail_part_list


def _build_heavy_dataset_spec(
    workspace_root: Path,
    team_count: int,
    source_receiver_data_root: Path | None = None,
    generated_file_path_list: list[Path] | None = None,
) -> dict:
    data_files: dict[str, dict[str, list[dict[str, str]]]] = {}
    receiver_data_root = _resolve_heavy_receiver_data_root(workspace_root)
    source_receiver_data_root = (
        Path(source_receiver_data_root)
        if source_receiver_data_root is not None
        else _resolve_repo_receiver_data_root()
    )
    source_sample_pair_by_subject = _collect_real_heavy_source_sample_pairs(source_receiver_data_root)
    source_subject_key_list = sorted(source_sample_pair_by_subject.keys())
    if not source_subject_key_list:
        raise FileNotFoundError(
            f"heavy source dataset not found under {source_receiver_data_root}; "
            "at least one subject directory with both vme/vmi .dat and _meta.txt pairs is required"
        )
    selected_subject_key_list = source_subject_key_list
    if HEAVY_DEFAULT_DATASET_SUBJECT_COUNT is not None:
        selected_subject_key_list = source_subject_key_list[: max(1, int(HEAVY_DEFAULT_DATASET_SUBJECT_COUNT))]
    for subject_key in selected_subject_key_list:
        subject_id = str(subject_key)
        vme_entry_list: list[dict[str, str]] = []
        vmi_entry_list: list[dict[str, str]] = []
        paradigm_pair_map = source_sample_pair_by_subject[subject_key]
        for paradigm_name, entry_list in (("vme", paradigm_pair_map["vme"]), ("vmi", paradigm_pair_map["vmi"])):
            for source_dat_path, source_meta_path in entry_list:
                session_id = _resolve_session_id_from_source_path(source_dat_path)
                target_dat_path = receiver_data_root / subject_id / session_id / source_dat_path.name
                target_meta_path = target_dat_path.with_name(f"{target_dat_path.stem}_meta.txt")
                target_dat_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_dat_path, target_dat_path)
                _copy_real_heavy_meta_file(source_meta_path, target_meta_path, target_data_file_name=target_dat_path.name)
                if generated_file_path_list is not None:
                    generated_file_path_list.extend([target_dat_path, target_meta_path])
                target_entry = {
                    "source_path": str(target_dat_path),
                    "yaml_path": (
                        f"Collector/receiver/virtual_receiver/data/{subject_id}/{session_id}/{target_dat_path.name}"
                    ),
                }
                if paradigm_name == "vme":
                    vme_entry_list.append(target_entry)
                else:
                    vmi_entry_list.append(target_entry)
        data_files[subject_id] = {
            "vme": vme_entry_list,
            "vmi": vmi_entry_list,
        }
    return {"data_files": data_files}


def _collect_real_heavy_source_sample_pairs(
    source_receiver_data_root: Path,
) -> dict[str, dict[str, list[tuple[Path, Path]]]]:
    source_sample_pair_by_subject: dict[str, dict[str, list[tuple[Path, Path]]]] = {}
    if not source_receiver_data_root.exists():
        return source_sample_pair_by_subject
    for subject_root in sorted(path for path in source_receiver_data_root.iterdir() if path.is_dir()):
        subject_key = subject_root.name
        if not subject_key.strip():
            continue
        vme_pair_list = _find_real_heavy_source_sample_pair_list(subject_root, "vme", preferred_session_name="session1")
        vmi_pair_list = _find_real_heavy_source_sample_pair_list(subject_root, "vmi", preferred_session_name="session2")
        if not vme_pair_list or not vmi_pair_list:
            continue
        source_sample_pair_by_subject[subject_key] = {
            "vme": vme_pair_list,
            "vmi": vmi_pair_list,
        }
    return source_sample_pair_by_subject


def _find_real_heavy_source_sample_pair_list(
    subject_root: Path,
    exp_name: str,
    preferred_session_name: str,
) -> list[tuple[Path, Path]]:
    exp_name_text = str(exp_name).strip().lower()
    preferred_session_name_text = str(preferred_session_name).strip().lower()
    session_name_order = [
        preferred_session_name_text,
        *[
            session_name
            for session_name in ("session1", "session2")
            if session_name != preferred_session_name_text
        ],
    ]
    pair_list: list[tuple[Path, Path]] = []
    for session_name in session_name_order:
        session_root = subject_root / session_name
        if not session_root.exists():
            continue
        for dat_path in sorted(session_root.glob(f"*_{exp_name_text}_run*.dat")):
            meta_path = dat_path.with_name(f"{dat_path.stem}_meta.txt")
            if meta_path.exists():
                pair_list.append((dat_path, meta_path))
    return pair_list


def _resolve_session_id_from_source_path(source_path: Path) -> str:
    for path_part in source_path.parts:
        path_part_text = str(path_part).strip()
        if path_part_text.lower().startswith("session"):
            return path_part_text
    return "session_unknown"


def _copy_real_heavy_meta_file(
    source_meta_path: Path,
    target_meta_path: Path,
    target_data_file_name: str,
) -> None:
    meta_text = source_meta_path.read_text(encoding="utf-8")
    updated_line_list: list[str] = []
    replaced = False
    for line in meta_text.splitlines():
        if line.startswith("data_file="):
            updated_line_list.append(f"data_file={str(target_data_file_name).strip()}")
            replaced = True
            continue
        updated_line_list.append(line)
    if not replaced:
        updated_line_list.insert(0, f"data_file={str(target_data_file_name).strip()}")
    target_meta_path.write_text("\n".join(updated_line_list) + "\n", encoding="utf-8")


def _resolve_heavy_receiver_data_root(workspace_root: Path) -> Path:
    return (
        workspace_root
        / "app"
        / "Collector"
        / "Collector"
        / "receiver"
        / "virtual_receiver"
        / "data"
    )


def _resolve_repo_receiver_data_root() -> Path:
    return (
        PROJECT_ROOT
        / "app"
        / "Collector"
        / "Collector"
        / "receiver"
        / "virtual_receiver"
        / "data"
    )


def _ignore_heavy_workspace_app_copy(directory: str, names: list[str]) -> set[str]:
    ignored_names: set[str] = set()
    current_path = Path(directory)
    normalized_parts = {part.lower() for part in current_path.parts}
    if "__pycache__" in names:
        ignored_names.add("__pycache__")
    if ".pytest_cache" in names:
        ignored_names.add(".pytest_cache")
    if "virtual_receiver" in normalized_parts:
        if "data" in names:
            ignored_names.add("data")
        if "org_data" in names:
            ignored_names.add("org_data")
    return ignored_names


def _ignore_heavy_workspace_proceed_copy(directory: str, names: list[str]) -> set[str]:
    ignored_names: set[str] = set()
    current_path = Path(directory)
    if "__pycache__" in names:
        ignored_names.add("__pycache__")
    if ".idea" in names:
        ignored_names.add(".idea")
    if current_path.resolve() == (PROJECT_ROOT / "proceed" / "centrol").resolve():
        if "runtime" in names:
            ignored_names.add("runtime")
    return ignored_names


def _ignore_heavy_workspace_dashboard_copy(directory: str, names: list[str]) -> set[str]:
    ignored_names = {name for name in ("node_modules", ".vite") if name in names}
    if "__pycache__" in names:
        ignored_names.add("__pycache__")
    return ignored_names


def _cleanup_heavy_generated_receiver_data(environment: HeavyEnvironment, progress: HeavyProgressReporter) -> None:
    generated_file_path_list = environment.generated_receiver_data_file_list
    if not generated_file_path_list:
        return
    removed_count = 0
    missing_count = 0
    for file_path in generated_file_path_list:
        if not file_path.exists():
            missing_count += 1
            continue
        try:
            file_path.unlink()
            removed_count += 1
        except OSError:
            continue
    progress.emit(
        6,
        "cleanup_generated_data",
        (
            f"removed_copied_files={removed_count} "
            f"missing_files={missing_count} "
            f"data_root={environment.generated_receiver_data_root}"
        ),
    )


def dump_heavy_environment_snapshot(environment: HeavyEnvironment) -> dict:
    central_config_path = (
        environment.workspace_root
        / "app"
        / "CentralController"
        / "CentralController"
        / "config"
        / "CentralControllerConfig.yml"
    )
    runtime_stage_path = (
        environment.workspace_root
        / "app"
        / "ProcessHub"
        / "ApplicationFramework"
        / "config"
        / "RuntimeStageCoordinatorLauncherConfig.yml"
    )
    virtual_receiver_path = (
        environment.workspace_root
        / "app"
        / "Collector"
        / "Collector"
        / "receiver"
        / "virtual_receiver"
        / "VirtualReceiverConfig.yml"
    )
    return {
        "central_controller_config": yaml.safe_load(central_config_path.read_text(encoding="utf-8")),
        "runtime_stage_config": yaml.safe_load(runtime_stage_path.read_text(encoding="utf-8")),
        "virtual_receiver_config": yaml.safe_load(virtual_receiver_path.read_text(encoding="utf-8")),
    }


def _prepare_team_algorithm_workspaces(
    artifact_root: Path,
    team_config_list: list[dict],
    observation_root: Path,
) -> dict[str, Path]:
    workspace_by_team_id: dict[str, Path] = {}
    algorithm_workspace_root = artifact_root / "algorithm_workspaces"
    if algorithm_workspace_root.exists():
        shutil.rmtree(algorithm_workspace_root, ignore_errors=True)
    for team_config in team_config_list:
        team_id = str(team_config["team_id"])
        team_workspace_root = algorithm_workspace_root / team_id
        _prepare_single_team_algorithm_workspace(
            team_workspace_root=team_workspace_root,
            team_id=team_id,
            algorithm_port=int(team_config["algorithm_port"]),
            algorithm_profile=str(team_config.get("algorithm_profile") or "normal"),
            observation_root=observation_root,
        )
        workspace_by_team_id[team_id] = team_workspace_root
    return workspace_by_team_id


def _prepare_single_team_algorithm_workspace(
    team_workspace_root: Path,
    team_id: str,
    algorithm_port: int,
    algorithm_profile: str,
    observation_root: Path,
) -> None:
    app_root = team_workspace_root / "app"
    shutil.copytree(PROJECT_ROOT / "app" / "Algorithm", app_root / "Algorithm")
    _patch_algorithm_config_manager_port(
        app_root / "Algorithm" / "Algorithm" / "service" / "ConfigManager.py",
        algorithm_port,
    )
    _patch_algorithm_implement_for_profile(
        app_root / "Algorithm" / "Algorithm" / "method" / "model_artifacts" / "baseline_example" / "AlgorithmImplement.py",
        team_id=team_id,
        algorithm_profile=algorithm_profile,
        observation_root=observation_root,
    )
    _patch_predict_worker_manager_for_heavy(
        app_root / "Algorithm" / "Algorithm" / "method" / "worker" / "PredictWorkerManager.py",
    )


def _patch_algorithm_config_manager_port(config_manager_path: Path, algorithm_port: int) -> None:
    content = config_manager_path.read_text(encoding="utf-8")
    original = "    __LOCKED_CONNECTION_CONFIG = {\n        'rpc_address': '[::]:9981',\n    }\n"
    patched = (
        "    __LOCKED_CONNECTION_CONFIG = {\n"
        f"        'rpc_address': '[::]:{int(algorithm_port)}',\n"
        "    }\n"
    )
    config_manager_path.write_text(
        _replace_once(content, original, patched, str(config_manager_path)),
        encoding="utf-8",
    )


def _patch_algorithm_implement_for_profile(
    algorithm_implement_path: Path,
    team_id: str,
    algorithm_profile: str,
    observation_root: Path,
) -> None:
    content = algorithm_implement_path.read_text(encoding="utf-8")
    if "import asyncio\n" not in content:
        content = content.replace("import copy\n", "import asyncio\nimport copy\n", 1)
    if "import os\n" not in content:
        content = content.replace("import logging\n", "import logging\nimport os\n", 1)
    init_original = (
        "        self.__predict_timeout_seconds: float | None = None\n"
        "        self.__predict_worker_manager = PredictWorkerManager()\n"
    )
    init_patched = (
        "        self.__predict_timeout_seconds: float | None = None\n"
        "        self.__predict_worker_manager = PredictWorkerManager()\n"
        f"        self.__heavy_fault_profile = {algorithm_profile!r}\n"
        f"        self.__heavy_fault_team_id = {team_id!r}\n"
        f"        self.__heavy_fault_observation_root = {str(observation_root)!r}\n"
        "        self.__heavy_fault_injected_by_stage: set[tuple[str, str, str, str]] = set()\n"
        "        self.__heavy_fault_consumed_by_stage: set[tuple[str, str, str, str]] = set()\n"
    )
    content = _replace_once(content, init_original, init_patched, str(algorithm_implement_path))

    helper_original = (
        "    def get_required_channel_labels(self) -> dict[str, list[str]]:\n"
        "        # baseline示例明确声明所需通道，框架会在算法初始化后完成校验与重排。\n"
        "        return copy.deepcopy(self.__required_channel_labels)\n\n"
        "    async def calibrate(self) -> CalibrationStageResultObject:\n"
    )
    helper_patched = (
        "    def get_required_channel_labels(self) -> dict[str, list[str]]:\n"
        "        # baseline示例明确声明所需通道，框架会在算法初始化后完成校验与重排。\n"
        "        return copy.deepcopy(self.__required_channel_labels)\n\n"
        "    def __apply_heavy_runtime_resource_limits(self) -> None:\n"
        "        if str(self.__torch_device) != 'cpu':\n"
        "            return\n"
        "        try:\n"
        "            torch.set_num_threads(1)\n"
        "        except RuntimeError:\n"
        "            pass\n"
        "        try:\n"
        "            torch.set_num_interop_threads(1)\n"
        "        except RuntimeError:\n"
        "            pass\n\n"
        "    async def calibrate(self) -> CalibrationStageResultObject:\n"
    )
    content = _replace_once(content, helper_original, helper_patched, str(algorithm_implement_path))

    predict_original = (
        "        # 输入的 trial_data 已经是一个完整 trial，最后一行仍包含 trigger 通道。\n"
        "        # preprocess_single_trial() 会把它整理成模型输入张量。\n"
        "        input_tensor = self.__preprocessor.preprocess_single_trial(\n"
    )
    predict_patched = (
        "        # 输入的 trial_data 已经是一个完整 trial，最后一行仍包含 trigger 通道。\n"
        "        # preprocess_single_trial() 会把它整理成模型输入张量。\n"
        "        heavy_fault_result = self.__maybe_apply_heavy_fault_profile()\n"
        "        if heavy_fault_result is not None:\n"
        "            return heavy_fault_result\n"
        "        input_tensor = self.__preprocessor.preprocess_single_trial(\n"
    )
    content = _replace_once(content, predict_original, predict_patched, str(algorithm_implement_path))

    run_timeout_original = (
        "                    )\n"
        "                    continue\n"
        "                predict_complete_wallclock = time.time()\n"
    )
    run_timeout_patched = (
        "                    )\n"
        "                    continue\n"
        "                if self.__should_drop_heavy_fault_result():\n"
        "                    self.__record_heavy_fault_observation(\n"
        "                        event='drop_late_result',\n"
        "                        detail={\n"
        "                            'trial_context': trial_context,\n"
        "                            'reason': 'heavy_late_result_after_platform_timeout',\n"
        "                        },\n"
        "                    )\n"
        "                    continue\n"
        "                predict_complete_wallclock = time.time()\n"
    )
    content = _replace_once(content, run_timeout_original, run_timeout_patched, str(algorithm_implement_path))

    report_original = (
        "                report_dispatch_wallclock = time.time()\n"
        "                await self._proxy.report(AlgorithmResultObject(result=result))\n"
        "                report_complete_wallclock = time.time()\n"
    )
    report_patched = (
        "                report_dispatch_wallclock = time.time()\n"
        "                await self._proxy.report(AlgorithmResultObject(result=result))\n"
        "                if self.__should_duplicate_heavy_fault_result():\n"
        "                    self.__record_heavy_fault_observation(\n"
        "                        event='duplicate_result_report',\n"
        "                        detail={'trial_context': trial_context},\n"
        "                    )\n"
        "                    await self._proxy.report(AlgorithmResultObject(result=result))\n"
        "                report_complete_wallclock = time.time()\n"
    )
    content = _replace_once(content, report_original, report_patched, str(algorithm_implement_path))

    runtime_init_original = (
        "        if runtime_predict_timeout_seconds not in (None, ''):\n"
        "            self.__predict_worker_manager.set_timeout_seconds(float(runtime_predict_timeout_seconds))\n"
        "        self.__predict_timeout_seconds = self.__predict_worker_manager.get_timeout_seconds()\n"
        "        self.__torch_device = self.__resolve_torch_device()\n"
        "        self.__log_torch_device_usage(log_context='runtime_initialized')\n"
    )
    runtime_init_patched = (
        "        if runtime_predict_timeout_seconds not in (None, ''):\n"
        "            self.__predict_worker_manager.set_timeout_seconds(float(runtime_predict_timeout_seconds))\n"
        "        self.__predict_timeout_seconds = self.__predict_worker_manager.get_timeout_seconds()\n"
        "        self.__torch_device = self.__resolve_torch_device()\n"
        "        self.__apply_heavy_runtime_resource_limits()\n"
        "        self.__log_torch_device_usage(log_context='runtime_initialized')\n"
        "        self.__logger.info(\n"
        "            'heavy runtime initialized before device wait: team_id=%s profile=%s predict_timeout_seconds=%s torch_device=%s',\n"
        "            self.__heavy_fault_team_id,\n"
        "            self.__heavy_fault_profile,\n"
        "            self.__predict_timeout_seconds,\n"
        "            self.__torch_device,\n"
        "        )\n"
        "        self.__log_heavy_lifecycle('before_get_device')\n"
        "        # Device 信息必须在算法开始前先拿到。\n"
        "        self.__source_eeg_device = await self.__source_eeg.get_device()\n"
        "        self.__log_heavy_lifecycle(\n"
        "            'after_get_device',\n"
        "            {\n"
        "                'channel_number': self.__source_eeg_device.channel_number,\n"
        "                'sample_rate': self.__source_eeg_device.sample_rate,\n"
        "                'channel_label': list(self.__source_eeg_device.channel_label or []),\n"
        "            },\n"
        "        )\n"
        "        self.__channel_number = self.__source_eeg_device.channel_number\n"
        "        sample_rate = int(self.__source_eeg_device.sample_rate)\n"
        "        self.__sample_rate = sample_rate\n"
        "        self.__trial_point = int(self.__trial_duration * sample_rate)\n"
        "        self.__logger.info(\n"
        "            \"算法收到设备信息: channel_number=%s channel_label=%s sample_rate=%s\",\n"
        "            self.__channel_number,\n"
        "            self.__source_eeg_device.channel_label,\n"
        "            sample_rate,\n"
        "        )\n"
    )
    content = _replace_once(content, runtime_init_original + "        # Device 信息必须在算法开始前先拿到。\n        self.__source_eeg_device = await self.__source_eeg.get_device()\n        self.__channel_number = self.__source_eeg_device.channel_number\n        sample_rate = int(self.__source_eeg_device.sample_rate)\n        self.__sample_rate = sample_rate\n        self.__trial_point = int(self.__trial_duration * sample_rate)\n        self.__logger.info(\n            \"算法收到设备信息: channel_number=%s channel_label=%s sample_rate=%s\",\n            self.__channel_number,\n            self.__source_eeg_device.channel_label,\n            sample_rate,\n        )\n", runtime_init_patched, str(algorithm_implement_path))

    merge_original = "        return base_dict\n\n    async def __report_calibration_progress(\n"
    merge_patched = (
        "        return base_dict\n\n"
        "    def __maybe_apply_heavy_fault_profile(self) -> str | None:\n"
        "        profile_name = str(self.__heavy_fault_profile or 'normal').strip().lower()\n"
        "        if profile_name == 'normal' or not self.__should_inject_heavy_fault_once_per_stage():\n"
        "            return None\n"
        "        if profile_name in {'slow', 'late_result', 'disconnect_stream'}:\n"
        "            self.__record_heavy_fault_observation(event='timeout_fault_begin')\n"
        "            time.sleep(max(float(self.__predict_timeout_seconds or 1.0) + 0.25, 1.25))\n"
        "            return json.dumps({'predict_label': 0}, ensure_ascii=False)\n"
        "        if profile_name == 'resource_hog':\n"
        "            self.__record_heavy_fault_observation(event='resource_hog_begin', detail={'cpu_burn_ms': 1500, 'memory_kb': 2048})\n"
        "            deadline = time.perf_counter() + max(float(self.__predict_timeout_seconds or 1.0) + 0.25, 1.25)\n"
        "            memory_blob = bytearray(2048 * 1024)\n"
        "            checksum = 0\n"
        "            while time.perf_counter() < deadline:\n"
        "                checksum = (checksum + sum(memory_blob[:1024])) % 1000003\n"
        "            self.__record_heavy_fault_observation(event='resource_hog_end', detail={'checksum': checksum})\n"
        "            return json.dumps({'predict_label': 0}, ensure_ascii=False)\n"
        "        if profile_name == 'invalid_output':\n"
        "            self.__record_heavy_fault_observation(event='invalid_output')\n"
        "            return '{\"malformed_predict_label\":'\n"
        "        if profile_name == 'malicious':\n"
        "            for operation in ('read_hidden_score', 'write_results', 'network_access', 'kill_process'):\n"
        "                self.__record_heavy_fault_observation(\n"
        "                    event='malicious_action',\n"
        "                    detail={'operation': operation, 'status': 'blocked'},\n"
        "                )\n"
        "            return json.dumps({'unexpected_field': 'malicious_payload_blocked'}, ensure_ascii=False)\n"
        "        if profile_name == 'duplicate_result':\n"
        "            self.__record_heavy_fault_observation(event='duplicate_profile_armed')\n"
        "            return None\n"
        "        return None\n\n"
        "    def __should_inject_heavy_fault_once_per_stage(self) -> bool:\n"
        "        stage_signature = tuple(self.__current_stage_signature or ())\n"
        "        if len(stage_signature) != 4:\n"
        "            return False\n"
        "        if stage_signature in self.__heavy_fault_injected_by_stage:\n"
        "            return False\n"
        "        self.__heavy_fault_injected_by_stage.add(stage_signature)\n"
        "        return True\n\n"
        "    def __should_duplicate_heavy_fault_result(self) -> bool:\n"
        "        stage_signature = tuple(self.__current_stage_signature or ())\n"
        "        if self.__heavy_fault_profile != 'duplicate_result' or len(stage_signature) != 4:\n"
        "            return False\n"
        "        if stage_signature in self.__heavy_fault_consumed_by_stage:\n"
        "            return False\n"
        "        self.__heavy_fault_consumed_by_stage.add(stage_signature)\n"
        "        return True\n\n"
        "    def __should_drop_heavy_fault_result(self) -> bool:\n"
        "        stage_signature = tuple(self.__current_stage_signature or ())\n"
        "        if self.__heavy_fault_profile != 'late_result' or len(stage_signature) != 4:\n"
        "            return False\n"
        "        if stage_signature in self.__heavy_fault_consumed_by_stage:\n"
        "            return False\n"
        "        self.__heavy_fault_consumed_by_stage.add(stage_signature)\n"
        "        return True\n\n"
        "    def __record_heavy_fault_observation(self, event: str, detail: dict | None = None) -> None:\n"
        "        if self.__heavy_fault_profile == 'normal':\n"
        "            return\n"
        "        observation_root = self.__heavy_fault_observation_root\n"
        "        if not observation_root:\n"
        "            return\n"
        "        observation_dir = Path(observation_root)\n"
        "        observation_dir.mkdir(parents=True, exist_ok=True)\n"
        "        payload = {\n"
        "            'time': time.time(),\n"
        "            'team_id': self.__heavy_fault_team_id,\n"
        "            'profile': self.__heavy_fault_profile,\n"
        "            'event': event,\n"
        "            'stage_signature': list(self.__current_stage_signature or ()),\n"
        "        }\n"
        "        if detail:\n"
        "            payload.update(detail)\n"
        "        with (observation_dir / f'{self.__heavy_fault_team_id}.jsonl').open('a', encoding='utf-8') as file:\n"
        "            file.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + '\\n')\n\n"
        "    def __log_heavy_lifecycle(self, step: str, detail: dict | None = None) -> None:\n"
        "        log_payload = {\n"
        "            'team_id': self.__heavy_fault_team_id,\n"
        "            'profile': self.__heavy_fault_profile,\n"
        "            'step': step,\n"
        "            'stage_signature': list(self.__current_stage_signature or ()),\n"
        "        }\n"
        "        if detail:\n"
        "            log_payload.update(detail)\n"
        "        self.__logger.info('heavy lifecycle: %s', json.dumps(log_payload, ensure_ascii=False, sort_keys=True))\n\n"
        "    async def __report_calibration_progress(\n"
    )
    content = _replace_once(content, merge_original, merge_patched, str(algorithm_implement_path))
    calibrate_wait_original = (
        "        await self.__report_calibration_progress(\n"
        "            status='waiting',\n"
        "            progress=0.0,\n"
        "            message='等待校准数据',\n"
        "        ) # 这个函数用来统一记录当前校准阶段进度，便于日志和后续监控扩展\n\n"
        "        calibration_object = await self.__source_eeg.get_calibration()\n"
    )
    calibrate_wait_patched = (
        "        await self.__report_calibration_progress(\n"
        "            status='waiting',\n"
        "            progress=0.0,\n"
        "            message='等待校准数据',\n"
        "        ) # 这个函数用来统一记录当前校准阶段进度，便于日志和后续监控扩展\n"
        "        self.__log_heavy_lifecycle('before_get_calibration')\n\n"
        "        calibration_object = await self.__source_eeg.get_calibration()\n"
        "        self.__log_heavy_lifecycle(\n"
        "            'after_get_calibration',\n"
        "            {\n"
        "                'finish_flag': bool(calibration_object.finish_flag),\n"
        "                'subject_id': calibration_object.subject_id,\n"
        "                'exp_name': calibration_object.exp_name,\n"
        "                'exp_task': calibration_object.exp_task,\n"
        "                'session_id': calibration_object.session_id,\n"
        "            },\n"
        "        )\n"
    )
    content = _replace_once(content, calibrate_wait_original, calibrate_wait_patched, str(algorithm_implement_path))

    predict_worker_original = (
        "    async def __predict_with_worker(self, trial_data: np.ndarray) -> str:\n"
        "        return await self.__predict_worker_manager.predict(trial_data=trial_data)\n\n"
        "    def load_predict_session(\n"
    )
    predict_worker_patched = (
        "    def __use_heavy_inline_predict_mode(self) -> bool:\n"
        "        override_value = os.environ.get('BCI_HEAVY_INLINE_PREDICT_MODE')\n"
        "        if override_value in (None, ''):\n"
        "            return True\n"
        "        return str(override_value).strip().lower() not in {'0', 'false', 'no', 'off'}\n\n"
        "    async def __predict_with_worker(self, trial_data: np.ndarray) -> str:\n"
        "        if not self.__use_heavy_inline_predict_mode():\n"
        "            return await self.__predict_worker_manager.predict(trial_data=trial_data)\n"
        "        try:\n"
        "            return await asyncio.wait_for(\n"
        "                asyncio.to_thread(self.predict, trial_data),\n"
        "                timeout=max(float(self.__predict_timeout_seconds or 1.0), 0.01),\n"
        "            )\n"
        "        except asyncio.TimeoutError as exc:\n"
        "            raise PredictWorkerTimeoutError(\n"
        "                f'heavy inline predict timed out after {self.__predict_timeout_seconds}s'\n"
        "            ) from exc\n\n"
        "    def load_predict_session(\n"
    )
    content = _replace_once(content, predict_worker_original, predict_worker_patched, str(algorithm_implement_path))

    sync_worker_original = (
        "    async def __sync_predict_worker_model(self) -> None:\n"
        "        if self.__current_model is None or self.__current_stage_signature is None:\n"
        "            raise RuntimeError('predict worker sync requires current model and stage signature')\n"
        "        await self.__predict_worker_manager.sync_session(\n"
        "            runtime_config=self.__runtime_config,\n"
        "            stage_signature=self.__current_stage_signature,\n"
        "            sample_rate=self.__sample_rate,\n"
        "            channel_number=self.__channel_number,\n"
        "            trial_point=self.__trial_point,\n"
        "            model_state_dict=self.__build_predict_worker_state_dict(),\n"
        "        )\n"
    )
    sync_worker_patched = (
        "    async def __sync_predict_worker_model(self) -> None:\n"
        "        if self.__current_model is None or self.__current_stage_signature is None:\n"
        "            raise RuntimeError('predict worker sync requires current model and stage signature')\n"
        "        if self.__use_heavy_inline_predict_mode():\n"
        "            self.__logger.info(\n"
        "                'heavy inline predict mode active, skip predict worker session sync: stage_signature=%s',\n"
        "                self.__current_stage_signature,\n"
        "            )\n"
        "            return\n"
        "        await self.__predict_worker_manager.sync_session(\n"
        "            runtime_config=self.__runtime_config,\n"
        "            stage_signature=self.__current_stage_signature,\n"
        "            sample_rate=self.__sample_rate,\n"
        "            channel_number=self.__channel_number,\n"
        "            trial_point=self.__trial_point,\n"
        "            model_state_dict=self.__build_predict_worker_state_dict(),\n"
        "        )\n"
    )
    content = _replace_once(content, sync_worker_original, sync_worker_patched, str(algorithm_implement_path))
    algorithm_implement_path.write_text(content, encoding="utf-8")


def _patch_predict_worker_manager_for_heavy(predict_worker_manager_path: Path) -> None:
    content = predict_worker_manager_path.read_text(encoding="utf-8")
    if "import os" not in content:
        content = content.replace("import logging\n", "import logging\nimport os\n", 1)

    init_original = (
        "        if predict_timeout_seconds in (None, ''):\n"
        "            predict_timeout_seconds = self.__DEFAULT_PREDICT_TIMEOUT_SECONDS\n"
        "        self.__predict_timeout_seconds = float(predict_timeout_seconds)\n"
        "        self.__session_sync_timeout_seconds = max(10.0, self.__predict_timeout_seconds * 5.0)\n"
    )
    init_patched = (
        "        if predict_timeout_seconds in (None, ''):\n"
        "            predict_timeout_seconds = self.__DEFAULT_PREDICT_TIMEOUT_SECONDS\n"
        "        self.__predict_timeout_seconds = float(predict_timeout_seconds)\n"
        "        self.__session_sync_timeout_seconds = self.__resolve_session_sync_timeout_seconds(\n"
        "            self.__predict_timeout_seconds\n"
        "        )\n"
    )
    content = _replace_once(content, init_original, init_patched, str(predict_worker_manager_path))

    set_timeout_original = (
        "    def set_timeout_seconds(self, predict_timeout_seconds: float) -> None:\n"
        "        self.__predict_timeout_seconds = float(predict_timeout_seconds)\n"
        "        self.__session_sync_timeout_seconds = max(10.0, self.__predict_timeout_seconds * 5.0)\n\n"
        "    def get_timeout_seconds(self) -> float:\n"
        "        return self.__predict_timeout_seconds\n"
    )
    set_timeout_patched = (
        "    def set_timeout_seconds(self, predict_timeout_seconds: float) -> None:\n"
        "        self.__predict_timeout_seconds = float(predict_timeout_seconds)\n"
        "        self.__session_sync_timeout_seconds = self.__resolve_session_sync_timeout_seconds(\n"
        "            self.__predict_timeout_seconds\n"
        "        )\n\n"
        "    def get_timeout_seconds(self) -> float:\n"
        "        return self.__predict_timeout_seconds\n\n"
        "    async def ensure_worker_started(self) -> None:\n"
        "        await self.__start_worker_if_needed()\n\n"
        "    @staticmethod\n"
        "    def __resolve_session_sync_timeout_seconds(predict_timeout_seconds: float) -> float:\n"
        "        override_value = os.environ.get('BCI_PREDICT_WORKER_SESSION_SYNC_TIMEOUT_SECONDS')\n"
        "        if override_value not in (None, ''):\n"
        "            try:\n"
        "                return max(10.0, float(override_value))\n"
        "            except ValueError:\n"
        "                pass\n"
        "        return max(10.0, float(predict_timeout_seconds) * 5.0)\n"
    )
    content = _replace_once(content, set_timeout_original, set_timeout_patched, str(predict_worker_manager_path))
    predict_worker_manager_path.write_text(content, encoding="utf-8")


def _patch_headless_start_judge_stack(workspace_root: Path) -> None:
    start_judge_stack_path = workspace_root / "tools" / "start_judge_stack.py"
    content = start_judge_stack_path.read_text(encoding="utf-8")
    marker = (
        "RUNTIME_STAGE_LAUNCHER_CONFIG_PATH = (\n"
        "    APP_ROOT / 'ProcessHub' / 'ApplicationFramework' / 'config' / 'RuntimeStageCoordinatorLauncherConfig.yml'\n"
        ")\n"
    )
    injected = (
        marker
        + "\n\n"
        + "def is_headless_mode_enabled() -> bool:\n"
        + "    return str(os.environ.get('BCI_HEADLESS') or '').strip().lower() in {'1', 'true', 'yes', 'on'}\n"
        + "\n\n"
        + "def split_headless_command(command: str) -> list[str]:\n"
        + "    part_list = []\n"
        + "    current_char_list = []\n"
        + "    in_double_quote = False\n"
        + "    for char in str(command or '').strip():\n"
        + "        if char == '\"':\n"
        + "            in_double_quote = not in_double_quote\n"
        + "            continue\n"
        + "        if char.isspace() and not in_double_quote:\n"
        + "            if current_char_list:\n"
        + "                part_list.append(''.join(current_char_list))\n"
        + "                current_char_list = []\n"
        + "            continue\n"
        + "        current_char_list.append(char)\n"
        + "    if current_char_list:\n"
        + "        part_list.append(''.join(current_char_list))\n"
        + "    return part_list\n"
        + "\n\n"
        + "def wait_for_local_tcp_port(host: str, port: int, timeout_seconds: float = 30.0) -> bool:\n"
        + "    import socket\n"
        + "    deadline = time.time() + float(timeout_seconds)\n"
        + "    last_error = '<none>'\n"
        + "    while time.time() < deadline:\n"
        + "        probe_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        + "        probe_socket.settimeout(1.0)\n"
        + "        try:\n"
        + "            probe_socket.connect((host, int(port)))\n"
        + "            print(f'[judge-start] tcp ready: {host}:{port}')\n"
        + "            return True\n"
        + "        except OSError as exc:\n"
        + "            last_error = repr(exc)\n"
        + "        finally:\n"
        + "            probe_socket.close()\n"
        + "        time.sleep(0.5)\n"
        + "    print(f'[judge-start] tcp wait timeout: {host}:{port} last_error={last_error}')\n"
        + "    return False\n"
        + "\n\n"
        + "def wait_for_collector_runtime_ready(timeout_seconds: float = 90.0) -> bool:\n"
        + "    if not is_headless_mode_enabled():\n"
        + "        return True\n"
        + "    collector_log_path = CONTROL_ROOT / 'headless_logs' / 'BCI_Judge__Collector_Python.stdout.log'\n"
        + "    central_log_path = CONTROL_ROOT / 'headless_logs' / 'BCI_Judge__CentralController_Python.stdout.log'\n"
        + "    collector_ready_pattern_list = [\n"
        + "        'VirtualReceiverImplement.startup',\n"
        + "        'VirtualReceiver custom control topic',\n"
        + "        'collector_group_1.virtual_receiver_custom_control',\n"
        + "    ]\n"
        + "    central_ready_pattern_list = [\n"
        + "        \"component_id='collector_group_1'\",\n"
        + "        \"message_key='virtual_receiver_custom_control'\",\n"
        + "        \"topic='collector_group_1.virtual_receiver_custom_control'\",\n"
        + "    ]\n"
        + "    deadline = time.time() + float(timeout_seconds)\n"
        + "    last_collector_excerpt = '<missing>'\n"
        + "    last_central_excerpt = '<missing>'\n"
        + "    while time.time() < deadline:\n"
        + "        collector_text = ''\n"
        + "        central_text = ''\n"
        + "        if collector_log_path.exists():\n"
        + "            collector_text = collector_log_path.read_text(encoding='utf-8', errors='ignore')\n"
        + "            collector_line_list = collector_text.splitlines()\n"
        + "            last_collector_excerpt = '\\n'.join(collector_line_list[-6:]) if collector_line_list else '<empty>'\n"
        + "        if central_log_path.exists():\n"
        + "            central_text = central_log_path.read_text(encoding='utf-8', errors='ignore')\n"
        + "            central_line_list = central_text.splitlines()\n"
        + "            matching_line_list = [\n"
        + "                line for line in central_line_list if 'virtual_receiver_custom_control' in line or 'collector_group_1' in line\n"
        + "            ]\n"
        + "            tail_source_line_list = matching_line_list[-6:] if matching_line_list else central_line_list[-6:]\n"
        + "            last_central_excerpt = '\\n'.join(tail_source_line_list) if tail_source_line_list else '<empty>'\n"
        + "        collector_ready = all(pattern_text in collector_text for pattern_text in collector_ready_pattern_list)\n"
        + "        central_ready = all(pattern_text in central_text for pattern_text in central_ready_pattern_list)\n"
        + "        if collector_ready and central_ready:\n"
        + "            print(\n"
        + "                '[judge-start] collector runtime ready for ProcessHub startup: '\n"
        + "                f'collector_log={collector_log_path} central_log={central_log_path}'\n"
        + "            )\n"
        + "            time.sleep(1.5)\n"
        + "            return True\n"
        + "        time.sleep(0.5)\n"
        + "    print('[judge-start] collector runtime readiness timeout before ProcessHub startup')\n"
        + "    print(f'[judge-start] collector_log_tail={last_collector_excerpt}')\n"
        + "    print(f'[judge-start] central_log_tail={last_central_excerpt}')\n"
        + "    return False\n"
    )
    content = _replace_once(content, marker, injected, str(start_judge_stack_path))

    launch_sequence_original = (
        "    launch_component(\n"
        "        title='[BCI Judge] Collector Python',\n"
        "        cwd=APP_ROOT / 'Collector',\n"
        "        command=f'{python_command} -m ApplicationFramework.main',\n"
        "    )\n"
        "\n"
        "    for component_id in load_processor_component_id_list():\n"
        "        launch_component(\n"
        "            title=f'[BCI Judge] ProcessHub {component_id}',\n"
        "            cwd=APP_ROOT / 'ProcessHub',\n"
        "            command=f'{python_command} -m ApplicationFramework.main',\n"
        "            extra_env={\n"
        "                'COMPONENT_ID': component_id,\n"
        "            },\n"
        "        )\n"
    )
    launch_sequence_patched = (
        "    central_controller_component_port_ready = wait_for_local_tcp_port('127.0.0.1', 9002, timeout_seconds=45.0)\n"
        "    if not central_controller_component_port_ready:\n"
        "        print('[judge-start] central controller component port 9002 was not confirmed ready before Collector startup')\n"
        "\n"
        "    launch_component(\n"
        "        title='[BCI Judge] Collector Python',\n"
        "        cwd=APP_ROOT / 'Collector',\n"
        "        command=f'{python_command} -m ApplicationFramework.main',\n"
        "    )\n"
        "    collector_runtime_ready = wait_for_collector_runtime_ready(timeout_seconds=90.0)\n"
        "    if not collector_runtime_ready:\n"
        "        print('[judge-start] collector runtime was not confirmed ready, ProcessHub startup will proceed with risk noted')\n"
        "\n"
        "    for component_id in load_processor_component_id_list():\n"
        "        launch_component(\n"
        "            title=f'[BCI Judge] ProcessHub {component_id}',\n"
        "            cwd=APP_ROOT / 'ProcessHub',\n"
        "            command=f'{python_command} -m ApplicationFramework.main',\n"
        "            extra_env={\n"
        "                'COMPONENT_ID': component_id,\n"
        "            },\n"
        "        )\n"
    )
    content = _replace_once(content, launch_sequence_original, launch_sequence_patched, str(start_judge_stack_path))

    original = (
        "def start_component_window(title: str, cwd: Path, command: str, extra_env: dict | None = None) -> dict:\n"
        "    env = os.environ.copy()\n"
        "    if extra_env:\n"
        "        env.update({key: str(value) for key, value in extra_env.items()})\n"
        "    launcher_script_path = write_component_launcher_script(title, cwd, command)\n"
        "    process = subprocess.Popen(\n"
        "        ['cmd', '/k', str(launcher_script_path)],\n"
        "        cwd=str(cwd),\n"
        "        env=env,\n"
        "        creationflags=subprocess.CREATE_NEW_CONSOLE,\n"
        "    )\n"
        "    return {\n"
        "        'title': title,\n"
        "        'pid': process.pid,\n"
        "        'cwd': str(cwd),\n"
        "        'command': command,\n"
        "        'launcher_script_path': str(launcher_script_path),\n"
        "        'started_at': time.time(),\n"
        "    }\n"
    )
    patched = (
        "def start_component_window(title: str, cwd: Path, command: str, extra_env: dict | None = None) -> dict:\n"
        "    env = os.environ.copy()\n"
        "    if extra_env:\n"
        "        env.update({key: str(value) for key, value in extra_env.items()})\n"
        "    env.setdefault('PYTHONHASHSEED', '0')\n"
        "    started_at = time.time()\n"
        "    if is_headless_mode_enabled():\n"
        "        log_root = CONTROL_ROOT / 'headless_logs'\n"
        "        log_root.mkdir(parents=True, exist_ok=True)\n"
        "        safe_title = ''.join(char if char.isalnum() else '_' for char in title).strip('_') or 'component'\n"
        "        stdout_path = log_root / f'{safe_title}.stdout.log'\n"
        "        stderr_path = log_root / f'{safe_title}.stderr.log'\n"
        "        stdout_file = stdout_path.open('w', encoding='utf-8')\n"
        "        stderr_file = stderr_path.open('w', encoding='utf-8')\n"
        "        command_part_list = split_headless_command(command)\n"
        "        process = subprocess.Popen(\n"
        "            command_part_list,\n"
        "            cwd=str(cwd),\n"
        "            env=env,\n"
        "            stdout=stdout_file,\n"
        "            stderr=stderr_file,\n"
        "            text=True,\n"
        "        )\n"
        "        return {\n"
        "            'title': title,\n"
        "            'pid': process.pid,\n"
        "            'cwd': str(cwd),\n"
        "            'command': command,\n"
        "            'started_at': started_at,\n"
        "            'mode': 'headless',\n"
        "            'stdout_path': str(stdout_path),\n"
        "            'stderr_path': str(stderr_path),\n"
        "        }\n"
        "    launcher_script_path = write_component_launcher_script(title, cwd, command)\n"
        "    process = subprocess.Popen(\n"
        "        ['cmd', '/k', str(launcher_script_path)],\n"
        "        cwd=str(cwd),\n"
        "        env=env,\n"
        "        creationflags=subprocess.CREATE_NEW_CONSOLE,\n"
        "    )\n"
        "    return {\n"
        "        'title': title,\n"
        "        'pid': process.pid,\n"
        "        'cwd': str(cwd),\n"
        "        'command': command,\n"
        "        'started_at': started_at,\n"
        "        'launcher_script_path': str(launcher_script_path),\n"
        "        'mode': 'window',\n"
        "    }\n"
    )
    start_judge_stack_path.write_text(
        _replace_once(content, original, patched, str(start_judge_stack_path)),
        encoding="utf-8",
    )


def _patch_central_controller_component_monitor_for_heavy(workspace_root: Path) -> None:
    component_monitor_path = (
        workspace_root
        / "app"
        / "CentralController"
        / "CentralController"
        / "service"
        / "ComponentMonitor.py"
    )
    content = component_monitor_path.read_text(encoding="utf-8")
    original = (
        "        component_model_dict = self.__service_coordinator.get_registered_component_information_model_dict()\n"
        "        component_info_status_model_list: list[ComponentGroupStatusModel] = []\n"
        "        for component_model_id in component_model_dict:\n"
        "            registered_component_information_model = component_model_dict[component_model_id]\n"
    )
    patched = (
        "        component_model_dict = self.__service_coordinator.get_registered_component_information_model_dict()\n"
        "        component_model_item_list = list(component_model_dict.items())\n"
        "        component_info_status_model_list: list[ComponentGroupStatusModel] = []\n"
        "        for component_model_id, registered_component_information_model in component_model_item_list:\n"
    )
    component_monitor_path.write_text(
        _replace_once(content, original, patched, str(component_monitor_path)),
        encoding="utf-8",
    )


def _replace_once(content: str, original: str, replacement: str, context: str) -> str:
    if original not in content:
        raise AssertionError(f"failed to patch expected snippet in {context}")
    return content.replace(original, replacement, 1)
