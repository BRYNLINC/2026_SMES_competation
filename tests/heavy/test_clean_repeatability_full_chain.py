from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pytest
import yaml

from tests.helpers.heavy_runtime import (
    assert_heavy_completion_outputs,
    prepare_heavy_workspace,
    shutdown_heavy_environment,
    start_headless_judge_stack,
    start_heavy_algorithms,
    wait_for_heavy_completion,
)
from tests.helpers.project_paths import latest_artifacts_root, project_root
from tools.runtime_state_sqlite import (
    load_team_score_overview_rows,
    load_team_trial_record_rows,
    resolve_runtime_state_db_path,
)


pytestmark = [
    pytest.mark.heavy,
    pytest.mark.slow,
    pytest.mark.e2e,
    pytest.mark.layer("heavy"),
    pytest.mark.category("clean_run_repeatability"),
]


TRIAL_POINTS = 4000
FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS = 10
TRIALS_PER_RAW_TRIGGER = 60
RAW_TRIGGER_SEQUENCE = (1, 2, 3) * TRIALS_PER_RAW_TRIGGER
EXPECTED_ONLINE_TRIAL_COUNT = 8 * (
    TRIALS_PER_RAW_TRIGGER - FIXED_CALIBRATION_POOL_TRIALS_PER_CLASS
)
REPEATABILITY_TIMEOUT_SECONDS = 1200.0
STABLE_TRIAL_FIELDS = (
    "team_trial_index",
    "task_trial_index",
    "task_id",
    "subject_id",
    "exp_name",
    "exp_task",
    "session_id",
    "block_id",
    "trial_id",
    "true_label",
    "raw_predict_label",
    "predict_label",
    "is_correct",
    "is_timeout",
    "is_invalid_output",
    "report_position",
)


def _sha256_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_channel_labels() -> list[str]:
    config_path = (
        project_root()
        / "app"
        / "Collector"
        / "Collector"
        / "receiver"
        / "virtual_receiver"
        / "VirtualReceiverConfig.yml"
    )
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    eeg_channel_labels = list((payload.get("device_info") or {}).get("channel_label") or {})
    assert len(eeg_channel_labels) == 64
    return [*eeg_channel_labels, "HEO", "VEO", "EKG", "EMG", "TRIGGER"]


def _write_tiny_dat_pair(source_root: Path) -> None:
    subject_root = source_root / "sub_repeat"
    channel_labels = _load_channel_labels()
    sample_count = len(RAW_TRIGGER_SEQUENCE) * TRIAL_POINTS

    # The signal is intentionally simple. Random model initialization and the
    # calibration DataLoader still exercise all RNG state controlled by the fix.
    sample_matrix = np.zeros((sample_count, len(channel_labels)), dtype="<f4")
    for trial_index, raw_trigger in enumerate(RAW_TRIGGER_SEQUENCE):
        sample_matrix[trial_index * TRIAL_POINTS, -1] = float(raw_trigger)

    for exp_name, session_name in (("vme", "session1"), ("vmi", "session2")):
        session_root = subject_root / session_name
        session_root.mkdir(parents=True, exist_ok=True)
        dat_path = session_root / f"repeat_{exp_name}_run1.dat"
        sample_matrix.tofile(dat_path)
        meta_path = dat_path.with_name(f"{dat_path.stem}_meta.txt")
        meta_path.write_text(
            "\n".join(
                (
                    f"data_file={dat_path.name}",
                    "data_layout=timepoints_by_channels",
                    "storage_format=binary_float32_le",
                    "value_order=sample_major_trigger_last",
                    "original_data_shape=channels_by_timepoints_by_trials",
                    f"timepoints={sample_count}",
                    f"channels={len(channel_labels)}",
                    "eeg_channels=68",
                    "trials=1",
                    "sampling_rate_hz=1000",
                    f"duration_seconds={sample_count / 1000.0}",
                    f"event_count={len(RAW_TRIGGER_SEQUENCE)}",
                    "trigger_source=channel:TRIGGER",
                    f"channel_labels={','.join(channel_labels)}",
                )
            )
            + "\n",
            encoding="utf-8",
        )


def _restore_production_algorithm_and_shorten_calibration(algorithm_workspace_root: Path) -> Path:
    source_algorithm_package = project_root() / "app" / "Algorithm" / "Algorithm"
    target_algorithm_package = algorithm_workspace_root / "app" / "Algorithm" / "Algorithm"
    relative_file_list = (
        Path("method/model_artifacts/baseline_example/AlgorithmImplement.py"),
        Path("method/worker/PredictWorkerManager.py"),
    )
    for relative_path in relative_file_list:
        shutil.copy2(
            source_algorithm_package / relative_path,
            target_algorithm_package / relative_path,
        )

    implement_path = target_algorithm_package / relative_file_list[0]
    content = implement_path.read_text(encoding="utf-8")
    replacement_by_original = {
        "'calibration_trials_per_class_requested': 7": "'calibration_trials_per_class_requested': 1",
        "'device': 'cuda:0'": "'device': 'cpu'",
        "'calibration_epochs': 100": "'calibration_epochs': 1",
    }
    for original, replacement in replacement_by_original.items():
        assert content.count(original) == 1, original
        content = content.replace(original, replacement, 1)
    implement_path.write_text(content, encoding="utf-8")
    return implement_path


def _stable_trial_projection(row: dict) -> dict:
    return {field_name: row.get(field_name) for field_name in STABLE_TRIAL_FIELDS}


def _run_clean_match(run_root: Path, source_root: Path, python_executable: str) -> dict:
    environment = prepare_heavy_workspace(
        run_root,
        team_count=1,
        source_receiver_data_root=source_root,
    )
    implement_path = _restore_production_algorithm_and_shorten_calibration(
        environment.algorithm_workspace_by_team_id["team_0"]
    )
    try:
        start_heavy_algorithms(environment, python_executable)
        start_headless_judge_stack(environment, python_executable)
        wait_for_heavy_completion(
            environment,
            timeout_seconds=REPEATABILITY_TIMEOUT_SECONDS,
        )
        assert_heavy_completion_outputs(environment, expected_team_count=1)

        runtime_state_db_path = resolve_runtime_state_db_path(environment.workspace_root)
        trial_rows = load_team_trial_record_rows(runtime_state_db_path, "team_0")
        score_rows = load_team_score_overview_rows(runtime_state_db_path)
        manifest = json.loads(
            (environment.results_root / "control" / "launcher_manifest.json").read_text(
                encoding="utf-8"
            )
        )
        assert manifest["match_start_mode"] == "clear"
        assert (manifest.get("run_provenance") or {}).get("run_kind") == "clean_full_run"
        assert len(score_rows) == 1
        assert trial_rows
        return {
            "results_root": str(environment.results_root),
            "algorithm_sha256": _sha256_file(implement_path),
            "config_sha256_by_path": (manifest.get("run_provenance") or {}).get(
                "config_sha256_by_path"
            )
            or {},
            "stable_trials": [_stable_trial_projection(row) for row in trial_rows],
            "predict_time_ms": [float(row.get("predict_time_ms") or 0.0) for row in trial_rows],
            "timeout_count": sum(bool(row.get("is_timeout")) for row in trial_rows),
            "accuracy_percent": float(score_rows[0].get("mean_accuracy_percent") or 0.0),
            "total_score": float(score_rows[0].get("total_score") or 0.0),
        }
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("CLEAN-REPEAT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement(
    "Two independent 400-trial clean runs with identical code, config and input must produce identical predictions and accuracy"
)
@pytest.mark.tested(
    file=(
        "app/Algorithm/Algorithm/common/utils/seed.py;"
        "app/Algorithm/Algorithm/service/SourceReceiver/ContinuousDataSourceReceiver.py;"
        "app/Algorithm/Algorithm/method/worker/PredictWorkerProcess.py"
    ),
    function="seed_everything_for_stage/predict_worker_main",
)
def test_two_independent_clean_runs_have_identical_predictions_and_accuracy(
    python_executable: str,
) -> None:
    artifact_root = latest_artifacts_root() / "heavy" / "clean_repeatability_400"
    if artifact_root.exists():
        shutil.rmtree(artifact_root)
    source_root = artifact_root / "source_dataset"
    _write_tiny_dat_pair(source_root)

    first = _run_clean_match(artifact_root / "run_1", source_root, python_executable)
    second = _run_clean_match(artifact_root / "run_2", source_root, python_executable)

    assert first["algorithm_sha256"] == second["algorithm_sha256"]
    assert first["config_sha256_by_path"] == second["config_sha256_by_path"]
    assert len(first["stable_trials"]) == EXPECTED_ONLINE_TRIAL_COUNT == 400
    assert first["stable_trials"] == second["stable_trials"]
    assert first["accuracy_percent"] == second["accuracy_percent"]
    assert first["timeout_count"] == second["timeout_count"] == 0

    timing_mismatch_count = sum(
        first_value != second_value
        for first_value, second_value in zip(
            first["predict_time_ms"],
            second["predict_time_ms"],
        )
    )
    report = {
        "verdict": "prediction_and_accuracy_reproducible",
        "target_trial_count": EXPECTED_ONLINE_TRIAL_COUNT,
        "trial_count": len(first["stable_trials"]),
        "prediction_identical": True,
        "accuracy_identical": True,
        "timeout_identical": True,
        "score_identical": first["total_score"] == second["total_score"],
        "timing_mismatch_count": timing_mismatch_count,
        "first": {
            key: value
            for key, value in first.items()
            if key not in {"stable_trials", "predict_time_ms"}
        },
        "second": {
            key: value
            for key, value in second.items()
            if key not in {"stable_trials", "predict_time_ms"}
        },
    }
    (artifact_root / "repeatability_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
