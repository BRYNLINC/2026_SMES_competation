from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
import yaml

from tools import recovery_runtime as rr
from tools import results_snapshot as snapshots
from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("recovery_results")]


def _write_csv(file_path: Path, fieldnames: list[str], row_list: list[dict]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(row_list)


def _make_project_root(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    (project_root / "results" / "control").mkdir(parents=True, exist_ok=True)
    (project_root / "results" / "live").mkdir(parents=True, exist_ok=True)

    vr_path = rr.resolve_virtual_receiver_config_path(project_root)
    vr_path.parent.mkdir(parents=True, exist_ok=True)
    vr_path.write_text(
        yaml.safe_dump(
            {
                "device_info": {"other_information": {"exp_task_order": ["left_vs_rest", "right_vs_rest"]}},
                "data_files": {
                    "S1": {
                        "vme": [
                            "data/S1/session1/sub_S1_vme_run1.dat",
                            "data/S1/session2/sub_S1_vme_run1.dat",
                        ],
                        "vmi": ["data/S1/session2/sub_S1_vmi_run1.dat"],
                    }
                },
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    challenge_path = rr.resolve_mi_challenge_config_path(project_root)
    challenge_path.parent.mkdir(parents=True, exist_ok=True)
    challenge_path.write_text(
        yaml.safe_dump(
            {
                "score_config": {
                    "task_baseline_score": {
                        "vme_left_vs_rest": 0.0,
                        "vme_right_vs_rest": 0.0,
                        "vmi_left_vs_rest": 0.0,
                        "vmi_right_vs_rest": 0.0,
                    }
                }
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return project_root


def _trial_row(
    *,
    team_id: str,
    team_trial_index: int,
    task_trial_index: int,
    task_id: str,
    subject_id: str,
    exp_name: str,
    exp_task: str,
    session_id: str,
    block_id: str,
    trial_id: str,
    is_correct: bool,
    trial_score: float,
    predict_time_ms: float,
    cumulative_accuracy_percent: float,
    cumulative_score: float,
) -> dict:
    return {
        "team_id": team_id,
        "team_trial_index": team_trial_index,
        "task_trial_index": task_trial_index,
        "subject_id": subject_id,
        "task_id": task_id,
        "exp_name": exp_name,
        "exp_task": exp_task,
        "session_id": session_id,
        "block_id": block_id,
        "trial_id": trial_id,
        "true_label": "1",
        "predict_label": "1" if is_correct else "0",
        "is_correct": is_correct,
        "trial_score": trial_score,
        "is_timeout": False,
        "predict_time_ms": predict_time_ms,
        "cumulative_accuracy_percent": cumulative_accuracy_percent,
        "cumulative_score": cumulative_score,
        "report_position": f"{exp_name}_{trial_id}",
    }


@pytest.mark.test_id("COMP-REC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("restart_from_stage 后 trial 记录、task 概览、team 概览和 runtime_state.db 必须同步裁切到目标阶段之前")
@pytest.mark.tested(
    file="tools/recovery_runtime.py;tools/runtime_state_sqlite.py",
    function="apply_restart_from_stage/_rewrite_team_result_dir/load_team_trial_record_rows",
)
def test_apply_restart_from_stage_rebuilds_result_views_and_runtime_state_consistently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _make_project_root(tmp_path)
    results_root = project_root / "results"
    team_dir = results_root / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)

    row_list = [
        _trial_row(
            team_id="team_1",
            team_trial_index=1,
            task_trial_index=1,
            task_id="vme_left_vs_rest",
            subject_id="S1",
            exp_name="vme",
            exp_task="left_vs_rest",
            session_id="session1",
            block_id="1",
            trial_id="1",
            is_correct=True,
            trial_score=10.0,
            predict_time_ms=100.0,
            cumulative_accuracy_percent=100.0,
            cumulative_score=10.0,
        ),
        _trial_row(
            team_id="team_1",
            team_trial_index=2,
            task_trial_index=2,
            task_id="vme_left_vs_rest",
            subject_id="S1",
            exp_name="vme",
            exp_task="left_vs_rest",
            session_id="session1",
            block_id="1",
            trial_id="2",
            is_correct=False,
            trial_score=0.0,
            predict_time_ms=200.0,
            cumulative_accuracy_percent=50.0,
            cumulative_score=10.0,
        ),
        _trial_row(
            team_id="team_1",
            team_trial_index=3,
            task_trial_index=1,
            task_id="vme_right_vs_rest",
            subject_id="S1",
            exp_name="vme",
            exp_task="right_vs_rest",
            session_id="session1",
            block_id="2",
            trial_id="1",
            is_correct=True,
            trial_score=15.0,
            predict_time_ms=300.0,
            cumulative_accuracy_percent=100.0,
            cumulative_score=15.0,
        ),
        _trial_row(
            team_id="team_1",
            team_trial_index=4,
            task_trial_index=1,
            task_id="vmi_left_vs_rest",
            subject_id="S1",
            exp_name="vmi",
            exp_task="left_vs_rest",
            session_id="session2",
            block_id="5",
            trial_id="1",
            is_correct=True,
            trial_score=20.0,
            predict_time_ms=400.0,
            cumulative_accuracy_percent=100.0,
            cumulative_score=20.0,
        ),
    ]
    _write_csv(team_dir / "03_trial_records.csv", rr.TRIAL_RECORD_FIELDNAMES, row_list)

    monkeypatch.setattr(rr, "archive_results_snapshot", lambda project_root_arg, archive_reason: {"archive_reason": archive_reason})

    result = rr.apply_restart_from_stage(
        project_root,
        {
            "subject_id": "S1",
            "exp_name": "vmi",
            "exp_task": "left_vs_rest",
            "session_id": "session2",
        },
    )

    runtime_state_db_path = rss.resolve_runtime_state_db_path(project_root)
    rebuilt_trial_rows = rss.load_team_trial_record_rows(runtime_state_db_path, "team_1")
    rebuilt_task_rows = rss.load_team_task_overview_rows(runtime_state_db_path, "team_1")
    rebuilt_subject_rows = rss.load_team_subject_task_overview_rows(runtime_state_db_path, "team_1")
    rebuilt_team_row = rss.load_team_overview_row(runtime_state_db_path, "team_1")
    scoreboard_rows = rss.load_team_score_overview_rows(runtime_state_db_path)

    assert result["collector_start_selector"] == {
        "subject_id": "S1",
        "exp_name": "vmi",
        "exp_task": "left_vs_rest",
        "task_id": "vmi_left_vs_rest",
        "session_id": "session2",
        "block_id": 5,
    }
    assert [row["team_trial_index"] for row in rebuilt_trial_rows] == [1, 2, 3]
    assert [row["task_trial_index"] for row in rebuilt_trial_rows] == [1, 2, 1]
    assert [row["task_id"] for row in rebuilt_trial_rows] == [
        "vme_left_vs_rest",
        "vme_left_vs_rest",
        "vme_right_vs_rest",
    ]
    assert [row["task_id"] for row in rebuilt_task_rows] == [
        "vme_left_vs_rest",
        "vme_right_vs_rest",
        "vmi_left_vs_rest",
        "vmi_right_vs_rest",
    ]
    assert rebuilt_task_rows[0]["observed_trial_count"] == 2
    assert rebuilt_task_rows[0]["accuracy_percent"] == 50.0
    assert rebuilt_task_rows[1]["task_score"] == 15.0
    assert rebuilt_task_rows[2]["task_status"] == "not_started"
    assert rebuilt_subject_rows == [
        {
            "team_id": "team_1",
            "subject_id": "S1",
            "task_id": "vme_left_vs_rest",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "task_status": "running",
            "updated_at": rebuilt_subject_rows[0]["updated_at"],
            "observed_trial_count": 2,
            "accuracy_percent": 50.0,
        },
        {
            "team_id": "team_1",
            "subject_id": "S1",
            "task_id": "vme_right_vs_rest",
            "exp_name": "vme",
            "exp_task": "right_vs_rest",
            "task_status": "running",
            "updated_at": rebuilt_subject_rows[1]["updated_at"],
            "observed_trial_count": 1,
            "accuracy_percent": 100.0,
        },
    ]
    assert rebuilt_team_row is not None
    assert rebuilt_team_row["observed_trial_count"] == 3
    assert rebuilt_team_row["started_task_count"] == 2
    assert rebuilt_team_row["started_task_names"] == "vme_left_vs_rest|vme_right_vs_rest"
    assert rebuilt_team_row["total_score"] == 12.5
    assert scoreboard_rows[0]["team_id"] == "team_1"


@pytest.mark.test_id("COMP-REC-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("单队重建后 task_trials 目录只应保留目标阶段之前的任务明细文件")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="_rewrite_team_result_dir",
)
def test_rewrite_team_result_dir_recreates_task_trial_files_for_remaining_tasks(tmp_path: Path) -> None:
    team_dir = tmp_path / "results" / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)
    runtime_state_db_path = tmp_path / "results" / "runtime_state.db"

    _write_csv(
        team_dir / "03_trial_records.csv",
        rr.TRIAL_RECORD_FIELDNAMES,
        [
            _trial_row(
                team_id="team_1",
                team_trial_index=1,
                task_trial_index=1,
                task_id="vme_left_vs_rest",
                subject_id="S1",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session1",
                block_id="1",
                trial_id="1",
                is_correct=True,
                trial_score=10.0,
                predict_time_ms=100.0,
                cumulative_accuracy_percent=100.0,
                cumulative_score=10.0,
            ),
            _trial_row(
                team_id="team_1",
                team_trial_index=2,
                task_trial_index=1,
                task_id="vmi_left_vs_rest",
                subject_id="S1",
                exp_name="vmi",
                exp_task="left_vs_rest",
                session_id="session2",
                block_id="2",
                trial_id="1",
                is_correct=False,
                trial_score=0.0,
                predict_time_ms=300.0,
                cumulative_accuracy_percent=0.0,
                cumulative_score=0.0,
            ),
        ],
    )
    (team_dir / "task_trials").mkdir(parents=True, exist_ok=True)
    (team_dir / "task_trials" / "obsolete.csv").write_text("obsolete", encoding="utf-8")

    summary = rr._rewrite_team_result_dir(
        team_dir=team_dir,
        target_stage_index=1,
        stage_index_by_checkpoint_id={
            "S1|vme|left_vs_rest|session1": 0,
            "S1|vmi|left_vs_rest|session2": 1,
        },
        configured_task_id_list=["vme_left_vs_rest", "vmi_left_vs_rest"],
        runtime_state_db_path=runtime_state_db_path,
    )

    task_trial_dir = team_dir / "task_trials"
    assert summary == {
        "team_id": "team_1",
        "observed_trial_count": 1,
        "started_task_count": 1,
        "target_stage_index": 1,
    }
    assert not (task_trial_dir / "obsolete.csv").exists()
    assert (task_trial_dir / "vme_left_vs_rest_trial_records.csv").exists()
    assert not (task_trial_dir / "vmi_left_vs_rest_trial_records.csv").exists()


@pytest.mark.test_id("COMP-REC-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("restart_from_stage 完成后必须导出总榜 CSV，内容与 runtime_state.db 中 team_score_overview 保持一致")
@pytest.mark.tested(
    file="tools/recovery_runtime.py;tools/runtime_state_sqlite.py",
    function="apply_restart_from_stage/export_team_score_overview_csv",
)
def test_apply_restart_from_stage_exports_scoreboard_csv_from_runtime_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _make_project_root(tmp_path)
    team_dir = project_root / "results" / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        team_dir / "03_trial_records.csv",
        rr.TRIAL_RECORD_FIELDNAMES,
        [
            _trial_row(
                team_id="team_1",
                team_trial_index=1,
                task_trial_index=1,
                task_id="vme_left_vs_rest",
                subject_id="S1",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session1",
                block_id="1",
                trial_id="1",
                is_correct=True,
                trial_score=10.0,
                predict_time_ms=123.0,
                cumulative_accuracy_percent=100.0,
                cumulative_score=10.0,
            )
        ],
    )
    monkeypatch.setattr(rr, "archive_results_snapshot", lambda project_root_arg, archive_reason: {"archive_reason": archive_reason})

    rr.apply_restart_from_stage(
        project_root,
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "right_vs_rest",
            "session_id": "session1",
        },
    )

    csv_path = project_root / "results" / "00_team_score_overview.csv"
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        row_list = list(csv.DictReader(file))

    runtime_state_rows = rss.load_team_score_overview_rows(rss.resolve_runtime_state_db_path(project_root))
    assert len(row_list) == 1
    assert row_list[0]["team_id"] == runtime_state_rows[0]["team_id"] == "team_1"
    assert float(row_list[0]["total_score"]) == runtime_state_rows[0]["total_score"] == 10.0


@pytest.mark.test_id("COMP-REC-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("restart_from_stage 完成后必须写出已应用恢复 manifest，供 ProcessHub 恢复历史 trial 状态")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="apply_restart_from_stage",
)
def test_apply_restart_from_stage_writes_applied_recovery_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _make_project_root(tmp_path)
    team_dir = project_root / "results" / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        team_dir / "03_trial_records.csv",
        rr.TRIAL_RECORD_FIELDNAMES,
        [
            _trial_row(
                team_id="team_1",
                team_trial_index=1,
                task_trial_index=1,
                task_id="vme_left_vs_rest",
                subject_id="S1",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session1",
                block_id="1",
                trial_id="1",
                is_correct=True,
                trial_score=10.0,
                predict_time_ms=123.0,
                cumulative_accuracy_percent=100.0,
                cumulative_score=10.0,
            )
        ],
    )
    monkeypatch.setattr(rr, "archive_results_snapshot", lambda project_root_arg, archive_reason: {"archive_reason": archive_reason})

    result = rr.apply_restart_from_stage(
        project_root,
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "right_vs_rest",
            "session_id": "session1",
        },
    )

    manifest_path = project_root / "results" / "control" / "applied_recovery.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["recovery_mode"] == "restart_from_stage"
    assert manifest["stage"] == {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "right_vs_rest",
        "session_id": "session1",
    }
    assert manifest["collector_start_selector"] == result["collector_start_selector"]
    assert manifest["team_summary_list"] == result["team_summary_list"]
    assert isinstance(manifest["applied_at"], float)


@pytest.mark.test_id("COMP-REC-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("从 session2 重跑时必须保留相同 subject/exp/task 的 session1 结果")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="_rewrite_team_result_dir")
def test_rewrite_team_result_dir_distinguishes_same_task_across_sessions(tmp_path: Path) -> None:
    team_dir = tmp_path / "results" / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)
    runtime_state_db_path = tmp_path / "results" / "runtime_state.db"

    _write_csv(
        team_dir / "03_trial_records.csv",
        rr.TRIAL_RECORD_FIELDNAMES,
        [
            _trial_row(
                team_id="team_1",
                team_trial_index=1,
                task_trial_index=1,
                task_id="vme_left_vs_rest",
                subject_id="S1",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session1",
                block_id="1",
                trial_id="1",
                is_correct=True,
                trial_score=10.0,
                predict_time_ms=100.0,
                cumulative_accuracy_percent=100.0,
                cumulative_score=10.0,
            ),
            _trial_row(
                team_id="team_1",
                team_trial_index=2,
                task_trial_index=2,
                task_id="vme_left_vs_rest",
                subject_id="S1",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session2",
                block_id="3",
                trial_id="1",
                is_correct=False,
                trial_score=0.0,
                predict_time_ms=200.0,
                cumulative_accuracy_percent=50.0,
                cumulative_score=10.0,
            ),
        ],
    )

    rr._rewrite_team_result_dir(
        team_dir=team_dir,
        target_stage_index=2,
        stage_index_by_checkpoint_id={
            "S1|vme|left_vs_rest|session1": 0,
            "S1|vme|right_vs_rest|session1": 1,
            "S1|vme|left_vs_rest|session2": 2,
        },
        configured_task_id_list=["vme_left_vs_rest", "vme_right_vs_rest"],
        runtime_state_db_path=runtime_state_db_path,
    )

    rebuilt_rows = rss.load_team_trial_record_rows(runtime_state_db_path, "team_1")
    assert [(row["session_id"], row["trial_id"]) for row in rebuilt_rows] == [("session1", "1")]


@pytest.mark.test_id("COMP-REC-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("指定阶段重赛必须兼容并保留正式结果中的算法输出判罚字段")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="_rewrite_team_result_dir")
def test_rewrite_team_result_dir_preserves_current_trial_result_schema(tmp_path: Path) -> None:
    team_dir = tmp_path / "results" / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)
    runtime_state_db_path = tmp_path / "results" / "runtime_state.db"
    trial_row = _trial_row(
        team_id="team_1",
        team_trial_index=1,
        task_trial_index=1,
        task_id="vme_left_vs_rest",
        subject_id="S1",
        exp_name="vme",
        exp_task="left_vs_rest",
        session_id="session1",
        block_id="1",
        trial_id="1",
        is_correct=False,
        trial_score=0.0,
        predict_time_ms=100.0,
        cumulative_accuracy_percent=0.0,
        cumulative_score=0.0,
    )
    trial_row.update(
        {
            "raw_predict_label": "bad",
            "predict_label": "bad",
            "is_invalid_output": True,
            "judge_message": "算法输出超出允许范围",
        }
    )
    _write_csv(
        team_dir / "00_team_overview.csv",
        snapshots.TEAM_FIELDS,
        [
            {
                "team_id": "team_1",
                "global_seed": 2026,
                "collector_session_shuffle_seed": "S1|vme|left_vs_rest|session1:123",
            }
        ],
    )
    _write_csv(team_dir / "03_trial_records.csv", snapshots.TRIAL_FIELDS, [trial_row])

    rr._rewrite_team_result_dir(
        team_dir=team_dir,
        target_stage_index=1,
        stage_index_by_checkpoint_id={
            "S1|vme|left_vs_rest|session1": 0,
            "S1|vme|right_vs_rest|session1": 1,
        },
        configured_task_id_list=["vme_left_vs_rest", "vme_right_vs_rest"],
        runtime_state_db_path=runtime_state_db_path,
    )

    assert rr.TEAM_OVERVIEW_FIELDNAMES == snapshots.TEAM_FIELDS
    assert rr.TASK_OVERVIEW_FIELDNAMES == snapshots.TASK_FIELDS
    assert rr.SUBJECT_TASK_OVERVIEW_FIELDNAMES == snapshots.SUBJECT_TASK_FIELDS
    assert rr.TRIAL_RECORD_FIELDNAMES == snapshots.TRIAL_FIELDS
    with (team_dir / "00_team_overview.csv").open("r", encoding="utf-8-sig", newline="") as file:
        team_reader = csv.DictReader(file)
        rebuilt_team_row = next(team_reader)
    assert team_reader.fieldnames == snapshots.TEAM_FIELDS
    assert rebuilt_team_row["global_seed"] == "2026"
    assert rebuilt_team_row["collector_session_shuffle_seed"] == "S1|vme|left_vs_rest|session1:123"
    database_team_row = rss.load_team_overview_row(runtime_state_db_path, "team_1")
    assert database_team_row is not None
    assert database_team_row["global_seed"] == "2026"
    assert database_team_row["collector_session_shuffle_seed"] == "S1|vme|left_vs_rest|session1:123"
    for result_file_path in (
        team_dir / "03_trial_records.csv",
        team_dir / "task_trials" / "vme_left_vs_rest_trial_records.csv",
    ):
        with result_file_path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            rebuilt_row_list = list(reader)
        assert reader.fieldnames == snapshots.TRIAL_FIELDS
        assert rebuilt_row_list[0]["raw_predict_label"] == "bad"
        assert rebuilt_row_list[0]["is_invalid_output"] == "True"
        assert rebuilt_row_list[0]["judge_message"] == "算法输出超出允许范围"
