from __future__ import annotations

import csv
from pathlib import Path

import pytest

from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("runtime_state")]


@pytest.mark.test_id("COMP-RSS-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("runtime_state.db 的 scoreboard 导出应保持按分数排序并输出完整字段")
@pytest.mark.tested(
    file="tools/runtime_state_sqlite.py",
    function="write_team_score_overview_row/load_team_score_overview_rows/export_team_score_overview_csv",
)
def test_runtime_state_scoreboard_export_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "results" / "runtime_state.db"
    csv_path = tmp_path / "results" / "00_team_score_overview.csv"
    fieldnames = [
        "team_id",
        "total_score",
        "run_status",
        "updated_at",
        "observed_trial_count",
        "configured_task_count",
        "started_task_count",
        "mean_accuracy_percent",
        "avg_reaction_time_ms",
        "started_task_names",
    ]

    rss.write_team_score_overview_row(
        db_path,
        {
            "team_id": "team_b",
            "total_score": 78.1,
            "run_status": "running",
            "updated_at": "2026-04-23T12:00:00",
            "observed_trial_count": 4,
            "configured_task_count": 2,
            "started_task_count": 2,
            "mean_accuracy_percent": 60.0,
            "avg_reaction_time_ms": 420.0,
            "started_task_names": "vme_left_vs_rest|vmi_right_vs_rest",
        },
    )
    rss.write_team_score_overview_row(
        db_path,
        {
            "team_id": "team_a",
            "total_score": 90.5,
            "run_status": "finished",
            "updated_at": "2026-04-23T12:05:00",
            "observed_trial_count": 8,
            "configured_task_count": 4,
            "started_task_count": 4,
            "mean_accuracy_percent": 88.0,
            "avg_reaction_time_ms": 210.0,
            "started_task_names": "vme_left_vs_rest|vme_right_vs_rest|vmi_left_vs_rest|vmi_right_vs_rest",
        },
    )

    rss.export_team_score_overview_csv(db_path, csv_path, fieldnames)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        row_list = list(csv.DictReader(file))

    assert [row["team_id"] for row in row_list] == ["team_a", "team_b"]
    assert row_list[0]["total_score"] == "90.5"
    assert row_list[0]["run_status"] == "finished"
    assert row_list[1]["started_task_names"] == "vme_left_vs_rest|vmi_right_vs_rest"


@pytest.mark.test_id("COMP-RSS-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("team/task/subject/trial 写入后应能通过 runtime_state.db 形成一致的读取视图")
@pytest.mark.tested(
    file="tools/runtime_state_sqlite.py",
    function="write_team_overview_row/replace_team_task_overview_rows/replace_team_subject_task_overview_rows/replace_team_trial_record_rows",
)
def test_runtime_state_component_views_remain_consistent_after_multi_table_updates(tmp_path: Path) -> None:
    db_path = tmp_path / "results" / "runtime_state.db"

    rss.write_team_overview_row(
        db_path,
        {
            "team_id": "team_0",
            "total_score": 45.0,
            "run_status": "running",
            "updated_at": "2026-04-23T13:00:00",
            "observed_trial_count": 2,
            "configured_task_count": 2,
            "started_task_count": 1,
            "mean_accuracy_percent": 50.0,
            "avg_reaction_time_ms": 300.0,
            "started_task_names": "vme_left_vs_rest",
        },
    )
    rss.replace_team_task_overview_rows(
        db_path,
        "team_0",
        [
            {
                "team_id": "team_0",
                "task_id": "vme_left_vs_rest",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "task_status": "running",
                "updated_at": "2026-04-23T13:00:00",
                "subject_count": 1,
                "observed_trial_count": 2,
                "accuracy_percent": 50.0,
                "avg_reaction_time_ms": 300.0,
                "task_score": 45.0,
            }
        ],
    )
    rss.replace_team_subject_task_overview_rows(
        db_path,
        "team_0",
        [
            {
                "team_id": "team_0",
                "subject_id": "sub_01",
                "task_id": "vme_left_vs_rest",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "task_status": "running",
                "updated_at": "2026-04-23T13:00:00",
                "observed_trial_count": 2,
                "accuracy_percent": 50.0,
            }
        ],
    )
    rss.replace_team_trial_record_rows(
        db_path,
        "team_0",
        [
            {
                "team_id": "team_0",
                "team_trial_index": 1,
                "task_trial_index": 1,
                "task_id": "vme_left_vs_rest",
                "subject_id": "sub_01",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": "session1",
                "block_id": "session1",
                "trial_id": "1",
                "true_label": "1",
                "predict_label": "1",
                "is_correct": True,
                "trial_score": 1.0,
                "predict_time_ms": 250.0,
                "cumulative_accuracy_percent": 100.0,
                "cumulative_score": 30.0,
                "is_timeout": False,
                "report_position": "eeg_1:100.0",
            },
            {
                "team_id": "team_0",
                "team_trial_index": 2,
                "task_trial_index": 2,
                "task_id": "vme_left_vs_rest",
                "subject_id": "sub_01",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": "session1",
                "block_id": "session1",
                "trial_id": "2",
                "true_label": "1",
                "predict_label": "0",
                "is_correct": False,
                "trial_score": 0.0,
                "predict_time_ms": 350.0,
                "cumulative_accuracy_percent": 50.0,
                "cumulative_score": 45.0,
                "is_timeout": False,
                "report_position": "eeg_1:200.0",
            },
        ],
    )

    team_row = rss.load_team_overview_row(db_path, "team_0")
    task_rows = rss.load_team_task_overview_rows(db_path, "team_0")
    subject_rows = rss.load_team_subject_task_overview_rows(db_path, "team_0")
    trial_rows = rss.load_team_trial_record_rows(db_path, "team_0")

    assert team_row is not None
    assert team_row["started_task_names"] == "vme_left_vs_rest"
    assert task_rows == [
        {
            "team_id": "team_0",
            "task_id": "vme_left_vs_rest",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "task_status": "running",
            "updated_at": "2026-04-23T13:00:00",
            "subject_count": 1,
            "observed_trial_count": 2,
            "accuracy_percent": 50.0,
            "avg_reaction_time_ms": 300.0,
            "task_score": 45.0,
        }
    ]
    assert subject_rows == [
        {
            "team_id": "team_0",
            "subject_id": "sub_01",
            "task_id": "vme_left_vs_rest",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "task_status": "running",
            "updated_at": "2026-04-23T13:00:00",
            "observed_trial_count": 2,
            "accuracy_percent": 50.0,
        }
    ]
    assert [row["trial_id"] for row in trial_rows] == ["1", "2"]
    assert trial_rows[-1]["cumulative_score"] == 45.0
