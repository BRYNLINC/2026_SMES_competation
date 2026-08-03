from __future__ import annotations

import csv
from pathlib import Path

import pytest

from tests.helpers.result_assertions import (
    REQUIRED_SCOREBOARD_FIELDS,
    assert_required_fields,
    assert_scoreboard_sorted,
    assert_timeout_trial_row,
    load_csv_rows,
    summarize_result_rows,
)


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("result_assertions")]


@pytest.mark.test_id("RESULT-HELP-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("result_assertions 必须能读取 utf-8-sig CSV 并校验 scoreboard 必备字段")
@pytest.mark.tested(file="tests/helpers/result_assertions.py", function="load_csv_rows/assert_required_fields")
def test_result_assertions_load_csv_rows_and_validate_required_fields(tmp_path: Path) -> None:
    csv_path = tmp_path / "scoreboard.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=sorted(REQUIRED_SCOREBOARD_FIELDS))
        writer.writeheader()
        writer.writerow({"team_id": "team_0", "total_score": "10", "run_status": "finished", "observed_trial_count": "2"})

    row_list = load_csv_rows(csv_path)

    assert row_list == [{"observed_trial_count": "2", "run_status": "finished", "team_id": "team_0", "total_score": "10"}]
    assert_required_fields(row_list[0], REQUIRED_SCOREBOARD_FIELDS)


@pytest.mark.test_id("RESULT-HELP-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("scoreboard 断言必须按 total_score 降序、team_id 升序校验排名稳定性")
@pytest.mark.tested(file="tests/helpers/result_assertions.py", function="assert_scoreboard_sorted")
def test_result_assertions_scoreboard_sorted_detects_valid_rank_order() -> None:
    assert_scoreboard_sorted(
        [
            {"team_id": "team_a", "total_score": 10},
            {"team_id": "team_b", "total_score": 10},
            {"team_id": "team_c", "total_score": 5},
        ]
    )


@pytest.mark.test_id("RESULT-HELP-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("timeout trial 断言必须校验 is_timeout 和 predict_time_ms 非负")
@pytest.mark.tested(file="tests/helpers/result_assertions.py", function="assert_timeout_trial_row")
def test_result_assertions_timeout_trial_row_requires_timeout_marker_and_non_negative_time() -> None:
    assert_timeout_trial_row(
        {
            "team_id": "team_0",
            "team_trial_index": "1",
            "task_id": "vme_left_vs_rest",
            "subject_id": "S1",
            "trial_id": "1",
            "is_timeout": "true",
            "predict_time_ms": "1000",
            "cumulative_score": "0",
        }
    )


@pytest.mark.test_id("RESULT-HELP-04")
@pytest.mark.priority("P2")
@pytest.mark.requirement("result_assertions 摘要必须统计行数、队伍数和 timeout 数，供 CSV 报告附加工件复盘")
@pytest.mark.tested(file="tests/helpers/result_assertions.py", function="summarize_result_rows")
def test_result_assertions_summary_counts_rows_teams_and_timeouts() -> None:
    summary = summarize_result_rows(
        [
            {"team_id": "team_0", "is_timeout": "true"},
            {"team_id": "team_0", "is_timeout": "false"},
            {"team_id": "team_1", "is_timeout": "1"},
        ]
    )

    assert summary == {"row_count": 3, "team_count": 2, "timeout_count": 2}
