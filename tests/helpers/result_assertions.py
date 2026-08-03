from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


REQUIRED_SCOREBOARD_FIELDS = {
    "team_id",
    "total_score",
    "run_status",
    "observed_trial_count",
}
REQUIRED_TRIAL_FIELDS = {
    "team_id",
    "team_trial_index",
    "task_id",
    "subject_id",
    "trial_id",
    "is_timeout",
    "predict_time_ms",
    "cumulative_score",
}


def load_csv_rows(csv_path: str | Path) -> list[dict[str, str]]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def assert_required_fields(row: dict[str, Any], required_fields: set[str]) -> None:
    missing_fields = sorted(field for field in required_fields if field not in row)
    if missing_fields:
        raise AssertionError(f"missing required fields: {missing_fields}")


def assert_scoreboard_sorted(row_list: list[dict[str, Any]]) -> None:
    normalized_rank = [
        (-float(row.get("total_score") or 0), str(row.get("team_id") or ""))
        for row in row_list
    ]
    if normalized_rank != sorted(normalized_rank):
        raise AssertionError("scoreboard rows are not sorted by total_score desc, team_id asc")


def assert_timeout_trial_row(row: dict[str, Any]) -> None:
    assert_required_fields(row, REQUIRED_TRIAL_FIELDS)
    is_timeout_value = str(row.get("is_timeout")).strip().lower()
    if is_timeout_value not in {"true", "1", "yes"}:
        raise AssertionError(f"trial row is not marked timeout: {row}")
    predict_time_ms = float(row.get("predict_time_ms") or 0)
    if predict_time_ms < 0:
        raise AssertionError(f"predict_time_ms must not be negative: {row}")


def summarize_result_rows(row_list: list[dict[str, Any]]) -> dict[str, Any]:
    team_id_set = {str(row.get("team_id") or "") for row in row_list if row.get("team_id")}
    timeout_count = sum(1 for row in row_list if str(row.get("is_timeout")).strip().lower() in {"true", "1", "yes"})
    return {
        "row_count": len(row_list),
        "team_count": len(team_id_set),
        "timeout_count": timeout_count,
    }
