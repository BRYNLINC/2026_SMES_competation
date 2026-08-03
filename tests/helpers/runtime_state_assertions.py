from __future__ import annotations

from pathlib import Path
from typing import Any

from tools import runtime_state_sqlite as rss


def load_runtime_scoreboard(db_path: str | Path) -> list[dict[str, Any]]:
    return rss.load_team_score_overview_rows(Path(db_path))


def assert_runtime_scoreboard_consistent(db_path: str | Path, expected_team_ids: list[str]) -> None:
    row_list = load_runtime_scoreboard(db_path)
    observed_team_ids = [str(row.get("team_id")) for row in row_list]
    if sorted(observed_team_ids) != sorted(expected_team_ids):
        raise AssertionError(f"scoreboard team ids mismatch: observed={observed_team_ids}, expected={expected_team_ids}")
    normalized_rank = [
        (-float(row.get("total_score") or 0), str(row.get("team_id") or ""))
        for row in row_list
    ]
    if normalized_rank != sorted(normalized_rank):
        raise AssertionError("runtime scoreboard is not sorted by total_score desc, team_id asc")


def assert_team_trial_sequence(db_path: str | Path, team_id: str, expected_trial_ids: list[str]) -> None:
    row_list = rss.load_team_trial_record_rows(Path(db_path), team_id)
    observed_trial_ids = [str(row.get("trial_id")) for row in row_list]
    if observed_trial_ids != [str(trial_id) for trial_id in expected_trial_ids]:
        raise AssertionError(f"trial sequence mismatch for {team_id}: observed={observed_trial_ids}, expected={expected_trial_ids}")


def assert_json_state_keys_exist(db_path: str | Path, state_key_list: list[str]) -> None:
    missing_keys = [
        state_key
        for state_key in state_key_list
        if not rss.json_state_exists(Path(db_path), state_key)
    ]
    if missing_keys:
        raise AssertionError(f"missing json_state keys: {missing_keys}")


def summarize_runtime_state(db_path: str | Path) -> dict[str, Any]:
    path = Path(db_path)
    return {
        "scoreboard_team_count": len(rss.load_team_score_overview_rows(path)),
        "runtime_stage_state_exists": rss.json_state_exists(path, rss.STATE_KEY_RUNTIME_STAGE_STATUS),
        "match_control_state_exists": rss.json_state_exists(path, rss.STATE_KEY_MATCH_CONTROL_STATUS),
        "team_state_count": rss.count_json_state_by_prefix(path, rss.TEAM_STATE_KEY_PREFIX),
    }
