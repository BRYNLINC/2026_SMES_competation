from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.runtime_state_assertions import (
    assert_json_state_keys_exist,
    assert_runtime_scoreboard_consistent,
    assert_team_trial_sequence,
    summarize_runtime_state,
)
from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("runtime_state_assertions")]


@pytest.mark.test_id("RSS-ASSERT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("runtime_state_assertions 必须校验 scoreboard 队伍集合和排名稳定性")
@pytest.mark.tested(file="tests/helpers/runtime_state_assertions.py", function="assert_runtime_scoreboard_consistent")
def test_runtime_state_assertions_validate_scoreboard_team_set_and_rank(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.write_team_score_overview_row(db_path, {"team_id": "team_b", "total_score": 5})
    rss.write_team_score_overview_row(db_path, {"team_id": "team_a", "total_score": 10})

    assert_runtime_scoreboard_consistent(db_path, ["team_a", "team_b"])


@pytest.mark.test_id("RSS-ASSERT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("runtime_state_assertions 必须校验某队 trial 顺序，避免 task 切换或恢复后 trial 错乱")
@pytest.mark.tested(file="tests/helpers/runtime_state_assertions.py", function="assert_team_trial_sequence")
def test_runtime_state_assertions_validate_team_trial_sequence(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.replace_team_trial_record_rows(
        db_path,
        "team_0",
        [
            {"team_id": "team_0", "team_trial_index": 1, "trial_id": "1"},
            {"team_id": "team_0", "team_trial_index": 2, "trial_id": "2"},
        ],
    )

    assert_team_trial_sequence(db_path, "team_0", ["1", "2"])


@pytest.mark.test_id("RSS-ASSERT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("runtime_state_assertions 必须校验关键 json_state 键存在，供恢复和 dashboard 状态测试复用")
@pytest.mark.tested(file="tests/helpers/runtime_state_assertions.py", function="assert_json_state_keys_exist/summarize_runtime_state")
def test_runtime_state_assertions_validate_json_state_keys_and_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.write_json_state(db_path, rss.STATE_KEY_RUNTIME_STAGE_STATUS, {"status": "running"})
    rss.write_json_state(db_path, rss.STATE_KEY_MATCH_CONTROL_STATUS, {"status": "started"})
    rss.write_json_state(db_path, f"{rss.TEAM_STATE_KEY_PREFIX}team_0", {"connection_status": "connected"})

    assert_json_state_keys_exist(
        db_path,
        [rss.STATE_KEY_RUNTIME_STAGE_STATUS, rss.STATE_KEY_MATCH_CONTROL_STATUS, f"{rss.TEAM_STATE_KEY_PREFIX}team_0"],
    )
    assert summarize_runtime_state(db_path) == {
        "scoreboard_team_count": 0,
        "runtime_stage_state_exists": True,
        "match_control_state_exists": True,
        "team_state_count": 1,
    }
