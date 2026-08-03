from __future__ import annotations

import pytest

from tests.helpers.result_assertions import assert_scoreboard_sorted, assert_timeout_trial_row
from tests.helpers.runtime_state_assertions import (
    assert_json_state_keys_exist,
    assert_runtime_scoreboard_consistent,
    assert_team_trial_sequence,
)
from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("result_runtime_assertions")]


@pytest.mark.test_id("COMP-ASSERT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("结果断言 helper 与 runtime_state helper 必须能联合验证比赛结束后 scoreboard、team state、trial timeout 的一致性")
@pytest.mark.tested(
    file="tests/helpers/result_assertions.py;tests/helpers/runtime_state_assertions.py;tools/runtime_state_sqlite.py",
    function="assert_scoreboard_sorted/assert_timeout_trial_row/assert_runtime_scoreboard_consistent/assert_team_trial_sequence/assert_json_state_keys_exist",
)
def test_result_and_runtime_assertion_helpers_validate_finished_match_consistency(tmp_path) -> None:
    db_path = tmp_path / "runtime_state.db"
    scoreboard_rows = [
        {"team_id": "team_0", "total_score": 10, "run_status": "finished", "observed_trial_count": 2},
        {"team_id": "team_1", "total_score": 5, "run_status": "finished", "observed_trial_count": 2},
    ]
    for row in scoreboard_rows:
        rss.write_team_score_overview_row(db_path, row)
    rss.replace_team_trial_record_rows(
        db_path,
        "team_1",
        [
            {
                "team_id": "team_1",
                "team_trial_index": 1,
                "trial_id": "1",
                "task_id": "vme_left_vs_rest",
                "subject_id": "S1",
                "is_timeout": True,
                "predict_time_ms": 1000,
                "cumulative_score": 0,
            },
            {
                "team_id": "team_1",
                "team_trial_index": 2,
                "trial_id": "2",
                "task_id": "vme_left_vs_rest",
                "subject_id": "S1",
                "is_timeout": False,
                "predict_time_ms": 200,
                "cumulative_score": 5,
            },
        ],
    )
    rss.write_json_state(db_path, rss.STATE_KEY_MATCH_CONTROL_STATUS, {"status": "finished"})
    rss.write_json_state(db_path, f"{rss.TEAM_STATE_KEY_PREFIX}team_1", {"connection_status": "disconnected"})

    assert_scoreboard_sorted(scoreboard_rows)
    assert_runtime_scoreboard_consistent(db_path, ["team_0", "team_1"])
    assert_team_trial_sequence(db_path, "team_1", ["1", "2"])
    assert_json_state_keys_exist(db_path, [rss.STATE_KEY_MATCH_CONTROL_STATUS, f"{rss.TEAM_STATE_KEY_PREFIX}team_1"])
    assert_timeout_trial_row(rss.load_team_trial_record_rows(db_path, "team_1")[0])
