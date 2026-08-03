from __future__ import annotations

from pathlib import Path

import pytest

from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("runtime_state")]


@pytest.mark.test_id("RSS-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("运行态 SQLite schema 可重复创建且核心表存在")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="ensure_runtime_state_schema")
def test_ensure_runtime_state_schema_creates_expected_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "results" / "runtime_state.db"
    rss.ensure_runtime_state_schema(db_path)
    rss.ensure_runtime_state_schema(db_path)

    with rss._connect(db_path) as connection:  # type: ignore[attr-defined]
        table_name_set = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    assert {"json_state", "team_score_overview", "team_overview", "task_overview", "subject_task_overview", "trial_record"} <= table_name_set


@pytest.mark.test_id("RSS-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("json_state 写入后可读回")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="write_json_state/read_json_state")
def test_json_state_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    payload = {"trial_id": 3, "updated_at": 123.4, "name": "当前trial"}
    rss.write_json_state(db_path, rss.STATE_KEY_CURRENT_TRIAL, payload)
    assert rss.json_state_exists(db_path, rss.STATE_KEY_CURRENT_TRIAL) is True
    assert rss.read_json_state(db_path, rss.STATE_KEY_CURRENT_TRIAL) == payload


@pytest.mark.test_id("RSS-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("按 prefix 列出与计数 team 状态")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="list_json_state_by_prefix/count_json_state_by_prefix")
def test_list_json_state_by_prefix_and_count(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.write_json_state(db_path, "team:team_0", {"team_id": "team_0", "updated_at": 1})
    rss.write_json_state(db_path, "team:team_1", {"team_id": "team_1", "updated_at": 2})
    rss.write_json_state(db_path, "other:key", {"value": 1, "updated_at": 3})

    team_row_list = rss.list_json_state_by_prefix(db_path, rss.TEAM_STATE_KEY_PREFIX)
    assert [row["team_id"] for row in team_row_list] == ["team_0", "team_1"]
    assert rss.count_json_state_by_prefix(db_path, rss.TEAM_STATE_KEY_PREFIX) == 2


@pytest.mark.test_id("RSS-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("scoreboard 行写入后按分数降序读取")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="write_team_score_overview_row/load_team_score_overview_rows")
def test_team_score_overview_sorted_by_score_then_team_id(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.write_team_score_overview_row(db_path, {"team_id": "team_b", "total_score": 10.0, "updated_at": "t1"})
    rss.write_team_score_overview_row(db_path, {"team_id": "team_a", "total_score": 10.0, "updated_at": "t2"})
    rss.write_team_score_overview_row(db_path, {"team_id": "team_c", "total_score": 8.0, "updated_at": "t3"})
    row_list = rss.load_team_score_overview_rows(db_path)
    assert [row["team_id"] for row in row_list] == ["team_a", "team_b", "team_c"]


@pytest.mark.test_id("RSS-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("单队 task 概览覆盖写会清理旧行")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="replace_team_task_overview_rows/load_team_task_overview_rows")
def test_replace_team_task_overview_rows_replaces_old_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.replace_team_task_overview_rows(
        db_path,
        "team_0",
        [
            {"team_id": "team_0", "task_id": "task_1", "task_score": 1.0},
            {"team_id": "team_0", "task_id": "task_2", "task_score": 2.0},
        ],
    )
    rss.replace_team_task_overview_rows(
        db_path,
        "team_0",
        [
            {"team_id": "team_0", "task_id": "task_2", "task_score": 3.0},
        ],
    )
    row_list = rss.load_team_task_overview_rows(db_path, "team_0")
    assert row_list == [{"team_id": "team_0", "task_id": "task_2", "task_score": 3.0}]


@pytest.mark.test_id("RSS-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("trial record 覆盖写后按 team_trial_index 排序")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="replace_team_trial_record_rows/load_team_trial_record_rows")
def test_replace_team_trial_record_rows_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.replace_team_trial_record_rows(
        db_path,
        "team_0",
        [
            {"team_id": "team_0", "team_trial_index": 2, "trial_id": "2", "updated_at": 2.0},
            {"team_id": "team_0", "team_trial_index": 1, "trial_id": "1", "updated_at": 1.0},
        ],
    )
    row_list = rss.load_team_trial_record_rows(db_path, "team_0")
    assert [row["team_trial_index"] for row in row_list] == [1, 2]
    assert [row["trial_id"] for row in row_list] == ["1", "2"]


@pytest.mark.test_id("RSS-06A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("trial record 增量 upsert 应避免重复并允许后续追加")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="upsert_team_trial_record_rows/load_team_trial_record_rows")
def test_upsert_team_trial_record_rows_appends_without_duplication(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    rss.upsert_team_trial_record_rows(
        db_path,
        "team_0",
        [
            {"team_id": "team_0", "team_trial_index": 1, "trial_id": "1", "updated_at": 1.0},
        ],
    )
    rss.upsert_team_trial_record_rows(
        db_path,
        "team_0",
        [
            {"team_id": "team_0", "team_trial_index": 1, "trial_id": "1", "updated_at": 2.0},
            {"team_id": "team_0", "team_trial_index": 2, "trial_id": "2", "updated_at": 3.0},
        ],
    )
    row_list = rss.load_team_trial_record_rows(db_path, "team_0")
    assert [row["team_trial_index"] for row in row_list] == [1, 2]
    assert [row["trial_id"] for row in row_list] == ["1", "2"]
    assert len(row_list) == 2


@pytest.mark.test_id("RSS-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("导出 scoreboard CSV 可被后续流程读取")
@pytest.mark.tested(file="tools/runtime_state_sqlite.py", function="export_team_score_overview_csv")
def test_export_team_score_overview_csv(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state.db"
    csv_path = tmp_path / "scoreboard.csv"
    rss.write_team_score_overview_row(db_path, {"team_id": "team_0", "total_score": 12.5, "updated_at": "t"})
    rss.export_team_score_overview_csv(db_path, csv_path, ["team_id", "total_score", "updated_at"])
    csv_text = csv_path.read_text(encoding="utf-8-sig")
    assert "team_id,total_score,updated_at" in csv_text
    assert "team_0,12.5,t" in csv_text
