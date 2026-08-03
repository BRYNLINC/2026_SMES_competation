from __future__ import annotations

import csv
import json
import os
import sqlite3
import time
from pathlib import Path

import pytest

from tools import results_archive as ra
from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("results_archive")]


def _make_project_root(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    (project_root / "results").mkdir(parents=True, exist_ok=True)
    return project_root


@pytest.mark.test_id("ARCH-01")
@pytest.mark.priority("P1")
@pytest.mark.requirement("归档名需要安全且包含 reason")
@pytest.mark.tested(file="tools/results_archive.py", function="_build_archive_name")
def test_build_archive_name_normalizes_reason() -> None:
    archive_name = ra._build_archive_name("bad reason/with spaces")  # type: ignore[attr-defined]
    assert "bad_reason_with_spaces" in archive_name
    assert "/" not in archive_name


@pytest.mark.test_id("ARCH-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("active results payload 判断准确")
@pytest.mark.tested(file="tools/results_archive.py", function="has_active_results_payload")
def test_has_active_results_payload_detects_runtime_db_and_live_dir(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    assert ra.has_active_results_payload(project_root) is False

    runtime_db = project_root / "results" / "runtime_state.db"
    runtime_db.write_text("db", encoding="utf-8")
    assert ra.has_active_results_payload(project_root) is True


@pytest.mark.test_id("ARCH-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("归档快照会复制 active results 且保留 manifest")
@pytest.mark.tested(file="tools/results_archive.py", function="archive_results_snapshot")
def test_archive_results_snapshot_copies_active_payload(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    live_dir = project_root / "results" / "live"
    control_dir = project_root / "results" / "control"
    team_dir = project_root / "results" / "team_0"
    live_dir.mkdir()
    control_dir.mkdir()
    team_dir.mkdir()
    current_trial_path = live_dir / "current_trial.json"
    current_trial_path.write_text("{}", encoding="utf-8")
    (control_dir / "request.json").write_text("{}", encoding="utf-8")
    source_modified_at = time.time() - 3600.0
    os.utime(current_trial_path, (source_modified_at, source_modified_at))

    runtime_db = project_root / "results" / "runtime_state.db"
    team_row = {
        "team_id": "team_0",
        "total_score": 88.5,
        "run_status": "running",
        "updated_at": "2026-07-16T12:00:00+08:00",
    }
    rss.write_team_score_overview_row(runtime_db, team_row)
    rss.write_team_overview_row(runtime_db, team_row)
    (team_dir / "00_team_overview.csv").write_text(
        "team_id,total_score\nteam_0,1.0\n",
        encoding="utf-8-sig",
    )

    manifest = ra.archive_results_snapshot(project_root, "manual")
    assert manifest is not None
    archive_root = Path(manifest["archive_root"])
    assert (archive_root / "results" / "live" / "current_trial.json").exists()
    assert (archive_root / "manifest.json").exists()
    assert (archive_root / "results" / "00_team_score_overview.csv").exists()
    assert not (archive_root / "results" / "runtime_state.db-wal").exists()
    assert not (archive_root / "results" / "runtime_state.db-shm").exists()

    with sqlite3.connect(archive_root / "results" / "runtime_state.db") as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with (archive_root / "results" / "00_team_score_overview.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as file:
        scoreboard_rows = list(csv.DictReader(file))
    assert [row["team_id"] for row in scoreboard_rows] == ["team_0"]
    assert scoreboard_rows[0]["total_score"] == "88.5"

    stored_manifest = json.loads((archive_root / "manifest.json").read_text(encoding="utf-8"))
    assert stored_manifest["archive_reason"] == "manual"
    assert stored_manifest["archive_schema_version"] == 2
    assert stored_manifest["integrity"]["status"] == "ok"
    inventory_by_path = {
        item["path"]: item for item in stored_manifest["file_inventory"]
    }
    current_trial_inventory = inventory_by_path["live/current_trial.json"]
    assert current_trial_inventory["source_modified_at_ns"] == current_trial_path.stat().st_mtime_ns
    archived_mtime_ns = (archive_root / "results" / "live" / "current_trial.json").stat().st_mtime_ns
    assert archived_mtime_ns == stored_manifest["archive_payload_modified_at_ns"]
    assert ra.verify_archive_manifest(archive_root)["status"] == "ok"


@pytest.mark.test_id("ARCH-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("清理 active results 时保留 history")
@pytest.mark.tested(file="tools/results_archive.py", function="clear_active_results_root")
def test_clear_active_results_root_preserves_history(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    results_root = project_root / "results"
    (results_root / "live").mkdir()
    (results_root / "control").mkdir()
    (results_root / "history").mkdir()
    (results_root / "runtime_state.db").write_text("db", encoding="utf-8")
    (results_root / "live" / "file.txt").write_text("x", encoding="utf-8")
    (results_root / "history" / "old.txt").write_text("y", encoding="utf-8")

    ra.clear_active_results_root(project_root)

    assert (results_root / "history" / "old.txt").exists()
    assert not (results_root / "live").exists()
    assert not (results_root / "runtime_state.db").exists()


@pytest.mark.test_id("ARCH-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("归档并清理后 active results 被清空")
@pytest.mark.tested(file="tools/results_archive.py", function="archive_and_clear_active_results")
def test_archive_and_clear_active_results(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    (project_root / "results" / "control").mkdir()
    (project_root / "results" / "control" / "request.json").write_text("{}", encoding="utf-8")
    manifest = ra.archive_and_clear_active_results(project_root, "startup_clear")
    assert manifest is not None
    assert not (project_root / "results" / "control").exists()
    assert (project_root / "results" / "history").exists()


@pytest.mark.test_id("ARCH-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("损坏数据库不得生成成功归档或清空活动结果")
@pytest.mark.tested(file="tools/results_snapshot.py", function="archive_snapshot")
def test_corrupt_database_prevents_archive_clear(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    results_root = project_root / "results"
    control_dir = results_root / "control"
    control_dir.mkdir()
    (control_dir / "request.json").write_text("{}", encoding="utf-8")
    runtime_db = results_root / "runtime_state.db"
    runtime_db.write_bytes(b"not-a-sqlite-database")

    with pytest.raises((RuntimeError, sqlite3.DatabaseError)):
        ra.archive_and_clear_active_results(project_root, "corrupt")

    assert runtime_db.exists()
    assert (control_dir / "request.json").exists()


@pytest.mark.test_id("ARCH-07")
@pytest.mark.priority("P0")
@pytest.mark.requirement("归档后的内容或修改时间变化必须被完整性校验识别")
@pytest.mark.tested(file="tools/results_snapshot.py", function="verify_archive")
def test_verify_archive_detects_post_archive_change(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    control_dir = project_root / "results" / "control"
    control_dir.mkdir()
    (control_dir / "request.json").write_text("{}", encoding="utf-8")
    manifest = ra.archive_results_snapshot(project_root, "tamper_check")
    assert manifest is not None
    archive_root = Path(manifest["archive_root"])
    archived_request = archive_root / "results" / "control" / "request.json"

    archived_request.write_text('{"changed": true}', encoding="utf-8")

    verification = ra.verify_archive_manifest(archive_root)
    assert verification["status"] == "failed"
    assert any(
        "SHA-256 mismatch" in issue or "modification time changed" in issue
        for issue in verification["issues"]
    )


@pytest.mark.test_id("ARCH-08")
@pytest.mark.priority("P0")
@pytest.mark.requirement("归档数据库和分任务 CSV 的 task_trial_index 均按任务独立连续编号")
@pytest.mark.tested(file="tools/results_snapshot.py", function="_normalize_task_trial_indices")
def test_archive_normalizes_task_trial_index_per_task(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    runtime_db = project_root / "results" / "runtime_state.db"
    team_row = {"team_id": "team_0", "total_score": 1.0, "run_status": "finished"}
    rss.write_team_score_overview_row(runtime_db, team_row)
    rss.write_team_overview_row(runtime_db, team_row)
    rss.replace_team_trial_record_rows(
        runtime_db,
        "team_0",
        [
            {"team_id": "team_0", "team_trial_index": 1, "task_trial_index": 1, "task_id": "task_a"},
            {"team_id": "team_0", "team_trial_index": 2, "task_trial_index": 2, "task_id": "task_b"},
            {"team_id": "team_0", "team_trial_index": 3, "task_trial_index": 3, "task_id": "task_a"},
        ],
    )

    manifest = ra.archive_results_snapshot(project_root, "normalize_task_index")
    assert manifest is not None
    archive_root = Path(manifest["archive_root"])
    with sqlite3.connect(archive_root / "results" / "runtime_state.db") as connection:
        db_rows = connection.execute(
            "SELECT task_id, task_trial_index FROM trial_record ORDER BY team_trial_index"
        ).fetchall()
    assert db_rows == [("task_a", 1), ("task_b", 1), ("task_a", 2)]
    assert manifest["normalization"]["rewritten_trial_record_count"] == 2
    assert ra.verify_archive_manifest(archive_root)["status"] == "ok"
