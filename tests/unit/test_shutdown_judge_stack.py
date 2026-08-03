from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import shutdown_judge_stack as sjs


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("shutdown")]


@pytest.mark.test_id("SHUT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("process manifest 写入后可读且包含 metadata")
@pytest.mark.tested(file="tools/shutdown_judge_stack.py", function="write_process_manifest")
def test_write_process_manifest(tmp_path: Path) -> None:
    sjs.write_process_manifest(
        tmp_path,
        [{"pid": 123, "title": "judge"}],
        metadata={"match_start_mode": "clear"},
    )
    manifest_path = tmp_path / "results" / "control" / "judge_process_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["processes"] == [{"pid": 123, "title": "judge"}]
    assert payload["metadata"] == {"match_start_mode": "clear"}


@pytest.mark.test_id("SHUT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("读取损坏 JSON manifest 时返回 None")
@pytest.mark.tested(file="tools/shutdown_judge_stack.py", function="_read_json_file")
def test_read_json_file_returns_none_for_invalid_json(tmp_path: Path) -> None:
    file_path = tmp_path / "bad.json"
    file_path.write_text("{bad", encoding="utf-8")
    assert sjs._read_json_file(file_path) is None  # type: ignore[attr-defined]


@pytest.mark.test_id("SHUT-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("关闭流程最终会删除 manifest 文件")
@pytest.mark.tested(file="tools/shutdown_judge_stack.py", function="shutdown_judge_runtime")
def test_shutdown_judge_runtime_removes_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = tmp_path / "results" / "control" / "judge_process_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(sjs, "_kill_recorded_processes", lambda project_root, quiet: 1)
    monkeypatch.setattr(sjs, "_kill_title_window_processes", lambda quiet: False)
    monkeypatch.setattr(sjs, "_kill_listening_port_owners", lambda port_list, quiet: 0)
    monkeypatch.setattr(sjs, "_wait_for_ports_released", lambda port_list, timeout_seconds, quiet: (True, {}))

    report = sjs.shutdown_judge_runtime(tmp_path, reason="test", quiet=True, timeout_seconds=0.1)
    assert report["clean_shutdown"] is True
    assert not manifest_path.exists()


@pytest.mark.test_id("SHUT-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("关闭流程返回 remaining port 信息")
@pytest.mark.tested(file="tools/shutdown_judge_stack.py", function="shutdown_judge_runtime")
def test_shutdown_judge_runtime_returns_remaining_port_pid_map(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sjs, "_kill_recorded_processes", lambda project_root, quiet: 0)
    monkeypatch.setattr(sjs, "_kill_title_window_processes", lambda quiet: False)
    monkeypatch.setattr(sjs, "_kill_listening_port_owners", lambda port_list, quiet: 2)
    monkeypatch.setattr(sjs, "_wait_for_ports_released", lambda port_list, timeout_seconds, quiet: (False, {18080: [111]}))

    report = sjs.shutdown_judge_runtime(tmp_path, reason="timeout", quiet=True, timeout_seconds=0.1)
    assert report["clean_shutdown"] is False
    assert report["remaining_port_pid_map"] == {18080: [111]}


@pytest.mark.test_id("SHUT-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("resolve process manifest path 指向 results/control")
@pytest.mark.tested(file="tools/shutdown_judge_stack.py", function="_resolve_process_manifest_path")
def test_resolve_process_manifest_path(tmp_path: Path) -> None:
    assert sjs._resolve_process_manifest_path(tmp_path) == tmp_path / "results" / "control" / "judge_process_manifest.json"  # type: ignore[attr-defined]
