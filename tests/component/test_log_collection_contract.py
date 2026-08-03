from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.log_collector import collect_log_file_paths, collect_log_snippets


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("log_collection")]


@pytest.mark.test_id("COMP-LOG-01")
@pytest.mark.priority("P1")
@pytest.mark.requirement("日志采集契约必须能同时收集 JudgeWeb、ProcessHub、results/control 日志，并提取尾部片段用于失败工件")
@pytest.mark.tested(file="tests/helpers/log_collector.py", function="collect_log_file_paths/collect_log_snippets")
def test_log_collection_contract_gathers_multi_component_log_paths_and_tail_snippets(tmp_path: Path) -> None:
    judge_log = tmp_path / "app" / "JudgeWeb" / "JudgeWeb" / "log" / "judgeWeb.log"
    process_log = tmp_path / "app" / "ProcessHub" / "ProcessHub" / "log" / "processHub.log"
    control_log = tmp_path / "results" / "control" / "launcher.log"
    for log_path, payload in (
        (judge_log, "j1\nj2\nj3\n"),
        (process_log, "p1\np2\n"),
        (control_log, "c1\nc2\nc3\nc4\n"),
    ):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(payload, encoding="utf-8")

    log_path_list = collect_log_file_paths(tmp_path)
    snippet_map = collect_log_snippets(tmp_path, max_lines=2)

    assert log_path_list == sorted([judge_log, process_log, control_log])
    assert snippet_map[str(judge_log)] == ["j2", "j3"]
    assert snippet_map[str(process_log)] == ["p1", "p2"]
    assert snippet_map[str(control_log)] == ["c3", "c4"]
