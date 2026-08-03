from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.log_collector import collect_log_file_paths, collect_log_snippets


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("log_collector")]


@pytest.mark.test_id("LOG-HELP-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("log_collector 必须从 JudgeWeb、ProcessHub、results 递归收集日志文件路径")
@pytest.mark.tested(file="tests/helpers/log_collector.py", function="collect_log_file_paths")
def test_log_collector_collects_log_paths_from_expected_roots(tmp_path: Path) -> None:
    judge_log = tmp_path / "app" / "JudgeWeb" / "JudgeWeb" / "log" / "judgeWeb.log"
    process_log = tmp_path / "app" / "ProcessHub" / "ProcessHub" / "log" / "processHub.log"
    result_log = tmp_path / "results" / "control" / "launcher.log"
    for log_path in (judge_log, process_log, result_log):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("line1\nline2\n", encoding="utf-8")

    log_path_list = collect_log_file_paths(tmp_path)

    assert log_path_list == sorted([judge_log, process_log, result_log])


@pytest.mark.test_id("LOG-HELP-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("log_collector 必须支持按最大行数提取日志尾部片段，便于失败工件复盘")
@pytest.mark.tested(file="tests/helpers/log_collector.py", function="collect_log_snippets")
def test_log_collector_collects_tail_snippets_by_line_limit(tmp_path: Path) -> None:
    judge_log = tmp_path / "app" / "JudgeWeb" / "JudgeWeb" / "log" / "judgeWeb.log"
    judge_log.parent.mkdir(parents=True, exist_ok=True)
    judge_log.write_text("a\nb\nc\nd\n", encoding="utf-8")

    snippet_map = collect_log_snippets(tmp_path, max_lines=2)

    assert snippet_map[str(judge_log)] == ["c", "d"]
