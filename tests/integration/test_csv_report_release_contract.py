from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.report_writer import REPORT_FIELDNAME_LIST, SUMMARY_FIELDNAME_LIST


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LATEST_REPORT = PROJECT_ROOT / "tests" / "artifacts" / "latest" / "test_report.csv"
LATEST_SUMMARY = PROJECT_ROOT / "tests" / "artifacts" / "latest" / "test_summary.csv"


pytestmark = [pytest.mark.integration, pytest.mark.layer("integration"), pytest.mark.category("csv_report_release_contract")]


@pytest.mark.test_id("INT-CSV-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("测试结束后必须输出 latest/test_report.csv 与 latest/test_summary.csv，供发布门禁与现场追溯使用")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="pytest_sessionfinish")
def test_latest_csv_report_targets_are_defined() -> None:
    assert LATEST_REPORT.parent.name == "latest"
    assert LATEST_SUMMARY.parent.name == "latest"
    assert LATEST_REPORT.name == "test_report.csv"
    assert LATEST_SUMMARY.name == "test_summary.csv"


@pytest.mark.test_id("INT-CSV-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("测试报告表头必须覆盖 test_id/layer/category/priority/status/expected_result 等发布判定字段")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportWriter.write_report")
def test_csv_report_contains_release_gate_columns() -> None:
    for field_name in (
        "test_id",
        "layer",
        "category",
        "priority",
        "status",
        "expected_result",
        "actual_result",
        "artifact_dir",
        "environment",
    ):
        assert field_name in REPORT_FIELDNAME_LIST


@pytest.mark.test_id("INT-CSV-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("测试汇总表必须覆盖 release_gate 与 blocking_failure_test_ids，供决赛发布前快速判断阻断项")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportWriter.write_summary")
def test_csv_summary_contains_release_gate_columns() -> None:
    for field_name in ("group_by", "group_value", "release_gate", "blocking_failure_count", "blocking_failure_test_ids"):
        assert field_name in SUMMARY_FIELDNAME_LIST
