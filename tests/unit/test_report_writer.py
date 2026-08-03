from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.helpers.report_writer import (
    CsvReportPlugin,
    CsvReportWriter,
    TestReportRow,
    sanitize_csv_cell,
    windows_extended_length_path,
)
from tests.helpers.pytest_run_lock import PytestRunLock


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("test_report")]


@pytest.mark.test_id("REPORT-00")
@pytest.mark.priority("P0")
@pytest.mark.requirement("Windows artifact cleanup must use extended-length paths")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="windows_extended_length_path")
def test_windows_extended_length_path_adds_prefix(tmp_path: Path) -> None:
    resolved_path = str(tmp_path.resolve())

    assert windows_extended_length_path(tmp_path, platform_name="nt") == "\\\\?\\" + resolved_path
    assert windows_extended_length_path(tmp_path, platform_name="posix") == resolved_path


def _start_report_plugin_holder(project_root: Path) -> subprocess.Popen[str]:
    holder_code = r'''
import sys
from pathlib import Path
from tests.helpers.report_writer import CsvReportPlugin

plugin = CsvReportPlugin(Path(sys.argv[1]))
plugin.pytest_sessionstart(session=None)
print("READY", flush=True)
sys.stdin.readline()
plugin.pytest_sessionfinish(session=None, exitstatus=0)
'''
    process = subprocess.Popen(
        [sys.executable, "-c", holder_code, str(project_root)],
        cwd=Path(__file__).resolve().parents[2],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert process.stdout is not None
    ready_line = process.stdout.readline().strip()
    if ready_line != "READY":
        assert process.stderr is not None
        stderr_text = process.stderr.read()
        process.kill()
        process.wait(timeout=10)
        raise AssertionError(f"report plugin holder failed to start: {ready_line!r}\n{stderr_text}")
    return process


def _stop_report_plugin_holder(process: subprocess.Popen[str]) -> None:
    assert process.stdin is not None
    process.stdin.write("\n")
    process.stdin.flush()
    try:
        return_code = process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        return_code = process.wait(timeout=10)
    assert process.stderr is not None
    assert return_code == 0, process.stderr.read()


@pytest.mark.test_id("REPORT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("CSV 单元格必须防止 Excel 公式注入")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="sanitize_csv_cell")
def test_sanitize_csv_cell_prevents_formula_injection() -> None:
    assert sanitize_csv_cell("=SUM(A1:A2)") == "'=SUM(A1:A2)"
    assert sanitize_csv_cell("+1") == "'+1"
    assert sanitize_csv_cell("-2") == "'-2"
    assert sanitize_csv_cell("@cmd") == "'@cmd"


@pytest.mark.test_id("REPORT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("report writer 输出 UTF-8-SIG CSV")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportWriter.write_report")
def test_csv_report_writer_outputs_expected_header(tmp_path: Path) -> None:
    report_path = tmp_path / "test_report.csv"
    writer = CsvReportWriter()
    writer.write_report(
        [
            TestReportRow(
                run_id="run",
                started_at="2026-01-01T00:00:00+08:00",
                finished_at="2026-01-01T00:00:01+08:00",
                duration_seconds="1.0",
                test_id="RID-01",
                pytest_nodeid="tests/x.py::test_y",
                layer="unit",
                category="sample",
                priority="P0",
                requirement="sample",
                tested_file="file.py",
                tested_function="func",
                scenario="scenario",
                input_summary="input",
                fault_injection="none",
                expected_result="ok",
                actual_result="ok",
                status="passed",
                failure_type="",
                failure_message="",
                artifact_dir="artifacts",
                log_files="",
                result_files="",
                screenshot_files="",
                runtime_state_db="",
                environment="local",
                host_role="single_machine_simulation",
                team_count="1",
                algorithm_profiles="team_0=normal",
                network_profile="normal",
                recovery_mode="none",
                git_commit="unknown",
                git_dirty="true",
                notes="",
            )
        ],
        report_path,
    )
    with report_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        row_list = list(reader)
    assert reader.fieldnames is not None
    assert "test_id" in reader.fieldnames
    assert row_list[0]["test_id"] == "RID-01"


@pytest.mark.test_id("REPORT-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("CSV 汇总必须把 P0 失败标记为 release gate blocked，并列出阻断测试 ID")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin._build_summary_rows")
def test_csv_report_plugin_summary_marks_release_gate_blocked_for_p0_failure(tmp_path: Path) -> None:
    plugin = CsvReportPlugin(tmp_path)
    plugin.run_id = "run-summary"
    plugin.row_list = [
        TestReportRow(
            run_id="run-summary",
            started_at="2026-01-01T00:00:00+08:00",
            finished_at="2026-01-01T00:00:01+08:00",
            duration_seconds="1.000",
            test_id="RID-P0-FAIL",
            pytest_nodeid="tests/x.py::test_p0_failure",
            layer="unit",
            category="sample",
            priority="P0",
            requirement="critical",
            tested_file="file.py",
            tested_function="func",
            scenario="failure",
            input_summary="",
            fault_injection="none",
            expected_result="passed",
            actual_result="failed",
            status="failed",
            failure_type="assertion",
            failure_message="boom",
            artifact_dir="artifacts",
            log_files="",
            result_files="",
            screenshot_files="",
            runtime_state_db="",
            environment="local",
            host_role="single_machine_simulation",
            team_count="1",
            algorithm_profiles="team_0=normal",
            network_profile="normal",
            recovery_mode="none",
            git_commit="unknown",
            git_dirty="true",
            notes="",
        ),
        TestReportRow(
            run_id="run-summary",
            started_at="2026-01-01T00:00:02+08:00",
            finished_at="2026-01-01T00:00:03+08:00",
            duration_seconds="1.000",
            test_id="RID-P1-PASS",
            pytest_nodeid="tests/x.py::test_p1_pass",
            layer="unit",
            category="sample",
            priority="P1",
            requirement="non-critical",
            tested_file="file.py",
            tested_function="func",
            scenario="pass",
            input_summary="",
            fault_injection="none",
            expected_result="passed",
            actual_result="passed",
            status="passed",
            failure_type="",
            failure_message="",
            artifact_dir="artifacts",
            log_files="",
            result_files="",
            screenshot_files="",
            runtime_state_db="",
            environment="local",
            host_role="single_machine_simulation",
            team_count="1",
            algorithm_profiles="team_0=normal",
            network_profile="normal",
            recovery_mode="none",
            git_commit="unknown",
            git_dirty="true",
            notes="",
        ),
    ]

    summary_rows = plugin._build_summary_rows()
    layer_rows = [row for row in summary_rows if row["group_by"] == "layer" and row["group_value"] == "unit"]

    assert len(layer_rows) == 1
    assert layer_rows[0]["failed_count"] == 1
    assert layer_rows[0]["passed_count"] == 1
    assert layer_rows[0]["blocking_failure_count"] == 1
    assert layer_rows[0]["release_gate"] == "blocked"
    assert layer_rows[0]["blocking_failure_test_ids"] == "RID-P0-FAIL"


@pytest.mark.test_id("REPORT-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("测试 metadata 缺失时，报告插件必须生成 fallback test_id 并记录缺失字段 notes")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin._extract_metadata")
def test_csv_report_plugin_extract_metadata_marks_incomplete_fields(tmp_path: Path) -> None:
    plugin = CsvReportPlugin(tmp_path)

    class DummyItem:
        nodeid = "tests/sample/test_demo.py::test_case"
        name = "test_case"

        @staticmethod
        def get_closest_marker(name: str):
            if name == "layer":
                return SimpleNamespace(args=("unit",), kwargs={})
            if name == "category":
                return SimpleNamespace(args=("demo",), kwargs={})
            return None

    metadata = plugin._extract_metadata(DummyItem())

    assert metadata["test_id"].startswith("tests_sample_test_demo.py__test_case")
    assert metadata["layer"] == "unit"
    assert metadata["category"] == "demo"
    assert metadata["priority"] == "P2"
    assert metadata["notes"] == "metadata_incomplete:tested_file,tested_function"


@pytest.mark.test_id("REPORT-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("测试报告目录必须只保留 latest 和 pytest_tmp* 临时目录，避免长期累计历史结果占满磁盘，同时不误删当前运行的唯一 basetemp")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin._prune_non_latest_artifacts")
def test_csv_report_plugin_prunes_old_artifact_directories(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "tests" / "artifacts"
    (artifacts_root / "latest").mkdir(parents=True, exist_ok=True)
    (artifacts_root / "pytest_tmp").mkdir(parents=True, exist_ok=True)
    (artifacts_root / "pytest_tmp_run_123").mkdir(parents=True, exist_ok=True)
    (artifacts_root / "20260420-010203-local").mkdir(parents=True, exist_ok=True)
    old_file = artifacts_root / "obsolete.txt"
    old_file.write_text("old", encoding="utf-8")

    plugin = CsvReportPlugin(tmp_path)
    plugin._prune_non_latest_artifacts()

    remaining_names = sorted(path.name for path in artifacts_root.iterdir())
    assert remaining_names == ["latest", "pytest_tmp", "pytest_tmp_run_123"]


@pytest.mark.test_id("REPORT-05A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("测试会话开始后 latest 必须立即写入 run_manifest 与 live_status，避免现场误把旧工件当成本次运行")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin.pytest_sessionstart")
def test_csv_report_plugin_writes_run_manifest_and_live_status_on_sessionstart(tmp_path: Path) -> None:
    plugin = CsvReportPlugin(tmp_path)
    plugin.run_id = "run-live"

    plugin.pytest_sessionstart(session=None)

    run_manifest = json.loads((tmp_path / "tests" / "artifacts" / "latest" / "run_manifest.json").read_text(encoding="utf-8"))
    live_status = json.loads((tmp_path / "tests" / "artifacts" / "latest" / "live_status.json").read_text(encoding="utf-8"))

    assert run_manifest["run_id"] == "run-live"
    assert run_manifest["status"] == "running"
    assert live_status["run_id"] == "run-live"
    assert live_status["phase"] == "sessionstart"
    assert live_status["status"] == "running"


@pytest.mark.test_id("REPORT-05F")
@pytest.mark.priority("P0")
@pytest.mark.requirement("latest 被残留进程占用时测试会话必须立即失败，不能静默保留半删除工件后继续执行")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin._reset_latest_artifact_root")
def test_csv_report_plugin_aborts_when_latest_cannot_be_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest_root = tmp_path / "tests" / "artifacts" / "latest"
    sentinel_path = latest_root / "heavy" / "real_full_chain" / "heavy_workspace" / "app" / "sentinel.txt"
    sentinel_path.parent.mkdir(parents=True, exist_ok=True)
    sentinel_path.write_text("stale", encoding="utf-8")
    real_rmtree = shutil.rmtree

    def fake_rmtree(path, ignore_errors=False, *args, **kwargs):
        if Path(path) == latest_root:
            if ignore_errors:
                return None
            raise PermissionError("simulated stale heavy process")
        return real_rmtree(path, ignore_errors=ignore_errors, *args, **kwargs)

    monkeypatch.setattr("tests.helpers.report_writer.shutil.rmtree", fake_rmtree)
    plugin = CsvReportPlugin(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="cannot reset pytest latest artifacts"):
            plugin.pytest_sessionstart(session=None)
        assert sentinel_path.read_text(encoding="utf-8") == "stale"
    finally:
        plugin.run_lock.release()


@pytest.mark.test_id("REPORT-05C")
@pytest.mark.priority("P0")
@pytest.mark.requirement("已有 pytest 会话运行时，第二个会话必须在清空 latest 前失败")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin.pytest_sessionstart")
def test_csv_report_plugin_rejects_concurrent_session_before_resetting_latest(tmp_path: Path) -> None:
    process = _start_report_plugin_holder(tmp_path)
    sentinel_path = (
        tmp_path
        / "tests"
        / "artifacts"
        / "latest"
        / "heavy_workspace"
        / "sentinel.dat"
    )
    sentinel_path.parent.mkdir(parents=True, exist_ok=True)
    sentinel_path.write_bytes(b"KEEP")
    try:
        second_plugin = CsvReportPlugin(tmp_path)
        with pytest.raises(pytest.UsageError, match="another pytest session is active") as exc_info:
            second_plugin.pytest_sessionstart(session=None)
        assert '"pid"' in str(exc_info.value)
        assert '"run_id"' in str(exc_info.value)
        assert sentinel_path.read_bytes() == b"KEEP"
    finally:
        _stop_report_plugin_holder(process)


@pytest.mark.test_id("REPORT-05D")
@pytest.mark.priority("P0")
@pytest.mark.requirement("持锁 pytest 进程异常退出后，后续会话必须依靠 OS 自动释放恢复运行")
@pytest.mark.tested(file="tests/helpers/pytest_run_lock.py", function="PytestRunLock.acquire")
def test_csv_report_plugin_recovers_after_lock_owner_process_is_killed(tmp_path: Path) -> None:
    process = _start_report_plugin_holder(tmp_path)
    process.kill()
    process.wait(timeout=10)

    plugin = CsvReportPlugin(tmp_path)
    try:
        plugin.pytest_sessionstart(session=None)
        manifest_path = tmp_path / "tests" / "artifacts" / "latest" / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["status"] == "running"
    finally:
        plugin.pytest_sessionfinish(session=None, exitstatus=0)


@pytest.mark.test_id("REPORT-05E")
@pytest.mark.priority("P1")
@pytest.mark.requirement("锁释放必须幂等，且不得删除已被其他 owner 覆盖的 metadata")
@pytest.mark.tested(file="tests/helpers/pytest_run_lock.py", function="PytestRunLock.release")
def test_pytest_run_lock_release_is_idempotent_and_preserves_foreign_metadata(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "tests" / "artifacts"
    run_lock = PytestRunLock(artifacts_root=artifacts_root, run_id="owner-a")
    run_lock.acquire()
    foreign_metadata = {
        "owner_token": "foreign-owner-token",
        "pid": 99999,
        "run_id": "owner-b",
    }
    run_lock.metadata_path.write_text(
        json.dumps(foreign_metadata, ensure_ascii=False),
        encoding="utf-8",
    )

    run_lock.release()
    run_lock.release()

    assert json.loads(run_lock.metadata_path.read_text(encoding="utf-8")) == foreign_metadata
    next_lock = PytestRunLock(artifacts_root=artifacts_root, run_id="owner-c")
    next_lock.acquire()
    next_lock.release()


@pytest.mark.test_id("REPORT-05B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("报告插件收尾阶段不能被其他用例对 time.time 的有限 monkeypatch 污染而崩溃")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="pytest_sessionfinish/_build_summary_rows")
def test_csv_report_plugin_sessionfinish_ignores_exhausted_time_time_monkeypatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = CsvReportPlugin(tmp_path)
    plugin.run_id = "run-time-safe"
    plugin.pytest_sessionstart(session=None)
    plugin.row_list = [
        TestReportRow(
            run_id="run-time-safe",
            started_at="2026-01-01T00:00:00+08:00",
            finished_at="2026-01-01T00:00:01+08:00",
            duration_seconds="1.000",
            test_id="RID-PASS-01",
            pytest_nodeid="tests/x.py::test_pass",
            layer="unit",
            category="sample",
            priority="P1",
            requirement="safe finish",
            tested_file="file.py",
            tested_function="func",
            scenario="pass",
            input_summary="",
            fault_injection="none",
            expected_result="passed",
            actual_result="passed",
            status="passed",
            failure_type="",
            failure_message="",
            artifact_dir="artifacts",
            log_files="",
            result_files="",
            screenshot_files="",
            runtime_state_db="",
            environment="local",
            host_role="single_machine_simulation",
            team_count="1",
            algorithm_profiles="team_0=normal",
            network_profile="normal",
            recovery_mode="none",
            git_commit="unknown",
            git_dirty="true",
            notes="",
        )
    ]

    time_values = iter([0.0])
    monkeypatch.setattr("tests.helpers.report_writer.time.time", lambda: next(time_values))

    plugin.pytest_sessionfinish(session=None, exitstatus=0)

    summary_path = tmp_path / "tests" / "artifacts" / "latest" / "test_summary.csv"
    assert summary_path.exists()


@pytest.mark.test_id("REPORT-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("setup 阶段 skip 也必须进入 CSV 报告，避免动态导入异常场景在最终报告中丢失")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin.pytest_runtest_makereport")
def test_csv_report_plugin_records_skipped_setup_phase(tmp_path: Path) -> None:
    plugin = CsvReportPlugin(tmp_path)
    plugin.pytest_sessionstart(session=None)

    class DummyItem:
        nodeid = "tests/sample/test_skip.py::test_skipped_case"
        name = "test_skipped_case"

        @staticmethod
        def get_closest_marker(name: str):
            mapping = {
                "test_id": SimpleNamespace(args=("SKIP-CASE-01",), kwargs={}),
                "layer": SimpleNamespace(args=("algorithm_interface",), kwargs={}),
                "category": SimpleNamespace(args=("import_gate",), kwargs={}),
                "priority": SimpleNamespace(args=("P1",), kwargs={}),
                "requirement": SimpleNamespace(args=("skip reason must be reported",), kwargs={}),
                "tested": SimpleNamespace(args=(), kwargs={"file": "demo.py", "function": "demo"}),
            }
            return mapping.get(name)

    class DummyExcInfo:
        typename = "Skipped"
        value = pytest.skip.Exception("grpc unavailable")

        def errisinstance(self, expected):
            if isinstance(expected, tuple):
                return any(self.errisinstance(item) for item in expected)
            if expected is BaseException:
                return True
            return expected is pytest.skip.Exception

        def __str__(self) -> str:
            return "Skipped: grpc unavailable"

    call = SimpleNamespace(
        when="setup",
        start=1.0,
        stop=1.1,
        excinfo=DummyExcInfo(),
    )

    plugin.pytest_runtest_makereport(DummyItem(), call)

    assert len(plugin.row_list) == 1
    row = plugin.row_list[0]
    assert row.status == "skipped"
    assert row.failure_type == "skip"
    assert "grpc unavailable" in row.failure_message


@pytest.mark.test_id("REPORT-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("测试执行过程中必须实时打印 skip 明细，便于现场直接定位被跳过用例")
@pytest.mark.tested(file="tests/helpers/report_writer.py", function="CsvReportPlugin.pytest_runtest_logreport")
def test_csv_report_plugin_prints_skip_details_to_console(tmp_path: Path) -> None:
    plugin = CsvReportPlugin(tmp_path)
    report = SimpleNamespace(
        outcome="skipped",
        when="setup",
        nodeid="tests/sample/test_skip.py::test_skipped_case",
        longrepr=("tests/sample/test_skip.py", 12, "Skipped: grpc unavailable"),
    )

    captured_messages: list[str] = []

    def fake_print(*args, **kwargs) -> None:
        captured_messages.append(" ".join(str(arg) for arg in args))

    import builtins

    original_print = builtins.print
    builtins.print = fake_print
    try:
        plugin.pytest_runtest_logreport(report)
    finally:
        builtins.print = original_print

    assert len(captured_messages) == 1
    assert "[skip] tests/sample/test_skip.py::test_skipped_case" in captured_messages[0]
    assert "grpc unavailable" in captured_messages[0]
