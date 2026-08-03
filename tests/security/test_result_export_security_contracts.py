from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHALLENGE_MI_FILE = (
    PROJECT_ROOT
    / "app"
    / "ProcessHub"
    / "ProcessHub"
    / "bci_competition"
    / "challenge"
    / "MI"
    / "ChallengeMI.py"
)
REPORT_WRITER_FILE = PROJECT_ROOT / "tests" / "helpers" / "report_writer.py"


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("result_export_security")]


def _read(relative_path: Path) -> str:
    return relative_path.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("SEC-EXPORT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("测试报告 CSV 必须统一通过 sanitize_csv_cell 转义，避免 =/+/-/@ 公式注入")
@pytest.mark.tested(
    file="tests/helpers/report_writer.py",
    function="sanitize_csv_cell/CsvReportWriter.write_report/CsvReportWriter.write_summary",
)
def test_report_writer_routes_all_csv_cells_through_sanitize_function() -> None:
    content = _read(REPORT_WRITER_FILE)

    assert 'if text[:1] in {"=", "+", "-", "@"}:' in content
    assert "return \"'\" + text" in content
    assert "writer.writerow({key: sanitize_csv_cell(value) for key, value in asdict(row).items()})" in content
    assert "writer.writerow({key: sanitize_csv_cell(row.get(key, \"\")) for key in SUMMARY_FIELDNAME_LIST})" in content


@pytest.mark.test_id("SEC-EXPORT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("ChallengeMI 结果导出必须把 team_id/subject_id/task_id 作为普通字段写入 CSV，而不是拼接成用户可控路径")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__build_trial_record_export_row_list/__resolve_result_dir",
)
def test_challenge_mi_exports_identity_fields_as_csv_columns_not_path_segments() -> None:
    content = _read(CHALLENGE_MI_FILE)

    assert "'team_id': self.__resolve_team_id()" in content
    assert "'subject_id': record.get('subject_id')" in content
    assert "'task_id': current_task_id" in content
    assert "return self.__resolve_results_root_dir() / self.__resolve_team_id()" in content
    assert "task_id is not None and current_task_id != str(task_id)" in content


@pytest.mark.test_id("SEC-EXPORT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("ChallengeMI 结果文件清理只能删除当前队结果目录下的受控文件，不得递归删 results 根目录")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__cleanup_legacy_result_files",
)
def test_challenge_mi_cleanup_is_scoped_to_known_result_files_without_recursive_root_delete() -> None:
    content = _read(CHALLENGE_MI_FILE)

    assert "def __cleanup_legacy_result_files(result_dir: Path) -> None:" in content
    assert "legacy_file_path = result_dir / legacy_file_name" in content
    assert "task_trials_dir = result_dir / 'task_trials'" in content
    assert "task_trial_file_path.unlink()" in content
    assert "shutil.rmtree(" not in content
