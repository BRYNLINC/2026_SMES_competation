from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFTEST_FILE = PROJECT_ROOT / "tests" / "conftest.py"
PYTEST_INI_FILE = PROJECT_ROOT / "pytest.ini"


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("temp_cleanup")]


@pytest.mark.test_id("TEMP-CLEAN-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("pytest 必须在每个 tmp_path/tmpdir 用例后清理测试子内容，并在 session 结束后清理 basetemp，避免硬盘被测试临时文件打满")
@pytest.mark.tested(
    file="tests/conftest.py;pytest.ini",
    function="cleanup_per_test_temp_roots/pytest_sessionfinish",
)
def test_pytest_temp_cleanup_policy_is_configured() -> None:
    conftest_content = CONFTEST_FILE.read_text(encoding="utf-8")
    pytest_ini_content = PYTEST_INI_FILE.read_text(encoding="utf-8")

    assert "--basetemp=tests/artifacts/pytest_tmp" in pytest_ini_content
    assert "def pytest_sessionfinish" in conftest_content
    assert "shutil.rmtree(basetemp_path, ignore_errors=True)" in conftest_content
    assert "def cleanup_per_test_temp_roots" in conftest_content
    assert "request.getfixturevalue(\"tmp_path\")" in conftest_content
    assert "request.getfixturevalue(\"tmpdir\")" in conftest_content
    assert "for child_path in list(tmp_root.iterdir()):" in conftest_content
    assert "shutil.rmtree(child_path, ignore_errors=True)" in conftest_content
    assert "child_path.unlink(missing_ok=True)" in conftest_content


@pytest.mark.test_id("TEMP-CLEAN-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("双击 BAT 运行时必须覆盖 pytest.ini 的固定 basetemp，改为每次运行唯一目录，避免 Windows 句柄残留导致批量 tmp_path PermissionError")
@pytest.mark.tested(file="run_automated_tests.bat", function=":build_pytest_plan")
def test_run_automated_tests_bat_overrides_static_basetemp_with_unique_run_directory() -> None:
    bat_content = (PROJECT_ROOT / "run_automated_tests.bat").read_text(encoding="utf-8")

    assert 'set "PYTEST_RUN_ID=run_%RANDOM%_%RANDOM%_%RANDOM%"' in bat_content
    assert 'set "PYTEST_BASETEMP=.pt_%PYTEST_RUN_ID%"' in bat_content
    assert '--basetemp=%PYTEST_BASETEMP%' in bat_content
