from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_AUTOMATED_TESTS_BAT = PROJECT_ROOT / "run_automated_tests.bat"


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("automation_entry")]


def _read_bat() -> str:
    return RUN_AUTOMATED_TESTS_BAT.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("COND-BAT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("一键测试 BAT 必须声明所有计划中的 profile 到 pytest 路径映射")
@pytest.mark.tested(
    file="run_automated_tests.bat",
    function=":build_pytest_plan",
)
def test_run_automated_tests_bat_declares_all_expected_profiles() -> None:
    content = _read_bat()

    assert 'set "PYTEST_ARGS_1=tests\\unit"' in content
    assert 'set "PYTEST_ARGS_1=tests\\unit tests\\component"' in content
    assert 'set "PYTEST_ARGS_1=tests"' in content
    assert 'set "PYTEST_MARK_EXPR_1=not requires_admin and not manual_network and not heavy"' in content
    assert (
        'set "PYTEST_ARGS_1=tests\\unit tests\\condition tests\\component '
        'tests\\algorithm_interface tests\\integration tests\\resilience tests\\security tests\\dashboard tests\\e2e"'
        in content
    )
    assert 'set "PYTEST_MARK_EXPR_1=not heavy"' in content
    assert 'set "PYTEST_ARGS_1=tests\\unit tests\\condition tests\\component tests\\algorithm_interface tests\\integration tests\\resilience tests\\security tests\\dashboard tests\\e2e tests\\heavy"' in content
    assert 'set "PYTEST_ARGS_1=tests\\heavy"' in content


@pytest.mark.test_id("COND-BAT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("一键测试 BAT 必须设置本地快速回归所需环境变量和 CSV 输出路径")
@pytest.mark.tested(
    file="run_automated_tests.bat",
    function="entrypoint_contract",
)
def test_run_automated_tests_bat_sets_environment_and_prints_latest_reports() -> None:
    content = _read_bat()

    assert 'set "PYTHONHASHSEED=0"' in content
    assert 'set "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1"' in content
    assert 'if not defined BCI_TEST_ENVIRONMENT set "BCI_TEST_ENVIRONMENT=local"' in content
    assert 'if not exist "%ROOT%tests\\artifacts" mkdir "%ROOT%tests\\artifacts"' in content
    assert "latest_report=%ROOT%tests\\artifacts\\latest\\test_report.csv" in content
    assert "latest_summary=%ROOT%tests\\artifacts\\latest\\test_summary.csv" in content


@pytest.mark.test_id("COND-BAT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("一键测试 BAT 对未知 profile 必须给出明确错误提示")
@pytest.mark.tested(
    file="run_automated_tests.bat",
    function=":build_pytest_plan",
)
def test_run_automated_tests_bat_rejects_unknown_profile() -> None:
    content = _read_bat()

    assert '[run_automated_tests] unknown profile: %PROFILE%' in content
    assert 'allowed profiles: unit core nightly release all heavy' in content
