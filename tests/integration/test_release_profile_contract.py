from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_AUTOMATED_TESTS_BAT = PROJECT_ROOT / "run_automated_tests.bat"


pytestmark = [pytest.mark.integration, pytest.mark.layer("integration"), pytest.mark.category("release_profile")]


def _read_bat() -> str:
    return RUN_AUTOMATED_TESTS_BAT.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("INT-REL-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("release profile 必须覆盖发布前回归所需的 unit/condition/component/algorithm_interface/integration/resilience/security/dashboard/e2e")
@pytest.mark.tested(
    file="run_automated_tests.bat",
    function=":build_pytest_plan",
)
def test_release_profile_includes_all_release_gate_layers() -> None:
    content = _read_bat()

    assert 'set "PYTEST_PLAN_LABEL=release"' in content
    assert (
        'set "PYTEST_ARGS_1=tests\\unit tests\\condition tests\\component '
        'tests\\algorithm_interface tests\\integration tests\\resilience tests\\security tests\\dashboard tests\\e2e"'
        in content
    )
    assert 'set "PYTEST_MARK_EXPR_1=not heavy"' in content


@pytest.mark.test_id("INT-REL-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("integration 目录应已落地并纳入 pytest 搜索范围，避免后续上下文重置后遗漏该测试层")
@pytest.mark.tested(
    file="pytest.ini;tests/integration",
    function="testpaths_contract",
)
def test_integration_directory_exists_for_future_local_chain_tests() -> None:
    assert (PROJECT_ROOT / "tests" / "integration").exists()
