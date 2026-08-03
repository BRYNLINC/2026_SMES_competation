from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANUAL_FILE = PROJECT_ROOT / "final_multi_machine_test_manual.md"
RUN_BAT_FILE = PROJECT_ROOT / "run_automated_tests.bat"


pytestmark = [pytest.mark.e2e, pytest.mark.layer("e2e"), pytest.mark.category("tiny_match_contract")]


@pytest.mark.test_id("E2E-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("The e2e layer and final multi-machine manual must remain present.")
@pytest.mark.tested(file="tests/e2e;final_multi_machine_test_manual.md", function="directory_contract")
def test_e2e_directory_and_manual_exist_for_tiny_match_followup() -> None:
    assert (PROJECT_ROOT / "tests" / "e2e").exists()
    assert MANUAL_FILE.exists()


@pytest.mark.test_id("E2E-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("The release profile must include dashboard and e2e layers.")
@pytest.mark.tested(file="run_automated_tests.bat", function=":build_pytest_plan")
def test_release_profile_includes_dashboard_and_e2e_layers() -> None:
    content = RUN_BAT_FILE.read_text(encoding="utf-8", errors="ignore")

    assert (
        'set "PYTEST_ARGS_1=tests\\unit tests\\condition tests\\component '
        'tests\\algorithm_interface tests\\integration tests\\resilience tests\\security tests\\dashboard tests\\e2e"'
        in content
    )
    assert 'set "PYTEST_MARK_EXPR_1=not heavy"' in content


@pytest.mark.test_id("E2E-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("Tiny-match e2e contracts depend on the core orchestration helpers.")
@pytest.mark.tested(
    file="tools/start_judge_stack.py;tools/shutdown_judge_stack.py;tools/recovery_runtime.py;tests/helpers/fake_algorithm_server.py",
    function="file_presence_contract",
)
def test_tiny_match_contract_requires_core_orchestration_files() -> None:
    for path in (
        PROJECT_ROOT / "tools" / "start_judge_stack.py",
        PROJECT_ROOT / "tools" / "shutdown_judge_stack.py",
        PROJECT_ROOT / "tools" / "recovery_runtime.py",
        PROJECT_ROOT / "tests" / "helpers" / "fake_algorithm_server.py",
        PROJECT_ROOT / "tests" / "helpers" / "config_factory.py",
    ):
        assert path.exists()
