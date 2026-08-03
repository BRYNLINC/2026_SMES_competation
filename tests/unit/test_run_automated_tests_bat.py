from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("test_runner")]


def _load_bat_text(project_root: Path) -> str:
    return (project_root / "run_automated_tests.bat").read_text(encoding="utf-8")


@pytest.mark.test_id("BAT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("一键测试脚本必须存在并设置 PYTHONHASHSEED")
@pytest.mark.tested(file="run_automated_tests.bat", function="script_contract")
def test_run_automated_tests_bat_exists_and_sets_pythonhashseed(project_root_path: Path) -> None:
    bat_path = project_root_path / "run_automated_tests.bat"
    assert bat_path.exists()
    bat_text = _load_bat_text(project_root_path)
    assert 'set "PYTHONHASHSEED=0"' in bat_text
    assert 'if not defined SystemRoot set "SystemRoot=C:\\Windows"' in bat_text
    assert 'if not defined WINDIR set "WINDIR=%SystemRoot%"' in bat_text
    assert 'if not defined ComSpec set "ComSpec=%SystemRoot%\\System32\\cmd.exe"' in bat_text


@pytest.mark.test_id("BAT-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("一键测试脚本必须禁用 pytest 第三方插件自动加载")
@pytest.mark.tested(file="run_automated_tests.bat", function="script_contract")
def test_run_automated_tests_bat_disables_pytest_plugin_autoload(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)
    assert 'set "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1"' in bat_text


@pytest.mark.test_id("BAT-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("一键测试脚本必须检查 pytest 是否安装")
@pytest.mark.tested(file="run_automated_tests.bat", function=":check_pytest")
def test_run_automated_tests_bat_checks_pytest(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)
    assert ":check_pytest" in bat_text
    assert "pytest is not installed" in bat_text


@pytest.mark.test_id("BAT-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("一键测试脚本支持 unit/core/nightly/release/all/heavy profile，且默认双击时 heavy 放在最后执行")
@pytest.mark.tested(file="run_automated_tests.bat", function=":build_pytest_plan")
def test_run_automated_tests_bat_supports_expected_profiles(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)
    assert 'if "%PROFILE%"=="" set "PROFILE=all"' in bat_text
    assert 'set "PYTEST_RUN_ID=run_%RANDOM%_%RANDOM%_%RANDOM%"' in bat_text
    assert 'set "PYTEST_BASETEMP=.pt_%PYTEST_RUN_ID%"' in bat_text
    assert 'set "PYTEST_COMMON_ARGS=-o console_output_style=progress --basetemp=%PYTEST_BASETEMP%"' in bat_text
    for profile in ("unit", "core", "nightly", "release", "all", "heavy"):
        assert f'"%PROFILE%"=="{profile}"' in bat_text
    assert 'tests\\e2e tests\\heavy' in bat_text
    assert 'set "PYTEST_PHASE_1=AllWithHeavyLast"' in bat_text
    assert 'heavy tests run last' in bat_text


@pytest.mark.test_id("BAT-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("一键测试脚本必须输出 latest CSV 报告路径和 passed/skipped/failed 汇总")
@pytest.mark.tested(file="run_automated_tests.bat", function="script_contract")
def test_run_automated_tests_bat_prints_latest_report_paths(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)
    assert "latest_report=%ROOT%tests\\artifacts\\latest\\test_report.csv" in bat_text
    assert "latest_summary=%ROOT%tests\\artifacts\\latest\\test_summary.csv" in bat_text
    assert ":print_summary_counts" in bat_text
    assert "summary_counts passed=" in bat_text


@pytest.mark.test_id("BAT-06")
@pytest.mark.priority("P1")
@pytest.mark.requirement("未知 profile 必须给出错误提示并列出允许的 profile，避免现场误用 bat 参数")
@pytest.mark.tested(file="run_automated_tests.bat", function=":build_pytest_plan")
def test_run_automated_tests_bat_rejects_unknown_profile_with_help(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)
    assert 'echo [run_automated_tests] unknown profile: %PROFILE%' in bat_text
    assert 'echo [run_automated_tests] allowed profiles: unit core nightly release all heavy' in bat_text


@pytest.mark.test_id("BAT-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("nightly profile 必须显式排除 requires_admin 与 manual_network 用例，保证无人值守执行稳定")
@pytest.mark.tested(file="run_automated_tests.bat", function=":build_pytest_plan")
def test_run_automated_tests_bat_nightly_profile_excludes_manual_network_cases(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)
    assert 'set "PYTEST_ARGS_1=tests"' in bat_text
    assert 'set "PYTEST_MARK_EXPR_1=not requires_admin and not manual_network and not heavy"' in bat_text
    assert '-m "%CURRENT_MARK_EXPR%"' in bat_text


@pytest.mark.test_id("BAT-08A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("默认 all profile 必须在同一个 pytest 会话中先跑常规全量，再把 heavy 17 队重型测试放在最后执行")
@pytest.mark.tested(file="run_automated_tests.bat", function=":build_pytest_plan/:run_pytest_phase")
def test_run_automated_tests_bat_runs_heavy_phase_last_for_default_all_profile(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)

    assert 'set "PYTEST_PLAN_LABEL=all_with_heavy_last"' in bat_text
    assert 'set "PYTEST_COMMON_ARGS=-o console_output_style=progress --capture=tee-sys --basetemp=%PYTEST_BASETEMP%"' in bat_text
    assert 'set "PYTEST_ARGS_1=tests\\unit tests\\condition tests\\component tests\\algorithm_interface tests\\integration tests\\resilience tests\\security tests\\dashboard tests\\e2e tests\\heavy"' in bat_text
    assert bat_text.index("tests\\e2e") < bat_text.index("tests\\heavy")
    assert "real 17-team full-chain execution" in bat_text
    assert "17 队" not in bat_text


@pytest.mark.test_id("BAT-08B")
@pytest.mark.priority("P1")
@pytest.mark.requirement("heavy 与 all profile 必须显示实时 heavy 进度信息，便于双击 BAT 时观察当前步骤与预计剩余时间")
@pytest.mark.tested(file="run_automated_tests.bat", function=":build_pytest_plan/:run_pytest_phase")
def test_run_automated_tests_bat_enables_live_heavy_progress_output(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)

    assert "heavy progress format: [heavy_progress]" in bat_text
    assert "ETA is estimated" in bat_text
    assert "tests\\artifacts\\latest\\heavy\\real_full_chain\\" in bat_text
    assert "pytest_common_args=%PYTEST_COMMON_ARGS%" in bat_text
    assert 'set "PYTEST_COMMON_ARGS=-o console_output_style=progress --capture=tee-sys --basetemp=%PYTEST_BASETEMP%"' in bat_text
    assert '"%BCI_PYTHON_EXE%" -m pytest %PYTEST_COMMON_ARGS% %CURRENT_ARGS%' in bat_text


@pytest.mark.test_id("BAT-08")
@pytest.mark.priority("P0")
@pytest.mark.requirement("手动双击运行 BAT 时必须 pause 显示错误或结果，避免窗口闪退无法诊断")
@pytest.mark.tested(file="run_automated_tests.bat", function=":finish")
def test_run_automated_tests_bat_pauses_for_manual_double_click(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)

    assert 'set "PAUSE_ON_EXIT=1"' in bat_text
    assert "BCI_TEST_NO_PAUSE" in bat_text
    assert 'if /I "%PROFILE%"=="--no-pause"' in bat_text
    assert '--no-pause' in bat_text
    assert "Press any key to close this window" in bat_text
    assert "pause >nul" in bat_text


@pytest.mark.test_id("BAT-09")
@pytest.mark.priority("P1")
@pytest.mark.requirement("BAT 必须尊重外部传入的 BCI_PYTHON_EXE，但要先验证 pytest/asyncio/socket 可用，避免真实 heavy 选到坏解释器")
@pytest.mark.tested(file="run_automated_tests.bat", function=":resolve_python")
def test_run_automated_tests_bat_preserves_external_python_override(project_root_path: Path) -> None:
    bat_text = _load_bat_text(project_root_path)

    assert 'if defined BCI_PYTHON_EXE if exist "%BCI_PYTHON_EXE%"' in bat_text
    assert 'call :check_python_runtime "%BCI_PYTHON_EXE%"' in bat_text
    assert 'import pytest, asyncio, socket, yaml, grpc, numpy, injector' in bat_text
    assert 'from Algorithm.api.proto import AlgorithmRPCService_pb2_grpc' in bat_text
    assert bat_text.index('if defined BCI_PYTHON_EXE if exist "%BCI_PYTHON_EXE%"') < bat_text.index('set "BCI_PYTHON_EXE="')
    assert bat_text.index('set "BCI_PYTHON_EXE="') < bat_text.index('call :try_python_candidate "D:\\anaconda3\\envs\\BCI_competation_2026\\python.exe"')
    assert bat_text.index('call :try_python_candidate "D:\\anaconda3\\envs\\BCI_competation_2026\\python.exe"') < bat_text.index('call :try_python_candidate "D:\\anaconda3\\envs\\BCI_competition_2026\\python.exe"')
    assert bat_text.index('call :try_python_candidate "D:\\anaconda3\\envs\\BCI_competition_2026\\python.exe"') < bat_text.index('call :try_python_candidate "D:\\anaconda3\\python.exe"')
