@echo off
setlocal
title BCI Final Automated Tests

set "ROOT=%~dp0"
set "PROFILE=%~1"
set "NO_PAUSE=%~2"
set "PYTEST_EXIT=0"
set "FINAL_EXIT=0"
if /I "%PROFILE%"=="--no-pause" (
  set "PROFILE="
  set "NO_PAUSE=--no-pause"
)
if "%PROFILE%"=="" set "PROFILE=all"

set "PAUSE_ON_EXIT=1"
if /I "%BCI_TEST_NO_PAUSE%"=="1" set "PAUSE_ON_EXIT=0"
if /I "%NO_PAUSE%"=="--no-pause" set "PAUSE_ON_EXIT=0"

cd /d "%ROOT%" || (
  echo [run_automated_tests] failed to enter root: %ROOT%
  set "PYTEST_EXIT=1"
  goto :finish
)

if not defined SystemRoot set "SystemRoot=C:\Windows"
if not defined WINDIR set "WINDIR=%SystemRoot%"
if not defined ComSpec set "ComSpec=%SystemRoot%\System32\cmd.exe"
set "PATH=%SystemRoot%\System32;%SystemRoot%;%SystemRoot%\System32\Wbem;%PATH%"

call :resolve_python
if errorlevel 1 (
  set "PYTEST_EXIT=1"
  goto :finish
)

set "PYTHONHASHSEED=0"
set "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1"
if not defined BCI_TEST_ENVIRONMENT set "BCI_TEST_ENVIRONMENT=local"
if not defined BCI_TEST_HOST_ROLE set "BCI_TEST_HOST_ROLE=single_machine_simulation"
if not defined BCI_TEST_NETWORK_PROFILE set "BCI_TEST_NETWORK_PROFILE=normal"
if not defined BCI_TEST_RECOVERY_MODE set "BCI_TEST_RECOVERY_MODE=none"

if not exist "%ROOT%tests\artifacts" mkdir "%ROOT%tests\artifacts"

echo [run_automated_tests] python=%BCI_PYTHON_EXE%
echo [run_automated_tests] profile=%PROFILE%

call :check_pytest
if errorlevel 1 (
  set "PYTEST_EXIT=1"
  goto :finish
)
call :build_pytest_plan
if errorlevel 1 (
  set "PYTEST_EXIT=1"
  goto :finish
)

echo [run_automated_tests] execution_plan=%PYTEST_PLAN_LABEL%
if defined PYTEST_INFO_1 echo [run_automated_tests] info=%PYTEST_INFO_1%
if defined PYTEST_INFO_2 echo [run_automated_tests] info=%PYTEST_INFO_2%

call :run_pytest_phase 1 "%PYTEST_ARGS_1%" "%PYTEST_PHASE_1%" "%PYTEST_MARK_EXPR_1%"
if defined PYTEST_ARGS_2 call :run_pytest_phase 2 "%PYTEST_ARGS_2%" "%PYTEST_PHASE_2%" "%PYTEST_MARK_EXPR_2%"
if defined PYTEST_ARGS_3 call :run_pytest_phase 3 "%PYTEST_ARGS_3%" "%PYTEST_PHASE_3%" "%PYTEST_MARK_EXPR_3%"

set "PYTEST_EXIT=%FINAL_EXIT%"

call :print_summary_counts
echo [run_automated_tests] latest_report=%ROOT%tests\artifacts\latest\test_report.csv
echo [run_automated_tests] latest_summary=%ROOT%tests\artifacts\latest\test_summary.csv
echo [run_automated_tests] exit_code=%PYTEST_EXIT%
goto :finish

:resolve_python
if defined BCI_PYTHON_EXE if exist "%BCI_PYTHON_EXE%" (
  call :check_python_runtime "%BCI_PYTHON_EXE%"
  if not errorlevel 1 exit /b 0
  echo [run_automated_tests] configured BCI_PYTHON_EXE failed runtime self-check: %BCI_PYTHON_EXE%
)
set "BCI_PYTHON_EXE="
call :try_python_candidate "D:\anaconda3\envs\BCI_competation_2026\python.exe"
if not defined BCI_PYTHON_EXE call :try_python_candidate "D:\anaconda3\envs\BCI_competition_2026\python.exe"
if not defined BCI_PYTHON_EXE call :try_python_candidate "D:\anaconda3\python.exe"
if not defined BCI_PYTHON_EXE call :try_python_candidate "C:\Users\admin\.conda\envs\BCI_competation_test\python.exe"
if not defined BCI_PYTHON_EXE for %%I in (python.exe) do call :try_python_candidate "%%~$PATH:I"
if not defined BCI_PYTHON_EXE (
  echo [run_automated_tests] Python not found or failed runtime self-check.
  echo [run_automated_tests] Required imports: pytest, asyncio, socket, yaml, grpc, numpy, injector, and project grpc stubs.
  exit /b 1
)
exit /b 0

:try_python_candidate
set "BCI_PYTHON_CANDIDATE=%~1"
if "%BCI_PYTHON_CANDIDATE%"=="" exit /b 0
if not exist "%BCI_PYTHON_CANDIDATE%" exit /b 0
call :check_python_runtime "%BCI_PYTHON_CANDIDATE%"
if errorlevel 1 exit /b 0
set "BCI_PYTHON_EXE=%BCI_PYTHON_CANDIDATE%"
exit /b 0

:check_python_runtime
set "BCI_PYTHON_CANDIDATE=%~1"
set "PYTHONHASHSEED=0"
"%BCI_PYTHON_CANDIDATE%" -c "import sys; sys.path.insert(0, r'%ROOT%app\Algorithm'); import pytest, asyncio, socket, yaml, grpc, numpy, injector; from Algorithm.api.proto import AlgorithmRPCService_pb2_grpc; s=socket.socket(); s.close()" >nul 2>nul
exit /b %ERRORLEVEL%

:finish
if "%PAUSE_ON_EXIT%"=="1" (
  echo.
  echo [run_automated_tests] finished. Press any key to close this window.
  pause >nul
)
endlocal & exit /b %PYTEST_EXIT%

:check_pytest
"%BCI_PYTHON_EXE%" -c "import pytest; print(pytest.__version__)" >nul 2>nul
if errorlevel 1 (
  echo [run_automated_tests] pytest is not installed in %BCI_PYTHON_EXE%.
  echo [run_automated_tests] install pytest first, then rerun this script.
  exit /b 1
)
exit /b 0

:run_pytest_phase
set "CURRENT_PHASE=%~1"
set "CURRENT_ARGS=%~2"
set "CURRENT_LABEL=%~3"
set "CURRENT_MARK_EXPR=%~4"
if "%CURRENT_ARGS%"=="" exit /b 0
echo [run_automated_tests] phase_%CURRENT_PHASE%=%CURRENT_LABEL%
if defined CURRENT_MARK_EXPR (
  echo [run_automated_tests] pytest_args=%CURRENT_ARGS% -m "%CURRENT_MARK_EXPR%"
) else (
  echo [run_automated_tests] pytest_args=%CURRENT_ARGS%
)
if defined PYTEST_COMMON_ARGS echo [run_automated_tests] pytest_common_args=%PYTEST_COMMON_ARGS%
echo [run_automated_tests] starting...
if defined CURRENT_MARK_EXPR (
  "%BCI_PYTHON_EXE%" -m pytest %PYTEST_COMMON_ARGS% %CURRENT_ARGS% -m "%CURRENT_MARK_EXPR%"
) else (
  "%BCI_PYTHON_EXE%" -m pytest %PYTEST_COMMON_ARGS% %CURRENT_ARGS%
)
set "PHASE_EXIT=%ERRORLEVEL%"
if not "%PHASE_EXIT%"=="0" set "FINAL_EXIT=%PHASE_EXIT%"
exit /b 0

:print_summary_counts
set "SUMMARY_CSV=%ROOT%tests\artifacts\latest\test_summary.csv"
if not exist "%SUMMARY_CSV%" (
  echo [run_automated_tests] summary_counts unavailable: %SUMMARY_CSV% not found
  exit /b 0
)
"%BCI_PYTHON_EXE%" -c "import csv, pathlib; path = pathlib.Path(r'%SUMMARY_CSV%'); rows = list(csv.DictReader(path.open('r', encoding='utf-8-sig', newline=''))); layer_rows = [row for row in rows if row.get('group_by') == 'layer']; total = next((row for row in layer_rows if row.get('group_value') == 'all'), None); passed = int(total.get('passed_count') or 0) if total else sum(int(row.get('passed_count') or 0) for row in layer_rows); failed = int(total.get('failed_count') or 0) if total else sum(int(row.get('failed_count') or 0) for row in layer_rows); skipped = int(total.get('skipped_count') or 0) if total else sum(int(row.get('skipped_count') or 0) for row in layer_rows); print(f'[run_automated_tests] summary_counts passed={passed} skipped={skipped} failed={failed}')" 2>nul
if errorlevel 1 (
  echo [run_automated_tests] summary_counts unavailable: failed to parse latest_summary.csv
)
exit /b 0

:build_pytest_plan
set "PYTEST_PLAN_LABEL="
set "PYTEST_INFO_1="
set "PYTEST_INFO_2="
set "PYTEST_ARGS_1="
set "PYTEST_ARGS_2="
set "PYTEST_ARGS_3="
set "PYTEST_MARK_EXPR_1="
set "PYTEST_MARK_EXPR_2="
set "PYTEST_MARK_EXPR_3="
set "PYTEST_PHASE_1="
set "PYTEST_PHASE_2="
set "PYTEST_PHASE_3="
set "PYTEST_RUN_ID=run_%RANDOM%_%RANDOM%_%RANDOM%"
set "PYTEST_BASETEMP=.pt_%PYTEST_RUN_ID%"
set "PYTEST_COMMON_ARGS=-o console_output_style=progress --basetemp=%PYTEST_BASETEMP%"
if /I "%PROFILE%"=="unit" (
  set "PYTEST_PLAN_LABEL=unit"
  set "PYTEST_ARGS_1=tests\unit"
  set "PYTEST_PHASE_1=Unit"
)
if /I "%PROFILE%"=="core" (
  set "PYTEST_PLAN_LABEL=core"
  set "PYTEST_ARGS_1=tests\unit tests\component"
  set "PYTEST_PHASE_1=Core"
)
if /I "%PROFILE%"=="nightly" (
  set "PYTEST_PLAN_LABEL=nightly"
  set "PYTEST_ARGS_1=tests"
  set "PYTEST_MARK_EXPR_1=not requires_admin and not manual_network and not heavy"
  set "PYTEST_PHASE_1=Nightly"
)
if /I "%PROFILE%"=="release" (
  set "PYTEST_PLAN_LABEL=release"
  set "PYTEST_ARGS_1=tests\unit tests\condition tests\component tests\algorithm_interface tests\integration tests\resilience tests\security tests\dashboard tests\e2e"
  set "PYTEST_MARK_EXPR_1=not heavy"
  set "PYTEST_PHASE_1=Release"
)
if /I "%PROFILE%"=="heavy" (
  set "PYTEST_PLAN_LABEL=heavy"
  set "PYTEST_INFO_1=heavy tests run last in default double-click mode; this profile runs only the real 17-team full-chain heavy layer."
  set "PYTEST_INFO_2=heavy progress format: [heavy_progress] step/percent/elapsed/eta/stage/detail. ETA is estimated from the formal 17-team stage set and may run for up to 180 minutes. Logs/artifacts: tests\artifacts\latest\heavy\real_full_chain\"
  set "PYTEST_COMMON_ARGS=-o console_output_style=progress --capture=tee-sys --basetemp=%PYTEST_BASETEMP%"
  set "PYTEST_ARGS_1=tests\heavy"
  set "PYTEST_PHASE_1=Heavy"
)
if /I "%PROFILE%"=="all" (
  set "PYTEST_PLAN_LABEL=all_with_heavy_last"
  set "PYTEST_INFO_1=default double-click mode runs one combined pytest session so the final CSV report contains standard and heavy results together."
  set "PYTEST_INFO_2=heavy tests run last: real 17-team full-chain execution with judge stack, algorithms, stage transitions and result integration. ETA is estimated from the formal stage set and the heavy layer may run for up to 180 minutes. Logs/artifacts: tests\artifacts\latest\heavy\real_full_chain\"
  set "PYTEST_COMMON_ARGS=-o console_output_style=progress --capture=tee-sys --basetemp=%PYTEST_BASETEMP%"
  set "PYTEST_ARGS_1=tests\unit tests\condition tests\component tests\algorithm_interface tests\integration tests\resilience tests\security tests\dashboard tests\e2e tests\heavy"
  set "PYTEST_PHASE_1=AllWithHeavyLast"
)
if not defined PYTEST_PLAN_LABEL (
  echo [run_automated_tests] unknown profile: %PROFILE%
  echo [run_automated_tests] allowed profiles: unit core nightly release all heavy
  exit /b 1
)
exit /b 0
