@echo off
setlocal
set "ROOT=%~dp0.."
cd /d "%ROOT%"
call :resolve_python || exit /b 1
set "PYTHONHASHSEED=0"
echo [auto-recovery] stage restart resume scheduled.
"%BCI_PYTHON_EXE%" "%ROOT%\tools\shutdown_judge_stack.py" --reason restart_from_stage_auto_resume --timeout-seconds 60
if errorlevel 1 (
  echo [auto-recovery] shutdown wait failed, abort resume startup.
  exit /b %errorlevel%
)
call "%ROOT%\startup_judge_resume.bat"
if errorlevel 1 exit /b %errorlevel%
set "START_MATCH_URL=http://127.0.0.1:18080/api/v1/control/start-match"
for /L %%I in (1,1,120) do (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri '%START_MATCH_URL%' -TimeoutSec 5; if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) { exit 0 }; exit 1 } catch { exit 1 }"
  if not errorlevel 1 goto :done
  ping 127.0.0.1 -n 3 >nul
)
echo [auto-recovery] start-match readiness wait timed out.
:done
exit /b 0

:resolve_python
set "BCI_PYTHON_EXE=C:\Users\admin\.conda\envs\BCI_competation_test\python.exe"
if exist "%ROOT%\Python310\python.exe" set "BCI_PYTHON_EXE=%ROOT%\Python310\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competation_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competation_2026\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competition_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competition_2026\python.exe"
if not defined BCI_PYTHON_EXE for %%I in (python.exe) do set "BCI_PYTHON_EXE=%%~$PATH:I"
if not defined BCI_PYTHON_EXE (
  echo [auto-recovery] Python not found.
  exit /b 1
)
exit /b 0
