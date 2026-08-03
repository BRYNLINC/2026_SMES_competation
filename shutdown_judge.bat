@echo off
setlocal
set "ROOT=%~dp0"
call :resolve_python || exit /b 1
set "PYTHONHASHSEED=0"
"%BCI_PYTHON_EXE%" "%ROOT%tools\shutdown_judge_stack.py" --reason manual_shutdown
if errorlevel 1 (
  echo [shutdown_judge] judge-side ports are still occupied after shutdown attempt.
  pause
  exit /b %errorlevel%
)
echo Judge components shutdown completed.
pause
exit /b 0

:resolve_python
set "BCI_PYTHON_EXE="
if exist "%ROOT%Python310\python.exe" set "BCI_PYTHON_EXE=%ROOT%Python310\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competation_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competation_2026\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competition_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competition_2026\python.exe"
if not defined BCI_PYTHON_EXE for %%I in (python.exe) do set "BCI_PYTHON_EXE=%%~$PATH:I"
if not defined BCI_PYTHON_EXE (
  echo [shutdown_judge] Python not found.
  pause
  exit /b 1
)
exit /b 0
