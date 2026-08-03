@echo off
setlocal
set "ROOT=%~dp0"
call :resolve_python || exit /b 1
call :resolve_java || exit /b 1
call :resolve_npm
set "BCI_MATCH_START_MODE=resume"
set "BCI_JAVA_EXE=%BCI_JAVA_EXE%"
set "PYTHONHASHSEED=0"
if defined BCI_NPM_EXE set "BCI_NPM_EXE=%BCI_NPM_EXE%"
"%BCI_PYTHON_EXE%" "%ROOT%tools\start_judge_stack.py"
if errorlevel 1 (
  echo [startup_judge_resume] startup failed with exit code %errorlevel%.
  pause
  exit /b %errorlevel%
)
exit /b 0

:resolve_python
if defined BCI_PYTHON_EXE if exist "%BCI_PYTHON_EXE%" exit /b 0
set "BCI_PYTHON_EXE=C:\Users\admin\.conda\envs\BCI_competation_test\python.exe"
if exist "%ROOT%Python310\python.exe" set "BCI_PYTHON_EXE=%ROOT%Python310\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\software\anaconda\envs\world_robot_env\python.exe" set "BCI_PYTHON_EXE=D:\software\anaconda\envs\world_robot_env\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competation_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competation_2026\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competition_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competition_2026\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\python.exe"
if not defined BCI_PYTHON_EXE for %%I in (python.exe) do set "BCI_PYTHON_EXE=%%~$PATH:I"
if not defined BCI_PYTHON_EXE (
  echo [startup_judge_resume] Python not found.
  pause
  exit /b 1
)
exit /b 0

:resolve_java
if defined BCI_JAVA_EXE if exist "%BCI_JAVA_EXE%" exit /b 0
set "BCI_JAVA_EXE="
if exist "%ROOT%jdk8\bin\java.exe" set "BCI_JAVA_EXE=%ROOT%jdk8\bin\java.exe"
if not defined BCI_JAVA_EXE if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "BCI_JAVA_EXE=%JAVA_HOME%\bin\java.exe"
if not defined BCI_JAVA_EXE if exist "C:\Program Files\Common Files\Oracle\Java\javapath\java.exe" set "BCI_JAVA_EXE=C:\Program Files\Common Files\Oracle\Java\javapath\java.exe"
if not defined BCI_JAVA_EXE (
  where java >nul 2>nul
  if not errorlevel 1 set "BCI_JAVA_EXE=java"
)
if not defined BCI_JAVA_EXE (
  echo [startup_judge_resume] Java not found.
  pause
  exit /b 1
)
exit /b 0

:resolve_npm
set "BCI_NPM_EXE="
if exist "%ROOT%nodejs\npm.cmd" set "BCI_NPM_EXE=%ROOT%nodejs\npm.cmd"
if not defined BCI_NPM_EXE for %%I in (npm.cmd) do set "BCI_NPM_EXE=%%~$PATH:I"
if not defined BCI_NPM_EXE for %%I in (npm.exe) do set "BCI_NPM_EXE=%%~$PATH:I"
if not defined BCI_NPM_EXE (
  echo [startup_judge_resume] npm not found, judge-dashboard will not be auto-started.
)
exit /b 0
