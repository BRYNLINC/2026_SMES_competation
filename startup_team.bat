@echo off
setlocal
set "ROOT=%~dp0"
call :resolve_python || exit /b 1
call :terminate_existing_team_windows
call :cleanup_listening_port
call :ensure_port_available || exit /b 1
call :ensure_firewall_rule
set "PYTHONHASHSEED=0"
start "[BCI Team] Algorithm Python" /D "%ROOT%app\Algorithm" cmd /k "title [BCI Team] Algorithm Python && ""%BCI_PYTHON_EXE%"" -m Algorithm.main"
if errorlevel 1 (
  echo [startup_team] startup failed with exit code %errorlevel%.
  pause
  exit /b %errorlevel%
)
exit /b 0

:terminate_existing_team_windows
taskkill /FI "WINDOWTITLE eq [BCI Team]*" /T /F >nul 2>nul
exit /b 0

:cleanup_listening_port
set "BCI_PORT_CLEANED_PID_LIST="
for /f "tokens=5" %%a in ('netstat -aon ^| findstr /R /C:":9981 .*LISTENING"') do (
  call :kill_pid %%a
)
if defined BCI_PORT_CLEANED_PID_LIST (
  echo [startup_team] Killed stale listeners on TCP 9981: %BCI_PORT_CLEANED_PID_LIST%
  timeout /t 1 >nul
)
exit /b 0

:kill_pid
set "BCI_KILL_PID=%~1"
if "%BCI_KILL_PID%"=="" exit /b 0
echo [startup_team] Reclaiming TCP 9981 from PID %BCI_KILL_PID%...
taskkill /F /PID %BCI_KILL_PID% >nul 2>nul
if errorlevel 1 (
  echo [startup_team] WARNING: failed to kill PID %BCI_KILL_PID%.
  exit /b 0
)
if defined BCI_PORT_CLEANED_PID_LIST (
  set "BCI_PORT_CLEANED_PID_LIST=%BCI_PORT_CLEANED_PID_LIST%,%BCI_KILL_PID%"
) else (
  set "BCI_PORT_CLEANED_PID_LIST=%BCI_KILL_PID%"
)
exit /b 0

:ensure_port_available
set "BCI_PORT_IN_USE_PID="
for /f "tokens=5" %%a in ('netstat -aon ^| findstr /R /C:":9981 .*LISTENING"') do (
  set "BCI_PORT_IN_USE_PID=%%a"
  goto :port_found
)
exit /b 0

:port_found
echo [startup_team] ERROR: TCP 9981 is already occupied by PID %BCI_PORT_IN_USE_PID%.
echo [startup_team] ERROR: automatic cleanup did not succeed, please check this process manually.
tasklist /FI "PID eq %BCI_PORT_IN_USE_PID%"
pause
exit /b 1

:ensure_firewall_rule
set "BCI_FIREWALL_RULE_NAME=BCI Competition Algorithm 9981"
netsh advfirewall firewall show rule name="%BCI_FIREWALL_RULE_NAME%" >nul 2>&1
if not errorlevel 1 (
  echo [startup_team] Firewall rule already exists: "%BCI_FIREWALL_RULE_NAME%".
  exit /b 0
)
netsh advfirewall firewall add rule name="%BCI_FIREWALL_RULE_NAME%" dir=in action=allow protocol=TCP localport=9981 >nul 2>&1
if errorlevel 1 (
  echo [startup_team] WARNING: failed to add inbound firewall rule for TCP 9981.
  echo [startup_team] WARNING: please run this script as Administrator once, or manually allow TCP 9981 inbound.
  exit /b 0
)
echo [startup_team] Added inbound firewall rule for TCP 9981.
exit /b 0

:resolve_python
if defined BCI_PYTHON_EXE if exist "%BCI_PYTHON_EXE%" exit /b 0
set "BCI_PYTHON_EXE="
if exist "%ROOT%Python310\python.exe" set "BCI_PYTHON_EXE=%ROOT%Python310\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\software\anaconda\envs\world_robot_env\python.exe" set "BCI_PYTHON_EXE=D:\software\anaconda\envs\world_robot_env\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competation_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competation_2026\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\envs\BCI_competition_2026\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\envs\BCI_competition_2026\python.exe"
if not defined BCI_PYTHON_EXE if exist "D:\anaconda3\python.exe" set "BCI_PYTHON_EXE=D:\anaconda3\python.exe"
if not defined BCI_PYTHON_EXE for %%I in (python.exe) do set "BCI_PYTHON_EXE=%%~$PATH:I"
if not defined BCI_PYTHON_EXE (
  echo [startup_team] Python not found.
  pause
  exit /b 1
)
exit /b 0
