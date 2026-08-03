@echo off
taskkill /FI "WINDOWTITLE eq [BCI Team]*" /T /F >nul 2>nul
for /f "tokens=5" %%a in ('netstat -aon ^| findstr /R /C:":9981 .*LISTENING"') do (
  taskkill /F /PID %%a >nul 2>nul
)
echo Team algorithm shutdown signal sent.
pause
