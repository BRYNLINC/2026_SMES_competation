@echo off
setlocal enabledelayedexpansion


set PORTS=9001 9002 9003 9004 9005 9006


for %%p in (%PORTS%) do (
    echo Checking port %%p...
    for /f "tokens=5" %%a in ('netstat -aon ^| findstr /R /C:":%%p[" /C:"LISTENING"') do (
        set pid=%%a
        echo Found process with PID !pid! using port %%p.
        taskkill /F /PID !pid!
    )
)

echo Done.
endlocal