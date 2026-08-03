@echo off
setlocal
for %%I in ("%~dp0.") do set "originalPath=%%~fI"
set "pythonExe=D:\anaconda3\envs\BCI_competation_2026\python.exe"

if not exist "%pythonExe%" (
    echo [startup] Python not found: %pythonExe%
    pause
    exit /b 1
)

if not exist "%originalPath%\app\Algorithm\Algorithm\log" mkdir "%originalPath%\app\Algorithm\Algorithm\log"
if not exist "%originalPath%\app\CentralController\ApplicationFramework\log" mkdir "%originalPath%\app\CentralController\ApplicationFramework\log"
if not exist "%originalPath%\app\CentralController\CentralController\log" mkdir "%originalPath%\app\CentralController\CentralController\log"
if not exist "%originalPath%\app\Collector\ApplicationFramework\log" mkdir "%originalPath%\app\Collector\ApplicationFramework\log"
if not exist "%originalPath%\app\Collector\Collector\log" mkdir "%originalPath%\app\Collector\Collector\log"
if not exist "%originalPath%\app\ProcessHub\ApplicationFramework\log" mkdir "%originalPath%\app\ProcessHub\ApplicationFramework\log"
if not exist "%originalPath%\app\ProcessHub\ProcessHub\log" mkdir "%originalPath%\app\ProcessHub\ProcessHub\log"

echo [startup] Launching Central Java Controller
start "[BCI] Central Java Controller" /D "%originalPath%\proceed\centrol" cmd /k "title [BCI] Central Java Controller && java -jar centrol.jar"
timeout /t 15 /nobreak

echo [startup] Launching CentralController Python
start "[BCI] CentralController Python" /D "%originalPath%\app\CentralController" cmd /k "title [BCI] CentralController Python && ""%pythonExe%"" -m ApplicationFramework.main"

set "LAUNCHER_CONFIG_PATH=%originalPath%\app\ProcessHub\ApplicationFramework\config\RuntimeStageCoordinatorLauncherConfig.yml"
echo [startup] Launching RuntimeStageCoordinator Python
start "[BCI] RuntimeStageCoordinator Python" /D "%originalPath%\app\ProcessHub" cmd /k "title [BCI] RuntimeStageCoordinator Python && ""%pythonExe%"" -m ApplicationFramework.main"
set "LAUNCHER_CONFIG_PATH="

echo [startup] Launching Collector Java Bridge
start "[BCI] Collector Java Bridge" /D "%originalPath%\proceed\collector" cmd /k "title [BCI] Collector Java Bridge && java -jar collector.jar"

echo [startup] Launching Task Java Bridge
start "[BCI] Task Java Bridge" /D "%originalPath%\proceed\task" cmd /k "title [BCI] Task Java Bridge && java -jar task.jar"

echo [startup] Launching Algorithm Python
start "[BCI] Algorithm Python" /D "%originalPath%\app\Algorithm" cmd /k "title [BCI] Algorithm Python && ""%pythonExe%"" -m Algorithm.main"
timeout /t 15 /nobreak

echo [startup] Launching Collector Python
start "[BCI] Collector Python" /D "%originalPath%\app\Collector" cmd /k "title [BCI] Collector Python && ""%pythonExe%"" -m ApplicationFramework.main"

echo [startup] Launching ProcessHub Python
start "[BCI] ProcessHub Python" /D "%originalPath%\app\ProcessHub" cmd /k "title [BCI] ProcessHub Python && ""%pythonExe%"" -m ApplicationFramework.main"

endlocal
