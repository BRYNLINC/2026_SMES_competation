import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from tools.recovery_runtime import prepare_resume_recovery
from tools.results_archive import archive_and_clear_active_results
from tools.shutdown_judge_stack import shutdown_judge_runtime, write_process_manifest


RESULTS_ROOT = PROJECT_ROOT / 'results'
CONTROL_ROOT = RESULTS_ROOT / 'control'
APP_ROOT = PROJECT_ROOT / 'app'
PROCEED_ROOT = PROJECT_ROOT / 'proceed'
DASHBOARD_ROOT = PROJECT_ROOT / 'judge-dashboard'
CENTRAL_CONTROLLER_CONFIG_PATH = (
    APP_ROOT / 'CentralController' / 'CentralController' / 'config' / 'CentralControllerConfig.yml'
)
RUNTIME_STAGE_LAUNCHER_CONFIG_PATH = (
    APP_ROOT / 'ProcessHub' / 'ApplicationFramework' / 'config' / 'RuntimeStageCoordinatorLauncherConfig.yml'
)
RUN_PROVENANCE_CONFIG_PATH_LIST = (
    CENTRAL_CONTROLLER_CONFIG_PATH,
    APP_ROOT / 'Collector' / 'Collector' / 'receiver' / 'virtual_receiver' / 'VirtualReceiverConfig.yml',
    APP_ROOT / 'ProcessHub' / 'ProcessHub' / 'bci_competition' / 'challenge' / 'MI' / 'ChallengeMI.yml',
    APP_ROOT / 'ProcessHub' / 'ProcessHub' / 'bci_competition' / 'task' / 'config' / 'BCICompetitionTaskFinalConfig.yml',
)


def main() -> int:
    match_start_mode = str(os.environ.get('BCI_MATCH_START_MODE') or 'clear').strip().lower()
    python_executable = normalize_executable_path(sys.executable)
    java_executable = resolve_java_executable(
        os.environ.get('BCI_JAVA_EXE'),
        os.environ.get('JAVA_HOME'),
    )
    npm_executable = resolve_npm_executable()
    python_command = build_process_command([python_executable])
    java_command = build_process_command([java_executable])
    dashboard_command = (
        build_process_command([
            npm_executable,
            'run',
            'dev',
            '--',
            '--host',
            '127.0.0.1',
            '--port',
            '5173',
        ])
        if npm_executable else None
    )

    ensure_runtime_directories()
    shutdown_report = shutdown_judge_runtime(
        project_root=PROJECT_ROOT,
        reason=f'startup_preflight_{match_start_mode}',
        quiet=False,
        timeout_seconds=45.0,
    )
    if not shutdown_report.get('clean_shutdown'):
        print('[judge-start] 预清理后仍有裁判侧端口未释放，已中止本次启动。')
        print(f"[judge-start] remaining_ports={shutdown_report.get('remaining_port_pid_map')}")
        return 1

    try:
        applied_recovery = prepare_results_root(match_start_mode)
    except PermissionError as exc:
        locked_path = getattr(exc, 'filename', None) or repr(exc)
        print('[judge-start] 无法清理 results，文件正被其他程序占用。')
        print(f'[judge-start] locked_path={locked_path}')
        print('[judge-start] 请先关闭以下可能占用 results 的程序后重试：')
        print('[judge-start] 1. DB Browser / SQLite 查看工具')
        print('[judge-start] 2. 上一次未完全退出的 JudgeWeb / ProcessHub / Collector / Dashboard')
        print('[judge-start] 3. 打开 runtime_state.db 的 Python / 编辑器插件')
        print('[judge-start] 如当前是重新开赛，请先执行 shutdown_judge.bat，再重新运行 startup_judge_clear.bat。')
        return 1
    except Exception as exc:
        print('[judge-start] 恢复预处理失败，已中止启动。')
        print(f'[judge-start] error={exc!r}')
        return 1

    run_provenance = build_run_provenance(match_start_mode, applied_recovery)
    write_launcher_manifest(
        match_start_mode,
        python_executable,
        java_executable,
        npm_executable,
        applied_recovery,
        run_provenance,
    )

    launched_process_row_list: list[dict] = []
    process_manifest_metadata = {
        'match_start_mode': match_start_mode,
        'applied_recovery': applied_recovery,
        'run_provenance': run_provenance,
    }
    write_process_manifest(PROJECT_ROOT, launched_process_row_list, metadata=process_manifest_metadata)

    print(f'[judge-start] mode={match_start_mode}')
    print(f'[judge-start] python={python_executable}')
    print(f'[judge-start] java={java_executable}')
    if npm_executable:
        print(f'[judge-start] npm={npm_executable}')
    else:
        print('[judge-start] npm=NOT FOUND, judge-dashboard will not be auto-started')
    if applied_recovery:
        print(f"[judge-start] recovery_mode={applied_recovery.get('recovery_mode')}")
        if applied_recovery.get('stage'):
            stage = applied_recovery['stage']
            print(
                '[judge-start] recovery_stage='
                f"{stage.get('subject_id')} / {stage.get('exp_name')} / {stage.get('exp_task')}"
            )

    def launch_component(title: str, cwd: Path, command: str, extra_env: dict | None = None) -> None:
        process_row = start_component_window(
            title=title,
            cwd=cwd,
            command=command,
            extra_env=extra_env,
        )
        launched_process_row_list.append(process_row)
        write_process_manifest(PROJECT_ROOT, launched_process_row_list, metadata=process_manifest_metadata)

    launch_component(
        title='[BCI Judge] Central Java Controller',
        cwd=PROCEED_ROOT / 'centrol',
        command=f'{java_command} -jar centrol.jar',
    )
    time.sleep(15)

    launch_component(
        title='[BCI Judge] CentralController Python',
        cwd=APP_ROOT / 'CentralController',
        command=f'{python_command} -m ApplicationFramework.main',
    )

    launch_component(
        title='[BCI Judge] RuntimeStageCoordinator Python',
        cwd=APP_ROOT / 'ProcessHub',
        command=f'{python_command} -m ApplicationFramework.main',
        extra_env={
            'LAUNCHER_CONFIG_PATH': str(RUNTIME_STAGE_LAUNCHER_CONFIG_PATH),
        },
    )

    launch_component(
        title='[BCI Judge] Collector Java Bridge',
        cwd=PROCEED_ROOT / 'collector',
        command=f'{java_command} -jar collector.jar',
    )
    launch_component(
        title='[BCI Judge] Task Java Bridge',
        cwd=PROCEED_ROOT / 'task',
        command=f'{java_command} -jar task.jar',
    )
    time.sleep(15)

    launch_component(
        title='[BCI Judge] Collector Python',
        cwd=APP_ROOT / 'Collector',
        command=f'{python_command} -m ApplicationFramework.main',
    )

    for component_id in load_processor_component_id_list():
        launch_component(
            title=f'[BCI Judge] ProcessHub {component_id}',
            cwd=APP_ROOT / 'ProcessHub',
            command=f'{python_command} -m ApplicationFramework.main',
            extra_env={
                'COMPONENT_ID': component_id,
            },
        )

    launch_component(
        title='[BCI Judge] JudgeWeb',
        cwd=APP_ROOT / 'JudgeWeb',
        command=f'{python_command} -m JudgeWeb.main',
    )
    judge_web_ready = wait_for_http_service(
        'http://127.0.0.1:18080/healthz',
        timeout_seconds=12.0,
        service_name='JudgeWeb',
    )
    if dashboard_command and DASHBOARD_ROOT.exists():
        launch_component(
            title='[BCI Judge] Judge Dashboard',
            cwd=DASHBOARD_ROOT,
            command=dashboard_command,
        )
        wait_for_http_service(
            'http://127.0.0.1:5173',
            timeout_seconds=20.0,
            service_name='Judge Dashboard',
            success_hint='dashboard dev server responded',
        )
    if not judge_web_ready:
        print('[judge-start] JudgeWeb 未成功监听 18080，请查看 app/JudgeWeb/JudgeWeb/log/judgeWeb.log 和 JudgeWeb 控制台窗口。')
    print('[judge-start] judge web url: http://127.0.0.1:18080')
    print('[judge-start] judge dashboard url: http://127.0.0.1:5173')
    print('[judge-start] all judge-side components launched')
    return 0



def wait_for_http_service(
    url: str,
    timeout_seconds: float,
    service_name: str,
    success_hint: str | None = None,
) -> bool:
    deadline = time.time() + timeout_seconds
    last_error: str | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as response:
                if 200 <= int(response.status) < 500:
                    if success_hint:
                        print(f'[judge-start] {service_name} ready: {success_hint}')
                    else:
                        print(f'[judge-start] {service_name} ready: {url}')
                    return True
        except urllib.error.URLError as exc:
            last_error = str(exc)
        except Exception as exc:
            last_error = repr(exc)
        time.sleep(1.0)
    print(f'[judge-start] {service_name} health check failed after {timeout_seconds:.0f}s: url={url} last_error={last_error}')
    return False


def ensure_runtime_directories() -> None:
    for relative_dir in (
        Path('app/Algorithm/Algorithm/log'),
        Path('app/CentralController/ApplicationFramework/log'),
        Path('app/CentralController/CentralController/log'),
        Path('app/Collector/ApplicationFramework/log'),
        Path('app/Collector/Collector/log'),
        Path('app/ProcessHub/ApplicationFramework/log'),
        Path('app/ProcessHub/ProcessHub/log'),
        Path('results/live'),
        Path('results/control'),
        Path('results/history'),
    ):
        (PROJECT_ROOT / relative_dir).mkdir(parents=True, exist_ok=True)



def prepare_results_root(match_start_mode: str) -> dict:
    if match_start_mode == 'resume':
        RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
        return prepare_resume_recovery(PROJECT_ROOT)
    history_archive = archive_and_clear_active_results(PROJECT_ROOT, 'startup_clear')
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
    (RESULTS_ROOT / 'live').mkdir(parents=True, exist_ok=True)
    (RESULTS_ROOT / 'control').mkdir(parents=True, exist_ok=True)
    (RESULTS_ROOT / 'history').mkdir(parents=True, exist_ok=True)
    return {
        'recovery_mode': 'clear_start',
        'stage': None,
        'collector_start_selector': None,
        'requested_at': None,
        'applied_at': time.time(),
        'history_archive': history_archive,
    }



def write_launcher_manifest(
    match_start_mode: str,
    python_executable: str,
    java_executable: str,
    npm_executable: str | None,
    applied_recovery: dict | None,
    run_provenance: dict | None = None,
) -> None:
    CONTROL_ROOT.mkdir(parents=True, exist_ok=True)
    payload = {
        'match_start_mode': match_start_mode,
        'python_executable': python_executable,
        'java_executable': java_executable,
        'npm_executable': npm_executable,
        'processor_component_id_list': load_processor_component_id_list(),
        'applied_recovery': applied_recovery,
        'run_provenance': run_provenance or build_run_provenance(
            match_start_mode,
            applied_recovery,
        ),
        'judge_process_manifest_path': str(_resolve_process_manifest_path()),
        'launched_at': time.time(),
    }
    file_path = CONTROL_ROOT / 'launcher_manifest.json'
    temp_file_path = file_path.with_suffix(file_path.suffix + '.tmp')
    temp_file_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    temp_file_path.replace(file_path)


def build_run_provenance(match_start_mode: str, applied_recovery: dict | None) -> dict:
    recovery_mode = str((applied_recovery or {}).get('recovery_mode') or '').strip()
    clean_full_run = match_start_mode == 'clear' and recovery_mode == 'clear_start'
    git_revision = _read_git_value(['rev-parse', 'HEAD'])
    git_status = _read_git_value(
        ['status', '--short', '--untracked-files=no'],
        allow_empty=True,
    )
    return {
        'run_kind': 'clean_full_run' if clean_full_run else 'recovery_run',
        'project_root': str(PROJECT_ROOT.resolve()),
        'git_revision': git_revision,
        'git_tracked_dirty': None if git_status is None else bool(git_status),
        'config_sha256_by_path': {
            _provenance_path_label(config_path): _sha256_file(config_path)
            for config_path in RUN_PROVENANCE_CONFIG_PATH_LIST
            if config_path.is_file()
        },
    }


def _read_git_value(argument_list: list[str], *, allow_empty: bool = False) -> str | None:
    try:
        result = subprocess.run(
            ['git', *argument_list],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
            timeout=5.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value if value or allow_empty else None


def _sha256_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open('rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _provenance_path_label(file_path: Path) -> str:
    try:
        return file_path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return file_path.resolve().as_posix()



def load_processor_component_id_list() -> list[str]:
    if not CENTRAL_CONTROLLER_CONFIG_PATH.exists():
        return []
    with CENTRAL_CONTROLLER_CONFIG_PATH.open('r', encoding='utf-8') as file:
        config_dict = yaml.safe_load(file) or {}
    component_dict = config_dict.get('components') or {}
    component_id_list = [
        component_id
        for component_id, component_config in component_dict.items()
        if (component_config or {}).get('component_type') == 'PROCESSOR'
    ]
    component_id_list.sort()
    return component_id_list



def normalize_executable_path(raw_value: str) -> str:
    normalized_value = str(raw_value or '').strip()
    normalized_value = normalized_value.replace(r'\"', '"').replace(r"\'", "'")
    for _ in range(4):
        previous_value = normalized_value
        if len(normalized_value) >= 2 and (
            (normalized_value[0] == '"' and normalized_value[-1] == '"')
            or (normalized_value[0] == "'" and normalized_value[-1] == "'")
        ):
            normalized_value = normalized_value[1:-1].strip()
        if previous_value == normalized_value:
            break
    return normalized_value or raw_value


def normalize_environment_path(raw_value: str | None) -> str:
    normalized_value = normalize_executable_path(str(raw_value or '')).strip()
    if len(normalized_value) >= 3 and normalized_value[0] in ('\\', '/') and normalized_value[2] == ':':
        normalized_value = normalized_value[1:]
    while len(normalized_value) > 3 and normalized_value[-1] in ('\\', '/'):
        normalized_value = normalized_value[:-1]
    return normalized_value


def resolve_java_executable(raw_java_executable: str | None, raw_java_home: str | None) -> str:
    candidate_list: list[str] = []
    configured_java = normalize_executable_path(str(raw_java_executable or '')).strip()
    if configured_java and configured_java.lower() != 'java':
        configured_path = Path(configured_java)
        if configured_path.is_dir():
            candidate_list.extend(
                str(configured_path / relative_path)
                for relative_path in (
                    Path('bin/java.exe'),
                    Path('bin/java'),
                    Path('jre/bin/java.exe'),
                    Path('jre/bin/java'),
                )
            )
        else:
            candidate_list.append(configured_java)
    bundled_java = PROJECT_ROOT / 'jdk8' / 'bin' / 'java.exe'
    candidate_list.append(str(bundled_java))
    java_home = normalize_environment_path(raw_java_home)
    if java_home:
        if Path(java_home).name.lower() in {'java.exe', 'java'}:
            candidate_list.append(java_home)
        candidate_list.append(str(Path(java_home) / 'bin' / 'java.exe'))
    for candidate in candidate_list:
        normalized_candidate = normalize_executable_path(candidate)
        if normalized_candidate and Path(normalized_candidate).is_file():
            return normalized_candidate
    discovered_java = shutil.which('java')
    if discovered_java:
        return normalize_executable_path(discovered_java)
    return configured_java or 'java'



def build_process_command(command_part_list: list[str]) -> str:
    normalized_command_part_list = [
        normalize_executable_path(command_part)
        for command_part in command_part_list
        if str(command_part or '').strip() != ''
    ]
    return subprocess.list2cmdline(normalized_command_part_list)



def resolve_npm_executable() -> str | None:
    raw_value = str(os.environ.get('BCI_NPM_EXE') or '').strip()
    if raw_value:
        return normalize_executable_path(raw_value)
    for command_name in ('npm.cmd', 'npm'):
        command_path = shutil.which(command_name)
        if command_path:
            return normalize_executable_path(command_path)
    return None



def _resolve_process_manifest_path() -> Path:
    return CONTROL_ROOT / 'judge_process_manifest.json'



def start_component_window(title: str, cwd: Path, command: str, extra_env: dict | None = None) -> dict:
    env = os.environ.copy()
    if extra_env:
        env.update({key: str(value) for key, value in extra_env.items()})
    launcher_script_path = write_component_launcher_script(title, cwd, command)
    process = subprocess.Popen(
        ['cmd', '/k', str(launcher_script_path)],
        cwd=str(cwd),
        env=env,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )
    return {
        'title': title,
        'pid': process.pid,
        'cwd': str(cwd),
        'command': command,
        'launcher_script_path': str(launcher_script_path),
        'started_at': time.time(),
    }


def write_component_launcher_script(title: str, cwd: Path, command: str) -> Path:
    launcher_directory = CONTROL_ROOT / 'startup_commands'
    launcher_directory.mkdir(parents=True, exist_ok=True)
    safe_title = ''.join(character if character.isalnum() else '_' for character in title).strip('_')
    safe_title = safe_title[:48] or 'component'
    launcher_script_path = launcher_directory / f'{time.time_ns()}_{safe_title}.cmd'
    launcher_script_path.write_text(
        '\n'.join(
            [
                '@echo off',
                'chcp 65001 >nul',
                f'title {escape_cmd_title(title)}',
                'rem The Python launcher sets this window working directory.',
                command,
                '',
            ]
        ),
        encoding='utf-8',
    )
    return launcher_script_path


def escape_cmd_title(title: str) -> str:
    return str(title).replace('^', '^^').replace('&', '^&').replace('|', '^|').replace('<', '^<').replace('>', '^>')


if __name__ == '__main__':
    raise SystemExit(main())
