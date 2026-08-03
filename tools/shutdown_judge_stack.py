import argparse
import json
import os
import subprocess
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_ROOT = PROJECT_ROOT / 'results'
CONTROL_ROOT = RESULTS_ROOT / 'control'
KEY_PORT_LIST = [18080, 5173, 7963, 8972, 9000, 9002, 9003, 8864]


def _print(message: str, quiet: bool = False) -> None:
    if not quiet:
        print(message)


def _run_command(command_part_list: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command_part_list,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='ignore',
    )


def _read_json_file(file_path: Path):
    if not file_path.exists():
        return None
    try:
        return json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None


def _remove_file(file_path: Path) -> None:
    try:
        if file_path.exists():
            file_path.unlink()
    except OSError:
        return


def _resolve_process_manifest_path(project_root: Path) -> Path:
    return project_root / 'results' / 'control' / 'judge_process_manifest.json'


def write_process_manifest(project_root: Path, process_row_list: list[dict], metadata: dict | None = None) -> None:
    process_manifest_path = _resolve_process_manifest_path(project_root)
    process_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'project_root': str(project_root),
        'generated_at': time.time(),
        'processes': process_row_list,
    }
    if metadata:
        payload['metadata'] = metadata
    temp_file_path = process_manifest_path.with_suffix(process_manifest_path.suffix + '.tmp')
    temp_file_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    temp_file_path.replace(process_manifest_path)


def _find_listening_port_pid_map(port_list: list[int]) -> dict[int, list[int]]:
    result = _run_command(['cmd', '/c', 'netstat -ano -p tcp'])
    if result.returncode not in {0, 1}:
        return {}

    pid_set_by_port: dict[int, set[int]] = {}
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if line == '':
            continue
        part_list = line.split()
        if len(part_list) < 5:
            continue
        protocol, local_address, _, state, pid_text = part_list[:5]
        if protocol.upper() != 'TCP':
            continue
        if state.upper() != 'LISTENING':
            continue
        if not pid_text.isdigit():
            continue
        pid_value = int(pid_text)
        for port in port_list:
            if local_address.endswith(f':{port}'):
                pid_set_by_port.setdefault(port, set()).add(pid_value)
                break

    return {
        port: sorted(pid_set)
        for port, pid_set in pid_set_by_port.items()
        if pid_set
    }


def _query_process_name(pid_value: int) -> str | None:
    if pid_value <= 0:
        return None
    result = _run_command(
        [
            'powershell',
            '-NoProfile',
            '-Command',
            (
                f"$process = Get-Process -Id {pid_value} -ErrorAction SilentlyContinue; "
                "if ($process) { $process.ProcessName }"
            ),
        ]
    )
    if result.returncode != 0:
        return None
    process_name = (result.stdout or '').strip()
    return process_name or None


def _kill_pid_tree(pid_value: int, quiet: bool, reason: str) -> bool:
    if pid_value <= 0 or pid_value == os.getpid():
        return False
    if pid_value == 4:
        _print(f'[judge-shutdown] skip system pid on {reason}: pid={pid_value}', quiet)
        return False
    result = _run_command(['taskkill', '/PID', str(pid_value), '/T', '/F'])
    succeeded = result.returncode == 0
    if succeeded:
        _print(f'[judge-shutdown] killed pid tree: pid={pid_value} reason={reason}', quiet)
        return True

    merged_output = '\n'.join(
        text.strip()
        for text in (result.stdout, result.stderr)
        if str(text or '').strip() != ''
    )
    if 'not found' in merged_output.lower() or 'no running instance' in merged_output.lower():
        _print(f'[judge-shutdown] pid already exited: pid={pid_value} reason={reason}', quiet)
        return False

    _print(
        f'[judge-shutdown] failed to kill pid tree: pid={pid_value} reason={reason} detail={merged_output}',
        quiet,
    )
    return False


def _kill_recorded_processes(project_root: Path, quiet: bool) -> int:
    process_manifest_path = _resolve_process_manifest_path(project_root)
    manifest_dict = _read_json_file(process_manifest_path) or {}
    process_row_list = manifest_dict.get('processes') or []
    killed_count = 0
    for process_row in process_row_list:
        pid_value = int(process_row.get('pid') or 0)
        title = str(process_row.get('title') or '').strip() or 'unknown'
        process_name = (_query_process_name(pid_value) or '').strip().lower()
        if process_name not in {'cmd', 'cmd.exe'}:
            continue
        if _kill_pid_tree(pid_value, quiet, f'manifest:{title}'):
            killed_count += 1
    return killed_count


def _kill_title_window_processes(quiet: bool) -> bool:
    result = _run_command(['taskkill', '/FI', 'WINDOWTITLE eq [BCI Judge]*', '/T', '/F'])
    merged_output = '\n'.join(
        text.strip()
        for text in (result.stdout, result.stderr)
        if str(text or '').strip() != ''
    )
    succeeded = result.returncode == 0
    if succeeded:
        _print('[judge-shutdown] killed window-title matched judge consoles', quiet)
        return True
    if 'no tasks are running' in merged_output.lower():
        _print('[judge-shutdown] no judge console windows found', quiet)
        return False
    _print(f'[judge-shutdown] title cleanup detail={merged_output}', quiet)
    return False


def _kill_listening_port_owners(port_list: list[int], quiet: bool) -> int:
    port_pid_map = _find_listening_port_pid_map(port_list)
    killed_count = 0
    for port, pid_list in sorted(port_pid_map.items()):
        for pid_value in pid_list:
            if _kill_pid_tree(pid_value, quiet, f'port:{port}'):
                killed_count += 1
    return killed_count


def _wait_for_ports_released(port_list: list[int], timeout_seconds: float, quiet: bool) -> tuple[bool, dict[int, list[int]]]:
    deadline = time.time() + timeout_seconds
    remaining_port_pid_map: dict[int, list[int]] = {}
    while time.time() < deadline:
        remaining_port_pid_map = _find_listening_port_pid_map(port_list)
        if not remaining_port_pid_map:
            _print('[judge-shutdown] key judge-side ports are fully released', quiet)
            return True, {}
        time.sleep(1.0)
    _print(
        f'[judge-shutdown] wait for port release timed out: remaining={remaining_port_pid_map}',
        quiet,
    )
    return False, remaining_port_pid_map


def shutdown_judge_runtime(
    project_root: Path,
    reason: str = 'manual',
    quiet: bool = False,
    timeout_seconds: float = 45.0,
) -> dict:
    _print(f'[judge-shutdown] begin shutdown: reason={reason}', quiet)
    process_manifest_path = _resolve_process_manifest_path(project_root)
    process_manifest_path.parent.mkdir(parents=True, exist_ok=True)

    manifest_kill_count = _kill_recorded_processes(project_root, quiet)
    title_killed = _kill_title_window_processes(quiet)
    port_kill_count = _kill_listening_port_owners(KEY_PORT_LIST, quiet)
    clean_shutdown, remaining_port_pid_map = _wait_for_ports_released(KEY_PORT_LIST, timeout_seconds, quiet)

    _remove_file(process_manifest_path)

    return {
        'reason': reason,
        'manifest_kill_count': manifest_kill_count,
        'title_killed': title_killed,
        'port_kill_count': port_kill_count,
        'clean_shutdown': clean_shutdown,
        'remaining_port_pid_map': remaining_port_pid_map,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Shutdown judge-side runtime and wait for port release.')
    parser.add_argument('--reason', default='manual', help='Shutdown reason for logs.')
    parser.add_argument('--timeout-seconds', type=float, default=45.0, help='Wait timeout for port release.')
    parser.add_argument('--quiet', action='store_true', help='Reduce stdout logs.')
    args = parser.parse_args()

    report = shutdown_judge_runtime(
        project_root=PROJECT_ROOT,
        reason=str(args.reason or 'manual'),
        quiet=bool(args.quiet),
        timeout_seconds=float(args.timeout_seconds or 45.0),
    )
    if report.get('clean_shutdown'):
        return 0
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
