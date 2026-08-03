from __future__ import annotations

import os
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen[str]
    stdout_path: Path
    stderr_path: Path
    cwd: Path
    command: list[str]
    started_at: float

    @property
    def pid(self) -> int:
        return int(self.process.pid)


def build_subprocess_env(env: dict[str, str] | None = None) -> dict[str, str]:
    final_env = os.environ.copy()
    final_env.setdefault("PYTHONHASHSEED", "0")
    if env:
        final_env.update(env)
    if os.name == "nt":
        system_root = str(
            final_env.get("SystemRoot")
            or final_env.get("WINDIR")
            or r"C:\Windows"
        )
        final_env.setdefault("SystemRoot", system_root)
        final_env.setdefault("WINDIR", system_root)
        final_env.setdefault("ComSpec", str(Path(system_root) / "System32" / "cmd.exe"))
        system_path_list = [
            str(Path(system_root)),
            str(Path(system_root) / "System32"),
            str(Path(system_root) / "System32" / "Wbem"),
            str(Path(system_root) / "System32" / "WindowsPowerShell" / "v1.0"),
        ]
        current_path_list = [
            item for item in str(final_env.get("PATH") or "").split(os.pathsep) if item.strip()
        ]
        normalized_path_set = {item.lower() for item in current_path_list}
        merged_path_list = list(current_path_list)
        for system_path in reversed(system_path_list):
            if system_path.lower() not in normalized_path_set:
                merged_path_list.insert(0, system_path)
        final_env["PATH"] = os.pathsep.join(merged_path_list)
    return final_env


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _is_port_listening_by_netstat(port):
            return True
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1.0)
                sock.connect((host, port))
                return True
        except OSError:
            time.sleep(0.2)
    return False


def wait_for_http(url: str, timeout: float = 10.0) -> bool:
    import urllib.request
    import urllib.error

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as response:
                if 200 <= int(response.status) < 500:
                    return True
        except (urllib.error.URLError, ValueError, TimeoutError):
            if _is_http_ready_by_powershell(url):
                return True
            time.sleep(0.2)
        except OSError:
            if _is_http_ready_by_powershell(url):
                return True
            time.sleep(0.2)
    return False


def _is_port_listening_by_netstat(port: int) -> bool:
    try:
        result = subprocess.run(
            ["cmd", "/c", "netstat -ano -p tcp"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            env=build_subprocess_env(),
            check=False,
        )
    except OSError:
        return False
    if result.returncode not in {0, 1}:
        return False
    expected_suffix = f":{int(port)}"
    for raw_line in (result.stdout or "").splitlines():
        part_list = raw_line.strip().split()
        if len(part_list) < 5:
            continue
        protocol, local_address, _, state, pid_text = part_list[:5]
        if protocol.upper() != "TCP" or state.upper() != "LISTENING":
            continue
        if not pid_text.isdigit():
            continue
        if local_address.endswith(expected_suffix):
            return True
    return False


def _is_http_ready_by_powershell(url: str) -> bool:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    "$ProgressPreference='SilentlyContinue'; "
                    f"$response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 -Uri '{url}' "
                    "-ErrorAction SilentlyContinue; "
                    "if ($response -and [int]$response.StatusCode -ge 200 -and [int]$response.StatusCode -lt 500) { exit 0 } "
                    "exit 1"
                ),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            env=build_subprocess_env(),
            check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def start_python_module(
    python_executable: str,
    module: str,
    cwd: Path,
    artifact_dir: Path,
    env: dict[str, str] | None = None,
    name: str | None = None,
) -> ManagedProcess:
    return _start_process(
        name=name or module,
        command=[python_executable, "-m", module],
        cwd=cwd,
        artifact_dir=artifact_dir,
        env=env,
    )


def terminate_tree(managed_process: ManagedProcess, timeout: float = 10.0) -> None:
    process = managed_process.process
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout)


def _start_process(
    name: str,
    command: list[str],
    cwd: Path,
    artifact_dir: Path,
    env: dict[str, str] | None = None,
) -> ManagedProcess:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = artifact_dir / f"{name}.stdout.log"
    stderr_path = artifact_dir / f"{name}.stderr.log"
    final_env = build_subprocess_env(env)
    stdout_file = stdout_path.open("w", encoding="utf-8")
    stderr_file = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=final_env,
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
    )
    return ManagedProcess(
        name=name,
        process=process,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        cwd=cwd,
        command=command,
        started_at=time.time(),
    )
