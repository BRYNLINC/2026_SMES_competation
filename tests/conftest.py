from __future__ import annotations

import inspect
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.helpers.process_runner import build_subprocess_env
from tests.helpers.project_paths import project_root
from tests.helpers.report_writer import CsvReportPlugin


PROJECT_ROOT = project_root()
ALGORITHM_APP_ROOT = PROJECT_ROOT / "app" / "Algorithm"


def _python_runtime_self_check(candidate: str) -> bool:
    candidate_text = str(candidate or "").strip()
    if candidate_text == "":
        return False
    candidate_path = Path(candidate_text)
    if not candidate_path.exists():
        return False
    final_env = build_subprocess_env()
    try:
        result = subprocess.run(
            [
                candidate_text,
                "-c",
                (
                    f"import sys; "
                    f"sys.path.insert(0, r'{str(ALGORITHM_APP_ROOT)}'); "
                    "import pytest, asyncio, socket, yaml, grpc, numpy, injector; "
                    "from Algorithm.api.proto import AlgorithmRPCService_pb2_grpc; "
                    "sock = socket.socket(); sock.close()"
                ),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            env=final_env,
            check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def pytest_configure(config: pytest.Config) -> None:
    os.environ.setdefault("PYTHONHASHSEED", "0")
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    plugin = CsvReportPlugin(PROJECT_ROOT)
    config._bci_csv_report_plugin = plugin  # type: ignore[attr-defined]
    config.pluginmanager.register(plugin, "bci_csv_report_plugin")


def pytest_unconfigure(config: pytest.Config) -> None:
    plugin = getattr(config, "_bci_csv_report_plugin", None)
    if plugin is not None:
        config.pluginmanager.unregister(plugin)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    basetemp = session.config.option.basetemp
    if not basetemp:
        return
    basetemp_path = Path(str(basetemp))
    if basetemp_path.exists():
        shutil.rmtree(basetemp_path, ignore_errors=True)


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    test_function = pyfuncitem.obj
    if not inspect.iscoroutinefunction(test_function):
        return None
    try:
        import asyncio
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"asyncio import unavailable in current environment: {exc!r}")
    fixture_arg_dict = {
        arg_name: pyfuncitem.funcargs[arg_name]
        for arg_name in pyfuncitem._fixtureinfo.argnames  # type: ignore[attr-defined]
    }
    asyncio.run(test_function(**fixture_arg_dict))
    return True


@pytest.fixture(scope="session")
def project_root_path() -> Path:
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def python_executable() -> str:
    candidate_list = [
        os.environ.get("BCI_PYTHON_EXE"),
        r"D:\anaconda3\envs\BCI_competation_2026\python.exe",
        r"D:\anaconda3\envs\BCI_competition_2026\python.exe",
        r"D:\anaconda3\python.exe",
        sys.executable,
    ]
    for candidate in candidate_list:
        if _python_runtime_self_check(str(candidate or "")):
            return str(Path(str(candidate)).resolve())
    for path_dir in os.environ.get("PATH", "").split(os.pathsep):
        candidate_path = Path(path_dir) / "python.exe"
        if _python_runtime_self_check(str(candidate_path)):
            return str(candidate_path.resolve())
    return sys.executable


@pytest.fixture()
def run_python(python_executable: str):
    def _run(*args: str, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        final_env = build_subprocess_env()
        if env:
            final_env.update(env)
        return subprocess.run(
            [python_executable, *args],
            cwd=str(cwd or PROJECT_ROOT),
            env=final_env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            check=False,
        )

    return _run


@pytest.fixture(autouse=True)
def cleanup_per_test_temp_roots(request: pytest.FixtureRequest):
    tmp_root_list: list[Path] = []
    if "tmp_path" in request.fixturenames:
        try:
            tmp_root_list.append(request.getfixturevalue("tmp_path"))
        except Exception:
            # Windows 上前序测试若仍占用 basetemp 子目录，tmp_path 初始化可能在清理旧目录时失败。
            # 这里不把该基础设施问题升级为当前用例自身失败，交由 pytest 原始错误处理。
            pass
    if "tmpdir" in request.fixturenames:
        try:
            tmp_root_list.append(Path(str(request.getfixturevalue("tmpdir"))))
        except Exception:
            pass

    yield

    for tmp_root in tmp_root_list:
        if not tmp_root.exists():
            continue
        for child_path in list(tmp_root.iterdir()):
            if child_path.is_dir():
                shutil.rmtree(child_path, ignore_errors=True)
            else:
                try:
                    child_path.unlink(missing_ok=True)
                except PermissionError:
                    pass
