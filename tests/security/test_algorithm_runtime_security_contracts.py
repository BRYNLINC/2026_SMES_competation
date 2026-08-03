from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALGORITHM_MAIN_FILE = PROJECT_ROOT / "app" / "Algorithm" / "Algorithm" / "main.py"
BUSINESS_MANAGER_FILE = PROJECT_ROOT / "app" / "Algorithm" / "Algorithm" / "service" / "BusinessManager.py"


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("algorithm_runtime_security")]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("SEC-ALG-01")
@pytest.mark.priority("P1")
@pytest.mark.requirement("算法主入口默认只创建本地日志/stderr 文件，不应在启动阶段主动外联公网或写裁判机 results 目录")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/main.py",
    function="startup/__main__",
)
def test_algorithm_main_limits_startup_side_effects_to_local_logs_and_stderr() -> None:
    content = _read(ALGORITHM_MAIN_FILE)
    startup_body = content.split("async def startup", 1)[1].split("def ensure_logging_targets", 1)[0]
    ensure_logging_targets_body = content.split("def ensure_logging_targets", 1)[1].split("if __name__ == '__main__':", 1)[0]
    main_guard_body = content.split("if __name__ == '__main__':", 1)[1]

    assert "stderr_path = Path('stderr.txt')" in content
    assert "ensure_logging_targets(logging_config" in startup_body
    assert "base_dir=Path(os.getcwd())" in startup_body
    assert "log_file_path = base_dir / str(filename)" in ensure_logging_targets_body
    assert "log_file_path.parent.mkdir(parents=True, exist_ok=True)" in ensure_logging_targets_body
    assert "log_file_path.touch(exist_ok=True)" in ensure_logging_targets_body
    assert "sys.stderr = stderr_path.open('w', encoding='utf-8')" in main_guard_body
    assert "results/" not in content
    assert "http://" not in content
    assert "https://" not in content


@pytest.mark.test_id("SEC-ALG-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("算法业务层报告结果时只能通过 RPCController 回传 ProcessHub，不得直接写裁判机 results 文件")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/service/BusinessManager.py",
    function="report",
)
def test_business_manager_reports_via_rpc_without_direct_results_file_write() -> None:
    content = _read(BUSINESS_MANAGER_FILE)
    report_body = content.split("async def report", 1)[1].split("def get_algorithm_config", 1)[0]

    assert "await self.__rpc_controller.report(algorithm_report_message_model)" in report_body
    assert "results/" not in report_body
    assert ".write_text(" not in report_body
    assert ".unlink(" not in report_body


@pytest.mark.test_id("SEC-ALG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("算法运行时大小统计与结果日志只能访问受控目录，不得使用 taskkill、shutil.rmtree、任意网络请求等破坏性入口")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/main.py;app/Algorithm/Algorithm/service/BusinessManager.py",
    function="startup/__measure_platform_model_size_mb",
)
def test_algorithm_runtime_paths_avoid_destructive_or_network_side_effect_primitives() -> None:
    combined_content = _read(ALGORITHM_MAIN_FILE) + "\n" + _read(BUSINESS_MANAGER_FILE)

    forbidden_tokens = [
        "taskkill",
        "shutil.rmtree",
        "requests.get(",
        "requests.post(",
        "urllib.request.urlopen(",
        "socket.create_connection(",
        "os.system(",
    ]

    for token in forbidden_tokens:
        assert token not in combined_content
