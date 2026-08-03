from __future__ import annotations

import ast
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALGORITHM_MAIN_FILE = PROJECT_ROOT / "app" / "Algorithm" / "Algorithm" / "main.py"
ALGORITHM_METHOD_FILE = (
    PROJECT_ROOT / "app" / "Algorithm" / "Algorithm" / "method" / "model_artifacts" / "baseline_example" / "AlgorithmImplement.py"
)
METHOD_MANAGER_FILE = PROJECT_ROOT / "app" / "Algorithm" / "Algorithm" / "service" / "MethodManager.py"
PREDICT_WORKER_FILE = PROJECT_ROOT / "app" / "Algorithm" / "Algorithm" / "method" / "worker" / "PredictWorkerProcess.py"


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("algorithm_code_injection")]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _parse(path: Path) -> ast.Module:
    return ast.parse(_read(path), filename=str(path))


@pytest.mark.test_id("SEC-INJECT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("选手算法主入口、baseline 算法与方法管理器不得暴露 eval/exec/os.system/pickle.loads/marshal.loads 等代码注入入口")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/main.py;app/Algorithm/Algorithm/method/model_artifacts/baseline_example/AlgorithmImplement.py;app/Algorithm/Algorithm/service/MethodManager.py",
    function="static_contract",
)
def test_algorithm_runtime_files_avoid_high_risk_code_injection_primitives() -> None:
    dangerous_call_list: list[str] = []
    for path in (ALGORITHM_MAIN_FILE, ALGORITHM_METHOD_FILE, METHOD_MANAGER_FILE):
        tree = _parse(path)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            call_name = _resolve_call_name(node.func)
            if call_name in {
                "eval",
                "exec",
                "pickle.loads",
                "marshal.loads",
                "os.system",
                "subprocess.Popen",
                "subprocess.run",
                "requests.get",
                "requests.post",
                "urllib.request.urlopen",
            }:
                dangerous_call_list.append(f"{path.name}:{call_name}")

    assert dangerous_call_list == []


def _resolve_call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent_name = _resolve_call_name(node.value)
        if parent_name:
            return f"{parent_name}.{node.attr}"
        return node.attr
    return ""


@pytest.mark.test_id("SEC-INJECT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("算法异常上报必须截断 exception_message，避免恶意代码通过异常通道回传超长 payload 或命令串")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/service/MethodManager.py",
    function="__run_algorithm_method",
)
def test_method_manager_truncates_exception_message_for_malicious_exception_payloads() -> None:
    content = _read(METHOD_MANAGER_FILE)

    assert "exception_message=str(exc_value) if len(str(exc_value)) <= 20 else str(exc_value)[:20]" in content
    assert "exception_stack_trace=traceback.format_tb(exception_traceback)" in content


@pytest.mark.test_id("SEC-INJECT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("predict worker 出错时只能把 traceback 作为普通字符串放回队列，不能执行任意恢复脚本或命令")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/method/worker/PredictWorkerProcess.py",
    function="predict_worker_main",
)
def test_predict_worker_failure_channel_is_data_only_without_command_execution() -> None:
    content = _read(PREDICT_WORKER_FILE)

    assert "result_queue.put(" in content
    assert "'type': 'worker_error'" in content
    assert "'traceback': traceback.format_exc()" in content
    assert "subprocess" not in content
    assert "os.system(" not in content
