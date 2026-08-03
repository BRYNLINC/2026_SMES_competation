from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from queue import Empty

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALGORITHM_APP_ROOT = PROJECT_ROOT / "app" / "Algorithm"
if str(ALGORITHM_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(ALGORITHM_APP_ROOT))

from Algorithm.method.worker.PredictWorkerManager import PredictWorkerManager
from Algorithm.method.worker.PredictWorkerProcess import _load_algorithm_instance


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("algorithm_predict_worker")]


class _DeadProcess:
    pid = 12345
    exitcode = 3

    def is_alive(self) -> bool:
        return False


class _BlockingResultQueue:
    def get(self, block: bool, timeout: float):
        raise Empty()


@pytest.mark.test_id("PWM-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("predict worker 应接收当前算法入口配置，避免子进程硬编码加载 baseline")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/method/worker/PredictWorkerManager.py",
    function="__start_worker_if_needed",
)
def test_worker_manager_keeps_method_config_for_spawned_worker() -> None:
    method_config = {
        "workspace_path": "D:/workspace",
        "method_class_file": "custom/AlgorithmImplement.py",
        "method_class_name": "AlgorithmImplement",
    }

    manager = PredictWorkerManager(method_config=method_config)

    assert manager._PredictWorkerManager__method_config == method_config  # type: ignore[attr-defined]
    method_config["method_class_file"] = "mutated.py"
    assert manager._PredictWorkerManager__method_config["method_class_file"] == "custom/AlgorithmImplement.py"  # type: ignore[attr-defined]


@pytest.mark.test_id("PWM-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("predict worker 已退出时等待响应应立即暴露退出码，而不是等到 timeout 后触发本地断线")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/method/worker/PredictWorkerManager.py",
    function="__wait_for_response",
)
def test_wait_for_response_reports_dead_worker_without_waiting_for_timeout() -> None:
    async def _run() -> None:
        manager = PredictWorkerManager(predict_timeout_seconds=1.0)
        manager._PredictWorkerManager__process = _DeadProcess()  # type: ignore[attr-defined]
        manager._PredictWorkerManager__result_queue = _BlockingResultQueue()  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="predict worker process exited unexpectedly: exitcode=3"):
            await manager._PredictWorkerManager__wait_for_response(  # type: ignore[attr-defined]
                timeout_seconds=30.0,
                matcher=lambda payload: False,
            )

    asyncio.run(_run())


@pytest.mark.test_id("PWM-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("predict worker 应按 method_config 动态加载指定算法文件")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/method/worker/PredictWorkerProcess.py",
    function="_load_algorithm_instance",
)
def test_predict_worker_process_loads_algorithm_from_method_config(tmp_path: Path) -> None:
    algorithm_file = tmp_path / "CustomAlgorithm.py"
    algorithm_file.write_text(
        "\n".join(
            [
                "class CustomAlgorithm:",
                "    def __init__(self):",
                "        self.loaded_from_custom_file = True",
            ]
        ),
        encoding="utf-8",
    )

    algorithm_instance = _load_algorithm_instance(
        {
            "workspace_path": str(tmp_path),
            "method_class_file": "CustomAlgorithm.py",
            "method_class_name": "CustomAlgorithm",
        }
    )

    assert algorithm_instance.loaded_from_custom_file is True
