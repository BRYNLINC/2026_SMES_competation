from __future__ import annotations

import asyncio
import importlib
import random
import sys
from pathlib import Path

import numpy as np
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALGORITHM_APP_ROOT = PROJECT_ROOT / "app" / "Algorithm"
if str(ALGORITHM_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(ALGORITHM_APP_ROOT))

from Algorithm.common.utils.seed import (  # noqa: E402
    DEFAULT_GLOBAL_SEED,
    build_stage_seed,
    seed_everything,
    seed_everything_for_stage,
)
from Algorithm.method.model.AlgorithmObject import AlgorithmCalibrationObject  # noqa: E402
from Algorithm.service.SourceReceiver.ContinuousDataSourceReceiver import (  # noqa: E402
    ContinuousDataSourceReceiver,
)

receiver_module = importlib.import_module(
    "Algorithm.service.SourceReceiver.ContinuousDataSourceReceiver"
)


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("reproducibility")]


def _draw_random_state() -> tuple[float, float]:
    return random.random(), float(np.random.random())


@pytest.mark.test_id("SEED-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("同一 stage 的随机状态必须与此前已经运行过多少 stage 无关")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/common/utils/seed.py",
    function="build_stage_seed/seed_everything_for_stage",
)
def test_stage_seed_is_stable_and_independent_of_prior_rng_consumption() -> None:
    stage = ("sub_15", "vme", "right_vs_rest", "session1")
    first_seed = build_stage_seed(*stage)

    seed_everything(DEFAULT_GLOBAL_SEED)
    for _ in range(37):
        _draw_random_state()
    seed_everything_for_stage(*stage)
    first_draw = _draw_random_state()

    seed_everything(DEFAULT_GLOBAL_SEED)
    for _ in range(3):
        _draw_random_state()
    seed_everything_for_stage(*stage)
    second_draw = _draw_random_state()

    assert first_seed == build_stage_seed(*stage)
    assert first_seed != build_stage_seed("sub_16", *stage[1:])
    assert first_draw == second_draw


@pytest.mark.test_id("SEED-01B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("单机 PyTorch 校准结果必须与恢复前随机数消耗量无关")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/common/utils/seed.py",
    function="seed_everything_for_stage",
)
def test_stage_seed_makes_single_machine_torch_training_repeatable() -> None:
    torch = pytest.importorskip("torch")
    stage = ("sub_15", "vme", "right_vs_rest", "session1")
    train_x = torch.tensor([[0.1, 0.2], [0.9, 0.8]], dtype=torch.float32)
    train_y = torch.tensor([0, 1], dtype=torch.long)

    def train_once(prior_draw_count: int):
        seed_everything(DEFAULT_GLOBAL_SEED)
        torch.rand(prior_draw_count)
        seed_everything_for_stage(*stage)
        model = torch.nn.Linear(2, 2)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
        for _ in range(5):
            optimizer.zero_grad()
            loss = torch.nn.functional.cross_entropy(model(train_x), train_y)
            loss.backward()
            optimizer.step()
        return tuple(parameter.detach().clone() for parameter in model.parameters())

    first_parameters = train_once(3)
    second_parameters = train_once(97)

    for first_parameter, second_parameter in zip(first_parameters, second_parameters):
        assert torch.equal(first_parameter, second_parameter)


@pytest.mark.test_id("SEED-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("校准对象交给选手算法前必须按 stage 重置随机状态")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/service/SourceReceiver/ContinuousDataSourceReceiver.py",
    function="get_calibration",
)
def test_receiver_resets_stage_seed_before_returning_calibration(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_stage: list[tuple[str, str, str, str]] = []

    def fake_seed_for_stage(subject_id, exp_name, exp_task, session_id, **kwargs):
        captured_stage.append((subject_id, exp_name, exp_task, session_id))
        return 123

    monkeypatch.setattr(receiver_module, "seed_everything_for_stage", fake_seed_for_stage)

    async def _run() -> AlgorithmCalibrationObject:
        receiver = ContinuousDataSourceReceiver()
        calibration_object = AlgorithmCalibrationObject(
            subject_id="S1",
            exp_name="vmi",
            exp_task="left_vs_rest",
            session_id="session2",
            finish_flag=False,
        )
        receiver._ContinuousDataSourceReceiver__calibration_queue.put_nowait(calibration_object)  # type: ignore[attr-defined]
        return await receiver.get_calibration()

    result = asyncio.run(_run())

    assert result.subject_id == "S1"
    assert result.stage_seed == 123
    assert captured_stage == [("S1", "vmi", "left_vs_rest", "session2")]


@pytest.mark.test_id("SEED-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("预测 worker 加载 stage 模型时必须使用同一 stage seed")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/method/worker/PredictWorkerProcess.py",
    function="predict_worker_main",
)
def test_predict_worker_seeds_each_loaded_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    from Algorithm.method.worker import PredictWorkerProcess as worker_module

    seed_calls: list[tuple] = []

    monkeypatch.setattr(worker_module, "seed_everything", lambda seed: seed_calls.append(("global", seed)))
    monkeypatch.setattr(
        worker_module,
        "seed_everything_for_stage",
        lambda *stage: seed_calls.append(("stage", *stage)) or 456,
    )

    class FakeAlgorithm:
        def load_predict_session(self, **kwargs):
            return "cpu"

    monkeypatch.setattr(worker_module, "_load_algorithm_instance", lambda config: FakeAlgorithm())

    class CommandQueue:
        def __init__(self):
            self.commands = [
                {
                    "command": "load_session",
                    "session_token": "token",
                    "stage_signature": ["S1", "vme", "right_vs_rest", "session1"],
                },
                {"command": "shutdown"},
            ]

        def get(self):
            return self.commands.pop(0)

    class ResultQueue:
        def __init__(self):
            self.payloads = []

        def put(self, payload):
            self.payloads.append(payload)

    result_queue = ResultQueue()
    worker_module.predict_worker_main(CommandQueue(), result_queue, {})

    assert seed_calls == [
        ("global", DEFAULT_GLOBAL_SEED),
        ("stage", "S1", "vme", "right_vs_rest", "session1"),
    ]
    assert result_queue.payloads[0]["stage_seed"] == 456
