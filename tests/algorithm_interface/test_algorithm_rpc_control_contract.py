from __future__ import annotations

import sys
from pathlib import Path

import pytest

try:
    import asyncio
    import yaml

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    ALGORITHM_APP_ROOT = PROJECT_ROOT / "app" / "Algorithm"
    PROCESS_HUB_APP_ROOT = PROJECT_ROOT / "app" / "ProcessHub"
    for path in (ALGORITHM_APP_ROOT, PROCESS_HUB_APP_ROOT):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmStatusEnum
    from Algorithm.control.AlgorithmRPCServiceControlServer import AlgorithmRPCServiceControlServer
    from Algorithm.common.enum.ServiceStatusEnum import ServiceStatusEnum as AlgorithmServiceStatusEnum
    from Common.protobuf.BaseDataClassMessage_pb2 import EmptyMessage, StringMessage
except Exception as exc:  # pragma: no cover - environment-dependent import gate
    asyncio = None
    yaml = None
    AlgorithmStatusEnum = None
    AlgorithmRPCServiceControlServer = None
    AlgorithmServiceStatusEnum = None
    EmptyMessage = None
    StringMessage = None
    _ALGORITHM_RPC_CONTROL_IMPORT_ERROR = exc
else:
    _ALGORITHM_RPC_CONTROL_IMPORT_ERROR = None


pytestmark = [pytest.mark.algorithm_interface, pytest.mark.layer("algorithm_interface"), pytest.mark.category("algorithm_rpc_control")]


if AlgorithmRPCServiceControlServer is None or asyncio is None:
    pytestmark.append(
        pytest.mark.skip(
            reason=f"Algorithm RPC control import unavailable in current environment: {_ALGORITHM_RPC_CONTROL_IMPORT_ERROR!r}"
        )
    )


STATUS_CASES = (
    [
        (AlgorithmServiceStatusEnum.INITIALIZING, AlgorithmStatusEnum.INITIALIZING),
        (AlgorithmServiceStatusEnum.READY, AlgorithmStatusEnum.READY),
        (AlgorithmServiceStatusEnum.RUNNING, AlgorithmStatusEnum.RUNNING),
        (AlgorithmServiceStatusEnum.ERROR, AlgorithmStatusEnum.ERROR),
    ]
    if AlgorithmServiceStatusEnum is not None and AlgorithmStatusEnum is not None
    else []
)


class _FakeBusinessManager:
    def __init__(self) -> None:
        self.received_config: dict | None = None
        self.return_config: dict = {
            "sources": {
                "eeg_1": None,
                "eeg_2": None,
            },
            "challenge_to_algorithm_config": {
                "predict_timeout_seconds": 1.0,
                "task_id": "mi_left_vs_rest",
            },
        }

    async def receive_config(self, config_dict: dict) -> None:
        self.received_config = config_dict

    async def get_config(self) -> dict:
        return self.return_config


class _FakeCoreController:
    def __init__(self, status=None) -> None:
        self._status = status if status is not None else AlgorithmServiceStatusEnum.READY
        self.exit_called = False
        self.exit_call_count = 0

    def get_service_status(self) -> AlgorithmServiceStatusEnum:
        return self._status

    async def exit(self) -> None:
        self.exit_called = True
        self.exit_call_count += 1


@pytest.mark.test_id("ALG-CTRL-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("getStatus 必须把算法内部服务状态准确映射到 RPC 协议枚举")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py",
    function="getStatus",
)
@pytest.mark.parametrize(("service_status", "expected_status"), STATUS_CASES)
@pytest.mark.asyncio
async def test_get_status_maps_core_service_status_to_rpc_enum(
    service_status: AlgorithmServiceStatusEnum,
    expected_status: AlgorithmStatusEnum,
) -> None:
    server = AlgorithmRPCServiceControlServer(
        business_manager=_FakeBusinessManager(),
        core_controller=_FakeCoreController(status=service_status),
    )

    response = await server.getStatus(EmptyMessage(), context=None)

    assert response.status == expected_status.value


@pytest.mark.test_id("ALG-CTRL-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("sendConfig 必须接收 YAML 配置并传递给业务层，保持 challenge_to_algorithm_config 内容不丢失")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py",
    function="sendConfig",
)
@pytest.mark.asyncio
async def test_send_config_passes_yaml_payload_to_business_manager() -> None:
    business_manager = _FakeBusinessManager()
    server = AlgorithmRPCServiceControlServer(
        business_manager=business_manager,
        core_controller=_FakeCoreController(),
    )
    config_dict = {
        "sources": {"eeg_1": None},
        "challenge_to_algorithm_config": {
            "predict_timeout_seconds": 1.0,
            "calibration_trials_per_class_requested": 0,
        },
    }

    response = await server.sendConfig(StringMessage(data=yaml.safe_dump(config_dict)), context=None)

    assert isinstance(response, EmptyMessage)
    assert business_manager.received_config == config_dict


@pytest.mark.test_id("ALG-CTRL-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("getConfig 必须返回 YAML，且包含 sources 与 challenge_to_algorithm_config 两类主键")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py",
    function="getConfig",
)
@pytest.mark.asyncio
async def test_get_config_returns_yaml_with_sources_and_challenge_config() -> None:
    business_manager = _FakeBusinessManager()
    server = AlgorithmRPCServiceControlServer(
        business_manager=business_manager,
        core_controller=_FakeCoreController(),
    )

    response = await server.getConfig(EmptyMessage(), context=None)
    payload = yaml.safe_load(response.data)

    assert payload == business_manager.return_config
    assert sorted(payload.keys()) == ["challenge_to_algorithm_config", "sources"]
    assert payload["challenge_to_algorithm_config"]["predict_timeout_seconds"] == 1.0


@pytest.mark.test_id("ALG-CTRL-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("shutdown 必须立即返回成功，并异步触发 core_controller.exit，避免阻塞裁判侧关闭流程")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py",
    function="shutdown",
)
@pytest.mark.asyncio
async def test_shutdown_schedules_core_controller_exit_and_returns_success() -> None:
    core_controller = _FakeCoreController()
    server = AlgorithmRPCServiceControlServer(
        business_manager=_FakeBusinessManager(),
        core_controller=core_controller,
    )

    response = await server.shutdown(EmptyMessage(), context=None)
    await asyncio.sleep(0)

    assert response.data is True
    assert core_controller.exit_called is True
    assert core_controller.exit_call_count == 1
