from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_APP_ROOT = PROJECT_ROOT / "app" / "Collector"
if str(COLLECTOR_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(COLLECTOR_APP_ROOT))

try:
    from Collector.receiver.virtual_receiver.VirtualReceiverImplement import VirtualReceiverImplement
except Exception as exc:  # pragma: no cover - environment-dependent import gate
    VirtualReceiverImplement = None
    _VIRTUAL_RECEIVER_IMPORT_ERROR = exc
else:
    _VIRTUAL_RECEIVER_IMPORT_ERROR = None


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("virtual_receiver")]
if VirtualReceiverImplement is None:
    pytestmark.append(
        pytest.mark.skip(reason=f"VirtualReceiver import unavailable: {_VIRTUAL_RECEIVER_IMPORT_ERROR!r}")
    )


@pytest.mark.test_id("VR-STAGE-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("单队私有校准包瞬时发送失败时必须重发完整 DevicePackage+chunks，不能直接跳过该队")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverImplement.py",
    function="__send_calibration_payload_to_team_with_retry",
)
def test_calibration_payload_distribution_retries_the_same_team_before_advancing() -> None:
    async def _run() -> None:
        receiver = VirtualReceiverImplement()
        attempt_list: list[tuple[str, str]] = []

        async def flaky_send(
            team_id: str,
            runtime_data_file_model,
            calibration_chunk_list: list[bytes],
            calibration_delivery_id: str,
        ) -> None:
            attempt_list.append((team_id, calibration_delivery_id))
            if len(attempt_list) < 3:
                raise asyncio.TimeoutError("shared stream write timeout")

        setattr(receiver, "_VirtualReceiverImplement__send_calibration_payload_to_team", flaky_send)
        setattr(receiver, "_VirtualReceiverImplement__CALIBRATION_PAYLOAD_SEND_RETRY_LIMIT", 3)
        setattr(receiver, "_VirtualReceiverImplement__CALIBRATION_PAYLOAD_SEND_RETRY_DELAY_SECONDS", 0.0)

        retry_sender = getattr(
            receiver,
            "_VirtualReceiverImplement__send_calibration_payload_to_team_with_retry",
            None,
        )
        assert retry_sender is not None, "校准包分发缺少队级完整重试入口"

        await retry_sender(
            team_id="team_4",
            runtime_data_file_model=SimpleNamespace(
                subject_id="qyq",
                exp_name="vme",
                exp_task="right_vs_rest",
                session_id="session2",
            ),
            calibration_chunk_list=[b"chunk"],
            calibration_delivery_id="delivery-fixed-for-retry",
        )

        assert attempt_list == [
            ("team_4", "delivery-fixed-for-retry"),
            ("team_4", "delivery-fixed-for-retry"),
            ("team_4", "delivery-fixed-for-retry"),
        ]

    asyncio.run(_run())


@pytest.mark.test_id("VR-STAGE-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("校准阶段分发最终失败时必须写 error 状态并停止数据循环，不能继续下一阶段或发送正常结束标志")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverImplement.py",
    function="__read_data/__publish_current_trial_error_state",
)
def test_read_data_stops_at_failed_stage_and_publishes_error_state() -> None:
    async def _run() -> None:
        receiver = VirtualReceiverImplement()
        stage_list = [
            SimpleNamespace(subject_id="qyq", exp_name="vme", exp_task="right_vs_rest", session_id="session2"),
            SimpleNamespace(subject_id="qyq", exp_name="vmi", exp_task="left_vs_rest", session_id="session1"),
        ]
        attempted_stage_list: list[str] = []
        error_payload_list: list[tuple[object, BaseException]] = []
        transponder_payload_list: list[object] = []

        async def no_wait() -> None:
            return None

        async def load_team_data(**kwargs):
            return {"team_0": [object()]}

        async def load_online_data(**kwargs):
            return [], [object()]

        async def fail_stage(*, runtime_task_group_model, **kwargs) -> None:
            attempted_stage_list.append(runtime_task_group_model.session_id)
            raise asyncio.TimeoutError("calibration_private_team_4")

        def publish_error(runtime_task_group_model, error: BaseException) -> None:
            error_payload_list.append((runtime_task_group_model, error))

        class FakeTransponder:
            async def send_data(self, payload) -> None:
                transponder_payload_list.append(payload)

        receiver._VirtualReceiverImplement__send_flag_event.set()  # type: ignore[attr-defined]
        setattr(receiver, "_VirtualReceiverImplement__wait_until_calibration_trial_count_ready", no_wait)
        setattr(receiver, "_VirtualReceiverImplement__get_runtime_task_group_model_list", lambda: stage_list)
        setattr(receiver, "_VirtualReceiverImplement__load_team_file_task_data_model_list_dict", load_team_data)
        setattr(receiver, "_VirtualReceiverImplement__load_file_task_data_model_list", load_online_data)
        setattr(receiver, "_VirtualReceiverImplement__send_runtime_task_group", fail_stage)
        setattr(receiver, "_VirtualReceiverImplement__publish_current_trial_error_state", publish_error)
        receiver._receiver_transponder = FakeTransponder()  # type: ignore[attr-defined]

        await receiver._VirtualReceiverImplement__read_data()  # type: ignore[attr-defined]

        assert attempted_stage_list == ["session2"]
        assert len(error_payload_list) == 1
        assert error_payload_list[0][0] is stage_list[0]
        assert isinstance(error_payload_list[0][1], asyncio.TimeoutError)
        assert transponder_payload_list == []

    asyncio.run(_run())
