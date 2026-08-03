from __future__ import annotations

import io
import logging
import struct
import sys
import time
from pathlib import Path

import pytest
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
for app_root in (
    PROJECT_ROOT / "app" / "ProcessHub",
    PROJECT_ROOT / "app" / "Algorithm",
    PROJECT_ROOT / "app" / "Collector",
):
    if str(app_root) not in sys.path:
        sys.path.insert(0, str(app_root))

from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel, AlgorithmReportMessageModel
from Common.model.CommonMessageModel import DataPackageModel, DevicePackageModel, ResultPackageModel
from ProcessHub.bci_competition.task.BCICompetitionTaskFinal import BCICompetitionTaskFinal
from ProcessHub.algorithm_connector.exception.ProcessHubAlgorithmConnectorException import (
    ProcessHubAlgorithmConnectorClosedException,
)
from ProcessHub.common.enum.ServiceStatusEnum import ServiceStatusEnum
from componentframework.common.enum.DataTypeEnum import DataTypeEnum


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("task_final_scoring")]


def test_final_score_includes_unstarted_configured_tasks_as_zero() -> None:
    score_context = {
        "team_id": "team_partial",
        "task_order": [
            "vme_left_vs_rest",
            "vme_right_vs_rest",
            "vmi_left_vs_rest",
            "vmi_right_vs_rest",
        ],
        "task_baseline_score_dict": {
            "vme_left_vs_rest": 0.0,
            "vme_right_vs_rest": 0.0,
            "vmi_left_vs_rest": 0.0,
            "vmi_right_vs_rest": 0.0,
        },
        "task_summary": {
            "vme_left_vs_rest": {
                "trial_count": 10,
                "task_score": 80.0,
                "baseline_score": 0.0,
                "accuracy_score": 78.0,
                "reaction_time_score": 2.0,
            },
            "vme_right_vs_rest": {
                "trial_count": 10,
                "task_score": 40.0,
                "baseline_score": 0.0,
                "accuracy_score": 39.0,
                "reaction_time_score": 1.0,
            },
        },
    }

    final_score_result = (
        BCICompetitionTaskFinal
        ._BCICompetitionTaskFinal__build_final_score_result(score_context)
    )

    assert final_score_result["task_count"] == 4
    assert final_score_result["started_task_count"] == 2
    assert final_score_result["total_score"] == pytest.approx(30.0)
    assert final_score_result["task_metric_list"][2]["adjusted_task_score"] == 0.0
    assert final_score_result["task_metric_list"][3]["adjusted_task_score"] == 0.0


async def test_task_boundary_forces_previous_pending_trials_to_timeout_before_reconnect() -> None:
    task = BCICompetitionTaskFinal()
    old_task_signature = ("S1", "vme", "left_vs_rest")
    captured_timeout_context_list: list[dict] = []
    captured_terminal_event_list: list[dict] = []

    class FakeChallenge:
        async def receive_timeout_trial(self, timeout_context: dict) -> None:
            captured_timeout_context_list.append(timeout_context)

    async def fake_emit_trial_terminal_event(**kwargs):
        captured_terminal_event_list.append(kwargs)
        return True

    task._BCICompetitionTaskFinal__logger = logging.getLogger("test_task_final_scoring")
    task._BCICompetitionTaskFinal__current_challenge = FakeChallenge()
    task._BCICompetitionTaskFinal__emit_trial_terminal_event = fake_emit_trial_terminal_event
    task._BCICompetitionTaskFinal__task_status = ServiceStatusEnum.STOPPING
    task._BCICompetitionTaskFinal__algorithm_disconnected_for_current_task = True
    task._BCICompetitionTaskFinal__disconnected_task_signature = old_task_signature
    task._BCICompetitionTaskFinal__timeout_limit_seconds = 1.0
    task._BCICompetitionTaskFinal__timeout_predict_label = "wrong"
    task._BCICompetitionTaskFinal__current_subject_id = "S1"
    task._BCICompetitionTaskFinal__current_exp_name = "vmi"
    task._BCICompetitionTaskFinal__current_exp_task = "right_vs_rest"
    task._BCICompetitionTaskFinal__current_session_id = "session_new"

    task._BCICompetitionTaskFinal__register_pending_trial_timing(
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session_old",
            "block_id": "block_old",
            "trial_id": "7",
            "trial_start_position": 960,
            "trial_end_position": 1000,
            "trial_ready_wallclock": time.time() - 0.2,
            "task_signature": old_task_signature,
        }
    )

    scored_count = await (
        task
        ._BCICompetitionTaskFinal__force_timeout_pending_trials_for_task_signature(
            old_task_signature,
            reason="task_boundary_cleanup_before_reconnect",
        )
    )

    assert scored_count == 1
    assert len(captured_timeout_context_list) == 1
    timeout_context = captured_timeout_context_list[0]
    assert timeout_context["platform_subject_id"] == "S1"
    assert timeout_context["platform_exp_name"] == "vme"
    assert timeout_context["platform_exp_task"] == "left_vs_rest"
    assert timeout_context["platform_session_id"] == "session_old"
    assert timeout_context["platform_trial_id"] == "7"
    assert timeout_context["platform_timeout"] is True
    assert timeout_context["platform_timeout_reason"] == "algorithm_disconnected_for_current_task"
    assert timeout_context["predict_time_ms"] == 0.0
    assert captured_terminal_event_list[0]["terminal_type"] == "timeout"
    assert len(task._BCICompetitionTaskFinal__pending_trial_timing_queue) == 0
    assert task._BCICompetitionTaskFinal__pending_trial_timing_by_end_position == {}


async def test_unmatched_result_package_is_not_scored_or_finalized() -> None:
    task = BCICompetitionTaskFinal()
    captured_report_list: list[AlgorithmReportMessageModel] = []
    captured_finalize_payload_list: list[dict] = []

    class FakeChallenge:
        async def receive_report(self, algorithm_report_message_model: AlgorithmReportMessageModel) -> None:
            captured_report_list.append(algorithm_report_message_model)

    async def fake_enqueue_report_finalization(payload: dict) -> None:
        captured_finalize_payload_list.append(payload)

    task._BCICompetitionTaskFinal__logger = logging.getLogger("test_task_final_scoring")
    task._BCICompetitionTaskFinal__task_status = ServiceStatusEnum.RUNNING
    task._BCICompetitionTaskFinal__current_challenge = FakeChallenge()
    task._BCICompetitionTaskFinal__enqueue_report_finalization = fake_enqueue_report_finalization

    await task.receive_report(
        AlgorithmReportMessageModel(
            timestamp_ms=int(time.time() * 1000),
            package=ResultPackageModel(result='{"predict_label": "1"}'),
        )
    )

    assert captured_report_list == []
    assert captured_finalize_payload_list == []


async def test_calibration_disconnect_emits_internal_forfeit_without_fake_ready() -> None:
    task = BCICompetitionTaskFinal()
    task._BCICompetitionTaskFinal__team_id = "team_1"
    task._BCICompetitionTaskFinal__group_id = "group_1"
    task._BCICompetitionTaskFinal__processor_component_id = "team_1.group_1"
    task._BCICompetitionTaskFinal__collector_component_id = "collector_group_1"
    task._BCICompetitionTaskFinal__current_subject_id = "S1"
    task._BCICompetitionTaskFinal__current_exp_name = "vme"
    task._BCICompetitionTaskFinal__current_exp_task = "left_vs_rest"
    task._BCICompetitionTaskFinal__current_session_id = "session1"
    task._BCICompetitionTaskFinal__current_stage_phase = "calibration"
    captured_live_status_list: list[dict] = []
    captured_runtime_event_list: list[dict] = []

    task._BCICompetitionTaskFinal__publish_team_live_status = (  # type: ignore[attr-defined]
        lambda **payload: captured_live_status_list.append(dict(payload))
    )

    async def fake_send_runtime_stage_event(payload: dict) -> bool:
        captured_runtime_event_list.append(dict(payload))
        return True

    task._BCICompetitionTaskFinal__send_runtime_stage_event = fake_send_runtime_stage_event  # type: ignore[attr-defined]

    await task._BCICompetitionTaskFinal__mark_algorithm_disconnected(  # type: ignore[attr-defined]
        disconnect_reason="algorithm_data_connection_closed_before_task_finished: report_stream_closed",
        algorithm_address="10.0.0.2:9981",
    )

    assert captured_live_status_list[-1]["connection_status"] == "disconnected"
    assert captured_live_status_list[-1]["forfeit_current_task"] is True
    assert [event["event_type"] for event in captured_runtime_event_list] == [
        "team_calibration_forfeited"
    ]
    assert captured_runtime_event_list[0]["stage_context"] == {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "left_vs_rest",
        "session_id": "session1",
    }
    assert "calibration_ready" not in captured_runtime_event_list[0]


async def test_calibration_device_metadata_populates_complete_forfeit_stage_context() -> None:
    task = BCICompetitionTaskFinal()
    device_message = AlgorithmDataMessageModel(
        source_label="eeg_1_calibration_private",
        timestamp_ms=1,
        package=DevicePackageModel(
            data_type=DataTypeEnum.EEG,
            channel_number=2,
            sample_rate=1000.0,
            channel_label=["C3", "C4"],
            other_information={
                "subject_id": "S9",
                "exp_name": "vmi",
                "exp_task": "right_vs_rest",
                "session_id": "session2",
                "stream_role": "calibration",
            },
        ),
    )

    await task._BCICompetitionTaskFinal__update_trial_timing_context(device_message)

    assert task._BCICompetitionTaskFinal__current_subject_id == "S9"
    assert task._BCICompetitionTaskFinal__current_exp_name == "vmi"
    assert task._BCICompetitionTaskFinal__current_exp_task == "right_vs_rest"
    assert task._BCICompetitionTaskFinal__current_session_id == "session2"
    assert task._BCICompetitionTaskFinal__current_stage_phase == "calibration"


async def test_initial_online_device_disconnect_releases_calibration_barrier() -> None:
    task = BCICompetitionTaskFinal()
    task._BCICompetitionTaskFinal__team_id = "team_14"
    task._BCICompetitionTaskFinal__group_id = "group_1"
    task._BCICompetitionTaskFinal__processor_component_id = "team_14.group_1"
    task._BCICompetitionTaskFinal__collector_component_id = "collector_group_1"
    captured_runtime_event_list: list[dict] = []

    async def fake_send_runtime_stage_event(payload: dict) -> bool:
        captured_runtime_event_list.append(dict(payload))
        return True

    task._BCICompetitionTaskFinal__send_runtime_stage_event = fake_send_runtime_stage_event  # type: ignore[attr-defined]
    await task._BCICompetitionTaskFinal__update_trial_timing_context(
        AlgorithmDataMessageModel(
            source_label="eeg_1_online_shared",
            timestamp_ms=1,
            package=DevicePackageModel(
                data_type=DataTypeEnum.EEG,
                channel_number=2,
                sample_rate=1000.0,
                channel_label=["FT8", "FC1"],
                other_information={
                    "subject_id": "sub_15",
                    "exp_name": "vme",
                    "exp_task": "left_vs_rest",
                    "session_id": "session1",
                    "stream_role": "online",
                },
            ),
        )
    )

    await task._BCICompetitionTaskFinal__mark_algorithm_disconnected(
        disconnect_reason="grpc_unavailable: Stream removed (Connection reset -- 10054)",
        algorithm_address="10.11.11.119:9981",
    )

    assert task._BCICompetitionTaskFinal__current_stage_phase == "online"
    assert [event["event_type"] for event in captured_runtime_event_list] == [
        "team_calibration_forfeited"
    ]
    assert captured_runtime_event_list[0]["stage_context"] == {
        "subject_id": "sub_15",
        "exp_name": "vme",
        "exp_task": "left_vs_rest",
        "session_id": "session1",
    }


async def test_online_disconnect_after_calibration_ready_does_not_reopen_calibration_terminal() -> None:
    task = BCICompetitionTaskFinal()
    stage_signature = ("sub_15", "vme", "left_vs_rest", "session1")
    task._BCICompetitionTaskFinal__current_subject_id = stage_signature[0]
    task._BCICompetitionTaskFinal__current_exp_name = stage_signature[1]
    task._BCICompetitionTaskFinal__current_exp_task = stage_signature[2]
    task._BCICompetitionTaskFinal__current_session_id = stage_signature[3]
    task._BCICompetitionTaskFinal__current_stage_phase = "online"
    task._BCICompetitionTaskFinal__calibration_ready_stage_signature_set.add(stage_signature)
    captured_runtime_event_list: list[dict] = []

    async def fake_send_runtime_stage_event(payload: dict) -> bool:
        captured_runtime_event_list.append(dict(payload))
        return True

    task._BCICompetitionTaskFinal__send_runtime_stage_event = fake_send_runtime_stage_event  # type: ignore[attr-defined]

    await task._BCICompetitionTaskFinal__mark_algorithm_disconnected(
        disconnect_reason="grpc_unavailable: Stream removed (Connection reset -- 10054)",
        algorithm_address="10.11.11.119:9981",
    )

    assert captured_runtime_event_list == []


async def test_runtime_session_handshake_rejects_closed_data_stream() -> None:
    task = BCICompetitionTaskFinal()

    class FakeChallenge:
        async def receive_algorithm_config(self, config_dict: dict) -> None:
            return None

        async def get_to_algorithm_config(self) -> dict:
            return {}

    class FakeConnector:
        async def startup(self) -> None:
            return None

        async def data_connect(self) -> None:
            return None

        async def pull_algorithm_config(self) -> dict:
            return {}

        async def push_algorithm_config(self, config_dict: dict) -> None:
            return None

        def is_transport_active(self) -> bool:
            return False

        def get_algorithm_address(self) -> str:
            return "10.0.0.9:9981"

    task._BCICompetitionTaskFinal__current_challenge = FakeChallenge()
    task._algorithm_connector = FakeConnector()

    with pytest.raises(ProcessHubAlgorithmConnectorClosedException):
        await task._BCICompetitionTaskFinal__establish_algorithm_runtime_session(
            allow_current_task_join=False
        )


async def test_calibration_delivery_is_buffered_and_replay_is_hidden_from_preliminary_pyd() -> None:
    task = BCICompetitionTaskFinal()
    task._BCICompetitionTaskFinal__team_id = "team_0"
    task._BCICompetitionTaskFinal__current_subject_id = "S1"
    task._BCICompetitionTaskFinal__current_exp_name = "vme"
    task._BCICompetitionTaskFinal__current_exp_task = "left_vs_rest"
    task._BCICompetitionTaskFinal__current_session_id = "session1"
    delivery_id = "delivery-compatible-with-preliminary-pyd"

    device_message = AlgorithmDataMessageModel(
        source_label="eeg_1_calibration_private",
        timestamp_ms=1,
        package=DevicePackageModel(
            data_type=DataTypeEnum.EEG,
            channel_number=2,
            sample_rate=1000.0,
            channel_label=["C3", "C4"],
            other_information={
                "subject_id": "S1",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": "session1",
                "stream_role": "calibration",
                "calibration_delivery_id": delivery_id,
            },
        ),
    )
    assert task._BCICompetitionTaskFinal__should_drop_completed_calibration_delivery_message(device_message) is False
    normalized_device_message = task._BCICompetitionTaskFinal__normalize_algorithm_source_label(
        device_message
    )
    assert await task._BCICompetitionTaskFinal__prepare_messages_for_algorithm_forward(
        device_message,
        normalized_device_message,
    ) == []

    calibration_buffer = io.BytesIO()
    np.savez_compressed(
        calibration_buffer,
        subject_id=np.asarray("S1"),
        exp_name=np.asarray("vme"),
        exp_task=np.asarray("left_vs_rest"),
        session_id=np.asarray("session1"),
        data=np.zeros((1, 2, 4), dtype=np.float32),
        label=np.asarray([1], dtype=np.int64),
    )
    calibration_payload = calibration_buffer.getvalue()
    chunk_header = struct.pack(
        ">4sIII",
        b"CAL1",
        1,
        0,
        len(calibration_payload),
    )
    chunk_message = AlgorithmDataMessageModel(
        source_label="eeg_1_calibration_private",
        timestamp_ms=2,
        package=DataPackageModel(
            data_position=0,
            data=chunk_header + calibration_payload,
        ),
    )
    normalized_chunk_message = task._BCICompetitionTaskFinal__normalize_algorithm_source_label(
        chunk_message
    )
    forwarded_message_list = await task._BCICompetitionTaskFinal__prepare_messages_for_algorithm_forward(
        chunk_message,
        normalized_chunk_message,
    )

    assert len(forwarded_message_list) == 2
    assert isinstance(forwarded_message_list[0].package, DevicePackageModel)
    assert isinstance(forwarded_message_list[1].package, DataPackageModel)
    assert all(message.source_label == "eeg_1" for message in forwarded_message_list)
    assert bytes(forwarded_message_list[1].package.data).startswith(b"CAL1")
    assert getattr(
        forwarded_message_list[-1],
        "_calibration_delivery_id_to_complete",
    ) == delivery_id

    task._BCICompetitionTaskFinal__mark_calibration_delivery_forwarded(delivery_id)
    assert task._BCICompetitionTaskFinal__should_drop_completed_calibration_delivery_message(device_message) is True
    assert task._BCICompetitionTaskFinal__should_drop_completed_calibration_delivery_message(chunk_message) is True
