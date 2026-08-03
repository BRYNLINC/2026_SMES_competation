from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import numpy as np
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALGORITHM_APP_ROOT = PROJECT_ROOT / "app" / "Algorithm"
if str(ALGORITHM_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(ALGORITHM_APP_ROOT))

from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel
from Algorithm.service.SourceReceiver.ContinuousDataSourceReceiver import ContinuousDataSourceReceiver
from Common.model.CommonMessageModel import DataPackageModel, DevicePackageModel
from componentframework.common.enum.DataTypeEnum import DataTypeEnum


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("algorithm_source_receiver")]


@pytest.mark.test_id("SRC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("算法已进入 online 消费阶段时，stream_role 滞留 calibration 不能丢弃 online 数据")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/service/SourceReceiver/ContinuousDataSourceReceiver.py",
    function="__data_model_process",
)
async def test_online_consumer_accepts_online_data_when_stream_role_is_stale_calibration() -> None:
    receiver = ContinuousDataSourceReceiver()
    receiver.set_source_label("eeg_1")
    receiver.set_required_channel_labels(["C3", "C4"])
    await receiver.set_message_model(
        AlgorithmDataMessageModel(
            source_label="eeg_1",
            timestamp_ms=1,
            package=DevicePackageModel(
                data_type=DataTypeEnum.EEG,
                channel_number=2,
                sample_rate=1000.0,
                channel_label=["C3", "C4"],
                other_information={
                    "stream_role": "calibration",
                    "subject_id": "S1",
                    "exp_name": "vme",
                    "exp_task": "left_vs_rest",
                    "session_id": "session1",
                },
            ),
        )
    )

    pending_get_data = asyncio.create_task(receiver.get_data())
    await asyncio.sleep(0)

    await receiver.set_message_model(
        AlgorithmDataMessageModel(
            source_label="eeg_1",
            timestamp_ms=2,
            package=DataPackageModel(
                data=np.asarray([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
                data_position=100,
            ),
        )
    )

    algorithm_data_object = await asyncio.wait_for(pending_get_data, timeout=1.0)
    assert algorithm_data_object.start_position == 100
    assert algorithm_data_object.subject_id == "S1"
    assert algorithm_data_object.other_information["stream_role"] == "calibration"
    np.testing.assert_allclose(
        algorithm_data_object.data,
        np.asarray(
            [
                [1.0, 3.0],
                [2.0, 4.0],
                [0.0, 0.0],
            ],
            dtype=np.float32,
        ),
    )
