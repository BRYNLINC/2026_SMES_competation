from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESS_HUB_APP_ROOT = PROJECT_ROOT / "app" / "ProcessHub"
if str(PROCESS_HUB_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(PROCESS_HUB_APP_ROOT))

from Algorithm.api.converter.AlgorithmRPCMessageConverter import AlgorithmRPCMessageConverter
from Algorithm.api.model.AlgorithmRPCServiceModel import (
    AlgorithmDataMessageModel,
    AlgorithmReportMessageModel,
    AlgorithmStatusEnum,
    AlgorithmStatusMessageModel,
)
from Common.model.CommonMessageModel import (
    DevicePackageModel,
    DataTypeEnum,
    ReportSourceInformationModel,
    ResultPackageModel,
)


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("algorithm_rpc")]


@pytest.mark.test_id("ARC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("AlgorithmDataMessage protobuf/model 转换应保留 oneof 包类型和时间戳")
@pytest.mark.tested(
    file="app/ProcessHub/Algorithm/api/converter/AlgorithmRPCMessageConverter.py",
    function="model_to_protobuf/protobuf_to_model",
)
def test_algorithm_data_message_roundtrip_preserves_device_package() -> None:
    model = AlgorithmDataMessageModel(
        source_label="eeg_1",
        timestamp_ms=123456,
        package=DevicePackageModel(
            data_type=DataTypeEnum.EEG,
            channel_number=8,
            sample_rate=1000.0,
            channel_label=["C3", "CZ", "C4"],
            other_information={"exp_task": "left_vs_rest"},
        ),
    )

    protobuf_message = AlgorithmRPCMessageConverter.model_to_protobuf(model)
    roundtrip_model = AlgorithmRPCMessageConverter.protobuf_to_model(protobuf_message)

    assert protobuf_message.WhichOneof("package") == "devicePackage"
    assert protobuf_message.sourceLabel == "eeg_1"
    assert protobuf_message.timestamp_ms == 123456
    assert roundtrip_model.source_label == "eeg_1"
    assert roundtrip_model.timestamp_ms == 123456
    assert roundtrip_model.package.channel_number == 8
    assert roundtrip_model.package.channel_label == ["C3", "CZ", "C4"]
    assert roundtrip_model.package.other_information == {"exp_task": "left_vs_rest"}


@pytest.mark.test_id("ARC-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("AlgorithmReportMessage protobuf/model 转换应保留结果 payload 和 source position")
@pytest.mark.tested(
    file="app/ProcessHub/Algorithm/api/converter/AlgorithmRPCMessageConverter.py",
    function="model_to_protobuf/protobuf_to_model",
)
def test_algorithm_report_message_roundtrip_preserves_result_payload_and_positions() -> None:
    model = AlgorithmReportMessageModel(
        timestamp_ms=7890,
        package=ResultPackageModel(
            result='{"predict_label": 1, "predict_time_ms": 88}',
            report_source_information=[
                ReportSourceInformationModel(source_label="eeg_1", position=512.0),
                ReportSourceInformationModel(source_label="eeg_2", position=768.0),
            ],
        ),
    )

    protobuf_message = AlgorithmRPCMessageConverter.model_to_protobuf(model)
    roundtrip_model = AlgorithmRPCMessageConverter.protobuf_to_model(protobuf_message)

    assert protobuf_message.WhichOneof("package") == "resultPackage"
    assert protobuf_message.resultPackage.WhichOneof("result") == "stringMessage"
    assert protobuf_message.resultPackage.reportSourceInformation[0].sourceLabel == "eeg_1"
    assert roundtrip_model.timestamp_ms == 7890
    assert roundtrip_model.package.result == '{"predict_label": 1, "predict_time_ms": 88}'
    assert [
        (item.source_label, item.position)
        for item in roundtrip_model.package.report_source_information
    ] == [("eeg_1", 512.0), ("eeg_2", 768.0)]


@pytest.mark.test_id("ARC-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("AlgorithmStatus protobuf/model 转换应正确映射枚举状态")
@pytest.mark.tested(
    file="app/ProcessHub/Algorithm/api/converter/AlgorithmRPCMessageConverter.py",
    function="model_to_protobuf/protobuf_to_model",
)
def test_algorithm_status_message_roundtrip_preserves_status_enum() -> None:
    model = AlgorithmStatusMessageModel(status=AlgorithmStatusEnum.RUNNING)

    protobuf_message = AlgorithmRPCMessageConverter.model_to_protobuf(model)
    roundtrip_model = AlgorithmRPCMessageConverter.protobuf_to_model(protobuf_message)

    assert protobuf_message.status == 3
    assert roundtrip_model.status is AlgorithmStatusEnum.RUNNING
