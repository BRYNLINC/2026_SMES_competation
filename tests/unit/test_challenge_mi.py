from __future__ import annotations

import csv
import hashlib
import json
import logging
import sys
from pathlib import Path
from types import MethodType

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESS_HUB_APP_ROOT = PROJECT_ROOT / "app" / "ProcessHub"
if str(PROCESS_HUB_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(PROCESS_HUB_APP_ROOT))

from ProcessHub.bci_competition.challenge.MI import ChallengeMI as challenge_mi_module
from ProcessHub.bci_competition.challenge.MI.ChallengeMI import ChallengeMI
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel
from Common.model.CommonMessageModel import (
    DataPackageModel,
    DataTypeEnum,
    DevicePackageModel,
    ReportSourceInformationModel,
)
from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("challenge_mi")]


@pytest.fixture()
def challenge() -> ChallengeMI:
    instance = ChallengeMI()
    instance._ChallengeMI__logger = logging.getLogger("test_challenge_mi")
    instance._ChallengeMI__config_dict = {
        "score_config": {
            "accuracy_score_max": 80.0,
            "accuracy_stability_penalty_lambda": 0.5,
            "reaction_time_score_max": 2.0,
            "reaction_time_reference_ms": 1000.0,
            "channel_score_max": 8.0,
            "channel_reference_count": 8,
            "calibration_score_max": 7.0,
            "calibration_reference_trials_per_class": 10,
            "model_size_score_max": 3.0,
            "model_size_reference_mb": 150.0,
            "task_baseline_score": {
                "mi_left_vs_rest": 10.0,
                "mi_right_vs_rest": 12.0,
            },
            "timeout_predict_label": "wrong_from_score_config",
        },
        "strategy_config": {
            "timeout_setting": {
                "predict_timeout": {
                    "timeout_limit": 1.0,
                    "timeout_predict_label": "timeout_label_from_strategy",
                }
            }
        },
    }
    return instance


def _run_coroutine_sync(coroutine):
    try:
        coroutine.send(None)
    except StopIteration as exc:
        return exc.value
    raise AssertionError("coroutine did not finish synchronously")


def test_device_update_uses_subject_id_from_device_metadata(challenge: ChallengeMI) -> None:
    message = AlgorithmDataMessageModel(
        source_label="eeg_1",
        timestamp_ms=1,
        package=DevicePackageModel(
            data_type=DataTypeEnum.EEG,
            channel_number=2,
            sample_rate=1000.0,
            channel_label=["C3", "C4"],
            other_information={
                "subject_id": "sub_15",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": "session1",
                "stream_role": "online",
            },
        ),
    )

    forwarded_message = _run_coroutine_sync(challenge.receive_message(message))

    assert forwarded_message is message
    assert challenge._ChallengeMI__current_subject_id == "sub_15"


def _make_trial_record(
    *,
    subject_id: str = "S1",
    task_id: str = "mi_left_vs_rest",
    exp_name: str = "mi",
    exp_task: str = "left_vs_rest",
    session_id: str = "session_1",
    trial_id: str = "1",
    predict_label: str = "1",
    true_label: str = "1",
    is_correct: bool | None = True,
    trial_score: float | None = 1.0,
    predict_time_ms: float | None = 200.0,
) -> dict:
    return {
        "subject_id": subject_id,
        "task_id": task_id,
        "task": exp_name,
        "exam": exp_task,
        "stage_id": f"{task_id}|{session_id}",
        "transport_block_id": session_id,
        "block_id": session_id,
        "exp_name": exp_name,
        "exp_task": exp_task,
        "session_id": session_id,
        "trial_id": trial_id,
        "raw_label": true_label,
        "raw_predict_label": predict_label,
        "true_label": true_label,
        "predict_label": predict_label,
        "is_correct": is_correct,
        "trial_score": trial_score,
        "is_timeout": False,
        "is_invalid_output": False,
        "judge_message": None,
        "platform_trial_start_position": 100,
        "platform_trial_end_position": 200,
        "platform_trial_ready_wallclock": 10.0,
        "platform_report_receive_wallclock": 10.2,
        "predict_time_ms": predict_time_ms,
        "platform_raw_trigger_value": 1,
        "platform_true_label": true_label,
        "report_source_information": [{"source_label": "eeg_1", "position": 200.0}],
    }


@pytest.mark.test_id("CMI-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("结果 payload 解析应兼容 dict、标量 JSON、非法文本和空值")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__parse_result_payload",
)
@pytest.mark.parametrize(
    ("raw_result", "expected"),
    [
        ('{"predict_label": 1, "predict_time_ms": 12}', {"predict_label": 1, "predict_time_ms": 12}),
        ("1", {"predict_label": "1"}),
        ("not-json", {"predict_label": "not-json"}),
        (None, {}),
    ],
)
def test_parse_result_payload_variants(raw_result, expected) -> None:
    assert ChallengeMI._ChallengeMI__parse_result_payload(raw_result) == expected


@pytest.mark.test_id("CMI-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("算法输出解析应正确区分超时、合法输出和非法输出")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_predict_output",
)
def test_resolve_predict_output_handles_timeout_valid_and_invalid(challenge: ChallengeMI) -> None:
    timeout_result = challenge._ChallengeMI__resolve_predict_output("0", is_timeout=True)
    valid_result = challenge._ChallengeMI__resolve_predict_output("1", is_timeout=False)
    invalid_result = challenge._ChallengeMI__resolve_predict_output("3", is_timeout=False)
    missing_result = challenge._ChallengeMI__resolve_predict_output("", is_timeout=False)

    assert timeout_result["predict_label"] == "timeout_label_from_strategy"
    assert timeout_result["judge_message"] is not None
    assert valid_result == {
        "predict_label": "1",
        "raw_predict_label": "1",
        "is_invalid_output": False,
        "judge_message": None,
    }
    assert invalid_result["is_invalid_output"] is True
    assert "仅允许 0/1" in str(invalid_result["judge_message"])
    assert missing_result["is_invalid_output"] is True
    assert "缺少 predict_label" in str(missing_result["judge_message"])


@pytest.mark.test_id("CMI-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("超时配置应从 strategy_config 读取并优先于 score_config")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_timeout_seconds/__resolve_timeout_predict_label",
)
def test_resolve_timeout_settings_from_strategy(challenge: ChallengeMI) -> None:
    assert challenge._ChallengeMI__resolve_timeout_seconds() == 1.0
    assert challenge._ChallengeMI__resolve_timeout_predict_label() == "timeout_label_from_strategy"


@pytest.mark.test_id("CMI-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("通道数解析应遵循 requested_channel_count 优先级")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_channel_count",
)
def test_resolve_channel_count_priority(challenge: ChallengeMI) -> None:
    challenge._ChallengeMI__algorithm_metadata = {
        "requested_channel_count": "6",
        "used_channel_count": "4",
        "required_channel_labels": {"eeg_1": ["C3", "CZ", "C4"]},
    }
    challenge._ChallengeMI__requested_channel_count = 2
    assert challenge._ChallengeMI__resolve_channel_count() == 6

    challenge._ChallengeMI__algorithm_metadata = {
        "requested_channel_count": "bad",
        "required_channel_labels": {"eeg_1": ["C3", "CZ", "C4", "CP3"]},
    }
    challenge._ChallengeMI__requested_channel_count = 5
    assert challenge._ChallengeMI__resolve_channel_count() == 5


@pytest.mark.test_id("CMI-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("校准 trial 数应裁剪到 0 到默认上限范围")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_calibration_trials_per_class",
)
def test_resolve_calibration_trials_per_class_clamps_to_supported_range(challenge: ChallengeMI) -> None:
    challenge._ChallengeMI__algorithm_metadata = {"calibration_trials_per_class_requested": "11"}
    assert challenge._ChallengeMI__resolve_calibration_trials_per_class() == 10

    challenge._ChallengeMI__algorithm_metadata = {"calibration_trials_per_class_requested": "-2"}
    assert challenge._ChallengeMI__resolve_calibration_trials_per_class() == 0

    challenge._ChallengeMI__algorithm_metadata = {"calibration_trials_per_class_requested": "bad"}
    challenge._ChallengeMI__requested_calibration_trial_count = 3
    assert challenge._ChallengeMI__resolve_calibration_trials_per_class() == 3


@pytest.mark.test_id("CMI-06")
@pytest.mark.priority("P1")
@pytest.mark.requirement("模型大小解析应接受合法浮点并忽略非法值")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_model_size_mb",
)
def test_resolve_model_size_mb_handles_valid_invalid_and_missing(challenge: ChallengeMI) -> None:
    challenge._ChallengeMI__algorithm_metadata = {"platform_model_size_mb": "12.5", "platform_team_id": "team_0"}
    assert challenge._ChallengeMI__resolve_model_size_mb() == 12.5

    challenge._ChallengeMI__algorithm_metadata = {"platform_model_size_mb": "bad", "platform_team_id": "team_0"}
    assert challenge._ChallengeMI__resolve_model_size_mb() is None

    challenge._ChallengeMI__algorithm_metadata = {"platform_team_id": "team_0"}
    assert challenge._ChallengeMI__resolve_model_size_mb() is None


@pytest.mark.test_id("CMI-07")
@pytest.mark.priority("P0")
@pytest.mark.requirement("评分函数应按配置公式返回稳定结果")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__compute_accuracy_score/__compute_reaction_time_score/__compute_channel_score/__compute_calibration_score/__compute_model_size_score",
)
def test_score_components_follow_expected_formula(challenge: ChallengeMI) -> None:
    assert challenge._ChallengeMI__compute_accuracy_score(90.0, 10.0) == pytest.approx(68.0)
    assert challenge._ChallengeMI__compute_reaction_time_score(250.0) == pytest.approx(1.5)
    assert challenge._ChallengeMI__compute_channel_score(1) == pytest.approx(8.0)
    assert challenge._ChallengeMI__compute_channel_score(8) == pytest.approx(0.0)
    assert challenge._ChallengeMI__compute_calibration_score(0) == pytest.approx(7.0)
    assert challenge._ChallengeMI__compute_calibration_score(10) == pytest.approx(0.0)
    assert challenge._ChallengeMI__compute_model_size_score(75.0) == pytest.approx(1.5)
    assert challenge._ChallengeMI__compute_model_size_score(None) == pytest.approx(0.0)


@pytest.mark.test_id("CMI-08")
@pytest.mark.priority("P1")
@pytest.mark.requirement("队伍标识解析应遵循 platform_team_id、team_id、环境变量、默认值顺序")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_team_id",
)
def test_resolve_team_id_priority(challenge: ChallengeMI, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEAM_ID", "env_team")

    challenge._ChallengeMI__algorithm_metadata = {"platform_team_id": "platform_team", "team_id": "meta_team"}
    assert challenge._ChallengeMI__resolve_team_id() == "platform_team"

    challenge._ChallengeMI__algorithm_metadata = {"team_id": "meta_team"}
    assert challenge._ChallengeMI__resolve_team_id() == "meta_team"

    challenge._ChallengeMI__algorithm_metadata = {}
    assert challenge._ChallengeMI__resolve_team_id() == "env_team"


@pytest.mark.test_id("CMI-09")
@pytest.mark.priority("P1")
@pytest.mark.requirement("标签与任务辅助函数应正确标准化输入")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__map_label_value/__normalize_binary_label/__normalize_raw_trigger_value/__coerce_optional_int/__resolve_task_id/__split_task_id",
)
def test_helper_methods_normalize_task_and_label_values() -> None:
    assert ChallengeMI._ChallengeMI__map_label_value("left_vs_rest", "3") == "0"
    assert ChallengeMI._ChallengeMI__map_label_value("right_vs_rest", "2") == "1"
    assert ChallengeMI._ChallengeMI__map_label_value("right_vs_rest", "1") == "1"
    assert ChallengeMI._ChallengeMI__map_label_value("right_vs_rest", "7") is None
    assert ChallengeMI._ChallengeMI__normalize_binary_label(" 1 ") == "1"
    assert ChallengeMI._ChallengeMI__normalize_binary_label("2") is None
    assert ChallengeMI._ChallengeMI__normalize_raw_trigger_value("3") == 3
    assert ChallengeMI._ChallengeMI__normalize_raw_trigger_value("7") is None
    assert ChallengeMI._ChallengeMI__coerce_optional_int("9") == 9
    assert ChallengeMI._ChallengeMI__coerce_optional_int("bad") is None
    assert ChallengeMI._ChallengeMI__resolve_task_id("mi", "left_vs_rest") == "mi_left_vs_rest"
    assert ChallengeMI._ChallengeMI__split_task_id("mi_right_vs_rest") == ("mi", "right_vs_rest")


@pytest.mark.test_id("CMI-10")
@pytest.mark.priority("P1")
@pytest.mark.requirement("结果文件路径解析应优先尝试 VirtualReceiver 配置附近目录")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_virtual_receiver_data_file_path",
)
def test_resolve_virtual_receiver_data_file_path_uses_config_relative_candidates(
    challenge: ChallengeMI,
    tmp_path: Path,
) -> None:
    collector_root = tmp_path / "app" / "Collector"
    config_dir = collector_root / "Collector" / "receiver" / "virtual_receiver"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "VirtualReceiverConfig.yml"
    config_path.write_text("{}", encoding="utf-8")

    data_file = collector_root / "Collector" / "receiver" / "virtual_receiver" / "data" / "S1" / "trial.dat"
    data_file.parent.mkdir(parents=True)
    data_file.write_bytes(b"demo")

    challenge._ChallengeMI__virtual_receiver_config_path = config_path
    resolved = challenge._ChallengeMI__resolve_virtual_receiver_data_file_path(
        "Collector/receiver/virtual_receiver/data/S1/trial.dat"
    )

    assert resolved == data_file


@pytest.mark.test_id("CMI-11")
@pytest.mark.priority("P0")
@pytest.mark.requirement("重复 trial 记录只能计分一次，避免污染累计分数")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__append_record_and_score",
)
def test_append_record_and_score_deduplicates_same_trial(
    challenge: ChallengeMI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        challenge,
        "_ChallengeMI__persist_incremental_result_snapshot",
        lambda: None,
    )
    challenge._ChallengeMI__algorithm_metadata = {"platform_team_id": "team_dup"}

    record = _make_trial_record()
    challenge._ChallengeMI__append_record_and_score(record.copy())
    challenge._ChallengeMI__append_record_and_score(record.copy())

    assert len(challenge._ChallengeMI__trial_record_list) == 1
    assert len(challenge._ChallengeMI__score_package_list) == 1
    assert len(challenge._ChallengeMI__record_key_set) == 1


@pytest.mark.test_id("CMI-12")
@pytest.mark.priority("P0")
@pytest.mark.requirement("任务汇总应统计分任务准确率并对低于 baseline 的任务置零")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__build_task_summary_dict",
)
def test_build_task_summary_dict_applies_baseline_floor(challenge: ChallengeMI) -> None:
    left_record = _make_trial_record(task_id="mi_left_vs_rest", trial_id="1", predict_time_ms=100.0)
    left_record["score_snapshot"] = {
        "channel_rounds": 8,
        "channel_score": 0.0,
        "calibration_rounds": 10,
        "calibration_score": 0.0,
        "model_size_mb": 100.0,
        "model_size_score": 1.0,
        "cumulative_avg_reaction_time_ms": 100.0,
        "cumulative_avg_reaction_time_score": 1.8,
        "cumulative_accuracy_percent": 100.0,
        "cumulative_accuracy_std_percent": 0.0,
        "cumulative_accuracy_score": 80.0,
        "cumulative_score": 81.8,
    }
    right_record = _make_trial_record(
        subject_id="S2",
        task_id="mi_right_vs_rest",
        exp_task="right_vs_rest",
        trial_id="1",
        predict_label="0",
        true_label="1",
        is_correct=False,
        trial_score=0.0,
        predict_time_ms=900.0,
    )
    right_record["score_snapshot"] = {
        "channel_rounds": 8,
        "channel_score": 0.0,
        "calibration_rounds": 10,
        "calibration_score": 0.0,
        "model_size_mb": 100.0,
        "model_size_score": 1.0,
        "cumulative_avg_reaction_time_ms": 900.0,
        "cumulative_avg_reaction_time_score": 0.2,
        "cumulative_accuracy_percent": 0.0,
        "cumulative_accuracy_std_percent": 0.0,
        "cumulative_accuracy_score": 0.0,
        "cumulative_score": 1.2,
    }
    challenge._ChallengeMI__trial_record_list = [left_record, right_record]

    summary = challenge._ChallengeMI__build_task_summary_dict()

    assert summary["mi_left_vs_rest"]["trial_count"] == 1
    assert summary["mi_left_vs_rest"]["per_subject_accuracy_percent"] == {"S1": 100.0}
    assert summary["mi_left_vs_rest"]["adjusted_task_score"] == pytest.approx(81.8)
    assert summary["mi_right_vs_rest"]["trial_count"] == 1
    assert summary["mi_right_vs_rest"]["per_subject_accuracy_percent"] == {"S2": 0.0}
    assert summary["mi_right_vs_rest"]["baseline_score"] == 12.0
    assert summary["mi_right_vs_rest"]["adjusted_task_score"] == 0.0


@pytest.mark.test_id("CMI-13")
@pytest.mark.priority("P1")
@pytest.mark.requirement("trial 导出行应按 team/task 顺序编号并序列化 report position")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__build_trial_record_export_row_list",
)
def test_build_trial_record_export_row_list_formats_report_positions(challenge: ChallengeMI) -> None:
    challenge._ChallengeMI__algorithm_metadata = {"platform_team_id": "team_export"}
    first_record = _make_trial_record(task_id="mi_left_vs_rest", trial_id="1")
    first_record["score_snapshot"] = {
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": 10.0,
    }
    second_record = _make_trial_record(
        task_id="mi_left_vs_rest",
        trial_id="2",
        predict_label="0",
        true_label="1",
        is_correct=False,
        trial_score=0.0,
    )
    second_record["score_snapshot"] = {
        "cumulative_accuracy_percent": 50.0,
        "cumulative_score": 5.0,
    }
    other_task_record = _make_trial_record(task_id="mi_right_vs_rest", trial_id="1")
    other_task_record["score_snapshot"] = {
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": 10.0,
    }
    challenge._ChallengeMI__trial_record_list = [first_record, other_task_record, second_record]

    row_list = challenge._ChallengeMI__build_trial_record_export_row_list("mi_left_vs_rest")
    all_row_list = challenge._ChallengeMI__build_trial_record_export_row_list()

    assert row_list[0]["team_id"] == "team_export"
    assert row_list[0]["team_trial_index"] == 1
    assert row_list[0]["task_trial_index"] == 1
    assert row_list[0]["report_position"] == "eeg_1:200.0"
    assert row_list[1]["team_trial_index"] == 3
    assert row_list[1]["task_trial_index"] == 2
    assert row_list[1]["cumulative_accuracy_percent"] == 50.0
    assert [row["task_trial_index"] for row in all_row_list] == [1, 1, 2]


@pytest.mark.test_id("CMI-14")
@pytest.mark.priority("P0")
@pytest.mark.requirement("trial 记录落盘后应同时写入 CSV 与 runtime_state.db")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__persist_trial_record_files",
)
def test_persist_trial_record_files_writes_csv_and_runtime_state(
    challenge: ChallengeMI,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results_root = tmp_path / "results"
    monkeypatch.setattr(challenge_mi_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        ChallengeMI,
        "_ChallengeMI__resolve_results_root_dir",
        staticmethod(lambda: results_root),
    )
    challenge._ChallengeMI__algorithm_metadata = {"platform_team_id": "team_trial"}
    record = _make_trial_record()
    record["score_snapshot"] = {
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": 10.0,
    }
    challenge._ChallengeMI__trial_record_list = [record]

    challenge._ChallengeMI__persist_trial_record_files()

    trial_csv = results_root / "team_trial" / "03_trial_records.csv"
    task_csv = results_root / "team_trial" / "task_trials" / "mi_left_vs_rest_trial_records.csv"
    assert trial_csv.exists()
    assert task_csv.exists()

    with trial_csv.open("r", encoding="utf-8-sig", newline="") as file:
        row_list = list(csv.DictReader(file))
    assert len(row_list) == 1
    assert row_list[0]["team_id"] == "team_trial"
    assert row_list[0]["trial_id"] == "1"

    db_row_list = rss.load_team_trial_record_rows(results_root / "runtime_state.db", "team_trial")
    assert len(db_row_list) == 1
    assert db_row_list[0]["task_id"] == "mi_left_vs_rest"
    assert db_row_list[0]["trial_id"] == "1"


@pytest.mark.test_id("CMI-14A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("增量 trial 落盘应只追加新 trial 且不重复已有 runtime_state.db 行")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__persist_trial_record_files/__sync_trial_record_rows_to_runtime_state",
)
def test_persist_trial_record_files_incremental_db_write_appends_new_rows(
    challenge: ChallengeMI,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results_root = tmp_path / "results"
    monkeypatch.setattr(challenge_mi_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        ChallengeMI,
        "_ChallengeMI__resolve_results_root_dir",
        staticmethod(lambda: results_root),
    )
    challenge._ChallengeMI__algorithm_metadata = {"platform_team_id": "team_trial"}

    first_record = _make_trial_record(trial_id="1")
    first_record["score_snapshot"] = {
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": 10.0,
    }
    second_record = _make_trial_record(trial_id="2", predict_label="0", true_label="0", predict_time_ms=250.0)
    second_record["score_snapshot"] = {
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": 20.0,
    }

    challenge._ChallengeMI__trial_record_list = [first_record]
    challenge._ChallengeMI__persist_trial_record_files(incremental_db_write=True)

    challenge._ChallengeMI__trial_record_list = [first_record, second_record]
    challenge._ChallengeMI__persist_trial_record_files(incremental_db_write=True)

    db_row_list = rss.load_team_trial_record_rows(results_root / "runtime_state.db", "team_trial")
    assert len(db_row_list) == 2
    assert [row["trial_id"] for row in db_row_list] == ["1", "2"]


@pytest.mark.test_id("CMI-15")
@pytest.mark.priority("P0")
@pytest.mark.requirement("总分结果落盘后应写入 team/task/subject 概览和 scoreboard")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__persist_score_result_file",
)
def test_persist_score_result_file_writes_summary_csvs_and_db(
    challenge: ChallengeMI,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results_root = tmp_path / "results"
    monkeypatch.setattr(challenge_mi_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        ChallengeMI,
        "_ChallengeMI__resolve_results_root_dir",
        staticmethod(lambda: results_root),
    )
    challenge._ChallengeMI__algorithm_metadata = {"platform_team_id": "team_summary"}
    seed_source = "S1|mi|left_vs_rest|session_1"
    expected_shuffle_seed = int.from_bytes(hashlib.sha256(seed_source.encode("utf-8")).digest()[:8], "big")
    expected_shuffle_seed_text = f"{seed_source}:{expected_shuffle_seed}"
    challenge._ChallengeMI__trial_record_list = [
        _make_trial_record(subject_id="S1", exp_name="mi", exp_task="left_vs_rest", session_id="session_1")
    ]
    final_score_result = {
        "team_id": "team_summary",
        "total_score": 88.8,
        "record_count": 2,
        "mean_accuracy_percent": 75.0,
        "avg_reaction_time_ms": 320.0,
        "task_metric_list": [
            {
                "task_name": "mi_left_vs_rest",
                "exp_name": "mi",
                "exp_task": "left_vs_rest",
                "subject_count": 1,
                "trial_count": 2,
                "cumulative_accuracy_percent": 75.0,
                "avg_reaction_time_ms": 320.0,
                "task_score": 44.4,
            }
        ],
        "task_summary": {
            "mi_left_vs_rest": {
                "exp_name": "mi",
                "exp_task": "left_vs_rest",
                "trial_count": 2,
                "per_subject_accuracy_percent": {"S1": 75.0},
                "per_subject_trial_count": {"S1": 2},
            }
        },
    }

    challenge._ChallengeMI__persist_score_result_file(final_score_result)

    team_csv = results_root / "team_summary" / "00_team_overview.csv"
    task_csv = results_root / "team_summary" / "01_task_overview.csv"
    subject_csv = results_root / "team_summary" / "02_subject_task_overview.csv"
    root_scoreboard_csv = results_root / "00_team_score_overview.csv"
    assert team_csv.exists()
    assert task_csv.exists()
    assert subject_csv.exists()
    assert root_scoreboard_csv.exists()

    team_row = rss.load_team_overview_row(results_root / "runtime_state.db", "team_summary")
    score_rows = rss.load_team_score_overview_rows(results_root / "runtime_state.db")
    task_rows = rss.load_team_task_overview_rows(results_root / "runtime_state.db", "team_summary")
    subject_rows = rss.load_team_subject_task_overview_rows(results_root / "runtime_state.db", "team_summary")
    assert team_row is not None
    assert team_row["team_id"] == "team_summary"
    assert score_rows[0]["team_id"] == "team_summary"
    assert task_rows[0]["task_id"] == "mi_left_vs_rest"
    assert subject_rows[0]["subject_id"] == "S1"
    assert team_row["global_seed"] == 2026
    assert team_row["collector_session_shuffle_seed"] == expected_shuffle_seed_text

    with team_csv.open("r", encoding="utf-8-sig", newline="") as file:
        csv_row = next(csv.DictReader(file))
    assert csv_row["global_seed"] == "2026"
    assert csv_row["collector_session_shuffle_seed"] == expected_shuffle_seed_text


@pytest.mark.test_id("CMI-16")
@pytest.mark.priority("P1")
@pytest.mark.requirement("历史遗留结果文件清理应删除旧文件与 task_trials 下残留 CSV")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__cleanup_legacy_result_files",
)
def test_cleanup_legacy_result_files_removes_old_exports(tmp_path: Path) -> None:
    result_dir = tmp_path / "team_cleanup"
    task_trials_dir = result_dir / "task_trials"
    task_trials_dir.mkdir(parents=True)

    for file_name in (
        "score_result.json",
        "00_team_overview.csv",
        "03_trial_records.csv",
        "demo_result.csv",
        "demo_score.csv",
        "demo_trial_summary.csv",
    ):
        (result_dir / file_name).write_text("legacy", encoding="utf-8")
    (task_trials_dir / "mi_left_vs_rest_trial_records.csv").write_text("legacy", encoding="utf-8")

    ChallengeMI._ChallengeMI__cleanup_legacy_result_files(result_dir)

    assert not (result_dir / "score_result.json").exists()
    assert not (result_dir / "00_team_overview.csv").exists()
    assert not (result_dir / "demo_result.csv").exists()
    assert not (result_dir / "demo_score.csv").exists()
    assert not (result_dir / "demo_trial_summary.csv").exists()
    assert not (task_trials_dir / "mi_left_vs_rest_trial_records.csv").exists()


@pytest.mark.test_id("CMI-17")
@pytest.mark.priority("P0")
@pytest.mark.requirement("trial payload 构建应覆盖真值、超时、非法输出和 report source 回退路径")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__build_trial_record_from_payload",
)
def test_build_trial_record_from_payload_handles_multiple_label_and_output_paths(
    challenge: ChallengeMI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    challenge._ChallengeMI__current_subject_id = "CTX_SUBJECT"
    challenge._ChallengeMI__current_exp_name = "mi"
    challenge._ChallengeMI__current_exp_task = "left_vs_rest"
    challenge._ChallengeMI__current_session_id = "session_ctx"
    challenge._ChallengeMI__current_block_id = "block_ctx"

    direct_record = challenge._ChallengeMI__build_trial_record_from_payload(
        {
            "platform_subject_id": "S1",
            "platform_exp_name": "mi",
            "platform_exp_task": "left_vs_rest",
            "platform_session_id": "session_1",
            "platform_block_id": "block_1",
            "platform_trial_id": "3",
            "platform_true_label": "1",
            "platform_raw_trigger_value": "3",
            "predict_label": "1",
            "predict_time_ms": 88,
        }
    )
    assert direct_record["true_label"] == "1"
    assert direct_record["raw_label"] == "3"
    assert direct_record["is_correct"] is True
    assert direct_record["trial_score"] == 1.0
    assert direct_record["report_source_information"] == []

    raw_trigger_record = challenge._ChallengeMI__build_trial_record_from_payload(
        {
            "platform_subject_id": "S2",
            "platform_exp_name": "mi",
            "platform_exp_task": "right_vs_rest",
            "platform_session_id": "session_2",
            "platform_trial_id": "4",
            "platform_raw_trigger_value": "2",
            "predict_label": "0",
            "platform_runtime_ms": 999,
            "report_source_position": 456.0,
        }
    )
    assert raw_trigger_record["true_label"] == "1"
    assert raw_trigger_record["is_correct"] is False
    assert raw_trigger_record["trial_score"] == 0.0
    assert raw_trigger_record["predict_time_ms"] == 999.0
    assert raw_trigger_record["report_source_information"] == [{"source_label": "eeg_1", "position": 456.0}]

    monkeypatch.setattr(
        challenge,
        "_ChallengeMI__resolve_labels",
        MethodType(lambda self, **kwargs: ("2", "0"), challenge),
    )
    fallback_record = challenge._ChallengeMI__build_trial_record_from_payload(
        {
            "platform_subject_id": "S3",
            "platform_exp_name": "mi",
            "platform_exp_task": "left_vs_rest",
            "platform_session_id": "session_3",
            "platform_trial_id": "5",
            "predict_label": "bad-value",
            "predict_time_ms": "not-a-number",
        },
        [ReportSourceInformationModel(source_label="eeg_aux", position=12.5)],
    )
    assert fallback_record["raw_label"] == "2"
    assert fallback_record["true_label"] == "0"
    assert fallback_record["is_invalid_output"] is True
    assert fallback_record["is_correct"] is False
    assert fallback_record["trial_score"] == 0.0
    assert fallback_record["predict_time_ms"] == 0.0
    assert fallback_record["report_source_information"] == [{"source_label": "eeg_aux", "position": 12.5}]

    timeout_record = challenge._ChallengeMI__build_trial_record_from_payload(
        {
            "platform_subject_id": "S4",
            "platform_exp_name": "mi",
            "platform_exp_task": "left_vs_rest",
            "platform_session_id": "session_4",
            "platform_trial_id": "6",
            "platform_timeout": True,
            "predict_label": "0",
        }
    )
    assert timeout_record["is_timeout"] is True
    assert timeout_record["predict_label"] == "timeout_label_from_strategy"
    assert timeout_record["is_correct"] is False
    assert timeout_record["trial_score"] == 0.0


@pytest.mark.test_id("CMI-18")
@pytest.mark.priority("P1")
@pytest.mark.requirement("report source 信息归一化应接受 dict、model 并支持 payload 回退")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__normalize_report_source_information",
)
def test_normalize_report_source_information_supports_multiple_input_forms() -> None:
    normalized = ChallengeMI._ChallengeMI__normalize_report_source_information(
        [
            {"source_label": "eeg_1", "position": 100.0},
            ReportSourceInformationModel(source_label="eeg_2", position=200.0),
        ],
        {},
    )
    fallback = ChallengeMI._ChallengeMI__normalize_report_source_information(
        None,
        {"report_source_label": "eeg_fallback", "report_source_position": 321.0},
    )
    empty = ChallengeMI._ChallengeMI__normalize_report_source_information(None, {})

    assert normalized == [
        {"source_label": "eeg_1", "position": 100.0},
        {"source_label": "eeg_2", "position": 200.0},
    ]
    assert fallback == [{"source_label": "eeg_fallback", "position": 321.0}]
    assert empty == []


@pytest.mark.test_id("CMI-19")
@pytest.mark.priority("P1")
@pytest.mark.requirement("score snapshot 应累计反应时、准确率和稳定性分数")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__build_record_score_snapshot",
)
def test_build_record_score_snapshot_accumulates_accuracy_and_runtime(challenge: ChallengeMI) -> None:
    first_record = _make_trial_record(trial_id="1", predict_time_ms=100.0, is_correct=True, trial_score=1.0)
    second_record = _make_trial_record(
        trial_id="2",
        predict_time_ms=300.0,
        predict_label="0",
        true_label="1",
        is_correct=False,
        trial_score=0.0,
    )
    challenge._ChallengeMI__trial_record_list = [first_record, second_record]

    snapshot = challenge._ChallengeMI__build_record_score_snapshot(second_record)

    assert snapshot["cumulative_avg_reaction_time_ms"] == pytest.approx(200.0)
    assert snapshot["cumulative_accuracy"] == pytest.approx(0.5)
    assert snapshot["cumulative_accuracy_percent"] == pytest.approx(50.0)
    assert snapshot["cumulative_accuracy_std_percent"] > 0.0
    assert snapshot["cumulative_avg_reaction_time_score"] == pytest.approx(1.6)
    assert snapshot["cumulative_accuracy_score"] == pytest.approx(30.0)
    assert snapshot["cumulative_score"] == pytest.approx(31.6)


@pytest.mark.test_id("CMI-20")
@pytest.mark.priority("P1")
@pytest.mark.requirement("predict_time_ms 边界值解析应覆盖 0、超时值、负值和非法文本")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__resolve_predict_time_ms",
)
@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"predict_time_ms": 0}, 0.0),
        ({"predict_time_ms": 1001}, 1001.0),
        ({"platform_runtime_ms": -5}, -5.0),
        ({"predict_time_ms": "bad"}, 0.0),
        ({}, None),
    ],
)
def test_resolve_predict_time_ms_boundary_values(challenge: ChallengeMI, payload: dict, expected) -> None:
    assert challenge._ChallengeMI__resolve_predict_time_ms(payload) == expected


@pytest.mark.test_id("CMI-21")
@pytest.mark.priority("P1")
@pytest.mark.requirement("score package 文本应包含 task、trial、预测、真值和累计分数")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__build_trial_score_package",
)
def test_build_trial_score_package_formats_human_readable_summary() -> None:
    record = _make_trial_record()
    record["score_snapshot"] = {
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": 42.5,
    }

    score_package = ChallengeMI()._ChallengeMI__build_trial_score_package(record)

    assert "task=mi" in score_package.show_text
    assert "trial=1" in score_package.show_text
    assert "pred=1" in score_package.show_text
    assert "true=1" in score_package.show_text
    assert "score=42.500" in score_package.show_text
    assert score_package.score == pytest.approx(42.5)
    assert score_package.trial_time == pytest.approx(200.0)
    assert score_package.block_id == "mi_left_vs_rest|session_1"


@pytest.mark.test_id("CMI-22")
@pytest.mark.priority("P0")
@pytest.mark.requirement("发送给算法端的配置必须强制注入 predict_timeout_seconds，且不污染原始 challenge_to_algorithm_config")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="get_to_algorithm_config",
)
def test_get_to_algorithm_config_injects_predict_timeout_seconds_without_mutating_source_config(
    challenge: ChallengeMI,
) -> None:
    challenge._ChallengeMI__config_dict["challenge_to_algorithm_config"] = {
        "requested_channel_count": 8,
        "calibration_trials_per_class_requested": 5,
    }

    payload = _run_coroutine_sync(challenge.get_to_algorithm_config())

    assert payload == {
        "requested_channel_count": 8,
        "calibration_trials_per_class_requested": 5,
        "predict_timeout_seconds": 1.0,
    }
    assert challenge._ChallengeMI__config_dict["challenge_to_algorithm_config"] == {
        "requested_channel_count": 8,
        "calibration_trials_per_class_requested": 5,
    }


@pytest.mark.test_id("CMI-23")
@pytest.mark.priority("P0")
@pytest.mark.requirement("receive_timeout_trial 必须为缺失字段补齐 timeout 标签和默认耗时，并生成一条 timeout 记录")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="receive_timeout_trial",
)
def test_receive_timeout_trial_fills_default_timeout_fields_and_appends_record(
    challenge: ChallengeMI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(challenge, "_ChallengeMI__persist_incremental_result_snapshot", lambda: None)
    monkeypatch.setattr(
        challenge,
        "_ChallengeMI__resolve_labels",
        MethodType(lambda self, **kwargs: ("3", "1"), challenge),
    )

    _run_coroutine_sync(
        challenge.receive_timeout_trial(
            {
                "platform_subject_id": "S1",
                "platform_exp_name": "mi",
                "platform_exp_task": "left_vs_rest",
                "platform_session_id": "session_timeout",
                "platform_trial_id": "9",
            }
        )
    )

    assert len(challenge._ChallengeMI__trial_record_list) == 1
    record = challenge._ChallengeMI__trial_record_list[0]
    assert record["is_timeout"] is True
    assert record["predict_label"] == "timeout_label_from_strategy"
    assert record["predict_time_ms"] == pytest.approx(1000.0)
    assert record["trial_score"] == 0.0
    assert len(challenge._ChallengeMI__score_package_list) == 1


@pytest.mark.test_id("CMI-24")
@pytest.mark.priority("P1")
@pytest.mark.requirement("当前任务实时指标应暴露非法输出判定、裁判消息和 trial 数，供 JudgeWeb 与 live 状态使用")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="get_live_task_metrics",
)
def test_get_live_task_metrics_reflects_latest_invalid_output_and_trial_count(challenge: ChallengeMI) -> None:
    record = _make_trial_record(trial_id="3", predict_label="bad", is_correct=False, trial_score=0.0)
    record["task_id"] = "mi_left_vs_rest"
    record["is_invalid_output"] = True
    record["judge_message"] = "算法输出 predict_label=bad 超出允许范围，仅允许 0/1"
    record["score_snapshot"] = {
        "cumulative_accuracy_percent": 50.0,
        "cumulative_score": 15.0,
    }
    challenge._ChallengeMI__trial_record_list = [record]

    metrics = challenge.get_live_task_metrics()

    assert metrics == {
        "current_trial_score": 0.0,
        "current_task_score": 15.0,
        "current_task_accuracy_percent": 50.0,
        "judge_message": "算法输出 predict_label=bad 超出允许范围，仅允许 0/1",
        "is_invalid_output": True,
        "current_task_trial_count": 1,
        "task_id": "mi_left_vs_rest",
    }


@pytest.mark.test_id("CMI-25")
@pytest.mark.priority("P0")
@pytest.mark.requirement("timeout_trigger 必须从 DataPackage JSON 中提取 report source 信息并委托到 timeout trial 处理")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="timeout_trigger",
)
def test_timeout_trigger_parses_data_package_and_forwards_timeout_payload(
    challenge: ChallengeMI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict] = []

    async def fake_receive_timeout_trial(payload: dict) -> None:
        captured.append(payload)

    monkeypatch.setattr(challenge, "receive_timeout_trial", fake_receive_timeout_trial)

    message_model = AlgorithmDataMessageModel(
        source_label="eeg_1",
        timestamp_ms=1234,
        package=DataPackageModel(
            data='{"predict_label": 0, "report_source_label": "eeg_aux", "report_source_position": 456.5}'
        ),
    )

    result = _run_coroutine_sync(challenge.timeout_trigger(message_model))

    assert result is None
    assert captured == [
        {
            "predict_label": 0,
            "report_source_label": "eeg_aux",
            "report_source_position": 456.5,
            "report_source_information": [{"source_label": "eeg_aux", "position": 456.5}],
        }
    ]


@pytest.mark.test_id("CMI-26")
@pytest.mark.priority("P0")
@pytest.mark.requirement("receive_algorithm_config 必须缓存算法元数据、解析申请值并触发结果目录准备")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="receive_algorithm_config",
)
def test_receive_algorithm_config_updates_cached_requests_and_prepares_result_dir(
    challenge: ChallengeMI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepare_calls: list[bool] = []

    def fake_prepare_result_dir(*, force_cleanup: bool = False):
        prepare_calls.append(force_cleanup)
        return Path("ignored")

    challenge._ChallengeMI__task_static_score_snapshot_dict = {
        "mi_left_vs_rest": {"channel_score": 5.0}
    }
    monkeypatch.setattr(challenge, "_ChallengeMI__prepare_result_dir", fake_prepare_result_dir)

    _run_coroutine_sync(
        challenge.receive_algorithm_config(
            {
                "platform_team_id": "team_cfg",
                "requested_channel_count": "6",
                "calibration_trials_per_class_requested": "4",
                "platform_model_size_mb": "12.5",
            }
        )
    )

    assert challenge._ChallengeMI__algorithm_metadata["platform_team_id"] == "team_cfg"
    assert challenge._ChallengeMI__requested_channel_count == 6
    assert challenge._ChallengeMI__requested_calibration_trial_count == 4
    assert challenge._ChallengeMI__task_static_score_snapshot_dict == {}
    assert prepare_calls == [True]


@pytest.mark.test_id("CMI-26A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("restart_from_stage 恢复启动时必须把目标阶段之前的历史 trial 导入 ChallengeMI 内存和 runtime_state")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="receive_algorithm_config/__hydrate_preserved_trial_records",
)
def test_receive_algorithm_config_hydrates_preserved_trials_for_restart_from_stage(
    challenge: ChallengeMI,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results_root = tmp_path / "results"
    control_root = results_root / "control"
    team_dir = results_root / "team_resume"
    control_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(challenge_mi_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        ChallengeMI,
        "_ChallengeMI__resolve_results_root_dir",
        staticmethod(lambda: results_root),
    )

    control_root.joinpath("applied_recovery.json").write_text(
        json.dumps(
            {
                "recovery_mode": "restart_from_stage",
                "stage": {"subject_id": "S1", "exp_name": "mi", "exp_task": "right_vs_rest"},
                "collector_start_selector": {
                    "subject_id": "S1",
                    "exp_name": "mi",
                    "exp_task": "right_vs_rest",
                    "task_id": "mi_right_vs_rest",
                    "session_id": "session_1",
                    "block_id": 2,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    with (team_dir / "03_trial_records.csv").open("w", encoding="utf-8-sig", newline="") as file:
        fieldnames = challenge._ChallengeMI__build_trial_record_export_fieldnames()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "team_id": "team_resume",
                "team_trial_index": 1,
                "task_trial_index": 1,
                "subject_id": "S1",
                "task_id": "mi_left_vs_rest",
                "exp_name": "mi",
                "exp_task": "left_vs_rest",
                "session_id": "session_1",
                "block_id": "1",
                "trial_id": "1",
                "true_label": "1",
                "raw_predict_label": "1",
                "predict_label": "1",
                "is_correct": True,
                "trial_score": 1.0,
                "is_timeout": False,
                "is_invalid_output": False,
                "judge_message": "",
                "predict_time_ms": 120.0,
                "cumulative_accuracy_percent": 100.0,
                "cumulative_score": 88.8,
                "report_position": "eeg_1:200.0",
            }
        )

    _run_coroutine_sync(
        challenge.receive_algorithm_config(
            {
                "platform_team_id": "team_resume",
                "requested_channel_count": "6",
                "calibration_trials_per_class_requested": "4",
            }
        )
    )

    assert len(challenge._ChallengeMI__trial_record_list) == 1
    preserved_record = challenge._ChallengeMI__trial_record_list[0]
    assert preserved_record["task_id"] == "mi_left_vs_rest"
    assert preserved_record["trial_id"] == "1"
    assert preserved_record["score_snapshot"]["cumulative_accuracy_percent"] == pytest.approx(100.0)
    assert challenge._ChallengeMI__current_global_trial_id == 1

    db_row_list = rss.load_team_trial_record_rows(results_root / "runtime_state.db", "team_resume")
    assert len(db_row_list) == 1
    assert db_row_list[0]["task_id"] == "mi_left_vs_rest"
    assert db_row_list[0]["trial_id"] == "1"


@pytest.mark.test_id("CMI-27")
@pytest.mark.priority("P0")
@pytest.mark.requirement("finalize_score_result 必须标记 challenge 已关闭、缓存最终结果并以 root overview 模式落盘")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="finalize_score_result",
)
def test_finalize_score_result_sets_closed_state_and_persists_final_payload(
    challenge: ChallengeMI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[tuple[dict, bool]] = []

    def fake_persist(final_score_result: dict, export_root_team_overview_file: bool = True) -> None:
        persisted.append((final_score_result, export_root_team_overview_file))

    monkeypatch.setattr(challenge, "_ChallengeMI__persist_score_result_file", fake_persist)
    final_score_result = {
        "team_id": "team_final",
        "total_score": 91.2,
        "task_metric_list": [],
        "task_summary": {},
    }

    challenge.finalize_score_result(final_score_result)

    assert challenge.is_closed is True
    assert challenge._ChallengeMI__final_score_result_cache == final_score_result
    assert persisted == [(final_score_result, True)]
