from __future__ import annotations

from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
VIRTUAL_RECEIVER_CONFIG_PATH = (
    PROJECT_ROOT / "app" / "Collector" / "Collector" / "receiver" / "virtual_receiver" / "VirtualReceiverConfig.yml"
)


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("virtual_receiver")]


def _load_virtual_receiver_config() -> dict:
    return yaml.safe_load(VIRTUAL_RECEIVER_CONFIG_PATH.read_text(encoding="utf-8"))


@pytest.mark.test_id("COND-VR-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("VirtualReceiver 数据集应同时覆盖 vme/vmi 且每被试至少包含 session1/session2")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml",
    function="data_files_dataset_contract",
)
def test_virtual_receiver_dataset_covers_vme_vmi_and_two_sessions_per_subject() -> None:
    payload = _load_virtual_receiver_config()
    data_files = payload["data_files"]

    assert data_files
    for subject_id, paradigm_map in data_files.items():
        assert set(paradigm_map) == {"vme", "vmi"}
        for paradigm_key, file_list in paradigm_map.items():
            assert len(file_list) >= 4, f"{subject_id}/{paradigm_key} expected at least 4 files"
            session_name_set = {Path(path).parts[-2] for path in file_list}
            assert {"session1", "session2"} <= session_name_set


@pytest.mark.test_id("COND-VR-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("VirtualReceiver 设备信息中的 EEG/辅助/触发通道定义应完整且互不为空")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml",
    function="device_info_contract",
)
def test_virtual_receiver_device_info_lists_required_channel_groups() -> None:
    payload = _load_virtual_receiver_config()
    device_info = payload["device_info"]
    other_information = device_info["other_information"]

    channel_label_map = device_info["channel_label"]
    assert device_info["channel_number"] == len(channel_label_map)
    assert device_info["sample_rate"] == 1000
    assert len(other_information["aux_channel_alias"]) >= 4
    assert len(other_information["trigger_channel_alias"]) >= 3
    assert "TRIGGER" in other_information["trigger_channel_alias"]
    assert "EMG" in other_information["aux_channel_alias"]


@pytest.mark.test_id("COND-VR-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("VirtualReceiver 发送策略应固定 burst 模式和大包发送点数")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml",
    function="send_config_contract",
)
def test_virtual_receiver_send_config_matches_final_mode_expectations() -> None:
    payload = _load_virtual_receiver_config()
    send_config = payload["send_config"]

    assert send_config["online_replay_mode"] == "burst"
    assert int(send_config["send_package_points"]) >= 4000
