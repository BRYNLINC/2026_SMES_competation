from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
for app_root in (
    PROJECT_ROOT / "app" / "Collector",
    PROJECT_ROOT / "app" / "CentralController",
):
    if str(app_root) not in sys.path:
        sys.path.insert(0, str(app_root))

from Collector.receiver.virtual_receiver.VirtualReceiverConfigCreator import VirtualReceiverConfigCreator
from CentralController.config.CentralControllerConfigCreator import CentralControllerConfigCreator


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("config")]


@pytest.mark.test_id("CFG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("VirtualReceiver 数据扫描需按被试与范式分组并过滤无效文件")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfigCreator.py",
    function="create_data_files",
)
def test_virtual_receiver_create_data_files_groups_subject_and_paradigm(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "S1").mkdir(parents=True)
    (data_root / "S2").mkdir(parents=True)

    (data_root / "S1" / "subjectA_vme_run1.dat").write_bytes(b"")
    (data_root / "S1" / "subjectA_vmi_run2.dat").write_bytes(b"")
    (data_root / "S1" / "ignore_me.txt").write_text("", encoding="utf-8")
    (data_root / "S1" / "bad_name.dat").write_bytes(b"")
    (data_root / "S2" / "subjectB_VME_run2.dat").write_bytes(b"")

    creator = VirtualReceiverConfigCreator()
    payload = creator.create_data_files(str(data_root), "Collector/receiver/virtual_receiver/data", ["dat"])

    assert payload == {
        "data_files": {
            "S1": {
                "vme": ["Collector/receiver/virtual_receiver/data/S1/subjectA_vme_run1.dat"],
                "vmi": ["Collector/receiver/virtual_receiver/data/S1/subjectA_vmi_run2.dat"],
            },
            "S2": {
                "vme": ["Collector/receiver/virtual_receiver/data/S2/subjectB_VME_run2.dat"],
            },
        }
    }


@pytest.mark.test_id("CFG-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("VirtualReceiver device_info 应包含通道数和任务顺序")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfigCreator.py",
    function="create_device_info",
)
def test_virtual_receiver_create_device_info_contains_channel_metadata() -> None:
    payload = VirtualReceiverConfigCreator.create_device_info()
    device_info = payload["device_info"]

    assert device_info["channel_number"] == len(VirtualReceiverConfigCreator.EEG_CHANNEL_LABEL_LIST)
    assert list(device_info["channel_label"].keys())[:3] == ["FP1", "FPZ", "FP2"]
    assert device_info["other_information"]["exp_task_order"] == ["left_vs_rest", "right_vs_rest"]
    assert "TRIG" in device_info["other_information"]["trigger_channel_alias"]


@pytest.mark.test_id("CFG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("VirtualReceiver run 应输出完整 YAML 配置文件")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfigCreator.py",
    function="run",
)
def test_virtual_receiver_run_writes_config_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        VirtualReceiverConfigCreator,
        "create_data_files",
        lambda self, target_dir, prefix_dir, extensions: {"data_files": {"S1": {"vme": ["mock.dat"]}}},
    )

    VirtualReceiverConfigCreator().run()

    payload = yaml.safe_load((tmp_path / "VirtualReceiverConfig.yml").read_text(encoding="utf-8"))
    assert payload["send_config"]["online_replay_mode"] == "burst"
    assert payload["message"] == {"virtual_receiver_custom_control": None}
    assert payload["data_files"] == {"S1": {"vme": ["mock.dat"]}}


@pytest.mark.test_id("CFG-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("CentralController PROCESSOR 配置应包含队伍路由和算法地址")
@pytest.mark.tested(
    file="app/CentralController/CentralController/config/CentralControllerConfigCreator.py",
    function="create_processor_model",
)
def test_central_controller_create_processor_model_contains_algorithm_routing() -> None:
    component = CentralControllerConfigCreator.create_processor_model(
        "group_1",
        {
            "team_id": "team_7",
            "team_display_name": "测试队伍",
            "team_host": "10.0.0.7",
        },
    )

    assert component.component_id == "team_7.group_1"
    assert component.component_info["algorithm_connection"]["address"] == "10.0.0.7:9981"
    assert component.component_info["collector_component_id"] == "collector_group_1"
    assert component.message_key_topic_dict["eeg_1_calibration_private"] == "team_7.group_1.calibration"


@pytest.mark.test_id("CFG-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("RuntimeStageCoordinator 配置应包含按组的队伍等待名单")
@pytest.mark.tested(
    file="app/CentralController/CentralController/config/CentralControllerConfigCreator.py",
    function="create_runtime_stage_coordinator_model",
)
def test_central_controller_runtime_stage_coordinator_contains_team_map() -> None:
    component = CentralControllerConfigCreator.create_runtime_stage_coordinator_model(
        ["group_1", "group_2"],
        ["team_0", "team_1"],
    )

    assert component.component_id == "runtime_stage_coordinator"
    assert component.component_group_id == "group_base"
    assert component.component_info["team_id_list_by_group"] == {
        "group_1": ["team_0", "team_1"],
        "group_2": ["team_0", "team_1"],
    }
    assert component.message_key_topic_dict["runtime_stage_status"] == "runtime_stage.status"


@pytest.mark.test_id("CFG-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("CentralController run 输出应剔除禁用队伍并保留核心组件")
@pytest.mark.tested(
    file="app/CentralController/CentralController/config/CentralControllerConfigCreator.py",
    function="run",
)
def test_central_controller_run_writes_expected_components(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "app" / "CentralController" / "CentralController" / "config"
    config_dir.mkdir(parents=True)
    other_workdir = tmp_path / "some_other_workdir"
    other_workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "CentralController.config.CentralControllerConfigCreator.__file__",
        str(config_dir / "CentralControllerConfigCreator.py"),
    )
    monkeypatch.chdir(other_workdir)
    creator = CentralControllerConfigCreator()
    creator.team_config_list = [
        {
            "team_id": "team_enabled",
            "team_display_name": "Enabled Team",
            "team_host": "127.0.0.1",
            "enabled": True,
        },
        {
            "team_id": "team_disabled",
            "team_display_name": "Disabled Team",
            "team_host": "127.0.0.2",
            "enabled": False,
        },
    ]
    creator.team_id_list = ["team_enabled"]
    creator.group_id_list = ["group_1"]
    creator.components = []

    creator.run()

    config_path = config_dir / "CentralControllerConfig.yml"
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    components = payload["components"]

    assert not (other_workdir / "CentralControllerConfig.yml").exists()

    assert "team_enabled.group_1" in components
    assert "team_disabled.group_1" not in components
    assert components["collector_group_1"]["component_info"]["team_id_list"] == ["team_enabled"]
    assert components["runtime_stage_coordinator"]["component_info"]["team_id_list_by_group"] == {
        "group_1": ["team_enabled"]
    }
    assert "central_controller" in components
