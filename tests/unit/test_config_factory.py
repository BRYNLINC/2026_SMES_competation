from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.helpers.config_factory import (
    build_team_config,
    patch_judge_web_config,
    write_central_controller_config,
    write_runtime_stage_config,
    write_virtual_receiver_config,
)


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("config_factory")]


@pytest.mark.test_id("CFG-FAC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("config_factory 必须生成唯一 team_id 和唯一算法 RPC 端口，供多队本机模拟复用")
@pytest.mark.tested(file="tests/helpers/config_factory.py", function="build_team_config")
def test_build_team_config_produces_unique_team_ids_ports_and_profiles() -> None:
    payload = build_team_config(3, 21080, profiles={1: "slow", "team_2": "malicious"})

    assert [item["team_id"] for item in payload] == ["team_0", "team_1", "team_2"]
    assert [item["algorithm_rpc_address"] for item in payload] == [
        "127.0.0.1:21080",
        "127.0.0.1:21081",
        "127.0.0.1:21082",
    ]
    assert [item["algorithm_profile"] for item in payload] == ["normal", "slow", "malicious"]


@pytest.mark.test_id("CFG-FAC-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("临时 CentralControllerConfig.yml 必须只写入 sandbox，并保持 enabled 队伍、ProcessHub 数量与算法地址一致")
@pytest.mark.tested(file="tests/helpers/config_factory.py", function="write_central_controller_config")
def test_write_central_controller_config_keeps_enabled_teams_and_rpc_addresses(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    team_config_list = build_team_config(2, 22080)
    team_config_list.append(
        {
            "team_id": "team_disabled",
            "team_display_name": "Disabled",
            "team_host": "127.0.0.1",
            "enabled": False,
            "algorithm_profile": "normal",
            "algorithm_rpc_address": "127.0.0.1:22999",
        }
    )

    config_path = write_central_controller_config(sandbox_root, team_config_list)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    components = payload["components"]

    assert config_path == (
        sandbox_root / "app" / "CentralController" / "CentralController" / "config" / "CentralControllerConfig.yml"
    )
    assert "team_0.group_1" in components
    assert "team_1.group_1" in components
    assert "team_disabled.group_1" not in components
    assert components["team_0.group_1"]["component_info"]["algorithm_connection"]["address"] == "127.0.0.1:22080"
    assert components["team_1.group_1"]["component_info"]["algorithm_connection"]["address"] == "127.0.0.1:22081"
    assert components["runtime_stage_coordinator"]["component_info"]["team_id_list_by_group"] == {
        "group_1": ["team_0", "team_1"]
    }


@pytest.mark.test_id("CFG-FAC-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("RuntimeStageCoordinator 临时配置必须允许缩短试次间隔和 watchdog timeout，加速自动化测试")
@pytest.mark.tested(file="tests/helpers/config_factory.py", function="write_runtime_stage_config")
def test_write_runtime_stage_config_contains_group_map_and_shortened_timing_fields(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    config_path = write_runtime_stage_config(
        sandbox_root,
        {"group_1": ["team_0", "team_1"]},
        timings={
            "trial_release_interval_seconds": 0.01,
            "trial_terminal_watchdog_base_timeout_seconds": 0.12,
            "trial_terminal_watchdog_grace_seconds": 0.02,
        },
    )

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    component_info = payload["runtime_stage_coordinator_component_info"]

    assert payload["application"]["application_class_name"] == "ApplicationImplement"
    assert component_info["team_id_list_by_group"] == {"group_1": ["team_0", "team_1"]}
    assert component_info["trial_release_interval_seconds"] == 0.01
    assert component_info["trial_terminal_watchdog_base_timeout_seconds"] == 0.12
    assert component_info["trial_terminal_watchdog_grace_seconds"] == 0.02


@pytest.mark.test_id("CFG-FAC-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("VirtualReceiver 临时配置必须只接受存在的数据文件路径，避免集成测试引用空数据集")
@pytest.mark.tested(file="tests/helpers/config_factory.py", function="write_virtual_receiver_config")
def test_write_virtual_receiver_config_validates_existing_dataset_paths_and_writes_yaml(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    data_file = sandbox_root / "app" / "Collector" / "Collector" / "receiver" / "virtual_receiver" / "data" / "S1" / "trial.dat"
    data_file.parent.mkdir(parents=True, exist_ok=True)
    data_file.write_bytes(b"fake-eeg")

    config_path = write_virtual_receiver_config(
        sandbox_root,
        {
            "data_files": {
                "S1": {
                    "vme": [
                        {
                            "source_path": "app/Collector/Collector/receiver/virtual_receiver/data/S1/trial.dat",
                            "yaml_path": "Collector/receiver/virtual_receiver/data/S1/trial.dat",
                        }
                    ]
                }
            }
        },
    )

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert payload["send_config"]["online_replay_mode"] == "burst"
    assert payload["data_files"] == {
        "S1": {
            "vme": ["Collector/receiver/virtual_receiver/data/S1/trial.dat"]
        }
    }


@pytest.mark.test_id("CFG-FAC-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("JudgeWeb 临时配置必须绑定测试端口且保留 loopback CORS，避免与生产端口冲突")
@pytest.mark.tested(file="tests/helpers/config_factory.py", function="patch_judge_web_config")
def test_patch_judge_web_config_rewrites_server_binding_and_cors_origins(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    config_path = patch_judge_web_config(sandbox_root, host="127.0.0.1", port=18181, local_only=False)

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    server_payload = payload["server"]

    assert server_payload["host"] == "127.0.0.1"
    assert server_payload["port"] == 18181
    assert server_payload["local_only"] is False
    assert "http://127.0.0.1:18181" in server_payload["cors_allow_origins"]
    assert "http://localhost:5173" in server_payload["cors_allow_origins"]


@pytest.mark.test_id("CFG-FAC-06")
@pytest.mark.priority("P1")
@pytest.mark.requirement("VirtualReceiver 临时配置在 dataset_spec 为空或文件缺失时必须明确报错")
@pytest.mark.tested(file="tests/helpers/config_factory.py", function="write_virtual_receiver_config")
def test_write_virtual_receiver_config_rejects_missing_dataset_files(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"

    with pytest.raises(FileNotFoundError):
        write_virtual_receiver_config(
            sandbox_root,
            {
                "data_files": {
                    "S1": {
                        "vme": ["app/Collector/Collector/receiver/virtual_receiver/data/S1/missing.dat"]
                    }
                }
            },
        )
