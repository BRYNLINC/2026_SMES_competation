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


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("config_factory_contract")]


def _write_dataset_file(sandbox_root: Path, relative_path: str) -> Path:
    file_path = sandbox_root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"mock-data")
    return file_path


@pytest.mark.test_id("COMP-CFG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("config_factory 生成的中心控制、协调器、JudgeWeb、VirtualReceiver 临时配置必须在 sandbox 内彼此一致")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="build_team_config/write_central_controller_config/write_runtime_stage_config/write_virtual_receiver_config/patch_judge_web_config",
)
def test_config_factory_generated_bundle_keeps_team_roster_and_ports_consistent(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    team_config_list = build_team_config(3, 25080, profiles=["normal", "slow", "duplicate_result"])
    team_config_list[2]["enabled"] = False
    enabled_team_id_list = [team["team_id"] for team in team_config_list if team.get("enabled", True)]

    dataset_file = _write_dataset_file(
        sandbox_root,
        "app/Collector/Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat",
    )
    central_path = write_central_controller_config(sandbox_root, team_config_list)
    runtime_path = write_runtime_stage_config(sandbox_root, {"group_1": enabled_team_id_list})
    judge_path = patch_judge_web_config(sandbox_root, host="127.0.0.1", port=18182, local_only=True)
    vr_path = write_virtual_receiver_config(
        sandbox_root,
        {
            "data_files": {
                "S1": {
                    "vme": [
                        {
                            "source_path": str(dataset_file),
                            "yaml_path": "Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat",
                        }
                    ]
                }
            }
        },
    )

    central_payload = yaml.safe_load(central_path.read_text(encoding="utf-8"))
    runtime_payload = yaml.safe_load(runtime_path.read_text(encoding="utf-8"))
    judge_payload = yaml.safe_load(judge_path.read_text(encoding="utf-8"))
    vr_payload = yaml.safe_load(vr_path.read_text(encoding="utf-8"))

    processor_component_id_list = sorted(
        component_id
        for component_id, component in central_payload["components"].items()
        if component["component_type"] == "PROCESSOR"
    )

    assert processor_component_id_list == ["team_0.group_1", "team_1.group_1"]
    assert central_payload["components"]["collector_group_1"]["component_info"]["team_id_list"] == enabled_team_id_list
    assert central_payload["components"]["runtime_stage_coordinator"]["component_info"]["team_id_list_by_group"] == {
        "group_1": enabled_team_id_list
    }
    assert runtime_payload["runtime_stage_coordinator_component_info"]["team_id_list_by_group"] == {
        "group_1": enabled_team_id_list
    }
    assert judge_payload["server"]["port"] == 18182
    assert judge_payload["server"]["local_only"] is True
    assert vr_payload["data_files"] == {
        "S1": {"vme": ["Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat"]}
    }


@pytest.mark.test_id("COMP-CFG-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("CentralController 临时配置中的 ProcessHub 组件必须保留每队 profile 与独立算法端口，供后续多队联调复用")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="build_team_config/write_central_controller_config",
)
def test_config_factory_preserves_algorithm_profiles_and_unique_rpc_addresses_in_processor_components(
    tmp_path: Path,
) -> None:
    sandbox_root = tmp_path / "sandbox"
    team_config_list = build_team_config(2, 26080, profiles={0: "normal", 1: "malicious"})

    central_path = write_central_controller_config(sandbox_root, team_config_list)
    central_payload = yaml.safe_load(central_path.read_text(encoding="utf-8"))
    components = central_payload["components"]

    assert components["team_0.group_1"]["component_info"]["algorithm_profile"] == "normal"
    assert components["team_1.group_1"]["component_info"]["algorithm_profile"] == "malicious"
    assert components["team_0.group_1"]["component_info"]["algorithm_connection"]["address"] == "127.0.0.1:26080"
    assert components["team_1.group_1"]["component_info"]["algorithm_connection"]["address"] == "127.0.0.1:26081"
