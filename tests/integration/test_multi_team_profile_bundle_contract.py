from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.helpers.config_factory import (
    build_team_config,
    write_central_controller_config,
    write_runtime_stage_config,
    write_virtual_receiver_config,
)
from tests.helpers.project_paths import copy_project_subset_to_sandbox


pytestmark = [pytest.mark.integration, pytest.mark.layer("integration"), pytest.mark.category("multi_team_profile_bundle")]


def _make_dataset_file(sandbox_root: Path, relative_path: str) -> Path:
    file_path = sandbox_root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"dataset")
    return file_path


@pytest.mark.test_id("INT-MULTI-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("多队混合 profile 集成包必须把 normal/slow/late_result/malicious 的 profile 与端口稳定映射到各 ProcessHub")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="build_team_config/write_central_controller_config",
)
def test_multi_team_profile_bundle_keeps_profile_to_team_and_port_mapping_stable(tmp_path: Path) -> None:
    sandbox_root = copy_project_subset_to_sandbox(tmp_path)
    team_config_list = build_team_config(
        4,
        31080,
        profiles=["normal", "slow", "late_result", "malicious"],
    )

    central_path = write_central_controller_config(sandbox_root, team_config_list)
    payload = yaml.safe_load(central_path.read_text(encoding="utf-8"))
    components = payload["components"]

    assert components["team_0.group_1"]["component_info"]["algorithm_profile"] == "normal"
    assert components["team_1.group_1"]["component_info"]["algorithm_profile"] == "slow"
    assert components["team_2.group_1"]["component_info"]["algorithm_profile"] == "late_result"
    assert components["team_3.group_1"]["component_info"]["algorithm_profile"] == "malicious"
    assert components["team_3.group_1"]["component_info"]["algorithm_connection"]["address"] == "127.0.0.1:31083"


@pytest.mark.test_id("INT-MULTI-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("多队集成 sandbox 必须允许禁用部分队伍后仅保留 enabled roster，并保持数据集配置可解析")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="write_central_controller_config/write_runtime_stage_config/write_virtual_receiver_config",
)
def test_multi_team_profile_bundle_supports_partial_disable_without_breaking_runtime_roster(tmp_path: Path) -> None:
    sandbox_root = copy_project_subset_to_sandbox(tmp_path)
    dataset_file = _make_dataset_file(
        sandbox_root,
        "app/Collector/Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat",
    )
    team_config_list = build_team_config(4, 32080, profiles=["normal", "slow", "late_result", "malicious"])
    team_config_list[1]["enabled"] = False
    team_config_list[3]["enabled"] = False
    enabled_team_id_list = [team["team_id"] for team in team_config_list if team.get("enabled", True)]

    central_path = write_central_controller_config(sandbox_root, team_config_list)
    runtime_path = write_runtime_stage_config(sandbox_root, {"group_1": enabled_team_id_list})
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
    vr_payload = yaml.safe_load(vr_path.read_text(encoding="utf-8"))

    assert "team_1.group_1" not in central_payload["components"]
    assert "team_3.group_1" not in central_payload["components"]
    assert runtime_payload["runtime_stage_coordinator_component_info"]["team_id_list_by_group"] == {
        "group_1": ["team_0", "team_2"]
    }
    assert vr_payload["data_files"]["S1"]["vme"] == [
        "Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat"
    ]
