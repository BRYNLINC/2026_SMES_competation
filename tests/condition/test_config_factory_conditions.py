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


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("config_factory_conditions")]


def _make_dataset_file(sandbox_root: Path, relative_path: str) -> Path:
    file_path = sandbox_root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"dataset")
    return file_path


@pytest.mark.test_id("COND-CFG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("队伍数量为 0 时，config_factory 生成的临时 CentralControllerConfig 不得包含任何 PROCESSOR 组件")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="build_team_config/write_central_controller_config",
)
def test_build_team_config_supports_zero_team_and_writes_processor_free_central_config(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    team_config_list = build_team_config(0, 29080)

    central_path = write_central_controller_config(sandbox_root, team_config_list)
    payload = yaml.safe_load(central_path.read_text(encoding="utf-8"))
    processor_component_id_list = [
        component_id
        for component_id, component in payload["components"].items()
        if component["component_type"] == "PROCESSOR"
    ]

    assert team_config_list == []
    assert processor_component_id_list == []
    assert payload["components"]["runtime_stage_coordinator"]["component_info"]["team_id_list_by_group"] == {
        "group_1": []
    }


@pytest.mark.test_id("COND-CFG-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("build_team_config 必须支持 8 队和按 team_id 指定 profile，且算法端口保持唯一")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="build_team_config",
)
def test_build_team_config_supports_eight_teams_with_unique_ports_and_team_named_profiles() -> None:
    team_config_list = build_team_config(
        8,
        30080,
        profiles={"team_0": "normal", "team_3": "slow", "team_7": "malicious"},
    )

    assert len(team_config_list) == 8
    assert len({team["algorithm_rpc_address"] for team in team_config_list}) == 8
    assert team_config_list[0]["algorithm_profile"] == "normal"
    assert team_config_list[3]["algorithm_profile"] == "slow"
    assert team_config_list[7]["algorithm_profile"] == "malicious"


@pytest.mark.test_id("COND-CFG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("RuntimeStageCoordinator 临时配置必须允许空 group roster 和自定义 timeout 映射，以覆盖边界配置矩阵")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="write_runtime_stage_config",
)
def test_write_runtime_stage_config_preserves_empty_group_roster_and_custom_timeout_mapping(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    config_path = write_runtime_stage_config(
        sandbox_root,
        {"group_1": [], "group_2": ["team_0"]},
        timings={
            "trial_terminal_watchdog_base_timeout_seconds_by_task_id": {"vme_left_vs_rest": 0.12},
        },
    )

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    component_info = payload["runtime_stage_coordinator_component_info"]

    assert component_info["team_id_list_by_group"] == {"group_1": [], "group_2": ["team_0"]}
    assert component_info["trial_terminal_watchdog_base_timeout_seconds_by_task_id"] == {
        "vme_left_vs_rest": 0.12
    }


@pytest.mark.test_id("COND-CFG-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("VirtualReceiver 临时配置在多被试多 session 数据集下必须保留每个 subject/paradigm 的独立路径列表")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="write_virtual_receiver_config",
)
def test_write_virtual_receiver_config_supports_multi_subject_multi_session_dataset_matrix(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"
    s1_vme = _make_dataset_file(
        sandbox_root,
        "app/Collector/Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat",
    )
    s1_vmi = _make_dataset_file(
        sandbox_root,
        "app/Collector/Collector/receiver/virtual_receiver/data/S1/session2/sub_S1_vmi_run1.dat",
    )
    s2_vme = _make_dataset_file(
        sandbox_root,
        "app/Collector/Collector/receiver/virtual_receiver/data/S2/session1/sub_S2_vme_run1.dat",
    )

    config_path = write_virtual_receiver_config(
        sandbox_root,
        {
            "data_files": {
                "S1": {
                    "vme": [{"source_path": str(s1_vme), "yaml_path": "Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat"}],
                    "vmi": [{"source_path": str(s1_vmi), "yaml_path": "Collector/receiver/virtual_receiver/data/S1/session2/sub_S1_vmi_run1.dat"}],
                },
                "S2": {
                    "vme": [{"source_path": str(s2_vme), "yaml_path": "Collector/receiver/virtual_receiver/data/S2/session1/sub_S2_vme_run1.dat"}],
                },
            }
        },
    )

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert payload["data_files"] == {
        "S1": {
            "vme": ["Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vme_run1.dat"],
            "vmi": ["Collector/receiver/virtual_receiver/data/S1/session2/sub_S1_vmi_run1.dat"],
        },
        "S2": {
            "vme": ["Collector/receiver/virtual_receiver/data/S2/session1/sub_S2_vme_run1.dat"],
        },
    }


@pytest.mark.test_id("COND-CFG-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("VirtualReceiver 临时配置在 data_files 为空时必须 fail closed，避免比赛空跑")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="write_virtual_receiver_config",
)
def test_write_virtual_receiver_config_rejects_empty_data_files_payload(tmp_path: Path) -> None:
    sandbox_root = tmp_path / "sandbox"

    with pytest.raises(ValueError, match="dataset_spec.data_files must include at least one existing file"):
        write_virtual_receiver_config(sandbox_root, {"data_files": {}})
