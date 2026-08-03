from __future__ import annotations

from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("config_edge")]


def _load_yaml(relative_path: str) -> dict:
    return yaml.safe_load((PROJECT_ROOT / relative_path).read_text(encoding="utf-8"))


@pytest.mark.test_id("COND-EDGE-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("CentralControllerConfig.yml 至少应存在一个 PROCESSOR 和一个 COLLECTOR")
@pytest.mark.tested(
    file="app/CentralController/CentralController/config/CentralControllerConfig.yml",
    function="static_topology_contract",
)
def test_central_controller_config_contains_minimum_runtime_topology() -> None:
    payload = _load_yaml("app/CentralController/CentralController/config/CentralControllerConfig.yml")
    components = payload["components"]

    processor_ids = [component_id for component_id, component in components.items() if component["component_type"] == "PROCESSOR"]
    collector_ids = [component_id for component_id, component in components.items() if component["component_type"] == "COLLECTOR"]
    assert processor_ids
    assert collector_ids
    assert "runtime_stage_coordinator" in components
    assert "central_controller" in components


@pytest.mark.test_id("COND-EDGE-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("RuntimeStageCoordinator 配置中的 team_id_list_by_group 不得为空")
@pytest.mark.tested(
    file="app/CentralController/CentralController/config/CentralControllerConfig.yml",
    function="runtime_stage_topology_contract",
)
def test_runtime_stage_coordinator_group_waiting_roster_is_not_empty() -> None:
    payload = _load_yaml("app/CentralController/CentralController/config/CentralControllerConfig.yml")
    coordinator = payload["components"]["runtime_stage_coordinator"]
    team_id_list_by_group = coordinator["component_info"]["team_id_list_by_group"]

    assert team_id_list_by_group
    for group_id, team_id_list in team_id_list_by_group.items():
        assert group_id
        assert team_id_list


@pytest.mark.test_id("COND-EDGE-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("启动脚本和关键配置文件必须全部存在于仓库根目录约定位置")
@pytest.mark.tested(
    file="startup_judge_clear.bat;startup_judge_resume.bat;startup_team.bat;judge-dashboard/package.json;app/JudgeWeb/JudgeWeb/config/JudgeWebConfig.yml",
    function="filesystem_presence_contract",
)
def test_required_entrypoint_files_exist() -> None:
    required_relative_path_list = [
        "startup_judge_clear.bat",
        "startup_judge_resume.bat",
        "startup_team.bat",
        "judge-dashboard/package.json",
        "app/JudgeWeb/JudgeWeb/config/JudgeWebConfig.yml",
        "app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml",
        "app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.yml",
        "app/ProcessHub/ApplicationFramework/config/RuntimeStageCoordinatorLauncherConfig.yml",
    ]
    missing_path_list = [
        str(PROJECT_ROOT / relative_path)
        for relative_path in required_relative_path_list
        if not (PROJECT_ROOT / relative_path).exists()
    ]
    assert not missing_path_list, f"missing required files: {missing_path_list}"


@pytest.mark.test_id("COND-EDGE-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("JudgeWeb CORS 与 Dashboard dev/preview 端口应保持一致")
@pytest.mark.tested(
    file="app/JudgeWeb/JudgeWeb/config/JudgeWebConfig.yml;judge-dashboard/package.json",
    function="dashboard_host_port_contract",
)
def test_judge_web_cors_allows_dashboard_dev_and_preview_ports() -> None:
    judge_web_config = _load_yaml("app/JudgeWeb/JudgeWeb/config/JudgeWebConfig.yml")
    cors_origin_list = judge_web_config["server"]["cors_allow_origins"]

    assert "http://127.0.0.1:5173" in cors_origin_list
    assert "http://localhost:5173" in cors_origin_list
    assert "http://127.0.0.1:4173" in cors_origin_list
    assert "http://localhost:4173" in cors_origin_list
