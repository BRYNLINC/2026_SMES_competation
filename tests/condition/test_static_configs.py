from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("config")]


def _load_yaml(relative_path: str) -> dict:
    path = PROJECT_ROOT / relative_path
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.mark.test_id("COND-CONFIG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("CentralControllerConfig.yml 必须包含唯一组件 ID、队伍 ID 和合法算法 RPC 地址")
@pytest.mark.tested(
    file="app/CentralController/CentralController/config/CentralControllerConfig.yml",
    function="static_yaml_structure",
)
def test_central_controller_config_contains_unique_enabled_team_routes() -> None:
    payload = _load_yaml("app/CentralController/CentralController/config/CentralControllerConfig.yml")
    components = payload["components"]

    component_id_list = list(components.keys())
    assert len(component_id_list) == len(set(component_id_list))

    processor_component_map = {
        component_id: component
        for component_id, component in components.items()
        if component.get("component_type") == "PROCESSOR"
    }
    team_id_list = [component["component_info"]["team_id"] for component in processor_component_map.values()]
    assert len(team_id_list) == len(set(team_id_list))
    assert team_id_list

    for component_id, component in processor_component_map.items():
        algorithm_address = component["component_info"]["algorithm_connection"]["address"]
        assert re.fullmatch(r"[^:]+:\d{2,5}", algorithm_address)
        assert component["message_key_topic_dict"]["runtime_stage_event"] == "runtime_stage.event"
        assert component["component_info"]["processor_component_id"] == component_id


@pytest.mark.test_id("COND-CONFIG-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("VirtualReceiverConfig.yml 的数据集路径必须存在且命名满足 run 规则")
@pytest.mark.tested(
    file="app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml",
    function="static_yaml_structure",
)
def test_virtual_receiver_config_data_files_exist_and_follow_expected_naming() -> None:
    payload = _load_yaml("app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml")
    data_files = payload["data_files"]
    exp_task_order = payload["device_info"]["other_information"]["exp_task_order"]
    assert exp_task_order == ["left_vs_rest", "right_vs_rest"]

    expected_name_pattern = re.compile(r".+_(vme|vmi)_run[12]\.dat$", re.IGNORECASE)
    observed_subject_count = 0
    for subject_id, paradigm_map in data_files.items():
        observed_subject_count += 1
        assert paradigm_map
        for paradigm_key, file_list in paradigm_map.items():
            assert paradigm_key in {"vme", "vmi"}
            assert file_list
            for relative_path in file_list:
                assert expected_name_pattern.search(relative_path.replace("\\", "/"))
                absolute_path = PROJECT_ROOT / "app" / "Collector" / Path(relative_path.replace("/", "\\"))
                assert absolute_path.exists(), f"missing dataset file: {absolute_path}"
    assert observed_subject_count > 0


@pytest.mark.test_id("COND-CONFIG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("ChallengeMI.yml 必须定义评分配置、timeout 策略和隐藏分数据源")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.yml",
    function="static_yaml_structure",
)
def test_challenge_mi_config_contains_score_and_timeout_contract() -> None:
    payload = _load_yaml("app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.yml")

    assert payload["sources"]["hidden_score"] == "private.hidden_score"
    assert set(payload["challenge_to_algorithm_config"]["triggers"]) == {1, 2, 3}
    assert payload["score_config"]["channel_reference_count"] == 8
    assert payload["score_config"]["calibration_reference_trials_per_class"] == 10
    assert payload["score_config"]["model_size_reference_mb"] == 150.0
    timeout_setting = payload["strategy_config"]["timeout_setting"]["timeout_trigger"]
    assert timeout_setting["source_label"] == "eeg_1"
    assert timeout_setting["timeout_limit"] == 1.0
    assert set(timeout_setting["timeout_trigger_events"]) == {"241"}


@pytest.mark.test_id("COND-CONFIG-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("JudgeWebConfig.yml 和 RuntimeStageCoordinatorLauncherConfig.yml 必须暴露本地启动所需关键字段")
@pytest.mark.tested(
    file="app/JudgeWeb/JudgeWeb/config/JudgeWebConfig.yml;app/ProcessHub/ApplicationFramework/config/RuntimeStageCoordinatorLauncherConfig.yml",
    function="static_yaml_structure",
)
def test_judge_web_and_runtime_stage_launcher_configs_expose_required_fields() -> None:
    judge_web_config = _load_yaml("app/JudgeWeb/JudgeWeb/config/JudgeWebConfig.yml")
    launcher_config = _load_yaml("app/ProcessHub/ApplicationFramework/config/RuntimeStageCoordinatorLauncherConfig.yml")

    assert judge_web_config["server"]["local_only"] is True
    assert judge_web_config["server"]["host"] == "127.0.0.1"
    assert judge_web_config["server"]["port"] == 18080
    assert "http://127.0.0.1:5173" in judge_web_config["server"]["cors_allow_origins"]
    assert judge_web_config["match"]["trial_cycle_seconds"] == 1.3
    assert launcher_config["application"]["application_class_name"] == "ApplicationImplement"
    assert launcher_config["application"]["application_class_file"] == "RuntimeStageCoordinator/application/ApplicationImplement.py"
