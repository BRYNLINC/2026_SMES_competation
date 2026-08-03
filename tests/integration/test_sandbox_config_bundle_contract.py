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
from tests.helpers.project_paths import copy_project_subset_to_sandbox


pytestmark = [pytest.mark.integration, pytest.mark.layer("integration"), pytest.mark.category("sandbox_bundle")]


def _make_dataset_file(sandbox_root: Path, relative_path: str) -> Path:
    file_path = sandbox_root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"dataset")
    return file_path


@pytest.mark.test_id("INT-SBX-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("集成层必须能够在临时 sandbox 中生成一整套可解析的裁判配置，为后续 tiny match 链路测试提供稳定入口")
@pytest.mark.tested(
    file="tests/helpers/project_paths.py;tests/helpers/config_factory.py",
    function="copy_project_subset_to_sandbox/write_central_controller_config/write_runtime_stage_config/write_virtual_receiver_config/patch_judge_web_config",
)
def test_sandbox_bundle_generation_creates_parseable_config_set_for_future_tiny_match(tmp_path: Path) -> None:
    sandbox_root = copy_project_subset_to_sandbox(tmp_path)
    dataset_file = _make_dataset_file(
        sandbox_root,
        "app/Collector/Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vmi_run1.dat",
    )
    team_config_list = build_team_config(2, 27080, profiles=["normal", "slow"])

    central_path = write_central_controller_config(sandbox_root, team_config_list)
    runtime_path = write_runtime_stage_config(sandbox_root, {"group_1": ["team_0", "team_1"]})
    judge_path = patch_judge_web_config(sandbox_root, "127.0.0.1", 18183, True)
    vr_path = write_virtual_receiver_config(
        sandbox_root,
        {
            "data_files": {
                "S1": {
                    "vmi": [
                        {
                            "source_path": str(dataset_file),
                            "yaml_path": "Collector/receiver/virtual_receiver/data/S1/session1/sub_S1_vmi_run1.dat",
                        }
                    ]
                }
            }
        },
    )

    for path in (central_path, runtime_path, judge_path, vr_path):
        assert path.exists()
        assert sandbox_root in path.parents
        assert yaml.safe_load(path.read_text(encoding="utf-8")) is not None


@pytest.mark.test_id("INT-SBX-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("sandbox 配置包中的多队 roster、短 watchdog 配置和算法 profile 映射必须一致，避免后续集成构建上下文漂移")
@pytest.mark.tested(
    file="tests/helpers/config_factory.py",
    function="build_team_config/write_central_controller_config/write_runtime_stage_config",
)
def test_sandbox_bundle_keeps_team_profiles_and_runtime_stage_timings_aligned(tmp_path: Path) -> None:
    sandbox_root = copy_project_subset_to_sandbox(tmp_path)
    team_config_list = build_team_config(2, 28080, profiles={0: "normal", 1: "late_result"})

    central_path = write_central_controller_config(sandbox_root, team_config_list)
    runtime_path = write_runtime_stage_config(
        sandbox_root,
        {"group_1": ["team_0", "team_1"]},
        timings={
            "trial_release_interval_seconds": 0.02,
            "trial_terminal_watchdog_base_timeout_seconds": 0.15,
            "trial_terminal_watchdog_grace_seconds": 0.03,
        },
    )

    central_payload = yaml.safe_load(central_path.read_text(encoding="utf-8"))
    runtime_payload = yaml.safe_load(runtime_path.read_text(encoding="utf-8"))

    assert central_payload["components"]["team_0.group_1"]["component_info"]["algorithm_profile"] == "normal"
    assert central_payload["components"]["team_1.group_1"]["component_info"]["algorithm_profile"] == "late_result"
    assert runtime_payload["runtime_stage_coordinator_component_info"]["team_id_list_by_group"] == {
        "group_1": ["team_0", "team_1"]
    }
    assert runtime_payload["runtime_stage_coordinator_component_info"]["trial_release_interval_seconds"] == 0.02
    assert runtime_payload["runtime_stage_coordinator_component_info"]["trial_terminal_watchdog_base_timeout_seconds"] == 0.15
