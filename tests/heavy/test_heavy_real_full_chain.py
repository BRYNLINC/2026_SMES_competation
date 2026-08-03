from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.helpers.heavy_runtime import (
    HEAVY_DEFAULT_TEAM_COUNT,
    HEAVY_MATCH_TIMEOUT_SECONDS,
    assert_heavy_completion_outputs,
    build_heavy_team_scenarios,
    dump_heavy_environment_snapshot,
    prepare_heavy_workspace,
    read_launcher_manifest,
    read_process_manifest,
    shutdown_heavy_environment,
    shutdown_and_preserve_heavy_failure_artifacts,
    start_headless_judge_stack,
    start_heavy_algorithms,
    validate_heavy_python_runtime,
    wait_for_heavy_completion,
)
from tests.helpers.project_paths import latest_artifacts_root


pytestmark = [
    pytest.mark.heavy,
    pytest.mark.slow,
    pytest.mark.e2e,
    pytest.mark.layer("heavy"),
    pytest.mark.category("heavy_real_full_chain"),
]


@pytest.mark.test_id("HEAVY-REAL-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 必须是真实全链路重型测试：同机启动 17 队真实算法进程与真实裁判栈，等待真实完赛并校验结果整合")
@pytest.mark.tested(
    file="tests/helpers/heavy_runtime.py;tools/start_judge_stack.py;app/Algorithm/Algorithm/main.py",
    function="prepare_heavy_workspace/start_heavy_algorithms/start_headless_judge_stack",
)
def test_heavy_real_full_chain_starts_seventeen_real_algorithms_and_headless_judge_stack(
    python_executable: str,
) -> None:
    artifact_root = latest_artifacts_root() / "heavy" / "real_full_chain"
    if artifact_root.exists():
        import shutil

        shutil.rmtree(artifact_root, ignore_errors=True)
    environment = prepare_heavy_workspace(artifact_root, team_count=HEAVY_DEFAULT_TEAM_COUNT)
    failure_cleanup_attempted = False
    try:
        start_heavy_algorithms(environment, python_executable)
        start_headless_judge_stack(environment, python_executable)
        wait_for_heavy_completion(environment, timeout_seconds=HEAVY_MATCH_TIMEOUT_SECONDS)

        launcher_manifest = read_launcher_manifest(environment)
        process_manifest = read_process_manifest(environment)
        assert_heavy_completion_outputs(environment, expected_team_count=HEAVY_DEFAULT_TEAM_COUNT)

        assert launcher_manifest["match_start_mode"] == "clear"
        assert len(launcher_manifest["processor_component_id_list"]) == HEAVY_DEFAULT_TEAM_COUNT
        assert process_manifest["metadata"]["match_start_mode"] == "clear"
        assert len(process_manifest["processes"]) >= HEAVY_DEFAULT_TEAM_COUNT + 7
    except Exception:
        failure_cleanup_attempted = True
        shutdown_and_preserve_heavy_failure_artifacts(
            environment,
            test_label="HEAVY-REAL-01",
        )
        raise
    finally:
        if not failure_cleanup_attempted:
            shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-REAL-00")
@pytest.mark.priority("P0")
@pytest.mark.requirement("真实 heavy 运行前必须验证 Python 运行时可用；asyncio/socket/grpc 不可用时不得伪造通过")
@pytest.mark.tested(file="tests/helpers/heavy_runtime.py", function="validate_heavy_python_runtime")
def test_heavy_real_python_runtime_preflight(python_executable: str) -> None:
    validate_heavy_python_runtime(python_executable)


@pytest.mark.test_id("HEAVY-REAL-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 必须以真实 17 队配置覆盖完整裁判链路：CentralController 与 RuntimeStageCoordinator 按 17 队生成，VirtualReceiver 复制正式全量数据集而不伪造扩容 subject")
@pytest.mark.tested(
    file="tests/helpers/heavy_runtime.py;tests/helpers/config_factory.py",
    function="prepare_heavy_workspace/dump_heavy_environment_snapshot",
)
def test_heavy_real_full_chain_generates_seventeen_team_runtime_configs(tmp_path: Path) -> None:
    environment = prepare_heavy_workspace(tmp_path / "artifacts", team_count=HEAVY_DEFAULT_TEAM_COUNT)
    try:
        snapshot = dump_heavy_environment_snapshot(environment)
        scenario_by_team_id = build_heavy_team_scenarios(HEAVY_DEFAULT_TEAM_COUNT)

        processor_component_id_list = sorted(
            [
                component_id
                for component_id, component in snapshot["central_controller_config"]["components"].items()
                if component["component_type"] == "PROCESSOR"
            ],
            key=lambda component_id: int(component_id.split(".")[0].split("_")[-1]),
        )
        assert processor_component_id_list == [f"team_{index}.group_1" for index in range(HEAVY_DEFAULT_TEAM_COUNT)]
        assert (
            snapshot["runtime_stage_config"]["runtime_stage_coordinator_component_info"]["team_id_list_by_group"]["group_1"]
            == [f"team_{index}" for index in range(HEAVY_DEFAULT_TEAM_COUNT)]
        )
        source_virtual_receiver_config = Path(
            "app/Collector/Collector/receiver/virtual_receiver/VirtualReceiverConfig.yml"
        )
        source_virtual_receiver_payload = yaml.safe_load(
            source_virtual_receiver_config.read_text(encoding="utf-8")
        ) or {}
        assert sorted(snapshot["virtual_receiver_config"]["data_files"].keys()) == sorted(
            (source_virtual_receiver_payload.get("data_files") or {}).keys()
        )
        assert len(snapshot["virtual_receiver_config"]["data_files"]) < HEAVY_DEFAULT_TEAM_COUNT
        assert snapshot["central_controller_config"]["components"]["team_1.group_1"]["component_info"]["algorithm_profile"] == "slow"
        assert snapshot["central_controller_config"]["components"]["team_5.group_1"]["component_info"]["algorithm_profile"] == "invalid_output"
        assert snapshot["central_controller_config"]["components"]["team_6.group_1"]["component_info"]["algorithm_profile"] == "resource_hog"
        assert snapshot["central_controller_config"]["components"]["team_7.group_1"]["component_info"]["algorithm_profile"] == "malicious"
        assert snapshot["central_controller_config"]["components"]["team_9.group_1"]["component_info"]["algorithm_profile"] == "slow"
        assert snapshot["central_controller_config"]["components"]["team_10.group_1"]["component_info"]["algorithm_profile"] == "late_result"
        assert snapshot["central_controller_config"]["components"]["team_14.group_1"]["component_info"]["algorithm_profile"] == "resource_hog"
        assert snapshot["central_controller_config"]["components"]["team_15.group_1"]["component_info"]["algorithm_profile"] == "malicious"
        assert environment.scenario_by_team_id == scenario_by_team_id
        assert scenario_by_team_id["team_1"].expected_observation == "timeout_trial"
        assert scenario_by_team_id["team_2"].expected_observation == "late_timeout_trial"
        assert scenario_by_team_id["team_3"].expected_observation == "transient_stream_timeout"
        assert scenario_by_team_id["team_4"].expected_observation == "deduplicated_trial"
        assert scenario_by_team_id["team_5"].expected_observation == "invalid_output_trial"
        assert scenario_by_team_id["team_6"].expected_observation == "resource_timeout_trial"
        assert scenario_by_team_id["team_7"].expected_observation == "malicious_blocked"
    finally:
        shutdown_heavy_environment(environment)


@pytest.mark.test_id("HEAVY-REAL-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("heavy 层文档和执行入口必须明确说明它是真实全链路重型测试，且默认双击 bat 会在最后执行")
@pytest.mark.tested(file="run_automated_tests.bat;tests/初赛README.md", function="heavy_profile_contract")
def test_heavy_real_profile_documentation_contract() -> None:
    bat_text = Path("run_automated_tests.bat").read_text(encoding="utf-8")
    readme_text = Path("tests/初赛README.md").read_text(encoding="utf-8")

    assert "heavy tests run last" in bat_text
    assert "17-team full-chain" in bat_text or "17 队" in bat_text
    assert "真实全链路" in readme_text or "real full chain" in readme_text.lower()
