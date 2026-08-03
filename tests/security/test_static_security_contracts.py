from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("static_security")]


def _read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("SEC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("核心恢复、启动、JudgeWeb 与算法配置交换链路必须使用 yaml.safe_load/safe_dump，禁止使用不安全 YAML 反序列化")
@pytest.mark.tested(
    file="tools/recovery_runtime.py;tools/start_judge_stack.py;app/JudgeWeb/JudgeWeb/main.py;app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCServiceControlClient.py;app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py",
    function="load_yaml_file/sendConfig/getConfig",
)
def test_core_runtime_paths_use_safe_yaml_operations() -> None:
    expected_safe_paths = [
        "tools/recovery_runtime.py",
        "tools/start_judge_stack.py",
        "app/JudgeWeb/JudgeWeb/main.py",
        "app/ProcessHub/ProcessHub/algorithm_connector/facade/AlgorithmRPCServiceControlClient.py",
        "app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py",
    ]

    for relative_path in expected_safe_paths:
        content = _read_text(relative_path)
        assert "yaml.safe_load" in content or "yaml.safe_dump" in content
        assert "yaml.unsafe_load" not in content


@pytest.mark.test_id("SEC-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("对选手可触达的核心入口禁止出现 eval/exec/os.system/pickle.loads 这类高危执行入口")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py;tools/recovery_runtime.py;app/JudgeWeb/JudgeWeb/main.py;startup_team.bat",
    function="static_contract",
)
def test_competition_exposed_entrypoints_avoid_high_risk_dynamic_execution() -> None:
    guarded_paths = [
        "app/Algorithm/Algorithm/control/AlgorithmRPCServiceControlServer.py",
        "tools/recovery_runtime.py",
        "app/JudgeWeb/JudgeWeb/main.py",
        "startup_team.bat",
    ]

    forbidden_tokens = [
        "eval(",
        "exec(",
        "pickle.loads",
        "marshal.loads",
        "os.system(",
        "yaml.unsafe_load",
    ]

    for relative_path in guarded_paths:
        content = _read_text(relative_path)
        for token in forbidden_tokens:
            assert token not in content


@pytest.mark.test_id("SEC-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("JudgeWeb 必须默认仅允许本机访问，防止现场网络中其他机器直接操控控制面")
@pytest.mark.tested(
    file="app/JudgeWeb/JudgeWeb/main.py",
    function="DEFAULT_LOCAL_ONLY/enforce_local_only",
)
def test_judge_web_defaults_to_local_only_access_control() -> None:
    content = _read_text("app/JudgeWeb/JudgeWeb/main.py")

    assert "DEFAULT_LOCAL_ONLY = True" in content
    assert "JudgeWeb 当前仅允许本机访问" in content
    assert "is_local_only_enabled()" in content
    assert "is_loopback_host(client_host)" in content


@pytest.mark.test_id("SEC-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("选手机启动脚本只能启动固定的 Algorithm.main，不能暴露任意命令拼接入口")
@pytest.mark.tested(
    file="startup_team.bat",
    function="startup_entry_contract",
)
def test_startup_team_bat_launches_fixed_algorithm_entry_without_user_command_passthrough() -> None:
    content = _read_text("startup_team.bat")

    assert "Algorithm.main" in content
    assert '[BCI Team] Algorithm Python' in content
    assert "%*" not in content
    assert "powershell -Command" not in content
    assert "cmd /c %*" not in content


@pytest.mark.test_id("SEC-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("自动化测试与裁判机启动 BAT 不得通过 shell=True 方式执行外部命令，避免命令注入扩大")
@pytest.mark.tested(
    file="run_automated_tests.bat;startup_judge_clear.bat;startup_judge_resume.bat;startup_team.bat",
    function="script_contract",
)
def test_bat_entrypoints_do_not_expose_shell_true_style_passthrough_patterns() -> None:
    for relative_path in (
        "run_automated_tests.bat",
        "startup_judge_clear.bat",
        "startup_judge_resume.bat",
        "startup_team.bat",
    ):
        content = _read_text(relative_path)
        assert "powershell -Command %*" not in content
        assert "cmd /c %*" not in content
        assert "start %*" not in content
