from __future__ import annotations

import zipfile
from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("startup")]


def _read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("COND-START-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("裁判机启动脚本必须设置 clear/resume 模式、Python hash seed 和 Java 变量")
@pytest.mark.tested(
    file="startup_judge_clear.bat;startup_judge_resume.bat",
    function="batch_entrypoint_contract",
)
@pytest.mark.parametrize(
    ("relative_path", "expected_mode", "failure_prefix"),
    [
        ("startup_judge_clear.bat", "clear", "[startup_judge_clear]"),
        ("startup_judge_resume.bat", "resume", "[startup_judge_resume]"),
    ],
)
def test_judge_startup_scripts_export_required_environment(
    relative_path: str,
    expected_mode: str,
    failure_prefix: str,
) -> None:
    content = _read_text(relative_path)

    assert f'set "BCI_MATCH_START_MODE={expected_mode}"' in content
    assert 'set "PYTHONHASHSEED=0"' in content
    assert 'set "BCI_JAVA_EXE=%BCI_JAVA_EXE%"' in content
    assert 'tools\\start_judge_stack.py' in content
    assert 'call :resolve_python' in content
    assert 'call :resolve_java' in content
    assert failure_prefix in content


@pytest.mark.test_id("COND-START-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("选手启动脚本必须清理 9981 端口、设置随机种子并启动 Algorithm.main")
@pytest.mark.tested(
    file="startup_team.bat",
    function="batch_entrypoint_contract",
)
def test_team_startup_script_contains_port_cleanup_and_algorithm_entry() -> None:
    content = _read_text("startup_team.bat")

    assert 'set "PYTHONHASHSEED=0"' in content
    assert ":cleanup_listening_port" in content
    assert ":ensure_port_available" in content
    assert ":ensure_firewall_rule" in content
    assert 'localport=9981' in content
    assert "Algorithm.main" in content
    assert "[BCI Team] Algorithm Python" in content


@pytest.mark.test_id("COND-START-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("judge-dashboard package.json 必须声明 dev/build/lint/preview 脚本和 React/Vite 依赖")
@pytest.mark.tested(
    file="judge-dashboard/package.json",
    function="package_manifest_contract",
)
def test_judge_dashboard_package_manifest_contains_build_entrypoints() -> None:
    package_json = yaml.safe_load(_read_text("judge-dashboard/package.json"))

    assert package_json["name"] == "judge-dashboard"
    assert package_json["private"] is True
    assert package_json["type"] == "module"
    assert set(package_json["scripts"]) >= {"dev", "build", "lint", "preview"}
    assert "vite build" in package_json["scripts"]["build"]
    assert "react" in package_json["dependencies"]
    assert "react-dom" in package_json["dependencies"]
    assert "vite" in package_json["devDependencies"]


@pytest.mark.test_id("COND-START-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("中央 Java gRPC 默认端口必须与 Python CentralController 固定连接端口 9000 一致，源码资源与可运行 jar 不得漂移")
@pytest.mark.tested(
    file="proceed/centrol/src/main/resources/application.yaml;proceed/centrol/centrol.jar",
    function="grpc_server_port_contract",
)
def test_central_java_grpc_default_port_matches_python_controller_contract() -> None:
    source_payload = yaml.safe_load(
        _read_text("proceed/centrol/src/main/resources/application.yaml")
    )
    with zipfile.ZipFile(PROJECT_ROOT / "proceed" / "centrol" / "centrol.jar") as archive:
        jar_payload = yaml.safe_load(
            archive.read("BOOT-INF/classes/application.yaml").decode("utf-8")
        )

    assert source_payload["grpc"]["server"]["port"] == "${proceedPort:9000}"
    assert jar_payload["grpc"]["server"]["port"] == "${proceedPort:9000}"
