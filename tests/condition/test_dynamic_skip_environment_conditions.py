from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.grpc_algorithm_client import probe_grpc_availability


PROJECT_ROOT = Path(__file__).resolve().parents[2]


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("dynamic_skip_environment")]


@pytest.mark.test_id("COND-SKIP-01")
@pytest.mark.priority("P1")
@pytest.mark.requirement("当前环境的动态 skip 原因必须被文档化，明确指向 asyncio/grpc/uvicorn 导入链系统异常")
@pytest.mark.tested(file="tests/初赛README.md", function="environment_skip_documentation")
def test_readme_documents_dynamic_import_environment_issue() -> None:
    content = (PROJECT_ROOT / "tests" / "初赛README.md").read_text(encoding="utf-8")

    assert "WinError 10106" in content
    assert "asyncio" in content
    assert "uvicorn" in content
    assert "skip" in content


@pytest.mark.test_id("COND-SKIP-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("gRPC 可用性探针必须能提供结构化原因，作为算法接口层 skip 的统一判定来源")
@pytest.mark.tested(file="tests/helpers/grpc_algorithm_client.py", function="probe_grpc_availability")
def test_grpc_probe_returns_structured_reason_when_unavailable() -> None:
    availability = probe_grpc_availability()

    assert isinstance(availability.available, bool)
    if not availability.available:
        assert availability.reason


@pytest.mark.test_id("COND-SKIP-03")
@pytest.mark.priority("P2")
@pytest.mark.requirement("算法接口目录中至少要有导入门控或 skip 说明，避免动态链路异常时直接中断全量测试")
@pytest.mark.tested(file="tests/algorithm_interface", function="import_gate_contract")
def test_algorithm_interface_tests_keep_environment_import_gate_contract() -> None:
    file_list = [
        PROJECT_ROOT / "tests" / "algorithm_interface" / "test_algorithm_rpc_control_contract.py",
        PROJECT_ROOT / "tests" / "algorithm_interface" / "test_algorithm_rpc_service_control_client_contract.py",
    ]
    for file_path in file_list:
        content = file_path.read_text(encoding="utf-8")
        assert "import unavailable" in content or "pytest.mark.skip" in content
