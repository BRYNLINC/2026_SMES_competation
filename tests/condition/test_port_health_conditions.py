from __future__ import annotations

import pytest

from tests.helpers.http_client import build_local_url
from tests.helpers.port_probe import classify_port_conflicts, normalize_port_list


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("port_health_conditions")]


@pytest.mark.test_id("COND-PORT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("裁判关键端口集合必须覆盖 JudgeWeb、Dashboard、Java bridge 和内部服务端口")
@pytest.mark.tested(file="tests/helpers/port_probe.py;tools/shutdown_judge_stack.py", function="normalize_port_list/KEY_PORT_LIST")
def test_required_judge_ports_match_shutdown_guard_coverage() -> None:
    required_port_list = normalize_port_list([18080, 5173, 7963, 8972, 9000, 9002, 9003, 8864])

    assert required_port_list == [18080, 5173, 7963, 8972, 9000, 9002, 9003, 8864]


@pytest.mark.test_id("COND-PORT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("JudgeWeb 与 Dashboard 本地健康检查 URL 必须固定到 127.0.0.1:18080/healthz 和 127.0.0.1:5173")
@pytest.mark.tested(file="tests/helpers/http_client.py;tools/start_judge_stack.py", function="build_local_url")
def test_judge_service_health_urls_match_startup_contract() -> None:
    assert build_local_url("127.0.0.1", 18080, "healthz") == "http://127.0.0.1:18080/healthz"
    assert build_local_url("127.0.0.1", 5173, "") == "http://127.0.0.1:5173/"


@pytest.mark.test_id("COND-PORT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("端口冲突条件矩阵必须能标出 JudgeWeb、Dashboard 与内部 bridge 的冲突 PID")
@pytest.mark.tested(file="tests/helpers/port_probe.py", function="classify_port_conflicts")
def test_port_conflict_condition_matrix_marks_conflicting_pids() -> None:
    conflict_map = classify_port_conflicts([18080, 5173, 9000], {18080: [101], 5173: [], 9000: [201, 202]})

    assert conflict_map == {18080: [101], 9000: [201, 202]}
