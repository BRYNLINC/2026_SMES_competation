from __future__ import annotations

import pytest

from tests.helpers.http_client import validate_json_keys, validate_status_code
from tests.helpers.port_probe import classify_port_conflicts, is_port_set_clean


pytestmark = [pytest.mark.component, pytest.mark.layer("component"), pytest.mark.category("port_http_helpers")]


@pytest.mark.test_id("COMP-PORT-HTTP-01")
@pytest.mark.priority("P1")
@pytest.mark.requirement("port_probe 与 http_client helper 必须能联合表达“端口已释放且健康接口返回预期结构”的组件契约")
@pytest.mark.tested(
    file="tests/helpers/port_probe.py;tests/helpers/http_client.py",
    function="classify_port_conflicts/is_port_set_clean/validate_status_code/validate_json_keys",
)
def test_port_and_http_helper_contract_expresses_clean_port_and_valid_health_payload() -> None:
    listening_port_pid_map = {18080: [], 5173: []}
    health_payload = {"status": "ok", "timestamp": 123, "service": "JudgeWeb"}

    assert classify_port_conflicts([18080, 5173], listening_port_pid_map) == {}
    assert is_port_set_clean([18080, 5173], listening_port_pid_map) is True
    validate_status_code(200, 200)
    validate_json_keys(health_payload, ("status", "timestamp", "service"))
