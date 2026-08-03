from __future__ import annotations

import pytest

from tests.helpers.http_client import build_local_url, validate_json_keys, validate_status_code


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("http_client")]


@pytest.mark.test_id("HTTP-HELP-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("http_client helper 必须稳定构造本地 URL，供 JudgeWeb 健康检查和 API 契约复用")
@pytest.mark.tested(file="tests/helpers/http_client.py", function="build_local_url")
def test_http_client_builds_local_url_with_normalized_path() -> None:
    assert build_local_url("127.0.0.1", 18080, "healthz") == "http://127.0.0.1:18080/healthz"
    assert build_local_url("localhost", 5173, "/") == "http://localhost:5173/"


@pytest.mark.test_id("HTTP-HELP-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("http_client helper 必须校验状态码和 JSON 必备字段，供 JudgeWeb API 结构断言复用")
@pytest.mark.tested(file="tests/helpers/http_client.py", function="validate_status_code/validate_json_keys")
def test_http_client_validates_status_codes_and_required_json_keys() -> None:
    validate_status_code(200, 200)
    validate_json_keys({"status": "ok", "timestamp": 1}, ("status", "timestamp"))
