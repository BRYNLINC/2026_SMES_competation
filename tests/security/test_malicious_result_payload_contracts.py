from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESS_HUB_APP_ROOT = PROJECT_ROOT / "app" / "ProcessHub"
if str(PROCESS_HUB_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(PROCESS_HUB_APP_ROOT))

from ProcessHub.bci_competition.challenge.MI.ChallengeMI import ChallengeMI


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("malicious_result_payload")]


@pytest.mark.test_id("SEC-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("恶意结果 payload 即使包含脚本标签或命令串，也只能被当作普通 predict_label 文本处理，不能触发解析执行")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__parse_result_payload/__resolve_predict_output",
)
def test_challenge_mi_treats_malicious_result_payload_as_plain_invalid_text() -> None:
    raw_result = '{"predict_label": "__import__(\\"os\\").system(\\"calc\\")", "predict_time_ms": 5}'

    payload = ChallengeMI._ChallengeMI__parse_result_payload(raw_result)
    resolution = ChallengeMI()._ChallengeMI__resolve_predict_output(
        payload.get("predict_label"),
        is_timeout=False,
    )

    assert payload["predict_label"] == '__import__("os").system("calc")'
    assert resolution["predict_label"] == '__import__("os").system("calc")'
    assert resolution["is_invalid_output"] is True
    assert "仅允许 0/1" in str(resolution["judge_message"])


@pytest.mark.test_id("SEC-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("半截 JSON、超大字符串风格的恶意输出应回落为普通字符串，不得抛异常破坏判题主流程")
@pytest.mark.tested(
    file="app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py",
    function="__parse_result_payload",
)
@pytest.mark.parametrize(
    ("raw_result", "expected_predict_label"),
    [
        ('{"predict_label": 1', '{"predict_label": 1'),
        ("A" * 4096, "A" * 4096),
    ],
)
def test_challenge_mi_parse_result_payload_falls_back_to_plain_text_for_malformed_or_oversized_text(
    raw_result: str,
    expected_predict_label: str,
) -> None:
    payload = ChallengeMI._ChallengeMI__parse_result_payload(raw_result)

    assert payload == {"predict_label": expected_predict_label}

