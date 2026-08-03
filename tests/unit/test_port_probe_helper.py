from __future__ import annotations

import pytest

from tests.helpers.port_probe import classify_port_conflicts, is_port_set_clean, normalize_port_list


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("port_probe")]


@pytest.mark.test_id("PORT-HELP-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("port_probe 必须对端口列表去重、过滤非法值并归一化为 int")
@pytest.mark.tested(file="tests/helpers/port_probe.py", function="normalize_port_list")
def test_port_probe_normalizes_port_list_and_drops_invalid_values() -> None:
    assert normalize_port_list(["18080", 5173, "5173", "0", 65536, "9000"]) == [18080, 5173, 9000]


@pytest.mark.test_id("PORT-HELP-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("port_probe 必须能输出冲突端口到 PID 映射，并判断端口集合是否已清空")
@pytest.mark.tested(file="tests/helpers/port_probe.py", function="classify_port_conflicts/is_port_set_clean")
def test_port_probe_classifies_port_conflicts_and_clean_state() -> None:
    listening_map = {18080: [101], 5173: [201], 9000: []}

    assert classify_port_conflicts([18080, 9000], listening_map) == {18080: [101]}
    assert is_port_set_clean([9000], listening_map) is True
    assert is_port_set_clean([5173], listening_map) is False
