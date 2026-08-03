from __future__ import annotations

import random

import pytest

from tests.helpers.network_proxy import (
    NetworkFaultProfile,
    is_half_open_blocked,
    resolve_delay_ms,
    should_disconnect,
    should_drop_packet,
)


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("network_fault_conditions")]


@pytest.mark.test_id("COND-NET-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("网络延迟条件矩阵必须覆盖 0/100/999/1000/1001ms，供预测窗口边界测试复用")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="resolve_delay_ms")
@pytest.mark.parametrize("delay_ms", [0, 100, 999, 1000, 1001])
def test_network_fixed_delay_condition_matrix_covers_prediction_window_boundaries(delay_ms: int) -> None:
    profile = NetworkFaultProfile(mode="fixed_delay", fixed_delay_ms=delay_ms)

    assert resolve_delay_ms(profile, packet_index=1) == delay_ms


@pytest.mark.test_id("COND-NET-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("抖动范围即使 min/max 反向配置也必须归一化，避免现场手工配置顺序错误导致测试不确定")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="resolve_delay_ms")
def test_network_jitter_condition_normalizes_reversed_min_max_bounds() -> None:
    profile = NetworkFaultProfile(mode="jitter", jitter_min_ms=300, jitter_max_ms=100)

    observed_delay_ms = resolve_delay_ms(profile, packet_index=2, rng=random.Random(3))

    assert 100 <= observed_delay_ms <= 300


@pytest.mark.test_id("COND-NET-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("丢包率必须 clamp 到 0..1，覆盖非法负数和超过 100% 的配置输入")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="should_drop_packet")
def test_network_drop_rate_condition_clamps_out_of_range_values() -> None:
    assert should_drop_packet(NetworkFaultProfile(mode="drop", drop_rate=-1.0), packet_index=1, rng=random.Random(0)) is False
    assert should_drop_packet(NetworkFaultProfile(mode="drop", drop_rate=2.0), packet_index=1, rng=random.Random(0)) is True


@pytest.mark.test_id("COND-NET-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("断连条件必须准确区分阈值前、阈值点和阈值后，供 RPC 流中断测试复用")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="should_disconnect")
def test_network_disconnect_after_n_condition_triggers_at_threshold_and_afterwards() -> None:
    profile = NetworkFaultProfile(mode="disconnect_after_n", disconnect_after_packets=3)

    assert should_disconnect(profile, packet_index=2) is False
    assert should_disconnect(profile, packet_index=3) is True
    assert should_disconnect(profile, packet_index=4) is True


@pytest.mark.test_id("COND-NET-05")
@pytest.mark.priority("P1")
@pytest.mark.requirement("半开连接必须按 read/write 方向独立阻断，覆盖单向网络异常条件")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="is_half_open_blocked")
def test_network_half_open_condition_is_direction_specific() -> None:
    write_blocked = NetworkFaultProfile(mode="half_open", half_open_direction="write")
    read_blocked = NetworkFaultProfile(mode="half_open", half_open_direction="read")

    assert is_half_open_blocked(write_blocked, direction="write") is True
    assert is_half_open_blocked(write_blocked, direction="read") is False
    assert is_half_open_blocked(read_blocked, direction="read") is True
    assert is_half_open_blocked(read_blocked, direction="write") is False
