from __future__ import annotations

import random

import pytest

from tests.helpers.network_proxy import (
    NetworkFaultProfile,
    describe_profile,
    is_half_open_blocked,
    resolve_delay_ms,
    should_disconnect,
    should_drop_packet,
)


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("network_proxy_profiles")]


@pytest.mark.test_id("NET-PROFILE-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("网络模式矩阵必须覆盖 50/200/800/1200ms 固定延迟，供 timeout 与非 timeout 条件测试复用")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="resolve_delay_ms/describe_profile")
@pytest.mark.parametrize("delay_ms", [50, 200, 800, 1200])
def test_network_proxy_fixed_delay_profiles_cover_required_timeout_thresholds(delay_ms: int) -> None:
    profile = NetworkFaultProfile(mode="fixed_delay", fixed_delay_ms=delay_ms)

    assert resolve_delay_ms(profile, packet_index=1) == delay_ms
    assert describe_profile(profile) == f"fixed_delay(delay={delay_ms}ms)"


@pytest.mark.test_id("NET-PROFILE-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("抖动模式必须能稳定生成 0-300ms 范围内的随机延迟，不越界")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="resolve_delay_ms")
def test_network_proxy_jitter_profile_stays_within_expected_bounds() -> None:
    profile = NetworkFaultProfile(mode="jitter", jitter_min_ms=0, jitter_max_ms=300)
    rng = random.Random(11)

    generated_delay_list = [resolve_delay_ms(profile, packet_index=index, rng=rng) for index in range(10)]

    assert all(0 <= delay <= 300 for delay in generated_delay_list)
    assert any(delay > 0 for delay in generated_delay_list)


@pytest.mark.test_id("NET-PROFILE-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("丢包、断连、半开连接三种故障模式必须能组合表达计划中的网络波动矩阵")
@pytest.mark.tested(
    file="tests/helpers/network_proxy.py",
    function="should_drop_packet/should_disconnect/is_half_open_blocked/describe_profile",
)
def test_network_proxy_profiles_cover_drop_disconnect_and_half_open_scenarios() -> None:
    drop_profile = NetworkFaultProfile(mode="drop", drop_rate=0.2)
    disconnect_profile = NetworkFaultProfile(mode="disconnect_after_n", disconnect_after_packets=3)
    half_open_profile = NetworkFaultProfile(mode="half_open", half_open_direction="read")

    assert describe_profile(drop_profile) == "drop(rate=20%)"
    assert describe_profile(disconnect_profile) == "disconnect_after_n(packets=3)"
    assert should_drop_packet(drop_profile, packet_index=1, rng=random.Random(1)) is True
    assert should_disconnect(disconnect_profile, packet_index=2) is False
    assert should_disconnect(disconnect_profile, packet_index=3) is True
    assert is_half_open_blocked(half_open_profile, direction="read") is True
    assert is_half_open_blocked(half_open_profile, direction="write") is False
