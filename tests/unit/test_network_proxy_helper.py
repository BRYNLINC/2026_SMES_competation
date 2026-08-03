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


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("network_proxy")]


@pytest.mark.test_id("NET-HELP-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("network proxy helper 必须能把正常、固定延迟、抖动、丢包、半开等模式格式化为稳定描述")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="describe_profile")
def test_describe_profile_formats_supported_network_modes() -> None:
    assert describe_profile(NetworkFaultProfile(mode="normal")) == "normal(delay=0ms, drop=0%)"
    assert describe_profile(NetworkFaultProfile(mode="fixed_delay", fixed_delay_ms=200)) == "fixed_delay(delay=200ms)"
    assert describe_profile(NetworkFaultProfile(mode="jitter", jitter_min_ms=0, jitter_max_ms=300)) == "jitter(range=0-300ms)"
    assert describe_profile(NetworkFaultProfile(mode="drop", drop_rate=0.2)) == "drop(rate=20%)"
    assert describe_profile(NetworkFaultProfile(mode="half_open", half_open_direction="write")) == "half_open(direction=write)"


@pytest.mark.test_id("NET-HELP-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("固定延迟与抖动模式必须返回可预测的 delay 毫秒值，便于构造 50/200/800/1200ms 网络条件")
@pytest.mark.tested(file="tests/helpers/network_proxy.py", function="resolve_delay_ms")
def test_resolve_delay_ms_supports_fixed_delay_and_seeded_jitter() -> None:
    fixed = NetworkFaultProfile(mode="fixed_delay", fixed_delay_ms=800)
    jitter = NetworkFaultProfile(mode="jitter", jitter_min_ms=100, jitter_max_ms=120)

    assert resolve_delay_ms(fixed, packet_index=1) == 800
    assert resolve_delay_ms(jitter, packet_index=3, rng=random.Random(7)) == 110


@pytest.mark.test_id("NET-HELP-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("丢包、断连、半开连接判断必须可独立计算，供网络故障矩阵复用")
@pytest.mark.tested(
    file="tests/helpers/network_proxy.py",
    function="should_drop_packet/should_disconnect/is_half_open_blocked",
)
def test_network_proxy_fault_decisions_cover_drop_disconnect_and_half_open() -> None:
    drop_profile = NetworkFaultProfile(mode="drop", drop_rate=1.0)
    disconnect_profile = NetworkFaultProfile(mode="disconnect_after_n", disconnect_after_packets=5)
    half_open_profile = NetworkFaultProfile(mode="half_open", half_open_direction="write")

    assert should_drop_packet(drop_profile, packet_index=1, rng=random.Random(0)) is True
    assert should_disconnect(disconnect_profile, packet_index=4) is False
    assert should_disconnect(disconnect_profile, packet_index=5) is True
    assert is_half_open_blocked(half_open_profile, direction="write") is True
    assert is_half_open_blocked(half_open_profile, direction="read") is False
