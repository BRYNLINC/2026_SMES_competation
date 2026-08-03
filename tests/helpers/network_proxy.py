from __future__ import annotations

from dataclasses import dataclass
import random


@dataclass(frozen=True)
class NetworkFaultProfile:
    mode: str
    fixed_delay_ms: int = 0
    jitter_min_ms: int = 0
    jitter_max_ms: int = 0
    drop_rate: float = 0.0
    disconnect_after_packets: int | None = None
    half_open_direction: str | None = None


def describe_profile(profile: NetworkFaultProfile) -> str:
    if profile.mode == "normal":
        return "normal(delay=0ms, drop=0%)"
    if profile.mode == "fixed_delay":
        return f"fixed_delay(delay={int(profile.fixed_delay_ms)}ms)"
    if profile.mode == "jitter":
        return f"jitter(range={int(profile.jitter_min_ms)}-{int(profile.jitter_max_ms)}ms)"
    if profile.mode == "drop":
        return f"drop(rate={profile.drop_rate:.0%})"
    if profile.mode == "disconnect_after_n":
        return f"disconnect_after_n(packets={profile.disconnect_after_packets})"
    if profile.mode == "half_open":
        return f"half_open(direction={profile.half_open_direction or 'unknown'})"
    return f"unknown(mode={profile.mode})"


def resolve_delay_ms(profile: NetworkFaultProfile, *, packet_index: int, rng: random.Random | None = None) -> int:
    if profile.mode == "fixed_delay":
        return max(0, int(profile.fixed_delay_ms))
    if profile.mode == "jitter":
        generator = rng or random.Random(packet_index)
        low = min(int(profile.jitter_min_ms), int(profile.jitter_max_ms))
        high = max(int(profile.jitter_min_ms), int(profile.jitter_max_ms))
        return max(0, int(generator.randint(low, high)))
    return 0


def should_drop_packet(profile: NetworkFaultProfile, *, packet_index: int, rng: random.Random | None = None) -> bool:
    if profile.mode != "drop":
        return False
    rate = min(1.0, max(0.0, float(profile.drop_rate)))
    generator = rng or random.Random(packet_index)
    return generator.random() < rate


def should_disconnect(profile: NetworkFaultProfile, *, packet_index: int) -> bool:
    if profile.mode != "disconnect_after_n":
        return False
    if profile.disconnect_after_packets is None:
        return False
    return int(packet_index) >= int(profile.disconnect_after_packets)


def is_half_open_blocked(profile: NetworkFaultProfile, *, direction: str) -> bool:
    if profile.mode != "half_open":
        return False
    normalized_direction = str(direction or "").strip().lower()
    configured_direction = str(profile.half_open_direction or "").strip().lower()
    return configured_direction != "" and normalized_direction == configured_direction
