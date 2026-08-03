from __future__ import annotations

import pytest

from tests.helpers.fake_algorithm_server import DeterministicFakeAlgorithmServer, build_profile, describe_profile


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("fake_algorithm_resource_hog")]


@pytest.mark.test_id("SEC-HOG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("resource_hog profile 必须显式暴露 CPU/内存占用意图，供平台超时与资源清理策略测试复用")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="build_profile/emit_prediction/snapshot",
)
def test_fake_algorithm_resource_hog_profile_exposes_cpu_and_memory_load_without_real_side_effects() -> None:
    server = DeterministicFakeAlgorithmServer(
        build_profile("resource_hog", cpu_burn_ms=2200, memory_blob_kb=4096, predict_timeout_ms=1000)
    )

    action = server.emit_prediction(report_source_position="trial_end", now_ms=500)[0]
    snapshot = server.snapshot()

    assert action.kind == "resource_hog"
    assert action.accepted is False
    assert action.reason == "resource_hog_profile"
    assert action.payload == {"cpu_burn_ms": 2200, "memory_blob_kb": 4096}
    assert snapshot.resource_hog_events == [{"cpu_burn_ms": 2200, "memory_blob_kb": 4096}]


@pytest.mark.test_id("SEC-HOG-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("resource_hog profile 描述文本必须稳定携带 CPU/内存字段，便于 CSV 报告直接区分资源占用异常")
@pytest.mark.tested(
    file="tests/helpers/fake_algorithm_server.py",
    function="describe_profile",
)
def test_fake_algorithm_resource_hog_profile_description_is_stable() -> None:
    summary = describe_profile(build_profile("resource_hog", cpu_burn_ms=1800, memory_blob_kb=1024))

    assert summary == "resource_hog, cpu_burn_ms=1800, memory_blob_kb=1024"
