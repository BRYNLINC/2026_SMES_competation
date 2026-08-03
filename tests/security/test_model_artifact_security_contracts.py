from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUSINESS_MANAGER_FILE = PROJECT_ROOT / "app" / "Algorithm" / "Algorithm" / "service" / "BusinessManager.py"


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("model_artifact_security")]


def _read_business_manager() -> str:
    return BUSINESS_MANAGER_FILE.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("SEC-MODEL-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("模型目录大小统计必须只遍历固定 model_artifacts 根目录，且遇到链接或重解析点时直接拒绝统计")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/service/BusinessManager.py",
    function="__resolve_model_artifact_root_path/__measure_directory_size_bytes/__is_reparse_point",
)
def test_model_artifact_size_measurement_rejects_symlink_and_reparse_point_escape() -> None:
    content = _read_business_manager()

    assert "return (package_root / 'method' / 'model_artifacts').resolve()" in content
    assert "if cls.__is_reparse_point(current_stat):" in content
    assert 'raise ValueError(f"目录包含链接或重解析点，拒绝统计: {current_path}")' in content
    assert "if cls.__is_reparse_point(entry_stat):" in content
    assert 'raise ValueError(f"检测到链接或重解析点，拒绝统计: {entry_path}")' in content


@pytest.mark.test_id("SEC-MODEL-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("模型大小统计失败时只能告警并不上报 platform_model_size_mb，不能让算法主流程崩溃")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/service/BusinessManager.py",
    function="__measure_platform_model_size_mb/get_config",
)
def test_model_artifact_size_measurement_fails_closed_without_crashing_algorithm_runtime() -> None:
    content = _read_business_manager()

    assert "except (OSError, ValueError) as exc:" in content
    assert "将不上报 platform_model_size_mb" in content
    assert "return None" in content
    assert "if self.__platform_model_size_mb is not None:" in content
    assert "config_dict['platform_model_size_mb'] = self.__platform_model_size_mb" in content


@pytest.mark.test_id("SEC-MODEL-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("模型大小上报必须来源于框架统计值，而不是选手自填字符串，避免 model_size 作弊")
@pytest.mark.tested(
    file="app/Algorithm/Algorithm/service/BusinessManager.py",
    function="initial_system/get_config",
)
def test_model_artifact_size_is_framework_measured_and_exported_via_get_config_only() -> None:
    content = _read_business_manager()

    assert "self.__platform_model_size_mb = self.__measure_platform_model_size_mb()" in content
    assert "config_dict['requested_channel_count']" in content
    assert "config_dict['platform_model_size_mb'] = self.__platform_model_size_mb" in content
    assert "self.__algorithm_config_dict.update(config_dict)" in content
