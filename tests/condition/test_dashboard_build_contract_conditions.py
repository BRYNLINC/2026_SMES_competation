from __future__ import annotations

import json
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_JSON_FILE = PROJECT_ROOT / "judge-dashboard" / "package.json"


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("dashboard_build_contract")]


def _load_package_json() -> dict:
    return json.loads(PACKAGE_JSON_FILE.read_text(encoding="utf-8"))


@pytest.mark.test_id("COND-DASH-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("judge-dashboard 必须存在 dev/build/lint/preview 脚本，满足前端冒烟与发布构建测试入口")
@pytest.mark.tested(file="judge-dashboard/package.json", function="npm_script_contract")
def test_dashboard_package_json_contains_required_scripts() -> None:
    payload = _load_package_json()
    scripts = payload["scripts"]

    assert scripts["dev"] == "vite"
    assert scripts["build"] == "tsc -b && vite build"
    assert scripts["lint"] == "eslint ."
    assert scripts["preview"] == "vite preview"


@pytest.mark.test_id("COND-DASH-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("judge-dashboard 构建契约必须包含 react/react-dom/vite/typescript，避免前端层在现场缺关键依赖")
@pytest.mark.tested(file="judge-dashboard/package.json", function="dependency_contract")
def test_dashboard_package_json_contains_core_build_dependencies() -> None:
    payload = _load_package_json()

    assert "react" in payload["dependencies"]
    assert "react-dom" in payload["dependencies"]
    assert "vite" in payload["devDependencies"]
    assert "typescript" in payload["devDependencies"]
