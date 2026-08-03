from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers import project_root
from tests.helpers import project_paths as pp


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("test_infra")]


@pytest.mark.test_id("INFRA-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("测试辅助路径解析必须稳定指向仓库根目录")
@pytest.mark.tested(file="tests/helpers/project_paths.py", function="project_root")
def test_project_root_points_to_repository_root() -> None:
    root = project_root()
    assert root == pp.project_root()
    assert (root / "app").exists()
    assert (root / "tools").exists()
    assert (root / "final_multi_machine_test_manual.md").exists()


@pytest.mark.test_id("INFRA-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("run artifact 根目录应唯一且自动创建")
@pytest.mark.tested(file="tests/helpers/project_paths.py", function="make_run_artifact_root")
def test_make_run_artifact_root_creates_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BCI_TEST_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))

    artifact_root = pp.make_run_artifact_root("infra_test_case", profile="unit")

    assert artifact_root.exists()
    assert artifact_root.name.endswith("-infra_test_case")
    assert artifact_root.parent == tmp_path / "artifacts"


@pytest.mark.test_id("INFRA-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("组件 PYTHONPATH 解析正确")
@pytest.mark.tested(file="tests/helpers/project_paths.py", function="resolve_pythonpath")
def test_resolve_pythonpath_returns_existing_component_root() -> None:
    algorithm_root = Path(pp.resolve_pythonpath("Algorithm"))
    judge_web_root = Path(pp.resolve_pythonpath("JudgeWeb"))
    assert algorithm_root.name == "Algorithm"
    assert judge_web_root.name == "JudgeWeb"
    assert algorithm_root.exists()
    assert judge_web_root.exists()
