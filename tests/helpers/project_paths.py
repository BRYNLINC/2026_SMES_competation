from __future__ import annotations

import os
import re
import shutil
import time
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def app_root() -> Path:
    return project_root() / "app"


def tools_root() -> Path:
    return project_root() / "tools"


def results_root() -> Path:
    return project_root() / "results"


def tests_root() -> Path:
    return project_root() / "tests"


def artifacts_root() -> Path:
    override = os.environ.get("BCI_TEST_ARTIFACTS_ROOT")
    if override:
        return Path(override)
    return tests_root() / "artifacts"


def make_run_artifact_root(test_name: str, profile: str = "local") -> Path:
    normalized_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(test_name or "test")).strip("._")
    if not normalized_name:
        normalized_name = "test"
    run_id = time.strftime("%Y%m%d-%H%M%S") + f"-{profile}"
    root = artifacts_root() / f"{run_id}-{normalized_name}"
    root.mkdir(parents=True, exist_ok=False)
    return root


def latest_artifacts_root() -> Path:
    root = artifacts_root() / "latest"
    root.mkdir(parents=True, exist_ok=True)
    return root


def copy_project_subset_to_sandbox(destination_root: Path) -> Path:
    destination_root.mkdir(parents=True, exist_ok=True)
    sandbox_root = destination_root / "sandbox"
    if sandbox_root.exists():
        shutil.rmtree(sandbox_root)
    sandbox_root.mkdir(parents=True, exist_ok=True)

    def _ignore_project_subset_copy(directory: str, names: list[str]) -> set[str]:
        ignored_names: set[str] = set()
        current_path = Path(directory)
        normalized_parts = {part.lower() for part in current_path.parts}
        if "__pycache__" in names:
            ignored_names.add("__pycache__")
        if ".pytest_cache" in names:
            ignored_names.add(".pytest_cache")
        if "data" in names and "virtual_receiver" in normalized_parts:
            ignored_names.add("data")
        if "org_data" in names and "virtual_receiver" in normalized_parts:
            ignored_names.add("org_data")
        return ignored_names

    for name in ("app", "tools", "初赛README.md", "pytest.ini", "final_multi_machine_test_manual.md"):
        source = project_root() / name
        destination = sandbox_root / name
        if not source.exists():
            continue
        if source.is_dir():
            shutil.copytree(source, destination, ignore=_ignore_project_subset_copy)
        else:
            shutil.copy2(source, destination)
    (sandbox_root / "results").mkdir(parents=True, exist_ok=True)
    return sandbox_root


def resolve_pythonpath(component: str) -> str:
    component_name = str(component or "").strip()
    root = project_root()
    mapping = {
        "Algorithm": root / "app" / "Algorithm",
        "Collector": root / "app" / "Collector",
        "ProcessHub": root / "app" / "ProcessHub",
        "CentralController": root / "app" / "CentralController",
        "JudgeWeb": root / "app" / "JudgeWeb",
    }
    resolved = mapping.get(component_name)
    if resolved is None:
        raise KeyError(f"Unknown component for PYTHONPATH: {component_name}")
    return str(resolved)
