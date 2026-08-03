from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tools import recovery_runtime as rr


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("recovery")]


def _make_empty_project_root(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    (project_root / "results" / "control").mkdir(parents=True, exist_ok=True)
    vr_path = rr.resolve_virtual_receiver_config_path(project_root)
    vr_path.parent.mkdir(parents=True, exist_ok=True)
    vr_path.write_text(yaml.safe_dump({"data_files": {}}, allow_unicode=True), encoding="utf-8")
    challenge_path = rr.resolve_mi_challenge_config_path(project_root)
    challenge_path.parent.mkdir(parents=True, exist_ok=True)
    challenge_path.write_text(yaml.safe_dump({}, allow_unicode=True), encoding="utf-8")
    return project_root


@pytest.mark.test_id("REC-08")
@pytest.mark.priority("P1")
@pytest.mark.requirement("空数据配置下 stage catalog 为空列表")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="load_stage_catalog")
def test_load_stage_catalog_returns_empty_list_for_empty_config(tmp_path: Path) -> None:
    project_root = _make_empty_project_root(tmp_path)
    assert rr.load_stage_catalog(project_root) == []


@pytest.mark.test_id("REC-09")
@pytest.mark.priority("P1")
@pytest.mark.requirement("无 baseline 且无 stage 时 configured task order 为空")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="load_configured_task_order")
def test_load_configured_task_order_returns_empty_without_baseline_or_stage(tmp_path: Path) -> None:
    project_root = _make_empty_project_root(tmp_path)
    assert rr.load_configured_task_order(project_root) == []


@pytest.mark.test_id("REC-10")
@pytest.mark.priority("P1")
@pytest.mark.requirement("非法 restart 请求且无 stage 时返回 None")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="load_pending_recovery_request")
def test_load_pending_recovery_request_invalid_restart_without_stage_returns_none(tmp_path: Path) -> None:
    project_root = _make_empty_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        '{"requested_at": 10, "payload": {"recovery_mode": "restart_from_stage"}}',
        encoding="utf-8",
    )
    assert rr.load_pending_recovery_request(control_root) is None
