from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from tools import recovery_runtime as rr


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("recovery")]


def _make_project_root(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    (project_root / "results" / "control").mkdir(parents=True, exist_ok=True)
    vr_path = rr.resolve_virtual_receiver_config_path(project_root)
    vr_path.parent.mkdir(parents=True, exist_ok=True)
    vr_path.write_text(
        yaml.safe_dump(
            {
                "device_info": {"other_information": {"exp_task_order": ["left_vs_rest", "right_vs_rest"]}},
                "data_files": {
                    "S1": {
                        "vme": [
                            "data/S1/session1/sub_S1_vme_run1.dat",
                            "data/S1/session2/sub_S1_vme_run1.dat",
                        ],
                        "vmi": ["data/S1/session2/sub_S1_vmi_run1.dat"],
                    }
                },
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    challenge_path = rr.resolve_mi_challenge_config_path(project_root)
    challenge_path.parent.mkdir(parents=True, exist_ok=True)
    challenge_path.write_text(
        yaml.safe_dump(
            {
                "score_config": {
                    "task_baseline_score": {
                        "vme_left_vs_rest": 0.0,
                        "vme_right_vs_rest": 0.0,
                        "vmi_left_vs_rest": 0.0,
                        "vmi_right_vs_rest": 0.0,
                    }
                }
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return project_root


@pytest.mark.test_id("REC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("stage payload 规范化时非法输入返回 None")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="normalize_stage_payload")
def test_normalize_stage_payload() -> None:
    assert rr.normalize_stage_payload(None) is None
    assert rr.normalize_stage_payload({"subject_id": "S1", "exp_name": "vme"}) is None
    assert rr.normalize_stage_payload(
        {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"}
    ) is None
    assert rr.normalize_stage_payload(
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session2",
        }
    ) == {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "left_vs_rest",
        "session_id": "session2",
    }


@pytest.mark.test_id("REC-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("checkpoint id 格式固定")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="build_checkpoint_id")
def test_build_checkpoint_id() -> None:
    assert rr.build_checkpoint_id(None) == ""
    assert rr.build_checkpoint_id(
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session2",
        }
    ) == "S1|vme|left_vs_rest|session2"


@pytest.mark.test_id("REC-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("stage catalog 从 VirtualReceiverConfig 正确展开")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="load_stage_catalog")
def test_load_stage_catalog(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    stage_catalog = rr.load_stage_catalog(project_root)
    assert [row["checkpoint_id"] for row in stage_catalog] == [
        "S1|vme|left_vs_rest|session1",
        "S1|vme|right_vs_rest|session1",
        "S1|vme|left_vs_rest|session2",
        "S1|vme|right_vs_rest|session2",
        "S1|vmi|left_vs_rest|session2",
        "S1|vmi|right_vs_rest|session2",
    ]


@pytest.mark.test_id("REC-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("configured task order 优先读取 Challenge baseline")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="load_configured_task_order")
def test_load_configured_task_order(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    assert rr.load_configured_task_order(project_root) == [
        "vme_left_vs_rest",
        "vme_right_vs_rest",
        "vmi_left_vs_rest",
        "vmi_right_vs_rest",
    ]


@pytest.mark.test_id("REC-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("指定 checkpoint 可映射到 stage selector")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="find_stage_selector")
def test_find_stage_selector(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    selector = rr.find_stage_selector(
        project_root,
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session2",
        },
    )
    assert selector is not None
    assert selector["task_id"] == "vme_left_vs_rest"
    assert selector["session_id"] == "session2"


@pytest.mark.test_id("REC-05A")
@pytest.mark.priority("P0")
@pytest.mark.requirement("旧三字段阶段仅在唯一匹配 session 时允许兼容解析")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="resolve_stage_payload")
def test_resolve_stage_payload_accepts_unique_legacy_stage_and_rejects_ambiguous_stage(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)

    assert rr.resolve_stage_payload(
        project_root,
        {"subject_id": "S1", "exp_name": "vmi", "exp_task": "left_vs_rest"},
    ) == {
        "subject_id": "S1",
        "exp_name": "vmi",
        "exp_task": "left_vs_rest",
        "session_id": "session2",
    }
    with pytest.raises(ValueError, match="session_id"):
        rr.resolve_stage_payload(
            project_root,
            {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"},
        )


@pytest.mark.test_id("REC-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("恢复请求选择最新合法请求")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="load_pending_recovery_request")
def test_load_pending_recovery_request_prefers_latest_valid_request(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    (control_root / rr.LEGACY_RESUME_REQUEST_FILE_NAME).write_text(json.dumps({"requested_at": 10}), encoding="utf-8")
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        json.dumps(
            {
                "requested_at": 20,
                "payload": {
                    "recovery_mode": "restart_from_stage",
                    "stage": {
                        "subject_id": "S1",
                        "exp_name": "vme",
                        "exp_task": "left_vs_rest",
                        "session_id": "session2",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    request_payload = rr.load_pending_recovery_request(control_root)
    assert request_payload == {
        "recovery_mode": "restart_from_stage",
        "stage": {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session2",
        },
        "requested_at": 20,
    }


@pytest.mark.test_id("REC-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("stale control request 可清理")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="clear_stale_control_requests")
def test_clear_stale_control_requests(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    for file_name in rr.STALE_CONTROL_FILE_NAME_LIST:
        (control_root / file_name).write_text("{}", encoding="utf-8")
    rr.clear_stale_control_requests(control_root)
    assert all(not (control_root / file_name).exists() for file_name in rr.STALE_CONTROL_FILE_NAME_LIST)
