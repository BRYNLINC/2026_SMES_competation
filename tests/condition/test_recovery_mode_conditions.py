from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from tools import recovery_runtime as rr


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("recovery_mode")]


def _make_project_root(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    (project_root / "results" / "control").mkdir(parents=True, exist_ok=True)

    virtual_receiver_config_path = rr.resolve_virtual_receiver_config_path(project_root)
    virtual_receiver_config_path.parent.mkdir(parents=True, exist_ok=True)
    virtual_receiver_config_path.write_text(
        yaml.safe_dump(
            {
                "device_info": {
                    "other_information": {
                        "exp_task_order": ["left_vs_rest", "right_vs_rest"],
                    }
                },
                "data_files": {
                    "S1": {
                        "vme": [
                            "Collector/receiver/virtual_receiver/data/S1/session1/sub001_S1_vme_run1.dat",
                            "Collector/receiver/virtual_receiver/data/S1/session2/sub001_S1_vme_run2.dat",
                        ],
                        "vmi": [
                            "Collector/receiver/virtual_receiver/data/S1/session1/sub001_S1_vmi_run1.dat",
                        ],
                    }
                },
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    challenge_config_path = rr.resolve_mi_challenge_config_path(project_root)
    challenge_config_path.parent.mkdir(parents=True, exist_ok=True)
    challenge_config_path.write_text(
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


@pytest.mark.test_id("COND-REC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("恢复请求中缺失 stage 的 restart_from_stage 必须被拒绝")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="load_pending_recovery_request",
)
def test_load_pending_recovery_request_rejects_restart_without_stage(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        json.dumps(
            {
                "requested_at": 100,
                "payload": {
                    "recovery_mode": "restart_from_stage",
                    "stage": {},
                },
            }
        ),
        encoding="utf-8",
    )

    assert rr.load_pending_recovery_request(control_root) is None


@pytest.mark.test_id("COND-REC-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("恢复模式应优先选择最新请求，即使旧格式 resume_request 仍存在")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="load_pending_recovery_request",
)
def test_load_pending_recovery_request_prefers_latest_across_legacy_and_new_formats(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    (control_root / rr.LEGACY_RESUME_REQUEST_FILE_NAME).write_text(
        json.dumps({"requested_at": 10}),
        encoding="utf-8",
    )
    (control_root / rr.LEGACY_RESTART_STAGE_REQUEST_FILE_NAME).write_text(
        json.dumps(
            {
                "requested_at": 20,
                "payload": {
                    "subject_id": "S1",
                    "exp_name": "vme",
                    "exp_task": "left_vs_rest",
                },
            }
        ),
        encoding="utf-8",
    )
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        json.dumps(
            {
                "requested_at": 30,
                "payload": {
                    "recovery_mode": "continue_from_checkpoint",
                },
            }
        ),
        encoding="utf-8",
    )

    request_payload = rr.load_pending_recovery_request(control_root)
    assert request_payload == {
        "recovery_mode": "continue_from_checkpoint",
        "stage": None,
        "requested_at": 30,
    }


@pytest.mark.test_id("COND-REC-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("stage catalog 中每个 checkpoint_id 必须唯一且 task_id 与 exp_name/exp_task 一致")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="load_stage_catalog",
)
def test_stage_catalog_checkpoint_ids_are_unique_and_task_ids_are_consistent(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)

    stage_catalog = rr.load_stage_catalog(project_root)

    checkpoint_id_list = [row["checkpoint_id"] for row in stage_catalog]
    assert len(checkpoint_id_list) == len(set(checkpoint_id_list))
    for row in stage_catalog:
        assert row["task_id"] == f"{row['exp_name']}_{row['exp_task']}"
        assert row["checkpoint_id"] == (
            f"{row['subject_id']}|{row['exp_name']}|{row['exp_task']}|{row['session_id']}"
        )
