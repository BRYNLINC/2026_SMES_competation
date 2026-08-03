from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from tools import recovery_runtime as rr


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("recovery_request")]


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
                        "vme": ["data/S1/session1/sub_S1_vme_run1.dat"],
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
                    }
                }
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return project_root


@pytest.mark.test_id("RES-REC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("若最新恢复请求损坏，系统必须回退到更早的合法请求，而不是把恢复状态整体清空")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="load_pending_recovery_request",
)
def test_load_pending_recovery_request_falls_back_to_older_valid_request_when_latest_restart_is_invalid(
    tmp_path: Path,
) -> None:
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
                    "recovery_mode": "restart_from_stage",
                    "stage": {},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = rr.load_pending_recovery_request(control_root)

    assert payload == {
        "recovery_mode": "restart_from_stage",
        "stage": {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
        },
        "requested_at": 20,
    }


@pytest.mark.test_id("RES-REC-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("若最新请求文件 JSON 损坏，恢复请求选择仍应保留旧的合法 continue_from_checkpoint 请求")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="load_pending_recovery_request",
)
def test_load_pending_recovery_request_ignores_corrupted_new_file_and_keeps_valid_resume_request(
    tmp_path: Path,
) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"

    (control_root / rr.LEGACY_RESUME_REQUEST_FILE_NAME).write_text(
        json.dumps({"requested_at": 15}),
        encoding="utf-8",
    )
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text("{bad-json", encoding="utf-8")

    payload = rr.load_pending_recovery_request(control_root)

    assert payload == {
        "recovery_mode": "continue_from_checkpoint",
        "stage": None,
        "requested_at": 15,
    }


@pytest.mark.test_id("RES-REC-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("prepare_resume_recovery 在无合法请求时必须清空陈旧控制文件并回落到 continue_from_checkpoint 默认模式")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="prepare_resume_recovery",
)
def test_prepare_resume_recovery_cleans_stale_controls_when_no_valid_request_exists(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    for file_name in rr.STALE_CONTROL_FILE_NAME_LIST:
        (control_root / file_name).write_text("{}", encoding="utf-8")
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        json.dumps(
            {
                "requested_at": 99,
                "payload": {
                    "recovery_mode": "restart_from_stage",
                    "stage": {},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = rr.prepare_resume_recovery(project_root)

    assert payload["recovery_mode"] == "continue_from_checkpoint"
    assert payload["stage"] is None
    assert payload["collector_start_selector"] is None
    assert not any((control_root / file_name).exists() for file_name in rr.STALE_CONTROL_FILE_NAME_LIST)
