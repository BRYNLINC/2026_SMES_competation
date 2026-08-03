from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
import yaml

from tools import recovery_runtime as rr


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("recovery_snapshot")]


def _write_csv(file_path: Path, fieldnames: list[str], row_list: list[dict]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(row_list)


def _make_project_root(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    (project_root / "results" / "control").mkdir(parents=True, exist_ok=True)
    (project_root / "results" / "live").mkdir(parents=True, exist_ok=True)

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
                        "vme": ["Collector/receiver/virtual_receiver/data/S1/session1/sub001_S1_vme_run1.dat"],
                        "vmi": ["Collector/receiver/virtual_receiver/data/S1/session2/sub001_S1_vmi_run1.dat"],
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


@pytest.mark.test_id("COND-REC-SNAP-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("restart_from_stage 完成后必须清理所有陈旧控制请求文件，避免二次恢复误触发")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="apply_restart_from_stage/clear_stale_control_requests",
)
def test_apply_restart_from_stage_clears_all_stale_control_requests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = _make_project_root(tmp_path)
    results_root = project_root / "results"
    control_root = results_root / "control"
    team_dir = results_root / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(team_dir / "03_trial_records.csv", rr.TRIAL_RECORD_FIELDNAMES, [])
    for file_name in rr.STALE_CONTROL_FILE_NAME_LIST:
        (control_root / file_name).write_text("{}", encoding="utf-8")
    (results_root / "live" / "stale.txt").write_text("stale", encoding="utf-8")

    monkeypatch.setattr(rr, "archive_results_snapshot", lambda project_root_arg, archive_reason: {"archive_reason": archive_reason})

    result = rr.apply_restart_from_stage(
        project_root,
        {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"},
    )

    assert result["recovery_mode"] == "restart_from_stage"
    assert not any((control_root / file_name).exists() for file_name in rr.STALE_CONTROL_FILE_NAME_LIST)
    assert not (results_root / "live" / "stale.txt").exists()


@pytest.mark.test_id("COND-REC-SNAP-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("continue_from_checkpoint 请求生效后必须清理控制目录中的遗留恢复文件")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="prepare_resume_recovery/load_pending_recovery_request",
)
def test_prepare_resume_recovery_continue_mode_clears_pending_request_files(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        json.dumps(
            {
                "requested_at": 11,
                "payload": {
                    "recovery_mode": "continue_from_checkpoint",
                    "stage": {
                        "subject_id": "S1",
                        "exp_name": "vme",
                        "exp_task": "right_vs_rest",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = rr.prepare_resume_recovery(project_root)

    assert payload["recovery_mode"] == "continue_from_checkpoint"
    assert payload["stage"] == {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "right_vs_rest",
    }
    assert payload["requested_at"] == 11
    assert not (control_root / rr.RECOVERY_REQUEST_FILE_NAME).exists()


@pytest.mark.test_id("COND-REC-SNAP-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("restart_from_stage 的恢复归档原因必须带上阶段标识，便于赛后定位恢复来源")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="apply_restart_from_stage",
)
def test_apply_restart_from_stage_records_stage_specific_archive_reason(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = _make_project_root(tmp_path)
    results_root = project_root / "results"
    team_dir = results_root / "team_1"
    team_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(team_dir / "03_trial_records.csv", rr.TRIAL_RECORD_FIELDNAMES, [])

    captured: dict[str, str] = {}

    def _fake_archive(project_root_arg: Path, archive_reason: str) -> dict:
        captured["archive_reason"] = archive_reason
        return {"archive_reason": archive_reason}

    monkeypatch.setattr(rr, "archive_results_snapshot", _fake_archive)

    rr.apply_restart_from_stage(
        project_root,
        {"subject_id": "S1", "exp_name": "vmi", "exp_task": "right_vs_rest"},
    )

    assert captured["archive_reason"] == "restart_from_stage_S1_vmi_right_vs_rest_session2"
