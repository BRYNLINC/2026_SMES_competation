from __future__ import annotations

import csv
from pathlib import Path

import pytest
import yaml

from tools import recovery_runtime as rr
from tools import runtime_state_sqlite as rss


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("recovery_restart")]


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

    vr_path = rr.resolve_virtual_receiver_config_path(project_root)
    vr_path.parent.mkdir(parents=True, exist_ok=True)
    vr_path.write_text(
        yaml.safe_dump(
            {
                "device_info": {"other_information": {"exp_task_order": ["left_vs_rest", "right_vs_rest"]}},
                "data_files": {
                    "S1": {
                        "vme": ["data/S1/session1/sub_S1_vme_run1.dat"],
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


def _trial_row(
    *,
    team_id: str,
    team_trial_index: int,
    task_trial_index: int,
    subject_id: str,
    task_id: str,
    exp_name: str,
    exp_task: str,
    session_id: str,
    trial_id: str,
    cumulative_score: float,
) -> dict:
    return {
        "team_id": team_id,
        "team_trial_index": team_trial_index,
        "task_trial_index": task_trial_index,
        "subject_id": subject_id,
        "task_id": task_id,
        "exp_name": exp_name,
        "exp_task": exp_task,
        "session_id": session_id,
        "block_id": session_id,
        "trial_id": trial_id,
        "true_label": "1",
        "predict_label": "1",
        "is_correct": True,
        "trial_score": 10.0,
        "is_timeout": False,
        "predict_time_ms": 200.0,
        "cumulative_accuracy_percent": 100.0,
        "cumulative_score": cumulative_score,
        "report_position": f"{exp_name}:{trial_id}",
    }


@pytest.mark.test_id("RES-RESTART-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("restart_from_stage 在多队场景下必须只裁切目标阶段之后结果，并保持总榜按 total_score 降序稳定排序")
@pytest.mark.tested(
    file="tools/recovery_runtime.py;tools/runtime_state_sqlite.py",
    function="apply_restart_from_stage/load_team_score_overview_rows",
)
def test_apply_restart_from_stage_keeps_multi_team_scoreboard_consistent_and_sorted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _make_project_root(tmp_path)
    results_root = project_root / "results"
    for team_id, row_list in {
        "team_0": [
            _trial_row(
                team_id="team_0",
                team_trial_index=1,
                task_trial_index=1,
                subject_id="S1",
                task_id="vme_left_vs_rest",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session1",
                trial_id="1",
                cumulative_score=12.0,
            ),
            _trial_row(
                team_id="team_0",
                team_trial_index=2,
                task_trial_index=1,
                subject_id="S1",
                task_id="vmi_left_vs_rest",
                exp_name="vmi",
                exp_task="left_vs_rest",
                session_id="session2",
                trial_id="1",
                cumulative_score=24.0,
            ),
        ],
        "team_1": [
            _trial_row(
                team_id="team_1",
                team_trial_index=1,
                task_trial_index=1,
                subject_id="S1",
                task_id="vme_left_vs_rest",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session1",
                trial_id="1",
                cumulative_score=8.0,
            ),
            _trial_row(
                team_id="team_1",
                team_trial_index=2,
                task_trial_index=1,
                subject_id="S1",
                task_id="vmi_left_vs_rest",
                exp_name="vmi",
                exp_task="left_vs_rest",
                session_id="session2",
                trial_id="1",
                cumulative_score=16.0,
            ),
        ],
    }.items():
        team_dir = results_root / team_id
        team_dir.mkdir(parents=True, exist_ok=True)
        _write_csv(team_dir / "03_trial_records.csv", rr.TRIAL_RECORD_FIELDNAMES, row_list)

    monkeypatch.setattr(rr, "archive_results_snapshot", lambda project_root_arg, archive_reason: {"archive_reason": archive_reason})

    result = rr.apply_restart_from_stage(
        project_root,
        {"subject_id": "S1", "exp_name": "vmi", "exp_task": "left_vs_rest"},
    )

    runtime_state_db_path = rss.resolve_runtime_state_db_path(project_root)
    scoreboard_rows = rss.load_team_score_overview_rows(runtime_state_db_path)
    team_0_trials = rss.load_team_trial_record_rows(runtime_state_db_path, "team_0")
    team_1_trials = rss.load_team_trial_record_rows(runtime_state_db_path, "team_1")

    assert result["recovery_mode"] == "restart_from_stage"
    assert [row["team_id"] for row in scoreboard_rows] == ["team_0", "team_1"]
    assert scoreboard_rows[0]["total_score"] == 12.0
    assert scoreboard_rows[1]["total_score"] == 8.0
    assert len(team_0_trials) == 1
    assert len(team_1_trials) == 1
    assert team_0_trials[0]["task_id"] == "vme_left_vs_rest"
    assert team_1_trials[0]["task_id"] == "vme_left_vs_rest"


@pytest.mark.test_id("RES-RESTART-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("prepare_resume_recovery 命中 restart_from_stage 时必须把 requested_at 透传到 applied recovery，供 resume 后重连场景追踪")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="prepare_resume_recovery",
)
def test_prepare_resume_recovery_preserves_requested_at_for_restart_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _make_project_root(tmp_path)
    control_root = project_root / "results" / "control"
    captured: list[dict] = []

    def fake_apply_restart_from_stage(
        project_root_arg: Path,
        stage_payload: dict,
        *,
        requested_at=None,
    ) -> dict:
        captured.append(
            {
                "project_root": project_root_arg,
                "stage": stage_payload,
                "requested_at": requested_at,
            }
        )
        return {
            "recovery_mode": "restart_from_stage",
            "stage": stage_payload,
            "collector_start_selector": {"task_id": "vme_left_vs_rest"},
            "team_summary_list": [],
            "history_archive": {"archive_reason": "restart_from_stage_S1_vme_left_vs_rest"},
            "applied_at": 123.0,
        }

    monkeypatch.setattr(rr, "apply_restart_from_stage", fake_apply_restart_from_stage)
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        '{"requested_at": 88, "payload": {"recovery_mode": "restart_from_stage", "stage": {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"}}}',
        encoding="utf-8",
    )

    payload = rr.prepare_resume_recovery(project_root)

    assert captured == [
        {
            "project_root": project_root,
            "stage": {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"},
            "requested_at": 88,
        }
    ]
    assert payload["requested_at"] == 88
    assert payload["recovery_mode"] == "restart_from_stage"
    assert payload["stage"] == {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"}


@pytest.mark.test_id("RES-RESTART-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("恢复中断后再次启动必须复用已校验的同请求快照，而不是校验半成品结果")
@pytest.mark.tested(file="tools/recovery_runtime.py", function="_find_reusable_restart_archive/prepare_resume_recovery")
def test_prepare_resume_recovery_reuses_verified_archive_after_partial_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _make_project_root(tmp_path)
    results_root = project_root / "results"
    control_root = results_root / "control"
    team_dir = results_root / "team_0"
    team_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        team_dir / "03_trial_records.csv",
        rr.TRIAL_RECORD_FIELDNAMES,
        [
            _trial_row(
                team_id="team_0",
                team_trial_index=1,
                task_trial_index=1,
                subject_id="S1",
                task_id="vme_left_vs_rest",
                exp_name="vme",
                exp_task="left_vs_rest",
                session_id="session1",
                trial_id="1",
                cumulative_score=10.0,
            )
        ],
    )

    requested_at = 100.0
    archive_root = results_root / "history" / "100_restart_from_stage_S1_vme_right_vs_rest_session1"
    archive_root.mkdir(parents=True, exist_ok=True)
    (archive_root / "manifest.json").write_text(
        '{"archive_reason":"restart_from_stage_S1_vme_right_vs_rest_session1",'
        '"captured_at":101.0,"archive_root":"archive"}',
        encoding="utf-8",
    )
    monkeypatch.setattr(rr, "verify_archive_manifest", lambda archive_path: {"status": "ok"})
    monkeypatch.setattr(
        rr,
        "archive_results_snapshot",
        lambda project_root_arg, archive_reason: (_ for _ in ()).throw(
            AssertionError("a verified retry archive should be reused")
        ),
    )
    (control_root / rr.RECOVERY_REQUEST_FILE_NAME).write_text(
        '{"requested_at":100.0,"payload":{"recovery_mode":"restart_from_stage",'
        '"stage":{"subject_id":"S1","exp_name":"vme",'
        '"exp_task":"right_vs_rest","session_id":"session1"}}}',
        encoding="utf-8",
    )

    result = rr.prepare_resume_recovery(project_root)

    assert result["history_archive_reused"] is True
    assert result["history_archive"]["archive_reason"] == "restart_from_stage_S1_vme_right_vs_rest_session1"
    assert not (control_root / rr.RECOVERY_REQUEST_FILE_NAME).exists()
    assert len(rss.load_team_trial_record_rows(rss.resolve_runtime_state_db_path(project_root), "team_0")) == 1


@pytest.mark.test_id("RES-RESTART-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("非法 checkpoint 的 restart_from_stage 必须抛出明确错误，避免比赛被拉起到不存在阶段")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="apply_restart_from_stage",
)
def test_apply_restart_from_stage_rejects_unknown_checkpoint_with_clear_error(tmp_path: Path) -> None:
    project_root = _make_project_root(tmp_path)

    with pytest.raises(ValueError, match="指定恢复阶段不存在于配置阶段列表中"):
        rr.apply_restart_from_stage(
            project_root,
            {"subject_id": "S9", "exp_name": "vmi", "exp_task": "right_vs_rest"},
        )
