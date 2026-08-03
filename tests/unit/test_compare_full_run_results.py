from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from tools.compare_full_run_results import compare_full_run_results


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("reproducibility")]


def _write_csv(file_path: Path, row_list: list[dict]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open('w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(row_list[0]))
        writer.writeheader()
        writer.writerows(row_list)


def _write_run(
    results_root: Path,
    *,
    mode: str = 'clear',
    recovery_mode: str = 'clear_start',
    processor_list: list[str] | None = None,
    score: float = 80.0,
    accuracy: float = 75.0,
    predict_time_ms: float = 10.0,
    predict_label: str = '1',
) -> None:
    processor_list = processor_list or ['team_0.group_1']
    control_root = results_root / 'control'
    control_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        'match_start_mode': mode,
        'processor_component_id_list': processor_list,
        'applied_recovery': {
            'recovery_mode': recovery_mode,
            'stage': (
                None
                if recovery_mode == 'clear_start'
                else {
                    'subject_id': 'S1',
                    'exp_name': 'vme',
                    'exp_task': 'right_vs_rest',
                    'session_id': 'session1',
                }
            ),
        },
        'run_provenance': {
            'run_kind': (
                'clean_full_run'
                if mode == 'clear' and recovery_mode == 'clear_start'
                else 'recovery_run'
            ),
            'project_root': 'C:/same-project',
            'git_revision': 'abc123',
            'git_tracked_dirty': False,
            'config_sha256_by_path': {'config.yml': 'hash'},
        },
    }
    (control_root / 'launcher_manifest.json').write_text(
        json.dumps(manifest),
        encoding='utf-8',
    )
    _write_csv(
        results_root / '00_team_score_overview.csv',
        [
            {
                'team_id': 'team_0',
                'total_score': score,
                'mean_accuracy_percent': accuracy,
            }
        ],
    )
    _write_csv(
        results_root / 'team_0' / '03_trial_records.csv',
        [
            {
                'team_trial_index': 1,
                'subject_id': 'S1',
                'task_id': 'vme_right_vs_rest',
                'session_id': 'session1',
                'block_id': 1,
                'trial_id': 1,
                'true_label': '1',
                'raw_predict_label': predict_label,
                'predict_label': predict_label,
                'is_correct': str(predict_label == '1'),
                'is_timeout': 'False',
                'is_invalid_output': 'False',
                'report_position': 'eeg_1:4000',
                'predict_time_ms': predict_time_ms,
            }
        ],
    )


@pytest.mark.test_id('COMPARE-01')
@pytest.mark.priority('P0')
@pytest.mark.requirement('同版本、同配置的两次 clean 全量结果应支持逐 trial 一致性判定')
@pytest.mark.tested(file='tools/compare_full_run_results.py', function='compare_full_run_results')
def test_identical_clean_runs_are_reproducible(tmp_path: Path) -> None:
    first_root = tmp_path / 'first'
    second_root = tmp_path / 'second'
    _write_run(first_root)
    _write_run(second_root)

    report = compare_full_run_results(first_root, second_root)

    assert report['verdict'] == 'reproducible'
    assert report['prediction_identical'] is True
    assert report['score_identical'] is True
    assert report['issues'] == []


@pytest.mark.test_id('COMPARE-02')
@pytest.mark.priority('P0')
@pytest.mark.requirement('resume/restart_from_stage 结果不得伪装成第二次 clean 全量结果参与可复现对比')
@pytest.mark.tested(file='tools/compare_full_run_results.py', function='_check_run_provenance')
def test_recovery_run_is_rejected_as_non_comparable(tmp_path: Path) -> None:
    first_root = tmp_path / 'first'
    second_root = tmp_path / 'second'
    _write_run(first_root)
    _write_run(
        second_root,
        mode='resume',
        recovery_mode='restart_from_stage',
    )

    report = compare_full_run_results(first_root, second_root)

    assert report['verdict'] == 'not_comparable'
    assert any(issue['code'] == 'not_clean_full_run' for issue in report['issues'])


@pytest.mark.test_id('COMPARE-03')
@pytest.mark.priority('P1')
@pytest.mark.requirement('预测相同但墙钟耗时不同必须单独标记为计时分波动')
@pytest.mark.tested(file='tools/compare_full_run_results.py', function='_compare_team')
def test_timing_only_score_variance_is_classified_separately(tmp_path: Path) -> None:
    first_root = tmp_path / 'first'
    second_root = tmp_path / 'second'
    _write_run(first_root, score=80.0, predict_time_ms=10.0)
    _write_run(second_root, score=79.9, predict_time_ms=20.0)

    report = compare_full_run_results(first_root, second_root)

    assert report['verdict'] == 'prediction_reproducible_timing_score_varies'
    assert report['prediction_identical'] is True
    assert report['accuracy_identical'] is True
    assert report['score_identical'] is False
    assert report['field_mismatch_total']['predict_time_ms'] == 1
