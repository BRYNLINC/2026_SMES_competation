from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path


TRIAL_IDENTITY_FIELDS = (
    'team_trial_index',
    'subject_id',
    'task_id',
    'session_id',
    'block_id',
    'trial_id',
)
PREDICTION_FIELDS = (
    'raw_predict_label',
    'predict_label',
    'is_correct',
    'is_timeout',
    'is_invalid_output',
    'report_position',
)


class ComparisonInputError(RuntimeError):
    pass


def compare_full_run_results(first_root: Path, second_root: Path) -> dict:
    first_root = first_root.resolve()
    second_root = second_root.resolve()
    first_manifest = _read_json(first_root / 'control' / 'launcher_manifest.json')
    second_manifest = _read_json(second_root / 'control' / 'launcher_manifest.json')
    issue_list: list[dict] = []

    first_summary = _manifest_summary(first_manifest)
    second_summary = _manifest_summary(second_manifest)
    _check_run_provenance(first_summary, second_summary, issue_list)

    first_score_by_team = _load_score_by_team(first_root)
    second_score_by_team = _load_score_by_team(second_root)
    first_team_set = set(first_score_by_team)
    second_team_set = set(second_score_by_team)
    if first_team_set != second_team_set:
        _add_issue(
            issue_list,
            'blocker',
            'scoreboard_team_set_mismatch',
            first_only=sorted(first_team_set - second_team_set),
            second_only=sorted(second_team_set - first_team_set),
        )

    common_team_list = sorted(first_team_set & second_team_set)
    team_comparison_list = [
        _compare_team(first_root, second_root, team_id, first_score_by_team, second_score_by_team)
        for team_id in common_team_list
    ]

    field_mismatch_total = {
        field_name: sum(
            int(team_comparison['field_mismatch_count'].get(field_name, 0))
            for team_comparison in team_comparison_list
        )
        for field_name in (*TRIAL_IDENTITY_FIELDS, 'true_label', *PREDICTION_FIELDS, 'predict_time_ms')
    }
    row_count_identical = all(
        item['first_trial_count'] == item['second_trial_count']
        for item in team_comparison_list
    )
    team_set_identical = first_team_set == second_team_set
    prediction_identical = (
        team_set_identical
        and row_count_identical
        and all(
            field_mismatch_total[field_name] == 0
            for field_name in (*TRIAL_IDENTITY_FIELDS, 'true_label', *PREDICTION_FIELDS)
        )
    )
    accuracy_identical = team_set_identical and all(
        item['accuracy_identical'] for item in team_comparison_list
    )
    score_identical = team_set_identical and all(
        item['score_identical'] for item in team_comparison_list
    )
    comparable = not any(issue['severity'] == 'blocker' for issue in issue_list)

    if not comparable:
        verdict = 'not_comparable'
    elif prediction_identical and accuracy_identical and score_identical:
        verdict = 'reproducible'
    elif prediction_identical and accuracy_identical:
        verdict = 'prediction_reproducible_timing_score_varies'
    else:
        verdict = 'prediction_or_accuracy_mismatch'

    return {
        'verdict': verdict,
        'comparable': comparable,
        'prediction_identical': prediction_identical,
        'accuracy_identical': accuracy_identical,
        'score_identical': score_identical,
        'first': first_summary,
        'second': second_summary,
        'first_team_count': len(first_team_set),
        'second_team_count': len(second_team_set),
        'common_team_count': len(common_team_list),
        'field_mismatch_total': field_mismatch_total,
        'issues': issue_list,
        'teams': team_comparison_list,
    }


def _manifest_summary(manifest: dict) -> dict:
    applied_recovery = manifest.get('applied_recovery') or {}
    provenance = manifest.get('run_provenance') or {}
    match_start_mode = str(manifest.get('match_start_mode') or '').strip()
    recovery_mode = str(applied_recovery.get('recovery_mode') or '').strip()
    derived_run_kind = (
        'clean_full_run'
        if match_start_mode == 'clear' and recovery_mode == 'clear_start'
        else 'recovery_run'
    )
    stage = applied_recovery.get('stage') if isinstance(applied_recovery, dict) else None
    return {
        'match_start_mode': match_start_mode,
        'recovery_mode': recovery_mode,
        'run_kind': str(provenance.get('run_kind') or derived_run_kind),
        'recovery_stage': stage,
        'processor_component_id_list': sorted(
            str(item) for item in manifest.get('processor_component_id_list') or []
        ),
        'project_root': provenance.get('project_root') or _project_root_from_manifest(manifest),
        'git_revision': provenance.get('git_revision'),
        'git_tracked_dirty': provenance.get('git_tracked_dirty'),
        'config_sha256_by_path': provenance.get('config_sha256_by_path') or {},
    }


def _check_run_provenance(first: dict, second: dict, issue_list: list[dict]) -> None:
    for label, summary in (('first', first), ('second', second)):
        if summary['run_kind'] != 'clean_full_run':
            _add_issue(
                issue_list,
                'blocker',
                'not_clean_full_run',
                run=label,
                match_start_mode=summary['match_start_mode'],
                recovery_mode=summary['recovery_mode'],
                recovery_stage=summary['recovery_stage'],
            )

    if first['processor_component_id_list'] != second['processor_component_id_list']:
        _add_issue(
            issue_list,
            'blocker',
            'processor_set_mismatch',
            first=first['processor_component_id_list'],
            second=second['processor_component_id_list'],
        )

    first_revision = first.get('git_revision')
    second_revision = second.get('git_revision')
    if first_revision and second_revision and first_revision != second_revision:
        _add_issue(
            issue_list,
            'blocker',
            'git_revision_mismatch',
            first=first_revision,
            second=second_revision,
        )
    elif not first_revision or not second_revision:
        _add_issue(issue_list, 'blocker', 'git_revision_unavailable')

    first_hashes = first.get('config_sha256_by_path') or {}
    second_hashes = second.get('config_sha256_by_path') or {}
    if first_hashes and second_hashes and first_hashes != second_hashes:
        _add_issue(
            issue_list,
            'blocker',
            'configuration_fingerprint_mismatch',
            first=first_hashes,
            second=second_hashes,
        )
    elif not first_hashes or not second_hashes:
        _add_issue(issue_list, 'blocker', 'configuration_fingerprint_unavailable')

    if first.get('project_root') != second.get('project_root'):
        _add_issue(
            issue_list,
            'warning',
            'project_root_mismatch',
            first=first.get('project_root'),
            second=second.get('project_root'),
        )


def _compare_team(
    first_root: Path,
    second_root: Path,
    team_id: str,
    first_score_by_team: dict[str, dict],
    second_score_by_team: dict[str, dict],
) -> dict:
    first_rows = _read_csv(first_root / team_id / '03_trial_records.csv')
    second_rows = _read_csv(second_root / team_id / '03_trial_records.csv')
    field_mismatch_count = {
        field_name: 0
        for field_name in (*TRIAL_IDENTITY_FIELDS, 'true_label', *PREDICTION_FIELDS, 'predict_time_ms')
    }
    first_difference = None
    for row_index, (first_row, second_row) in enumerate(zip(first_rows, second_rows), start=1):
        for field_name in field_mismatch_count:
            if _normalized_value(first_row.get(field_name)) == _normalized_value(second_row.get(field_name)):
                continue
            field_mismatch_count[field_name] += 1
            if first_difference is None:
                first_difference = {
                    'row_index': row_index,
                    'field': field_name,
                    'first': first_row.get(field_name),
                    'second': second_row.get(field_name),
                    'identity': {
                        key: first_row.get(key) for key in TRIAL_IDENTITY_FIELDS
                    },
                }

    first_score_row = first_score_by_team[team_id]
    second_score_row = second_score_by_team[team_id]
    first_score = _as_float(first_score_row.get('total_score'))
    second_score = _as_float(second_score_row.get('total_score'))
    first_accuracy = _as_float(first_score_row.get('mean_accuracy_percent'))
    second_accuracy = _as_float(second_score_row.get('mean_accuracy_percent'))
    return {
        'team_id': team_id,
        'first_trial_count': len(first_rows),
        'second_trial_count': len(second_rows),
        'field_mismatch_count': field_mismatch_count,
        'first_difference': first_difference,
        'first_total_score': first_score,
        'second_total_score': second_score,
        'total_score_delta': second_score - first_score,
        'score_identical': math.isclose(first_score, second_score, rel_tol=0.0, abs_tol=1e-12),
        'first_accuracy_percent': first_accuracy,
        'second_accuracy_percent': second_accuracy,
        'accuracy_delta': second_accuracy - first_accuracy,
        'accuracy_identical': math.isclose(first_accuracy, second_accuracy, rel_tol=0.0, abs_tol=1e-12),
    }


def _load_score_by_team(results_root: Path) -> dict[str, dict]:
    row_list = _read_csv(results_root / '00_team_score_overview.csv')
    score_by_team = {}
    for row in row_list:
        team_id = str(row.get('team_id') or '').strip()
        if not team_id:
            raise ComparisonInputError(f'scoreboard row has empty team_id: {results_root}')
        if team_id in score_by_team:
            raise ComparisonInputError(f'duplicate scoreboard team_id={team_id}: {results_root}')
        score_by_team[team_id] = row
    return score_by_team


def _read_csv(file_path: Path) -> list[dict]:
    if not file_path.is_file():
        raise ComparisonInputError(f'required CSV not found: {file_path}')
    with file_path.open('r', encoding='utf-8-sig', newline='') as file:
        return list(csv.DictReader(file))


def _read_json(file_path: Path) -> dict:
    if not file_path.is_file():
        raise ComparisonInputError(f'required manifest not found: {file_path}')
    try:
        payload = json.loads(file_path.read_text(encoding='utf-8-sig'))
    except json.JSONDecodeError as exc:
        raise ComparisonInputError(f'invalid JSON manifest: {file_path}: {exc}') from exc
    if not isinstance(payload, dict):
        raise ComparisonInputError(f'manifest is not an object: {file_path}')
    return payload


def _project_root_from_manifest(manifest: dict) -> str | None:
    process_manifest_path = str(manifest.get('judge_process_manifest_path') or '').strip()
    if not process_manifest_path:
        return None
    path = Path(process_manifest_path)
    try:
        return str(path.parents[2])
    except IndexError:
        return None


def _normalized_value(value) -> str:
    return str(value or '').strip()


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _add_issue(issue_list: list[dict], severity: str, code: str, **detail) -> None:
    issue_list.append({'severity': severity, 'code': code, **detail})


def format_text_report(report: dict) -> str:
    mismatch = report['field_mismatch_total']
    lines = [
        f"verdict={report['verdict']}",
        (
            f"teams first={report['first_team_count']} second={report['second_team_count']} "
            f"common={report['common_team_count']}"
        ),
        (
            f"trial mismatches identity={sum(mismatch[field] for field in TRIAL_IDENTITY_FIELDS)} "
            f"truth={mismatch['true_label']} prediction={mismatch['predict_label']} "
            f"timeout={mismatch['is_timeout']} report_position={mismatch['report_position']} "
            f"timing={mismatch['predict_time_ms']}"
        ),
        (
            f"identical prediction={report['prediction_identical']} "
            f"accuracy={report['accuracy_identical']} score={report['score_identical']}"
        ),
    ]
    for issue in report['issues']:
        lines.append(f"{issue['severity']}:{issue['code']} {json.dumps(issue, ensure_ascii=False)}")
    return '\n'.join(lines)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Compare two competition result roots after validating run provenance.'
    )
    parser.add_argument('first_results_root', type=Path)
    parser.add_argument('second_results_root', type=Path)
    parser.add_argument('--json', action='store_true', dest='as_json')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    try:
        report = compare_full_run_results(
            args.first_results_root,
            args.second_results_root,
        )
    except ComparisonInputError as exc:
        print(f'input_error={exc}')
        return 2
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(format_text_report(report))
    return 0 if report['verdict'] == 'reproducible' else 1


if __name__ == '__main__':
    raise SystemExit(main())
