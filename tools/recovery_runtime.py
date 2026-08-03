import csv
import json
import os
import shutil
import tempfile
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml

from tools.results_archive import archive_results_snapshot, verify_archive_manifest
from tools.results_snapshot import (
    SUBJECT_TASK_FIELDS as SNAPSHOT_SUBJECT_TASK_FIELDS,
    TASK_FIELDS as SNAPSHOT_TASK_FIELDS,
    TEAM_FIELDS as SNAPSHOT_TEAM_FIELDS,
    TRIAL_FIELDS as SNAPSHOT_TRIAL_FIELDS,
)
from tools.runtime_state_sqlite import (
    export_team_score_overview_csv,
    replace_team_subject_task_overview_rows,
    replace_team_task_overview_rows,
    replace_team_trial_record_rows,
    resolve_runtime_state_db_path,
    write_team_overview_row,
    write_team_score_overview_row,
)


RECOVERY_REQUEST_FILE_NAME = 'recovery_request.json'
APPLIED_RECOVERY_FILE_NAME = 'applied_recovery.json'
LEGACY_RESUME_REQUEST_FILE_NAME = 'resume_request.json'
LEGACY_RESTART_STAGE_REQUEST_FILE_NAME = 'restart_stage_request.json'
STALE_CONTROL_FILE_NAME_LIST = [
    'pause_request.json',
    'resume_control_request.json',
    'start_match_request.json',
    'clear_results.json',
    RECOVERY_REQUEST_FILE_NAME,
    LEGACY_RESUME_REQUEST_FILE_NAME,
    LEGACY_RESTART_STAGE_REQUEST_FILE_NAME,
]
TEAM_OVERVIEW_FIELDNAMES = list(SNAPSHOT_TEAM_FIELDS)
TASK_OVERVIEW_FIELDNAMES = list(SNAPSHOT_TASK_FIELDS)
SUBJECT_TASK_OVERVIEW_FIELDNAMES = list(SNAPSHOT_SUBJECT_TASK_FIELDS)
TRIAL_RECORD_FIELDNAMES = list(SNAPSHOT_TRIAL_FIELDS)


def load_yaml_file(file_path: Path) -> dict:
    if not file_path.exists():
        return {}
    with file_path.open('r', encoding='utf-8') as file:
        return yaml.safe_load(file) or {}


def normalize_stage_payload(payload, *, require_session_id: bool = True) -> dict | None:
    if not isinstance(payload, dict):
        return None
    subject_id = str(payload.get('subject_id') or '').strip()
    exp_name = str(payload.get('exp_name') or '').strip()
    exp_task = str(payload.get('exp_task') or '').strip()
    session_id = str(payload.get('session_id') or '').strip()
    if (
        subject_id == ''
        or exp_name == ''
        or exp_task == ''
        or (require_session_id and session_id == '')
    ):
        return None
    normalized_payload = {
        'subject_id': subject_id,
        'exp_name': exp_name,
        'exp_task': exp_task,
    }
    if session_id:
        normalized_payload['session_id'] = session_id
    return normalized_payload


def build_checkpoint_id(stage_payload: dict | None) -> str:
    normalized_stage_payload = normalize_stage_payload(stage_payload)
    if normalized_stage_payload is None:
        return ''
    return (
        f"{normalized_stage_payload['subject_id']}|"
        f"{normalized_stage_payload['exp_name']}|"
        f"{normalized_stage_payload['exp_task']}|"
        f"{normalized_stage_payload['session_id']}"
    )


def resolve_virtual_receiver_config_path(project_root: Path) -> Path:
    return (
        project_root
        / 'app'
        / 'Collector'
        / 'Collector'
        / 'receiver'
        / 'virtual_receiver'
        / 'VirtualReceiverConfig.yml'
    )


def resolve_mi_challenge_config_path(project_root: Path) -> Path:
    return (
        project_root
        / 'app'
        / 'ProcessHub'
        / 'ProcessHub'
        / 'bci_competition'
        / 'challenge'
        / 'MI'
        / 'ChallengeMI.yml'
    )


def resolve_session_id(file_path: str) -> str:
    file_path_text = str(file_path or '')
    parent_name = Path(file_path_text).parent.name
    if parent_name:
        return parent_name
    return 'session1'


def load_stage_catalog(project_root: Path) -> list[dict]:
    virtual_receiver_config = load_yaml_file(resolve_virtual_receiver_config_path(project_root))
    configured_exp_task_order = (
        (((virtual_receiver_config.get('device_info') or {}).get('other_information')) or {}).get('exp_task_order')
        or ['left_vs_rest', 'right_vs_rest']
    )
    exp_task_order = [
        str(exp_task).strip()
        for exp_task in configured_exp_task_order
        if str(exp_task).strip()
    ] or ['left_vs_rest', 'right_vs_rest']
    data_files_dict = virtual_receiver_config.get('data_files') or {}

    stage_catalog: list[dict] = []
    for subject_id, exp_files_dict in data_files_dict.items():
        if not isinstance(exp_files_dict, dict):
            continue
        subject_block_id = 0
        visited_runtime_stage_key_set: set[tuple[str, str, str, str]] = set()
        for exp_name, file_path_list in exp_files_dict.items():
            if not isinstance(file_path_list, list):
                continue
            for file_path in file_path_list:
                session_id = resolve_session_id(file_path)
                for exp_task in exp_task_order:
                    runtime_stage_key = (str(subject_id), str(exp_name), str(exp_task), str(session_id))
                    if runtime_stage_key in visited_runtime_stage_key_set:
                        continue
                    visited_runtime_stage_key_set.add(runtime_stage_key)
                    subject_block_id += 1
                    stage_catalog.append(
                        {
                            'subject_id': str(subject_id),
                            'exp_name': str(exp_name),
                            'exp_task': str(exp_task),
                            'task_id': f'{exp_name}_{exp_task}',
                            'session_id': str(session_id),
                            'block_id': subject_block_id,
                            'checkpoint_id': (
                                f'{subject_id}|{exp_name}|{exp_task}|{session_id}'
                            ),
                        }
                    )
    return stage_catalog


def load_configured_task_order(project_root: Path) -> list[str]:
    challenge_config = load_yaml_file(resolve_mi_challenge_config_path(project_root))
    baseline_score_dict = ((challenge_config.get('score_config') or {}).get('task_baseline_score')) or {}
    task_order = [str(task_id).strip() for task_id in baseline_score_dict.keys() if str(task_id).strip()]
    if task_order:
        return task_order
    task_order = []
    for stage in load_stage_catalog(project_root):
        task_id = str(stage.get('task_id') or '').strip()
        if task_id and task_id not in task_order:
            task_order.append(task_id)
    return task_order


def resolve_stage_payload(project_root: Path, stage_payload: dict) -> dict:
    relaxed_stage_payload = normalize_stage_payload(
        stage_payload,
        require_session_id=False,
    )
    if relaxed_stage_payload is None:
        raise ValueError('缺少合法的恢复阶段')

    matching_stage_list = []
    for stage_catalog_row in load_stage_catalog(project_root):
        if any(
            str(stage_catalog_row.get(field_name) or '') != relaxed_stage_payload[field_name]
            for field_name in ('subject_id', 'exp_name', 'exp_task')
        ):
            continue
        requested_session_id = relaxed_stage_payload.get('session_id')
        if requested_session_id and str(stage_catalog_row.get('session_id') or '') != requested_session_id:
            continue
        matching_stage_list.append(stage_catalog_row)

    if not matching_stage_list:
        stage_text = ' / '.join(
            str(relaxed_stage_payload.get(field_name) or '')
            for field_name in ('subject_id', 'exp_name', 'exp_task', 'session_id')
            if str(relaxed_stage_payload.get(field_name) or '')
        )
        raise ValueError(f'指定恢复阶段不存在于配置阶段列表中: {stage_text}')
    if len(matching_stage_list) > 1:
        raise ValueError(
            '旧恢复请求缺少 session_id，无法唯一定位阶段，请由裁判重新选择具体 session_id: '
            f"{relaxed_stage_payload['subject_id']} / "
            f"{relaxed_stage_payload['exp_name']} / "
            f"{relaxed_stage_payload['exp_task']}"
        )

    matched_stage = matching_stage_list[0]
    return {
        'subject_id': str(matched_stage.get('subject_id') or ''),
        'exp_name': str(matched_stage.get('exp_name') or ''),
        'exp_task': str(matched_stage.get('exp_task') or ''),
        'session_id': str(matched_stage.get('session_id') or ''),
    }


def find_stage_selector(project_root: Path, stage_payload: dict) -> dict | None:
    try:
        normalized_stage_payload = resolve_stage_payload(project_root, stage_payload)
    except ValueError:
        return None
    checkpoint_id = build_checkpoint_id(normalized_stage_payload)
    for stage_catalog_row in load_stage_catalog(project_root):
        if stage_catalog_row.get('checkpoint_id') == checkpoint_id:
            return {
                'subject_id': stage_catalog_row.get('subject_id'),
                'exp_name': stage_catalog_row.get('exp_name'),
                'exp_task': stage_catalog_row.get('exp_task'),
                'task_id': stage_catalog_row.get('task_id'),
                'session_id': stage_catalog_row.get('session_id'),
                'block_id': stage_catalog_row.get('block_id'),
            }
    return None


def _read_json_file(file_path: Path) -> dict | None:
    if not file_path.exists():
        return None
    try:
        return json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None


def _build_restart_archive_reason(stage_payload: dict) -> str:
    return (
        f"restart_from_stage_{stage_payload['subject_id']}_"
        f"{stage_payload['exp_name']}_{stage_payload['exp_task']}_"
        f"{stage_payload['session_id']}"
    )


def _find_reusable_restart_archive(
    project_root: Path,
    archive_reason: str,
    requested_at,
) -> dict | None:
    """Find the verified pre-recovery snapshot for an interrupted retry."""
    try:
        requested_at_value = float(requested_at)
    except (TypeError, ValueError):
        return None
    if requested_at_value <= 0:
        return None

    history_root = project_root / 'results' / 'history'
    if not history_root.exists():
        return None
    candidate_root_list = sorted(
        (
            item
            for item in history_root.iterdir()
            if item.is_dir() and not item.name.startswith('.')
        ),
        key=lambda item: item.name,
        reverse=True,
    )
    for archive_root in candidate_root_list:
        manifest = _read_json_file(archive_root / 'manifest.json')
        if not isinstance(manifest, dict):
            continue
        if str(manifest.get('archive_reason') or '') != archive_reason:
            continue
        try:
            captured_at = float(manifest.get('captured_at') or manifest.get('archived_at') or 0.0)
        except (TypeError, ValueError):
            continue
        if captured_at < requested_at_value:
            continue
        try:
            verification = verify_archive_manifest(archive_root)
        except Exception:
            continue
        if verification.get('status') != 'ok':
            continue
        return manifest
    return None


def write_applied_recovery_manifest(control_root: Path, applied_recovery: dict) -> None:
    control_root.mkdir(parents=True, exist_ok=True)
    file_path = control_root / APPLIED_RECOVERY_FILE_NAME
    temp_file_path = file_path.with_suffix(file_path.suffix + '.tmp')
    temp_file_path.write_text(
        json.dumps(applied_recovery, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    temp_file_path.replace(file_path)


def clear_applied_recovery_manifest(control_root: Path) -> None:
    try:
        (control_root / APPLIED_RECOVERY_FILE_NAME).unlink(missing_ok=True)
    except OSError:
        return


def load_pending_recovery_request(control_root: Path) -> dict | None:
    candidate_list: list[dict] = []
    recovery_request_payload = _read_json_file(control_root / RECOVERY_REQUEST_FILE_NAME)
    if isinstance(recovery_request_payload, dict):
        payload = recovery_request_payload.get('payload') or {}
        recovery_mode = str(payload.get('recovery_mode') or '').strip()
        if recovery_mode in {'continue_from_checkpoint', 'restart_from_stage'}:
            candidate_list.append(
                {
                    'recovery_mode': recovery_mode,
                    'stage': normalize_stage_payload(
                        payload.get('stage'),
                        require_session_id=False,
                    ),
                    'requested_at': recovery_request_payload.get('requested_at'),
                }
            )

    legacy_resume_payload = _read_json_file(control_root / LEGACY_RESUME_REQUEST_FILE_NAME)
    if isinstance(legacy_resume_payload, dict):
        candidate_list.append(
            {
                'recovery_mode': 'continue_from_checkpoint',
                'stage': None,
                'requested_at': legacy_resume_payload.get('requested_at'),
            }
        )

    legacy_restart_payload = _read_json_file(control_root / LEGACY_RESTART_STAGE_REQUEST_FILE_NAME)
    if isinstance(legacy_restart_payload, dict):
        candidate_list.append(
            {
                'recovery_mode': 'restart_from_stage',
                'stage': normalize_stage_payload(
                    (legacy_restart_payload.get('payload') or {}),
                    require_session_id=False,
                ),
                'requested_at': legacy_restart_payload.get('requested_at'),
            }
        )

    if not candidate_list:
        return None
    candidate_list.sort(
        key=lambda item: float(item.get('requested_at') or 0.0),
        reverse=True,
    )
    for resolved_request in candidate_list:
        if resolved_request.get('recovery_mode') == 'restart_from_stage' and resolved_request.get('stage') is None:
            continue
        return resolved_request
    return None


def clear_stale_control_requests(control_root: Path) -> None:
    for file_name in STALE_CONTROL_FILE_NAME_LIST:
        file_path = control_root / file_name
        try:
            file_path.unlink(missing_ok=True)
        except OSError:
            continue


def _clear_live_root(live_root: Path) -> None:
    if not live_root.exists():
        live_root.mkdir(parents=True, exist_ok=True)
        return
    for child in live_root.iterdir():
        try:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink(missing_ok=True)
        except FileNotFoundError:
            continue
    live_root.mkdir(parents=True, exist_ok=True)


def _remove_runtime_state_db_files(project_root: Path) -> None:
    db_path = resolve_runtime_state_db_path(project_root)
    for suffix in ('', '-wal', '-shm'):
        try:
            Path(f'{db_path}{suffix}').unlink(missing_ok=True)
        except OSError:
            continue


def _read_csv_rows(file_path: Path) -> list[dict]:
    if not file_path.exists():
        return []
    with file_path.open('r', encoding='utf-8-sig', newline='') as csv_file:
        return list(csv.DictReader(csv_file))


def _write_csv_rows(file_path: Path, fieldnames: list[str], row_list: list[dict]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_file_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w',
            encoding='utf-8-sig',
            newline='',
            dir=file_path.parent,
            prefix=f'{file_path.name}.',
            suffix='.tmp',
            delete=False,
        ) as tmp_file:
            tmp_file_path = Path(tmp_file.name)
            writer = csv.DictWriter(tmp_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(row_list)
        os.replace(tmp_file_path, file_path)
    finally:
        if tmp_file_path is not None and tmp_file_path.exists():
            tmp_file_path.unlink(missing_ok=True)


def _to_float(value, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if parsed != parsed:
        return default
    return parsed


def _mean(number_list: list[float]) -> float:
    if not number_list:
        return 0.0
    return sum(number_list) / len(number_list)


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    value_text = str(value or '').strip().lower()
    return value_text in {'1', 'true', 'yes', 'y'}


def _rebuild_trial_rows(filtered_row_list: list[dict]) -> list[dict]:
    rebuilt_row_list: list[dict] = []
    task_trial_index_by_task_id: dict[str, int] = defaultdict(int)
    for team_trial_index, row in enumerate(filtered_row_list, start=1):
        task_id = str(row.get('task_id') or '').strip()
        task_trial_index_by_task_id[task_id] += 1
        rebuilt_row = dict(row)
        rebuilt_row['team_trial_index'] = team_trial_index
        rebuilt_row['task_trial_index'] = task_trial_index_by_task_id[task_id]
        rebuilt_row_list.append(rebuilt_row)
    return rebuilt_row_list


def _build_task_metric_summary(
    configured_task_id_list: list[str],
    trial_row_list: list[dict],
) -> tuple[list[dict], list[dict], dict]:
    task_row_list: list[dict] = []
    subject_task_row_list: list[dict] = []
    task_metric_by_task_id: dict[str, dict] = {}
    now_text = datetime.now().isoformat(timespec='seconds')
    trial_row_list_by_task_id: dict[str, list[dict]] = defaultdict(list)
    for row in trial_row_list:
        trial_row_list_by_task_id[str(row.get('task_id') or '').strip()].append(row)

    for task_id in configured_task_id_list:
        task_trial_row_list = trial_row_list_by_task_id.get(task_id, [])
        exp_name = None
        exp_task = None
        if task_trial_row_list:
            exp_name = task_trial_row_list[0].get('exp_name')
            exp_task = task_trial_row_list[0].get('exp_task')
        elif '_' in task_id:
            exp_name, exp_task = task_id.split('_', 1)

        correct_value_list: list[float] = []
        predict_time_value_list: list[float] = []
        subject_correct_value_list_by_subject_id: dict[str, list[float]] = defaultdict(list)

        for row in task_trial_row_list:
            if row.get('exp_name') not in (None, ''):
                exp_name = row.get('exp_name')
            if row.get('exp_task') not in (None, ''):
                exp_task = row.get('exp_task')
            correct_value = 1.0 if _to_bool(row.get('is_correct')) else 0.0
            correct_value_list.append(correct_value)
            subject_id = str(row.get('subject_id') or '').strip()
            if subject_id:
                subject_correct_value_list_by_subject_id[subject_id].append(correct_value)
            predict_time_ms = row.get('predict_time_ms')
            if predict_time_ms not in (None, ''):
                predict_time_value_list.append(_to_float(predict_time_ms))

        accuracy_percent = _mean(correct_value_list) * 100.0
        avg_reaction_time_ms = _mean(predict_time_value_list)
        task_score = _to_float(task_trial_row_list[-1].get('cumulative_score')) if task_trial_row_list else 0.0
        task_status = 'running' if task_trial_row_list else 'not_started'
        task_row = {
            'team_id': '',
            'task_id': task_id,
            'exp_name': exp_name,
            'exp_task': exp_task,
            'task_status': task_status,
            'updated_at': now_text,
            'subject_count': len(subject_correct_value_list_by_subject_id),
            'observed_trial_count': len(task_trial_row_list),
            'accuracy_percent': accuracy_percent,
            'avg_reaction_time_ms': avg_reaction_time_ms,
            'task_score': task_score,
        }
        task_row_list.append(task_row)
        task_metric_by_task_id[task_id] = task_row

        for subject_id in sorted(subject_correct_value_list_by_subject_id):
            subject_correct_value_list = subject_correct_value_list_by_subject_id[subject_id]
            subject_task_row_list.append(
                {
                    'team_id': '',
                    'subject_id': subject_id,
                    'task_id': task_id,
                    'exp_name': exp_name,
                    'exp_task': exp_task,
                    'task_status': 'running',
                    'updated_at': now_text,
                    'observed_trial_count': len(subject_correct_value_list),
                    'accuracy_percent': _mean(subject_correct_value_list) * 100.0,
                }
            )
    return task_row_list, subject_task_row_list, task_metric_by_task_id


def _resolve_team_overview_row(
    team_id: str,
    configured_task_id_list: list[str],
    task_row_list: list[dict],
    observed_trial_count: int,
    existing_team_overview_row: dict | None = None,
) -> dict:
    existing_team_overview_row = existing_team_overview_row or {}
    started_task_row_list = [
        task_row
        for task_row in task_row_list
        if int(task_row.get('observed_trial_count') or 0) > 0
    ]
    mean_source_task_row_list = started_task_row_list or task_row_list
    total_score = _mean([_to_float(task_row.get('task_score')) for task_row in mean_source_task_row_list])
    mean_accuracy_percent = _mean([_to_float(task_row.get('accuracy_percent')) for task_row in mean_source_task_row_list])
    avg_reaction_time_ms = _mean([_to_float(task_row.get('avg_reaction_time_ms')) for task_row in mean_source_task_row_list])
    return {
        'team_id': team_id,
        'total_score': total_score,
        'run_status': 'running' if started_task_row_list else 'idle',
        'updated_at': datetime.now().isoformat(timespec='seconds'),
        'global_seed': existing_team_overview_row.get('global_seed'),
        'collector_session_shuffle_seed': existing_team_overview_row.get(
            'collector_session_shuffle_seed'
        ),
        'observed_trial_count': observed_trial_count,
        'configured_task_count': len(configured_task_id_list),
        'started_task_count': len(started_task_row_list),
        'mean_accuracy_percent': mean_accuracy_percent,
        'avg_reaction_time_ms': avg_reaction_time_ms,
        'started_task_names': '|'.join(str(task_row.get('task_id') or '') for task_row in started_task_row_list),
    }


def _rewrite_team_result_dir(
    team_dir: Path,
    target_stage_index: int,
    stage_index_by_checkpoint_id: dict[str, int],
    configured_task_id_list: list[str],
    runtime_state_db_path: Path,
) -> dict:
    team_id = team_dir.name
    existing_team_overview_row_list = _read_csv_rows(team_dir / '00_team_overview.csv')
    existing_team_overview_row = existing_team_overview_row_list[0] if existing_team_overview_row_list else {}
    raw_trial_row_list = _read_csv_rows(team_dir / '03_trial_records.csv')
    filtered_trial_row_list = []
    for row in raw_trial_row_list:
        stage_payload = normalize_stage_payload(
            {
                'subject_id': row.get('subject_id'),
                'exp_name': row.get('exp_name'),
                'exp_task': row.get('exp_task'),
                'session_id': row.get('session_id'),
            }
        )
        checkpoint_id = build_checkpoint_id(stage_payload)
        stage_index = stage_index_by_checkpoint_id.get(checkpoint_id, -1)
        if stage_index < target_stage_index:
            filtered_trial_row_list.append(row)

    rebuilt_trial_row_list = _rebuild_trial_rows(filtered_trial_row_list)
    task_row_list, subject_task_row_list, _ = _build_task_metric_summary(
        configured_task_id_list,
        rebuilt_trial_row_list,
    )
    for task_row in task_row_list:
        task_row['team_id'] = team_id
    for subject_task_row in subject_task_row_list:
        subject_task_row['team_id'] = team_id

    team_overview_row = _resolve_team_overview_row(
        team_id,
        configured_task_id_list,
        task_row_list,
        len(rebuilt_trial_row_list),
        existing_team_overview_row,
    )

    _write_csv_rows(team_dir / '03_trial_records.csv', TRIAL_RECORD_FIELDNAMES, rebuilt_trial_row_list)
    _write_csv_rows(team_dir / '01_task_overview.csv', TASK_OVERVIEW_FIELDNAMES, task_row_list)
    _write_csv_rows(team_dir / '02_subject_task_overview.csv', SUBJECT_TASK_OVERVIEW_FIELDNAMES, subject_task_row_list)
    _write_csv_rows(team_dir / '00_team_overview.csv', TEAM_OVERVIEW_FIELDNAMES, [team_overview_row])

    task_trials_dir = team_dir / 'task_trials'
    if task_trials_dir.exists():
        shutil.rmtree(task_trials_dir, ignore_errors=True)
    task_trials_dir.mkdir(parents=True, exist_ok=True)
    task_row_list_by_task_id: dict[str, list[dict]] = defaultdict(list)
    for row in rebuilt_trial_row_list:
        task_row_list_by_task_id[str(row.get('task_id') or '').strip()].append(row)
    for task_id, row_list in task_row_list_by_task_id.items():
        _write_csv_rows(
            task_trials_dir / f'{task_id}_trial_records.csv',
            TRIAL_RECORD_FIELDNAMES,
            row_list,
        )

    replace_team_trial_record_rows(runtime_state_db_path, team_id, rebuilt_trial_row_list)
    replace_team_task_overview_rows(runtime_state_db_path, team_id, task_row_list)
    replace_team_subject_task_overview_rows(runtime_state_db_path, team_id, subject_task_row_list)
    write_team_overview_row(runtime_state_db_path, team_overview_row)
    write_team_score_overview_row(runtime_state_db_path, team_overview_row)
    return {
        'team_id': team_id,
        'observed_trial_count': len(rebuilt_trial_row_list),
        'started_task_count': int(team_overview_row.get('started_task_count') or 0),
        'target_stage_index': target_stage_index,
    }


def apply_restart_from_stage(
    project_root: Path,
    stage_payload: dict,
    *,
    requested_at=None,
) -> dict:
    normalized_stage_payload = resolve_stage_payload(project_root, stage_payload)

    stage_catalog = load_stage_catalog(project_root)
    stage_index_by_checkpoint_id = {
        str(stage_row.get('checkpoint_id')): index
        for index, stage_row in enumerate(stage_catalog)
    }
    checkpoint_id = build_checkpoint_id(normalized_stage_payload)
    if checkpoint_id not in stage_index_by_checkpoint_id:
        raise ValueError(
            '指定恢复阶段不存在于配置阶段列表中: '
            f"{normalized_stage_payload['subject_id']} / "
            f"{normalized_stage_payload['exp_name']} / "
            f"{normalized_stage_payload['exp_task']} / "
            f"{normalized_stage_payload['session_id']}"
        )

    configured_task_id_list = load_configured_task_order(project_root)
    results_root = project_root / 'results'
    live_root = results_root / 'live'
    control_root = results_root / 'control'
    runtime_state_db_path = resolve_runtime_state_db_path(project_root)
    results_root.mkdir(parents=True, exist_ok=True)
    live_root.mkdir(parents=True, exist_ok=True)
    control_root.mkdir(parents=True, exist_ok=True)

    archive_reason = _build_restart_archive_reason(normalized_stage_payload)
    history_archive = _find_reusable_restart_archive(
        project_root,
        archive_reason,
        requested_at,
    )
    history_archive_reused = history_archive is not None
    if history_archive is None:
        history_archive = archive_results_snapshot(project_root, archive_reason)
    _clear_live_root(live_root)
    _remove_runtime_state_db_files(project_root)

    team_summary_list = []
    for team_dir in sorted(
        item
        for item in results_root.iterdir()
        if item.is_dir() and item.name not in {'live', 'control', 'history'}
    ):
        team_summary_list.append(
            _rewrite_team_result_dir(
                team_dir=team_dir,
                target_stage_index=stage_index_by_checkpoint_id[checkpoint_id],
                stage_index_by_checkpoint_id=stage_index_by_checkpoint_id,
                configured_task_id_list=configured_task_id_list,
                runtime_state_db_path=runtime_state_db_path,
            )
        )

    export_team_score_overview_csv(
        runtime_state_db_path,
        results_root / '00_team_score_overview.csv',
        TEAM_OVERVIEW_FIELDNAMES,
    )

    stage_selector = find_stage_selector(project_root, normalized_stage_payload)
    applied_recovery = {
        'recovery_mode': 'restart_from_stage',
        'stage': normalized_stage_payload,
        'collector_start_selector': stage_selector,
        'team_summary_list': team_summary_list,
        'history_archive': history_archive,
        'history_archive_reused': history_archive_reused,
        'applied_at': time.time(),
    }
    if requested_at is not None:
        applied_recovery['requested_at'] = requested_at
    write_applied_recovery_manifest(control_root, applied_recovery)
    clear_stale_control_requests(control_root)
    return applied_recovery


def prepare_resume_recovery(project_root: Path) -> dict:
    results_root = project_root / 'results'
    control_root = results_root / 'control'
    results_root.mkdir(parents=True, exist_ok=True)
    control_root.mkdir(parents=True, exist_ok=True)

    pending_recovery_request = load_pending_recovery_request(control_root)
    if pending_recovery_request is None:
        clear_stale_control_requests(control_root)
        clear_applied_recovery_manifest(control_root)
        return {
            'recovery_mode': 'continue_from_checkpoint',
            'stage': None,
            'collector_start_selector': None,
            'requested_at': None,
            'applied_at': time.time(),
        }

    recovery_mode = pending_recovery_request.get('recovery_mode')
    if recovery_mode == 'restart_from_stage':
        applied_recovery = apply_restart_from_stage(
            project_root,
            pending_recovery_request.get('stage'),
            requested_at=pending_recovery_request.get('requested_at'),
        )
        applied_recovery['requested_at'] = pending_recovery_request.get('requested_at')
        return applied_recovery

    clear_stale_control_requests(control_root)
    clear_applied_recovery_manifest(control_root)
    return {
        'recovery_mode': 'continue_from_checkpoint',
        'stage': pending_recovery_request.get('stage'),
        'collector_start_selector': None,
        'requested_at': pending_recovery_request.get('requested_at'),
        'applied_at': time.time(),
    }
