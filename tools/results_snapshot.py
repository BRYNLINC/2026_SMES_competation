from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.runtime_state_sqlite import resolve_runtime_state_db_path


ARCHIVE_DIR_NAME = 'history'
ARCHIVE_SCHEMA_VERSION = 2
DB_AUXILIARY_SUFFIX_LIST = ('-wal', '-shm')
MTIME_TOLERANCE_NS = 2_000_000_000

TEAM_FIELDS = [
    'team_id', 'total_score', 'run_status', 'updated_at', 'global_seed',
    'collector_session_shuffle_seed', 'observed_trial_count',
    'configured_task_count', 'started_task_count', 'mean_accuracy_percent',
    'avg_reaction_time_ms', 'started_task_names',
]
TASK_FIELDS = [
    'team_id', 'task_id', 'exp_name', 'exp_task', 'task_status', 'updated_at',
    'subject_count', 'observed_trial_count', 'accuracy_percent',
    'avg_reaction_time_ms', 'task_score',
]
SUBJECT_TASK_FIELDS = [
    'team_id', 'subject_id', 'task_id', 'exp_name', 'exp_task', 'task_status',
    'updated_at', 'observed_trial_count', 'accuracy_percent',
]
TRIAL_FIELDS = [
    'team_id', 'team_trial_index', 'task_trial_index', 'subject_id', 'task_id',
    'exp_name', 'exp_task', 'session_id', 'block_id', 'trial_id', 'true_label',
    'raw_predict_label', 'predict_label', 'is_correct', 'trial_score',
    'is_timeout', 'is_invalid_output', 'judge_message', 'predict_time_ms',
    'cumulative_accuracy_percent', 'cumulative_score', 'report_position',
]

ROOT_SPEC = (
    'team_score_overview',
    '00_team_score_overview.csv',
    ('team_id',),
    TEAM_FIELDS,
    'total_score DESC, team_id ASC',
)
TEAM_SPEC_LIST = [
    ('team_overview', '00_team_overview.csv', ('team_id',), TEAM_FIELDS, 'team_id ASC'),
    ('task_overview', '01_task_overview.csv', ('team_id', 'task_id'), TASK_FIELDS, 'task_id ASC'),
    (
        'subject_task_overview',
        '02_subject_task_overview.csv',
        ('team_id', 'subject_id', 'task_id'),
        SUBJECT_TASK_FIELDS,
        'task_id ASC, subject_id ASC',
    ),
    (
        'trial_record',
        '03_trial_records.csv',
        ('team_id', 'team_trial_index'),
        TRIAL_FIELDS,
        'team_trial_index ASC',
    ),
]


def _utc_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(timespec='milliseconds')


def _local_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).astimezone().isoformat(timespec='milliseconds')


def build_archive_name(reason: str, timestamp: float | None = None) -> str:
    normalized_reason = ''.join(
        character if character.isalnum() or character in {'-', '_'} else '_'
        for character in str(reason or 'snapshot').strip()
    ).strip('_') or 'snapshot'
    resolved_timestamp = time.time() if timestamp is None else float(timestamp)
    local_datetime = datetime.fromtimestamp(resolved_timestamp)
    return (
        f"{local_datetime.strftime('%Y%m%d_%H%M%S')}_"
        f"{local_datetime.microsecond // 1000:03d}_{normalized_reason}"
    )


def iter_active_children(results_root: Path) -> list[Path]:
    if not results_root.exists():
        return []
    return sorted(
        (child for child in results_root.iterdir() if child.name != ARCHIVE_DIR_NAME),
        key=lambda child: child.name,
    )


def _path_has_payload(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_file():
        return path.stat().st_size > 0
    return next(path.iterdir(), None) is not None


def has_active_payload(project_root: Path) -> bool:
    return any(
        _path_has_payload(child)
        for child in iter_active_children(project_root / 'results')
    )


def _source_metadata(results_root: Path) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for child in iter_active_children(results_root):
        file_list = [child] if child.is_file() else sorted(
            path for path in child.rglob('*') if path.is_file()
        )
        for file_path in file_list:
            stat_result = file_path.stat()
            metadata[file_path.relative_to(results_root).as_posix()] = {
                'size_bytes': stat_result.st_size,
                'modified_at_ns': stat_result.st_mtime_ns,
                'modified_at_utc': _utc_iso(stat_result.st_mtime),
            }
    return metadata


def _is_database_artifact(path: Path, database_path: Path) -> bool:
    return path == database_path or any(
        path == Path(f'{database_path}{suffix}')
        for suffix in DB_AUXILIARY_SUFFIX_LIST
    )


def _copy_non_database_payload(
    results_root: Path,
    snapshot_results_root: Path,
    database_path: Path,
) -> None:
    for child in iter_active_children(results_root):
        if _is_database_artifact(child, database_path):
            continue
        destination = snapshot_results_root / child.name
        if child.is_dir():
            shutil.copytree(child, destination, copy_function=shutil.copy)
        else:
            shutil.copy(child, destination)


def _backup_database(source_path: Path, destination_path: Path) -> list[str]:
    temporary_path = destination_path.with_name(f'.{destination_path.name}.snapshot.tmp')
    for suffix in ('', *DB_AUXILIARY_SUFFIX_LIST):
        Path(f'{temporary_path}{suffix}').unlink(missing_ok=True)
    source_connection: sqlite3.Connection | None = None
    destination_connection: sqlite3.Connection | None = None
    integrity_result: list[str] = []
    try:
        source_connection = sqlite3.connect(source_path, timeout=30.0)
        source_connection.execute('PRAGMA busy_timeout=30000')
        destination_connection = sqlite3.connect(temporary_path, timeout=30.0)
        source_connection.backup(destination_connection, pages=1024, sleep=0.05)
        destination_connection.execute('PRAGMA journal_mode=DELETE')
        destination_connection.commit()
        integrity_result = [
            str(row[0])
            for row in destination_connection.execute('PRAGMA integrity_check').fetchall()
        ]
    finally:
        if destination_connection is not None:
            destination_connection.close()
        if source_connection is not None:
            source_connection.close()
    if integrity_result != ['ok']:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(f'SQLite backup integrity_check failed: {integrity_result}')
    os.replace(temporary_path, destination_path)
    for suffix in DB_AUXILIARY_SUFFIX_LIST:
        Path(f'{temporary_path}{suffix}').unlink(missing_ok=True)
        Path(f'{destination_path}{suffix}').unlink(missing_ok=True)
    return integrity_result


def _normalize_task_trial_indices(database_path: Path) -> int:
    connection = sqlite3.connect(database_path, timeout=30.0)
    try:
        if not _table_exists(connection, 'trial_record'):
            return 0
        row_list = connection.execute(
            '''
            SELECT team_id, team_trial_index, task_id, task_trial_index, payload_json
            FROM trial_record
            ORDER BY team_id ASC, team_trial_index ASC
            '''
        ).fetchall()
        next_index_by_team_task: dict[tuple[str, str], int] = {}
        update_row_list: list[tuple[int, str, str, int]] = []
        for team_id, team_trial_index, task_id, stored_task_trial_index, payload_json in row_list:
            counter_key = (str(team_id), str(task_id or ''))
            expected_task_trial_index = next_index_by_team_task.get(counter_key, 0) + 1
            next_index_by_team_task[counter_key] = expected_task_trial_index
            try:
                payload = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f'invalid trial_record payload_json for team={team_id} '
                    f'team_trial_index={team_trial_index}'
                ) from exc
            if not isinstance(payload, dict):
                raise RuntimeError(
                    f'non-object trial_record payload_json for team={team_id} '
                    f'team_trial_index={team_trial_index}'
                )
            if (
                int(stored_task_trial_index or 0) == expected_task_trial_index
                and int(payload.get('task_trial_index') or 0) == expected_task_trial_index
            ):
                continue
            payload['task_trial_index'] = expected_task_trial_index
            update_row_list.append((
                expected_task_trial_index,
                json.dumps(payload, ensure_ascii=False),
                str(team_id),
                int(team_trial_index),
            ))
        connection.executemany(
            '''
            UPDATE trial_record
            SET task_trial_index = ?, payload_json = ?
            WHERE team_id = ? AND team_trial_index = ?
            ''',
            update_row_list,
        )
        connection.commit()
        return len(update_row_list)
    finally:
        connection.close()


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone() is not None


def _payload_rows(
    connection: sqlite3.Connection,
    table_name: str,
    order_by: str,
    team_id: str | None = None,
) -> list[dict]:
    if not _table_exists(connection, table_name):
        return []
    where_sql = '' if team_id is None else ' WHERE team_id=?'
    parameters: tuple[Any, ...] = () if team_id is None else (team_id,)
    payload_list: list[dict] = []
    for row in connection.execute(
        f'SELECT payload_json FROM {table_name}{where_sql} ORDER BY {order_by}',
        parameters,
    ).fetchall():
        try:
            payload = json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            payload_list.append(payload)
    return payload_list


def _write_csv(path: Path, default_fields: list[str], row_list: list[dict]) -> None:
    field_list = list(default_fields)
    for row in row_list:
        for field in row:
            if field not in field_list:
                field_list.append(field)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f'.{path.name}.tmp')
    try:
        with temporary_path.open('w', encoding='utf-8-sig', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=field_list)
            writer.writeheader()
            writer.writerows(row_list)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _regenerate_csv_from_database(snapshot_results_root: Path) -> dict[str, int]:
    database_path = snapshot_results_root / 'runtime_state.db'
    connection = sqlite3.connect(database_path)
    try:
        root_rows = _payload_rows(connection, ROOT_SPEC[0], ROOT_SPEC[4])
        _write_csv(snapshot_results_root / ROOT_SPEC[1], ROOT_SPEC[3], root_rows)
        all_rows_by_table = {
            spec[0]: _payload_rows(connection, spec[0], spec[4])
            for spec in TEAM_SPEC_LIST
        }
        team_id_set = {
            str(row.get('team_id') or '').strip()
            for row_list in all_rows_by_table.values()
            for row in row_list
            if str(row.get('team_id') or '').strip()
        }
        for team_id in sorted(team_id_set):
            team_dir = snapshot_results_root / team_id
            trial_rows: list[dict] = []
            for table_name, file_name, _, fields, _ in TEAM_SPEC_LIST:
                row_list = [
                    row
                    for row in all_rows_by_table[table_name]
                    if str(row.get('team_id') or '').strip() == team_id
                ]
                _write_csv(team_dir / file_name, fields, row_list)
                if table_name == 'trial_record':
                    trial_rows = row_list
            task_trials_dir = team_dir / 'task_trials'
            if task_trials_dir.exists():
                shutil.rmtree(task_trials_dir)
            rows_by_task_id: dict[str, list[dict]] = {}
            for row in trial_rows:
                task_id = str(row.get('task_id') or '').strip()
                if task_id:
                    rows_by_task_id.setdefault(task_id, []).append(row)
            for task_id, task_rows in sorted(rows_by_task_id.items()):
                _write_csv(
                    task_trials_dir / f'{task_id}_trial_records.csv',
                    TRIAL_FIELDS,
                    task_rows,
                )
        return {
            ROOT_SPEC[0]: len(root_rows),
            **{table_name: len(rows) for table_name, rows in all_rows_by_table.items()},
        }
    finally:
        connection.close()


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open('r', encoding='utf-8-sig', newline='') as file:
        reader = csv.DictReader(file)
        return list(reader.fieldnames or []), list(reader)


def _key(row: dict, key_fields: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        '' if row.get(field) is None else str(row.get(field))
        for field in key_fields
    )


def _compare_keys(
    label: str,
    csv_path: Path,
    expected_rows: list[dict],
    key_fields: tuple[str, ...],
) -> list[str]:
    if not csv_path.exists():
        return [f'{label}: missing CSV']
    try:
        fields, actual_rows = _read_csv(csv_path)
    except (OSError, UnicodeError, csv.Error) as exc:
        return [f'{label}: unreadable CSV ({type(exc).__name__}: {exc})']
    issue_list = [
        f'{label}: missing key column {field}'
        for field in key_fields
        if field not in fields
    ]
    expected_by_key = {_key(row, key_fields): row for row in expected_rows}
    actual_by_key = {_key(row, key_fields): row for row in actual_rows}
    if len(actual_by_key) != len(actual_rows):
        issue_list.append(f'{label}: duplicate primary key')
    if set(expected_by_key) != set(actual_by_key):
        issue_list.append(
            f'{label}: key mismatch db={sorted(expected_by_key)} csv={sorted(actual_by_key)}'
        )
        return issue_list
    for row_key, expected_row in expected_by_key.items():
        actual_row = actual_by_key[row_key]
        for field, value in expected_row.items():
            if field not in fields:
                issue_list.append(f'{label}: missing payload column {field}')
                return issue_list
            expected_value = '' if value is None else str(value)
            actual_value = str(actual_row.get(field) or '')
            if expected_value != actual_value:
                issue_list.append(
                    f'{label}: value mismatch key={row_key} field={field} '
                    f'db={expected_value!r} csv={actual_value!r}'
                )
                return issue_list
    return issue_list


def _immutable_connection(database_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(
        f'{database_path.resolve().as_uri()}?mode=ro&immutable=1',
        uri=True,
        timeout=5.0,
    )


def inspect_snapshot(
    results_root: Path,
    expected_modified_at_ns: int | None = None,
) -> dict:
    issue_list: list[str] = []
    row_count_by_table: dict[str, int] = {}
    sqlite_result: list[str] = []
    rows_by_table: dict[str, list[dict]] = {}
    database_path = results_root / 'runtime_state.db'

    for csv_path in sorted(results_root.rglob('*.csv')) if results_root.exists() else []:
        try:
            fields, rows = _read_csv(csv_path)
            if not fields:
                issue_list.append(f'{csv_path.relative_to(results_root)}: missing header')
            if any(None in row for row in rows):
                issue_list.append(f'{csv_path.relative_to(results_root)}: column count mismatch')
        except (OSError, UnicodeError, csv.Error) as exc:
            issue_list.append(f'{csv_path.relative_to(results_root)}: unreadable ({exc})')

    if database_path.exists():
        for suffix in DB_AUXILIARY_SUFFIX_LIST:
            if Path(f'{database_path}{suffix}').exists():
                issue_list.append(f'unexpected SQLite sidecar: runtime_state.db{suffix}')
        try:
            connection = _immutable_connection(database_path)
            try:
                sqlite_result = [
                    str(row[0])
                    for row in connection.execute('PRAGMA integrity_check').fetchall()
                ]
                if sqlite_result != ['ok']:
                    issue_list.append(f'SQLite integrity_check failed: {sqlite_result}')
                for spec in (ROOT_SPEC, *TEAM_SPEC_LIST):
                    if not _table_exists(connection, spec[0]):
                        issue_list.append(f'SQLite table missing: {spec[0]}')
                        rows_by_table[spec[0]] = []
                    else:
                        rows_by_table[spec[0]] = _payload_rows(connection, spec[0], spec[4])
                    row_count_by_table[spec[0]] = len(rows_by_table[spec[0]])
            finally:
                connection.close()
        except sqlite3.DatabaseError as exc:
            issue_list.append(f'SQLite open/check failed: {type(exc).__name__}: {exc}')
    elif any(results_root.rglob('*.csv')):
        issue_list.append('runtime_state.db missing while CSV results exist')

    if rows_by_table:
        issue_list.extend(_compare_keys(
            ROOT_SPEC[1],
            results_root / ROOT_SPEC[1],
            rows_by_table.get(ROOT_SPEC[0], []),
            ROOT_SPEC[2],
        ))
        database_team_ids = {
            str(row.get('team_id') or '').strip()
            for row_list in rows_by_table.values()
            for row in row_list
            if str(row.get('team_id') or '').strip()
        }
        csv_team_ids = {
            path.parent.name
            for spec in TEAM_SPEC_LIST
            for path in results_root.glob(f'*/{spec[1]}')
        }
        csv_team_ids.update(
            path.parent.parent.name
            for path in results_root.glob('*/task_trials/*_trial_records.csv')
        )
        for team_id in sorted(database_team_ids | csv_team_ids):
            for spec in TEAM_SPEC_LIST:
                expected_rows = [
                    row
                    for row in rows_by_table.get(spec[0], [])
                    if str(row.get('team_id') or '').strip() == team_id
                ]
                issue_list.extend(_compare_keys(
                    f'{team_id}/{spec[1]}',
                    results_root / team_id / spec[1],
                    expected_rows,
                    spec[2],
                ))
            expected_by_task: dict[str, list[dict]] = {}
            for row in rows_by_table.get('trial_record', []):
                if str(row.get('team_id') or '').strip() != team_id:
                    continue
                task_id = str(row.get('task_id') or '').strip()
                if task_id:
                    expected_by_task.setdefault(task_id, []).append(row)
            task_dir = results_root / team_id / 'task_trials'
            actual_task_ids = {
                path.name.removesuffix('_trial_records.csv')
                for path in task_dir.glob('*_trial_records.csv')
            } if task_dir.exists() else set()
            for task_id in sorted(set(expected_by_task) | actual_task_ids):
                issue_list.extend(_compare_keys(
                    f'{team_id}/task_trials/{task_id}_trial_records.csv',
                    task_dir / f'{task_id}_trial_records.csv',
                    expected_by_task.get(task_id, []),
                    ('team_id', 'team_trial_index'),
                ))

    if expected_modified_at_ns is not None and results_root.exists():
        for file_path in sorted(path for path in results_root.rglob('*') if path.is_file()):
            if abs(file_path.stat().st_mtime_ns - expected_modified_at_ns) > MTIME_TOLERANCE_NS:
                issue_list.append(
                    f'{file_path.relative_to(results_root).as_posix()}: archive mtime mismatch'
                )

    return {
        'status': 'ok' if not issue_list else 'failed',
        'sqlite_integrity_check': sqlite_result,
        'database_row_count_by_table': row_count_by_table,
        'csv_file_count': len(list(results_root.rglob('*.csv'))) if results_root.exists() else 0,
        'issues': issue_list,
    }


def _normalize_mtime(results_root: Path, modified_at_ns: int) -> None:
    for path in sorted(results_root.rglob('*'), key=lambda item: len(item.parts), reverse=True):
        os.utime(path, ns=(modified_at_ns, modified_at_ns))
    os.utime(results_root, ns=(modified_at_ns, modified_at_ns))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _inventory(
    snapshot_results_root: Path,
    source_metadata: dict[str, dict[str, Any]],
) -> list[dict]:
    item_list: list[dict] = []
    for path in sorted(item for item in snapshot_results_root.rglob('*') if item.is_file()):
        relative_path = path.relative_to(snapshot_results_root).as_posix()
        stat_result = path.stat()
        source_item = source_metadata.get(relative_path)
        item_list.append({
            'path': relative_path,
            'size_bytes': stat_result.st_size,
            'sha256': _sha256(path),
            'archive_modified_at_ns': stat_result.st_mtime_ns,
            'archive_modified_at_utc': _utc_iso(stat_result.st_mtime),
            'source_modified_at_ns': source_item.get('modified_at_ns') if source_item else None,
            'source_modified_at_utc': source_item.get('modified_at_utc') if source_item else None,
        })
    return item_list


def _write_manifest(path: Path, payload: dict) -> None:
    temporary_path = path.with_name(f'.{path.name}.tmp')
    try:
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def archive_snapshot(project_root: Path, reason: str) -> dict | None:
    results_root = project_root / 'results'
    if not has_active_payload(project_root):
        return None

    captured_at_ns = time.time_ns()
    captured_at_ns -= captured_at_ns % 100
    captured_at = captured_at_ns / 1_000_000_000
    history_root = results_root / ARCHIVE_DIR_NAME
    history_root.mkdir(parents=True, exist_ok=True)
    archive_name = build_archive_name(reason, captured_at)
    archive_root = history_root / archive_name
    staging_root = history_root / f'.{archive_name}.{os.getpid()}.incomplete'
    if archive_root.exists() or staging_root.exists():
        raise FileExistsError(f'archive path already exists: {archive_root}')

    source_items = iter_active_children(results_root)
    source_metadata = _source_metadata(results_root)
    source_latest_ns = max(
        (item['modified_at_ns'] for item in source_metadata.values()),
        default=None,
    )
    snapshot_results_root = staging_root / 'results'
    database_path = resolve_runtime_state_db_path(project_root)
    try:
        snapshot_results_root.mkdir(parents=True, exist_ok=False)
        _copy_non_database_payload(results_root, snapshot_results_root, database_path)
        backup_integrity: list[str] = []
        normalized_task_trial_index_row_count = 0
        if database_path.exists():
            backup_integrity = _backup_database(
                database_path,
                snapshot_results_root / database_path.name,
            )
            normalized_task_trial_index_row_count = _normalize_task_trial_indices(
                snapshot_results_root / database_path.name
            )
            _regenerate_csv_from_database(snapshot_results_root)
        elif any(Path(f'{database_path}{suffix}').exists() for suffix in DB_AUXILIARY_SUFFIX_LIST):
            raise RuntimeError('SQLite sidecar exists without runtime_state.db')

        _normalize_mtime(snapshot_results_root, captured_at_ns)
        integrity = inspect_snapshot(snapshot_results_root, captured_at_ns)
        integrity['sqlite_backup_integrity_check'] = backup_integrity
        integrity['normalized_task_trial_index_row_count'] = normalized_task_trial_index_row_count
        if integrity['status'] != 'ok':
            raise RuntimeError(
                'results archive integrity validation failed: '
                + '; '.join(integrity['issues'][:10])
            )
        completed_at = time.time()
        manifest = {
            'archive_schema_version': ARCHIVE_SCHEMA_VERSION,
            'archive_reason': str(reason or 'snapshot'),
            'archived_at': captured_at,
            'captured_at': captured_at,
            'captured_at_utc': _utc_iso(captured_at),
            'captured_at_local': _local_iso(captured_at),
            'completed_at': completed_at,
            'completed_at_utc': _utc_iso(completed_at),
            'completed_at_local': _local_iso(completed_at),
            'source_results_root': str(results_root),
            'source_latest_modified_at_ns': source_latest_ns,
            'source_latest_modified_at_utc': (
                _utc_iso(source_latest_ns / 1_000_000_000)
                if source_latest_ns is not None else None
            ),
            'archive_root': str(archive_root),
            'archive_payload_modified_at_ns': captured_at_ns,
            'archive_payload_modified_at_utc': _utc_iso(captured_at),
            'source_item_name_list': [item.name for item in source_items],
            'archived_item_name_list': sorted(
                item.name for item in snapshot_results_root.iterdir()
            ),
            'file_inventory': _inventory(snapshot_results_root, source_metadata),
            'normalization': {
                'task_trial_index_scope': 'per_team_and_task',
                'rewritten_trial_record_count': normalized_task_trial_index_row_count,
            },
            'integrity': integrity,
        }
        _write_manifest(staging_root / 'manifest.json', manifest)
        os.replace(staging_root, archive_root)
        return manifest
    except Exception:
        if staging_root.exists():
            shutil.rmtree(staging_root, ignore_errors=True)
        raise


def verify_archive(archive_root: Path) -> dict:
    manifest_path = archive_root / 'manifest.json'
    if not manifest_path.exists():
        return {'status': 'failed', 'issues': ['manifest.json missing']}
    try:
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return {'status': 'failed', 'issues': [f'manifest unreadable: {exc}']}

    issue_list: list[str] = []
    if int(manifest.get('archive_schema_version') or 0) != ARCHIVE_SCHEMA_VERSION:
        issue_list.append(
            f"unsupported archive_schema_version={manifest.get('archive_schema_version')!r}"
        )
    snapshot_results_root = archive_root / 'results'
    inventory_by_path = {
        str(item.get('path') or ''): item
        for item in manifest.get('file_inventory') or []
        if str(item.get('path') or '')
    }
    actual_paths = {
        path.relative_to(snapshot_results_root).as_posix()
        for path in snapshot_results_root.rglob('*')
        if path.is_file()
    } if snapshot_results_root.exists() else set()
    if set(inventory_by_path) != actual_paths:
        issue_list.append('file inventory path set mismatch')
    for relative_path, item in inventory_by_path.items():
        path = snapshot_results_root / relative_path
        if not path.exists():
            continue
        stat_result = path.stat()
        if stat_result.st_size != int(item.get('size_bytes') or 0):
            issue_list.append(f'{relative_path}: size changed')
        if stat_result.st_mtime_ns != int(item.get('archive_modified_at_ns') or 0):
            issue_list.append(f'{relative_path}: modification time changed')
        if _sha256(path) != str(item.get('sha256') or ''):
            issue_list.append(f'{relative_path}: SHA-256 mismatch')
    expected_ns = manifest.get('archive_payload_modified_at_ns')
    integrity = inspect_snapshot(
        snapshot_results_root,
        int(expected_ns) if expected_ns is not None else None,
    )
    issue_list.extend(integrity['issues'])
    return {
        'status': 'ok' if not issue_list else 'failed',
        'archive_root': str(archive_root),
        'integrity': integrity,
        'issues': issue_list,
    }


def repair_finished_results(project_root: Path) -> dict:
    results_root = project_root / 'results'
    database_path = resolve_runtime_state_db_path(project_root)
    if not database_path.exists():
        raise FileNotFoundError(database_path)
    connection = sqlite3.connect(database_path, timeout=30.0)
    try:
        status_list = [
            str(json.loads(row[0]).get('run_status') or '').strip().lower()
            for row in connection.execute(
                'SELECT payload_json FROM team_score_overview'
            ).fetchall()
        ] if _table_exists(connection, 'team_score_overview') else []
    finally:
        connection.close()
    non_terminal_status_list = sorted({
        status
        for status in status_list
        if status not in {'finished', 'complete', 'completed'}
    })
    if non_terminal_status_list:
        raise RuntimeError(
            f'refusing to repair active results with run_status={non_terminal_status_list}'
        )
    rewritten_count = _normalize_task_trial_indices(database_path)
    row_count_by_table = _regenerate_csv_from_database(results_root)
    return {
        'status': 'ok',
        'run_status_list': status_list,
        'rewritten_trial_record_count': rewritten_count,
        'database_row_count_by_table': row_count_by_table,
    }


def clear_active(project_root: Path) -> None:
    results_root = project_root / 'results'
    history_root = results_root / ARCHIVE_DIR_NAME
    results_root.mkdir(parents=True, exist_ok=True)
    history_root.mkdir(parents=True, exist_ok=True)
    for child in iter_active_children(results_root):
        try:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink(missing_ok=True)
        except FileNotFoundError:
            continue


def archive_and_clear(project_root: Path, reason: str) -> dict | None:
    manifest = archive_snapshot(project_root, reason)
    clear_active(project_root)
    return manifest
