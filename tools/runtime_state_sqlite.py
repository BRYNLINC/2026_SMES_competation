import csv
import json
import math
import os
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Any


STATE_KEY_CURRENT_TRIAL = 'current_trial'
STATE_KEY_RUNTIME_STAGE_STATUS = 'runtime_stage_status'
STATE_KEY_MATCH_CONTROL_STATUS = 'match_control_status'
TEAM_STATE_KEY_PREFIX = 'team:'
SQLITE_CONNECTION_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30000
SQLITE_WRITE_RETRY_LIMIT = 5
SQLITE_WRITE_RETRY_DELAY_SECONDS = 0.2


def resolve_runtime_state_db_path(project_root: Path) -> Path:
    return project_root / 'results' / 'runtime_state.db'


def ensure_runtime_state_schema(db_path: Path) -> None:
    with _connect(db_path) as connection:
        connection.execute(
            '''
            CREATE TABLE IF NOT EXISTS json_state (
                state_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE IF NOT EXISTS team_score_overview (
                team_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                total_score REAL NOT NULL DEFAULT 0,
                run_status TEXT,
                updated_at_text TEXT,
                observed_trial_count INTEGER NOT NULL DEFAULT 0,
                configured_task_count INTEGER NOT NULL DEFAULT 0,
                started_task_count INTEGER NOT NULL DEFAULT 0,
                mean_accuracy_percent REAL NOT NULL DEFAULT 0,
                avg_reaction_time_ms REAL NOT NULL DEFAULT 0,
                started_task_names TEXT
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE IF NOT EXISTS team_overview (
                team_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                total_score REAL NOT NULL DEFAULT 0,
                run_status TEXT,
                updated_at_text TEXT,
                observed_trial_count INTEGER NOT NULL DEFAULT 0,
                configured_task_count INTEGER NOT NULL DEFAULT 0,
                started_task_count INTEGER NOT NULL DEFAULT 0,
                mean_accuracy_percent REAL NOT NULL DEFAULT 0,
                avg_reaction_time_ms REAL NOT NULL DEFAULT 0,
                started_task_names TEXT
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE IF NOT EXISTS task_overview (
                team_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                exp_name TEXT,
                exp_task TEXT,
                task_status TEXT,
                updated_at_text TEXT,
                subject_count INTEGER NOT NULL DEFAULT 0,
                observed_trial_count INTEGER NOT NULL DEFAULT 0,
                accuracy_percent REAL NOT NULL DEFAULT 0,
                avg_reaction_time_ms REAL NOT NULL DEFAULT 0,
                task_score REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (team_id, task_id)
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE IF NOT EXISTS subject_task_overview (
                team_id TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                exp_name TEXT,
                exp_task TEXT,
                task_status TEXT,
                updated_at_text TEXT,
                observed_trial_count INTEGER NOT NULL DEFAULT 0,
                accuracy_percent REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (team_id, subject_id, task_id)
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE IF NOT EXISTS trial_record (
                team_id TEXT NOT NULL,
                team_trial_index INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                task_trial_index INTEGER NOT NULL DEFAULT 0,
                task_id TEXT,
                subject_id TEXT,
                exp_name TEXT,
                exp_task TEXT,
                session_id TEXT,
                block_id TEXT,
                trial_id TEXT,
                true_label TEXT,
                predict_label TEXT,
                is_correct INTEGER NOT NULL DEFAULT 0,
                trial_score REAL NOT NULL DEFAULT 0,
                predict_time_ms REAL NOT NULL DEFAULT 0,
                cumulative_accuracy_percent REAL NOT NULL DEFAULT 0,
                cumulative_score REAL NOT NULL DEFAULT 0,
                is_timeout INTEGER NOT NULL DEFAULT 0,
                report_position TEXT,
                updated_at REAL NOT NULL,
                PRIMARY KEY (team_id, team_trial_index)
            )
            '''
        )
        _ensure_table_columns(
            connection,
            'task_overview',
            {
                'exp_name': 'TEXT',
                'exp_task': 'TEXT',
                'subject_count': 'INTEGER NOT NULL DEFAULT 0',
            },
        )
        _ensure_table_columns(
            connection,
            'subject_task_overview',
            {
                'exp_name': 'TEXT',
                'exp_task': 'TEXT',
            },
        )
        _ensure_table_columns(
            connection,
            'trial_record',
            {
                'task_trial_index': 'INTEGER NOT NULL DEFAULT 0',
                'true_label': 'TEXT',
                'predict_label': 'TEXT',
                'is_correct': 'INTEGER NOT NULL DEFAULT 0',
                'trial_score': 'REAL NOT NULL DEFAULT 0',
                'report_position': 'TEXT',
            },
        )
        connection.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_team_score_overview_rank
            ON team_score_overview (total_score DESC, team_id ASC)
            '''
        )
        connection.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_task_overview_team
            ON task_overview (team_id, task_id)
            '''
        )
        connection.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_subject_task_overview_team
            ON subject_task_overview (team_id, task_id, subject_id)
            '''
        )
        connection.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_trial_record_team
            ON trial_record (team_id, team_trial_index)
            '''
        )
        connection.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_trial_record_task
            ON trial_record (team_id, task_id, subject_id, session_id)
            '''
        )
        connection.commit()


def write_json_state(db_path: Path, state_key: str, payload: dict) -> None:
    ensure_runtime_state_schema(db_path)
    serialized_payload = json.dumps(payload, ensure_ascii=False)
    updated_at = _extract_updated_at(payload)
    with _connect(db_path) as connection:
        connection.execute(
            '''
            INSERT INTO json_state (state_key, payload_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            ''',
            (state_key, serialized_payload, updated_at),
        )
        connection.commit()


def read_json_state(db_path: Path, state_key: str) -> dict | None:
    if not db_path.exists():
        return None
    ensure_runtime_state_schema(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            'SELECT payload_json FROM json_state WHERE state_key = ?',
            (state_key,),
        ).fetchone()
    if row is None:
        return None
    return _deserialize_payload_row(row)


def list_json_state_by_prefix(db_path: Path, prefix: str) -> list[dict]:
    if not db_path.exists():
        return []
    ensure_runtime_state_schema(db_path)
    with _connect(db_path) as connection:
        row_list = connection.execute(
            '''
            SELECT payload_json
            FROM json_state
            WHERE state_key LIKE ?
            ORDER BY state_key ASC
            ''',
            (f'{prefix}%',),
        ).fetchall()
    return _deserialize_payload_rows(row_list)


def json_state_exists(db_path: Path, state_key: str) -> bool:
    if not db_path.exists():
        return False
    ensure_runtime_state_schema(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            'SELECT 1 FROM json_state WHERE state_key = ? LIMIT 1',
            (state_key,),
        ).fetchone()
    return row is not None


def count_json_state_by_prefix(db_path: Path, prefix: str) -> int:
    if not db_path.exists():
        return 0
    ensure_runtime_state_schema(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            'SELECT COUNT(1) AS count_value FROM json_state WHERE state_key LIKE ?',
            (f'{prefix}%',),
        ).fetchone()
    return int(row['count_value']) if row is not None else 0


def write_team_score_overview_row(db_path: Path, row: dict) -> None:
    team_id = str(row.get('team_id') or '').strip()
    if team_id == '':
        return
    serialized_payload = json.dumps(row, ensure_ascii=False)
    _run_write_transaction(
        db_path,
        lambda connection: connection.execute(
            '''
            INSERT INTO team_score_overview (
                team_id,
                payload_json,
                total_score,
                run_status,
                updated_at_text,
                observed_trial_count,
                configured_task_count,
                started_task_count,
                mean_accuracy_percent,
                avg_reaction_time_ms,
                started_task_names
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                total_score = excluded.total_score,
                run_status = excluded.run_status,
                updated_at_text = excluded.updated_at_text,
                observed_trial_count = excluded.observed_trial_count,
                configured_task_count = excluded.configured_task_count,
                started_task_count = excluded.started_task_count,
                mean_accuracy_percent = excluded.mean_accuracy_percent,
                avg_reaction_time_ms = excluded.avg_reaction_time_ms,
                started_task_names = excluded.started_task_names
            ''',
            (
                team_id,
                serialized_payload,
                _to_float(row.get('total_score')),
                str(row.get('run_status') or ''),
                _to_optional_str(row.get('updated_at')),
                int(_to_float(row.get('observed_trial_count'))),
                int(_to_float(row.get('configured_task_count'))),
                int(_to_float(row.get('started_task_count'))),
                _to_float(row.get('mean_accuracy_percent')),
                _to_float(row.get('avg_reaction_time_ms')),
                _to_optional_str(row.get('started_task_names')),
            ),
        ),
    )


def load_team_score_overview_rows(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    ensure_runtime_state_schema(db_path)
    with _connect(db_path) as connection:
        row_list = connection.execute(
            '''
            SELECT payload_json
            FROM team_score_overview
            ORDER BY total_score DESC, team_id ASC
            '''
        ).fetchall()
    return _deserialize_payload_rows(row_list)


def write_team_overview_row(db_path: Path, row: dict) -> None:
    team_id = str(row.get('team_id') or '').strip()
    if team_id == '':
        return
    serialized_payload = json.dumps(row, ensure_ascii=False)
    _run_write_transaction(
        db_path,
        lambda connection: connection.execute(
            '''
            INSERT INTO team_overview (
                team_id,
                payload_json,
                total_score,
                run_status,
                updated_at_text,
                observed_trial_count,
                configured_task_count,
                started_task_count,
                mean_accuracy_percent,
                avg_reaction_time_ms,
                started_task_names
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                total_score = excluded.total_score,
                run_status = excluded.run_status,
                updated_at_text = excluded.updated_at_text,
                observed_trial_count = excluded.observed_trial_count,
                configured_task_count = excluded.configured_task_count,
                started_task_count = excluded.started_task_count,
                mean_accuracy_percent = excluded.mean_accuracy_percent,
                avg_reaction_time_ms = excluded.avg_reaction_time_ms,
                started_task_names = excluded.started_task_names
            ''',
            (
                team_id,
                serialized_payload,
                _to_float(row.get('total_score')),
                str(row.get('run_status') or ''),
                _to_optional_str(row.get('updated_at')),
                int(_to_float(row.get('observed_trial_count'))),
                int(_to_float(row.get('configured_task_count'))),
                int(_to_float(row.get('started_task_count'))),
                _to_float(row.get('mean_accuracy_percent')),
                _to_float(row.get('avg_reaction_time_ms')),
                _to_optional_str(row.get('started_task_names')),
            ),
        ),
    )


def load_team_overview_row(db_path: Path, team_id: str) -> dict | None:
    return _load_single_payload_row(
        db_path,
        'team_overview',
        'team_id = ?',
        (str(team_id),),
    )


def replace_team_task_overview_rows(db_path: Path, team_id: str, row_list: list[dict]) -> None:
    team_id_text = str(team_id or '').strip()
    if team_id_text == '':
        return
    _run_write_transaction(
        db_path,
        lambda connection: _replace_team_task_overview_rows(connection, team_id_text, row_list),
    )


def load_team_task_overview_rows(db_path: Path, team_id: str) -> list[dict]:
    return _load_payload_rows(
        db_path,
        '''
        SELECT payload_json
        FROM task_overview
        WHERE team_id = ?
        ORDER BY task_id ASC
        ''',
        (str(team_id),),
    )


def replace_team_subject_task_overview_rows(db_path: Path, team_id: str, row_list: list[dict]) -> None:
    team_id_text = str(team_id or '').strip()
    if team_id_text == '':
        return
    _run_write_transaction(
        db_path,
        lambda connection: _replace_team_subject_task_overview_rows(connection, team_id_text, row_list),
    )


def load_team_subject_task_overview_rows(db_path: Path, team_id: str) -> list[dict]:
    return _load_payload_rows(
        db_path,
        '''
        SELECT payload_json
        FROM subject_task_overview
        WHERE team_id = ?
        ORDER BY task_id ASC, subject_id ASC
        ''',
        (str(team_id),),
    )


def replace_team_trial_record_rows(db_path: Path, team_id: str, row_list: list[dict]) -> None:
    team_id_text = str(team_id or '').strip()
    if team_id_text == '':
        return
    _run_write_transaction(
        db_path,
        lambda connection: _replace_team_trial_record_rows(connection, team_id_text, row_list),
    )


def upsert_team_task_overview_rows(db_path: Path, team_id: str, row_list: list[dict]) -> None:
    team_id_text = str(team_id or '').strip()
    if team_id_text == '':
        return
    _run_write_transaction(
        db_path,
        lambda connection: _upsert_team_task_overview_rows(connection, team_id_text, row_list),
    )


def upsert_team_subject_task_overview_rows(db_path: Path, team_id: str, row_list: list[dict]) -> None:
    team_id_text = str(team_id or '').strip()
    if team_id_text == '':
        return
    _run_write_transaction(
        db_path,
        lambda connection: _upsert_team_subject_task_overview_rows(connection, team_id_text, row_list),
    )


def upsert_team_trial_record_rows(db_path: Path, team_id: str, row_list: list[dict]) -> None:
    team_id_text = str(team_id or '').strip()
    if team_id_text == '':
        return
    _run_write_transaction(
        db_path,
        lambda connection: _upsert_team_trial_record_rows(connection, team_id_text, row_list),
    )


def load_team_trial_record_rows(db_path: Path, team_id: str) -> list[dict]:
    return _load_payload_rows(
        db_path,
        '''
        SELECT payload_json
        FROM trial_record
        WHERE team_id = ?
        ORDER BY team_trial_index ASC
        ''',
        (str(team_id),),
    )


def export_team_score_overview_csv(db_path: Path, csv_path: Path, fieldnames: list[str]) -> None:
    ensure_runtime_state_schema(db_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_file_path: Path | None = None
    with _connect(db_path) as connection:
        # Serialize concurrent team exporters and read the latest committed rows
        # before replacing the shared scoreboard CSV.
        connection.execute('BEGIN IMMEDIATE')
        db_row_list = connection.execute(
            '''
            SELECT payload_json
            FROM team_score_overview
            ORDER BY total_score DESC, team_id ASC
            '''
        ).fetchall()
        row_list = _deserialize_payload_rows(db_row_list)
        try:
            with tempfile.NamedTemporaryFile(
                mode='w',
                encoding='utf-8-sig',
                newline='',
                dir=csv_path.parent,
                prefix=f'{csv_path.name}.',
                suffix='.tmp',
                delete=False,
            ) as tmp_file:
                tmp_file_path = Path(tmp_file.name)
                writer = csv.DictWriter(tmp_file, fieldnames=fieldnames)
                writer.writeheader()
                for row in row_list:
                    writer.writerow({fieldname: row.get(fieldname) for fieldname in fieldnames})
            os.replace(tmp_file_path, csv_path)
            connection.commit()
        finally:
            if tmp_file_path is not None and tmp_file_path.exists():
                tmp_file_path.unlink(missing_ok=True)


def _load_single_payload_row(db_path: Path, table_name: str, where_clause: str, params: tuple[Any, ...]) -> dict | None:
    if not db_path.exists():
        return None
    ensure_runtime_state_schema(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            f'SELECT payload_json FROM {table_name} WHERE {where_clause} LIMIT 1',
            params,
        ).fetchone()
    if row is None:
        return None
    return _deserialize_payload_row(row)


def _load_payload_rows(db_path: Path, sql: str, params: tuple[Any, ...]) -> list[dict]:
    if not db_path.exists():
        return []
    ensure_runtime_state_schema(db_path)
    with _connect(db_path) as connection:
        row_list = connection.execute(sql, params).fetchall()
    return _deserialize_payload_rows(row_list)


def _deserialize_payload_rows(row_list: list[sqlite3.Row]) -> list[dict]:
    payload_list: list[dict] = []
    for row in row_list:
        payload = _deserialize_payload_row(row)
        if isinstance(payload, dict):
            payload_list.append(payload)
    return payload_list


def _deserialize_payload_row(row: sqlite3.Row) -> dict | None:
    try:
        payload = json.loads(row['payload_json'])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _ensure_table_columns(
    connection: sqlite3.Connection,
    table_name: str,
    column_definition_by_name: dict[str, str],
) -> None:
    existing_column_name_set = {
        str(row['name'])
        for row in connection.execute(f'PRAGMA table_info({table_name})').fetchall()
    }
    for column_name, column_definition in column_definition_by_name.items():
        if column_name in existing_column_name_set:
            continue
        connection.execute(
            f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}'
        )


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=SQLITE_CONNECTION_TIMEOUT_SECONDS)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA journal_mode=WAL')
    connection.execute('PRAGMA synchronous=NORMAL')
    connection.execute(f'PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}')
    return connection


def _run_write_transaction(
    db_path: Path,
    operation,
) -> None:
    retry_delay_seconds = SQLITE_WRITE_RETRY_DELAY_SECONDS
    for attempt_index in range(SQLITE_WRITE_RETRY_LIMIT):
        try:
            ensure_runtime_state_schema(db_path)
            with _connect(db_path) as connection:
                operation(connection)
                connection.commit()
            return
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_lock_error(exc) or attempt_index >= SQLITE_WRITE_RETRY_LIMIT - 1:
                raise
            time.sleep(retry_delay_seconds)
            retry_delay_seconds = min(retry_delay_seconds * 2.0, 1.0)


def _is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    error_text = str(exc).strip().lower()
    return 'database is locked' in error_text or 'database table is locked' in error_text


def _replace_team_task_overview_rows(connection: sqlite3.Connection, team_id_text: str, row_list: list[dict]) -> None:
    connection.execute('DELETE FROM task_overview WHERE team_id = ?', (team_id_text,))
    _upsert_team_task_overview_rows(connection, team_id_text, row_list)


def _upsert_team_task_overview_rows(connection: sqlite3.Connection, team_id_text: str, row_list: list[dict]) -> None:
    for row in row_list:
        task_id = str(row.get('task_id') or '').strip()
        if task_id == '':
            continue
        connection.execute(
            '''
            INSERT INTO task_overview (
                team_id,
                task_id,
                payload_json,
                exp_name,
                exp_task,
                task_status,
                updated_at_text,
                subject_count,
                observed_trial_count,
                accuracy_percent,
                avg_reaction_time_ms,
                task_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, task_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                exp_name = excluded.exp_name,
                exp_task = excluded.exp_task,
                task_status = excluded.task_status,
                updated_at_text = excluded.updated_at_text,
                subject_count = excluded.subject_count,
                observed_trial_count = excluded.observed_trial_count,
                accuracy_percent = excluded.accuracy_percent,
                avg_reaction_time_ms = excluded.avg_reaction_time_ms,
                task_score = excluded.task_score
            ''',
            (
                team_id_text,
                task_id,
                json.dumps(row, ensure_ascii=False),
                _to_optional_str(row.get('exp_name')),
                _to_optional_str(row.get('exp_task')),
                str(row.get('task_status') or ''),
                _to_optional_str(row.get('updated_at')),
                int(_to_float(row.get('subject_count'))),
                int(_to_float(row.get('observed_trial_count'))),
                _to_float(row.get('accuracy_percent')),
                _to_float(row.get('avg_reaction_time_ms')),
                _to_float(row.get('task_score')),
            ),
        )


def _replace_team_subject_task_overview_rows(connection: sqlite3.Connection, team_id_text: str, row_list: list[dict]) -> None:
    connection.execute('DELETE FROM subject_task_overview WHERE team_id = ?', (team_id_text,))
    _upsert_team_subject_task_overview_rows(connection, team_id_text, row_list)


def _upsert_team_subject_task_overview_rows(connection: sqlite3.Connection, team_id_text: str, row_list: list[dict]) -> None:
    for row in row_list:
        subject_id = str(row.get('subject_id') or '').strip()
        task_id = str(row.get('task_id') or '').strip()
        if subject_id == '' or task_id == '':
            continue
        connection.execute(
            '''
            INSERT INTO subject_task_overview (
                team_id,
                subject_id,
                task_id,
                payload_json,
                exp_name,
                exp_task,
                task_status,
                updated_at_text,
                observed_trial_count,
                accuracy_percent
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, subject_id, task_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                exp_name = excluded.exp_name,
                exp_task = excluded.exp_task,
                task_status = excluded.task_status,
                updated_at_text = excluded.updated_at_text,
                observed_trial_count = excluded.observed_trial_count,
                accuracy_percent = excluded.accuracy_percent
            ''',
            (
                team_id_text,
                subject_id,
                task_id,
                json.dumps(row, ensure_ascii=False),
                _to_optional_str(row.get('exp_name')),
                _to_optional_str(row.get('exp_task')),
                str(row.get('task_status') or ''),
                _to_optional_str(row.get('updated_at')),
                int(_to_float(row.get('observed_trial_count'))),
                _to_float(row.get('accuracy_percent')),
            ),
        )


def _replace_team_trial_record_rows(connection: sqlite3.Connection, team_id_text: str, row_list: list[dict]) -> None:
    connection.execute('DELETE FROM trial_record WHERE team_id = ?', (team_id_text,))
    _upsert_team_trial_record_rows(connection, team_id_text, row_list)


def _upsert_team_trial_record_rows(connection: sqlite3.Connection, team_id_text: str, row_list: list[dict]) -> None:
    for row in row_list:
        team_trial_index = int(_to_float(row.get('team_trial_index')))
        if team_trial_index <= 0:
            continue
        connection.execute(
            '''
            INSERT INTO trial_record (
                team_id,
                team_trial_index,
                payload_json,
                task_trial_index,
                task_id,
                subject_id,
                exp_name,
                exp_task,
                session_id,
                block_id,
                trial_id,
                true_label,
                predict_label,
                is_correct,
                trial_score,
                predict_time_ms,
                cumulative_accuracy_percent,
                cumulative_score,
                is_timeout,
                report_position,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, team_trial_index) DO UPDATE SET
                payload_json = excluded.payload_json,
                task_trial_index = excluded.task_trial_index,
                task_id = excluded.task_id,
                subject_id = excluded.subject_id,
                exp_name = excluded.exp_name,
                exp_task = excluded.exp_task,
                session_id = excluded.session_id,
                block_id = excluded.block_id,
                trial_id = excluded.trial_id,
                true_label = excluded.true_label,
                predict_label = excluded.predict_label,
                is_correct = excluded.is_correct,
                trial_score = excluded.trial_score,
                predict_time_ms = excluded.predict_time_ms,
                cumulative_accuracy_percent = excluded.cumulative_accuracy_percent,
                cumulative_score = excluded.cumulative_score,
                is_timeout = excluded.is_timeout,
                report_position = excluded.report_position,
                updated_at = excluded.updated_at
            ''',
            (
                team_id_text,
                team_trial_index,
                json.dumps(row, ensure_ascii=False),
                int(_to_float(row.get('task_trial_index'))),
                _to_optional_str(row.get('task_id')),
                _to_optional_str(row.get('subject_id')),
                _to_optional_str(row.get('exp_name')),
                _to_optional_str(row.get('exp_task')),
                _to_optional_str(row.get('session_id')),
                _to_optional_str(row.get('block_id')),
                _to_optional_str(row.get('trial_id')),
                _to_optional_str(row.get('true_label')),
                _to_optional_str(row.get('predict_label')),
                1 if _to_bool(row.get('is_correct')) else 0,
                _to_float(row.get('trial_score')),
                _to_float(row.get('predict_time_ms')),
                _to_float(row.get('cumulative_accuracy_percent')),
                _to_float(row.get('cumulative_score')),
                1 if _to_bool(row.get('is_timeout')) else 0,
                _to_optional_str(row.get('report_position')),
                time.time(),
            ),
        )


def _extract_updated_at(payload: Any) -> float:
    if isinstance(payload, dict):
        updated_at = payload.get('updated_at')
        if isinstance(updated_at, (int, float)):
            return float(updated_at)
    return time.time()


def _to_float(value: Any) -> float:
    try:
        parsed_value = float(value)
        if not math.isfinite(parsed_value):
            return 0.0
        return parsed_value
    except (TypeError, ValueError):
        return 0.0


def _to_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    value_text = str(value)
    return value_text if value_text != '' else None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {'1', 'true', 'yes', 'y'}
