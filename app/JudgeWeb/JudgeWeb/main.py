import csv
import json
import logging
import math
import os
import sqlite3
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parents[2]
RESULTS_ROOT = PROJECT_ROOT / 'results'
LIVE_ROOT = RESULTS_ROOT / 'live'
CONTROL_ROOT = RESULTS_ROOT / 'control'
CONFIG_PATH = APP_DIR / 'config' / 'JudgeWebConfig.yml'
CENTRAL_CONTROLLER_CONFIG_PATH = (
    PROJECT_ROOT / 'app' / 'CentralController' / 'CentralController' / 'config' / 'CentralControllerConfig.yml'
)
VIRTUAL_RECEIVER_CONFIG_PATH = (
    PROJECT_ROOT / 'app' / 'Collector' / 'Collector' / 'receiver' / 'virtual_receiver' / 'VirtualReceiverConfig.yml'
)
AUTO_RESTART_RESUME_HELPER_SCRIPT_PATH = PROJECT_ROOT / 'tools' / 'restart_judge_resume_after_recovery.bat'
LAUNCHER_MANIFEST_PATH = CONTROL_ROOT / 'launcher_manifest.json'
LOGGER = logging.getLogger('judgeWeb')
DEFAULT_LOCAL_CORS_ALLOW_ORIGINS = [
    'http://127.0.0.1:18080',
    'http://localhost:18080',
    'http://127.0.0.1:5173',
    'http://localhost:5173',
    'http://127.0.0.1:4173',
    'http://localhost:4173',
]
DEFAULT_LOCAL_ONLY = True

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from tools.runtime_state_sqlite import (  # noqa: E402
    STATE_KEY_CURRENT_TRIAL,
    STATE_KEY_MATCH_CONTROL_STATUS,
    STATE_KEY_RUNTIME_STAGE_STATUS,
    TEAM_STATE_KEY_PREFIX,
    count_json_state_by_prefix,
    json_state_exists,
    list_json_state_by_prefix,
    load_team_overview_row,
    load_team_score_overview_rows,
    load_team_subject_task_overview_rows,
    load_team_task_overview_rows,
    load_team_trial_record_rows,
    read_json_state,
    resolve_runtime_state_db_path,
)
from tools.recovery_runtime import (  # noqa: E402
    RECOVERY_REQUEST_FILE_NAME,
    build_checkpoint_id as build_recovery_checkpoint_id,
    load_pending_recovery_request,
    load_stage_catalog as load_recovery_stage_catalog,
    normalize_stage_payload as normalize_recovery_stage_payload,
    resolve_stage_payload as resolve_recovery_stage_payload,
)
from tools.results_archive import archive_and_clear_active_results  # noqa: E402

RUNTIME_STATE_DB_PATH = resolve_runtime_state_db_path(PROJECT_ROOT)


def configure_logging() -> None:
    log_root = APP_DIR / 'log'
    log_root.mkdir(parents=True, exist_ok=True)
    log_file_path = log_root / 'judgeWeb.log'

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    file_handler_exists = False
    for handler in list(root_logger.handlers):
        if not isinstance(handler, logging.FileHandler):
            continue
        if Path(getattr(handler, 'baseFilename', '')).resolve() != log_file_path.resolve():
            continue
        if isinstance(handler, TimedRotatingFileHandler):
            file_handler_exists = True
            continue
        root_logger.removeHandler(handler)
        handler.close()

    if not file_handler_exists:
        file_handler = TimedRotatingFileHandler(
            log_file_path,
            when='midnight',
            interval=1,
            backupCount=30,
            encoding='utf-8',
        )
        file_handler.suffix = '%Y-%m-%d'
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if not any(type(handler) is logging.StreamHandler for handler in root_logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)


configure_logging()


def load_yaml_file(file_path: Path) -> dict:
    if not file_path.exists():
        return {}
    with file_path.open('r', encoding='utf-8') as file:
        return yaml.safe_load(file) or {}


def load_json_file(file_path: Path) -> dict:
    if not file_path.exists():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        LOGGER.exception('读取 JSON 文件失败: %s', file_path)
        return {}
    return payload if isinstance(payload, dict) else {}


CONFIG = load_yaml_file(CONFIG_PATH)


async def collect_live_snapshot_sections(
    last_section_payload: dict[str, object],
    section_failure_count: dict[str, int],
    section_loader_list,
) -> dict[str, object]:
    live_payload = dict(last_section_payload)
    for section_name, section_loader in section_loader_list:
        try:
            section_payload = await section_loader()
        except Exception:
            failure_count = section_failure_count.get(section_name, 0) + 1
            section_failure_count[section_name] = failure_count
            if failure_count == 1 or failure_count % 25 == 0:
                LOGGER.exception(
                    'JudgeWeb websocket 分区读取失败，保留最后有效值: '
                    'section=%s consecutive_failure_count=%s',
                    section_name,
                    failure_count,
                )
            continue
        section_failure_count[section_name] = 0
        last_section_payload[section_name] = section_payload
        live_payload[section_name] = section_payload
    return live_payload


def create_app() -> FastAPI:
    app = FastAPI(title='Judge Web Backend', version='1.0.0')
    cors_allow_origins = (
        (((CONFIG.get('server') or {}).get('cors_allow_origins')) or DEFAULT_LOCAL_CORS_ALLOW_ORIGINS)
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allow_origins,
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    @app.middleware('http')
    async def enforce_local_only(request: Request, call_next):
        client_host = request.client.host if request.client else None
        if is_local_only_enabled() and not is_loopback_host(client_host):
            LOGGER.warning(
                '拒绝非本机 HTTP 访问: client_host=%s path=%s',
                client_host,
                request.url.path,
            )
            return JSONResponse(status_code=403, content={'detail': 'JudgeWeb 当前仅允许本机访问'})
        return await call_next(request)

    @app.get('/healthz')
    async def healthz():
        return {'status': 'ok', 'timestamp': time.time()}

    @app.get('/api/v1/match/overview')
    async def get_match_overview():
        team_registry = load_team_registry()
        team_status_list = build_team_status_list(team_registry)
        scoreboard_row_list = load_scoreboard_rows(team_registry)
        current_trial = enrich_current_trial(read_live_state_payload(STATE_KEY_CURRENT_TRIAL, 'current_trial.json'))
        match_control_status = load_match_control_status()
        runtime_stage_status = load_runtime_stage_status()
        return {
            'match_name': ((CONFIG.get('match') or {}).get('match_name')) or 'BCI Competition Final',
            'trial_cycle_seconds': ((CONFIG.get('match') or {}).get('trial_cycle_seconds')) or 1.3,
            'prediction_window_seconds': ((CONFIG.get('match') or {}).get('prediction_window_seconds')) or 1.0,
            'current_match_status': resolve_match_status(
                current_trial,
                team_status_list,
                match_control_status,
                runtime_stage_status,
            ),
            'match_control_status': match_control_status,
            'start_readiness': build_start_match_readiness(team_registry, team_status_list, runtime_stage_status),
            'team_count': len(team_registry),
            'connected_team_count': sum(
                1 for team_status in team_status_list if team_status.get('connection_status') == 'connected'
            ),
            'calibration_ready_team_count': sum(
                1
                for team_status in team_status_list
                if bool(team_status.get('calibration_ready'))
                and str(team_status.get('connection_status') or '').lower() == 'connected'
            ),
            'current_trial': current_trial,
            'scoreboard_preview': scoreboard_row_list,
            'updated_at': time.time(),
        }

    @app.get('/api/v1/match/current')
    async def get_match_current():
        current_trial = enrich_current_trial(read_live_state_payload(STATE_KEY_CURRENT_TRIAL, 'current_trial.json'))
        return {
            'match_name': ((CONFIG.get('match') or {}).get('match_name')) or 'BCI Competition Final',
            'current_trial': current_trial,
            'updated_at': time.time(),
        }

    @app.get('/api/v1/match/teams')
    async def get_match_teams():
        return {
            'team_list': build_team_status_list(load_team_registry()),
            'updated_at': time.time(),
        }

    @app.get('/api/v1/match/scoreboard')
    async def get_match_scoreboard():
        team_registry = load_team_registry()
        return {
            'scoreboard': load_scoreboard_rows(team_registry),
            'updated_at': time.time(),
        }

    @app.get('/api/v1/system/components')
    async def get_system_components():
        team_registry = load_team_registry()
        team_status_list = build_team_status_list(team_registry)
        runtime_stage_status = load_runtime_stage_status()
        return {
            'judge_web': {
                'status': 'running',
                'host': ((CONFIG.get('server') or {}).get('host')) or '127.0.0.1',
                'port': ((CONFIG.get('server') or {}).get('port')) or 18080,
            },
            'match_control_status': load_match_control_status(),
            'runtime_stage_status': runtime_stage_status,
            'team_component_status_list': [
                {
                    'team_id': team_status.get('team_id'),
                    'team_display_name': team_status.get('team_display_name'),
                    'processor_component_id': team_status.get('processor_component_id'),
                    'collector_component_id': team_status.get('collector_component_id'),
                    'connection_status': team_status.get('connection_status'),
                    'run_status': team_status.get('run_status'),
                    'calibration_status': team_status.get('calibration_status'),
                    'updated_at': team_status.get('updated_at'),
                }
                for team_status in team_status_list
            ],
            'updated_at': time.time(),
        }

    @app.get('/api/v1/control/status')
    async def get_control_status():
        match_control_status = load_match_control_status()
        team_registry = load_team_registry()
        team_status_list = build_team_status_list(team_registry)
        runtime_stage_status = load_runtime_stage_status()
        return {
            'match_control_status': match_control_status,
            'start_readiness': build_start_match_readiness(team_registry, team_status_list, runtime_stage_status),
            'updated_at': time.time(),
        }

    @app.get('/api/v1/recovery/status')
    async def get_recovery_status():
        results_exists = RESULTS_ROOT.exists()
        team_registry = load_team_registry()
        team_status_list = build_team_status_list(team_registry)
        scoreboard_row_list = load_scoreboard_rows(team_registry)
        checkpoint_list = load_restartable_recovery_checkpoint_list()
        pending_recovery_request = load_pending_recovery_request_status()
        result_dir_list = list_result_team_dir_names() if results_exists else []
        current_trial = enrich_current_trial(read_live_state_payload(STATE_KEY_CURRENT_TRIAL, 'current_trial.json'))
        recommended_recovery_stage = resolve_recommended_recovery_stage(checkpoint_list, current_trial)
        return {
            'results_root_exists': results_exists,
            'resume_available': bool(scoreboard_row_list or result_dir_list),
            'result_team_dir_list': result_dir_list,
            'checkpoint_count': len(checkpoint_list),
            'inplace_stage_restart_supported': True,
            'judge_restart_required_for_stage_restart': False,
            'pending_recovery_mode': pending_recovery_request.get('recovery_mode'),
            'pending_recovery_request': pending_recovery_request.get('stage'),
            'pending_recovery_request_at': pending_recovery_request.get('requested_at'),
            'pending_recovery_valid': pending_recovery_request.get('valid', False),
            'pending_recovery_message': pending_recovery_request.get('message'),
            'recommended_recovery_stage': recommended_recovery_stage,
            'pending_restart_stage_request': pending_recovery_request.get('stage'),
            'pending_restart_stage_request_at': pending_recovery_request.get('requested_at'),
            'pending_restart_stage_valid': pending_recovery_request.get('valid', False),
            'pending_restart_stage_message': pending_recovery_request.get('message'),
            'live_state_files': {
                'current_trial': json_state_exists(RUNTIME_STATE_DB_PATH, STATE_KEY_CURRENT_TRIAL) or (LIVE_ROOT / 'current_trial.json').exists(),
                'runtime_stage_status': json_state_exists(RUNTIME_STATE_DB_PATH, STATE_KEY_RUNTIME_STAGE_STATUS) or (LIVE_ROOT / 'runtime_stage_status.json').exists(),
                'match_control_status': json_state_exists(RUNTIME_STATE_DB_PATH, STATE_KEY_MATCH_CONTROL_STATUS) or (LIVE_ROOT / 'match_control_status.json').exists(),
                'team_live_count': max(
                    count_json_state_by_prefix(RUNTIME_STATE_DB_PATH, TEAM_STATE_KEY_PREFIX),
                    len(list((LIVE_ROOT / 'teams').glob('*.json'))) if (LIVE_ROOT / 'teams').exists() else 0,
                ),
            },
            'team_status_list': team_status_list,
            'updated_at': time.time(),
        }

    @app.get('/api/v1/recovery/checkpoints')
    async def get_recovery_checkpoints():
        return {
            'checkpoint_list': load_restartable_recovery_checkpoint_list(),
            'updated_at': time.time(),
        }

    @app.post('/api/v1/control/start-match')
    async def post_control_start_match():
        match_control_status = load_match_control_status()
        if match_control_status.get('match_started'):
            return {
                'ok': True,
                'message': '比赛已经处于开始状态',
                'match_control_status': match_control_status,
                'updated_at': time.time(),
            }
        team_registry = load_team_registry()
        team_status_list = build_team_status_list(team_registry)
        runtime_stage_status = load_runtime_stage_status()
        start_readiness = build_start_match_readiness(team_registry, team_status_list, runtime_stage_status)
        if not start_readiness.get('ready'):
            raise HTTPException(
                status_code=409,
                detail='；'.join(start_readiness.get('reason_list') or ['当前未满足开始比赛条件']),
            )
        delete_control_request('pause_request')
        delete_control_request('resume_control_request')
        payload = write_control_request('start_match_request', {})
        return {
            'ok': True,
            'message': '已记录开始比赛请求，等待协调器放行',
            'request': payload,
            'match_control_status': load_match_control_status(),
            'start_readiness': start_readiness,
            'updated_at': time.time(),
        }

    @app.post('/api/v1/recovery/clear-results')
    async def post_clear_results():
        clear_results_root()
        write_control_request('clear_results', {})
        return {
            'ok': True,
            'message': 'results 已清空',
            'updated_at': time.time(),
        }

    @app.post('/api/v1/recovery/request')
    async def post_recovery_request(request: Request):
        request_payload = await request.json()
        recovery_mode = str((request_payload or {}).get('recovery_mode') or '').strip()
        if recovery_mode not in {'continue_from_checkpoint', 'restart_from_stage'}:
            raise HTTPException(status_code=400, detail='缺少合法的 recovery_mode')
        stage_payload = normalize_stage_request_payload((request_payload or {}).get('stage'))
        if recovery_mode == 'restart_from_stage' and stage_payload is None:
            raise HTTPException(
                status_code=400,
                detail='重跑模式缺少合法的 subject_id / exp_name / exp_task / session_id',
            )
        if recovery_mode == 'continue_from_checkpoint' and stage_payload is None:
            recommended_recovery_stage = resolve_recommended_recovery_stage(
                load_restartable_recovery_checkpoint_list(),
                enrich_current_trial(read_live_state_payload(STATE_KEY_CURRENT_TRIAL, 'current_trial.json')),
            )
            stage_payload = recommended_recovery_stage
        if stage_payload is not None:
            checkpoint_id = build_checkpoint_id(stage_payload)
            checkpoint_id_set = {
                str(checkpoint.get('checkpoint_id') or '').strip()
                for checkpoint in load_restartable_recovery_checkpoint_list()
            }
            if checkpoint_id not in checkpoint_id_set:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f'指定阶段当前不可作为重跑起点: '
                        f"{stage_payload['subject_id']} / {stage_payload['exp_name']} / "
                        f"{stage_payload['exp_task']} / {stage_payload['session_id']}"
                    ),
                )
        payload = write_recovery_request(recovery_mode, stage_payload)
        return {
            'ok': True,
            'message': (
                '已记录断点继续恢复请求。下次按恢复流程重新拉起裁判端后，将继续沿用现有运行断点。'
                if recovery_mode == 'continue_from_checkpoint'
                else '已记录指定阶段重跑请求。'
            ),
            'request': payload,
            'judge_restart_required_for_stage_restart': False if recovery_mode == 'restart_from_stage' else True,
            'inplace_stage_restart_supported': True if recovery_mode == 'restart_from_stage' else False,
        }

    @app.post('/api/v1/recovery/resume')
    async def post_recovery_resume():
        payload = write_recovery_request('continue_from_checkpoint', None)
        return {
            'ok': True,
            'message': '已记录恢复续跑请求',
            'request': payload,
        }

    @app.post('/api/v1/recovery/restart-stage')
    async def post_recovery_restart_stage(request: Request):
        match_control_status = load_match_control_status()
        if not match_control_status.get('paused'):
            raise HTTPException(status_code=409, detail='指定阶段重跑只能在比赛已暂停后执行')
        request_payload = await request.json()
        stage_payload = normalize_stage_request_payload(request_payload)
        if stage_payload is None:
            raise HTTPException(
                status_code=400,
                detail='缺少合法的 subject_id / exp_name / exp_task / session_id',
            )
        checkpoint_id = build_checkpoint_id(stage_payload)
        checkpoint_id_set = {
            str(checkpoint.get('checkpoint_id') or '').strip()
            for checkpoint in load_restartable_recovery_checkpoint_list()
        }
        if checkpoint_id not in checkpoint_id_set:
            raise HTTPException(
                status_code=400,
                detail=(
                    f'指定阶段不存在于 checkpoint 列表: '
                    f"{stage_payload['subject_id']} / {stage_payload['exp_name']} / "
                    f"{stage_payload['exp_task']} / {stage_payload['session_id']}"
                ),
            )
        payload = write_recovery_request('restart_from_stage', stage_payload)
        restart_schedule = schedule_stage_restart_resume()
        return {
            'ok': True,
            'message': '已记录阶段重启请求，系统将自动重启裁判端并从指定阶段重新校准、重新开始比赛。',
            'request': payload,
            'restart_schedule': restart_schedule,
            'judge_restart_required_for_stage_restart': False,
            'inplace_stage_restart_supported': True,
        }

    @app.post('/api/v1/control/pause')
    async def post_control_pause():
        match_control_status = load_match_control_status()
        if not match_control_status.get('match_started'):
            delete_control_request('pause_request')
            raise HTTPException(status_code=409, detail='比赛尚未开始，不能写入暂停请求')
        payload = write_control_request('pause_request', {})
        return {
            'ok': True,
            'message': '已记录暂停请求，将在 trial 边界暂停',
            'request': payload,
        }

    @app.post('/api/v1/control/resume')
    async def post_control_resume():
        match_control_status = load_match_control_status()
        if not match_control_status.get('match_started'):
            delete_control_request('resume_control_request')
            raise HTTPException(status_code=409, detail='比赛尚未开始，不能写入继续请求')
        payload = write_control_request('resume_control_request', {})
        return {
            'ok': True,
            'message': '已记录继续请求，将恢复后续 trial 放行',
            'request': payload,
        }

    @app.get('/api/v1/match/summary')
    async def get_match_summary():
        team_registry = load_team_registry()
        return build_match_summary(team_registry)

    @app.get('/api/v1/match/summary/teams')
    async def get_match_summary_teams():
        team_registry = load_team_registry()
        return {
            'team_summary_list': load_match_summary_team_list(team_registry),
            'updated_at': time.time(),
        }

    @app.get('/api/v1/match/summary/teams/{team_id}')
    async def get_match_summary_team(team_id: str):
        team_registry = load_team_registry()
        team_summary = load_match_summary_team(team_registry, team_id)
        if team_summary is None:
            raise HTTPException(status_code=404, detail=f'team_id not found: {team_id}')
        return team_summary

    @app.websocket('/api/v1/ws/live')
    async def websocket_live(websocket: WebSocket):
        client_host = websocket.client.host if websocket.client else None
        if is_local_only_enabled() and not is_loopback_host(client_host):
            LOGGER.warning(
                '拒绝非本机 WebSocket 访问: client_host=%s path=%s',
                client_host,
                websocket.url.path,
            )
            await websocket.close(code=1008, reason='JudgeWeb 当前仅允许本机访问')
            return
        await websocket.accept()
        push_interval_seconds = float(
            (((CONFIG.get('server') or {}).get('websocket_push_interval_seconds')) or 0.2)
        )
        live_section_loader_list = (
            ('overview', get_match_overview),
            ('current', get_match_current),
            ('teams', get_match_teams),
            ('scoreboard', get_match_scoreboard),
            ('system', get_system_components),
            ('control', get_control_status),
            ('recovery', get_recovery_status),
        )
        last_live_section_payload: dict[str, object] = {}
        live_section_failure_count: dict[str, int] = {}
        try:
            while True:
                live_payload = await collect_live_snapshot_sections(
                    last_live_section_payload,
                    live_section_failure_count,
                    live_section_loader_list,
                )
                await websocket.send_json(
                    sanitize_json_compatible(live_payload)
                )
                await wait_seconds(push_interval_seconds)
        except WebSocketDisconnect:
            LOGGER.info('JudgeWeb websocket client disconnected')
            return
        except RuntimeError as exc:
            error_message = str(exc).lower()
            if 'websocket' in error_message and (
                'close' in error_message
                or 'disconnect' in error_message
                or 'cannot call "send"' in error_message
            ):
                LOGGER.info('JudgeWeb websocket closed during push: %s', exc)
                return
            LOGGER.exception('JudgeWeb websocket push failed')
            raise
        except Exception:
            LOGGER.exception('JudgeWeb websocket push failed')
            raise

    return app


app = create_app()


async def wait_seconds(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


def read_json_file(file_path: Path):
    if not file_path.exists():
        return None
    try:
        return json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None

def is_local_only_enabled() -> bool:
    server_config = CONFIG.get('server') or {}
    local_only = server_config.get('local_only')
    if local_only is None:
        return DEFAULT_LOCAL_ONLY
    return bool(local_only)


def is_loopback_host(host) -> bool:
    if host is None:
        return False
    host_text = str(host).strip().lower()
    if host_text in {'127.0.0.1', '::1', 'localhost'}:
        return True
    if host_text.startswith('::ffff:127.0.0.1'):
        return True
    return False


def read_live_state_payload(state_key: str, legacy_file_name: str) -> dict | None:
    payload = read_json_state(RUNTIME_STATE_DB_PATH, state_key)
    if isinstance(payload, dict):
        return payload
    return read_json_file(LIVE_ROOT / legacy_file_name)


def list_result_team_dir_names() -> list[str]:
    if not RESULTS_ROOT.exists():
        return []
    team_dir_name_list: list[str] = []
    for item in RESULTS_ROOT.iterdir():
        item_name = str(item.name or '')
        if item_name in {'live', 'control', 'history'}:
            continue
        if item_name.endswith(('.db', '.db-shm', '.db-wal', '.sqlite', '.sqlite-shm', '.sqlite-wal')):
            continue
        try:
            if item.is_dir():
                team_dir_name_list.append(item_name)
        except PermissionError:
            continue
    return sorted(team_dir_name_list)


def load_runtime_stage_status() -> dict:
    return normalize_runtime_stage_status(
        read_live_state_payload(STATE_KEY_RUNTIME_STAGE_STATUS, 'runtime_stage_status.json')
    )


def normalize_runtime_stage_status(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        return {
            'release_policy': None,
            'trial_release_interval_seconds': None,
            'trial_terminal_watchdog_base_timeout_seconds': None,
            'trial_terminal_watchdog_grace_seconds': None,
            'match_control_status': {},
            'updated_at': None,
            'group_status_list': [],
        }
    normalized_group_status_list = [
        normalize_runtime_stage_group_status(group_status)
        for group_status in (payload.get('group_status_list') or [])
        if isinstance(group_status, dict)
    ]
    return {
        'release_policy': payload.get('release_policy'),
        'trial_release_interval_seconds': to_float(payload.get('trial_release_interval_seconds'), 0.0) or None,
        'trial_terminal_watchdog_base_timeout_seconds': (
            to_float(payload.get('trial_terminal_watchdog_base_timeout_seconds'), 0.0) or None
        ),
        'trial_terminal_watchdog_grace_seconds': (
            to_float(payload.get('trial_terminal_watchdog_grace_seconds'), 0.0) or None
        ),
        'match_control_status': payload.get('match_control_status') or {},
        'updated_at': payload.get('updated_at'),
        'group_status_list': normalized_group_status_list,
    }


def normalize_runtime_stage_group_status(group_status: dict | None) -> dict:
    if not isinstance(group_status, dict):
        return {
            'group_id': '',
            'configured_team_id_list': [],
            'stage_status_list': [],
        }
    return {
        'group_id': str(group_status.get('group_id') or '').strip(),
        'configured_team_id_list': [
            str(team_id)
            for team_id in (group_status.get('configured_team_id_list') or [])
            if str(team_id).strip()
        ],
        'stage_status_list': [
            normalize_runtime_stage_stage_status(stage_status)
            for stage_status in (group_status.get('stage_status_list') or [])
            if isinstance(stage_status, dict)
        ],
    }


def normalize_runtime_stage_stage_status(stage_status: dict | None) -> dict:
    if not isinstance(stage_status, dict):
        return {}
    normalized_stage_status = dict(stage_status)
    normalized_stage_status['stage_key'] = str(stage_status.get('stage_key') or '').strip()
    normalized_stage_status['stage_context'] = normalize_stage_context(stage_status.get('stage_context'))
    normalized_stage_status['collector_prepared'] = to_bool(stage_status.get('collector_prepared'))
    normalized_stage_status['online_stage_released'] = to_bool(stage_status.get('online_stage_released'))
    normalized_stage_status['ready_team_id_list'] = normalize_string_list(stage_status.get('ready_team_id_list'))
    normalized_stage_status['pending_ready_team_id_list'] = normalize_string_list(stage_status.get('pending_ready_team_id_list'))
    normalized_stage_status['pending_release'] = (
        stage_status.get('pending_release')
        if isinstance(stage_status.get('pending_release'), dict)
        else None
    )
    normalized_stage_status['online_trial_count'] = int(to_float(stage_status.get('online_trial_count'), 0))
    normalized_stage_status['released_trial_id'] = int(to_float(stage_status.get('released_trial_id'), 0))
    normalized_stage_status['completed_trial_id_list'] = normalize_int_list(stage_status.get('completed_trial_id_list'))
    normalized_stage_status['completed_trial_count'] = int(
        to_float(stage_status.get('completed_trial_count'), len(normalized_stage_status['completed_trial_id_list']))
    )
    normalized_stage_status['max_completed_trial_id'] = int(
        to_float(
            stage_status.get('max_completed_trial_id'),
            normalized_stage_status['completed_trial_id_list'][-1] if normalized_stage_status['completed_trial_id_list'] else 0,
        )
    )
    normalized_stage_status['trial_sent_wallclock_by_trial'] = normalize_number_mapping(
        stage_status.get('trial_sent_wallclock_by_trial')
    )
    normalized_stage_status['next_release_target_wallclock_by_trial'] = normalize_number_mapping(
        stage_status.get('next_release_target_wallclock_by_trial')
    )
    normalized_stage_status['trial_terminal_team_id_list_by_trial'] = normalize_string_list_mapping(
        stage_status.get('trial_terminal_team_id_list_by_trial')
    )
    normalized_stage_status['trial_observed_terminal_team_id_list_by_trial'] = normalize_string_list_mapping(
        stage_status.get('trial_observed_terminal_team_id_list_by_trial')
    )
    normalized_stage_status['trial_forced_terminal_team_id_list_by_trial'] = normalize_string_list_mapping(
        stage_status.get('trial_forced_terminal_team_id_list_by_trial')
    )
    normalized_stage_status['trial_terminal_watchdog_deadline_wallclock_by_trial'] = normalize_number_mapping(
        stage_status.get('trial_terminal_watchdog_deadline_wallclock_by_trial')
    )
    normalized_stage_status['trial_terminal_watchdog_base_timeout_seconds_by_trial'] = normalize_number_mapping(
        stage_status.get('trial_terminal_watchdog_base_timeout_seconds_by_trial')
    )
    if not normalized_stage_status['trial_observed_terminal_team_id_list_by_trial']:
        normalized_stage_status['trial_observed_terminal_team_id_list_by_trial'] = dict(
            normalized_stage_status['trial_terminal_team_id_list_by_trial']
        )
    if not normalized_stage_status['trial_forced_terminal_team_id_list_by_trial']:
        normalized_stage_status['trial_forced_terminal_team_id_list_by_trial'] = {}
    return normalized_stage_status


def normalize_stage_context(stage_context) -> dict:
    if not isinstance(stage_context, dict):
        return {
            'subject_id': '',
            'exp_name': '',
            'exp_task': '',
            'session_id': '',
        }
    return {
        'subject_id': str(stage_context.get('subject_id') or '').strip(),
        'exp_name': str(stage_context.get('exp_name') or '').strip(),
        'exp_task': str(stage_context.get('exp_task') or '').strip(),
        'session_id': str(stage_context.get('session_id') or '').strip(),
    }


def normalize_string_list(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def normalize_int_list(value) -> list[int]:
    if not isinstance(value, list):
        return []
    normalized_list = []
    for item in value:
        normalized_list.append(int(to_float(item, 0)))
    return normalized_list


def normalize_number_mapping(value) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    normalized_mapping = {}
    for key, item in value.items():
        key_text = str(key).strip()
        if key_text == '':
            continue
        normalized_mapping[key_text] = to_float(item, 0.0)
    return normalized_mapping


def normalize_string_list_mapping(value) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    normalized_mapping = {}
    for key, item in value.items():
        key_text = str(key).strip()
        if key_text == '':
            continue
        normalized_mapping[key_text] = normalize_string_list(item)
    return normalized_mapping

def load_team_live_status_map() -> dict[str, dict]:
    team_status_by_team_id: dict[str, dict] = {}
    for payload in list_json_state_by_prefix(RUNTIME_STATE_DB_PATH, TEAM_STATE_KEY_PREFIX):
        team_id = str(payload.get('team_id') or '').strip()
        if team_id != '':
            team_status_by_team_id[team_id] = payload
    if team_status_by_team_id:
        return team_status_by_team_id
    team_live_root = LIVE_ROOT / 'teams'
    if not team_live_root.exists():
        return {}
    for file_path in team_live_root.glob('*.json'):
        payload = read_json_file(file_path)
        if not isinstance(payload, dict):
            continue
        team_id = str(payload.get('team_id') or file_path.stem).strip()
        if team_id != '':
            team_status_by_team_id[team_id] = payload
    return team_status_by_team_id


def load_team_registry() -> list[dict]:
    config_dict = load_yaml_file(CENTRAL_CONTROLLER_CONFIG_PATH)
    component_dict = (config_dict.get('components') or {})
    team_registry = []
    for component_id, component_config in component_dict.items():
        if (component_config or {}).get('component_type') != 'PROCESSOR':
            continue
        component_info = (component_config or {}).get('component_info') or {}
        team_registry.append(
            {
                'team_id': component_info.get('team_id') or component_id.split('.', 1)[0],
                'team_display_name': component_info.get('team_display_name') or component_info.get('team_id') or component_id,
                'team_host': component_info.get('team_host'),
                'group_id': component_info.get('group_id'),
                'processor_component_id': component_info.get('processor_component_id') or component_id,
            }
        )
    team_registry.sort(key=lambda item: item.get('team_id') or '')
    return team_registry


def load_recovery_checkpoint_list() -> list[dict]:
    team_registry = load_team_registry()
    configured_checkpoint_dict: dict[str, dict] = {}
    for configured_stage in load_recovery_stage_catalog(PROJECT_ROOT):
        stage_payload = normalize_stage_request_payload(configured_stage)
        if stage_payload is None:
            continue
        checkpoint_id = build_checkpoint_id(stage_payload)
        configured_checkpoint_dict[checkpoint_id] = {
            'checkpoint_id': checkpoint_id,
            **stage_payload,
            'block_id': configured_stage.get('block_id'),
            'created_at': 0,
            'description': '配置阶段（可作为指定阶段恢复目标）',
            'status': 'configured',
        }

    observed_checkpoint_dict: dict[str, dict] = {}
    for team_id in collect_team_id_list(team_registry):
        for row in load_team_trial_record_rows(RUNTIME_STATE_DB_PATH, team_id):
            stage_payload = normalize_stage_request_payload(row)
            if stage_payload is None:
                continue
            checkpoint_id = build_checkpoint_id(stage_payload)
            aggregate_row = observed_checkpoint_dict.setdefault(
                checkpoint_id,
                {
                    'checkpoint_id': checkpoint_id,
                    **stage_payload,
                    'created_at': row.get('updated_at') or time.time(),
                    'observed_team_id_set': set(),
                    'observed_trial_count': 0,
                },
            )
            aggregate_row['observed_team_id_set'].add(str(team_id))
            aggregate_row['observed_trial_count'] += 1
            updated_at = row.get('updated_at')
            if updated_at not in (None, ''):
                aggregate_row['created_at'] = updated_at

    checkpoint_list = []
    for checkpoint_id, configured_checkpoint in configured_checkpoint_dict.items():
        checkpoint_row = dict(configured_checkpoint)
        observed_checkpoint = observed_checkpoint_dict.get(checkpoint_id)
        if observed_checkpoint:
            checkpoint_row['created_at'] = observed_checkpoint.get('created_at') or checkpoint_row.get('created_at')
            checkpoint_row['description'] = (
                f"已观测 {len(observed_checkpoint.get('observed_team_id_set') or set())} 队结果，"
                f"累计 {int(observed_checkpoint.get('observed_trial_count') or 0)} 个 trial"
            )
            checkpoint_row['status'] = 'observed'
        checkpoint_list.append(checkpoint_row)

    for checkpoint_id, observed_checkpoint in observed_checkpoint_dict.items():
        if checkpoint_id in configured_checkpoint_dict:
            continue
        checkpoint_list.append(
            {
                'checkpoint_id': checkpoint_id,
                'subject_id': observed_checkpoint.get('subject_id'),
                'exp_name': observed_checkpoint.get('exp_name'),
                'exp_task': observed_checkpoint.get('exp_task'),
                'session_id': observed_checkpoint.get('session_id'),
                'created_at': observed_checkpoint.get('created_at') or time.time(),
                'description': (
                    f"运行态观测阶段，已观测 "
                    f"{len(observed_checkpoint.get('observed_team_id_set') or set())} 队结果，"
                    f"累计 {int(observed_checkpoint.get('observed_trial_count') or 0)} 个 trial"
                ),
                'status': 'observed',
            }
        )

    return checkpoint_list

def load_restartable_recovery_checkpoint_list() -> list[dict]:
    checkpoint_list = load_recovery_checkpoint_list()
    current_trial = enrich_current_trial(read_live_state_payload(STATE_KEY_CURRENT_TRIAL, 'current_trial.json'))
    current_stage_payload = normalize_stage_request_payload(current_trial) if is_current_trial_active(current_trial) else None
    current_checkpoint_id = build_checkpoint_id(current_stage_payload)
    restartable_checkpoint_list = []
    for checkpoint in checkpoint_list:
        checkpoint_id = str(checkpoint.get('checkpoint_id') or '').strip()
        checkpoint_status = str(checkpoint.get('status') or '').strip().lower()
        if checkpoint_status == 'observed' or (current_checkpoint_id and checkpoint_id == current_checkpoint_id):
            restartable_checkpoint_list.append(dict(checkpoint))
    return restartable_checkpoint_list


def build_team_status_list(team_registry: list[dict]) -> list[dict]:
    team_live_status_by_team_id = load_team_live_status_map()
    scoreboard_row_by_team_id = {
        str(row.get('team_id')): row
        for row in load_scoreboard_rows(team_registry)
        if row.get('team_id') is not None
    }
    team_status_list = []
    for team_info in team_registry:
        team_id = team_info.get('team_id')
        live_status = team_live_status_by_team_id.get(str(team_id), {})
        scoreboard_row = scoreboard_row_by_team_id.get(str(team_id), {})
        merged_status = dict(team_info)
        merged_status.update(
            {
                'connection_status': 'disconnected',
                'run_status': 'idle',
                'calibration_status': 'pending',
                'calibration_ready': False,
                'current_total_score': 0.0,
                'observed_trial_count': 0,
            }
        )
        merged_status.update(live_status)
        if scoreboard_row:
            merged_status.setdefault('scoreboard_row', scoreboard_row)
            merged_status['current_total_score'] = to_float(
                scoreboard_row.get('total_score'),
                merged_status.get('current_total_score'),
            )
            merged_status['observed_trial_count'] = int(
                to_float(
                    scoreboard_row.get('observed_trial_count'),
                    merged_status.get('observed_trial_count'),
                )
            )
            merged_status['mean_accuracy_percent'] = to_float(scoreboard_row.get('mean_accuracy_percent'))
            merged_status['avg_reaction_time_ms'] = to_float(scoreboard_row.get('avg_reaction_time_ms'))
        team_status_list.append(merged_status)

    team_status_list.sort(
        key=lambda item: (
            -to_float(item.get('current_total_score')),
            item.get('team_id') or '',
        )
    )
    return team_status_list


def resolve_team_display_label(team_info: dict | None) -> str:
    if not isinstance(team_info, dict):
        return ''
    team_id = str(team_info.get('team_id') or '').strip()
    team_display_name = str(team_info.get('team_display_name') or '').strip()
    if team_display_name and team_display_name != team_id:
        return f'{team_display_name}({team_id})' if team_id else team_display_name
    return team_display_name or team_id


def load_scoreboard_rows(team_registry: list[dict]) -> list[dict]:
    try:
        scoreboard_row_list = load_team_score_overview_rows(RUNTIME_STATE_DB_PATH)
    except sqlite3.OperationalError:
        LOGGER.warning(
            '读取 team_score_overview 失败，回退到 CSV/结果目录: db_path=%s',
            RUNTIME_STATE_DB_PATH,
            exc_info=True,
        )
        scoreboard_row_list = []
    if not scoreboard_row_list:
        team_overview_file_path = RESULTS_ROOT / '00_team_score_overview.csv'
        if team_overview_file_path.exists():
            scoreboard_row_list = read_csv_rows(team_overview_file_path)
        else:
            for team_info in team_registry:
                team_id = team_info.get('team_id')
                if team_id is None:
                    continue
                team_file_path = RESULTS_ROOT / str(team_id) / '00_team_overview.csv'
                if not team_file_path.exists():
                    continue
                first_row = read_csv_first_row(team_file_path)
                if first_row:
                    scoreboard_row_list.append(first_row)

    for row in scoreboard_row_list:
        total_score = to_float(row.get('total_score'), 0.0)
        row['average_score'] = total_score

    scoreboard_row_list.sort(
        key=lambda item: (
            -to_float(item.get('total_score')),
            item.get('team_id') or '',
        )
    )
    for rank_index, row in enumerate(scoreboard_row_list, start=1):
        row['rank'] = rank_index
    return scoreboard_row_list


def read_csv_rows(file_path: Path) -> list[dict]:
    with file_path.open('r', encoding='utf-8-sig', newline='') as csv_file:
        return list(csv.DictReader(csv_file))


def read_csv_first_row(file_path: Path) -> dict | None:
    row_list = read_csv_rows(file_path)
    return row_list[0] if row_list else None


def load_match_control_status() -> dict:
    payload = read_live_state_payload(STATE_KEY_MATCH_CONTROL_STATUS, 'match_control_status.json') or {}
    pending_recovery_request = load_pending_recovery_request_status()
    match_started = bool(payload.get('match_started'))
    waiting_start = payload.get('waiting_start')
    if waiting_start is None:
        waiting_start = not match_started
    return {
        'waiting_start': bool(waiting_start),
        'match_started': match_started,
        'match_finished': bool(payload.get('match_finished')),
        'finished_at': payload.get('finished_at'),
        'finished_team_id_list': [
            str(team_id)
            for team_id in (payload.get('finished_team_id_list') or [])
            if str(team_id).strip()
        ],
        'pause_requested': bool(payload.get('pause_requested')),
        'paused': bool(payload.get('paused')),
        'started_at': payload.get('started_at'),
        'paused_at': payload.get('paused_at'),
        'resumed_at': payload.get('resumed_at'),
        'last_seen_start_request_at': payload.get('last_seen_start_request_at'),
        'last_seen_pause_request_at': payload.get('last_seen_pause_request_at'),
        'last_seen_resume_request_at': payload.get('last_seen_resume_request_at'),
        'pending_recovery_mode': pending_recovery_request.get('recovery_mode'),
        'pending_recovery_request': pending_recovery_request.get('stage'),
        'pending_recovery_request_at': pending_recovery_request.get('requested_at'),
        'pending_recovery_valid': pending_recovery_request.get('valid', False),
        'pending_restart_stage_request': pending_recovery_request.get('stage'),
        'pending_restart_stage_request_at': pending_recovery_request.get('requested_at'),
        'pending_restart_stage_valid': pending_recovery_request.get('valid', False),
        'coordinator_started_at': payload.get('coordinator_started_at'),
        'updated_at': payload.get('updated_at'),
    }


def build_start_match_readiness(team_registry: list[dict], team_status_list: list[dict], runtime_stage_status: dict | None) -> dict:
    team_info_by_team_id = {
        str(team_info.get('team_id') or '').strip(): team_info
        for team_info in team_registry
        if str(team_info.get('team_id') or '').strip()
    }
    applied_recovery = load_applied_recovery_status()
    configured_team_id_list = [
        str(team_info.get('team_id') or '').strip()
        for team_info in team_registry
        if str(team_info.get('team_id') or '').strip()
    ]
    connected_team_id_set = {
        str(team_status.get('team_id') or '').strip()
        for team_status in team_status_list
        if str(team_status.get('connection_status') or '').lower() == 'connected'
    }
    pending_team_id_list = [
        team_id
        for team_id in configured_team_id_list
        if team_id not in connected_team_id_set
    ]
    pending_team_display_list = [
        resolve_team_display_label(team_info_by_team_id.get(team_id)) or team_id
        for team_id in pending_team_id_list
    ]
    calibration_ready_team_id_set = {
        str(team_status.get('team_id') or '').strip()
        for team_status in team_status_list
        if str(team_status.get('connection_status') or '').lower() == 'connected'
        and bool(team_status.get('calibration_ready'))
    }
    configured_team_id_list_by_group: dict[str, list[str]] = {}
    for team_info in team_registry:
        group_id = str(team_info.get('group_id') or '').strip()
        team_id = str(team_info.get('team_id') or '').strip()
        if group_id and team_id:
            configured_team_id_list_by_group.setdefault(group_id, []).append(team_id)
    runtime_group_status_by_group_id = {
        str(group_status.get('group_id') or '').strip(): group_status
        for group_status in (runtime_stage_status or {}).get('group_status_list') or []
        if str(group_status.get('group_id') or '').strip()
    }
    group_readiness_list: list[dict] = []
    for group_id in sorted(configured_team_id_list_by_group):
        group_status = runtime_group_status_by_group_id.get(group_id) or {}
        stage_status_list = list(group_status.get('stage_status_list') or [])
        collector_ready = any(
            bool(stage_status.get('collector_prepared'))
            or bool(stage_status.get('online_stage_released'))
            or int(stage_status.get('completed_trial_count') or 0) > 0
            for stage_status in stage_status_list
        )
        group_readiness_list.append(
            {
                'group_id': group_id,
                'configured_team_id_list': configured_team_id_list_by_group.get(group_id, []),
                'stage_observed': bool(stage_status_list),
                'collector_ready': collector_ready,
            }
        )
    pending_group_id_list = [
        group_readiness.get('group_id')
        for group_readiness in group_readiness_list
        if not bool(group_readiness.get('collector_ready'))
    ]
    reason_list: list[str] = []
    if not configured_team_id_list:
        reason_list.append('未检测到参赛队配置，不能开始比赛')
    if pending_team_id_list:
        reason_list.append(f"仍有赛队未完成开赛前准备: {', '.join(pending_team_display_list)}")
    recovery_mode = str(applied_recovery.get('recovery_mode') or '').strip()
    recovery_stage = normalize_stage_request_payload(applied_recovery.get('stage'))
    pending_calibration_team_id_list: list[str] = []
    if recovery_mode == 'restart_from_stage' and recovery_stage is not None:
        pending_calibration_team_id_list = [
            team_id
            for team_id in configured_team_id_list
            if team_id in connected_team_id_set and team_id not in calibration_ready_team_id_set
        ]
    return {
        'ready': len(reason_list) == 0,
        'reason_list': reason_list,
        'configured_team_id_list': configured_team_id_list,
        'connected_team_id_list': sorted(team_id for team_id in connected_team_id_set if team_id),
        'calibration_ready_team_id_list': sorted(team_id for team_id in calibration_ready_team_id_set if team_id),
        'pending_team_id_list': pending_team_id_list,
        'pending_calibration_team_id_list': pending_calibration_team_id_list,
        'pending_group_id_list': pending_group_id_list,
        'group_readiness_list': group_readiness_list,
        'recovery_mode': recovery_mode or None,
        'recovery_stage': recovery_stage,
        'updated_at': time.time(),
    }


def build_match_summary(team_registry: list[dict]) -> dict:
    team_summary_list = load_match_summary_team_list(team_registry)
    scoreboard_row_list = merge_scoreboard_with_registry(load_scoreboard_rows(team_registry), team_registry)
    current_trial = read_live_state_payload(STATE_KEY_CURRENT_TRIAL, 'current_trial.json')
    match_control_status = load_match_control_status()
    runtime_stage_status = load_runtime_stage_status()
    total_observed_trial_count = sum(int(team_summary.get('observed_trial_count') or 0) for team_summary in team_summary_list)
    total_timeout_count = sum(int(team_summary.get('timeout_count') or 0) for team_summary in team_summary_list)
    finished_team_count = sum(
        1 for team_summary in team_summary_list if str(team_summary.get('run_status') or '').lower() == 'finished'
    )
    return {
        'match_name': ((CONFIG.get('match') or {}).get('match_name')) or 'BCI Competition Final',
        'match_status': resolve_match_status(
            current_trial,
            build_team_status_list(team_registry),
            match_control_status,
            runtime_stage_status,
        ),
        'team_count': len(team_summary_list),
        'finished_team_count': finished_team_count,
        'match_finished': bool(match_control_status.get('match_finished')),
        'finished_at': match_control_status.get('finished_at'),
        'total_observed_trial_count': total_observed_trial_count,
        'total_timeout_count': total_timeout_count,
        'timeout_rate_percent': safe_percentage(total_timeout_count, total_observed_trial_count),
        'scoreboard': scoreboard_row_list,
        'task_summary_list': build_match_task_summary_list(team_summary_list),
        'updated_at': time.time(),
    }


def load_match_summary_team_list(team_registry: list[dict]) -> list[dict]:
    team_summary_list = []
    for team_id in collect_team_id_list(team_registry):
        team_summary = load_match_summary_team(team_registry, team_id)
        if team_summary is not None:
            team_summary_list.append(team_summary)
    team_summary_list.sort(
        key=lambda item: (
            int(item.get('rank') or 999999),
            item.get('team_id') or '',
        )
    )
    return team_summary_list


def load_match_summary_team(team_registry: list[dict], team_id: str) -> dict | None:
    if team_id is None:
        return None
    team_id_text = str(team_id)
    team_info_by_id = {str(item.get('team_id')): item for item in team_registry}
    scoreboard_row_by_team_id = {
        str(row.get('team_id')): row
        for row in load_scoreboard_rows(team_registry)
        if row.get('team_id') is not None
    }
    team_dir = RESULTS_ROOT / team_id_text
    live_status = load_team_live_status_map().get(team_id_text, {})
    scoreboard_row = scoreboard_row_by_team_id.get(team_id_text, {})
    team_info = team_info_by_id.get(team_id_text, {})

    team_overview_row = load_team_overview_row(RUNTIME_STATE_DB_PATH, team_id_text)
    if not team_overview_row:
        team_overview_row = read_csv_first_row(team_dir / '00_team_overview.csv') or scoreboard_row or {}

    task_overview_row_list = load_team_task_overview_rows(RUNTIME_STATE_DB_PATH, team_id_text)
    if not task_overview_row_list and (team_dir / '01_task_overview.csv').exists():
        task_overview_row_list = read_csv_rows(team_dir / '01_task_overview.csv')

    subject_task_overview_row_list = load_team_subject_task_overview_rows(RUNTIME_STATE_DB_PATH, team_id_text)
    if not subject_task_overview_row_list and (team_dir / '02_subject_task_overview.csv').exists():
        subject_task_overview_row_list = read_csv_rows(team_dir / '02_subject_task_overview.csv')

    trial_record_row_list = load_team_trial_record_rows(RUNTIME_STATE_DB_PATH, team_id_text)
    if not trial_record_row_list and (team_dir / '03_trial_records.csv').exists():
        trial_record_row_list = read_csv_rows(team_dir / '03_trial_records.csv')

    if not team_overview_row and not task_overview_row_list and not subject_task_overview_row_list and not trial_record_row_list and not live_status and not team_info:
        return None

    task_timeout_count_dict: dict[str, int] = defaultdict(int)
    task_trial_count_dict: dict[str, int] = defaultdict(int)
    for row in trial_record_row_list:
        task_id = str(row.get('task_id') or '')
        if task_id == '':
            continue
        task_trial_count_dict[task_id] += 1
        if to_bool(row.get('is_timeout')):
            task_timeout_count_dict[task_id] += 1

    task_summary_list = []
    for task_row in task_overview_row_list:
        task_id = str(task_row.get('task_id') or '')
        observed_trial_count = int(to_float(task_row.get('observed_trial_count'), task_trial_count_dict.get(task_id, 0)))
        timeout_count = int(task_timeout_count_dict.get(task_id, 0))
        task_summary_list.append(
            {
                'task_id': task_id,
                'exp_name': task_row.get('exp_name'),
                'exp_task': task_row.get('exp_task'),
                'task_status': task_row.get('task_status') or 'unknown',
                'subject_count': int(to_float(task_row.get('subject_count'))),
                'observed_trial_count': observed_trial_count,
                'accuracy_percent': to_float(task_row.get('accuracy_percent')),
                'avg_reaction_time_ms': to_float(task_row.get('avg_reaction_time_ms')),
                'task_score': to_float(task_row.get('task_score')),
                'average_score': to_float(task_row.get('task_score')),
                'timeout_count': timeout_count,
                'timeout_rate_percent': safe_percentage(timeout_count, observed_trial_count),
                'updated_at': task_row.get('updated_at'),
            }
        )

    timeout_count = sum(task_timeout_count_dict.values())
    observed_trial_count = int(to_float(team_overview_row.get('observed_trial_count'), len(trial_record_row_list)))
    return {
        'team_id': team_id_text,
        'team_display_name': (
            live_status.get('team_display_name')
            or team_info.get('team_display_name')
            or team_overview_row.get('team_id')
            or team_id_text
        ),
        'rank': int(to_float(scoreboard_row.get('rank'), 0)),
        'run_status': (
            live_status.get('run_status')
            or team_overview_row.get('run_status')
            or 'idle'
        ),
        'total_score': to_float(team_overview_row.get('total_score')),
        'observed_trial_count': observed_trial_count,
        'mean_accuracy_percent': to_float(team_overview_row.get('mean_accuracy_percent')),
        'avg_reaction_time_ms': to_float(team_overview_row.get('avg_reaction_time_ms')),
        'configured_task_count': int(to_float(team_overview_row.get('configured_task_count'))),
        'started_task_count': int(to_float(team_overview_row.get('started_task_count'))),
        'started_task_names': split_pipe_values(team_overview_row.get('started_task_names')),
        'timeout_count': timeout_count,
        'timeout_rate_percent': safe_percentage(timeout_count, observed_trial_count),
        'task_summary_list': task_summary_list,
        'subject_task_summary_list': normalize_subject_task_summary(subject_task_overview_row_list),
        'final_score_result': live_status.get('final_score_result'),
        'updated_at': live_status.get('updated_at') or team_overview_row.get('updated_at') or time.time(),
    }


def normalize_subject_task_summary(row_list: list[dict]) -> list[dict]:
    normalized_row_list = []
    for row in row_list:
        normalized_row_list.append(
            {
                'subject_id': row.get('subject_id'),
                'task_id': row.get('task_id'),
                'exp_name': row.get('exp_name'),
                'exp_task': row.get('exp_task'),
                'task_status': row.get('task_status') or 'unknown',
                'observed_trial_count': int(to_float(row.get('observed_trial_count'))),
                'accuracy_percent': to_float(row.get('accuracy_percent')),
                'updated_at': row.get('updated_at'),
            }
        )
    return normalized_row_list


def build_match_task_summary_list(team_summary_list: list[dict]) -> list[dict]:
    aggregate_dict: dict[str, dict] = {}
    for team_summary in team_summary_list:
        for task_summary in team_summary.get('task_summary_list') or []:
            task_id = str(task_summary.get('task_id') or '')
            if task_id == '':
                continue
            aggregate = aggregate_dict.setdefault(
                task_id,
                {
                    'task_id': task_id,
                    'exp_name': task_summary.get('exp_name'),
                    'exp_task': task_summary.get('exp_task'),
                    'team_count': 0,
                    'finished_team_count': 0,
                    'total_observed_trial_count': 0,
                    'total_timeout_count': 0,
                    'accuracy_weighted_sum': 0.0,
                    'task_score_sum': 0.0,
                },
            )
            aggregate['team_count'] += 1
            if str(task_summary.get('task_status') or '').lower() == 'finished':
                aggregate['finished_team_count'] += 1
            observed_trial_count = int(task_summary.get('observed_trial_count') or 0)
            aggregate['total_observed_trial_count'] += observed_trial_count
            aggregate['total_timeout_count'] += int(task_summary.get('timeout_count') or 0)
            aggregate['accuracy_weighted_sum'] += to_float(task_summary.get('accuracy_percent')) * observed_trial_count
            aggregate['task_score_sum'] += to_float(task_summary.get('task_score'))

    task_summary_list = []
    for task_id, aggregate in sorted(aggregate_dict.items()):
        team_count = int(aggregate.get('team_count') or 0)
        total_observed_trial_count = int(aggregate.get('total_observed_trial_count') or 0)
        total_timeout_count = int(aggregate.get('total_timeout_count') or 0)
        mean_accuracy_percent = (
            aggregate['accuracy_weighted_sum'] / total_observed_trial_count
            if total_observed_trial_count > 0
            else 0.0
        )
        mean_task_score = (
            aggregate['task_score_sum'] / team_count
            if team_count > 0
            else 0.0
        )
        task_summary_list.append(
            {
                'task_id': task_id,
                'exp_name': aggregate.get('exp_name'),
                'exp_task': aggregate.get('exp_task'),
                'team_count': team_count,
                'finished_team_count': int(aggregate.get('finished_team_count') or 0),
                'mean_accuracy_percent': mean_accuracy_percent,
                'mean_task_score': mean_task_score,
                'total_observed_trial_count': total_observed_trial_count,
                'total_timeout_count': total_timeout_count,
                'timeout_rate_percent': safe_percentage(total_timeout_count, total_observed_trial_count),
            }
        )
    return task_summary_list


def collect_team_id_list(team_registry: list[dict]) -> list[str]:
    team_id_set = {
        str(team_info.get('team_id'))
        for team_info in team_registry
        if team_info.get('team_id') is not None
    }
    if RESULTS_ROOT.exists():
        for item in RESULTS_ROOT.iterdir():
            item_name = str(item.name or '')
            if item_name in {'live', 'control', 'history'}:
                continue
            if item_name.endswith(('.db', '.db-shm', '.db-wal', '.sqlite', '.sqlite-shm', '.sqlite-wal')):
                continue
            try:
                if item.is_dir():
                    team_id_set.add(item_name)
            except PermissionError:
                continue
    return sorted(team_id for team_id in team_id_set if team_id)


def merge_scoreboard_with_registry(scoreboard_row_list: list[dict], team_registry: list[dict]) -> list[dict]:
    team_display_name_by_team_id = {
        str(team_info.get('team_id')): team_info.get('team_display_name')
        for team_info in team_registry
        if team_info.get('team_id') is not None
    }
    merged_row_list = []
    for row in scoreboard_row_list:
        team_id = str(row.get('team_id') or '')
        merged_row = dict(row)
        merged_row['team_display_name'] = team_display_name_by_team_id.get(team_id, team_id)
        merged_row_list.append(merged_row)
    return merged_row_list


TERMINAL_RUN_STATUS_SET = {'finished', 'closed', 'error', 'startup_failed', 'stopped'}


def resolve_match_status(
    current_trial: dict | None,
    team_status_list: list[dict],
    match_control_status: dict | None,
    runtime_stage_status: dict | None = None,
) -> str:
    match_started = bool((match_control_status or {}).get('match_started'))
    if bool((match_control_status or {}).get('match_finished')):
        return 'finished'
    if bool((match_control_status or {}).get('paused')):
        return 'paused'
    if is_current_trial_active(current_trial):
        return 'running'

    run_status_list = [str(team_status.get('run_status') or '').lower() for team_status in team_status_list]
    non_empty_run_status_list = [run_status for run_status in run_status_list if run_status]

    if match_started:
        return 'running'

    return 'waiting_start'


def has_incomplete_runtime_stage(runtime_stage_status: dict | None) -> bool:
    if not isinstance(runtime_stage_status, dict):
        return False
    for group_status in runtime_stage_status.get('group_status_list') or []:
        for stage_status in group_status.get('stage_status_list') or []:
            if is_stage_incomplete(stage_status):
                return True
    return False


def is_stage_incomplete(stage_status: dict | None) -> bool:
    if not isinstance(stage_status, dict):
        return False
    if bool(stage_status.get('collector_prepared')) and not bool(stage_status.get('online_stage_released')):
        return True
    if stage_status.get('pending_release') not in (None, {}, []):
        return True
    online_trial_count = int(to_float(stage_status.get('online_trial_count'), 0))
    completed_trial_count = int(to_float(stage_status.get('completed_trial_count'), 0))
    if online_trial_count > 0 and completed_trial_count < online_trial_count:
        return True
    return False


def is_current_trial_active(current_trial: dict | None) -> bool:
    if not isinstance(current_trial, dict):
        return False
    if str(current_trial.get('status') or '').lower() != 'running':
        return False
    next_release_target_wallclock = to_float(current_trial.get('next_release_target_wallclock'), 0.0)
    if next_release_target_wallclock > 0:
        return time.time() <= next_release_target_wallclock
    release_wallclock = to_float(current_trial.get('release_wallclock'), 0.0)
    dispatch_wallclock = to_float(current_trial.get('dispatch_wallclock'), 0.0)
    if release_wallclock > 0 and dispatch_wallclock > 0:
        return True
    cycle_end_wallclock = to_float(current_trial.get('cycle_end_wallclock'), 0.0)
    if cycle_end_wallclock <= 0:
        return True
    return time.time() <= cycle_end_wallclock


def clear_results_root() -> None:
    archive_and_clear_active_results(PROJECT_ROOT, 'judgeweb_clear_results')
    LIVE_ROOT.mkdir(parents=True, exist_ok=True)
    CONTROL_ROOT.mkdir(parents=True, exist_ok=True)
    (RESULTS_ROOT / 'history').mkdir(parents=True, exist_ok=True)


def schedule_stage_restart_resume() -> dict:
    helper_path = AUTO_RESTART_RESUME_HELPER_SCRIPT_PATH
    if not helper_path.exists():
        raise HTTPException(status_code=500, detail=f'缺少自动恢复脚本: {helper_path}')
    try:
        subprocess.Popen(
            [
                'powershell',
                '-NoProfile',
                '-ExecutionPolicy',
                'Bypass',
                '-Command',
                (
                    "Start-Process "
                    f"-FilePath '{helper_path}' "
                    f"-WorkingDirectory '{PROJECT_ROOT}'"
                ),
            ],
            creationflags=getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0),
            close_fds=True,
        )
    except OSError as exc:
        LOGGER.exception('调度指定阶段重跑自动恢复失败: helper=%s', helper_path)
        raise HTTPException(status_code=500, detail=f'自动调度指定阶段重跑失败: {exc}') from exc
    LOGGER.warning('已调度指定阶段重跑自动恢复: helper=%s detached=True', helper_path)
    return {
        'scheduled': True,
        'helper_path': str(helper_path),
        'scheduled_at': time.time(),
    }


def write_control_request(request_type: str, payload: dict) -> dict:
    CONTROL_ROOT.mkdir(parents=True, exist_ok=True)
    request_payload = {
        'request_type': request_type,
        'payload': payload,
        'requested_at': time.time(),
    }
    file_path = CONTROL_ROOT / f'{request_type}.json'
    safe_write_json_file(file_path, request_payload, log_name=f'control_request:{request_type}')
    return request_payload


def load_control_request(request_type: str) -> dict | None:
    file_path = CONTROL_ROOT / f'{request_type}.json'
    if not file_path.exists():
        return None
    try:
        return json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        LOGGER.exception('读取控制请求失败: request_type=%s file_path=%s', request_type, file_path)
        return None


def delete_control_request(request_type: str) -> None:
    file_path = CONTROL_ROOT / f'{request_type}.json'
    try:
        file_path.unlink(missing_ok=True)
    except OSError:
        LOGGER.exception('删除控制请求失败: request_type=%s file_path=%s', request_type, file_path)


def normalize_stage_request_payload(payload) -> dict | None:
    return normalize_recovery_stage_payload(payload)


def build_checkpoint_id(stage_payload: dict | None) -> str:
    return build_recovery_checkpoint_id(stage_payload)


def load_applied_recovery_status() -> dict:
    launcher_manifest = load_json_file(LAUNCHER_MANIFEST_PATH)
    applied_recovery = launcher_manifest.get('applied_recovery') or {}
    if not isinstance(applied_recovery, dict):
        return {
            'recovery_mode': None,
            'stage': None,
            'requested_at': None,
            'applied_at': None,
            'history_archive': None,
        }
    return {
        'recovery_mode': str(applied_recovery.get('recovery_mode') or '').strip() or None,
        'stage': normalize_stage_request_payload(applied_recovery.get('stage')),
        'requested_at': applied_recovery.get('requested_at'),
        'applied_at': applied_recovery.get('applied_at'),
        'history_archive': applied_recovery.get('history_archive'),
    }


def write_recovery_request(recovery_mode: str, stage_payload: dict | None) -> dict:
    delete_control_request('resume_request')
    delete_control_request('restart_stage_request')
    payload = {
        'recovery_mode': str(recovery_mode or '').strip(),
        'stage': normalize_stage_request_payload(stage_payload),
    }
    request_payload = {
        'request_type': 'recovery_request',
        'payload': payload,
        'requested_at': time.time(),
    }
    safe_write_json_file(
        CONTROL_ROOT / RECOVERY_REQUEST_FILE_NAME,
        request_payload,
        log_name='control_request:recovery_request',
    )
    return request_payload


def resolve_recommended_recovery_stage(checkpoint_list: list[dict], current_trial: dict | None) -> dict | None:
    stage_payload = normalize_stage_request_payload(current_trial)
    if stage_payload is not None:
        return stage_payload
    checkpoint_stage_list = [
        normalize_stage_request_payload(checkpoint)
        for checkpoint in checkpoint_list
    ]
    checkpoint_stage_list = [checkpoint for checkpoint in checkpoint_stage_list if checkpoint is not None]
    if checkpoint_stage_list:
        return checkpoint_stage_list[0]
    return None


def load_pending_recovery_request_status() -> dict:
    request_payload = load_pending_recovery_request(CONTROL_ROOT)
    if request_payload is None:
        return {
            'recovery_mode': None,
            'stage': None,
            'requested_at': None,
            'valid': False,
            'message': None,
        }
    raw_stage_payload = request_payload.get('stage')
    stage_payload = None
    stage_resolution_error = None
    if isinstance(raw_stage_payload, dict):
        try:
            stage_payload = resolve_recovery_stage_payload(PROJECT_ROOT, raw_stage_payload)
        except ValueError as exc:
            stage_resolution_error = str(exc)
    is_valid = True
    if isinstance(raw_stage_payload, dict) and stage_payload is None:
        is_valid = False
    elif stage_payload is not None:
        checkpoint_id = build_checkpoint_id(stage_payload)
        checkpoint_id_set = {
            str(checkpoint.get('checkpoint_id') or '').strip()
            for checkpoint in load_restartable_recovery_checkpoint_list()
        }
        is_valid = checkpoint_id in checkpoint_id_set
    return {
        'recovery_mode': request_payload.get('recovery_mode'),
        'stage': stage_payload,
        'requested_at': request_payload.get('requested_at'),
        'valid': is_valid,
        'message': (
            (
                '已记录恢复目标。指定阶段重跑会由系统自动调度裁判侧恢复。'
                if request_payload.get('recovery_mode') == 'restart_from_stage'
                else '已记录恢复目标。'
            )
            if is_valid else (
                stage_resolution_error
                or '当前记录的重跑目标尚未跑到或不再可用，请重新选择。'
            )
        ),
    }


def load_pending_restart_stage_request() -> dict:
    pending_recovery_request = load_pending_recovery_request_status()
    return {
        'stage': pending_recovery_request.get('stage'),
        'requested_at': pending_recovery_request.get('requested_at'),
        'valid': pending_recovery_request.get('valid', False),
        'message': pending_recovery_request.get('message'),
    }


def enrich_current_trial(current_trial: dict | None) -> dict | None:
    if not isinstance(current_trial, dict):
        return current_trial
    enriched_trial = dict(current_trial)
    subject_id = str(current_trial.get('subject_id') or '').strip()
    subject_id_list = load_subject_registry()
    subject_index = None
    if subject_id and subject_id in subject_id_list:
        subject_index = subject_id_list.index(subject_id) + 1
    elif subject_id:
        subject_id_list = subject_id_list + [subject_id]
        subject_index = len(subject_id_list)
    enriched_trial['current_subject_index'] = subject_index
    enriched_trial['total_subject_count'] = len(subject_id_list)
    return enriched_trial


def load_subject_registry() -> list[str]:
    config_dict = load_yaml_file(VIRTUAL_RECEIVER_CONFIG_PATH)
    data_files_dict = (config_dict.get('data_files') or {})
    subject_id_list = [str(subject_id) for subject_id in data_files_dict.keys() if str(subject_id).strip()]
    if subject_id_list:
        return subject_id_list
    discovered_subject_id_set: set[str] = set()
    runtime_stage_status = load_runtime_stage_status()
    for group_status in runtime_stage_status.get('group_status_list') or []:
        for stage_status in group_status.get('stage_status_list') or []:
            subject_id = str(((stage_status.get('stage_context') or {}).get('subject_id')) or '').strip()
            if subject_id:
                discovered_subject_id_set.add(subject_id)
    current_trial = read_live_state_payload(STATE_KEY_CURRENT_TRIAL, 'current_trial.json') or {}
    current_subject_id = str(current_trial.get('subject_id') or '').strip()
    if current_subject_id:
        discovered_subject_id_set.add(current_subject_id)
    return sorted(discovered_subject_id_set)


def safe_write_json_file(file_path: Path, payload: dict, log_name: str) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    serialized_payload = json.dumps(payload, ensure_ascii=False, indent=2)
    temp_file_path = file_path.with_suffix(file_path.suffix + '.tmp')
    last_exception = None
    for attempt in range(5):
        try:
            temp_file_path.write_text(serialized_payload, encoding='utf-8')
            temp_file_path.replace(file_path)
            return
        except PermissionError as exc:
            last_exception = exc
            time.sleep(0.02 * (attempt + 1))
        except OSError as exc:
            last_exception = exc
            break
    try:
        file_path.write_text(serialized_payload, encoding='utf-8')
    except OSError:
        LOGGER.exception(
            '写入 JSON 文件失败: log_name=%s file_path=%s last_exception=%s',
            log_name,
            file_path,
            repr(last_exception),
        )
    finally:
        try:
            if temp_file_path.exists():
                temp_file_path.unlink()
        except OSError:
            LOGGER.debug('清理临时文件失败: %s', temp_file_path)


def split_pipe_values(value) -> list[str]:
    if value is None:
        return []
    value_text = str(value).strip()
    if value_text == '':
        return []
    return [part for part in value_text.split('|') if part]


def safe_percentage(numerator: int | float, denominator: int | float) -> float:
    numerator_value = to_float(numerator)
    denominator_value = to_float(denominator)
    if denominator_value <= 0:
        return 0.0
    return (numerator_value / denominator_value) * 100.0


def to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    value_text = str(value).strip().lower()
    return value_text in {'1', 'true', 'yes', 'y'}


def to_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == '':
            return float(default)
        parsed_value = float(value)
        if not math.isfinite(parsed_value):
            return float(default)
        return parsed_value
    except (TypeError, ValueError):
        return float(default)


def sanitize_json_compatible(value):
    if isinstance(value, dict):
        return {str(key): sanitize_json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_compatible(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_compatible(item) for item in value]
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return None
    return value


if __name__ == '__main__':
    server_config = CONFIG.get('server') or {}
    uvicorn.run(
        'JudgeWeb.main:app',
        host=str(server_config.get('host') or '127.0.0.1'),
        port=int(server_config.get('port') or 18080),
        reload=False,
    )
