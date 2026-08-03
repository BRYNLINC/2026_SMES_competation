from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import logging.handlers
import sys
import types
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JUDGE_WEB_MAIN_PATH = PROJECT_ROOT / "app" / "JudgeWeb" / "JudgeWeb" / "main.py"


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("judge_web")]


def _load_judge_web_module():
    stub_module_map: dict[str, types.ModuleType] = {}

    def register_module(name: str, module: types.ModuleType) -> None:
        stub_module_map[name] = sys.modules.get(name)
        sys.modules[name] = module

    uvicorn_module = types.ModuleType("uvicorn")
    uvicorn_module.run = lambda *args, **kwargs: None
    register_module("uvicorn", uvicorn_module)

    fastapi_module = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class FastAPI:
        def __init__(self, *args, **kwargs):
            self.routes = []

        def add_middleware(self, *args, **kwargs):
            return None

        def middleware(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        def get(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        post = get
        websocket = get

    fastapi_module.FastAPI = FastAPI
    fastapi_module.HTTPException = HTTPException
    fastapi_module.Request = type("Request", (), {})
    fastapi_module.WebSocket = type("WebSocket", (), {})
    fastapi_module.WebSocketDisconnect = type("WebSocketDisconnect", (Exception,), {})
    register_module("fastapi", fastapi_module)

    cors_module = types.ModuleType("fastapi.middleware.cors")
    cors_module.CORSMiddleware = type("CORSMiddleware", (), {})
    register_module("fastapi.middleware.cors", cors_module)

    responses_module = types.ModuleType("fastapi.responses")

    class JSONResponse(dict):
        def __init__(self, status_code: int, content: dict):
            super().__init__(content)
            self.status_code = status_code
            self.content = content

    responses_module.JSONResponse = JSONResponse
    register_module("fastapi.responses", responses_module)

    spec = importlib.util.spec_from_file_location("judge_web_under_test", JUDGE_WEB_MAIN_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    original_timed_rotating_handler = logging.handlers.TimedRotatingFileHandler

    class DummyTimedRotatingFileHandler(logging.Handler):
        def __init__(self, *args, **kwargs):
            super().__init__()

        def emit(self, record):
            return None

    logging.handlers.TimedRotatingFileHandler = DummyTimedRotatingFileHandler
    try:
        spec.loader.exec_module(module)
    finally:
        logging.handlers.TimedRotatingFileHandler = original_timed_rotating_handler
        for name, original in stub_module_map.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
    return module


judge_web = _load_judge_web_module()


@pytest.mark.test_id("JW-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("JudgeWeb 默认只允许本机访问")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="is_local_only_enabled/is_loopback_host")
def test_local_only_and_loopback_host_defaults() -> None:
    assert judge_web.is_local_only_enabled() is True
    assert judge_web.is_loopback_host("127.0.0.1") is True
    assert judge_web.is_loopback_host("localhost") is True
    assert judge_web.is_loopback_host("192.168.1.10") is False


@pytest.mark.test_id("JW-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("坏 JSON 文件读取时返回空 dict")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="load_json_file")
def test_load_json_file_returns_empty_dict_for_invalid_json(tmp_path: Path) -> None:
    file_path = tmp_path / "bad.json"
    file_path.write_text("{bad", encoding="utf-8")
    logger = getattr(judge_web, "LOGGER")
    original_level = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        assert judge_web.load_json_file(file_path) == {}
    finally:
        logger.setLevel(original_level)


@pytest.mark.test_id("JW-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("safe_write_json_file 应原子写入合法 JSON")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="safe_write_json_file")
def test_safe_write_json_file_writes_json_payload(tmp_path: Path) -> None:
    file_path = tmp_path / "control" / "request.json"
    payload = {"a": 1, "b": "测试"}
    judge_web.safe_write_json_file(file_path, payload, log_name="unit")
    assert json.loads(file_path.read_text(encoding="utf-8")) == payload


@pytest.mark.test_id("JW-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("checkpoint id 构建与 recovery 工具保持一致")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="build_checkpoint_id")
def test_build_checkpoint_id() -> None:
    assert judge_web.build_checkpoint_id(None) == ""
    assert judge_web.build_checkpoint_id(
        {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest"}
    ) == ""
    assert judge_web.build_checkpoint_id(
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session2",
        }
    ) == "S1|vme|left_vs_rest|session2"


@pytest.mark.test_id("JW-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("比赛状态判定优先 finished 和 paused")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="resolve_match_status")
def test_resolve_match_status_priority() -> None:
    assert judge_web.resolve_match_status(None, [], {"match_finished": True}, {}) == "finished"
    assert judge_web.resolve_match_status(None, [], {"paused": True}, {}) == "paused"


@pytest.mark.test_id("JW-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("比赛未开始时状态为 waiting_start")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="resolve_match_status")
def test_resolve_match_status_not_started() -> None:
    assert judge_web.resolve_match_status(None, [], {"match_started": False}, {}) == "waiting_start"


@pytest.mark.test_id("JW-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("当前 trial 活跃时状态为 running")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="resolve_match_status/is_current_trial_active")
def test_resolve_match_status_running_when_current_trial_active() -> None:
    current_trial = {
        "trial_id": "1",
        "status": "running",
        "release_wallclock": 1.0,
        "dispatch_wallclock": 1.0,
    }
    assert judge_web.is_current_trial_active(current_trial) is True
    assert judge_web.resolve_match_status(current_trial, [], {"match_started": True}, {}) == "running"


@pytest.mark.test_id("JW-08")
@pytest.mark.priority("P1")
@pytest.mark.requirement("未完成 runtime stage 时比赛状态为 running，且可识别 stage incomplete")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="has_incomplete_runtime_stage/resolve_match_status")
def test_resolve_match_status_running_when_runtime_stage_incomplete_but_match_started() -> None:
    runtime_stage_status = {
        "group_status_list": [
            {
                "stage_status_list": [
                    {
                        "collector_prepared": True,
                        "online_stage_released": False,
                        "online_trial_count": 3,
                        "completed_trial_count": 0,
                    }
                ]
            }
        ]
    }
    assert judge_web.has_incomplete_runtime_stage(runtime_stage_status) is True
    assert judge_web.resolve_match_status(None, [], {"match_started": True}, runtime_stage_status) == "running"


@pytest.mark.test_id("JW-09")
@pytest.mark.priority("P0")
@pytest.mark.requirement("开始比赛 readiness 需至少一支已连接队伍")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="build_start_match_readiness")
def test_build_start_match_readiness_requires_connected_team() -> None:
    readiness = judge_web.build_start_match_readiness(
        team_registry=[{"team_id": "team_0"}],
        team_status_list=[{"team_id": "team_0", "connection_status": "disconnected"}],
        runtime_stage_status={},
    )
    assert readiness["ready"] is False
    assert readiness["reason_list"]


@pytest.mark.test_id("JW-10")
@pytest.mark.priority("P0")
@pytest.mark.requirement("开始比赛 readiness 在队伍已连接且无阻塞条件时为 true")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="build_start_match_readiness")
def test_build_start_match_readiness_success() -> None:
    readiness = judge_web.build_start_match_readiness(
        team_registry=[{"team_id": "team_0"}],
        team_status_list=[{"team_id": "team_0", "connection_status": "connected"}],
        runtime_stage_status={"group_status_list": []},
    )
    assert readiness["ready"] is True


@pytest.mark.test_id("JW-11")
@pytest.mark.priority("P1")
@pytest.mark.requirement("sanitize_json_compatible 将 NaN/inf 转为 None")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="sanitize_json_compatible")
def test_sanitize_json_compatible_handles_non_finite_values() -> None:
    payload = {"a": float("inf"), "b": float("nan"), "c": [1.0, float("-inf")]}
    assert judge_web.sanitize_json_compatible(payload) == {"a": None, "b": None, "c": [1.0, None]}


@pytest.mark.test_id("JW-12")
@pytest.mark.priority("P1")
@pytest.mark.requirement("load_applied_recovery_status 从 launcher manifest 提取 applied recovery")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="load_applied_recovery_status")
def test_load_applied_recovery_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launcher_manifest_path = tmp_path / "launcher_manifest.json"
    launcher_manifest_path.write_text(
        json.dumps(
            {
                "applied_recovery": {
                    "recovery_mode": "restart_from_stage",
                    "stage": {
                        "subject_id": "S1",
                        "exp_name": "vme",
                        "exp_task": "left_vs_rest",
                        "session_id": "session2",
                    },
                    "requested_at": 1.0,
                    "applied_at": 2.0,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(judge_web, "LAUNCHER_MANIFEST_PATH", launcher_manifest_path)
    payload = judge_web.load_applied_recovery_status()
    assert payload["recovery_mode"] == "restart_from_stage"
    assert payload["stage"] == {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "left_vs_rest",
        "session_id": "session2",
    }


@pytest.mark.test_id("JW-13")
@pytest.mark.priority("P1")
@pytest.mark.requirement("write_recovery_request 写入 recovery_request.json")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="write_recovery_request")
def test_write_recovery_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(judge_web, "CONTROL_ROOT", tmp_path / "control")
    payload = judge_web.write_recovery_request(
        "restart_from_stage",
        {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session2",
        },
    )
    file_path = tmp_path / "control" / judge_web.RECOVERY_REQUEST_FILE_NAME
    written_payload = json.loads(file_path.read_text(encoding="utf-8"))
    assert payload["payload"]["recovery_mode"] == "restart_from_stage"
    assert written_payload["payload"]["stage"]["subject_id"] == "S1"
    assert written_payload["payload"]["stage"]["session_id"] == "session2"


@pytest.mark.test_id("JW-14")
@pytest.mark.priority("P1")
@pytest.mark.requirement("resolve_recommended_recovery_stage 优先当前 trial，其次 checkpoint")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="resolve_recommended_recovery_stage")
def test_resolve_recommended_recovery_stage_prefers_current_trial() -> None:
    checkpoint_list = [
        {
            "subject_id": "S2",
            "exp_name": "vmi",
            "exp_task": "right_vs_rest",
            "session_id": "session1",
        }
    ]
    current_trial = {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "left_vs_rest",
        "session_id": "session2",
    }
    assert judge_web.resolve_recommended_recovery_stage(checkpoint_list, current_trial) == {
        "subject_id": "S1",
        "exp_name": "vme",
        "exp_task": "left_vs_rest",
        "session_id": "session2",
    }


@pytest.mark.test_id("JW-15")
@pytest.mark.priority("P0")
@pytest.mark.requirement("JudgeWeb checkpoint 列表必须分别暴露同任务的不同 session")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="load_recovery_checkpoint_list")
def test_load_recovery_checkpoint_list_keeps_sessions_distinct(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        judge_web,
        "load_recovery_stage_catalog",
        lambda project_root: [
            {
                "checkpoint_id": "S1|vme|left_vs_rest|session1",
                "subject_id": "S1",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": "session1",
                "block_id": 1,
            },
            {
                "checkpoint_id": "S1|vme|left_vs_rest|session2",
                "subject_id": "S1",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": "session2",
                "block_id": 3,
            },
        ],
    )
    monkeypatch.setattr(judge_web, "load_team_registry", lambda: [{"team_id": "team_1"}])
    monkeypatch.setattr(judge_web, "collect_team_id_list", lambda registry: ["team_1"])
    monkeypatch.setattr(
        judge_web,
        "load_team_trial_record_rows",
        lambda db_path, team_id: [
            {
                "subject_id": "S1",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": "session2",
                "updated_at": 20,
            }
        ],
    )

    checkpoint_list = judge_web.load_recovery_checkpoint_list()

    assert [row["checkpoint_id"] for row in checkpoint_list] == [
        "S1|vme|left_vs_rest|session1",
        "S1|vme|left_vs_rest|session2",
    ]
    assert [row["session_id"] for row in checkpoint_list] == ["session1", "session2"]
    assert [row["status"] for row in checkpoint_list] == ["configured", "observed"]


@pytest.mark.test_id("JW-16")
@pytest.mark.priority("P0")
@pytest.mark.requirement("WebSocket 单分区读取失败时保留最后有效值且不影响其他分区")
@pytest.mark.tested(file="app/JudgeWeb/JudgeWeb/main.py", function="collect_live_snapshot_sections")
def test_collect_live_snapshot_sections_isolates_section_failure() -> None:
    async def load_current():
        return {"trial_id": "trial-2"}

    async def load_scoreboard():
        raise RuntimeError("temporary scoreboard read failure")

    last_payload = {"scoreboard": {"scoreboard": [{"team_id": "team-1", "score": 10}]}}
    failure_count: dict[str, int] = {}
    logger = getattr(judge_web, "LOGGER")
    original_level = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        live_payload = asyncio.run(
            judge_web.collect_live_snapshot_sections(
                last_payload,
                failure_count,
                (
                    ("current", load_current),
                    ("scoreboard", load_scoreboard),
                ),
            )
        )
    finally:
        logger.setLevel(original_level)

    assert live_payload["current"] == {"trial_id": "trial-2"}
    assert live_payload["scoreboard"] == {
        "scoreboard": [{"team_id": "team-1", "score": 10}]
    }
    assert last_payload["current"] == {"trial_id": "trial-2"}
    assert failure_count == {"current": 0, "scoreboard": 1}
