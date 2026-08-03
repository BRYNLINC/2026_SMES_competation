from __future__ import annotations

import json

import pytest

try:
    import asyncio
    from app.ProcessHub.RuntimeStageCoordinator.application.ApplicationImplement import ApplicationImplement
except Exception as exc:  # pragma: no cover - environment-dependent import gate
    asyncio = None
    ApplicationImplement = None
    _RUNTIME_STAGE_IMPORT_ERROR = exc
else:
    _RUNTIME_STAGE_IMPORT_ERROR = None


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("runtime_stage")]


if ApplicationImplement is None or asyncio is None:
    pytestmark.append(
        pytest.mark.skip(
            reason=f"RuntimeStageCoordinator import unavailable in current environment: {_RUNTIME_STAGE_IMPORT_ERROR!r}"
        )
    )


def _make_payload(
    event_type: str,
    *,
    group_id: str = "group_1",
    team_id: str | None = None,
    stage_context: dict | None = None,
    trial_context: dict | None = None,
    **extra,
) -> bytes:
    payload = {
        "event_type": event_type,
        "group_id": group_id,
        "stage_context": stage_context
        or {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session1",
        },
    }
    if team_id is not None:
        payload["team_id"] = team_id
    if trial_context is not None:
        payload["trial_context"] = trial_context
    payload.update(extra)
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _configure_runtime_stage_coordinator(application: ApplicationImplement) -> None:
    setattr(application, "_ApplicationImplement__team_id_list_by_group", {"group_1": ["team_0", "team_1"]})
    setattr(application, "_ApplicationImplement__release_policy", "AUTO_RELEASE_WHEN_ALL_TEAMS_READY")
    setattr(application, "_ApplicationImplement__trial_release_interval_seconds", 0.0)
    setattr(application, "_ApplicationImplement__calibration_ready_timeout_seconds", 0.01)
    setattr(application, "_ApplicationImplement__trial_release_delivery_watchdog_timeout_seconds", 0.01)
    setattr(application, "_ApplicationImplement__trial_release_delivery_watchdog_max_resend_count", 1)
    setattr(application, "_ApplicationImplement__runtime_stage_control_topic", "runtime.control")
    setattr(application, "_ApplicationImplement__runtime_stage_event_topic", "runtime.event")
    setattr(application, "_ApplicationImplement__coordinator_started_wallclock", 1.0)


@pytest.mark.test_id("RSC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("未 start-match 时不得放行 online")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="_receive_runtime_stage_event")
def test_no_release_before_start_match() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(_make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=3))
        await application._receive_runtime_stage_event(_make_payload("team_calibration_ready", team_id="team_0"))
        await application._receive_runtime_stage_event(_make_payload("team_calibration_ready", team_id="team_1"))

        assert sent_payload_list == []
        assert application._ApplicationImplement__match_started is False  # type: ignore[attr-defined]

    asyncio.run(_run())


@pytest.mark.test_id("RSC-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("比赛开始后未全员 ready 时不得放行")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="_receive_runtime_stage_event")
def test_no_release_when_not_all_teams_ready() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(_make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=3))
        await application._receive_runtime_stage_event(_make_payload("team_calibration_ready", team_id="team_0"))

        assert sent_payload_list == []

    asyncio.run(_run())


@pytest.mark.test_id("RSC-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("比赛开始且全员 ready 后应只发送一次 allow_online_stage 和一次首 trial release")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="_receive_runtime_stage_event")
def test_release_once_when_all_teams_ready() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(_make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=3))
        await application._receive_runtime_stage_event(_make_payload("team_calibration_ready", team_id="team_0"))
        await application._receive_runtime_stage_event(_make_payload("team_calibration_ready", team_id="team_1"))

        assert [payload["control_type"] for payload in sent_payload_list] == ["allow_online_stage", "release_trial"]
        assert sent_payload_list[0]["stage_context"]["subject_id"] == "S1"
        assert sent_payload_list[1]["trial_id"] == 1

    asyncio.run(_run())


@pytest.mark.test_id("RSC-03F")
@pytest.mark.priority("P0")
@pytest.mark.requirement("校准掉线队伍进入 forfeited 终态后，不得阻塞其他 ready 队伍进入 online")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="_receive_runtime_stage_event")
def test_calibration_forfeit_is_terminal_and_releases_other_ready_teams() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(
            _make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=3)
        )
        await application._receive_runtime_stage_event(
            _make_payload("team_calibration_ready", team_id="team_0", calibration_ready=True)
        )
        forfeited_payload = _make_payload(
            "team_calibration_forfeited",
            team_id="team_1",
            disconnect_reason="report_stream_closed",
            algorithm_address="10.0.0.2:9981",
            forfeited_at=123.0,
        )
        await application._receive_runtime_stage_event(forfeited_payload)
        await application._receive_runtime_stage_event(forfeited_payload)

        assert [payload["control_type"] for payload in sent_payload_list] == [
            "allow_online_stage",
            "release_trial",
        ]
        snapshot = application._ApplicationImplement__build_runtime_stage_status_snapshot()  # type: ignore[attr-defined]
        stage_status = snapshot["group_status_list"][0]["stage_status_list"][0]
        assert stage_status["ready_team_id_list"] == ["team_0"]
        assert stage_status["forfeited_team_id_list"] == ["team_1"]
        assert stage_status["pending_ready_team_id_list"] == []
        assert stage_status["calibration_forfeit_detail_by_team"]["team_1"]["disconnect_reason"] == (
            "report_stream_closed"
        )

    asyncio.run(_run())


@pytest.mark.test_id("RSC-03FT")
@pytest.mark.priority("P0")
@pytest.mark.requirement("校准未就绪队伍不得继续阻塞当前 stage 的 online trial 终态屏障")
@pytest.mark.tested(
    file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py",
    function="__load_trial_barrier_team_id_list/__try_auto_release_next_trial",
)
def test_calibration_forfeit_is_excluded_from_online_trial_barrier() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(
            _make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=3)
        )
        await application._receive_runtime_stage_event(
            _make_payload("team_calibration_ready", team_id="team_0")
        )
        await application._receive_runtime_stage_event(
            _make_payload("team_calibration_forfeited", team_id="team_1")
        )
        await application._receive_runtime_stage_event(
            _make_payload(
                "team_trial_terminal",
                team_id="team_0",
                collector_component_id="collector_1",
                terminal_type="result",
                trial_context={"trial_id": 1},
            )
        )

        assert [payload["control_type"] for payload in sent_payload_list] == [
            "allow_online_stage",
            "release_trial",
            "release_trial",
        ]
        assert sent_payload_list[-1]["trial_id"] == 2
        snapshot = application._ApplicationImplement__build_runtime_stage_status_snapshot()  # type: ignore[attr-defined]
        stage_status = snapshot["group_status_list"][0]["stage_status_list"][0]
        assert stage_status["trial_barrier_team_id_list"] == ["team_0"]

    asyncio.run(_run())


@pytest.mark.test_id("RSC-03FA")
@pytest.mark.priority("P0")
@pytest.mark.requirement("若当前 stage 全队校准未就绪，Collector 每次确认 trial 已发送后仍应推进并完成 stage")
@pytest.mark.tested(
    file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py",
    function="_receive_runtime_stage_event/__try_auto_release_next_trial",
)
def test_all_calibration_forfeited_stage_still_advances_on_trial_sent() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(
            _make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=2)
        )
        await application._receive_runtime_stage_event(
            _make_payload("team_calibration_forfeited", team_id="team_0")
        )
        await application._receive_runtime_stage_event(
            _make_payload("team_calibration_forfeited", team_id="team_1")
        )
        await application._receive_runtime_stage_event(
            _make_payload(
                "team_trial_sent",
                collector_component_id="collector_1",
                trial_context={"trial_id": 1},
                trial_sent_wallclock=10.0,
            )
        )
        await application._receive_runtime_stage_event(
            _make_payload(
                "team_trial_sent",
                collector_component_id="collector_1",
                trial_context={"trial_id": 2},
                trial_sent_wallclock=11.0,
            )
        )

        assert [payload["control_type"] for payload in sent_payload_list] == [
            "allow_online_stage",
            "release_trial",
            "release_trial",
            "complete_online_stage",
        ]

    asyncio.run(_run())


@pytest.mark.test_id("RSC-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("重复 prepared/ready 不得重复发送 online 放行和首 trial 放行")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="_receive_runtime_stage_event")
def test_duplicate_events_do_not_duplicate_release() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        prepared_payload = _make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=3)
        ready_team_0 = _make_payload("team_calibration_ready", team_id="team_0")
        ready_team_1 = _make_payload("team_calibration_ready", team_id="team_1")

        await application._receive_runtime_stage_event(prepared_payload)
        await application._receive_runtime_stage_event(prepared_payload)
        await application._receive_runtime_stage_event(ready_team_0)
        await application._receive_runtime_stage_event(ready_team_0)
        await application._receive_runtime_stage_event(ready_team_1)
        await application._receive_runtime_stage_event(ready_team_1)

        assert [payload["control_type"] for payload in sent_payload_list] == ["allow_online_stage", "release_trial"]

    asyncio.run(_run())


@pytest.mark.test_id("RSC-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("ready 先到 prepared 后到时应正常发送 online 放行和首 trial 放行")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="_receive_runtime_stage_event")
def test_ready_before_prepared_still_releases() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(_make_payload("team_calibration_ready", team_id="team_0"))
        await application._receive_runtime_stage_event(_make_payload("team_calibration_ready", team_id="team_1"))
        await application._receive_runtime_stage_event(_make_payload("collector_stage_prepared", collector_component_id="collector_1", online_trial_count=3))

        assert [payload["control_type"] for payload in sent_payload_list] == ["allow_online_stage", "release_trial"]

    asyncio.run(_run())


@pytest.mark.test_id("RSC-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("校准超时后必须写错误状态并禁止缺失队伍的阶段进入 online")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="__wait_and_fail_calibration_after_timeout")
def test_calibration_timeout_writes_error_and_does_not_release_online() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        stage_context = {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session1",
        }
        stage_key = application._ApplicationImplement__build_stage_key("group_1", stage_context)  # type: ignore[attr-defined]
        application._ApplicationImplement__collector_prepared_stage_key_set.add(stage_key)  # type: ignore[attr-defined]
        application._ApplicationImplement__collector_stage_release_payload_by_stage_key[stage_key] = {  # type: ignore[attr-defined]
            "release_type": "online_stage_first_trial",
            "group_id": "group_1",
            "collector_component_id": "collector_1",
            "stage_context": stage_context,
            "next_trial_id": 1,
        }
        application._ApplicationImplement__team_ready_by_stage_key[stage_key] = {"team_0"}  # type: ignore[attr-defined]
        sent_payload_list: list[dict] = []
        written_state_list: list[tuple[str, dict, str]] = []

        async def fake_release(stage_key: str, payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__release_or_queue_pending = fake_release  # type: ignore[attr-defined]
        application._ApplicationImplement__safe_write_json_file = (  # type: ignore[attr-defined]
            lambda *, state_key, payload, log_name: written_state_list.append(
                (state_key, dict(payload), log_name)
            )
        )
        await application._ApplicationImplement__wait_and_fail_calibration_after_timeout(stage_key, "group_1")  # type: ignore[attr-defined]

        assert sent_payload_list == []
        assert len(written_state_list) == 1
        assert written_state_list[0][1]["status"] == "error"
        assert written_state_list[0][1]["error_type"] == "CalibrationReadyTimeoutError"
        assert written_state_list[0][1]["missing_ready_team_id_list"] == ["team_1"]

    asyncio.run(_run())


@pytest.mark.test_id("RSC-08")
@pytest.mark.priority("P0")
@pytest.mark.requirement("当前 trial 全队 terminal 后放行下一 trial")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="__try_auto_release_next_trial")
def test_all_terminal_releases_next_trial() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        stage_context = {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session1",
        }
        stage_key = application._ApplicationImplement__build_stage_key("group_1", stage_context)  # type: ignore[attr-defined]
        application._ApplicationImplement__released_trial_id_by_stage_key[stage_key] = 1  # type: ignore[attr-defined]
        application._ApplicationImplement__online_trial_count_by_stage_key[stage_key] = 3  # type: ignore[attr-defined]
        application._ApplicationImplement__team_trial_terminal_by_stage_trial_key[(stage_key, 1)] = {"team_0", "team_1"}  # type: ignore[attr-defined]
        sent_payload_list: list[dict] = []

        async def fake_release(stage_key: str, payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__release_or_queue_pending = fake_release  # type: ignore[attr-defined]
        await application._ApplicationImplement__try_auto_release_next_trial(  # type: ignore[attr-defined]
            stage_key,
            "group_1",
            {"collector_component_id": "collector_1", "stage_context": stage_context},
            1,
        )
        assert len(sent_payload_list) == 1
        assert sent_payload_list[0]["release_type"] == "trial"
        assert sent_payload_list[0]["current_trial_id"] == 1
        assert sent_payload_list[0]["next_trial_id"] == 2

    asyncio.run(_run())


@pytest.mark.test_id("RSC-09")
@pytest.mark.priority("P0")
@pytest.mark.requirement("最后一个 online trial 全队 terminal 后应发送 stage completed 控制，防止 Collector 提前切到下一 stage")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="__try_auto_release_next_trial")
def test_last_online_trial_marks_stage_completed() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        setattr(application, "_ApplicationImplement__match_started", True)
        stage_context = {
            "subject_id": "S1",
            "exp_name": "vme",
            "exp_task": "left_vs_rest",
            "session_id": "session1",
        }
        stage_key = application._ApplicationImplement__build_stage_key("group_1", stage_context)  # type: ignore[attr-defined]
        application._ApplicationImplement__released_trial_id_by_stage_key[stage_key] = 3  # type: ignore[attr-defined]
        application._ApplicationImplement__online_trial_count_by_stage_key[stage_key] = 3  # type: ignore[attr-defined]
        application._ApplicationImplement__team_trial_terminal_by_stage_trial_key[(stage_key, 3)] = {"team_0", "team_1"}  # type: ignore[attr-defined]
        sent_payload_list: list[dict] = []

        async def fake_release(stage_key: str, payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__release_or_queue_pending = fake_release  # type: ignore[attr-defined]
        await application._ApplicationImplement__try_auto_release_next_trial(  # type: ignore[attr-defined]
            stage_key,
            "group_1",
            {"collector_component_id": "collector_1", "stage_context": stage_context},
            3,
        )

        assert len(sent_payload_list) == 1
        assert sent_payload_list[0]["release_type"] == "online_stage_completed"
        assert sent_payload_list[0]["final_trial_id"] == 3

    asyncio.run(_run())


@pytest.mark.test_id("RSC-14")
@pytest.mark.priority("P0")
@pytest.mark.requirement("全部队伍完赛后应标记 match_finished")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="__try_mark_match_finished")
def test_team_run_finalized_marks_match_finished() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        written_state_list: list[dict] = []

        def fake_write() -> None:
            written_state_list.append({"written": True})

        application._ApplicationImplement__write_match_control_status = fake_write  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: json.loads(data.decode("utf-8"))  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(_make_payload("team_run_finalized", team_id="team_0", terminal_run_status="finished"))
        assert application._ApplicationImplement__match_finished is False  # type: ignore[attr-defined]
        await application._receive_runtime_stage_event(_make_payload("team_run_finalized", team_id="team_1", terminal_run_status="finished"))
        assert application._ApplicationImplement__match_finished is True  # type: ignore[attr-defined]
        assert written_state_list

    asyncio.run(_run())


@pytest.mark.test_id("RSC-15")
@pytest.mark.priority("P0")
@pytest.mark.requirement("非法 JSON、缺 group_id、未知 event_type 不得导致崩溃或发 control")
@pytest.mark.tested(file="app/ProcessHub/RuntimeStageCoordinator/application/ApplicationImplement.py", function="_receive_runtime_stage_event")
def test_invalid_event_payload_does_not_crash_or_send_control() -> None:
    async def _run() -> None:
        application = ApplicationImplement()
        _configure_runtime_stage_coordinator(application)
        sent_payload_list: list[dict] = []

        async def fake_send(payload: dict) -> None:
            sent_payload_list.append(dict(payload))

        application._ApplicationImplement__send_runtime_stage_control = fake_send  # type: ignore[attr-defined]
        application._ApplicationImplement__parse_data_message_payload = lambda data: None  # type: ignore[attr-defined]

        await application._receive_runtime_stage_event(b"not-json")
        application._ApplicationImplement__parse_data_message_payload = lambda data: {"event_type": "unknown", "group_id": "group_1", "stage_context": {"subject_id": "S1", "exp_name": "vme", "exp_task": "left_vs_rest", "session_id": "session1"}}  # type: ignore[attr-defined]
        await application._receive_runtime_stage_event(b"{}")
        application._ApplicationImplement__parse_data_message_payload = lambda data: {"event_type": "collector_stage_prepared"}  # type: ignore[attr-defined]
        await application._receive_runtime_stage_event(b"{}")

        assert sent_payload_list == []

    asyncio.run(_run())
