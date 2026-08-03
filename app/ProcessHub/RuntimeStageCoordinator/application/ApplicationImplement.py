import asyncio
import json
import logging
import logging.config
import os
import sys
import time
import uuid
from pathlib import Path

import yaml

from ApplicationFramework.api.interface.ComponentFrameworkOperatorInterface import ReceiveMessageOperatorInterface
from ApplicationFramework.api.model.ComponentModel import ComponentModel
from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from ApplicationFramework.application.interface.ApplicationInterface import ApplicationInterface
from Common.converter.CommonMessageConverter import CommonMessageConverter
from Common.model.CommonMessageModel import DataMessageModel, DataPackageModel
from Common.protobuf.CommonMessage_pb2 import DataMessage as DataMessage_pb2

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from tools.runtime_state_sqlite import (  # noqa: E402
    STATE_KEY_CURRENT_TRIAL,
    STATE_KEY_MATCH_CONTROL_STATUS,
    STATE_KEY_RUNTIME_STAGE_STATUS,
    resolve_runtime_state_db_path,
    write_json_state,
)


def ensure_logging_targets(logging_config: dict | None, base_dir: Path) -> None:
    if not isinstance(logging_config, dict):
        return
    for handler_config in (logging_config.get('handlers') or {}).values():
        if not isinstance(handler_config, dict):
            continue
        filename = handler_config.get('filename')
        if not filename:
            continue
        log_file_path = base_dir / str(filename)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        log_file_path.touch(exist_ok=True)


class ApplicationImplement(ApplicationInterface):
    # 统一的 runtime coordination message_key 定义。
    # 这些 key 会在 component_info 指向的 topic 上绑定，便于后续 UI / 外部控制复用。
    __RUNTIME_STAGE_EVENT_MESSAGE_KEY = 'runtime_stage_event'
    __RUNTIME_STAGE_CONTROL_MESSAGE_KEY = 'runtime_stage_control'
    __RUNTIME_STAGE_STATUS_MESSAGE_KEY = 'runtime_stage_status'
    __RUNTIME_STAGE_UI_CONTROL_MESSAGE_KEY = 'runtime_stage_ui_control'
    # runtime_stage_status 仅用于后续 UI / 外部监控。
    # 当前决赛主链的自动放行完全不依赖它，因此默认关闭，避免额外走一条非关键消息发送链路。
    __ENABLE_RUNTIME_STAGE_STATUS_DEFAULT = False
    __DEFAULT_CALIBRATION_READY_TIMEOUT_SECONDS = 300.0
    __DEFAULT_TRIAL_TERMINAL_WATCHDOG_BASE_TIMEOUT_SECONDS = 1.0
    __DEFAULT_TRIAL_TERMINAL_WATCHDOG_GRACE_SECONDS = 0.6
    __DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_TIMEOUT_SECONDS = 0.6
    __DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_MAX_RESEND_COUNT = 2
    # 当前默认策略：
    # 同一 group 内，只有当 Collector 已完成 stage 准备，且所有赛队都上报 calibration_ready 后，
    # 才自动释放 online 和第一个 trial。
    __AUTO_RELEASE_WHEN_ALL_TEAMS_READY = 'AUTO_RELEASE_WHEN_ALL_TEAMS_READY'

    def __init__(self):
        super().__init__()
        self.__component_model: ComponentModel = None
        self.__config_dict: dict = {}
        self.__finish_event = asyncio.Event()
        self.__logger = logging.getLogger("processHubLogger")
        self.__runtime_stage_event_topic: str = None
        self.__runtime_stage_control_topic: str = None
        self.__runtime_stage_status_topic: str = None
        self.__runtime_stage_ui_control_topic: str = None
        self.__enable_runtime_stage_status: bool = self.__ENABLE_RUNTIME_STAGE_STATUS_DEFAULT
        self.__release_policy: str = self.__AUTO_RELEASE_WHEN_ALL_TEAMS_READY
        self.__trial_release_interval_seconds: float = 1.3
        self.__calibration_ready_timeout_seconds: float = self.__DEFAULT_CALIBRATION_READY_TIMEOUT_SECONDS
        self.__trial_terminal_watchdog_base_timeout_seconds: float = (
            self.__DEFAULT_TRIAL_TERMINAL_WATCHDOG_BASE_TIMEOUT_SECONDS
        )
        self.__trial_terminal_watchdog_grace_seconds: float = (
            self.__DEFAULT_TRIAL_TERMINAL_WATCHDOG_GRACE_SECONDS
        )
        self.__trial_terminal_watchdog_base_timeout_seconds_by_task_id: dict[str, float] = {}
        self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_name: dict[str, float] = {}
        self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_task: dict[str, float] = {}
        self.__trial_release_delivery_watchdog_timeout_seconds: float = (
            self.__DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_TIMEOUT_SECONDS
        )
        self.__trial_release_delivery_watchdog_max_resend_count: int = (
            self.__DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_MAX_RESEND_COUNT
        )
        self.__trial_release_monotonic_by_stage_trial_key: dict[tuple[str, int], float] = {}
        self.__trial_sent_wallclock_by_stage_trial_key: dict[tuple[str, int], float] = {}
        self.__next_release_target_wallclock_by_stage_trial_key: dict[tuple[str, int], float] = {}
        self.__delayed_release_task_by_stage_trial_key: dict[tuple[str, int], asyncio.Task] = {}
        self.__trial_release_delivery_watchdog_task_by_stage_trial_key: dict[tuple[str, int], asyncio.Task] = {}
        self.__trial_release_payload_by_stage_trial_key: dict[tuple[str, int], dict] = {}
        self.__trial_release_lock_by_stage_key: dict[str, asyncio.Lock] = {}
        self.__trial_terminal_watchdog_task_by_stage_trial_key: dict[tuple[str, int], asyncio.Task] = {}
        self.__trial_terminal_watchdog_deadline_wallclock_by_stage_trial_key: dict[tuple[str, int], float] = {}
        self.__trial_terminal_watchdog_base_timeout_seconds_by_stage_trial_key: dict[tuple[str, int], float] = {}
        self.__forced_terminal_team_id_set_by_stage_trial_key: dict[tuple[str, int], set[str]] = {}
        self.__calibration_timeout_task_by_stage_key: dict[str, asyncio.Task] = {}
        self.__control_poll_task: asyncio.Task | None = None
        self.__coordinator_started_wallclock: float = time.time()
        self.__match_started: bool = False
        self.__match_started_wallclock: float | None = None
        self.__last_seen_start_request_at: float | None = None
        self.__pause_requested: bool = False
        self.__paused: bool = False
        self.__paused_wallclock: float | None = None
        self.__resumed_wallclock: float | None = None
        self.__last_seen_pause_request_at: float | None = None
        self.__last_seen_resume_request_at: float | None = None
        self.__match_finished: bool = False
        self.__match_finished_wallclock: float | None = None
        self.__finished_team_id_set: set[str] = set()
        # team_id_list_by_group 定义“某个 group 理论上应当等待哪些队伍”。
        # 协调器不自行发现队伍，而是以配置生成器给出的名单为准。
        self.__team_id_list_by_group: dict[str, list[str]] = {}
        # collector_prepared_stage_key_set:
        #   哪些 stage 已经完成 calibration 数据发送，正在等待进入 online。
        self.__collector_prepared_stage_key_set: set[str] = set()
        # team_ready_by_stage_key:
        #   当前 stage 下，哪些队伍已经完成 calibrate()。
        self.__team_ready_by_stage_key: dict[str, set[str]] = {}
        # calibration_forfeited_team_by_stage_key:
        #   当前 stage 下，算法连接在校准期间中断、已放弃本 stage 的队伍。
        #   该状态只用于裁判端解除 group barrier，不会伪造 calibration_ready 给选手算法。
        self.__calibration_forfeited_team_by_stage_key: dict[str, set[str]] = {}
        self.__calibration_forfeit_detail_by_stage_key: dict[str, dict[str, dict]] = {}
        # online_stage_released_stage_key_set:
        #   避免同一个 stage 被重复下发 allow_online_stage。
        self.__online_stage_released_stage_key_set: set[str] = set()
        # online_stage_completed_stage_key_set:
        #   当前 stage 的最后一个 online trial 已经被同组所有队伍处理完成，
        #   Collector 可以安全切到下一个 stage 的 calibration。
        self.__online_stage_completed_stage_key_set: set[str] = set()
        # released_trial_id_by_stage_key:
        #   记录当前 stage 已经放行到哪个 trial，防止重复 release。
        self.__released_trial_id_by_stage_key: dict[str, int] = {}
        # team_trial_terminal_by_stage_trial_key:
        #   统计某个 stage 的某个 trial 已经有多少队伍进入终态(result/timeout)。
        self.__team_trial_terminal_by_stage_trial_key: dict[tuple[str, int], set[str]] = {}
        # online_trial_count_by_stage_key:
        #   Collector 在 stage_prepared 时显式上报的该阶段 online trial 总数。
        self.__online_trial_count_by_stage_key: dict[str, int] = {}
        # 当前版本只维护内存中的状态快照，供日志和后续 UI 接口复用。
        # 不再把 runtime_stage_status 主动发布到消息总线，避免非关键状态通道影响主同步链路。
        self.__latest_runtime_stage_status_payload: dict = {}
        self.__pending_release_payload_by_stage_key: dict[str, dict] = {}
        self.__collector_stage_release_payload_by_stage_key: dict[str, dict] = {}

    async def initial(self) -> None:
        current_file_path = os.path.abspath(__file__)
        log_config_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(current_file_path))),
            'ProcessHub',
            'config',
            'LoggingConfig.yml',
        )
        with open(log_config_file_path, 'r', encoding='utf-8') as logging_file:
            logging_config = yaml.safe_load(logging_file)
        ensure_logging_targets(
            logging_config,
            base_dir=Path(os.path.dirname(os.path.dirname(os.path.dirname(current_file_path)))) / 'ProcessHub',
        )
        logging.config.dictConfig(logging_config)

        application_config_path = os.path.join(os.path.dirname(current_file_path), 'ApplicationImplement.yml')
        with open(application_config_path, 'r', encoding='utf-8') as f:
            self.__config_dict = yaml.safe_load(f)

        component_dict = self.__config_dict.get("component", {})
        component_type = component_dict.get('component_type', "")
        component_id = component_dict.get('component_id') or os.environ.get('COMPONENT_ID') or (
            component_type + '_' + str(uuid.uuid4())
        )
        self.__component_model = ComponentModel(
            component_id=component_id,
            component_type=component_type,
            component_info=component_dict.get('component_info', {}),
        )

    async def run(self) -> None:
        self.__coordinator_started_wallclock = time.time()
        await self.__load_runtime_config()
        await self.__bind_runtime_topics()
        self.__logger.info(
            "RuntimeStageCoordinator 启动完成: release_policy=%s trial_release_interval_seconds=%s "
            "trial_terminal_watchdog_base_timeout_seconds=%s trial_terminal_watchdog_grace_seconds=%s "
            "trial_release_delivery_watchdog_timeout_seconds=%s trial_release_delivery_watchdog_max_resend_count=%s "
            "event_topic=%s control_topic=%s status_topic=%s status_enabled=%s groups=%s",
            self.__release_policy,
            self.__trial_release_interval_seconds,
            self.__trial_terminal_watchdog_base_timeout_seconds,
            self.__trial_terminal_watchdog_grace_seconds,
            self.__trial_release_delivery_watchdog_timeout_seconds,
            self.__trial_release_delivery_watchdog_max_resend_count,
            self.__runtime_stage_event_topic,
            self.__runtime_stage_control_topic,
            self.__runtime_stage_status_topic,
            self.__enable_runtime_stage_status,
            self.__team_id_list_by_group,
        )
        self.__write_match_control_status()
        self.__refresh_runtime_stage_status_snapshot()
        self.__control_poll_task = asyncio.create_task(self.__poll_control_requests())

        class ReceiveRuntimeStageEventOperator(ReceiveMessageOperatorInterface):
            def __init__(self, application: ApplicationImplement):
                self.__application = application

            async def receive_message(self, data: bytes) -> None:
                await self.__application._receive_runtime_stage_event(data)

        await self._component_framework.subscribe_message(
            self.__RUNTIME_STAGE_EVENT_MESSAGE_KEY,
            ReceiveRuntimeStageEventOperator(self),
        )

        # MANUAL_RELEASE_FROM_UI 保留接口，但当前默认不启用。
        # 后续如接外部 UI，再显式启用。
        # class ReceiveRuntimeStageUiControlOperator(ReceiveMessageOperatorInterface):
        #     def __init__(self, application: ApplicationImplement):
        #         self.__application = application
        #
        #     async def receive_message(self, data: bytes) -> None:
        #         await self.__application._receive_runtime_stage_ui_control(data)
        #
        # if self.__runtime_stage_ui_control_topic is not None:
        #     await self._component_framework.subscribe_message(
        #         self.__RUNTIME_STAGE_UI_CONTROL_MESSAGE_KEY,
        #         ReceiveRuntimeStageUiControlOperator(self),
        #     )
        #
        # 后续若接外部 UI：
        # 1. query_status 类请求直接读取 __latest_runtime_stage_status_payload；
        # 2. manual_release_* 类请求在读取 snapshot 后，再显式触发 release control；
        # 3. 不再恢复“每个事件都自动推送 status 到消息总线”的模式。

        await self.__finish_event.wait()

    async def exit(self) -> None:
        if self.__control_poll_task is not None:
            self.__control_poll_task.cancel()
            try:
                await self.__control_poll_task
            except asyncio.CancelledError:
                pass
        for timeout_task in list(self.__calibration_timeout_task_by_stage_key.values()):
            timeout_task.cancel()
        for timeout_task in list(self.__calibration_timeout_task_by_stage_key.values()):
            try:
                await timeout_task
            except asyncio.CancelledError:
                pass
        self.__calibration_timeout_task_by_stage_key.clear()
        for delivery_watchdog_task in list(self.__trial_release_delivery_watchdog_task_by_stage_trial_key.values()):
            delivery_watchdog_task.cancel()
        for delivery_watchdog_task in list(self.__trial_release_delivery_watchdog_task_by_stage_trial_key.values()):
            try:
                await delivery_watchdog_task
            except asyncio.CancelledError:
                pass
        self.__trial_release_delivery_watchdog_task_by_stage_trial_key.clear()
        self.__trial_release_payload_by_stage_trial_key.clear()
        for watchdog_task in list(self.__trial_terminal_watchdog_task_by_stage_trial_key.values()):
            watchdog_task.cancel()
        for watchdog_task in list(self.__trial_terminal_watchdog_task_by_stage_trial_key.values()):
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
        self.__trial_terminal_watchdog_task_by_stage_trial_key.clear()
        self.__finish_event.set()

    def get_component_model(self) -> ComponentModel:
        return self.__component_model

    async def __load_runtime_config(self) -> None:
        # 组件本身不推导任何 group/team 信息，全部从 component_info 读取。
        component_model = await self._component_framework.get_component_model()
        component_info = component_model.component_info or {}
        self.__release_policy = component_info.get(
            'release_policy',
            self.__AUTO_RELEASE_WHEN_ALL_TEAMS_READY,
        )
        self.__team_id_list_by_group = {
            group_id: list(team_id_list or [])
            for group_id, team_id_list in (component_info.get('team_id_list_by_group') or {}).items()
        }
        self.__trial_release_interval_seconds = float(component_info.get('trial_release_interval_seconds', 1.3) or 1.3)
        self.__calibration_ready_timeout_seconds = float(
            component_info.get(
                'calibration_ready_timeout_seconds',
                self.__DEFAULT_CALIBRATION_READY_TIMEOUT_SECONDS,
            ) or self.__DEFAULT_CALIBRATION_READY_TIMEOUT_SECONDS
        )
        self.__trial_terminal_watchdog_base_timeout_seconds = float(
            component_info.get(
                'trial_terminal_watchdog_base_timeout_seconds',
                self.__DEFAULT_TRIAL_TERMINAL_WATCHDOG_BASE_TIMEOUT_SECONDS,
            ) or self.__DEFAULT_TRIAL_TERMINAL_WATCHDOG_BASE_TIMEOUT_SECONDS
        )
        self.__trial_terminal_watchdog_grace_seconds = float(
            component_info.get(
                'trial_terminal_watchdog_grace_seconds',
                self.__DEFAULT_TRIAL_TERMINAL_WATCHDOG_GRACE_SECONDS,
            ) or self.__DEFAULT_TRIAL_TERMINAL_WATCHDOG_GRACE_SECONDS
        )
        self.__trial_terminal_watchdog_base_timeout_seconds_by_task_id = self.__parse_timeout_mapping(
            component_info.get('trial_terminal_watchdog_base_timeout_seconds_by_task_id')
        )
        self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_name = self.__parse_timeout_mapping(
            component_info.get('trial_terminal_watchdog_base_timeout_seconds_by_exp_name')
        )
        self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_task = self.__parse_timeout_mapping(
            component_info.get('trial_terminal_watchdog_base_timeout_seconds_by_exp_task')
        )
        trial_release_delivery_watchdog_timeout_seconds = component_info.get(
            'trial_release_delivery_watchdog_timeout_seconds',
            self.__DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_TIMEOUT_SECONDS,
        )
        if trial_release_delivery_watchdog_timeout_seconds is None:
            trial_release_delivery_watchdog_timeout_seconds = (
                self.__DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_TIMEOUT_SECONDS
            )
        self.__trial_release_delivery_watchdog_timeout_seconds = float(
            trial_release_delivery_watchdog_timeout_seconds
        )
        trial_release_delivery_watchdog_max_resend_count = component_info.get(
            'trial_release_delivery_watchdog_max_resend_count',
            self.__DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_MAX_RESEND_COUNT,
        )
        if trial_release_delivery_watchdog_max_resend_count is None:
            trial_release_delivery_watchdog_max_resend_count = (
                self.__DEFAULT_TRIAL_RELEASE_DELIVERY_WATCHDOG_MAX_RESEND_COUNT
            )
        self.__trial_release_delivery_watchdog_max_resend_count = max(
            0,
            int(trial_release_delivery_watchdog_max_resend_count),
        )
        self.__runtime_stage_event_topic = component_info.get('runtime_stage_event_topic')
        self.__runtime_stage_control_topic = component_info.get('runtime_stage_control_topic')
        self.__runtime_stage_status_topic = component_info.get('runtime_stage_status_topic')
        self.__runtime_stage_ui_control_topic = component_info.get('runtime_stage_ui_control_topic')
        self.__enable_runtime_stage_status = bool(
            component_info.get(
                'enable_runtime_stage_status',
                self.__ENABLE_RUNTIME_STAGE_STATUS_DEFAULT,
            )
        )

    async def __bind_runtime_topics(self) -> None:
        if self.__runtime_stage_event_topic is not None:
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=self.__RUNTIME_STAGE_EVENT_MESSAGE_KEY,
                    topic=self.__runtime_stage_event_topic,
                )
            )
            self.__logger.info("已绑定 runtime stage event topic: %s", self.__runtime_stage_event_topic)
        if self.__runtime_stage_control_topic is not None:
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=self.__RUNTIME_STAGE_CONTROL_MESSAGE_KEY,
                    topic=self.__runtime_stage_control_topic,
                )
            )
            self.__logger.info("已绑定 runtime stage control topic: %s", self.__runtime_stage_control_topic)
        if self.__enable_runtime_stage_status and self.__runtime_stage_status_topic is not None:
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=self.__RUNTIME_STAGE_STATUS_MESSAGE_KEY,
                    topic=self.__runtime_stage_status_topic,
                )
            )
            self.__logger.info("已绑定 runtime stage status topic: %s", self.__runtime_stage_status_topic)
        if self.__runtime_stage_ui_control_topic is not None:
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=self.__RUNTIME_STAGE_UI_CONTROL_MESSAGE_KEY,
                    topic=self.__runtime_stage_ui_control_topic,
                )
            )
            self.__logger.info("已绑定 runtime stage ui control topic: %s", self.__runtime_stage_ui_control_topic)

    async def _receive_runtime_stage_event(self, data: bytes) -> None:
        # 所有 runtime 协调事件都在这里汇总。
        # 设计目标是把“Collector / ProcessHub 各自上报的局部状态”
        # 收敛为一套 group 级的 stage/trial barrier 判定。
        payload = None
        try:
            payload = self.__parse_data_message_payload(data)
            if not isinstance(payload, dict):
                self.__logger.warning("忽略无法解析的 runtime stage event: %s", data)
                return
            event_type = payload.get('event_type')
            stage_context = payload.get('stage_context') or {}
            group_id = payload.get('group_id')
            if not group_id or not stage_context:
                self.__logger.warning("忽略缺少 group_id 或 stage_context 的 runtime stage event: %s", payload)
                return
            self.__logger.info(
                "收到 runtime stage event: event_type=%s group_id=%s stage_context=%s team_id=%s trial_context=%s",
                event_type,
                group_id,
                stage_context,
                payload.get('team_id'),
                payload.get('trial_context'),
            )
            stage_key = self.__build_stage_key(group_id, stage_context)
            if event_type == 'collector_stage_prepared':
                self.__collector_prepared_stage_key_set.add(stage_key)
                self.__collector_stage_release_payload_by_stage_key[stage_key] = {
                    'release_type': 'online_stage_first_trial',
                    'group_id': group_id,
                    'collector_component_id': payload.get('collector_component_id'),
                    'stage_context': payload.get('stage_context'),
                    'next_trial_id': 1,
                }
                online_trial_count = payload.get('online_trial_count')
                if online_trial_count is not None:
                    self.__online_trial_count_by_stage_key[stage_key] = int(online_trial_count)
                self.__ensure_calibration_timeout_task(stage_key=stage_key, group_id=group_id)
                await self.__try_auto_release_online(stage_key, group_id, payload)
            elif event_type == 'team_calibration_ready':
                team_id = str(payload.get('team_id') or '').strip()
                if payload.get('calibration_ready') is False or team_id == '':
                    self.__logger.warning("忽略无效的 team_calibration_ready 事件: %s", payload)
                    return
                self.__team_ready_by_stage_key.setdefault(stage_key, set()).add(team_id)
                self.__calibration_forfeited_team_by_stage_key.setdefault(stage_key, set()).discard(team_id)
                self.__calibration_forfeit_detail_by_stage_key.setdefault(stage_key, {}).pop(team_id, None)
                await self.__try_auto_release_online(stage_key, group_id, payload)
            elif event_type == 'team_calibration_forfeited':
                team_id = str(payload.get('team_id') or '').strip()
                configured_team_id_set = set(self.__team_id_list_by_group.get(group_id, []))
                if team_id == '' or team_id not in configured_team_id_set:
                    self.__logger.warning("忽略未配置队伍的 team_calibration_forfeited 事件: %s", payload)
                    return
                self.__team_ready_by_stage_key.setdefault(stage_key, set()).discard(team_id)
                self.__calibration_forfeited_team_by_stage_key.setdefault(stage_key, set()).add(team_id)
                self.__calibration_forfeit_detail_by_stage_key.setdefault(stage_key, {})[team_id] = {
                    'disconnect_reason': payload.get('disconnect_reason'),
                    'algorithm_address': payload.get('algorithm_address'),
                    'forfeited_at': payload.get('forfeited_at'),
                    'event_id': payload.get('event_id'),
                }
                await self.__try_auto_release_online(stage_key, group_id, payload)
                released_trial_id = int(self.__released_trial_id_by_stage_key.get(stage_key, 0))
                if (
                    released_trial_id > 0
                    and (stage_key, released_trial_id) in self.__trial_sent_wallclock_by_stage_trial_key
                ):
                    await self.__try_auto_release_next_trial(
                        stage_key,
                        group_id,
                        payload,
                        released_trial_id,
                    )
                    self.__cancel_trial_terminal_watchdog_if_barrier_complete(
                        stage_key,
                        group_id,
                        released_trial_id,
                    )
            elif event_type == 'team_trial_sent':
                trial_context = payload.get('trial_context') or {}
                trial_id = int(trial_context.get('trial_id'))
                trial_sent_wallclock = self.__safe_float(payload.get('trial_sent_wallclock'))
                if trial_sent_wallclock is None:
                    self.__logger.warning(
                        "忽略缺少 trial_sent_wallclock 的 runtime stage event: event_type=%s payload=%s",
                        event_type,
                        payload,
                    )
                    return
                self.__trial_sent_wallclock_by_stage_trial_key[(stage_key, trial_id)] = trial_sent_wallclock
                self.__cancel_trial_release_delivery_watchdog(stage_key, trial_id)
                self.__ensure_trial_terminal_watchdog_task(
                    stage_key=stage_key,
                    group_id=group_id,
                    payload=payload,
                    trial_id=trial_id,
                    trial_sent_wallclock=trial_sent_wallclock,
                )
                if not self.__load_trial_barrier_team_id_list(stage_key, group_id):
                    await self.__try_auto_release_next_trial(stage_key, group_id, payload, trial_id)
            elif event_type == 'team_trial_terminal':
                trial_context = payload.get('trial_context') or {}
                trial_id = int(trial_context.get('trial_id'))
                terminal_team_id_set = self.__team_trial_terminal_by_stage_trial_key.setdefault((stage_key, trial_id), set())
                terminal_team_id_set.add(payload.get('team_id'))
                self.__logger.info(
                    "收到 team_trial_terminal: group_id=%s stage_key=%s trial_id=%s team_id=%s terminal_type=%s "
                    "terminal_progress=%s/%s observed_terminal_team_id_list=%s event_wallclock=%.6f",
                    group_id,
                    stage_key,
                    trial_id,
                    payload.get('team_id'),
                    payload.get('terminal_type'),
                    len(terminal_team_id_set),
                    len(self.__load_trial_barrier_team_id_list(stage_key, group_id)),
                    sorted(terminal_team_id_set),
                    time.time(),
                )
                await self.__try_auto_release_next_trial(stage_key, group_id, payload, trial_id)
                self.__cancel_trial_terminal_watchdog_if_barrier_complete(stage_key, group_id, trial_id)
            elif event_type == 'team_run_finalized':
                team_id = str(payload.get('team_id') or '').strip()
                terminal_run_status = str(payload.get('terminal_run_status') or '').strip().lower()
                if team_id == '':
                    self.__logger.warning("忽略缺少 team_id 的 team_run_finalized 事件: %s", payload)
                    return
                self.__finished_team_id_set.add(team_id)
                self.__logger.info(
                    "收到 team_run_finalized: group_id=%s team_id=%s terminal_run_status=%s finished_progress=%s/%s finished_team_id_list=%s",
                    group_id,
                    team_id,
                    terminal_run_status or 'unknown',
                    len(self.__finished_team_id_set),
                    len(self.__load_all_configured_team_id_set()),
                    sorted(self.__finished_team_id_set),
                )
                self.__try_mark_match_finished()
            else:
                self.__logger.warning("收到未识别的 runtime stage event_type: %s payload=%s", event_type, payload)
        except Exception:
            self.__logger.exception("处理 runtime stage event 失败: payload=%s", payload)
        finally:
            self.__refresh_runtime_stage_status_snapshot()

    async def __try_auto_release_online(self, stage_key: str, group_id: str, payload: dict) -> None:
        # stage 级放行逻辑：
        # 1. Collector 必须先声明自己已经发完 calibration；
        # 2. 同组所有队伍都必须进入校准终态：ready 或显式 forfeited；
        # 3. 满足后一次性发送 allow_online_stage + release_trial(1)。
        if self.__release_policy != self.__AUTO_RELEASE_WHEN_ALL_TEAMS_READY:
            return
        if not self.__match_started:
            self.__logger.info(
                "比赛尚未开始，online 继续等待前端 start-match: group_id=%s stage_key=%s",
                group_id,
                stage_key,
            )
            return
        if stage_key not in self.__collector_prepared_stage_key_set:
            return
        team_id_list = self.__team_id_list_by_group.get(group_id, [])
        if not team_id_list:
            self.__logger.warning("group 未配置 team_id_list，无法放行 online: group_id=%s stage_key=%s", group_id, stage_key)
            return
        ready_team_id_set = self.__team_ready_by_stage_key.get(stage_key, set())
        forfeited_team_id_set = self.__calibration_forfeited_team_by_stage_key.get(stage_key, set())
        terminal_team_id_set = ready_team_id_set | forfeited_team_id_set
        if not all(team_id in terminal_team_id_set for team_id in team_id_list):
            self.__logger.info(
                "online 暂不放行，仍有队伍未进入校准终态: "
                "group_id=%s stage_key=%s expected=%s ready=%s forfeited=%s collector_prepared=%s",
                group_id,
                stage_key,
                team_id_list,
                sorted(ready_team_id_set),
                sorted(forfeited_team_id_set),
                stage_key in self.__collector_prepared_stage_key_set,
            )
            return
        if stage_key in self.__online_stage_released_stage_key_set:
            self.__logger.debug("online 已放行，忽略重复触发: group_id=%s stage_key=%s", group_id, stage_key)
            return
        release_payload = dict(
            self.__collector_stage_release_payload_by_stage_key.get(stage_key)
            or {
                'release_type': 'online_stage_first_trial',
                'group_id': group_id,
                'collector_component_id': payload.get('collector_component_id'),
                'stage_context': payload.get('stage_context'),
                'next_trial_id': 1,
            }
        )
        await self.__release_or_queue_pending(stage_key=stage_key, payload=release_payload)

    def __ensure_calibration_timeout_task(self, stage_key: str, group_id: str) -> None:
        if self.__calibration_ready_timeout_seconds <= 0:
            return
        existing_task = self.__calibration_timeout_task_by_stage_key.get(stage_key)
        if existing_task is not None and not existing_task.done():
            return
        self.__calibration_timeout_task_by_stage_key[stage_key] = asyncio.create_task(
            self.__wait_and_fail_calibration_after_timeout(
                stage_key=stage_key,
                group_id=group_id,
            )
        )

    async def __wait_and_fail_calibration_after_timeout(self, stage_key: str, group_id: str) -> None:
        try:
            await asyncio.sleep(self.__calibration_ready_timeout_seconds)
            if stage_key in self.__online_stage_released_stage_key_set:
                return
            if stage_key not in self.__collector_prepared_stage_key_set:
                return
            team_id_list = self.__team_id_list_by_group.get(group_id, [])
            ready_team_id_set = self.__team_ready_by_stage_key.get(stage_key, set())
            forfeited_team_id_set = self.__calibration_forfeited_team_by_stage_key.get(stage_key, set())
            calibration_terminal_team_id_set = ready_team_id_set | forfeited_team_id_set
            missing_team_id_list = [
                team_id
                for team_id in team_id_list
                if team_id not in calibration_terminal_team_id_set
            ]
            if not missing_team_id_list:
                return
            release_payload = dict(self.__collector_stage_release_payload_by_stage_key.get(stage_key) or {})
            if not release_payload:
                return
            stage_context = dict(release_payload.get('stage_context') or {})
            self.__logger.error(
                "校准等待超时，禁止放行 online 阶段: group_id=%s stage_key=%s timeout_seconds=%s expected=%s ready=%s missing=%s",
                group_id,
                stage_key,
                self.__calibration_ready_timeout_seconds,
                team_id_list,
                sorted(ready_team_id_set),
                missing_team_id_list,
            )
            self.__safe_write_json_file(
                state_key=STATE_KEY_CURRENT_TRIAL,
                payload={
                    'group_id': group_id,
                    'collector_component_id': release_payload.get('collector_component_id'),
                    'subject_id': stage_context.get('subject_id'),
                    'exp_name': stage_context.get('exp_name'),
                    'exp_task': stage_context.get('exp_task'),
                    'session_id': stage_context.get('session_id'),
                    'status': 'error',
                    'error_type': 'CalibrationReadyTimeoutError',
                    'error_message': (
                        f"校准等待 {self.__calibration_ready_timeout_seconds:.1f}s 后仍有队伍未就绪: "
                        f"{missing_team_id_list}"
                    ),
                    'missing_ready_team_id_list': missing_team_id_list,
                    'ready_team_id_list': sorted(ready_team_id_set),
                    'forfeited_team_id_list': sorted(forfeited_team_id_set),
                    'recovery_advice': '检查消息桥接后，从当前阶段重新开始比赛。',
                    'updated_at': time.time(),
                },
                log_name='current_trial_calibration_timeout',
            )
        except asyncio.CancelledError:
            raise
        finally:
            self.__calibration_timeout_task_by_stage_key.pop(stage_key, None)

    def __ensure_trial_terminal_watchdog_task(
        self,
        stage_key: str,
        group_id: str,
        payload: dict,
        trial_id: int,
        trial_sent_wallclock: float,
    ) -> None:
        # 稳定主链统一只以真实的 team_trial_terminal(result/timeout) 事件驱动 trial 放行。
        # 历史上的 sent->watchdog->forced terminal 补齐路径会和真实终态事件竞态，
        # 在边界时刻产生“重复/过早 release_trial”，甚至出现 Collector 漏收那一次 release 的卡死现象。
        # 这里保留代码骨架仅作后续实验入口，但当前正式链路不再创建这类 watchdog。
        return
        if self.__trial_terminal_watchdog_grace_seconds < 0:
            return
        team_id_list = self.__load_trial_barrier_team_id_list(stage_key, group_id)
        if not team_id_list:
            return
        stage_context = payload.get('stage_context') or {}
        base_timeout_seconds = self.__resolve_trial_terminal_watchdog_base_timeout_seconds(stage_context)
        if base_timeout_seconds <= 0:
            return
        watchdog_key = (stage_key, int(trial_id))
        if self.__is_trial_terminal_barrier_complete(stage_key, group_id, trial_id):
            return
        existing_task = self.__trial_terminal_watchdog_task_by_stage_trial_key.get(watchdog_key)
        if existing_task is not None and not existing_task.done():
            return
        watchdog_deadline_wallclock = (
            trial_sent_wallclock + base_timeout_seconds + self.__trial_terminal_watchdog_grace_seconds
        )
        watchdog_delay_seconds = max(0.0, watchdog_deadline_wallclock - time.time())
        self.__trial_terminal_watchdog_deadline_wallclock_by_stage_trial_key[watchdog_key] = watchdog_deadline_wallclock
        self.__trial_terminal_watchdog_base_timeout_seconds_by_stage_trial_key[watchdog_key] = base_timeout_seconds
        self.__logger.info(
            "创建 trial 终态 watchdog: group_id=%s stage_key=%s trial_id=%s base_timeout_seconds=%s "
            "grace_seconds=%s deadline_wallclock=%.6f delay_seconds=%.3f",
            group_id,
            stage_key,
            trial_id,
            base_timeout_seconds,
            self.__trial_terminal_watchdog_grace_seconds,
            watchdog_deadline_wallclock,
            watchdog_delay_seconds,
        )
        self.__trial_terminal_watchdog_task_by_stage_trial_key[watchdog_key] = asyncio.create_task(
            self.__wait_and_force_trial_terminal_after_watchdog(
                delay_seconds=watchdog_delay_seconds,
                stage_key=stage_key,
                group_id=group_id,
                payload=dict(payload),
                trial_id=int(trial_id),
                base_timeout_seconds=base_timeout_seconds,
            )
        )

    def __cancel_trial_release_delivery_watchdog(self, stage_key: str, trial_id: int) -> None:
        watchdog_key = (stage_key, int(trial_id))
        watchdog_task = self.__trial_release_delivery_watchdog_task_by_stage_trial_key.pop(watchdog_key, None)
        if watchdog_task is not None and not watchdog_task.done():
            watchdog_task.cancel()
        self.__trial_release_payload_by_stage_trial_key.pop(watchdog_key, None)

    def __ensure_trial_release_delivery_watchdog_task(
        self,
        stage_key: str,
        group_id: str,
        collector_component_id: str,
        stage_context: dict,
        trial_id: int,
        release_payload: dict,
    ) -> None:
        if (
            self.__trial_release_delivery_watchdog_timeout_seconds <= 0
            or self.__trial_release_delivery_watchdog_max_resend_count <= 0
        ):
            return
        watchdog_key = (stage_key, int(trial_id))
        self.__cancel_trial_release_delivery_watchdog(stage_key, trial_id)
        self.__trial_release_payload_by_stage_trial_key[watchdog_key] = dict(release_payload)
        self.__logger.info(
            "创建 trial 放行投递 watchdog: group_id=%s stage_key=%s trial_id=%s timeout_seconds=%s max_resend_count=%s",
            group_id,
            stage_key,
            trial_id,
            self.__trial_release_delivery_watchdog_timeout_seconds,
            self.__trial_release_delivery_watchdog_max_resend_count,
        )
        self.__trial_release_delivery_watchdog_task_by_stage_trial_key[watchdog_key] = asyncio.create_task(
            self.__wait_and_resend_trial_release_if_unacknowledged(
                stage_key=stage_key,
                group_id=group_id,
                collector_component_id=collector_component_id,
                stage_context=stage_context,
                trial_id=int(trial_id),
            )
        )

    async def __wait_and_resend_trial_release_if_unacknowledged(
        self,
        stage_key: str,
        group_id: str,
        collector_component_id: str,
        stage_context: dict,
        trial_id: int,
    ) -> None:
        watchdog_key = (stage_key, int(trial_id))
        current_task = asyncio.current_task()
        try:
            for resend_attempt in range(1, self.__trial_release_delivery_watchdog_max_resend_count + 1):
                await asyncio.sleep(self.__trial_release_delivery_watchdog_timeout_seconds)
                if self.__trial_sent_wallclock_by_stage_trial_key.get(watchdog_key) is not None:
                    return
                if self.__released_trial_id_by_stage_key.get(stage_key, 0) != int(trial_id):
                    return
                release_payload = self.__trial_release_payload_by_stage_trial_key.get(watchdog_key)
                if not isinstance(release_payload, dict):
                    return
                self.__logger.warning(
                    "trial 放行后迟迟未收到 team_trial_sent，重发 release_trial: group_id=%s stage_key=%s "
                    "trial_id=%s collector_component_id=%s stage_context=%s resend_attempt=%s/%s timeout_seconds=%s",
                    group_id,
                    stage_key,
                    trial_id,
                    collector_component_id,
                    stage_context,
                    resend_attempt,
                    self.__trial_release_delivery_watchdog_max_resend_count,
                    self.__trial_release_delivery_watchdog_timeout_seconds,
                )
                try:
                    await self.__send_runtime_stage_control(dict(release_payload))
                except Exception:
                    self.__logger.exception(
                        "重发 release_trial 失败: group_id=%s stage_key=%s trial_id=%s resend_attempt=%s/%s",
                        group_id,
                        stage_key,
                        trial_id,
                        resend_attempt,
                        self.__trial_release_delivery_watchdog_max_resend_count,
                    )
            if self.__trial_sent_wallclock_by_stage_trial_key.get(watchdog_key) is None:
                self.__logger.error(
                    "trial 放行重发后仍未收到 team_trial_sent: group_id=%s stage_key=%s trial_id=%s "
                    "collector_component_id=%s stage_context=%s resend_count=%s",
                    group_id,
                    stage_key,
                    trial_id,
                    collector_component_id,
                    stage_context,
                    self.__trial_release_delivery_watchdog_max_resend_count,
                )
        except asyncio.CancelledError:
            raise
        finally:
            if self.__trial_release_delivery_watchdog_task_by_stage_trial_key.get(watchdog_key) is current_task:
                self.__trial_release_delivery_watchdog_task_by_stage_trial_key.pop(watchdog_key, None)
            self.__trial_release_payload_by_stage_trial_key.pop(watchdog_key, None)

    async def __wait_and_force_trial_terminal_after_watchdog(
        self,
        delay_seconds: float,
        stage_key: str,
        group_id: str,
        payload: dict,
        trial_id: int,
        base_timeout_seconds: float,
    ) -> None:
        watchdog_key = (stage_key, int(trial_id))
        try:
            await asyncio.sleep(delay_seconds)
            if self.__is_trial_terminal_barrier_complete(stage_key, group_id, trial_id):
                return
            team_id_list = self.__load_trial_barrier_team_id_list(stage_key, group_id)
            terminal_team_id_set = self.__team_trial_terminal_by_stage_trial_key.setdefault(watchdog_key, set())
            missing_team_id_list = [
                team_id
                for team_id in team_id_list
                if team_id not in terminal_team_id_set
            ]
            if not missing_team_id_list:
                return
            forced_team_id_set = self.__forced_terminal_team_id_set_by_stage_trial_key.setdefault(watchdog_key, set())
            forced_team_id_set.update(missing_team_id_list)
            terminal_team_id_set.update(missing_team_id_list)
            self.__logger.warning(
                "trial 终态 watchdog 触发，强制补齐缺失队伍终态以解除组屏障: group_id=%s stage_key=%s "
                "trial_id=%s base_timeout_seconds=%s grace_seconds=%s expected=%s missing=%s terminal_before_force=%s",
                group_id,
                stage_key,
                trial_id,
                base_timeout_seconds,
                self.__trial_terminal_watchdog_grace_seconds,
                team_id_list,
                missing_team_id_list,
                sorted(terminal_team_id_set - set(missing_team_id_list)),
            )
            forced_release_task = asyncio.create_task(
                self.__try_auto_release_next_trial(stage_key, group_id, payload, trial_id)
            )
            try:
                await asyncio.shield(forced_release_task)
            except asyncio.CancelledError:
                self.__logger.warning(
                    "trial 终态 watchdog 在关键放行路径收到取消，等待当前放行收尾后再退出: "
                    "group_id=%s stage_key=%s trial_id=%s",
                    group_id,
                    stage_key,
                    trial_id,
                )
                await asyncio.shield(forced_release_task)
                raise
            self.__refresh_runtime_stage_status_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.__logger.exception(
                "trial 终态 watchdog 执行失败: group_id=%s stage_key=%s trial_id=%s",
                group_id,
                stage_key,
                trial_id,
            )
        finally:
            self.__trial_terminal_watchdog_task_by_stage_trial_key.pop(watchdog_key, None)

    def __cancel_trial_terminal_watchdog_if_barrier_complete(self, stage_key: str, group_id: str, trial_id: int) -> None:
        if not self.__is_trial_terminal_barrier_complete(stage_key, group_id, trial_id):
            return
        watchdog_key = (stage_key, int(trial_id))
        watchdog_task = self.__trial_terminal_watchdog_task_by_stage_trial_key.pop(watchdog_key, None)
        if watchdog_task is not None and not watchdog_task.done():
            watchdog_task.cancel()

    def __load_trial_barrier_team_id_list(self, stage_key: str, group_id: str) -> list[str]:
        configured_team_id_list = list(self.__team_id_list_by_group.get(group_id, []))
        forfeited_team_id_set = self.__calibration_forfeited_team_by_stage_key.get(stage_key, set())
        return [
            team_id
            for team_id in configured_team_id_list
            if team_id not in forfeited_team_id_set
        ]

    def __is_trial_terminal_barrier_complete(self, stage_key: str, group_id: str, trial_id: int) -> bool:
        configured_team_id_list = self.__team_id_list_by_group.get(group_id, [])
        if not configured_team_id_list:
            return False
        team_id_list = self.__load_trial_barrier_team_id_list(stage_key, group_id)
        terminal_team_id_set = self.__team_trial_terminal_by_stage_trial_key.get((stage_key, int(trial_id)), set())
        return all(team_id in terminal_team_id_set for team_id in team_id_list)

    async def __try_auto_release_next_trial(
        self,
        stage_key: str,
        group_id: str,
        payload: dict,
        trial_id: int,
    ) -> None:
        # trial 级放行逻辑：
        # 当前 trial 只有在“同组所有队伍都进入终态(result/timeout)”后，
        # 才允许按照固定节拍继续放行下一 trial。
        configured_team_id_list = self.__team_id_list_by_group.get(group_id, [])
        if not configured_team_id_list:
            self.__logger.warning("group 未配置 team_id_list，无法放行 trial: group_id=%s stage_key=%s", group_id, stage_key)
            return
        team_id_list = self.__load_trial_barrier_team_id_list(stage_key, group_id)
        terminal_team_id_set = self.__team_trial_terminal_by_stage_trial_key.get((stage_key, trial_id), set())
        if not all(team_id in terminal_team_id_set for team_id in team_id_list):
            self.__logger.info(
                "trial 暂不放行，仍有队伍未进入终态: group_id=%s stage_key=%s trial_id=%s expected=%s terminal=%s",
                group_id,
                stage_key,
                trial_id,
                team_id_list,
                sorted(terminal_team_id_set),
            )
            return
        online_trial_count = self.__online_trial_count_by_stage_key.get(stage_key)
        if online_trial_count is not None and int(trial_id) >= int(online_trial_count):
            self.__logger.info(
                "当前 trial 已是最后一个 online trial，不再继续放行: group_id=%s stage_key=%s trial_id=%s online_trial_count=%s",
                group_id,
                stage_key,
                trial_id,
                online_trial_count,
            )
            await self.__mark_online_stage_completed(
                stage_key=stage_key,
                group_id=group_id,
                collector_component_id=payload.get('collector_component_id'),
                stage_context=payload.get('stage_context'),
                final_trial_id=int(trial_id),
            )
            return
        next_trial_id = int(trial_id) + 1
        if self.__released_trial_id_by_stage_key.get(stage_key, 0) >= next_trial_id:
            self.__logger.debug(
                "trial 已放行，忽略重复触发: group_id=%s stage_key=%s trial_id=%s next_trial_id=%s",
                group_id,
                stage_key,
                trial_id,
                next_trial_id,
            )
            return
        current_trial_release_monotonic = self.__trial_release_monotonic_by_stage_trial_key.get((stage_key, int(trial_id)))
        current_trial_sent_wallclock = self.__trial_sent_wallclock_by_stage_trial_key.get((stage_key, int(trial_id)))
        barrier_completed_wallclock = time.time()
        now = time.perf_counter()
        earliest_next_release_monotonic = (
            current_trial_release_monotonic + self.__trial_release_interval_seconds
            if current_trial_release_monotonic is not None else now
        )
        remaining_seconds = earliest_next_release_monotonic - now
        next_release_target_wallclock = (
            current_trial_sent_wallclock + remaining_seconds
            if current_trial_sent_wallclock is not None else None
        )
        if next_release_target_wallclock is not None:
            self.__next_release_target_wallclock_by_stage_trial_key[(stage_key, int(trial_id))] = next_release_target_wallclock
        self.__logger.info(
            "trial 终态已齐，评估下一 trial 放行时机: group_id=%s stage_key=%s current_trial_id=%s next_trial_id=%s "
            "barrier_completed_wallclock=%.6f last_release_monotonic=%s trial_sent_wallclock=%s "
            "trial_release_interval_seconds=%.3f earliest_next_release_monotonic=%.6f remaining_seconds=%.3f "
            "next_release_target_wallclock=%s",
            group_id,
            stage_key,
            trial_id,
            next_trial_id,
            barrier_completed_wallclock,
            f"{current_trial_release_monotonic:.6f}" if current_trial_release_monotonic is not None else "None",
            f"{current_trial_sent_wallclock:.6f}" if current_trial_sent_wallclock is not None else "None",
            self.__trial_release_interval_seconds,
            earliest_next_release_monotonic,
            remaining_seconds,
            f"{next_release_target_wallclock:.6f}" if next_release_target_wallclock is not None else "None",
        )
        if remaining_seconds > 0:
            delay_key = (stage_key, int(trial_id))
            delayed_task = self.__delayed_release_task_by_stage_trial_key.get(delay_key)
            if delayed_task is not None and not delayed_task.done():
                self.__logger.debug(
                    "trial 延迟放行任务已存在，忽略重复创建: group_id=%s stage_key=%s trial_id=%s remaining_seconds=%.3f",
                    group_id,
                    stage_key,
                    trial_id,
                    remaining_seconds,
                )
                return
            self.__logger.info(
                "trial 已全部终态，但需等待固定节拍后放行: group_id=%s stage_key=%s current_trial_id=%s "
                "next_trial_id=%s trial_sent_wallclock=%s next_release_target_wallclock=%s remaining_seconds=%.3f",
                group_id,
                stage_key,
                trial_id,
                next_trial_id,
                f"{current_trial_sent_wallclock:.6f}" if current_trial_sent_wallclock is not None else "None",
                f"{next_release_target_wallclock:.6f}" if next_release_target_wallclock is not None else "None",
                remaining_seconds,
            )
            self.__delayed_release_task_by_stage_trial_key[delay_key] = asyncio.create_task(
                self.__delay_release_next_trial(
                    delay_seconds=remaining_seconds,
                    stage_key=stage_key,
                    group_id=group_id,
                    collector_component_id=payload.get('collector_component_id'),
                    stage_context=payload.get('stage_context'),
                    current_trial_id=trial_id,
                    next_trial_id=next_trial_id,
                )
            )
            return
        await self.__release_or_queue_pending(
            stage_key=stage_key,
            payload={
                'release_type': 'trial',
                'group_id': group_id,
                'collector_component_id': payload.get('collector_component_id'),
                'stage_context': payload.get('stage_context'),
                'current_trial_id': int(trial_id),
                'next_trial_id': int(next_trial_id),
            },
        )

    async def __delay_release_next_trial(
        self,
        delay_seconds: float,
        stage_key: str,
        group_id: str,
        collector_component_id: str,
        stage_context: dict,
        current_trial_id: int,
        next_trial_id: int,
    ) -> None:
        try:
            delay_start_wallclock = time.time()
            self.__logger.info(
                "开始执行 trial 延迟放行: group_id=%s stage_key=%s current_trial_id=%s next_trial_id=%s "
                "delay_seconds=%.3f delay_start_wallclock=%.6f",
                group_id,
                stage_key,
                current_trial_id,
                next_trial_id,
                delay_seconds,
                delay_start_wallclock,
            )
            await asyncio.sleep(delay_seconds)
            self.__logger.info(
                "trial 延迟放行等待结束: group_id=%s stage_key=%s current_trial_id=%s next_trial_id=%s "
                "delay_seconds=%.3f release_attempt_wallclock=%.6f",
                group_id,
                stage_key,
                current_trial_id,
                next_trial_id,
                delay_seconds,
                time.time(),
            )
            await self.__release_or_queue_pending(
                stage_key=stage_key,
                payload={
                    'release_type': 'trial',
                    'group_id': group_id,
                    'collector_component_id': collector_component_id,
                    'stage_context': stage_context,
                    'current_trial_id': int(current_trial_id),
                    'next_trial_id': int(next_trial_id),
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.__logger.exception(
                "trial 延迟放行任务执行失败: group_id=%s stage_key=%s current_trial_id=%s next_trial_id=%s delay_seconds=%.3f",
                group_id,
                stage_key,
                current_trial_id,
                next_trial_id,
                delay_seconds,
            )
        finally:
            self.__delayed_release_task_by_stage_trial_key.pop((stage_key, int(current_trial_id)), None)

    async def __release_or_queue_pending(self, stage_key: str, payload: dict) -> None:
        if self.__paused or self.__pause_requested:
            self.__queue_pending_release(stage_key, payload)
            self.__activate_pause_at_boundary(stage_key=stage_key, payload=payload)
            return
        await self.__perform_release(stage_key=stage_key, payload=payload)

    def __queue_pending_release(self, stage_key: str, payload: dict) -> None:
        existing_payload = self.__pending_release_payload_by_stage_key.get(stage_key)
        if existing_payload == payload:
            self.__logger.debug("待恢复放行已存在，忽略重复记录: stage_key=%s payload=%s", stage_key, payload)
            return
        self.__pending_release_payload_by_stage_key[stage_key] = dict(payload)
        self.__logger.info("记录待恢复放行: stage_key=%s payload=%s", stage_key, payload)

    def __activate_pause_at_boundary(self, stage_key: str, payload: dict) -> None:
        if self.__paused or not self.__pause_requested:
            return
        self.__pause_requested = False
        self.__paused = True
        self.__paused_wallclock = time.time()
        self.__logger.info(
            "比赛已在 trial/stage 边界进入 paused: stage_key=%s boundary_context=%s paused_at=%.6f",
            stage_key,
            payload,
            self.__paused_wallclock,
        )
        self.__write_match_control_status()
        self.__refresh_runtime_stage_status_snapshot()

    def __try_activate_pause_for_pre_online_stage_boundary(self) -> bool:
        if self.__paused or not self.__pause_requested:
            return False
        for stage_key in sorted(self.__collector_prepared_stage_key_set):
            if stage_key in self.__online_stage_released_stage_key_set:
                continue
            boundary_payload = self.__collector_stage_release_payload_by_stage_key.get(stage_key)
            if not isinstance(boundary_payload, dict):
                continue
            self.__logger.info(
                "检测到 calibration->online 等待边界，立即响应 pause 请求: stage_key=%s ready=%s expected_release=%s",
                stage_key,
                sorted(self.__team_ready_by_stage_key.get(stage_key, set())),
                boundary_payload,
            )
            self.__activate_pause_at_boundary(stage_key=stage_key, payload=boundary_payload)
            return True
        return False

    async def __perform_release(self, stage_key: str, payload: dict) -> None:
        release_type = str(payload.get('release_type') or '').strip()
        if release_type == 'online_stage_first_trial':
            if stage_key in self.__online_stage_released_stage_key_set:
                self.__logger.debug("online 已放行，忽略重复发送: stage_key=%s payload=%s", stage_key, payload)
                return
            timeout_task = self.__calibration_timeout_task_by_stage_key.pop(stage_key, None)
            if timeout_task is not None and not timeout_task.done():
                timeout_task.cancel()
            self.__online_stage_released_stage_key_set.add(stage_key)
            self.__logger.info(
                "online 放行: group_id=%s stage_key=%s first_trial_id=%s forced_by_calibration_timeout=%s missing_ready_team_id_list=%s",
                payload.get('group_id'),
                stage_key,
                payload.get('next_trial_id'),
                bool(payload.get('forced_by_calibration_timeout')),
                payload.get('missing_ready_team_id_list'),
            )
            await self.__send_runtime_stage_control(
                {
                    'control_type': 'allow_online_stage',
                    'group_id': payload.get('group_id'),
                    'collector_component_id': payload.get('collector_component_id'),
                    'stage_context': payload.get('stage_context'),
                }
            )
            await self.__release_trial(
                stage_key=stage_key,
                group_id=payload.get('group_id'),
                collector_component_id=payload.get('collector_component_id'),
                stage_context=payload.get('stage_context'),
                next_trial_id=int(payload.get('next_trial_id') or 1),
                current_trial_id=None,
            )
            return
        if release_type == 'online_stage_completed':
            if stage_key in self.__online_stage_completed_stage_key_set:
                self.__logger.debug("online stage completed 已发送，忽略重复发送: stage_key=%s payload=%s", stage_key, payload)
                return
            self.__online_stage_completed_stage_key_set.add(stage_key)
            self.__logger.info(
                "online stage 完成: group_id=%s stage_key=%s final_trial_id=%s",
                payload.get('group_id'),
                stage_key,
                payload.get('final_trial_id'),
            )
            await self.__send_runtime_stage_control(
                {
                    'control_type': 'complete_online_stage',
                    'group_id': payload.get('group_id'),
                    'collector_component_id': payload.get('collector_component_id'),
                    'stage_context': payload.get('stage_context'),
                    'final_trial_id': int(payload.get('final_trial_id') or 0),
                }
            )
            return
        if release_type == 'trial':
            await self.__release_trial(
                stage_key=stage_key,
                group_id=payload.get('group_id'),
                collector_component_id=payload.get('collector_component_id'),
                stage_context=payload.get('stage_context'),
                next_trial_id=int(payload.get('next_trial_id')),
                current_trial_id=payload.get('current_trial_id'),
            )
            return
        self.__logger.warning("收到未识别的 release_type，忽略执行: stage_key=%s payload=%s", stage_key, payload)

    async def __mark_online_stage_completed(
        self,
        stage_key: str,
        group_id: str,
        collector_component_id: str | None,
        stage_context: dict | None,
        final_trial_id: int,
    ) -> None:
        if stage_key in self.__online_stage_completed_stage_key_set:
            return
        await self.__release_or_queue_pending(
            stage_key=stage_key,
            payload={
                'release_type': 'online_stage_completed',
                'group_id': group_id,
                'collector_component_id': collector_component_id,
                'stage_context': stage_context,
                'final_trial_id': int(final_trial_id),
            },
        )

    async def __release_trial(
        self,
        stage_key: str,
        group_id: str,
        collector_component_id: str,
        stage_context: dict,
        next_trial_id: int,
        current_trial_id: int | None,
    ) -> None:
        release_lock = self.__trial_release_lock_by_stage_key.setdefault(stage_key, asyncio.Lock())
        async with release_lock:
            if self.__released_trial_id_by_stage_key.get(stage_key, 0) >= int(next_trial_id):
                self.__logger.debug(
                    "trial 已放行，忽略重复发送: group_id=%s stage_key=%s current_trial_id=%s next_trial_id=%s",
                    group_id,
                    stage_key,
                    current_trial_id,
                    next_trial_id,
                )
                return
            release_wallclock = time.time()
            current_trial_sent_wallclock = (
                self.__trial_sent_wallclock_by_stage_trial_key.get((stage_key, int(current_trial_id)))
                if current_trial_id is not None else None
            )
            current_trial_next_release_target_wallclock = (
                self.__next_release_target_wallclock_by_stage_trial_key.get((stage_key, int(current_trial_id)))
                if current_trial_id is not None else None
            )
            self.__logger.info(
                "trial 放行: group_id=%s stage_key=%s current_trial_id=%s next_trial_id=%s trial_release_interval_seconds=%s "
                "release_wallclock=%.6f current_trial_sent_wallclock=%s current_trial_next_release_target_wallclock=%s",
                group_id,
                stage_key,
                current_trial_id,
                next_trial_id,
                self.__trial_release_interval_seconds,
                release_wallclock,
                f"{current_trial_sent_wallclock:.6f}" if current_trial_sent_wallclock is not None else "None",
                f"{current_trial_next_release_target_wallclock:.6f}"
                if current_trial_next_release_target_wallclock is not None else "None",
            )
            release_payload = {
                'control_type': 'release_trial',
                'group_id': group_id,
                'collector_component_id': collector_component_id,
                'stage_context': stage_context,
                'trial_id': int(next_trial_id),
                'release_wallclock': release_wallclock,
                'trial_release_interval_seconds': self.__trial_release_interval_seconds,
            }
            await self.__send_runtime_stage_control(release_payload)
            self.__released_trial_id_by_stage_key[stage_key] = int(next_trial_id)
            self.__trial_release_monotonic_by_stage_trial_key[(stage_key, int(next_trial_id))] = time.perf_counter()
            release_watchdog_task = self.__trial_terminal_watchdog_task_by_stage_trial_key.pop(
                (stage_key, int(next_trial_id)),
                None,
            )
            if release_watchdog_task is not None and not release_watchdog_task.done():
                release_watchdog_task.cancel()
            self.__trial_terminal_watchdog_deadline_wallclock_by_stage_trial_key.pop((stage_key, int(next_trial_id)), None)
            self.__trial_terminal_watchdog_base_timeout_seconds_by_stage_trial_key.pop((stage_key, int(next_trial_id)), None)
            self.__forced_terminal_team_id_set_by_stage_trial_key.pop((stage_key, int(next_trial_id)), None)
            self.__ensure_trial_release_delivery_watchdog_task(
                stage_key=stage_key,
                group_id=group_id,
                collector_component_id=collector_component_id,
                stage_context=stage_context,
                trial_id=int(next_trial_id),
                release_payload=release_payload,
            )
    async def __send_runtime_stage_control(self, payload: dict) -> None:
        self.__logger.info("发送 runtime stage control: %s", payload)
        await self._component_framework.send_message(
            self.__RUNTIME_STAGE_CONTROL_MESSAGE_KEY,
            CommonMessageConverter.model_to_protobuf(
                DataMessageModel(
                    package=DataPackageModel(
                        data_position=0.0,
                        data=json.dumps(payload, ensure_ascii=False),
                    )
                )
            ).SerializeToString(),
        )

    def __refresh_runtime_stage_status_snapshot(self) -> None:
        # status snapshot 只做本地缓存，不参与当前决赛主链路的消息发送。
        # 后续若接 UI，优先通过专用查询接口读取这个 snapshot，而不是在主流程里自动推送。
        self.__latest_runtime_stage_status_payload = self.__build_runtime_stage_status_snapshot()
        self.__write_runtime_stage_status_snapshot()
        self.__logger.debug("刷新 runtime stage status snapshot: %s", self.__latest_runtime_stage_status_payload)

    def __build_runtime_stage_status_snapshot(self) -> dict:
        all_stage_key_set = set(self.__collector_prepared_stage_key_set)
        all_stage_key_set.update(self.__team_ready_by_stage_key.keys())
        all_stage_key_set.update(self.__calibration_forfeited_team_by_stage_key.keys())
        all_stage_key_set.update(self.__released_trial_id_by_stage_key.keys())
        all_stage_key_set.update(stage_key for stage_key, _ in self.__team_trial_terminal_by_stage_trial_key.keys())
        all_stage_key_set.update(
            stage_key
            for stage_key, _ in self.__trial_terminal_watchdog_deadline_wallclock_by_stage_trial_key.keys()
        )
        all_stage_key_set.update(
            stage_key
            for stage_key, _ in self.__forced_terminal_team_id_set_by_stage_trial_key.keys()
        )

        stage_status_list_by_group: dict[str, list[dict]] = {}
        for stage_key in sorted(all_stage_key_set):
            group_id, stage_context = self.__parse_stage_key(stage_key)
            configured_team_id_list = list(self.__team_id_list_by_group.get(group_id, []))
            ready_team_id_list = sorted(self.__team_ready_by_stage_key.get(stage_key, set()))
            forfeited_team_id_list = sorted(
                self.__calibration_forfeited_team_by_stage_key.get(stage_key, set())
            )
            calibration_terminal_team_id_set = set(ready_team_id_list) | set(forfeited_team_id_list)
            pending_ready_team_id_list = [
                team_id
                for team_id in configured_team_id_list
                if team_id not in calibration_terminal_team_id_set
            ]
            trial_barrier_team_id_list = self.__load_trial_barrier_team_id_list(stage_key, group_id)
            trial_barrier_team_id_set = set(trial_barrier_team_id_list)
            trial_terminal_team_id_list_by_trial = {
                str(trial_id): sorted(team_id_set)
                for (candidate_stage_key, trial_id), team_id_set in self.__team_trial_terminal_by_stage_trial_key.items()
                if candidate_stage_key == stage_key
            }
            trial_observed_terminal_team_id_list_by_trial = {
                str(trial_id): sorted(
                    team_id_set
                    - self.__forced_terminal_team_id_set_by_stage_trial_key.get((candidate_stage_key, trial_id), set())
                )
                for (candidate_stage_key, trial_id), team_id_set in self.__team_trial_terminal_by_stage_trial_key.items()
                if candidate_stage_key == stage_key
            }
            trial_forced_terminal_team_id_list_by_trial = {
                str(trial_id): sorted(team_id_set)
                for (candidate_stage_key, trial_id), team_id_set in self.__forced_terminal_team_id_set_by_stage_trial_key.items()
                if candidate_stage_key == stage_key
            }
            completed_trial_id_list = sorted(
                trial_id
                for (candidate_stage_key, trial_id), team_id_set in self.__team_trial_terminal_by_stage_trial_key.items()
                if candidate_stage_key == stage_key
                and trial_barrier_team_id_set.issubset(team_id_set)
            )
            completed_trial_id_set = set(completed_trial_id_list)
            max_completed_trial_id = completed_trial_id_list[-1] if completed_trial_id_list else 0
            trial_sent_wallclock_by_trial = {
                str(trial_id): trial_sent_wallclock
                for (candidate_stage_key, trial_id), trial_sent_wallclock in self.__trial_sent_wallclock_by_stage_trial_key.items()
                if candidate_stage_key == stage_key
            }
            next_release_target_wallclock_by_trial = {
                str(trial_id): next_release_target_wallclock
                for (candidate_stage_key, trial_id), next_release_target_wallclock in self.__next_release_target_wallclock_by_stage_trial_key.items()
                if candidate_stage_key == stage_key
            }
            trial_terminal_watchdog_deadline_wallclock_by_trial = {
                str(trial_id): watchdog_deadline_wallclock
                for (candidate_stage_key, trial_id), watchdog_deadline_wallclock in (
                    self.__trial_terminal_watchdog_deadline_wallclock_by_stage_trial_key.items()
                )
                if candidate_stage_key == stage_key
            }
            trial_terminal_watchdog_base_timeout_seconds_by_trial = {
                str(trial_id): watchdog_base_timeout_seconds
                for (candidate_stage_key, trial_id), watchdog_base_timeout_seconds in (
                    self.__trial_terminal_watchdog_base_timeout_seconds_by_stage_trial_key.items()
                )
                if candidate_stage_key == stage_key
            }
            stage_status_list_by_group.setdefault(group_id, []).append(
                {
                    'stage_key': stage_key,
                    'stage_context': stage_context,
                    'collector_prepared': stage_key in self.__collector_prepared_stage_key_set,
                    'online_stage_completed': stage_key in self.__online_stage_completed_stage_key_set,
                    'ready_team_id_list': ready_team_id_list,
                    'forfeited_team_id_list': forfeited_team_id_list,
                    'calibration_forfeit_detail_by_team': dict(
                        self.__calibration_forfeit_detail_by_stage_key.get(stage_key, {})
                    ),
                    'pending_ready_team_id_list': pending_ready_team_id_list,
                    'trial_barrier_team_id_list': trial_barrier_team_id_list,
                    'online_stage_released': stage_key in self.__online_stage_released_stage_key_set,
                    'pending_release': self.__pending_release_payload_by_stage_key.get(stage_key),
                    'online_trial_count': self.__online_trial_count_by_stage_key.get(stage_key),
                    'released_trial_id': int(self.__released_trial_id_by_stage_key.get(stage_key, 0)),
                    'completed_trial_id_list': completed_trial_id_list,
                    'completed_trial_count': len(completed_trial_id_set),
                    'max_completed_trial_id': int(max_completed_trial_id),
                    'trial_sent_wallclock_by_trial': trial_sent_wallclock_by_trial,
                    'next_release_target_wallclock_by_trial': next_release_target_wallclock_by_trial,
                    'trial_terminal_team_id_list_by_trial': trial_terminal_team_id_list_by_trial,
                    'trial_observed_terminal_team_id_list_by_trial': trial_observed_terminal_team_id_list_by_trial,
                    'trial_forced_terminal_team_id_list_by_trial': trial_forced_terminal_team_id_list_by_trial,
                    'trial_terminal_watchdog_deadline_wallclock_by_trial': (
                        trial_terminal_watchdog_deadline_wallclock_by_trial
                    ),
                    'trial_terminal_watchdog_base_timeout_seconds_by_trial': (
                        trial_terminal_watchdog_base_timeout_seconds_by_trial
                    ),
                }
            )

        group_id_list = sorted(set(self.__team_id_list_by_group.keys()) | set(stage_status_list_by_group.keys()))
        group_status_list = [
            {
                'group_id': group_id,
                'configured_team_id_list': list(self.__team_id_list_by_group.get(group_id, [])),
                'stage_status_list': stage_status_list_by_group.get(group_id, []),
            }
            for group_id in group_id_list
        ]
        return {
            'release_policy': self.__release_policy,
            'trial_release_interval_seconds': self.__trial_release_interval_seconds,
            'trial_terminal_watchdog_base_timeout_seconds': self.__trial_terminal_watchdog_base_timeout_seconds,
            'trial_terminal_watchdog_grace_seconds': self.__trial_terminal_watchdog_grace_seconds,
            'match_control_status': self.__build_match_control_status_payload(),
            'updated_at': time.time(),
            'group_status_list': group_status_list,
        }

    @staticmethod
    def __resolve_live_state_root_dir() -> Path:
        return Path(__file__).resolve().parents[4] / 'results' / 'live'

    @staticmethod
    def __resolve_control_root_dir() -> Path:
        return Path(__file__).resolve().parents[4] / 'results' / 'control'

    def __write_runtime_stage_status_snapshot(self) -> None:
        self.__safe_write_json_file(
            state_key=STATE_KEY_RUNTIME_STAGE_STATUS,
            payload=self.__latest_runtime_stage_status_payload,
            log_name='runtime_stage_status',
        )

    def __build_match_control_status_payload(self) -> dict:
        return {
            'waiting_start': not self.__match_started,
            'match_started': self.__match_started,
            'match_finished': self.__match_finished,
            'finished_at': self.__match_finished_wallclock,
            'finished_team_id_list': sorted(self.__finished_team_id_set),
            'pause_requested': self.__pause_requested,
            'paused': self.__paused,
            'started_at': self.__match_started_wallclock,
            'paused_at': self.__paused_wallclock,
            'resumed_at': self.__resumed_wallclock,
            'last_seen_start_request_at': self.__last_seen_start_request_at,
            'last_seen_pause_request_at': self.__last_seen_pause_request_at,
            'last_seen_resume_request_at': self.__last_seen_resume_request_at,
            'coordinator_started_at': self.__coordinator_started_wallclock,
            'updated_at': time.time(),
        }

    def __write_match_control_status(self) -> None:
        self.__safe_write_json_file(
            state_key=STATE_KEY_MATCH_CONTROL_STATUS,
            payload=self.__build_match_control_status_payload(),
            log_name='match_control_status',
        )

    def __load_all_configured_team_id_set(self) -> set[str]:
        configured_team_id_set: set[str] = set()
        for team_id_list in self.__team_id_list_by_group.values():
            for team_id in team_id_list:
                team_id_text = str(team_id or '').strip()
                if team_id_text:
                    configured_team_id_set.add(team_id_text)
        return configured_team_id_set

    def __try_mark_match_finished(self) -> None:
        if self.__match_finished:
            return
        configured_team_id_set = self.__load_all_configured_team_id_set()
        if not configured_team_id_set:
            return
        if not configured_team_id_set.issubset(self.__finished_team_id_set):
            return
        self.__match_finished = True
        self.__match_finished_wallclock = time.time()
        self.__pause_requested = False
        self.__paused = False
        self.__paused_wallclock = None
        self.__logger.info(
            "全部赛队显式上报完赛，比赛进入 finished: finished_at=%.6f team_id_list=%s",
            self.__match_finished_wallclock,
            sorted(configured_team_id_set),
        )
        self.__write_match_control_status()

    async def __poll_control_requests(self) -> None:
        start_match_request_file_path = self.__resolve_control_root_dir() / 'start_match_request.json'
        pause_request_file_path = self.__resolve_control_root_dir() / 'pause_request.json'
        resume_control_request_file_path = self.__resolve_control_root_dir() / 'resume_control_request.json'
        while not self.__finish_event.is_set():
            try:
                status_changed = False
                if not self.__match_started:
                    payload = self.__read_json_file(start_match_request_file_path)
                    if isinstance(payload, dict):
                        requested_at = payload.get('requested_at')
                        request_type = payload.get('request_type')
                        if request_type == 'start_match_request' and self.__is_new_start_request(requested_at):
                            self.__last_seen_start_request_at = float(requested_at)
                            self.__match_started = True
                            self.__match_started_wallclock = time.time()
                            self.__pause_requested = False
                            self.__paused = False
                            self.__paused_wallclock = None
                            self.__resumed_wallclock = None
                            self.__logger.info(
                                "收到 start-match 请求，比赛进入 running 准备态: request_file=%s request_type=%s requested_at=%s started_at=%s",
                                start_match_request_file_path,
                                request_type,
                                requested_at,
                                self.__match_started_wallclock,
                            )
                            status_changed = True
                if self.__match_started:
                    pause_payload = self.__read_json_file(pause_request_file_path)
                    if isinstance(pause_payload, dict):
                        requested_at = pause_payload.get('requested_at')
                        request_type = pause_payload.get('request_type')
                        if request_type == 'pause_request' and self.__is_new_control_request(
                            requested_at=requested_at,
                            last_seen_at=self.__last_seen_pause_request_at,
                        ):
                            self.__last_seen_pause_request_at = float(requested_at)
                            if not self.__is_runtime_phase_control_request(requested_at):
                                self.__logger.info("忽略开赛前遗留的 pause 请求: requested_at=%s started_at=%s", requested_at, self.__match_started_wallclock)
                            elif self.__paused:
                                self.__logger.info("收到 pause 请求，但比赛已处于 paused 状态: requested_at=%s", requested_at)
                            else:
                                self.__pause_requested = True
                                if not self.__try_activate_pause_for_pre_online_stage_boundary():
                                    self.__logger.info(
                                        "收到 pause 请求，将在下一个 trial/stage 边界暂停: requested_at=%s",
                                        requested_at,
                                    )
                            status_changed = True
                    resume_payload = self.__read_json_file(resume_control_request_file_path)
                    if isinstance(resume_payload, dict):
                        requested_at = resume_payload.get('requested_at')
                        request_type = resume_payload.get('request_type')
                        if request_type == 'resume_control_request' and self.__is_new_control_request(
                            requested_at=requested_at,
                            last_seen_at=self.__last_seen_resume_request_at,
                        ):
                            self.__last_seen_resume_request_at = float(requested_at)
                            if not self.__is_runtime_phase_control_request(requested_at):
                                self.__logger.info("忽略开赛前遗留的 resume 请求: requested_at=%s started_at=%s", requested_at, self.__match_started_wallclock)
                            elif self.__pause_requested or self.__paused:
                                self.__pause_requested = False
                                self.__paused = False
                                self.__resumed_wallclock = time.time()
                                self.__logger.info(
                                    "收到 resume 请求，比赛恢复继续: requested_at=%s resumed_at=%s pending_release_count=%s",
                                    requested_at,
                                    self.__resumed_wallclock,
                                    len(self.__pending_release_payload_by_stage_key),
                                )
                                await self.__flush_pending_releases_after_resume()
                            else:
                                self.__logger.info("收到 resume 请求，但当前比赛未处于 pause 状态: requested_at=%s", requested_at)
                            status_changed = True
                if status_changed:
                    self.__write_match_control_status()
                    self.__refresh_runtime_stage_status_snapshot()
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.__logger.exception("轮询控制文件失败")
                await asyncio.sleep(1.0)

    def __is_new_start_request(self, requested_at) -> bool:
        try:
            request_wallclock = float(requested_at)
        except (TypeError, ValueError):
            return False
        if request_wallclock < float(self.__coordinator_started_wallclock):
            return False
        if self.__last_seen_start_request_at is not None and request_wallclock <= self.__last_seen_start_request_at:
            return False
        return True

    def __is_new_control_request(self, requested_at, last_seen_at) -> bool:
        try:
            request_wallclock = float(requested_at)
        except (TypeError, ValueError):
            return False
        if request_wallclock < float(self.__coordinator_started_wallclock):
            return False
        if last_seen_at is not None and request_wallclock <= float(last_seen_at):
            return False
        return True

    def __is_runtime_phase_control_request(self, requested_at) -> bool:
        if self.__match_started_wallclock is None:
            return False
        try:
            request_wallclock = float(requested_at)
        except (TypeError, ValueError):
            return False
        return request_wallclock > float(self.__match_started_wallclock)

    async def __flush_pending_releases_after_resume(self) -> None:
        if not self.__pending_release_payload_by_stage_key:
            return
        pending_items = sorted(self.__pending_release_payload_by_stage_key.items(), key=lambda item: item[0])
        self.__pending_release_payload_by_stage_key.clear()
        for stage_key, payload in pending_items:
            try:
                await self.__perform_release(stage_key=stage_key, payload=payload)
            except Exception:
                self.__pending_release_payload_by_stage_key[stage_key] = payload
                self.__logger.exception("resume 后执行待恢复放行失败，已重新入队: stage_key=%s payload=%s", stage_key, payload)

    def __safe_write_json_file(self, state_key: str, payload: dict, log_name: str) -> None:
        try:
            write_json_state(
                resolve_runtime_state_db_path(PROJECT_ROOT),
                state_key,
                payload,
            )
        except OSError:
            self.__logger.exception(
                "写入 live SQLite 状态失败: log_name=%s state_key=%s",
                log_name,
                state_key,
            )

    @staticmethod
    def __read_json_file(file_path: Path):
        if not file_path.exists():
            return None
        try:
            return json.loads(file_path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return None

    @staticmethod
    def __parse_data_message_payload(data: bytes):
        data_message_model = CommonMessageConverter.protobuf_to_model(
            DataMessage_pb2.FromString(data)
        )
        package = data_message_model.package
        if not isinstance(package, DataPackageModel):
            return None
        raw_data = package.data
        if not isinstance(raw_data, str):
            return None
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def __build_stage_key(group_id: str, stage_context: dict) -> str:
        return (
            f"{group_id}|{stage_context.get('subject_id')}|{stage_context.get('exp_name')}|"
            f"{stage_context.get('exp_task')}|{stage_context.get('session_id')}"
        )

    @staticmethod
    def __parse_stage_key(stage_key: str) -> tuple[str, dict]:
        group_id, subject_id, exp_name, exp_task, session_id = (stage_key.split('|', 4) + [''] * 5)[:5]
        return group_id, {
            'subject_id': subject_id,
            'exp_name': exp_name,
            'exp_task': exp_task,
            'session_id': session_id,
        }

    @staticmethod
    def __safe_float(value) -> float | None:
        try:
            parsed_value = float(value)
        except (TypeError, ValueError):
            return None
        if parsed_value <= 0:
            return None
        return parsed_value

    @classmethod
    def __parse_timeout_mapping(cls, raw_mapping) -> dict[str, float]:
        if not isinstance(raw_mapping, dict):
            return {}
        parsed_mapping = {}
        for key, value in raw_mapping.items():
            key_text = str(key or '').strip()
            parsed_value = cls.__safe_float(value)
            if key_text == '' or parsed_value is None:
                continue
            parsed_mapping[key_text] = parsed_value
        return parsed_mapping

    def __resolve_trial_terminal_watchdog_base_timeout_seconds(self, stage_context: dict) -> float:
        exp_name = str((stage_context or {}).get('exp_name') or '').strip()
        exp_task = str((stage_context or {}).get('exp_task') or '').strip()
        task_id = self.__build_task_id(exp_name=exp_name, exp_task=exp_task)
        if task_id in self.__trial_terminal_watchdog_base_timeout_seconds_by_task_id:
            return self.__trial_terminal_watchdog_base_timeout_seconds_by_task_id[task_id]
        if exp_name in self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_name:
            return self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_name[exp_name]
        if exp_task in self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_task:
            return self.__trial_terminal_watchdog_base_timeout_seconds_by_exp_task[exp_task]
        return self.__trial_terminal_watchdog_base_timeout_seconds

    @staticmethod
    def __build_task_id(exp_name: str, exp_task: str) -> str:
        exp_name_text = str(exp_name or '').strip()
        exp_task_text = str(exp_task or '').strip()
        if exp_name_text == '' and exp_task_text == '':
            return ''
        if exp_name_text == '':
            return exp_task_text
        if exp_task_text == '':
            return exp_name_text
        return f'{exp_name_text}_{exp_task_text}'



