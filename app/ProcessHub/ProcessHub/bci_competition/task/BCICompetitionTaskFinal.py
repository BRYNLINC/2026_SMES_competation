import importlib
import io
import json
import asyncio
import copy
import struct
import uuid
from pathlib import Path
import logging
import os
import sys
import time
import traceback
from collections import deque
from collections import namedtuple
from typing import Union

import numpy as np
import requests
import yaml
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel, AlgorithmReportMessageModel
from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from Common.converter.CommonMessageConverter import CommonMessageConverter
from Common.model.CommonMessageModel import (
    DataMessageModel,
    ControlPackageModel,
    DataPackageModel,
    DevicePackageModel,
    EventPackageModel,
    ExceptionPackageModel,
    InformationPackageModel,
    ResultPackageModel,
    ScorePackageModel,
)
from ProcessHub.algorithm_connector.exception.ProcessHubAlgorithmConnectorException import (
    ProcessHubAlgorithmConnectorClosedException,
)
from ProcessHub.algorithm_connector.interface.AlgorithmConnectorInterface import AlgorithmConnectorInterface
from ProcessHub.bci_competition.api.converter.AlgorithmConnectEventMessageConverter import (
    AlgorithmConnectEventMessageConverter,
)
from ProcessHub.bci_competition.api.message.MessageKeyEnum import MessageKeyEnum
from ProcessHub.bci_competition.api.model.AlgorithmConnectEventModel import (
    AlgorithmConnectClosedEventModel,
    AlgorithmConnectEventModel,
)
from ProcessHub.bci_competition.challenge.interface.ChallengeInterface import ChallengeInterface
from ProcessHub.bci_competition.task.interface.BCICompetitionTaskInterface import BCICompetitionTaskInterface
from ProcessHub.common.enum.ServiceStatusEnum import ServiceStatusEnum
from ProcessHub.orchestrator.model.SourceModel import SourceModel

PROCESS_HUB_APP_ROOT = Path(__file__).resolve().parents[4]
COLLECTOR_APP_ROOT = PROCESS_HUB_APP_ROOT / 'Collector'
if str(COLLECTOR_APP_ROOT) not in sys.path:
    # ProcessHub 运行时 cwd 在 app/ProcessHub。
    # Collector 侧代码在 app/Collector/Collector/...，因此这里需要补 app/Collector，
    # 才能复用 Collector 进程内原本使用的 `from Collector.xxx import ...` 路径。
    sys.path.append(str(COLLECTOR_APP_ROOT))
PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from Collector.receiver.virtual_receiver.api.proto.VirtualReceiverCustomControl_pb2 import (  # noqa: E402
    VirtualReceiverCustomControlMessage as VirtualReceiverCustomControlMessage_pb2,
    CalibrationTrialCountControlMessage as CalibrationTrialCountControlMessage_pb2,
)
from tools.runtime_state_sqlite import TEAM_STATE_KEY_PREFIX, resolve_runtime_state_db_path, write_json_state  # noqa: E402

# 历史残留：旧版 TimeoutTrigger 基于事件包再套一层异步计时器，
# 但当前决赛主流程已经统一切到“platform pending trial timeout”机制：
# 1. trial ready 时注册 pending trial；
# 2. timeout 按 trial_end_position 精确消费；
# 3. 正常结果 / 超时终态都统一汇入 runtime_stage_event。
# 旧逻辑当前既没有主流程入口，也依赖已废弃的 trigger_timeout_notification() 形态，
# 继续保留只会误导维护者，因此明确注释停用。
#
# class TimeoutTrigger:
#     def __init__(self, timeout_trigger_name: str, source_label: str, timeout_limit: float,
#                  timeout_trigger_event_set: set[float],
#                  outer_obj):
#         self.__timeout_trigger_name: str = timeout_trigger_name
#         self.__source_label: str = source_label
#         self.__timeout_limit: float = timeout_limit
#         self.__timeout_trigger_event_set: set[float]() = timeout_trigger_event_set
#         self.__outer_obj: BCICompetitionTaskFinal = outer_obj
#         self.__logger = logging.getLogger("processHubLogger")
#
#     def start_timer(self, algorithm_data_message_model: AlgorithmDataMessageModel) -> None:
#         if not isinstance(algorithm_data_message_model.package, EventPackageModel) or \
#                 algorithm_data_message_model.source_label != self.__source_label:
#             return
#         event_model = algorithm_data_message_model.package
#         matching_event_tuple_list = [(event_model.event_position[index], event_data)
#                                      for index, event_data in enumerate(event_model.event_data)
#                                      if float(event_data) in self.__timeout_trigger_event_set]
#         if matching_event_tuple_list is None or len(matching_event_tuple_list) == 0:
#             return
#         event_model = EventPackageModel()
#         event_model.event_position, event_model.event_data = zip(*matching_event_tuple_list)
#         algorithm_data_message_model_new = AlgorithmDataMessageModel(
#             source_label=algorithm_data_message_model.source_label,
#             timestamp_ms=algorithm_data_message_model.timestamp_ms,
#             package=event_model
#         )
#         if self.__timeout_limit is not None:
#             asyncio.create_task(self.__delay_trigger(algorithm_data_message_model_new))
#             self.__logger.debug(f"启动超时计时器{algorithm_data_message_model_new}")
#
#     async def __delay_trigger(self, algorithm_data_message_model: AlgorithmDataMessageModel):
#         await asyncio.sleep(self.__timeout_limit)
#         await self.__outer_obj.trigger_timeout_notification(algorithm_data_message_model)

class BCICompetitionTaskFinal(BCICompetitionTaskInterface):
    """
    决赛主线 task。

    这里承载多赛队校准协调、online 共享试次同步、
    平台侧 trial 计时和最终评分收尾逻辑。
    """

    __PRIVATE_SCORE_SOURCE_LABEL = 'hidden_score'
    __ALGORITHM_SOURCE_LABEL = 'eeg_1'
    __CALIBRATION_PRIVATE_SOURCE_LABEL = 'eeg_1_calibration_private'
    __ONLINE_SHARED_SOURCE_LABEL = 'eeg_1_online_shared'
    __RUNTIME_STAGE_EVENT_MESSAGE_KEY = 'runtime_stage_event'
    __CALIBRATION_READY_EVENT_TYPE = 'calibration_ready'
    __TERMINAL_RUN_STATUS_SET = {'finished', 'closed', 'error', 'startup_failed', 'stopped'}
    __ALGORITHM_REQUIRED_SOURCE_LABEL_SET = {'eeg_1'}
    __MAX_REQUESTED_CHANNEL_COUNT_PER_SOURCE = 8
    __CALIBRATION_CHUNK_MAGIC = b'CAL1'
    __CALIBRATION_CHUNK_HEADER_FORMAT = '>4sIII'
    __CALIBRATION_CHUNK_HEADER_SIZE = struct.calcsize(__CALIBRATION_CHUNK_HEADER_FORMAT)
    __MAX_CALIBRATION_CHUNK_BYTES = 512 * 1024

    def __init__(self):
        super().__init__()
        self.__default_algorithm_connect_closed_topic: str = None
        self.__default_report_topic: str = None
        self.__TrialMarkTuple = namedtuple("SendTrialModel", ["trial_id", "block_id", "subject_id"])
        self.__send_trial_mark_tuple_set: set = set()
        self.__challenge_class_name: str = None
        self.__challenge_class_file: str = None
        self.__current_challenge: ChallengeInterface = None
        self.__algorithm_error_flag: bool = False
        self.__task_status: ServiceStatusEnum = ServiceStatusEnum.STOPPED
        self.__logger = logging.getLogger("processHubLogger")
        self.__final_score = 0
        self.__final_score_result: dict | None = None
        self.__original_stderr = sys.stderr
        self.__virtual_receiver_custom_control_message_key = "virtual_receiver_custom_control"
        self.__incoming_message_queue = asyncio.Queue[Union[AlgorithmDataMessageModel, None]]()
        self.__incoming_message_consumer_task: Union[asyncio.Task, None] = None
        self.__report_finalize_queue = asyncio.Queue[dict | None]()
        self.__report_finalize_worker_task: asyncio.Task | None = None
        # 这些字段都是显式路由信息。
        # 本轮改造后，ProcessHub 不再通过 topic 名、component_id 或 source_label 去反推 team/group，
        # 而是统一从 component_info 读取并透传给 Collector / RuntimeStageCoordinator。
        self.__team_id: str = None
        self.__group_id: str = None
        self.__processor_component_id: str = None
        self.__collector_component_id: str = None
        self.__collector_custom_control_topic: str = None
        self.__runtime_stage_event_topic: str = None
        self.__team_display_name: str = None
        # requested calibration trial count 既会在 startup 后立即发送一次，
        # 也会在 collector 首次 device_update 到达后补发一次。
        # 原因是正式链路里 ProcessHub 可能先于 Collector 完成启动，
        # 导致第一次消息发出时 Collector 还未完成订阅。
        self.__requested_calibration_trial_count: int | None = None
        self.__collector_calibration_trial_count_resend_done: bool = False

        # 下面这一组状态专门用于“平台侧 trial 计时”。
        # 注意这里的计时起点不是算法内部时间，而是 ProcessHub 看到 trial_end 事件的 wallclock；
        # 终点则是 ProcessHub 收到算法结果的 wallclock。
        # 这样可以避免跨进程 / protobuf 时间戳不一致时影响 timeout 判定。
        self.__trial_window_seconds = 4.0
        self.__current_sample_rate: float = 0.0
        self.__current_subject_id: str = None
        self.__current_block_id: str = None
        self.__current_exp_name: str = None
        self.__current_exp_task: str = None
        self.__current_session_id: str = None
        self.__current_stage_phase: str | None = None
        self.__calibration_forfeit_event_sent_stage_signature_set: set[
            tuple[str, str, str, str]
        ] = set()
        self.__calibration_ready_stage_signature_set: set[tuple[str, str, str, str]] = set()
        self.__trial_counter_dict: dict[tuple[str, str, str, str], int] = {}
        self.__pending_trial_timing_queue = deque()
        self.__pending_trial_timing_by_end_position: dict[int, dict] = {}
        self.__buffered_result_by_end_position: dict[int, dict] = {}
        self.__timed_out_trial_end_position_set: set[int] = set()
        self.__timeout_scheduler_task: asyncio.Task | None = None
        self.__timeout_scheduler_wakeup_event = asyncio.Event()
        self.__timeout_scheduler_revision: int = 0
        self.__hidden_score_payload_dict: dict[tuple[str, str, str, str, str, str], dict] = {}
        self.__timeout_limit_seconds: float = 0.0
        self.__timeout_predict_label: str = "wrong"
        self.__report_stream_closed: bool = False
        self.__preliminary_run_start_wallclock: float | None = None
        self.__preliminary_run_start_monotonic: float | None = None
        self.__preliminary_runtime_logged: bool = False
        self.__team_live_status_payload: dict = {}
        self.__input_stream_finished: bool = False
        self.__algorithm_connection_ready: bool = False
        self.__algorithm_disconnected_for_current_task: bool = False
        self.__disconnected_task_signature: tuple[str, str, str] | None = None
        self.__reconnect_attempt_task_signature: tuple[str, str, str] | None = None
        self.__reconnect_attempt_in_progress: bool = False
        self.__startup_connect_retry_task: Union[asyncio.Task, None] = None
        self.__last_disconnect_wallclock: float | None = None
        self.__run_finalized: bool = False
        self.__terminal_run_event_sent: bool = False
        self.__requested_channel_labels_by_source: dict[str, list[str]] = {}
        self.__forward_channel_index_by_source: dict[str, list[int]] = {}
        self.__forward_channel_labels_by_source: dict[str, list[str]] = {}
        self.__incoming_device_channel_labels_by_source: dict[str, list[str]] = {}
        self.__incoming_device_channel_count_by_source: dict[str, int] = {}
        self.__forward_calibration_chunk_buffer_by_key: dict[tuple[str, str, str, str, str, str], dict] = {}
        self.__current_calibration_delivery_id: str | None = None
        self.__forward_calibration_device_message_by_delivery_id: dict[str, AlgorithmDataMessageModel] = {}
        self.__completed_calibration_delivery_id_set: set[str] = set()
        self.__startup_unconnected_calibration_message_buffer = deque()
        self.__startup_unconnected_calibration_buffer_replay_lock = asyncio.Lock()
        self.__startup_online_message_seen_before_connection: bool = False
        self.__startup_online_missed_task_signature: tuple[str, str, str] | None = None

    async def receive_message(self, algorithm_data_message_model: AlgorithmDataMessageModel):
        self.__logger.debug(f"{algorithm_data_message_model.source_label}收到消息"
                            f"{type(algorithm_data_message_model.package)}")
        if self.__task_status is ServiceStatusEnum.RUNNING:
            receive_wallclock = time.time()
            setattr(algorithm_data_message_model, '_processhub_receive_wallclock', receive_wallclock)
            setattr(algorithm_data_message_model, '_processhub_enqueue_wallclock', receive_wallclock)
            queue_size_before_put = self.__incoming_message_queue.qsize()
            await self.__incoming_message_queue.put(algorithm_data_message_model)
            queue_size_after_put = self.__incoming_message_queue.qsize()
            if self.__should_log_message_flow(
                algorithm_data_message_model,
                queue_size=queue_size_after_put,
            ):
                self.__logger.debug(
                    "平台侧消息入队: summary=%s receive_wallclock=%.6f queue_size_before=%s queue_size_after=%s",
                    self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                    receive_wallclock,
                    queue_size_before_put,
                    queue_size_after_put,
                )

    async def receive_report(self, algorithm_report_message_model: AlgorithmReportMessageModel):
        try:
            self.__logger.debug(f"收到报告{type(algorithm_report_message_model)}")
            if self.__task_status is ServiceStatusEnum.RUNNING:
                if isinstance(algorithm_report_message_model.package, ResultPackageModel):
                    report_receive_wallclock = time.time()
                    raw_result_summary = self.__summarize_result_payload_for_log(
                        algorithm_report_message_model.package.result
                    )
                    report_source_information_summary = self.__summarize_result_report_source_information_for_log(
                        algorithm_report_message_model.package
                    )
                    self.__logger.debug(
                        "收到算法结果: receive_wallclock=%.6f report_timestamp_ms=%s raw_result=%s report_source_information=%s pending_trial_count=%s",
                        report_receive_wallclock,
                        algorithm_report_message_model.timestamp_ms,
                        raw_result_summary,
                        report_source_information_summary,
                        len(self.__pending_trial_timing_queue),
                    )
                    matched_trial_timing = self.__consume_pending_trial_timing_for_result(
                        algorithm_report_message_model.package
                    )
                    if isinstance(matched_trial_timing, dict) and matched_trial_timing.get('timeout_discarded'):
                        self.__logger.warning(
                            "丢弃超时后的晚到算法结果: receive_wallclock=%.6f report_timestamp_ms=%s trial_end_position=%s raw_result=%s report_source_information=%s",
                            report_receive_wallclock,
                            algorithm_report_message_model.timestamp_ms,
                            matched_trial_timing.get('trial_end_position'),
                            raw_result_summary,
                            report_source_information_summary,
                        )
                        return
                    if isinstance(matched_trial_timing, dict) and matched_trial_timing.get('await_trial_ready'):
                        self.__buffer_result_until_trial_ready(
                            trial_end_position=matched_trial_timing.get('trial_end_position'),
                            algorithm_report_message_model=algorithm_report_message_model,
                            report_receive_wallclock=report_receive_wallclock,
                            raw_result_summary=raw_result_summary,
                            report_source_information_summary=report_source_information_summary,
                        )
                        return
                    if matched_trial_timing is not None:
                        runtime_ms = self.__calculate_runtime_ms(
                            matched_trial_timing,
                            report_receive_wallclock,
                        )
                        report_transport_ms = max(
                            0.0,
                            (report_receive_wallclock * 1000.0)
                            - float(algorithm_report_message_model.timestamp_ms or 0),
                        )
                        trial_end_queue_wait_ms = float(
                            matched_trial_timing.get('trial_end_message_queue_wait_ms') or 0.0
                        )
                        trial_ready_processing_ms = float(
                            matched_trial_timing.get('trial_ready_processing_ms') or 0.0
                        )
                        trial_end_message_receive_wallclock = matched_trial_timing.get(
                            'trial_end_message_receive_wallclock'
                        )
                        end_to_report_receive_ms = None
                        if trial_end_message_receive_wallclock is not None:
                            end_to_report_receive_ms = (
                                report_receive_wallclock - float(trial_end_message_receive_wallclock)
                            ) * 1000.0
                        matched_trial_timing['report_receive_wallclock'] = report_receive_wallclock
                        matched_trial_timing['runtime_ms'] = runtime_ms
                        self.__enrich_result_package(
                            algorithm_report_message_model.package,
                            matched_trial_timing,
                        )
                        self.__logger.debug(
                            "trial timing matched: subject_id=%s exp_name=%s exp_task=%s "
                            "session_id=%s block_id=%s trial_id=%s "
                            "trial_end_position=%s trial_start_position=%s runtime_ms=%.3f",
                            matched_trial_timing.get('subject_id'),
                            matched_trial_timing.get('exp_name'),
                            matched_trial_timing.get('exp_task'),
                            matched_trial_timing.get('session_id'),
                            matched_trial_timing.get('block_id'),
                            matched_trial_timing.get('trial_id'),
                            matched_trial_timing.get('trial_end_position'),
                            matched_trial_timing.get('trial_start_position'),
                            runtime_ms,
                        )
                        self.__logger.debug(
                            "算法结果匹配成功: report_timestamp_ms=%s receive_wallclock=%.6f subject_id=%s exp_name=%s exp_task=%s session_id=%s block_id=%s trial_id=%s predict_runtime_ms=%.3f raw_result=%s",
                            algorithm_report_message_model.timestamp_ms,
                            report_receive_wallclock,
                            matched_trial_timing.get('subject_id'),
                            matched_trial_timing.get('exp_name'),
                            matched_trial_timing.get('exp_task'),
                            matched_trial_timing.get('session_id'),
                            matched_trial_timing.get('block_id'),
                            matched_trial_timing.get('trial_id'),
                            runtime_ms,
                            raw_result_summary,
                        )
                        self.__logger.debug(
                            "平台侧trial计时: subject_id=%s block_id=%s "
                            "trial_end_position=%s trial_start_position=%s runtime_ms=%.3f",
                            matched_trial_timing['subject_id'],
                            matched_trial_timing['block_id'],
                            matched_trial_timing['trial_end_position'],
                            matched_trial_timing['trial_start_position'],
                            runtime_ms,
                        )
                        self.__logger.debug(
                            "平台侧算法结果链路拆解: subject_id=%s exp_name=%s exp_task=%s session_id=%s block_id=%s trial_id=%s "
                            "trial_end_queue_wait_ms=%.3f trial_ready_processing_ms=%.3f ready_to_result_ms=%.3f "
                            "report_transport_ms=%.3f end_to_result_ms=%s",
                            matched_trial_timing.get('subject_id'),
                            matched_trial_timing.get('exp_name'),
                            matched_trial_timing.get('exp_task'),
                            matched_trial_timing.get('session_id'),
                            matched_trial_timing.get('block_id'),
                            matched_trial_timing.get('trial_id'),
                            trial_end_queue_wait_ms,
                            trial_ready_processing_ms,
                            runtime_ms,
                            report_transport_ms,
                            f"{end_to_report_receive_ms:.3f}" if end_to_report_receive_ms is not None else "unknown",
                        )
                    else:
                        self.__logger.warning(
                            "收到算法结果但未匹配到待处理trial，已忽略且不进入计分: receive_wallclock=%.6f report_timestamp_ms=%s raw_result=%s report_source_information=%s pending_trial_count=%s",
                            report_receive_wallclock,
                            algorithm_report_message_model.timestamp_ms,
                            raw_result_summary,
                            report_source_information_summary,
                            len(self.__pending_trial_timing_queue),
                        )
                        return

                    if matched_trial_timing is not None:
                        result_payload = self.__parse_json_payload(
                            algorithm_report_message_model.package.result
                        ) or {}
                        await self.__current_challenge.receive_report(algorithm_report_message_model)
                        result_terminal_sent = await self.__emit_trial_terminal_event(
                            terminal_type='result',
                            trial_context=matched_trial_timing,
                        )
                        self.__logger.debug(
                            "result 终态事件发送结果: subject_id=%s exp_name=%s exp_task=%s session_id=%s "
                            "block_id=%s trial_id=%s sent=%s",
                            matched_trial_timing.get('subject_id'),
                            matched_trial_timing.get('exp_name'),
                            matched_trial_timing.get('exp_task'),
                            matched_trial_timing.get('session_id'),
                            matched_trial_timing.get('block_id'),
                            matched_trial_timing.get('trial_id'),
                            result_terminal_sent,
                        )
                    else:
                        result_payload = None
                        await self.__current_challenge.receive_report(algorithm_report_message_model)
                    await self.__enqueue_report_finalization(
                        {
                            'finalize_type': 'result',
                            'algorithm_report_message_model': algorithm_report_message_model,
                            'matched_trial_timing': matched_trial_timing,
                            'report_receive_wallclock': report_receive_wallclock,
                            'result_payload': result_payload,
                            'log_context': 'algorithm result package',
                        }
                    )
                    await self.__maybe_finalize_disconnected_run_after_stream_end()
                elif isinstance(algorithm_report_message_model.package, DataPackageModel):
                    await self.__handle_algorithm_runtime_event(algorithm_report_message_model)
                elif isinstance(algorithm_report_message_model.package, ExceptionPackageModel):
                    self.__logger.error(f"算法报告,算法异常:\n{algorithm_report_message_model.package}")
                    self.__original_stderr.write(
                        "[ERROR]" + f"算法报告,算法异常:{algorithm_report_message_model.package}"
                    )
                    self.__algorithm_error_flag = True
                    self.__publish_team_live_status(
                        run_status='error',
                        connection_status='error',
                        last_error_message=str(algorithm_report_message_model.package.exception_message),
                        last_error_type=str(algorithm_report_message_model.package.exception_type),
                    )
                    if self.__should_emit_calibration_unavailable_event():
                        await self.__emit_team_calibration_forfeited_event(
                            disconnect_reason=(
                                'algorithm_exception_during_calibration: '
                                f'{algorithm_report_message_model.package.exception_type}: '
                                f'{algorithm_report_message_model.package.exception_message}'
                            ),
                            algorithm_address=self._algorithm_connector.get_algorithm_address(),
                        )
                    await self.__send_report_package(
                        algorithm_report_message_model.package,
                        log_context="algorithm exception package",
                    )
        except Exception:
            err_str = traceback.format_exc()
            self.__original_stderr.write("[ERROR]" + err_str)
            raise Exception("error exit 1")

    async def receive_algorithm_connector_closed_event(
        self,
        algorithm_connector: AlgorithmConnectorInterface,
        disconnect_reason: str = 'unknown',
    ):
        algorithm_connector_address = algorithm_connector.get_algorithm_address()
        self.__logger.debug(
            f"收到算法关闭事件:{algorithm_connector_address}, reason={disconnect_reason}"
        )
        is_normal_finish = (
            self.__task_status is ServiceStatusEnum.RUNNING
            and self.__algorithm_error_flag is False
            and self.__input_stream_finished
            and len(self.__pending_trial_timing_queue) == 0
        )
        if is_normal_finish:
            self.__logger.info(
                "算法正常完赛结束，开始最终封板: address=%s algorithm_error_flag=%s",
                algorithm_connector_address,
                self.__algorithm_error_flag,
            )
            await self.__finalize_run(
                algorithm_address=algorithm_connector_address,
                connection_status='closed',
                finish_reason=f"algorithm completed address={algorithm_connector_address}",
                success_stderr_flag=True,
            )
            return

        if self.__task_status is ServiceStatusEnum.RUNNING and self.__algorithm_error_flag is False:
            await self.__mark_algorithm_disconnected(
                disconnect_reason=(
                    f'algorithm_data_connection_closed_before_task_finished: {disconnect_reason}'
                ),
                algorithm_address=algorithm_connector_address,
            )
            return

        if self.__task_status is ServiceStatusEnum.RUNNING and self.__should_emit_calibration_unavailable_event():
            await self.__emit_team_calibration_forfeited_event(
                disconnect_reason=f'algorithm_error_connection_closed: {disconnect_reason}',
                algorithm_address=algorithm_connector_address,
            )

        self.__publish_team_live_status(
            run_status='error' if self.__algorithm_error_flag else 'closed',
            connection_status='closed',
            algorithm_address=algorithm_connector_address,
        )
        await self.__emit_team_run_finalized_event(
            terminal_run_status='error' if self.__algorithm_error_flag else 'closed'
        )

    async def get_source_list(self) -> list[SourceModel]:
        return await self.__current_challenge.get_source_list()

    async def initial(self):
        self.__task_status = ServiceStatusEnum.INITIALIZING
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'BCICompetitionTaskFinalConfig.yml')
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict: dict = yaml.safe_load(f)
        challenge_dict = config_dict.get('challenge', dict())
        self.__challenge_class_file = challenge_dict.get('challenge_class_file', "")
        self.__challenge_class_name = challenge_dict.get('challenge_class_name', "")

        message_key_topic_dict = config_dict.get('message_key_topic_dict', dict())
        self.__default_report_topic = message_key_topic_dict.get(MessageKeyEnum.REPORT.value, None)
        self.__default_algorithm_connect_closed_topic = message_key_topic_dict.get(
            MessageKeyEnum.ALGORITHMCLOSED.value, None
        )

        await self._component_framework.bind_message(
            MessageBindingModel(message_key=MessageKeyEnum.REPORT.value, topic=self.__default_report_topic)
        )
        await self._component_framework.bind_message(
            MessageBindingModel(
                message_key=MessageKeyEnum.ALGORITHMCLOSED.value,
                topic=self.__default_algorithm_connect_closed_topic,
            )
        )

        self.__current_challenge = self.__load_challenge(self.__challenge_class_file, self.__challenge_class_name)
        self.__current_challenge.set_component_framework(self._component_framework)
        await self.__current_challenge.initial()

        self.__task_status = ServiceStatusEnum.READY

    async def startup(self):
        try:
            self.__task_status = ServiceStatusEnum.STARTING
            self.__preliminary_run_start_wallclock = time.time()
            self.__preliminary_run_start_monotonic = time.perf_counter()
            self.__preliminary_runtime_logged = False
            self.__trial_counter_dict.clear()
            self.__pending_trial_timing_queue.clear()
            self.__pending_trial_timing_by_end_position.clear()
            self.__buffered_result_by_end_position.clear()
            self.__timed_out_trial_end_position_set.clear()
            self.__hidden_score_payload_dict.clear()
            self.__current_sample_rate = 0.0
            self.__current_subject_id = None
            self.__current_block_id = None
            self.__current_exp_name = None
            self.__current_exp_task = None
            self.__current_session_id = None
            self.__current_stage_phase = None
            self.__calibration_forfeit_event_sent_stage_signature_set.clear()
            self.__calibration_ready_stage_signature_set.clear()
            self.__final_score_result = None
            self.__algorithm_error_flag = False
            self.__requested_calibration_trial_count = None
            self.__timeout_limit_seconds = 0.0
            self.__timeout_predict_label = "wrong"
            self.__report_stream_closed = False
            self.__collector_calibration_trial_count_resend_done = False
            self.__input_stream_finished = False
            self.__algorithm_connection_ready = False
            self.__algorithm_disconnected_for_current_task = False
            self.__disconnected_task_signature = None
            self.__reconnect_attempt_task_signature = None
            self.__reconnect_attempt_in_progress = False
            self.__startup_connect_retry_task = None
            self.__last_disconnect_wallclock = None
            self.__run_finalized = False
            self.__terminal_run_event_sent = False
            self.__requested_channel_labels_by_source.clear()
            self.__forward_channel_index_by_source.clear()
            self.__forward_channel_labels_by_source.clear()
            self.__incoming_device_channel_labels_by_source.clear()
            self.__incoming_device_channel_count_by_source.clear()
            self.__forward_calibration_chunk_buffer_by_key.clear()
            self.__startup_unconnected_calibration_message_buffer.clear()
            self.__startup_online_message_seen_before_connection = False
            self.__startup_online_missed_task_signature = None

            await self.__current_challenge.startup()
            await self.__load_timeout_config()
            await self.__load_component_runtime_routing()
            self.__team_live_status_payload = {}
            self.__publish_team_live_status(
                run_status='starting',
                connection_status='connecting',
                calibration_status='pending',
                current_total_score=0.0,
                current_trial_score=0.0,
                observed_trial_count=0,
                predict_label=None,
                true_label=None,
                predict_time_ms=None,
                is_timeout=None,
                is_invalid_output=False,
                judge_message=None,
                final_total_score=None,
                final_score_result=None,
                last_error_message=None,
                last_error_type=None,
                last_disconnect_at=None,
                last_disconnect_reason=None,
                recovery_advice=None,
                forfeit_current_task=False,
                forfeit_task_signature=None,
                reconnected_at=None,
            )
            self.__ensure_incoming_message_consumer_started()
            self.__ensure_report_finalize_worker_started()
            self.__ensure_timeout_scheduler_started()
            self.__task_status = ServiceStatusEnum.RUNNING
            await self.__refresh_team_live_score_fields()
            self.__publish_team_live_status(
                run_status='running',
                connection_status='connecting',
                recovery_advice='等待选手机启动并自动接入',
            )
            self.__startup_connect_retry_task = asyncio.create_task(
                self.__run_startup_connection_retry_loop()
            )
            self.__logger.info(
                "决赛总流程计时开始: start_wallclock=%.6f",
                self.__preliminary_run_start_wallclock,
            )
        except Exception:
            self.__publish_team_live_status(
                run_status='startup_failed',
                connection_status='error',
            )
            await self.__emit_team_run_finalized_event(terminal_run_status='startup_failed')
            err_str = traceback.format_exc()
            self.__original_stderr.write("[ERROR]" + err_str)
            raise Exception("error exit 1")

    async def shutdown(self):
        self.__task_status = ServiceStatusEnum.STOPPING
        self.__algorithm_connection_ready = False
        self.__publish_team_live_status(
            run_status='stopping',
            connection_status='disconnecting',
        )
        if self.__startup_connect_retry_task is not None:
            self.__startup_connect_retry_task.cancel()
            try:
                await self.__startup_connect_retry_task
            except asyncio.CancelledError:
                pass
            self.__startup_connect_retry_task = None
        if self.__incoming_message_consumer_task is not None:
            await self.__incoming_message_queue.put(None)
            await self.__incoming_message_consumer_task
            self.__incoming_message_consumer_task = None
        if self.__report_finalize_worker_task is not None:
            await self.__report_finalize_queue.put(None)
            await self.__report_finalize_worker_task
            self.__report_finalize_worker_task = None
        if self.__timeout_scheduler_task is not None:
            self.__timeout_scheduler_task.cancel()
            try:
                await self.__timeout_scheduler_task
            except asyncio.CancelledError:
                pass
            self.__timeout_scheduler_task = None
        self.__cancel_all_pending_timeout_tasks()
        self.__log_preliminary_total_runtime("task shutdown")
        await self.__current_challenge.shutdown()
        self.__current_challenge = None
        await self._algorithm_connector.data_disconnect()
        await self._algorithm_connector.shutdown_and_close_algorithm_system()
        self.__task_status = ServiceStatusEnum.STOPPED
        self.__publish_team_live_status(
            run_status='stopped',
            connection_status='stopped',
        )
        await self.__emit_team_run_finalized_event(terminal_run_status='stopped')

    def __ensure_incoming_message_consumer_started(self) -> None:
        if self.__incoming_message_consumer_task is not None and not self.__incoming_message_consumer_task.done():
            return
        self.__incoming_message_consumer_task = asyncio.create_task(self.__consume_incoming_message_queue())

    def __ensure_report_finalize_worker_started(self) -> None:
        if self.__report_finalize_worker_task is not None and not self.__report_finalize_worker_task.done():
            return
        self.__report_finalize_worker_task = asyncio.create_task(self.__run_report_finalize_worker())

    def __ensure_timeout_scheduler_started(self) -> None:
        if self.__timeout_scheduler_task is not None and not self.__timeout_scheduler_task.done():
            return
        self.__timeout_scheduler_task = asyncio.create_task(self.__run_timeout_scheduler())

    def __notify_timeout_scheduler(self) -> None:
        self.__timeout_scheduler_revision += 1
        self.__timeout_scheduler_wakeup_event.set()

    async def __run_startup_connection_retry_loop(self) -> None:
        retry_interval_seconds = 2.0
        algorithm_address = self._algorithm_connector.get_algorithm_address()
        while (
            self.__task_status is ServiceStatusEnum.RUNNING
            and not self.__algorithm_connection_ready
            and not self.__run_finalized
        ):
            try:
                # 赛前断线后，旧 connector 可能仍保留 READY/RUNNING 状态但数据流已失效。
                # 每轮重试前先主动收口旧 RPC 生命周期，确保后续 startup/data_connect 走完整重建路径。
                try:
                    await self._algorithm_connector.shutdown()
                except Exception:
                    self.__logger.exception(
                        "赛前自动重连前关闭旧算法连接失败，继续尝试重建: team_id=%s address=%s",
                        self.__team_id,
                        algorithm_address,
                    )
                await self.__establish_algorithm_runtime_session(allow_current_task_join=False)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.__logger.warning(
                    "算法端尚未接入，保持等待并稍后重试: team_id=%s address=%s error=%s",
                    self.__team_id,
                    algorithm_address,
                    f"{type(exc).__name__}: {exc}",
                )
                self.__publish_team_live_status(
                    run_status='running',
                    connection_status='connecting',
                    recovery_advice='等待选手机启动并自动接入',
                )
                await asyncio.sleep(retry_interval_seconds)

    async def __establish_algorithm_runtime_session(
        self,
        *,
        allow_current_task_join: bool,
    ) -> None:
        await self._algorithm_connector.startup()
        await self._algorithm_connector.data_connect()
        raw_algorithm_config = await self._algorithm_connector.pull_algorithm_config()
        algorithm_config = self.__sanitize_algorithm_config(raw_algorithm_config)
        self.__requested_channel_labels_by_source = copy.deepcopy(
            algorithm_config.get('requested_channel_labels') or {}
        )
        self.__forward_channel_index_by_source.clear()
        self.__forward_channel_labels_by_source.clear()
        self.__incoming_device_channel_labels_by_source.clear()
        self.__incoming_device_channel_count_by_source.clear()
        self.__forward_calibration_chunk_buffer_by_key.clear()
        if self.__team_id is not None and str(self.__team_id).strip() != '':
            algorithm_config = dict(algorithm_config or {})
            algorithm_config['platform_team_id'] = self.__team_id
        await self.__current_challenge.receive_algorithm_config(algorithm_config)
        requested_calibration_trial_count = self.__validate_requested_calibration_trial_count(algorithm_config)
        self.__requested_calibration_trial_count = requested_calibration_trial_count
        await self.__send_requested_calibration_trial_count_to_collector(
            requested_calibration_trial_count,
            allow_startup_failure=True,
        )
        to_algorithm_config = await self.__current_challenge.get_to_algorithm_config()
        await self._algorithm_connector.push_algorithm_config(to_algorithm_config)
        if not self._algorithm_connector.is_transport_active():
            self.__algorithm_connection_ready = False
            raise ProcessHubAlgorithmConnectorClosedException(
                'algorithm data stream closed during runtime-session handshake: '
                f'address={self._algorithm_connector.get_algorithm_address()}'
            )

        if allow_current_task_join:
            self.__algorithm_connection_ready = True
            self.__algorithm_disconnected_for_current_task = False
            self.__disconnected_task_signature = None
            self.__startup_online_message_seen_before_connection = False
            self.__startup_online_missed_task_signature = None
            await self.__refresh_team_live_score_fields()
            self.__publish_team_live_status(
                run_status='running',
                connection_status='connected',
                recovery_advice=None,
                forfeit_current_task=False,
                forfeit_task_signature=None,
                reconnected_at=time.time(),
            )
            self.__logger.info(
                "绠楁硶鍦ㄦ柊 task 杈圭晫宸查噸寤鸿繛鎺ュ苟绾冲叆姣旇禌锛屼笉鍐嶅鐢ㄥ惎鍔ㄦ湡鏍″噯缂撳瓨鍥炴斁: "
                "team_id=%s address=%s task_signature=%s",
                self.__team_id,
                self._algorithm_connector.get_algorithm_address(),
                self.__build_task_signature(),
            )
            return

        startup_online_seen_before_connection = self.__startup_online_message_seen_before_connection
        if startup_online_seen_before_connection and not allow_current_task_join:
            dropped_calibration_message_count = len(self.__startup_unconnected_calibration_message_buffer)
            self.__startup_unconnected_calibration_message_buffer.clear()
            if dropped_calibration_message_count > 0:
                self.__logger.info(
                    "算法接入前当前 task 已进入 online，丢弃启动期校准缓存并保持当前 task 按 timeout 处理: "
                    "team_id=%s dropped_calibration_message_count=%s",
                    self.__team_id,
                    dropped_calibration_message_count,
                )
        had_startup_calibration_buffer = (
            len(self.__startup_unconnected_calibration_message_buffer) > 0
            and not startup_online_seen_before_connection
        )
        if had_startup_calibration_buffer and not allow_current_task_join:
            self.__algorithm_disconnected_for_current_task = False
            self.__disconnected_task_signature = None
        replayed_message_count = await self.__replay_startup_unconnected_calibration_message_buffer()
        if self.__algorithm_disconnected_for_current_task:
            self.__algorithm_connection_ready = False
            raise RuntimeError('algorithm disconnected while replaying startup buffered messages')
        if not self._algorithm_connector.is_transport_active():
            self.__algorithm_connection_ready = False
            raise ProcessHubAlgorithmConnectorClosedException(
                'algorithm data stream closed after startup calibration replay: '
                f'address={self._algorithm_connector.get_algorithm_address()}'
            )
        self.__algorithm_connection_ready = True

        current_task_signature = self.__build_task_signature()
        if current_task_signature is None and startup_online_seen_before_connection:
            current_task_signature = self.__startup_online_missed_task_signature
        if not allow_current_task_join and current_task_signature is not None:
            if had_startup_calibration_buffer:
                self.__algorithm_disconnected_for_current_task = False
                self.__disconnected_task_signature = None
                await self.__refresh_team_live_score_fields()
                self.__publish_team_live_status(
                    run_status='running',
                    connection_status='connected',
                    recovery_advice=None,
                    forfeit_current_task=False,
                    forfeit_task_signature=None,
                    reconnected_at=time.time(),
                )
                self.__logger.info(
                    "算法在平台启动后接入，已回放启动期校准缓存并纳入当前 task: "
                    "team_id=%s address=%s task_signature=%s replayed_message_count=%s",
                    self.__team_id,
                    self._algorithm_connector.get_algorithm_address(),
                    current_task_signature,
                    replayed_message_count,
                )
                return
            if not startup_online_seen_before_connection:
                self.__algorithm_disconnected_for_current_task = False
                self.__disconnected_task_signature = None
                self.__startup_online_missed_task_signature = None
                await self.__refresh_team_live_score_fields()
                self.__publish_team_live_status(
                    run_status='running',
                    connection_status='connected',
                    recovery_advice=None,
                    forfeit_current_task=False,
                    forfeit_task_signature=None,
                    reconnected_at=time.time(),
                )
                self.__logger.info(
                    "算法在平台启动后接入，当前仅见到阶段元信息且未错过 online 数据，纳入当前 task: "
                    "team_id=%s address=%s task_signature=%s",
                    self.__team_id,
                    self._algorithm_connector.get_algorithm_address(),
                    current_task_signature,
                )
                return
            self.__algorithm_disconnected_for_current_task = True
            self.__disconnected_task_signature = current_task_signature
            self.__publish_team_live_status(
                run_status='running',
                connection_status='connected',
                recovery_advice='算法已接入；当前 task 继续按 timeout 处理，下一个 task 自动纳入比赛',
                forfeit_current_task=True,
                forfeit_task_signature=self.__serialize_task_signature(current_task_signature),
                reconnected_at=time.time(),
            )
            self.__logger.info(
                "算法在当前 task 已启动后才接入，当前 task 保持 timeout 处理，待下一个 task 再纳入比赛: "
                "team_id=%s address=%s task_signature=%s",
                self.__team_id,
                self._algorithm_connector.get_algorithm_address(),
                current_task_signature,
            )
            return

        self.__algorithm_disconnected_for_current_task = False
        self.__disconnected_task_signature = None
        self.__startup_online_message_seen_before_connection = False
        self.__startup_online_missed_task_signature = None
        await self.__refresh_team_live_score_fields()
        self.__publish_team_live_status(
            run_status='running',
            connection_status='connected',
            recovery_advice=None,
            forfeit_current_task=False,
            forfeit_task_signature=None,
            reconnected_at=time.time(),
        )
        self.__logger.info(
            "算法连接建立完成并已纳入当前比赛: team_id=%s address=%s",
            self.__team_id,
            self._algorithm_connector.get_algorithm_address(),
        )

    async def __consume_incoming_message_queue(self) -> None:
        # HEAD 里的隐藏分数缓存和 trial timing 逻辑
        while True:
            algorithm_data_message_model = await self.__incoming_message_queue.get()
            try:
                if algorithm_data_message_model is None:
                    return
                message_consume_start_wallclock = time.time()
                setattr(
                    algorithm_data_message_model,
                    '_processhub_consume_start_wallclock',
                    message_consume_start_wallclock,
                )
                queue_wait_ms = self.__calculate_incoming_message_queue_wait_ms(
                    algorithm_data_message_model,
                    consume_wallclock=message_consume_start_wallclock,
                )
                should_log_message_flow = self.__should_log_message_flow(
                    algorithm_data_message_model,
                    queue_size=self.__incoming_message_queue.qsize(),
                )
                if should_log_message_flow:
                    self.__logger.debug(
                        "平台侧消息开始处理: summary=%s queue_wait_ms=%.3f queue_size_after_get=%s",
                        self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                        queue_wait_ms,
                        self.__incoming_message_queue.qsize(),
                    )

                if self.__is_private_score_source(algorithm_data_message_model.source_label):
                    self.__cache_hidden_score_message(algorithm_data_message_model)
                    continue

                if self.__should_drop_completed_calibration_delivery_message(
                    algorithm_data_message_model
                ):
                    continue

                if (
                    not self.__algorithm_connection_ready
                    and not self.__algorithm_disconnected_for_current_task
                ):
                    if self.__should_buffer_startup_calibration_message(algorithm_data_message_model):
                        # Device metadata is also the authoritative stage context.
                        # Record it before buffering so a later disconnect can be
                        # attributed to the correct subject/session.
                        await self.__update_trial_timing_context(algorithm_data_message_model)
                        self.__buffer_startup_unconnected_calibration_message(algorithm_data_message_model)
                        continue
                    self.__mark_startup_online_message_seen_if_needed(algorithm_data_message_model)

                await self.__maybe_resend_requested_calibration_trial_count_on_device_update(
                    algorithm_data_message_model
                )
                if isinstance(algorithm_data_message_model.package, ControlPackageModel) and bool(algorithm_data_message_model.package.end_flag):
                    self.__input_stream_finished = True
                    self.__logger.info(
                        "检测到 Collector 输入流 end_flag: team_id=%s subject_id=%s exp_name=%s exp_task=%s session_id=%s pending_trial_count=%s",
                        self.__team_id,
                        self.__current_subject_id,
                        self.__current_exp_name,
                        self.__current_exp_task,
                        self.__current_session_id,
                        len(self.__pending_trial_timing_queue),
                    )
                await self.__update_trial_timing_context(algorithm_data_message_model)
                await self.__maybe_handle_task_boundary_for_reconnect()
                if not self.__algorithm_connection_ready:
                    self.__logger.debug(
                        "算法尚未接入，当前仅保留平台侧计时与状态推进，不向算法转发: source_label=%s "
                        "subject_id=%s exp_name=%s exp_task=%s session_id=%s",
                        algorithm_data_message_model.source_label,
                        self.__current_subject_id,
                        self.__current_exp_name,
                        self.__current_exp_task,
                        self.__current_session_id,
                    )
                    continue
                challenge_receive_start_wallclock = time.time()
                try:
                    preprocessed_message_model = await self.__current_challenge.receive_message(
                        algorithm_data_message_model
                    )
                    challenge_receive_complete_wallclock = time.time()
                    if preprocessed_message_model is None:
                        if should_log_message_flow:
                            self.__logger.debug(
                                "平台侧消息未转发给算法: summary=%s queue_wait_ms=%.3f challenge_preprocess_ms=%.3f total_consume_ms=%.3f reason=challenge_filtered",
                                self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                                queue_wait_ms,
                                (challenge_receive_complete_wallclock - challenge_receive_start_wallclock) * 1000.0,
                                (challenge_receive_complete_wallclock - message_consume_start_wallclock) * 1000.0,
                            )
                        continue

                    preprocessed_message_model = self.__normalize_algorithm_source_label(preprocessed_message_model)
                    forwarded_message_model_list = await self.__prepare_messages_for_algorithm_forward(
                        original_message_model=algorithm_data_message_model,
                        forwarded_message_model=preprocessed_message_model,
                    )
                except Exception as exc:
                    self.__logger.exception(
                        "算法消息预处理失败，当前 stage 标记为不可用: team_id=%s source_label=%s stage_phase=%s",
                        self.__team_id,
                        algorithm_data_message_model.source_label,
                        self.__current_stage_phase,
                    )
                    await self.__mark_algorithm_disconnected(
                        disconnect_reason=(
                            f'algorithm_message_preprocess_failed: {type(exc).__name__}: {exc}'
                        ),
                        algorithm_address=self._algorithm_connector.get_algorithm_address(),
                    )
                    continue
                if self.__algorithm_disconnected_for_current_task:
                    self.__logger.debug(
                        "算法已掉线，跳过向算法转发并等待平台侧 timeout: source_label=%s subject_id=%s exp_name=%s exp_task=%s session_id=%s",
                        algorithm_data_message_model.source_label,
                        self.__current_subject_id,
                        self.__current_exp_name,
                        self.__current_exp_task,
                        self.__current_session_id,
                    )
                    continue
                if len(forwarded_message_model_list) == 0:
                    if should_log_message_flow:
                        self.__logger.debug(
                            "平台侧消息未转发给算法: summary=%s queue_wait_ms=%.3f challenge_preprocess_ms=%.3f total_consume_ms=%.3f reason=forward_filter_buffering",
                            self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                            queue_wait_ms,
                            (challenge_receive_complete_wallclock - challenge_receive_start_wallclock) * 1000.0,
                            (time.time() - message_consume_start_wallclock) * 1000.0,
                        )
                    continue
                try:
                    send_to_algorithm_start_wallclock = time.time()
                    for forwarded_message_model in forwarded_message_model_list:
                        if (
                            algorithm_data_message_model.source_label == self.__CALIBRATION_PRIVATE_SOURCE_LABEL
                            and isinstance(forwarded_message_model.package, DataPackageModel)
                            and isinstance(forwarded_message_model.package.data, (bytes, bytearray))
                        ):
                            self.__logger.debug(
                                "转发 calibration chunk 到算法: source_label=%s bytes=%s exp_name=%s exp_task=%s session_id=%s",
                                algorithm_data_message_model.source_label,
                                len(forwarded_message_model.package.data),
                                self.__current_exp_name,
                                self.__current_exp_task,
                                self.__current_session_id,
                            )
                        await self._algorithm_connector.send_data(forwarded_message_model)
                        completed_delivery_id = getattr(
                            forwarded_message_model,
                            '_calibration_delivery_id_to_complete',
                            None,
                        )
                        if completed_delivery_id is not None:
                            self.__mark_calibration_delivery_forwarded(completed_delivery_id)
                        if (
                            algorithm_data_message_model.source_label == self.__CALIBRATION_PRIVATE_SOURCE_LABEL
                            and isinstance(forwarded_message_model.package, DataPackageModel)
                            and isinstance(forwarded_message_model.package.data, (bytes, bytearray))
                        ):
                            # 校准分块是大包，显式让出一次事件循环，
                            # 避免在阶段切换时把大量 chunk 一次性堆进发送链路。
                            await asyncio.sleep(0)
                    send_to_algorithm_complete_wallclock = time.time()
                except ProcessHubAlgorithmConnectorClosedException as exc:
                    await self.__mark_algorithm_disconnected(
                        disconnect_reason=f'algorithm_connector_closed_during_send: {exc}',
                        algorithm_address=self._algorithm_connector.get_algorithm_address(),
                    )
                    continue
                except Exception as exc:
                    await self.__mark_algorithm_disconnected(
                        disconnect_reason=f'algorithm_send_failed: {type(exc).__name__}: {exc}',
                        algorithm_address=self._algorithm_connector.get_algorithm_address(),
                    )
                    continue
                self.__logger.debug(
                    "%s转发消息%s",
                    algorithm_data_message_model.source_label,
                    type(forwarded_message_model_list[-1].package),
                )
                if should_log_message_flow:
                    self.__logger.debug(
                        "平台侧消息转发完成: summary=%s queue_wait_ms=%.3f challenge_preprocess_ms=%.3f send_to_algorithm_ms=%.3f total_consume_ms=%.3f forwarded_count=%s forwarded_summary=%s",
                        self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                        queue_wait_ms,
                        (challenge_receive_complete_wallclock - challenge_receive_start_wallclock) * 1000.0,
                        (send_to_algorithm_complete_wallclock - send_to_algorithm_start_wallclock) * 1000.0,
                        (send_to_algorithm_complete_wallclock - message_consume_start_wallclock) * 1000.0,
                        len(forwarded_message_model_list),
                        self.__summarize_incoming_message_for_log(forwarded_message_model_list[-1]),
                    )
            finally:
                await self.__maybe_finalize_disconnected_run_after_stream_end()
                await self.__maybe_finalize_unconnected_run_after_stream_end()
                self.__incoming_message_queue.task_done()

    def __should_buffer_startup_calibration_message(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> bool:
        if algorithm_data_message_model.source_label != self.__CALIBRATION_PRIVATE_SOURCE_LABEL:
            return False
        return isinstance(
            algorithm_data_message_model.package,
            (DevicePackageModel, DataPackageModel, InformationPackageModel),
        )

    def __mark_startup_online_message_seen_if_needed(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> None:
        if algorithm_data_message_model.source_label != self.__ONLINE_SHARED_SOURCE_LABEL:
            return
        if not isinstance(
            algorithm_data_message_model.package,
            (DataPackageModel, EventPackageModel, ControlPackageModel),
        ):
            return
        if self.__startup_online_message_seen_before_connection:
            return
        self.__startup_online_message_seen_before_connection = True
        self.__startup_online_missed_task_signature = (
            self.__build_task_signature()
            or self.__extract_task_signature_from_message(algorithm_data_message_model)
        )
        dropped_calibration_message_count = len(self.__startup_unconnected_calibration_message_buffer)
        self.__startup_unconnected_calibration_message_buffer.clear()
        self.__logger.info(
            "算法接入前当前 task 已进入 online，后续不再回放启动期校准缓存: "
            "team_id=%s source_label=%s package_type=%s dropped_calibration_message_count=%s",
            self.__team_id,
            algorithm_data_message_model.source_label,
            type(algorithm_data_message_model.package).__name__,
            dropped_calibration_message_count,
        )

    def __buffer_startup_unconnected_calibration_message(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> None:
        self.__maybe_reset_startup_online_marker_for_new_calibration(algorithm_data_message_model)
        self.__startup_unconnected_calibration_message_buffer.append(algorithm_data_message_model)
        buffer_size = len(self.__startup_unconnected_calibration_message_buffer)
        if buffer_size <= 5 or buffer_size % 200 == 0:
            self.__logger.info(
                "算法尚未接入，缓存启动期校准消息等待回放: team_id=%s source_label=%s "
                "package_type=%s buffered_count=%s",
                self.__team_id,
                algorithm_data_message_model.source_label,
                type(algorithm_data_message_model.package).__name__,
                buffer_size,
            )

    def __maybe_reset_startup_online_marker_for_new_calibration(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> None:
        if not self.__startup_online_message_seen_before_connection:
            return
        if algorithm_data_message_model.source_label != self.__CALIBRATION_PRIVATE_SOURCE_LABEL:
            return
        if not isinstance(algorithm_data_message_model.package, DevicePackageModel):
            return
        next_task_signature = self.__extract_task_signature_from_message(algorithm_data_message_model)
        missed_task_signature = self.__startup_online_missed_task_signature
        if missed_task_signature is not None and next_task_signature == missed_task_signature:
            return
        self.__startup_online_message_seen_before_connection = False
        self.__startup_online_missed_task_signature = None
        self.__logger.info(
            "检测到新 task 校准阶段，恢复启动期校准缓存等待算法接入: "
            "team_id=%s next_task_signature=%s previous_missed_task_signature=%s",
            self.__team_id,
            next_task_signature,
            missed_task_signature,
        )

    async def __replay_startup_unconnected_calibration_message_buffer(self) -> int:
        async with self.__startup_unconnected_calibration_buffer_replay_lock:
            replayed_message_count = 0
            while len(self.__startup_unconnected_calibration_message_buffer) > 0:
                algorithm_data_message_model = self.__startup_unconnected_calibration_message_buffer.popleft()
                message_consume_start_wallclock = time.time()
                setattr(
                    algorithm_data_message_model,
                    '_processhub_consume_start_wallclock',
                    message_consume_start_wallclock,
                )
                queue_wait_ms = self.__calculate_incoming_message_queue_wait_ms(
                    algorithm_data_message_model,
                    consume_wallclock=message_consume_start_wallclock,
                )
                try:
                    await self.__maybe_resend_requested_calibration_trial_count_on_device_update(
                        algorithm_data_message_model
                    )
                    if (
                        isinstance(algorithm_data_message_model.package, ControlPackageModel)
                        and bool(algorithm_data_message_model.package.end_flag)
                    ):
                        self.__input_stream_finished = True
                    await self.__update_trial_timing_context(algorithm_data_message_model)
                    await self.__maybe_handle_task_boundary_for_reconnect()
                    forwarded = await self.__forward_message_to_algorithm(
                        algorithm_data_message_model=algorithm_data_message_model,
                        message_consume_start_wallclock=message_consume_start_wallclock,
                        queue_wait_ms=queue_wait_ms,
                        should_log_message_flow=self.__should_log_message_flow(
                            algorithm_data_message_model,
                            queue_size=len(self.__startup_unconnected_calibration_message_buffer),
                        ),
                    )
                except Exception:
                    self.__startup_unconnected_calibration_message_buffer.appendleft(algorithm_data_message_model)
                    raise
                if not forwarded and self.__algorithm_disconnected_for_current_task:
                    self.__startup_unconnected_calibration_message_buffer.appendleft(algorithm_data_message_model)
                    break
                replayed_message_count += 1
                await asyncio.sleep(0)
            if replayed_message_count > 0:
                self.__logger.info(
                    "启动期校准消息已回放到算法: team_id=%s replayed_message_count=%s remaining_buffered_count=%s",
                    self.__team_id,
                    replayed_message_count,
                    len(self.__startup_unconnected_calibration_message_buffer),
                )
            return replayed_message_count

    async def __forward_message_to_algorithm(
        self,
        *,
        algorithm_data_message_model: AlgorithmDataMessageModel,
        message_consume_start_wallclock: float,
        queue_wait_ms: float,
        should_log_message_flow: bool,
    ) -> bool:
        challenge_receive_start_wallclock = time.time()
        preprocessed_message_model = await self.__current_challenge.receive_message(
            algorithm_data_message_model
        )
        challenge_receive_complete_wallclock = time.time()
        if preprocessed_message_model is None:
            if should_log_message_flow:
                self.__logger.debug(
                    "平台侧消息未转发给算法: summary=%s queue_wait_ms=%.3f "
                    "challenge_preprocess_ms=%.3f total_consume_ms=%.3f reason=challenge_filtered",
                    self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                    queue_wait_ms,
                    (challenge_receive_complete_wallclock - challenge_receive_start_wallclock) * 1000.0,
                    (challenge_receive_complete_wallclock - message_consume_start_wallclock) * 1000.0,
                )
            return False

        preprocessed_message_model = self.__normalize_algorithm_source_label(preprocessed_message_model)
        forwarded_message_model_list = await self.__prepare_messages_for_algorithm_forward(
            original_message_model=algorithm_data_message_model,
            forwarded_message_model=preprocessed_message_model,
        )
        if self.__algorithm_disconnected_for_current_task:
            self.__logger.debug(
                "算法已掉线，跳过向算法转发并等待平台侧 timeout: source_label=%s "
                "subject_id=%s exp_name=%s exp_task=%s session_id=%s",
                algorithm_data_message_model.source_label,
                self.__current_subject_id,
                self.__current_exp_name,
                self.__current_exp_task,
                self.__current_session_id,
            )
            return False
        if len(forwarded_message_model_list) == 0:
            if should_log_message_flow:
                self.__logger.debug(
                    "平台侧消息未转发给算法: summary=%s queue_wait_ms=%.3f "
                    "challenge_preprocess_ms=%.3f total_consume_ms=%.3f reason=forward_filter_buffering",
                    self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                    queue_wait_ms,
                    (challenge_receive_complete_wallclock - challenge_receive_start_wallclock) * 1000.0,
                    (time.time() - message_consume_start_wallclock) * 1000.0,
                )
            return False
        try:
            send_to_algorithm_start_wallclock = time.time()
            for forwarded_message_model in forwarded_message_model_list:
                if (
                    algorithm_data_message_model.source_label == self.__CALIBRATION_PRIVATE_SOURCE_LABEL
                    and isinstance(forwarded_message_model.package, DataPackageModel)
                    and isinstance(forwarded_message_model.package.data, (bytes, bytearray))
                ):
                    self.__logger.debug(
                        "转发 calibration chunk 到算法: source_label=%s bytes=%s exp_name=%s exp_task=%s session_id=%s",
                        algorithm_data_message_model.source_label,
                        len(forwarded_message_model.package.data),
                        self.__current_exp_name,
                        self.__current_exp_task,
                        self.__current_session_id,
                    )
                await self._algorithm_connector.send_data(forwarded_message_model)
                if (
                    algorithm_data_message_model.source_label == self.__CALIBRATION_PRIVATE_SOURCE_LABEL
                    and isinstance(forwarded_message_model.package, DataPackageModel)
                    and isinstance(forwarded_message_model.package.data, (bytes, bytearray))
                ):
                    await asyncio.sleep(0)
            send_to_algorithm_complete_wallclock = time.time()
        except ProcessHubAlgorithmConnectorClosedException as exc:
            await self.__mark_algorithm_disconnected(
                disconnect_reason=f'algorithm_connector_closed_during_send: {exc}',
                algorithm_address=self._algorithm_connector.get_algorithm_address(),
            )
            return False
        except Exception as exc:
            await self.__mark_algorithm_disconnected(
                disconnect_reason=f'algorithm_send_failed: {type(exc).__name__}: {exc}',
                algorithm_address=self._algorithm_connector.get_algorithm_address(),
            )
            return False
        self.__logger.debug(
            "%s转发消息%s",
            algorithm_data_message_model.source_label,
            type(forwarded_message_model_list[-1].package),
        )
        if should_log_message_flow:
            self.__logger.debug(
                "平台侧消息转发完成: summary=%s queue_wait_ms=%.3f challenge_preprocess_ms=%.3f "
                "send_to_algorithm_ms=%.3f total_consume_ms=%.3f forwarded_count=%s forwarded_summary=%s",
                self.__summarize_incoming_message_for_log(algorithm_data_message_model),
                queue_wait_ms,
                (challenge_receive_complete_wallclock - challenge_receive_start_wallclock) * 1000.0,
                (send_to_algorithm_complete_wallclock - send_to_algorithm_start_wallclock) * 1000.0,
                (send_to_algorithm_complete_wallclock - message_consume_start_wallclock) * 1000.0,
                len(forwarded_message_model_list),
                self.__summarize_incoming_message_for_log(forwarded_message_model_list[-1]),
            )
        return True

    def __build_task_signature(
        self,
        subject_id: str | None = None,
        exp_name: str | None = None,
        exp_task: str | None = None,
    ) -> tuple[str, str, str] | None:
        subject_text = str(subject_id or self.__current_subject_id or '').strip()
        exp_name_text = str(exp_name or self.__current_exp_name or '').strip()
        exp_task_text = str(exp_task or self.__current_exp_task or '').strip()
        if subject_text == '' or exp_name_text == '' or exp_task_text == '':
            return None
        return (subject_text, exp_name_text, exp_task_text)

    def __build_stage_signature(
        self,
        subject_id: str | None = None,
        exp_name: str | None = None,
        exp_task: str | None = None,
        session_id: str | None = None,
    ) -> tuple[str, str, str, str] | None:
        task_signature = self.__build_task_signature(subject_id, exp_name, exp_task)
        session_text = str(session_id or self.__current_session_id or '').strip()
        if task_signature is None or session_text == '':
            return None
        return (*task_signature, session_text)

    def __should_emit_calibration_unavailable_event(self) -> bool:
        stage_signature = self.__build_stage_signature()
        return (
            stage_signature is not None
            and stage_signature not in self.__calibration_ready_stage_signature_set
        )

    @staticmethod
    def __serialize_task_signature(task_signature: tuple[str, str, str] | None) -> dict | None:
        if task_signature is None:
            return None
        subject_id, exp_name, exp_task = task_signature
        return {
            'subject_id': subject_id,
            'exp_name': exp_name,
            'exp_task': exp_task,
        }

    def __extract_task_signature_from_message(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> tuple[str, str, str] | None:
        package = algorithm_data_message_model.package
        subject_id = self.__current_subject_id
        exp_name = self.__current_exp_name
        exp_task = self.__current_exp_task
        if isinstance(package, DevicePackageModel):
            other_information = package.other_information or {}
            subject_id = other_information.get('subject_id') or subject_id
            exp_name = other_information.get('exp_name') or exp_name
            exp_task = other_information.get('exp_task') or exp_task
        elif isinstance(package, InformationPackageModel):
            subject_id = package.subject_id or subject_id
        return self.__build_task_signature(subject_id, exp_name, exp_task)

    async def __mark_algorithm_disconnected(
        self,
        *,
        disconnect_reason: str,
        algorithm_address: str | None,
    ) -> None:
        if self.__algorithm_disconnected_for_current_task:
            return
        self.__algorithm_connection_ready = False
        if self.__is_before_competition_start():
            self.__last_disconnect_wallclock = time.time()
            self.__algorithm_disconnected_for_current_task = False
            self.__disconnected_task_signature = None
            self.__publish_team_live_status(
                run_status='running',
                connection_status='connecting',
                algorithm_address=algorithm_address,
                last_disconnect_at=self.__last_disconnect_wallclock,
                last_disconnect_reason=disconnect_reason,
                recovery_advice='等待选手机重新启动并自动接入',
                forfeit_current_task=False,
                forfeit_task_signature=None,
            )
            self.__logger.warning(
                '算法在比赛开始前断开，回退到等待接入状态并继续自动重连: '
                'team_id=%s address=%s disconnect_reason=%s',
                self.__team_id,
                algorithm_address,
                disconnect_reason,
            )
            self.__ensure_startup_connection_retry_loop_started()
            return
        self.__algorithm_disconnected_for_current_task = True
        self.__last_disconnect_wallclock = time.time()
        task_signature = self.__build_task_signature()
        if task_signature is not None:
            self.__disconnected_task_signature = task_signature
        self.__publish_team_live_status(
            run_status='running',
            connection_status='disconnected',
            algorithm_address=algorithm_address,
            last_disconnect_at=self.__last_disconnect_wallclock,
            last_disconnect_reason=disconnect_reason,
            recovery_advice='当前 task 后续按 timeout 处理，下一个 task 再尝试恢复',
            forfeit_current_task=True,
            forfeit_task_signature=self.__serialize_task_signature(self.__disconnected_task_signature),
        )
        if self.__should_emit_calibration_unavailable_event():
            await self.__emit_team_calibration_forfeited_event(
                disconnect_reason=disconnect_reason,
                algorithm_address=algorithm_address,
            )
        self.__logger.warning(
            '算法连接断开，当前 task 后续 trial 将按 timeout / 无效处理，待下一个 task 再尝试恢复: '
            'team_id=%s address=%s disconnect_reason=%s task_signature=%s',
            self.__team_id,
            algorithm_address,
            disconnect_reason,
            self.__disconnected_task_signature,
        )

    async def __finalize_run(
        self,
        *,
        algorithm_address: str | None,
        connection_status: str,
        finish_reason: str,
        success_stderr_flag: bool,
    ) -> None:
        if self.__run_finalized:
            return
        self.__run_finalized = True
        resolved_algorithm_address = algorithm_address or self._algorithm_connector.get_algorithm_address()

        await self.__current_challenge.receive_algorithm_connector_closed_event(resolved_algorithm_address)
        score_package_model_list = await self.__current_challenge.get_score()
        self.__final_score = await self.__calculate_final_score(score_package_model_list)
        if success_stderr_flag:
            self.__original_stderr.write("[SUCCESS]算法运行成功\n")
            self.__write_final_score_breakdown()
            self.__original_stderr.write(f"[SUCCESS]最终得分: {self.__final_score:.6f}\n")
        else:
            self.__logger.info(
                "以非正常断开后的自然收尾方式完成最终封板: address=%s score_count=%s final_score=%.6f",
                resolved_algorithm_address,
                len(score_package_model_list),
                float(self.__final_score),
            )
            self.__write_final_score_breakdown()

        taskId = os.environ.get('TASK_ID')
        ip = os.environ.get('IP')
        if taskId and ip:
            try:
                url = "http://%s:10088/task/updateScore" % (ip)
                params = {"taskId": taskId, "score": self.__final_score}
                requests.post(url, params, timeout=5)
            except requests.RequestException as exc:
                self.__logger.warning("外部平台分数上报失败: %s", exc)
        else:
            self.__logger.info("缺少 TASK_ID 或 IP，已跳过外部平台分数上报")

        await self.__flush_new_score_packages("最终封板,发送成绩")
        self.__logger.info(
            "最终封板补发成绩完成: address=%s total_sent_score_count=%s finish_reason=%s",
            resolved_algorithm_address,
            len(self.__send_trial_mark_tuple_set),
            finish_reason,
        )
        self.__log_preliminary_total_runtime(finish_reason=finish_reason)
        await self.__refresh_team_live_score_fields()
        self.__publish_team_live_status(
            run_status='finished',
            connection_status=connection_status,
            final_total_score=float(self.__final_score),
            final_score_result=self.__final_score_result,
            algorithm_address=resolved_algorithm_address,
            recovery_advice=None,
            forfeit_current_task=False,
            forfeit_task_signature=None,
        )
        await self.__emit_team_run_finalized_event(terminal_run_status='finished')
        algorithm_connect_event_model = AlgorithmConnectEventModel(
            package=AlgorithmConnectClosedEventModel(address=resolved_algorithm_address)
        )
        await self.__send_framework_message(
            MessageKeyEnum.ALGORITHMCLOSED.value,
            AlgorithmConnectEventMessageConverter.model_to_protobuf(
                algorithm_connect_event_model
            ).SerializeToString(),
            log_context=f"algorithm_closed address={resolved_algorithm_address}",
        )

    async def __maybe_finalize_disconnected_run_after_stream_end(self) -> None:
        if self.__run_finalized:
            return
        if self.__task_status is not ServiceStatusEnum.RUNNING:
            return
        if not self.__algorithm_disconnected_for_current_task:
            return
        if not self.__input_stream_finished:
            return
        if len(self.__pending_trial_timing_queue) != 0:
            return
        await self.__finalize_run(
            algorithm_address=self._algorithm_connector.get_algorithm_address(),
            connection_status='disconnected',
            finish_reason='algorithm disconnected but stream completed',
            success_stderr_flag=False,
        )

    async def __maybe_finalize_unconnected_run_after_stream_end(self) -> None:
        if self.__run_finalized:
            return
        if self.__task_status is not ServiceStatusEnum.RUNNING:
            return
        if self.__algorithm_connection_ready:
            return
        if not self.__input_stream_finished:
            return
        if len(self.__pending_trial_timing_queue) != 0:
            return
        await self.__finalize_run(
            algorithm_address=self._algorithm_connector.get_algorithm_address(),
            connection_status='disconnected',
            finish_reason='algorithm never connected but stream completed',
            success_stderr_flag=False,
        )

    def __is_before_competition_start(self) -> bool:
        return self.__build_task_signature() is None

    def __ensure_startup_connection_retry_loop_started(self) -> None:
        if self.__task_status is not ServiceStatusEnum.RUNNING:
            return
        if self.__algorithm_connection_ready or self.__run_finalized:
            return
        if self.__startup_connect_retry_task is not None and not self.__startup_connect_retry_task.done():
            return
        self.__startup_connect_retry_task = asyncio.create_task(
            self.__run_startup_connection_retry_loop()
        )

    async def __maybe_handle_task_boundary_for_reconnect(self) -> None:
        if not self.__algorithm_disconnected_for_current_task:
            return
        current_task_signature = self.__build_task_signature()
        if current_task_signature is None:
            return
        if self.__disconnected_task_signature is None:
            self.__disconnected_task_signature = current_task_signature
            self.__publish_team_live_status(
                forfeit_task_signature=self.__serialize_task_signature(current_task_signature),
            )
            return
        if current_task_signature == self.__disconnected_task_signature:
            return
        await self.__attempt_reconnect_for_new_task(current_task_signature)

    async def __attempt_reconnect_for_new_task(
        self,
        next_task_signature: tuple[str, str, str],
    ) -> None:
        if self.__reconnect_attempt_in_progress:
            return
        self.__reconnect_attempt_in_progress = True
        self.__reconnect_attempt_task_signature = next_task_signature
        algorithm_address = self._algorithm_connector.get_algorithm_address()
        previous_task_signature = self.__disconnected_task_signature
        if previous_task_signature is not None and previous_task_signature != next_task_signature:
            scored_pending_count = await self.__force_timeout_pending_trials_for_task_signature(
                previous_task_signature,
                reason="task_boundary_cleanup_before_reconnect",
            )
            if scored_pending_count > 0:
                self.__logger.warning(
                    "进入新 task 前已将旧 task 残留 pending trial 按 timeout 结算: "
                    "previous_task=%s next_task=%s scored_pending_count=%s",
                    previous_task_signature,
                    next_task_signature,
                    scored_pending_count,
                )
        self.__publish_team_live_status(
            run_status='running',
            connection_status='reconnecting',
            recovery_advice='检测到新 task，正在尝试恢复连接',
            forfeit_current_task=True,
            forfeit_task_signature=self.__serialize_task_signature(previous_task_signature),
        )
        self.__logger.info(
            '检测到进入新 task，开始尝试恢复算法连接: team_id=%s address=%s previous_task=%s next_task=%s',
            self.__team_id,
            algorithm_address,
            previous_task_signature,
            next_task_signature,
        )
        try:
            await self._algorithm_connector.shutdown()
        except Exception:
            self.__logger.exception('重连前关闭旧算法连接失败，继续尝试重建: address=%s', algorithm_address)
        try:
            await self.__establish_algorithm_runtime_session(allow_current_task_join=True)
            self.__publish_team_live_status(
                recovery_advice='已在新 task 恢复连接并重新纳入比赛',
            )
            self.__logger.info(
                '算法连接恢复成功，已在新 task 重新纳入比赛: team_id=%s address=%s next_task=%s',
                self.__team_id,
                algorithm_address,
                next_task_signature,
            )
        except Exception:
            self.__algorithm_disconnected_for_current_task = True
            self.__disconnected_task_signature = next_task_signature
            if self.__should_emit_calibration_unavailable_event():
                await self.__emit_team_calibration_forfeited_event(
                    disconnect_reason='algorithm_reconnect_failed_for_new_task',
                    algorithm_address=algorithm_address,
                )
            self.__publish_team_live_status(
                run_status='running',
                connection_status='disconnected',
                recovery_advice='当前 task 后续按 timeout 处理，下一个 task 再尝试恢复',
                forfeit_current_task=True,
                forfeit_task_signature=self.__serialize_task_signature(next_task_signature),
            )
            self.__logger.exception(
                '算法连接恢复失败，当前新 task 继续按 timeout / 无效处理: team_id=%s address=%s next_task=%s',
                self.__team_id,
                algorithm_address,
                next_task_signature,
            )
        finally:
            self.__reconnect_attempt_in_progress = False

    def __load_challenge(self, challenge_class_file: str, challenge_class_name: str) -> ChallengeInterface:
        self.__logger.debug('加载赛题: ' + challenge_class_file + ':' + challenge_class_name)
        workspace_path = os.getcwd()
        absolute_challenge_class_file = os.path.join(workspace_path, challenge_class_file)
        module_name = os.path.splitext(os.path.basename(absolute_challenge_class_file))[0]
        module_dir = os.path.dirname(absolute_challenge_class_file)
        if module_dir not in sys.path:
            sys.path.append(module_dir)
        module = importlib.import_module(module_name)
        challenge_class = getattr(module, challenge_class_name)
        return challenge_class()

    async def __load_timeout_config(self) -> None:
        strategy_config = await self.__current_challenge.get_to_strategy_config()
        timeout_setting_dict = (strategy_config or {}).get('timeout_setting', {}) or {}
        for timeout_parameter in timeout_setting_dict.values():
            if not isinstance(timeout_parameter, dict):
                continue
            timeout_limit = timeout_parameter.get('timeout_limit')
            if timeout_limit is not None:
                try:
                    self.__timeout_limit_seconds = float(timeout_limit)
                except (TypeError, ValueError):
                    self.__timeout_limit_seconds = 0.0
            timeout_predict_label = timeout_parameter.get('timeout_predict_label')
            if timeout_predict_label is not None and str(timeout_predict_label).strip() != "":
                self.__timeout_predict_label = str(timeout_predict_label).strip()
            break
        self.__logger.info(
            "已加载决赛 timeout 配置: timeout_limit_seconds=%s timeout_predict_label=%s",
            self.__timeout_limit_seconds,
            self.__timeout_predict_label,
        )

    async def __flush_new_score_packages(self, log_prefix: str) -> None:
        if self.__report_stream_closed:
            return
        score_package_model_list = await self.__current_challenge.get_score()
        for score_package_model in score_package_model_list:
            trial_mark_tuple = self.__TrialMarkTuple(
                trial_id=score_package_model.trial_id,
                block_id=score_package_model.block_id,
                subject_id=score_package_model.subject_id,
            )
            if trial_mark_tuple in self.__send_trial_mark_tuple_set:
                continue
            if not await self.__send_report_package(
                score_package_model,
                log_context=f"score trial_id={score_package_model.trial_id}",
            ):
                break
            self.__send_trial_mark_tuple_set.add(trial_mark_tuple)
            self.__logger.info(f"{log_prefix}:\n{score_package_model}")

    async def __enqueue_report_finalization(self, payload: dict) -> None:
        if self.__task_status is ServiceStatusEnum.STOPPING:
            return
        self.__ensure_report_finalize_worker_started()
        await self.__report_finalize_queue.put(dict(payload))

    async def __run_report_finalize_worker(self) -> None:
        while True:
            finalize_payload = await self.__report_finalize_queue.get()
            try:
                if finalize_payload is None:
                    return
                finalize_type = finalize_payload.get('finalize_type')
                if finalize_type == 'result':
                    await self.__finalize_result_report(finalize_payload)
                elif finalize_type == 'timeout':
                    await self.__finalize_timeout_report(finalize_payload)
                else:
                    self.__logger.warning("未知 report finalize 类型，已忽略: %s", finalize_type)
            except Exception:
                self.__logger.exception("report finalize worker 执行失败: payload=%s", finalize_payload)
            finally:
                self.__report_finalize_queue.task_done()

    async def __finalize_result_report(self, finalize_payload: dict) -> None:
        algorithm_report_message_model = finalize_payload.get('algorithm_report_message_model')
        matched_trial_timing = finalize_payload.get('matched_trial_timing')
        report_receive_wallclock = float(finalize_payload.get('report_receive_wallclock') or time.time())
        result_payload = finalize_payload.get('result_payload') or {}
        is_buffered_replay = bool(finalize_payload.get('is_buffered_replay', False))
        log_context = str(finalize_payload.get('log_context') or "algorithm result package")

        if algorithm_report_message_model is None:
            return
        if not await self.__send_report_package(
            algorithm_report_message_model.package,
            log_context=log_context,
        ):
            return
        await self.__flush_new_score_packages(
            "缓存算法结果回放,发送成绩" if is_buffered_replay else "算法报告,发送成绩"
        )
        await self.__refresh_team_live_score_fields()
        if matched_trial_timing is not None:
            self.__publish_team_live_status(
                run_status='running',
                connection_status='disconnected' if self.__algorithm_disconnected_for_current_task else 'connected',
                last_terminal_type='result',
                last_terminal_wallclock=report_receive_wallclock,
                subject_id=matched_trial_timing.get('subject_id'),
                exp_name=matched_trial_timing.get('exp_name'),
                exp_task=matched_trial_timing.get('exp_task'),
                session_id=matched_trial_timing.get('session_id'),
                block_id=matched_trial_timing.get('block_id'),
                trial_id=matched_trial_timing.get('trial_id'),
                trial_start_position=matched_trial_timing.get('trial_start_position'),
                trial_end_position=matched_trial_timing.get('trial_end_position'),
                predict_label=self.__extract_predict_label_from_result_package(
                    algorithm_report_message_model.package
                ),
                true_label=result_payload.get('platform_true_label'),
                raw_trigger_value=result_payload.get('platform_raw_trigger_value'),
                predict_time_ms=result_payload.get('predict_time_ms'),
                is_timeout=bool(result_payload.get('is_timeout', False)),
            )

    async def __finalize_timeout_report(self, finalize_payload: dict) -> None:
        matched_trial_timing = finalize_payload.get('matched_trial_timing')
        timeout_context = finalize_payload.get('timeout_context') or {}
        if matched_trial_timing is None:
            return
        await self.__flush_new_score_packages("算法超时,发送成绩")
        await self.__refresh_team_live_score_fields()
        self.__publish_team_live_status(
            run_status='running',
            connection_status='disconnected' if self.__algorithm_disconnected_for_current_task else 'connected',
            last_terminal_type='timeout',
            last_terminal_wallclock=timeout_context.get('platform_report_receive_wallclock'),
            subject_id=matched_trial_timing.get('subject_id'),
            exp_name=matched_trial_timing.get('exp_name'),
            exp_task=matched_trial_timing.get('exp_task'),
            session_id=matched_trial_timing.get('session_id'),
            block_id=matched_trial_timing.get('block_id'),
            trial_id=matched_trial_timing.get('trial_id'),
            trial_start_position=matched_trial_timing.get('trial_start_position'),
            trial_end_position=matched_trial_timing.get('trial_end_position'),
            predict_label=timeout_context.get('predict_label'),
            true_label=timeout_context.get('platform_true_label'),
            raw_trigger_value=timeout_context.get('platform_raw_trigger_value'),
            predict_time_ms=timeout_context.get('predict_time_ms'),
            is_timeout=True,
        )
        self.__logger.info(
            "trial timeout scored internally: subject_id=%s exp_name=%s exp_task=%s session_id=%s block_id=%s trial_id=%s",
            matched_trial_timing.get('subject_id'),
            matched_trial_timing.get('exp_name'),
            matched_trial_timing.get('exp_task'),
            matched_trial_timing.get('session_id'),
            matched_trial_timing.get('block_id'),
            matched_trial_timing.get('trial_id'),
        )

    async def __send_report_package(self, package, log_context: str) -> bool:
        return await self.__send_framework_message(
            MessageKeyEnum.REPORT.value,
            CommonMessageConverter.model_to_protobuf(
                DataMessageModel(package=package)
            ).SerializeToString(),
            log_context=log_context,
        )

    async def __send_framework_message(self, message_key: str, message_bytes: bytes, log_context: str) -> bool:
        if self.__report_stream_closed:
            return False

        try:
            await self._component_framework.send_message(message_key, message_bytes)
            return True
        except asyncio.InvalidStateError as exc:
            self.__report_stream_closed = True
            self.__cancel_all_pending_timeout_tasks()
            self.__logger.warning(
                "message stream already finished, skip future sends: key=%s context=%s error=%s",
                message_key,
                log_context,
                exc,
            )
            return False

    async def __calculate_final_score(self, score_package_model_list: list[ScorePackageModel]) -> float:
        get_final_score_context = getattr(self.__current_challenge, 'get_final_score_context', None)
        if callable(get_final_score_context):
            score_context = get_final_score_context()
            if isinstance(score_context, dict) and score_context:
                final_score_result = self.__build_final_score_result(score_context)
                self.__final_score_result = final_score_result
                finalize_score_result = getattr(self.__current_challenge, 'finalize_score_result', None)
                if callable(finalize_score_result):
                    finalize_score_result(final_score_result)
                self.__logger.info("最终得分明细: %s", final_score_result)
                return float(final_score_result.get('total_score', 0.0))

        self.__final_score_result = None
        return self.__calculate_fallback_final_score(score_package_model_list)

    def __write_final_score_breakdown(self) -> None:
        if not isinstance(self.__final_score_result, dict):
            return
        score_line = (
            "[SUCCESS]单项分数: "
            f"Sper={self.__safe_float(self.__final_score_result.get('sper_score')):.6f}, "
            f"Stime={self.__safe_float(self.__final_score_result.get('stime_score')):.6f}, "
            f"Schannel={self.__safe_float(self.__final_score_result.get('schannel_score')):.6f}, "
            f"Scal={self.__safe_float(self.__final_score_result.get('scal_score')):.6f}, "
            f"Ssize={self.__safe_float(self.__final_score_result.get('ssize_score')):.6f}"
        )
        self.__original_stderr.write(score_line + "\n")
        self.__logger.info(score_line)

    @classmethod
    def __build_final_score_result(cls, score_context: dict) -> dict:
        task_summary_dict = score_context.get('task_summary') or {}
        task_order = score_context.get('task_order')
        if not isinstance(task_order, list) or not task_order:
            task_order = sorted(task_summary_dict.keys())
        task_baseline_score_dict = score_context.get('task_baseline_score_dict') or {}

        task_metric_list = []
        for task_name in task_order:
            task_summary = task_summary_dict.get(task_name) or {}
            baseline_score = cls.__safe_float(task_summary.get('baseline_score', task_baseline_score_dict.get(task_name)))
            task_score = cls.__safe_float(task_summary.get('task_score'))
            adjusted_task_score = task_score if task_score >= baseline_score else 0.0
            task_metric_list.append(
                {
                    'task_name': task_name,
                    'exp_name': task_summary.get('exp_name'),
                    'exp_task': task_summary.get('exp_task'),
                    'cumulative_accuracy_percent': cls.__safe_float(task_summary.get('cumulative_accuracy_percent')),
                    'accuracy_score': cls.__safe_float(task_summary.get('accuracy_score')),
                    'avg_reaction_time_ms': cls.__safe_float(task_summary.get('avg_reaction_time_ms')),
                    'reaction_time_score': cls.__safe_float(task_summary.get('reaction_time_score')),
                    'channel_score': cls.__safe_float(task_summary.get('channel_score')),
                    'calibration_score': cls.__safe_float(task_summary.get('calibration_score')),
                    'model_size_score': cls.__safe_float(task_summary.get('model_size_score')),
                    'task_score': task_score,
                    'baseline_score': baseline_score,
                    'adjusted_task_score': adjusted_task_score,
                    'subject_count': int(task_summary.get('subject_count') or 0),
                    'trial_count': int(task_summary.get('trial_count') or 0),
                }
            )

        started_task_metric_list = [
            task_metric
            for task_metric in task_metric_list
            if int(task_metric.get('trial_count') or 0) > 0
        ]
        mean_metric_source_list = started_task_metric_list or task_metric_list
        total_score_metric_source_list = task_metric_list
        total_started_trial_count = sum(
            int(task_metric.get('trial_count') or 0)
            for task_metric in mean_metric_source_list
        )

        if total_started_trial_count > 0:
            mean_accuracy_percent = sum(
                cls.__safe_float(task_metric.get('cumulative_accuracy_percent'))
                * int(task_metric.get('trial_count') or 0)
                for task_metric in mean_metric_source_list
            ) / total_started_trial_count
            avg_reaction_time_ms = sum(
                cls.__safe_float(task_metric.get('avg_reaction_time_ms'))
                * int(task_metric.get('trial_count') or 0)
                for task_metric in mean_metric_source_list
            ) / total_started_trial_count
        else:
            mean_accuracy_percent = cls.__safe_mean(
                [task_metric['cumulative_accuracy_percent'] for task_metric in mean_metric_source_list]
            )
            avg_reaction_time_ms = cls.__safe_mean(
                [task_metric['avg_reaction_time_ms'] for task_metric in mean_metric_source_list]
            )

        avg_runtime_ms = cls.__safe_float(score_context.get('avg_runtime_ms') or avg_reaction_time_ms)

        sper_score = cls.__safe_mean([task_metric['accuracy_score'] for task_metric in mean_metric_source_list])
        stime_score = cls.__safe_mean([task_metric['reaction_time_score'] for task_metric in mean_metric_source_list])
        schannel_score = cls.__safe_mean([task_metric['channel_score'] for task_metric in mean_metric_source_list])
        scal_score = cls.__safe_mean([task_metric['calibration_score'] for task_metric in mean_metric_source_list])
        ssize_score = cls.__safe_mean([task_metric['model_size_score'] for task_metric in mean_metric_source_list])

        total_score = cls.__safe_mean(
            [task_metric['adjusted_task_score'] for task_metric in total_score_metric_source_list]
        )

        channel_count = int(score_context.get('channel_count') or 8)
        calibration_trials_per_class = int(score_context.get('calibration_trials_per_class') or 10)
        model_size_mb_raw = score_context.get('model_size_mb')
        size_score_enabled = bool(score_context.get('size_score_enabled'))
        model_size_mb = None if model_size_mb_raw is None else cls.__safe_float(model_size_mb_raw)

        return {
            'team_id': score_context.get('team_id'),
            'record_count': int(score_context.get('record_count') or 0),
            'hierarchy': score_context.get('hierarchy') or ['subject_id', 'task_id', 'session_id', 'trial_id'],
            'task_field': score_context.get('task_field') or 'task_id',
            'subtask_field': score_context.get('subtask_field') or 'exp_task',
            'task_count': len(task_metric_list),
            'started_task_count': len(started_task_metric_list),
            'task_metric_list': task_metric_list,
            'task_summary': task_summary_dict,
            'task_baseline_score_dict': task_baseline_score_dict,
            'mean_accuracy_percent': mean_accuracy_percent,
            'sper_score': sper_score,
            'avg_runtime_ms': avg_runtime_ms,
            'avg_reaction_time_ms': avg_reaction_time_ms,
            'runtime_definition': score_context.get('runtime_definition') or 'trial_end_to_result_report_ms',
            'stime_score': stime_score,
            'channel_count': channel_count,
            'schannel_score': schannel_score,
            'calibration_trials_per_class': calibration_trials_per_class,
            'scal_score': scal_score,
            'model_size_mb': model_size_mb,
            'size_score_enabled': size_score_enabled,
            'ssize_score': ssize_score,
            'window_length_seconds': cls.__safe_float(score_context.get('window_length_seconds') or 4.0),
            'total_score_formula': 'mean(configured_task_adjusted_task_score, unstarted_task_as_0)',
            'total_score': total_score,
            # 'notes': [
            #     'task score is cumulative accuracy score + cumulative reaction time score + static model/channel/calibration scores',
            #     'accuracy score uses accuracy_score_max * max(0, (mu_accuracy_percent - lambda * sigma_accuracy_percent) / 100), where sigma is the std of cumulative accuracy history',
            #     'current configuration uses four tasks: vme_left_vs_rest, vme_right_vs_rest, vmi_left_vs_rest, vmi_right_vs_rest',
            #     'final total score is the mean of the task scores after baseline gating',
            #     'timeout trials are forced to wrong prediction and 2000 ms reaction time',
            # ],
        }

    @staticmethod
    def __calculate_fallback_final_score(score_package_model_list: list[ScorePackageModel]) -> float:
        subject_block_trials = {}
        for model in score_package_model_list:
            key = (model.subject_id, model.block_id)
            if key not in subject_block_trials:
                subject_block_trials[key] = []
            subject_block_trials[key].append(model)

        final_scores = []
        for _, trial_list in subject_block_trials.items():
            sorted_trials = sorted(trial_list, key=lambda x: int(x.trial_id))
            if sorted_trials:
                final_scores.append(sorted_trials[-1].score)

        if not final_scores:
            return 0.0
        return sum(final_scores) / len(final_scores)

    @staticmethod
    def __safe_float(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def __safe_mean(cls, value_list: list[float]) -> float:
        normalized_value_list = [cls.__safe_float(value) for value in value_list]
        if not normalized_value_list:
            return 0.0
        return sum(normalized_value_list) / len(normalized_value_list)

    def __log_preliminary_total_runtime(self, finish_reason: str) -> None:
        if self.__preliminary_runtime_logged:
            return
        if self.__preliminary_run_start_wallclock is None or self.__preliminary_run_start_monotonic is None:
            return
        elapsed_seconds = time.perf_counter() - self.__preliminary_run_start_monotonic
        self.__preliminary_runtime_logged = True
        self.__logger.info(
            "决赛总流程计时结束: finish_reason=%s start_wallclock=%.6f elapsed_seconds=%.3f elapsed_minutes=%.3f",
            finish_reason,
            self.__preliminary_run_start_wallclock,
            elapsed_seconds,
            elapsed_seconds / 60.0,
        )

    async def __update_trial_timing_context(self, algorithm_data_message_model: AlgorithmDataMessageModel) -> None:
        package = algorithm_data_message_model.package
        if isinstance(package, DevicePackageModel):
            self.__current_sample_rate = float(package.sample_rate or 0.0)
            other_information = package.other_information or {}
            self.__current_subject_id = other_information.get('subject_id') or self.__current_subject_id
            self.__current_exp_name = other_information.get('exp_name')
            self.__current_exp_task = other_information.get('exp_task')
            self.__current_session_id = other_information.get('session_id')
            stream_role = str(other_information.get('stream_role') or '').strip().lower()
            if stream_role in {'calibration', 'online'}:
                self.__current_stage_phase = stream_role
            return

        if isinstance(package, InformationPackageModel):
            self.__current_subject_id = package.subject_id
            self.__current_block_id = package.block_id
            return

        if not isinstance(package, EventPackageModel):
            return

        if self.__current_sample_rate <= 0:
            return

        trial_point = int(self.__trial_window_seconds * self.__current_sample_rate)
        for event_position, event_data in zip(package.event_position, package.event_data):
            if str(event_data) != '241':
                continue

            trial_counter_key = (
                self.__current_subject_id,
                self.__current_exp_name,
                self.__current_exp_task,
                self.__current_session_id,
            )
            current_trial_id = self.__trial_counter_dict.get(trial_counter_key, 0) + 1
            self.__trial_counter_dict[trial_counter_key] = current_trial_id
            trial_end_position = int(float(event_position))
            trial_start_position = trial_end_position - trial_point
            trial_ready_wallclock = time.time()
            trial_end_message_receive_wallclock = getattr(
                algorithm_data_message_model,
                '_processhub_receive_wallclock',
                None,
            )
            trial_end_message_consume_wallclock = getattr(
                algorithm_data_message_model,
                '_processhub_consume_start_wallclock',
                None,
            )
            trial_end_message_queue_wait_ms = self.__calculate_incoming_message_queue_wait_ms(
                algorithm_data_message_model,
                consume_wallclock=trial_end_message_consume_wallclock,
            )
            trial_ready_processing_ms = 0.0
            if trial_end_message_consume_wallclock is not None:
                trial_ready_processing_ms = (
                    trial_ready_wallclock - float(trial_end_message_consume_wallclock)
                ) * 1000.0
            self.__register_pending_trial_timing(
                {
                    'subject_id': self.__current_subject_id,
                    'exp_name': self.__current_exp_name,
                    'exp_task': self.__current_exp_task,
                    'session_id': self.__current_session_id,
                    'block_id': self.__current_block_id,
                    'trial_id': str(current_trial_id),
                    'trial_start_position': trial_start_position,
                    'trial_end_position': trial_end_position,
                    'trial_ready_wallclock': trial_ready_wallclock,
                    'trial_end_message_receive_wallclock': trial_end_message_receive_wallclock,
                    'trial_end_message_consume_wallclock': trial_end_message_consume_wallclock,
                    'trial_end_message_queue_wait_ms': trial_end_message_queue_wait_ms,
                    'trial_ready_processing_ms': trial_ready_processing_ms,
                }
            )
            self.__logger.debug(
                "trial ready recorded: subject_id=%s exp_name=%s exp_task=%s "
                "session_id=%s block_id=%s trial_id=%s trial_end_position=%s "
                "trial_start_position=%s ready_wallclock=%.6f queue_wait_ms=%.3f ready_processing_ms=%.3f "
                "pending_trial_count=%s buffered_result_count=%s",
                self.__current_subject_id,
                self.__current_exp_name,
                self.__current_exp_task,
                self.__current_session_id,
                self.__current_block_id,
                current_trial_id,
                trial_end_position,
                trial_start_position,
                trial_ready_wallclock,
                trial_end_message_queue_wait_ms,
                trial_ready_processing_ms,
                len(self.__pending_trial_timing_queue),
                len(self.__buffered_result_by_end_position),
            )
            if self.__should_force_immediate_timeout_for_trial(
                self.__pending_trial_timing_by_end_position.get(trial_end_position)
            ):
                await self.__force_timeout_for_disconnected_trial(trial_end_position)
                continue
            await self.__replay_buffered_result_after_trial_ready(trial_end_position)

    def __pop_pending_trial_timing(self) -> dict | None:
        if not self.__pending_trial_timing_queue:
            self.__logger.warning("收到算法结果时未找到待匹配的trial计时记录")
            return None
        matched_trial_timing = self.__pending_trial_timing_queue.popleft()
        self.__pending_trial_timing_by_end_position.pop(matched_trial_timing.get('trial_end_position'), None)
        self.__cancel_timeout_task(
            matched_trial_timing,
            cancel_reason="result_fallback_pop",
        )
        return matched_trial_timing

    def __register_pending_trial_timing(self, trial_timing: dict) -> None:
        trial_timing = dict(trial_timing)
        trial_end_position = trial_timing.get('trial_end_position')
        trial_timing.setdefault(
            'task_signature',
            self.__build_task_signature(
                trial_timing.get('subject_id'),
                trial_timing.get('exp_name'),
                trial_timing.get('exp_task'),
            ),
        )
        self.__pending_trial_timing_queue.append(trial_timing)
        if trial_end_position is not None:
            self.__pending_trial_timing_by_end_position[int(trial_end_position)] = trial_timing
        if self.__timeout_limit_seconds > 0 and trial_end_position is not None:
            trial_timing['timeout_deadline_wallclock'] = (
                float(trial_timing.get('trial_ready_wallclock') or time.time()) + self.__timeout_limit_seconds
            )
            self.__logger.debug(
                "注册 trial timeout 截止时间: trial_end_position=%s timeout_limit_seconds=%s ready_wallclock=%.6f deadline_wallclock=%.6f",
                int(trial_end_position),
                self.__timeout_limit_seconds,
                float(trial_timing.get('trial_ready_wallclock') or 0.0),
                float(trial_timing.get('timeout_deadline_wallclock') or 0.0),
            )
            self.__notify_timeout_scheduler()

    def __consume_pending_trial_timing_for_result(self, result_package_model: ResultPackageModel) -> dict | None:
        trial_end_position = self.__extract_trial_end_position_from_result(result_package_model)
        if trial_end_position is not None and trial_end_position in self.__timed_out_trial_end_position_set:
            self.__timed_out_trial_end_position_set.remove(trial_end_position)
            return {
                'timeout_discarded': True,
                'trial_end_position': trial_end_position,
            }

        if trial_end_position is not None:
            matched_trial_timing = self.__remove_pending_trial_timing_by_end_position(trial_end_position)
            if matched_trial_timing is not None:
                return matched_trial_timing
            return {
                'await_trial_ready': True,
                'trial_end_position': trial_end_position,
            }

        return self.__pop_pending_trial_timing()

    def __buffer_result_until_trial_ready(
        self,
        trial_end_position: int | None,
        algorithm_report_message_model: AlgorithmReportMessageModel,
        report_receive_wallclock: float,
        raw_result_summary: str,
        report_source_information_summary: list[dict],
    ) -> None:
        if trial_end_position is None:
            return
        normalized_trial_end_position = int(trial_end_position)
        if normalized_trial_end_position in self.__buffered_result_by_end_position:
            self.__logger.warning(
                "同一 trial_end_position 已存在缓存结果，忽略后到重复结果: "
                "trial_end_position=%s report_receive_wallclock=%.6f raw_result=%s report_source_information=%s",
                normalized_trial_end_position,
                report_receive_wallclock,
                raw_result_summary,
                report_source_information_summary,
            )
            return
        self.__buffered_result_by_end_position[normalized_trial_end_position] = {
            'algorithm_report_message_model': algorithm_report_message_model,
            'report_receive_wallclock': report_receive_wallclock,
            'raw_result_summary': raw_result_summary,
            'report_source_information_summary': report_source_information_summary,
        }
        self.__logger.debug(
            "缓存早到算法结果，等待 trial ready 后回放: "
            "trial_end_position=%s report_receive_wallclock=%.6f raw_result=%s report_source_information=%s "
            "buffered_result_count=%s pending_trial_count=%s report_transport_ms=%s",
            normalized_trial_end_position,
            report_receive_wallclock,
            raw_result_summary,
            report_source_information_summary,
            len(self.__buffered_result_by_end_position),
            len(self.__pending_trial_timing_queue),
            (
                f"{max(0.0, (report_receive_wallclock * 1000.0) - float(algorithm_report_message_model.timestamp_ms or 0)):.3f}"
                if getattr(algorithm_report_message_model, 'timestamp_ms', None) is not None
                else "unknown"
            ),
        )

    async def __replay_buffered_result_after_trial_ready(self, trial_end_position: int) -> None:
        buffered_result = self.__buffered_result_by_end_position.pop(int(trial_end_position), None)
        if buffered_result is None:
            return

        matched_trial_timing = self.__remove_pending_trial_timing_by_end_position(
            int(trial_end_position),
            remove_reason="buffered_result_replay",
        )
        if matched_trial_timing is None:
            self.__logger.warning(
                "检测到缓存结果但未找到对应 pending trial，无法回放: trial_end_position=%s buffered_result_count=%s",
                trial_end_position,
                len(self.__buffered_result_by_end_position),
            )
            return

        report_receive_wallclock = float(buffered_result.get('report_receive_wallclock') or time.time())
        raw_result_summary = buffered_result.get('raw_result_summary')
        report_source_information_summary = buffered_result.get('report_source_information_summary')
        runtime_ms = self.__calculate_runtime_ms(
            matched_trial_timing,
            report_receive_wallclock,
        )
        matched_trial_timing['report_receive_wallclock'] = report_receive_wallclock
        matched_trial_timing['runtime_ms'] = runtime_ms
        self.__enrich_result_package(
            buffered_result['algorithm_report_message_model'].package,
            matched_trial_timing,
        )
        self.__logger.debug(
            "回放早到算法结果并完成匹配: trial_end_position=%s subject_id=%s exp_name=%s exp_task=%s "
            "session_id=%s block_id=%s trial_id=%s runtime_ms=%.3f buffered_wait_ms=%.3f buffered_result_count=%s",
            trial_end_position,
            matched_trial_timing.get('subject_id'),
            matched_trial_timing.get('exp_name'),
            matched_trial_timing.get('exp_task'),
            matched_trial_timing.get('session_id'),
            matched_trial_timing.get('block_id'),
            matched_trial_timing.get('trial_id'),
            runtime_ms,
            max(
                0.0,
                (
                    float(matched_trial_timing.get('trial_ready_wallclock') or report_receive_wallclock)
                    - report_receive_wallclock
                ) * -1000.0,
            ),
            len(self.__buffered_result_by_end_position),
        )
        self.__logger.debug(
            "回放结果详情: report_receive_wallclock=%.6f raw_result=%s report_source_information=%s",
            report_receive_wallclock,
            raw_result_summary,
            report_source_information_summary,
        )
        await self.__current_challenge.receive_report(buffered_result['algorithm_report_message_model'])
        result_terminal_sent = await self.__emit_trial_terminal_event(
            terminal_type='result',
            trial_context=matched_trial_timing,
        )
        self.__logger.debug(
            "buffered result 终态事件发送结果: subject_id=%s exp_name=%s exp_task=%s session_id=%s "
            "block_id=%s trial_id=%s sent=%s",
            matched_trial_timing.get('subject_id'),
            matched_trial_timing.get('exp_name'),
            matched_trial_timing.get('exp_task'),
            matched_trial_timing.get('session_id'),
            matched_trial_timing.get('block_id'),
            matched_trial_timing.get('trial_id'),
            result_terminal_sent,
        )
        result_payload = self.__parse_json_payload(
            buffered_result['algorithm_report_message_model'].package.result
        ) or {}
        await self.__enqueue_report_finalization(
            {
                'finalize_type': 'result',
                'algorithm_report_message_model': buffered_result['algorithm_report_message_model'],
                'matched_trial_timing': matched_trial_timing,
                'report_receive_wallclock': report_receive_wallclock,
                'result_payload': result_payload,
                'log_context': 'buffered algorithm result package',
                'is_buffered_replay': True,
            }
        )

    def __remove_pending_trial_timing_by_end_position(
        self,
        trial_end_position: int,
        *,
        remove_reason: str = "unknown",
    ) -> dict | None:
        matched_trial_timing = self.__pending_trial_timing_by_end_position.pop(int(trial_end_position), None)
        if matched_trial_timing is None:
            return None
        try:
            self.__pending_trial_timing_queue.remove(matched_trial_timing)
        except ValueError:
            pass
        self.__cancel_timeout_task(
            matched_trial_timing,
            cancel_reason=remove_reason,
        )
        return matched_trial_timing

    @staticmethod
    def __extract_trial_end_position_from_result(result_package_model: ResultPackageModel) -> int | None:
        for source_information in result_package_model.report_source_information or []:
            if source_information.position is None:
                continue
            try:
                return int(float(source_information.position))
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def __summarize_result_payload_for_log(raw_result) -> str:
        if isinstance(raw_result, str):
            try:
                parsed_payload = json.loads(raw_result)
            except json.JSONDecodeError:
                return raw_result
            if isinstance(parsed_payload, dict):
                return json.dumps(parsed_payload, ensure_ascii=False, sort_keys=True)
            return str(parsed_payload)
        if isinstance(raw_result, bytes):
            return f"<bytes len={len(raw_result)}>"
        return str(raw_result)

    @staticmethod
    def __summarize_result_report_source_information_for_log(
        result_package_model: ResultPackageModel,
    ) -> list[dict]:
        return [
            {
                'source_label': item.source_label,
                'position': item.position,
            }
            for item in (result_package_model.report_source_information or [])
        ]

    def __calculate_runtime_ms(self, matched_trial_timing: dict, report_receive_wallclock: float) -> float:
        trial_ready_wallclock = float(matched_trial_timing.get('trial_ready_wallclock') or report_receive_wallclock)
        runtime_ms = (report_receive_wallclock - trial_ready_wallclock) * 1000.0
        if runtime_ms >= 0.0:
            return runtime_ms
        self.__logger.debug(
            "检测到结果早于 trial ready 被处理，platform runtime 按 1ms 记账以避免 0ms 假象: "
            "subject_id=%s exp_name=%s exp_task=%s session_id=%s block_id=%s trial_id=%s "
            "trial_end_position=%s report_receive_wallclock=%.6f trial_ready_wallclock=%.6f raw_runtime_ms=%.3f",
            matched_trial_timing.get('subject_id'),
            matched_trial_timing.get('exp_name'),
            matched_trial_timing.get('exp_task'),
            matched_trial_timing.get('session_id'),
            matched_trial_timing.get('block_id'),
            matched_trial_timing.get('trial_id'),
            matched_trial_timing.get('trial_end_position'),
            report_receive_wallclock,
            trial_ready_wallclock,
            runtime_ms,
        )
        return 1.0

    @staticmethod
    def __calculate_incoming_message_queue_wait_ms(
        algorithm_data_message_model: AlgorithmDataMessageModel,
        *,
        consume_wallclock: float | None,
    ) -> float:
        enqueue_wallclock = getattr(algorithm_data_message_model, '_processhub_enqueue_wallclock', None)
        if enqueue_wallclock is None or consume_wallclock is None:
            return 0.0
        return max(0.0, (float(consume_wallclock) - float(enqueue_wallclock)) * 1000.0)

    def __should_log_message_flow(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
        *,
        queue_size: int,
    ) -> bool:
        if queue_size >= 5:
            return True
        package = algorithm_data_message_model.package
        if isinstance(package, ControlPackageModel) and bool(package.end_flag):
            return True
        if isinstance(package, EventPackageModel):
            return any(str(event_data) == '241' for event_data in (package.event_data or []))
        if isinstance(package, DataPackageModel) and not isinstance(package.data, (bytes, bytearray)):
            return True
        return False

    def __summarize_incoming_message_for_log(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> str:
        package = algorithm_data_message_model.package
        summary = {
            'source_label': algorithm_data_message_model.source_label,
            'package_type': type(package).__name__,
        }
        if isinstance(package, EventPackageModel):
            event_data_list = list(package.event_data or [])
            event_position_list = list(package.event_position or [])
            trial_end_position_list = [
                int(float(event_position))
                for event_position, event_data in zip(event_position_list, event_data_list)
                if str(event_data) == '241'
            ]
            summary['event_count'] = len(event_data_list)
            if trial_end_position_list:
                summary['trial_end_position_list'] = trial_end_position_list
        elif isinstance(package, ControlPackageModel):
            summary['end_flag'] = bool(package.end_flag)
        elif isinstance(package, DataPackageModel):
            if isinstance(package.data, (bytes, bytearray)):
                summary['bytes'] = len(package.data)
            else:
                summary['data_position'] = package.data_position
                summary['value_count'] = len(package.data) if package.data is not None else 0
        return json.dumps(summary, ensure_ascii=False, sort_keys=True)

    def __cancel_timeout_task(self, trial_timing: dict | None, cancel_reason: str = "unknown") -> None:
        if not isinstance(trial_timing, dict):
            return
        if trial_timing.pop('timeout_deadline_wallclock', None) is None:
            return
        self.__logger.debug(
            "取消未触发的 timeout 截止时间: cancel_reason=%s trial_end_position=%s",
            cancel_reason,
            trial_timing.get('trial_end_position'),
        )
        self.__notify_timeout_scheduler()

    def __cancel_all_pending_timeout_tasks(self) -> None:
        for trial_timing in list(self.__pending_trial_timing_queue):
            self.__cancel_timeout_task(trial_timing)

    def __should_force_immediate_timeout_for_trial(self, trial_timing: dict | None) -> bool:
        if not isinstance(trial_timing, dict):
            return False
        if not self.__algorithm_disconnected_for_current_task:
            return False
        task_signature = trial_timing.get('task_signature')
        if task_signature is None:
            return False
        if self.__disconnected_task_signature is None:
            return True
        return task_signature == self.__disconnected_task_signature

    async def __force_timeout_for_disconnected_trial(self, trial_end_position: int) -> None:
        trial_timing = self.__pending_trial_timing_by_end_position.get(int(trial_end_position))
        if not isinstance(trial_timing, dict):
            return
        self.__cancel_timeout_task(
            trial_timing,
            cancel_reason="forced_timeout_after_disconnect",
        )
        self.__logger.warning(
            "检测到当前 task 已断流，后续 trial 直接按 timeout 处理，不再继续等待常规 1s 截止: "
            "subject_id=%s exp_name=%s exp_task=%s session_id=%s block_id=%s trial_id=%s "
            "trial_end_position=%s disconnect_task_signature=%s",
            trial_timing.get('subject_id'),
            trial_timing.get('exp_name'),
            trial_timing.get('exp_task'),
            trial_timing.get('session_id'),
            trial_timing.get('block_id'),
            trial_timing.get('trial_id'),
            trial_timing.get('trial_end_position'),
            self.__disconnected_task_signature,
        )
        await self.__handle_trial_timeout(int(trial_end_position))

    def __cancel_pending_timeout_tasks_for_task_signature(
        self,
        task_signature: tuple[str, str, str] | None,
        *,
        cancel_reason: str,
    ) -> None:
        if task_signature is None:
            return
        for trial_timing in list(self.__pending_trial_timing_queue):
            if trial_timing.get('task_signature') != task_signature:
                continue
            self.__cancel_timeout_task(
                trial_timing,
                cancel_reason=cancel_reason,
            )

    def __drop_pending_trials_for_task_signature(
        self,
        task_signature: tuple[str, str, str] | None,
        *,
        drop_reason: str,
    ) -> int:
        if task_signature is None:
            return 0
        dropped_count = 0
        for trial_timing in list(self.__pending_trial_timing_queue):
            if trial_timing.get('task_signature') != task_signature:
                continue
            trial_end_position = trial_timing.get('trial_end_position')
            if trial_end_position is None:
                continue
            self.__remove_pending_trial_timing_by_end_position(
                int(trial_end_position),
                remove_reason=drop_reason,
            )
            dropped_count += 1
        if dropped_count > 0:
            self.__logger.warning(
                "已清理旧 task 残留 pending trial，避免跨 task timeout 串扰: task_signature=%s drop_reason=%s dropped_count=%s",
                task_signature,
                drop_reason,
                dropped_count,
            )
        return dropped_count

    async def __force_timeout_pending_trials_for_task_signature(
        self,
        task_signature: tuple[str, str, str] | None,
        *,
        reason: str,
    ) -> int:
        if task_signature is None:
            return 0
        scored_count = 0
        for trial_timing in list(self.__pending_trial_timing_queue):
            if trial_timing.get('task_signature') != task_signature:
                continue
            trial_end_position = trial_timing.get('trial_end_position')
            if trial_end_position is None:
                continue
            self.__logger.warning(
                "旧 task 残留 pending trial 将在切换 task 前按 timeout 结算: "
                "task_signature=%s trial_end_position=%s reason=%s",
                task_signature,
                trial_end_position,
                reason,
            )
            handled = await self.__handle_trial_timeout(
                int(trial_end_position),
                allow_stale_task_signature=True,
            )
            if handled:
                scored_count += 1
        return scored_count

    async def __handle_trial_timeout(
        self,
        trial_end_position: int,
        *,
        allow_stale_task_signature: bool = False,
    ) -> bool:
        try:
            matched_trial_timing = self.__remove_pending_trial_timing_by_end_position(
                trial_end_position,
                remove_reason="timeout_fired",
            )
            if matched_trial_timing is None:
                self.__logger.debug(
                    "timeout 任务结束但未找到待处理trial，可能已被正常结果消费: trial_end_position=%s",
                    trial_end_position,
                )
                return False
            task_signature = matched_trial_timing.get('task_signature')
            current_task_signature = self.__build_task_signature()
            if (
                not allow_stale_task_signature
                and
                task_signature is not None
                and current_task_signature is not None
                and task_signature != current_task_signature
            ):
                self.__logger.warning(
                    "检测到过期 task 的 timeout 任务，已丢弃避免跨 task 记分: trial_end_position=%s "
                    "timeout_task_signature=%s current_task_signature=%s",
                    trial_end_position,
                    task_signature,
                    current_task_signature,
                )
                return False

            forced_immediate_timeout = self.__should_force_immediate_timeout_for_trial(matched_trial_timing)
            matched_trial_timing['forced_immediate_timeout'] = forced_immediate_timeout
            self.__logger.warning(
                "开始处理 trial timeout: subject_id=%s exp_name=%s exp_task=%s session_id=%s "
                "block_id=%s trial_id=%s trial_end_position=%s pending_trial_count_after_pop=%s "
                "queue_wait_ms=%.3f ready_processing_ms=%.3f timeout_age_ms=%.3f buffered_result_exists=%s "
                "forced_immediate_timeout=%s",
                matched_trial_timing.get('subject_id'),
                matched_trial_timing.get('exp_name'),
                matched_trial_timing.get('exp_task'),
                matched_trial_timing.get('session_id'),
                matched_trial_timing.get('block_id'),
                matched_trial_timing.get('trial_id'),
                matched_trial_timing.get('trial_end_position'),
                len(self.__pending_trial_timing_queue),
                float(matched_trial_timing.get('trial_end_message_queue_wait_ms') or 0.0),
                float(matched_trial_timing.get('trial_ready_processing_ms') or 0.0),
                max(
                    0.0,
                    (
                        time.time()
                        - float(matched_trial_timing.get('trial_ready_wallclock') or time.time())
                    ) * 1000.0,
                ),
                int(trial_end_position) in self.__buffered_result_by_end_position,
                forced_immediate_timeout,
            )
            self.__timed_out_trial_end_position_set.add(int(trial_end_position))
            timeout_context = self.__build_timeout_context(matched_trial_timing)
            await self.__current_challenge.receive_timeout_trial(timeout_context)
            self.__logger.info(
                "trial timeout 记分完成，准备发送 timeout 终态事件: subject_id=%s exp_name=%s exp_task=%s "
                "session_id=%s block_id=%s trial_id=%s",
                matched_trial_timing.get('subject_id'),
                matched_trial_timing.get('exp_name'),
                matched_trial_timing.get('exp_task'),
                matched_trial_timing.get('session_id'),
                matched_trial_timing.get('block_id'),
                matched_trial_timing.get('trial_id'),
            )
            timeout_terminal_sent = await self.__emit_trial_terminal_event(
                terminal_type='timeout',
                trial_context=matched_trial_timing,
            )
            self.__logger.info(
                "timeout 终态事件发送结果: subject_id=%s exp_name=%s exp_task=%s session_id=%s "
                "block_id=%s trial_id=%s sent=%s",
                matched_trial_timing.get('subject_id'),
                matched_trial_timing.get('exp_name'),
                matched_trial_timing.get('exp_task'),
                matched_trial_timing.get('session_id'),
                matched_trial_timing.get('block_id'),
                matched_trial_timing.get('trial_id'),
                timeout_terminal_sent,
            )
            await self.__enqueue_report_finalization(
                {
                    'finalize_type': 'timeout',
                    'matched_trial_timing': matched_trial_timing,
                    'timeout_context': timeout_context,
                }
            )
            await self.__maybe_finalize_disconnected_run_after_stream_end()
            return True
        except asyncio.CancelledError:
            self.__logger.info(
                "timeout 任务被取消: trial_end_position=%s",
                trial_end_position,
            )
            return False
        except Exception:
            self.__logger.exception(
                "handle trial timeout failed: trial_end_position=%s",
                trial_end_position,
            )
            return False

    async def __run_timeout_scheduler(self) -> None:
        while True:
            try:
                next_trial_end_position, sleep_seconds = self.__select_next_timeout_candidate()
                observed_revision = self.__timeout_scheduler_revision
                if next_trial_end_position is None:
                    self.__timeout_scheduler_wakeup_event.clear()
                    if observed_revision != self.__timeout_scheduler_revision:
                        continue
                    await self.__timeout_scheduler_wakeup_event.wait()
                    continue
                if sleep_seconds > 0:
                    self.__timeout_scheduler_wakeup_event.clear()
                    if observed_revision != self.__timeout_scheduler_revision:
                        continue
                    try:
                        await asyncio.wait_for(
                            self.__timeout_scheduler_wakeup_event.wait(),
                            timeout=sleep_seconds,
                        )
                        continue
                    except asyncio.TimeoutError:
                        pass
                await self.__handle_trial_timeout(next_trial_end_position)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.__logger.exception("timeout 调度器执行失败")
                await asyncio.sleep(0.05)

    def __select_next_timeout_candidate(self) -> tuple[int | None, float]:
        if self.__timeout_limit_seconds <= 0:
            return None, 0.0
        now_wallclock = time.time()
        next_trial_end_position = None
        next_sleep_seconds = 0.0
        for trial_timing in self.__pending_trial_timing_queue:
            trial_end_position = trial_timing.get('trial_end_position')
            deadline_wallclock = trial_timing.get('timeout_deadline_wallclock')
            if trial_end_position is None or deadline_wallclock is None:
                continue
            sleep_seconds = float(deadline_wallclock) - now_wallclock
            if next_trial_end_position is None or sleep_seconds < next_sleep_seconds:
                next_trial_end_position = int(trial_end_position)
                next_sleep_seconds = sleep_seconds
        return next_trial_end_position, max(0.0, next_sleep_seconds)

    def __build_timeout_context(self, matched_trial_timing: dict) -> dict:
        forced_immediate_timeout = bool(matched_trial_timing.get('forced_immediate_timeout', False))
        timeout_runtime_ms = (
            0.0 if forced_immediate_timeout else self.__timeout_limit_seconds * 1000.0
        )
        timeout_report_receive_wallclock = (
            time.time()
            if forced_immediate_timeout else
            matched_trial_timing.get('trial_ready_wallclock', time.time()) + self.__timeout_limit_seconds
        )
        timeout_context = {
            'predict_label': self.__timeout_predict_label,
            'platform_subject_id': matched_trial_timing.get('subject_id'),
            'platform_exp_name': matched_trial_timing.get('exp_name'),
            'platform_exp_task': matched_trial_timing.get('exp_task'),
            'platform_session_id': matched_trial_timing.get('session_id'),
            'platform_block_id': matched_trial_timing.get('block_id'),
            'platform_trial_id': matched_trial_timing.get('trial_id'),
            'platform_trial_start_position': matched_trial_timing.get('trial_start_position'),
            'platform_trial_end_position': matched_trial_timing.get('trial_end_position'),
            'platform_trial_ready_wallclock': matched_trial_timing.get('trial_ready_wallclock'),
            'platform_report_receive_wallclock': timeout_report_receive_wallclock,
            'predict_time_ms': timeout_runtime_ms,
            'platform_timeout': True,
            'is_timeout': True,
            'platform_timeout_reason': (
                'algorithm_disconnected_for_current_task'
                if forced_immediate_timeout else
                'predict_timeout'
            ),
            'report_source_label': 'eeg_1',
            'report_source_position': matched_trial_timing.get('trial_end_position'),
            'report_source_information': [
                {
                    'source_label': 'eeg_1',
                    'position': matched_trial_timing.get('trial_end_position'),
                }
            ],
        }
        hidden_score_payload = self.__pop_hidden_score_payload(matched_trial_timing)
        if hidden_score_payload is not None:
            if hidden_score_payload.get('raw_trigger_value') is not None:
                timeout_context['platform_raw_trigger_value'] = hidden_score_payload.get('raw_trigger_value')
            if hidden_score_payload.get('true_label') is not None:
                timeout_context['platform_true_label'] = hidden_score_payload.get('true_label')

        return timeout_context

    def __enrich_result_package(self, result_package_model: ResultPackageModel, matched_trial_timing: dict) -> None:
        raw_result = result_package_model.result
        if isinstance(raw_result, str):
            try:
                payload = json.loads(raw_result)
                if not isinstance(payload, dict):
                    payload = {'predict_label': str(payload)}
            except json.JSONDecodeError:
                payload = {'predict_label': raw_result}
        else:
            payload = {'predict_label': raw_result}

        payload['platform_subject_id'] = matched_trial_timing.get('subject_id')
        payload['platform_exp_name'] = matched_trial_timing.get('exp_name')
        payload['platform_exp_task'] = matched_trial_timing.get('exp_task')
        payload['platform_session_id'] = matched_trial_timing.get('session_id')
        payload['platform_block_id'] = matched_trial_timing.get('block_id')
        payload['platform_trial_id'] = matched_trial_timing.get('trial_id')
        payload['platform_trial_start_position'] = matched_trial_timing.get('trial_start_position')
        payload['platform_trial_end_position'] = matched_trial_timing.get('trial_end_position')
        payload['platform_trial_ready_wallclock'] = matched_trial_timing.get('trial_ready_wallclock')
        payload['platform_report_receive_wallclock'] = matched_trial_timing.get('report_receive_wallclock')
        payload['predict_time_ms'] = matched_trial_timing.get('runtime_ms')

        hidden_score_payload = self.__pop_hidden_score_payload(matched_trial_timing)
        if hidden_score_payload is not None:
            if hidden_score_payload.get('raw_trigger_value') is not None:
                payload['platform_raw_trigger_value'] = hidden_score_payload.get('raw_trigger_value')
            if hidden_score_payload.get('true_label') is not None:
                payload['platform_true_label'] = hidden_score_payload.get('true_label')

        result_package_model.result = json.dumps(payload, ensure_ascii=False)

    def __is_private_score_source(self, source_label: str) -> bool:
        return source_label == self.__PRIVATE_SCORE_SOURCE_LABEL

    def __cache_hidden_score_message(self, algorithm_data_message_model: AlgorithmDataMessageModel) -> None:
        package = algorithm_data_message_model.package
        if not isinstance(package, DataPackageModel):
            self.__logger.warning("hidden_score 私有消息类型异常: %s", type(package))
            return

        payload = self.__parse_hidden_score_payload(package.data)
        if payload is None:
            self.__logger.warning("hidden_score 私有消息解析失败")
            return

        key = self.__build_hidden_score_key(
            payload.get('subject_id'),
            payload.get('exp_name'),
            payload.get('exp_task'),
            payload.get('session_id'),
            payload.get('block_id'),
            payload.get('trial_id'),
        )
        if key is None:
            self.__logger.warning("hidden_score 私有消息缺少关键字段: %s", payload)
            return

        self.__hidden_score_payload_dict[key] = {
            'raw_trigger_value': self.__normalize_raw_trigger_value(payload.get('raw_trigger_value')),
            'true_label': self.__normalize_binary_label(payload.get('true_label')),
        }

    def __pop_hidden_score_payload(self, matched_trial_timing: dict) -> dict | None:
        key = self.__build_hidden_score_key(
            matched_trial_timing.get('subject_id'),
            matched_trial_timing.get('exp_name'),
            matched_trial_timing.get('exp_task'),
            matched_trial_timing.get('session_id'),
            matched_trial_timing.get('block_id'),
            matched_trial_timing.get('trial_id'),
        )
        if key is None:
            return None
        return self.__hidden_score_payload_dict.pop(key, None)

    @staticmethod
    def __parse_hidden_score_payload(raw_data) -> dict | None:
        if isinstance(raw_data, (bytes, bytearray)):
            try:
                raw_data = raw_data.decode('utf-8')
            except UnicodeDecodeError:
                return None
        if not isinstance(raw_data, str):
            return None
        try:
            payload = json.loads(raw_data)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def __build_hidden_score_key(
        subject_id,
        exp_name,
        exp_task,
        session_id,
        block_id,
        trial_id,
    ) -> tuple[str, str, str, str, str, str] | None:
        value_list = [subject_id, exp_name, exp_task, session_id, block_id, trial_id]
        normalized_value_list = []
        for value in value_list:
            if value is None:
                return None
            value_text = str(value).strip()
            if value_text == "":
                return None
            normalized_value_list.append(value_text)
        return tuple(normalized_value_list)

    @staticmethod
    def __normalize_raw_trigger_value(raw_trigger_value) -> int | None:
        try:
            normalized_value = int(raw_trigger_value)
        except (TypeError, ValueError):
            return None
        if normalized_value in {1, 2, 3}:
            return normalized_value
        return None

    @staticmethod
    def __normalize_binary_label(true_label) -> str | None:
        if true_label is None:
            return None
        true_label_text = str(true_label).strip()
        if true_label_text in {'0', '1'}:
            return true_label_text
        return None

    # 修改原因：
    # startup() 里是通过 self.__validate_requested_calibration_trial_count(algorithm_config)
    # 调用这里的。原定义没有 @staticmethod 时，Python 会自动把 self 绑定成第一个参数，
    # 实际效果等价于 __validate_requested_calibration_trial_count(self, algorithm_config)，
    # 从而触发 "takes 1 positional argument but 2 were given"。
    # 这里显式声明为静态方法，说明它只依赖传入的 algorithm_config，不依赖实例状态。
    @staticmethod
    def __validate_requested_calibration_trial_count(algorithm_config: dict[str, Union[str, dict]]) -> int:
        requested_trial_count = algorithm_config.get('calibration_trials_per_class_requested', 10)
        try:
            requested_trial_count = int(requested_trial_count)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "算法配置 calibration_trials_per_class_requested 必须为 0~10 的整数"
            ) from exc

        if requested_trial_count < 0 or requested_trial_count > 10:
            raise ValueError(
                f"算法配置 calibration_trials_per_class_requested 超出允许范围 0~10: {requested_trial_count}"
            )
        return requested_trial_count

    async def __send_requested_calibration_trial_count_to_collector(
        self,
        requested_trial_count: int,
        allow_startup_failure: bool = False,
    ) -> None:
        # 这里是“算法申请校准trial数量 -> Collector 更新 team 配置”的唯一入口。
        # 消息里强制携带 team_id，避免 Collector 再通过 topic 推断队伍归属。
        custom_control_topic = self.__collector_custom_control_topic
        if custom_control_topic is None:
            self.__logger.warning("未配置 collector custom control topic，跳过校准trial申请下发")
            return

        await self._component_framework.bind_message(
            MessageBindingModel(
                message_key=self.__virtual_receiver_custom_control_message_key,
                topic=custom_control_topic,
            )
        )
        custom_control_message = VirtualReceiverCustomControlMessage_pb2(
            calibrationTrialCountControlMessage=CalibrationTrialCountControlMessage_pb2(
                teamId=self.__team_id or "",
                calibrationTrialCountPerClass=requested_trial_count
            )
        )
        try:
            await self._component_framework.send_message(
                self.__virtual_receiver_custom_control_message_key,
                custom_control_message.SerializeToString(),
            )
            self.__logger.info(
                "已向 collector 下发算法申请的每类校准trial数量: %s team_id=%s topic=%s",
                requested_trial_count,
                self.__team_id,
                custom_control_topic,
            )
        except Exception:
            if allow_startup_failure:
                # startup 阶段 Collector 可能刚完成进程启动但尚未稳定订阅 custom control topic。
                # 这种情况下，首次发送失败不应直接打断整轮任务；
                # 后续在收到 Collector 的 device_update 后，还会再补发一次同样的申请值。
                self.__logger.exception(
                    "startup 阶段首次发送校准trial申请失败，保留后续 device_update 补发: requested_trial_count=%s team_id=%s topic=%s",
                    requested_trial_count,
                    self.__team_id,
                    custom_control_topic,
                )
                return
            raise

    async def __maybe_resend_requested_calibration_trial_count_on_device_update(
        self,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> None:
        if self.__collector_calibration_trial_count_resend_done:
            return
        if self.__requested_calibration_trial_count is None:
            return
        if not isinstance(algorithm_data_message_model.package, DevicePackageModel):
            return
        if algorithm_data_message_model.source_label not in {
            self.__CALIBRATION_PRIVATE_SOURCE_LABEL,
            self.__ONLINE_SHARED_SOURCE_LABEL,
        }:
            return
        self.__collector_calibration_trial_count_resend_done = True
        self.__logger.info(
            "检测到 collector 已发送 device_update，补发一次校准trial申请: source_label=%s requested_trial_count=%s team_id=%s topic=%s",
            algorithm_data_message_model.source_label,
            self.__requested_calibration_trial_count,
            self.__team_id,
            self.__collector_custom_control_topic,
        )
        await self.__send_requested_calibration_trial_count_to_collector(
            self.__requested_calibration_trial_count
        )

    async def __load_component_runtime_routing(self) -> None:
        # 运行期确定性路由读取。
        # 这些字段来自 CentralController 生成的 component_info，是整个 runtime coordination 的前提。
        component_model = await self._component_framework.get_component_model()
        component_info = component_model.component_info or {}
        self.__team_id = component_info.get('team_id')
        self.__team_display_name = component_info.get('team_display_name') or self.__team_id
        self.__group_id = component_info.get('group_id')
        self.__processor_component_id = component_info.get('processor_component_id')
        self.__collector_component_id = component_info.get('collector_component_id')
        self.__collector_custom_control_topic = component_info.get('collector_custom_control_topic')
        self.__runtime_stage_event_topic = component_info.get('runtime_stage_event_topic')
        if self.__runtime_stage_event_topic is not None:
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=self.__RUNTIME_STAGE_EVENT_MESSAGE_KEY,
                    topic=self.__runtime_stage_event_topic,
                )
            )

    @staticmethod
    def __sanitize_algorithm_config(raw_algorithm_config: dict | None) -> dict[str, Union[str, dict]]:
        if not isinstance(raw_algorithm_config, dict):
            return {}

        sanitized_config: dict[str, Union[str, dict]] = {}
        calibration_trials_per_class_requested = raw_algorithm_config.get('calibration_trials_per_class_requested')
        if calibration_trials_per_class_requested is not None:
            sanitized_config['calibration_trials_per_class_requested'] = calibration_trials_per_class_requested
        platform_model_size_mb = raw_algorithm_config.get('platform_model_size_mb')
        if platform_model_size_mb not in (None, ''):
            try:
                sanitized_config['platform_model_size_mb'] = float(platform_model_size_mb)
            except (TypeError, ValueError):
                pass

        raw_requested_channel_labels = raw_algorithm_config.get('requested_channel_labels')
        normalized_requested_channel_labels: dict[str, list[str]] = {}
        if isinstance(raw_requested_channel_labels, dict):
            for source_label, channel_label_list in raw_requested_channel_labels.items():
                source_label_text = str(source_label or '').strip()
                if source_label_text == '' or not isinstance(channel_label_list, (list, tuple)):
                    continue
                if source_label_text not in BCICompetitionTaskFinal.__ALGORITHM_REQUIRED_SOURCE_LABEL_SET:
                    continue
                normalized_channel_label_list = []
                normalized_channel_label_key_set = set()
                for channel_label in channel_label_list:
                    channel_label_text = str(channel_label).strip()
                    if channel_label_text == '':
                        continue
                    normalized_channel_label_key = BCICompetitionTaskFinal.__normalize_channel_label_key(
                        channel_label_text
                    )
                    if normalized_channel_label_key == '':
                        continue
                    if normalized_channel_label_key in normalized_channel_label_key_set:
                        continue
                    normalized_channel_label_key_set.add(normalized_channel_label_key)
                    normalized_channel_label_list.append(channel_label_text)
                    if (
                        len(normalized_channel_label_list)
                        >= BCICompetitionTaskFinal.__MAX_REQUESTED_CHANNEL_COUNT_PER_SOURCE
                    ):
                        break
                if normalized_channel_label_list:
                    normalized_requested_channel_labels[source_label_text] = normalized_channel_label_list

        if normalized_requested_channel_labels:
            sanitized_config['requested_channel_labels'] = normalized_requested_channel_labels
            sanitized_config['requested_channel_count'] = sum(
                len(channel_label_list)
                for channel_label_list in normalized_requested_channel_labels.values()
            )
        return sanitized_config

    @staticmethod
    def __normalize_channel_label_key(channel_label: str) -> str:
        return ''.join(char for char in str(channel_label).upper() if char.isalnum())

    @classmethod
    def __normalize_algorithm_source_label(
        cls,
        algorithm_data_message_model: AlgorithmDataMessageModel,
    ) -> AlgorithmDataMessageModel:
        # Algorithm 侧仍然只关心统一 source_label='eeg_1'。
        # 因此 ProcessHub 在进入算法前，把：
        # - eeg_1_calibration_private
        # - eeg_1_online_shared
        # 统一归一化为 eeg_1。
        if algorithm_data_message_model.source_label not in {
            cls.__CALIBRATION_PRIVATE_SOURCE_LABEL,
            cls.__ONLINE_SHARED_SOURCE_LABEL,
        }:
            return algorithm_data_message_model
        return AlgorithmDataMessageModel(
            source_label=cls.__ALGORITHM_SOURCE_LABEL,
            timestamp_ms=algorithm_data_message_model.timestamp_ms,
            package=algorithm_data_message_model.package,
        )

    async def __prepare_messages_for_algorithm_forward(
        self,
        original_message_model: AlgorithmDataMessageModel,
        forwarded_message_model: AlgorithmDataMessageModel,
    ) -> list[AlgorithmDataMessageModel]:
        requested_channel_label_list = self.__requested_channel_labels_by_source.get(
            self.__ALGORITHM_SOURCE_LABEL
        ) or []
        package = forwarded_message_model.package
        is_calibration_private_message = (
            original_message_model.source_label == self.__CALIBRATION_PRIVATE_SOURCE_LABEL
        )
        calibration_delivery_id = self.__current_calibration_delivery_id
        if (
            is_calibration_private_message
            and calibration_delivery_id is not None
            and isinstance(package, DevicePackageModel)
        ):
            buffered_device_message = (
                self.__rewrite_forwarded_device_message(forwarded_message_model)
                if len(requested_channel_label_list) > 0
                else forwarded_message_model
            )
            self.__forward_calibration_device_message_by_delivery_id[calibration_delivery_id] = (
                buffered_device_message
            )
            self.__logger.debug(
                "缓存 calibration device，等待完整校准负载后再转发: team_id=%s delivery_id=%s",
                self.__team_id,
                calibration_delivery_id,
            )
            return []
        if (
            is_calibration_private_message
            and calibration_delivery_id is not None
            and isinstance(package, DataPackageModel)
            and isinstance(package.data, (bytes, bytearray))
        ):
            return await self.__rewrite_forwarded_calibration_chunk_messages(
                original_message_model=original_message_model,
                forwarded_message_model=forwarded_message_model,
            )
        if len(requested_channel_label_list) == 0:
            return [forwarded_message_model]

        if isinstance(package, DevicePackageModel):
            return [self.__rewrite_forwarded_device_message(forwarded_message_model)]
        if not isinstance(package, DataPackageModel):
            return [forwarded_message_model]
        if original_message_model.source_label == self.__ONLINE_SHARED_SOURCE_LABEL:
            return [self.__rewrite_forwarded_online_data_message(forwarded_message_model)]
        if (
            original_message_model.source_label == self.__CALIBRATION_PRIVATE_SOURCE_LABEL
            and isinstance(package.data, (bytes, bytearray))
        ):
            return await self.__rewrite_forwarded_calibration_chunk_messages(
                original_message_model=original_message_model,
                forwarded_message_model=forwarded_message_model,
            )
        return [forwarded_message_model]

    def __rewrite_forwarded_device_message(
        self,
        forwarded_message_model: AlgorithmDataMessageModel,
    ) -> AlgorithmDataMessageModel:
        package = forwarded_message_model.package
        source_label = forwarded_message_model.source_label
        incoming_channel_label_list = list(package.channel_label or [])
        incoming_channel_count = int(package.channel_number or len(incoming_channel_label_list) or 0)
        if incoming_channel_count <= 0:
            incoming_channel_count = len(incoming_channel_label_list)
        self.__incoming_device_channel_labels_by_source[source_label] = incoming_channel_label_list
        self.__incoming_device_channel_count_by_source[source_label] = incoming_channel_count

        forward_channel_index_list, forward_channel_label_list = self.__resolve_forward_channel_index_list(
            source_label=source_label,
            incoming_channel_label_list=incoming_channel_label_list,
        )
        if len(forward_channel_label_list) == 0:
            self.__forward_channel_index_by_source.pop(source_label, None)
            self.__forward_channel_labels_by_source.pop(source_label, None)
            return forwarded_message_model

        self.__forward_channel_index_by_source[source_label] = list(forward_channel_index_list)
        self.__forward_channel_labels_by_source[source_label] = list(forward_channel_label_list)
        rewritten_package = DevicePackageModel(
            data_type=package.data_type,
            channel_number=len(forward_channel_label_list),
            sample_rate=package.sample_rate,
            channel_label=list(forward_channel_label_list),
            other_information=dict(package.other_information or {}),
        )
        return AlgorithmDataMessageModel(
            source_label=forwarded_message_model.source_label,
            timestamp_ms=forwarded_message_model.timestamp_ms,
            package=rewritten_package,
        )

    def __rewrite_forwarded_online_data_message(
        self,
        forwarded_message_model: AlgorithmDataMessageModel,
    ) -> AlgorithmDataMessageModel:
        package = forwarded_message_model.package
        source_label = forwarded_message_model.source_label
        forward_channel_index_list = self.__forward_channel_index_by_source.get(source_label)
        incoming_channel_count = int(self.__incoming_device_channel_count_by_source.get(source_label) or 0)
        if (
            not isinstance(package.data, (list, tuple, np.ndarray))
            or not forward_channel_index_list
            or incoming_channel_count <= 0
        ):
            return forwarded_message_model

        online_data_array = np.asarray(package.data, dtype=np.float32)
        if online_data_array.ndim != 1 or online_data_array.size == 0:
            return forwarded_message_model
        if online_data_array.size % incoming_channel_count != 0:
            self.__logger.warning(
                "在线数据长度与设备通道数不匹配，跳过转发侧通道裁剪: source_label=%s data_size=%s incoming_channel_count=%s",
                source_label,
                online_data_array.size,
                incoming_channel_count,
            )
            return forwarded_message_model

        sample_count = online_data_array.size // incoming_channel_count
        online_data_matrix = online_data_array.reshape(sample_count, incoming_channel_count)
        selected_data_matrix = online_data_matrix[:, forward_channel_index_list]
        rewritten_package = DataPackageModel(
            data_position=package.data_position,
            data=selected_data_matrix.reshape(-1).astype(np.float32).tolist(),
        )
        return AlgorithmDataMessageModel(
            source_label=forwarded_message_model.source_label,
            timestamp_ms=forwarded_message_model.timestamp_ms,
            package=rewritten_package,
        )

    async def __rewrite_forwarded_calibration_chunk_messages(
        self,
        original_message_model: AlgorithmDataMessageModel,
        forwarded_message_model: AlgorithmDataMessageModel,
    ) -> list[AlgorithmDataMessageModel]:
        package = forwarded_message_model.package
        calibration_chunk_bytes = bytes(package.data)
        calibration_chunk_header = self.__parse_calibration_chunk_header(calibration_chunk_bytes)
        if calibration_chunk_header is None:
            self.__logger.warning(
                "收到无法识别的 calibration chunk，按原样转发: team_id=%s source_label=%s bytes=%s",
                self.__team_id,
                original_message_model.source_label,
                len(calibration_chunk_bytes),
            )
            return [forwarded_message_model]

        calibration_delivery_id = self.__current_calibration_delivery_id
        if (
            calibration_delivery_id is not None
            and calibration_delivery_id in self.__completed_calibration_delivery_id_set
        ):
            return []
        buffer_key = self.__build_forward_calibration_buffer_key(original_message_model)
        chunk_buffer = self.__forward_calibration_chunk_buffer_by_key.get(buffer_key)
        if chunk_buffer is None:
            chunk_buffer = {
                'total_chunk_number': calibration_chunk_header['total_chunk_number'],
                'total_payload_size': calibration_chunk_header['total_payload_size'],
                'chunk_payload_dict': {},
                'first_timestamp_ms': forwarded_message_model.timestamp_ms,
                'last_timestamp_ms': forwarded_message_model.timestamp_ms,
                'data_position': package.data_position,
            }
            self.__forward_calibration_chunk_buffer_by_key[buffer_key] = chunk_buffer
        chunk_buffer['last_timestamp_ms'] = forwarded_message_model.timestamp_ms
        chunk_buffer['chunk_payload_dict'][calibration_chunk_header['chunk_index']] = (
            calibration_chunk_header['chunk_payload']
        )

        if len(chunk_buffer['chunk_payload_dict']) < chunk_buffer['total_chunk_number']:
            return []

        ordered_chunk_payload_list = [
            chunk_buffer['chunk_payload_dict'][chunk_index]
            for chunk_index in range(chunk_buffer['total_chunk_number'])
        ]
        calibration_payload = b''.join(ordered_chunk_payload_list)
        calibration_payload = calibration_payload[:chunk_buffer['total_payload_size']]
        self.__forward_calibration_chunk_buffer_by_key.pop(buffer_key, None)
        filtered_payload = self.__filter_calibration_payload_bytes(
            source_label=forwarded_message_model.source_label,
            calibration_payload=calibration_payload,
        )
        filtered_chunk_list = self.__split_calibration_payload_to_chunk_list(filtered_payload)
        forwarded_message_list = [
            AlgorithmDataMessageModel(
                source_label=forwarded_message_model.source_label,
                timestamp_ms=chunk_buffer['last_timestamp_ms'],
                package=DataPackageModel(
                    data_position=chunk_buffer['data_position'],
                    data=filtered_chunk_bytes,
                ),
            )
            for filtered_chunk_bytes in filtered_chunk_list
        ]
        if calibration_delivery_id is not None:
            buffered_device_message = self.__forward_calibration_device_message_by_delivery_id.pop(
                calibration_delivery_id,
                None,
            )
            if buffered_device_message is not None:
                forwarded_message_list.insert(0, buffered_device_message)
            else:
                self.__logger.warning(
                    "校准负载已收齐但缺少对应 device，继续按兼容模式转发 chunks: team_id=%s delivery_id=%s",
                    self.__team_id,
                    calibration_delivery_id,
                )
            setattr(
                forwarded_message_list[-1],
                '_calibration_delivery_id_to_complete',
                calibration_delivery_id,
            )
        return forwarded_message_list

    def __resolve_forward_channel_index_list(
        self,
        source_label: str,
        incoming_channel_label_list: list[str],
    ) -> tuple[list[int], list[str]]:
        requested_channel_label_list = self.__requested_channel_labels_by_source.get(source_label) or []
        if len(requested_channel_label_list) == 0:
            return [], []

        normalized_incoming_label_to_index_dict = {}
        for channel_index, channel_label in enumerate(incoming_channel_label_list):
            normalized_channel_label = self.__normalize_channel_label_key(channel_label)
            if normalized_channel_label == '' or normalized_channel_label in normalized_incoming_label_to_index_dict:
                continue
            normalized_incoming_label_to_index_dict[normalized_channel_label] = channel_index

        forward_channel_index_list: list[int] = []
        forward_channel_label_list: list[str] = []
        missing_channel_label_list: list[str] = []
        for requested_channel_label in requested_channel_label_list:
            normalized_channel_label = self.__normalize_channel_label_key(requested_channel_label)
            if normalized_channel_label not in normalized_incoming_label_to_index_dict:
                missing_channel_label_list.append(str(requested_channel_label))
                continue
            forward_channel_index_list.append(normalized_incoming_label_to_index_dict[normalized_channel_label])
            forward_channel_label_list.append(str(requested_channel_label))

        if missing_channel_label_list:
            raise ValueError(
                f"算法声明的请求通道在裁判机设备映射中不存在: source_label={source_label}, "
                f"missing_channel_labels={missing_channel_label_list}, "
                f"available_channel_labels={incoming_channel_label_list}"
            )
        return forward_channel_index_list, forward_channel_label_list

    def __filter_calibration_payload_bytes(
        self,
        source_label: str,
        calibration_payload: bytes,
    ) -> bytes:
        forward_channel_index_list = self.__forward_channel_index_by_source.get(source_label)
        if not forward_channel_index_list:
            return calibration_payload

        with np.load(io.BytesIO(calibration_payload), allow_pickle=False) as npz_payload:
            calibration_payload_dict = {
                payload_key: npz_payload[payload_key]
                for payload_key in npz_payload.files
            }
        calibration_data = calibration_payload_dict.get('data')
        if isinstance(calibration_data, np.ndarray) and calibration_data.ndim == 3:
            calibration_payload_dict['data'] = calibration_data[:, forward_channel_index_list, :]
        output_buffer = io.BytesIO()
        np.savez_compressed(output_buffer, **calibration_payload_dict)
        return output_buffer.getvalue()

    def __parse_calibration_chunk_header(self, calibration_chunk_bytes: bytes) -> dict | None:
        if len(calibration_chunk_bytes) < self.__CALIBRATION_CHUNK_HEADER_SIZE:
            return None
        chunk_magic, total_chunk_number, chunk_index, total_payload_size = struct.unpack(
            self.__CALIBRATION_CHUNK_HEADER_FORMAT,
            calibration_chunk_bytes[:self.__CALIBRATION_CHUNK_HEADER_SIZE],
        )
        if chunk_magic != self.__CALIBRATION_CHUNK_MAGIC:
            return None
        return {
            'total_chunk_number': int(total_chunk_number),
            'chunk_index': int(chunk_index),
            'total_payload_size': int(total_payload_size),
            'chunk_payload': calibration_chunk_bytes[self.__CALIBRATION_CHUNK_HEADER_SIZE:],
        }

    def __build_forward_calibration_buffer_key(
        self,
        original_message_model: AlgorithmDataMessageModel,
    ) -> tuple[str, str, str, str, str, str]:
        return (
            str(original_message_model.source_label or ''),
            str(self.__current_subject_id or ''),
            str(self.__current_exp_name or ''),
            str(self.__current_exp_task or ''),
            str(self.__current_session_id or ''),
            str(self.__current_calibration_delivery_id or ''),
        )

    def __should_drop_completed_calibration_delivery_message(
        self,
        message_model: AlgorithmDataMessageModel,
    ) -> bool:
        if message_model.source_label != self.__CALIBRATION_PRIVATE_SOURCE_LABEL:
            return False
        package = message_model.package
        if isinstance(package, DevicePackageModel):
            delivery_id = str(
                (package.other_information or {}).get('calibration_delivery_id') or ''
            ).strip()
            self.__current_calibration_delivery_id = delivery_id or None
        else:
            delivery_id = self.__current_calibration_delivery_id
        if (
            delivery_id is None
            or delivery_id not in self.__completed_calibration_delivery_id_set
        ):
            return False
        self.__logger.info(
            "忽略 Kafka 重放的已完成校准消息: team_id=%s delivery_id=%s package_type=%s",
            self.__team_id,
            delivery_id,
            type(package).__name__,
        )
        return True

    def __mark_calibration_delivery_forwarded(self, delivery_id: str) -> None:
        delivery_id_text = str(delivery_id or '').strip()
        if delivery_id_text == '':
            return
        self.__completed_calibration_delivery_id_set.add(delivery_id_text)
        self.__logger.info(
            "校准负载已完整转发给算法: team_id=%s delivery_id=%s completed_delivery_count=%s",
            self.__team_id,
            delivery_id_text,
            len(self.__completed_calibration_delivery_id_set),
        )

    @classmethod
    def __split_calibration_payload_to_chunk_list(cls, calibration_payload: bytes) -> list[bytes]:
        payload_bytes = bytes(calibration_payload or b'')
        payload_size = len(payload_bytes)
        if payload_size == 0:
            chunk_payload_size = 0
            total_chunk_number = 1
        else:
            chunk_payload_size = cls.__MAX_CALIBRATION_CHUNK_BYTES
            total_chunk_number = (payload_size + chunk_payload_size - 1) // chunk_payload_size

        calibration_chunk_list: list[bytes] = []
        for chunk_index in range(total_chunk_number):
            chunk_start = chunk_index * chunk_payload_size
            chunk_end = chunk_start + chunk_payload_size
            chunk_payload = payload_bytes[chunk_start:chunk_end]
            chunk_header = struct.pack(
                cls.__CALIBRATION_CHUNK_HEADER_FORMAT,
                cls.__CALIBRATION_CHUNK_MAGIC,
                total_chunk_number,
                chunk_index,
                payload_size,
            )
            calibration_chunk_list.append(chunk_header + chunk_payload)
        return calibration_chunk_list

    async def __handle_algorithm_runtime_event(
        self,
        algorithm_report_message_model: AlgorithmReportMessageModel,
    ) -> None:
        # 算法侧通过 AlgorithmReportMessage(DataPackage) 上报运行时事件。
        # 当前只消费 calibration_ready，并把它升级为带 team/group/collector 信息的统一 runtime event。
        runtime_event_payload = self.__parse_json_payload(
            algorithm_report_message_model.package.data
        )
        if not isinstance(runtime_event_payload, dict):
            self.__logger.warning("收到无法解析的算法运行时事件: %s", algorithm_report_message_model.package.data)
            return
        if runtime_event_payload.get('event_type') != self.__CALIBRATION_READY_EVENT_TYPE:
            self.__logger.debug("忽略未识别的算法运行时事件: %s", runtime_event_payload)
            return
        calibration_ready = bool(runtime_event_payload.get('calibration_ready'))
        stage_context = runtime_event_payload.get('stage_context') or {}
        ready_stage_signature = self.__build_stage_signature(
            subject_id=stage_context.get('subject_id'),
            exp_name=stage_context.get('exp_name'),
            exp_task=stage_context.get('exp_task'),
            session_id=stage_context.get('session_id'),
        )
        if calibration_ready and ready_stage_signature is not None:
            self.__calibration_ready_stage_signature_set.add(ready_stage_signature)
        self.__publish_team_live_status(
            calibration_status='ready' if calibration_ready else 'pending',
            calibration_ready=calibration_ready,
            calibration_ready_wallclock=time.time(),
            subject_id=(runtime_event_payload.get('stage_context') or {}).get('subject_id'),
            exp_name=(runtime_event_payload.get('stage_context') or {}).get('exp_name'),
            exp_task=(runtime_event_payload.get('stage_context') or {}).get('exp_task'),
            session_id=(runtime_event_payload.get('stage_context') or {}).get('session_id'),
        )
        await self.__send_runtime_stage_event(
            {
                'event_type': 'team_calibration_ready',
                'team_id': self.__team_id,
                'group_id': self.__group_id,
                'processor_component_id': self.__processor_component_id,
                'collector_component_id': self.__collector_component_id,
                'stage_context': runtime_event_payload.get('stage_context'),
                'calibration_ready': calibration_ready,
                'algorithm_report_timestamp_ms': algorithm_report_message_model.timestamp_ms,
            }
        )

    async def __emit_team_calibration_forfeited_event(
        self,
        *,
        disconnect_reason: str,
        algorithm_address: str | None,
    ) -> bool:
        stage_context = {
            'subject_id': self.__current_subject_id,
            'exp_name': self.__current_exp_name,
            'exp_task': self.__current_exp_task,
            'session_id': self.__current_session_id,
        }
        if any(value is None or str(value).strip() == '' for value in stage_context.values()):
            self.__logger.warning(
                "校准阶段算法掉线但阶段信息不完整，无法发送 forfeited 事件: "
                "team_id=%s stage_context=%s disconnect_reason=%s",
                self.__team_id,
                stage_context,
                disconnect_reason,
            )
            return False
        stage_signature = self.__build_stage_signature(**stage_context)
        if stage_signature is None:
            return False
        if stage_signature in self.__calibration_forfeit_event_sent_stage_signature_set:
            return True
        event_sent = await self.__send_runtime_stage_event(
            {
                'event_type': 'team_calibration_forfeited',
                'team_id': self.__team_id,
                'group_id': self.__group_id,
                'processor_component_id': self.__processor_component_id,
                'collector_component_id': self.__collector_component_id,
                'stage_context': stage_context,
                'disconnect_reason': disconnect_reason,
                'algorithm_address': algorithm_address,
                'forfeited_at': time.time(),
            }
        )
        if event_sent:
            self.__calibration_forfeit_event_sent_stage_signature_set.add(stage_signature)
        return event_sent

    async def __emit_trial_terminal_event(
        self,
        terminal_type: str,
        trial_context: dict,
    ) -> bool:
        # trial_terminal 是多队同步的核心事件。
        # 无论该队是“正常返回结果”还是“平台侧判超时”，最终都会走这里通知协调器，
        # 让协调器判断是否可以自动放行下一个 shared online trial。
        return await self.__send_runtime_stage_event(
            {
                'event_type': 'team_trial_terminal',
                'terminal_type': terminal_type,
                'team_id': self.__team_id,
                'group_id': self.__group_id,
                'processor_component_id': self.__processor_component_id,
                'collector_component_id': self.__collector_component_id,
                'stage_context': {
                    'subject_id': trial_context.get('subject_id'),
                    'exp_name': trial_context.get('exp_name'),
                    'exp_task': trial_context.get('exp_task'),
                    'session_id': trial_context.get('session_id'),
                },
                'trial_context': {
                    'block_id': trial_context.get('block_id'),
                    'trial_id': trial_context.get('trial_id'),
                    'trial_start_position': trial_context.get('trial_start_position'),
                    'trial_end_position': trial_context.get('trial_end_position'),
                },
            }
        )

    async def __send_runtime_stage_event(self, payload: dict) -> bool:
        if self.__runtime_stage_event_topic is None:
            self.__logger.warning("未配置 runtime stage event topic，跳过发送: %s", payload)
            return False

        runtime_stage_event_payload = dict(payload)
        runtime_stage_event_payload.setdefault('event_id', str(uuid.uuid4()))
        event_type = runtime_stage_event_payload.get('event_type')
        terminal_type = runtime_stage_event_payload.get('terminal_type')
        team_id = runtime_stage_event_payload.get('team_id')
        group_id = runtime_stage_event_payload.get('group_id')
        stage_context = runtime_stage_event_payload.get('stage_context')
        trial_context = runtime_stage_event_payload.get('trial_context')
        serialized_message = CommonMessageConverter.model_to_protobuf(
            DataMessageModel(
                package=DataPackageModel(
                    data_position=0.0,
                    data=json.dumps(runtime_stage_event_payload, ensure_ascii=False),
                )
            )
        ).SerializeToString()

        try:
            self.__logger.debug(
                "发送 runtime stage event: event_id=%s event_type=%s terminal_type=%s "
                "team_id=%s group_id=%s stage_context=%s trial_context=%s",
                runtime_stage_event_payload.get('event_id'),
                event_type,
                terminal_type,
                team_id,
                group_id,
                stage_context,
                trial_context,
            )
            await self._component_framework.send_message(
                self.__RUNTIME_STAGE_EVENT_MESSAGE_KEY,
                serialized_message,
            )
            self.__logger.debug(
                "runtime stage event 发送成功: event_id=%s event_type=%s terminal_type=%s "
                "team_id=%s group_id=%s",
                runtime_stage_event_payload.get('event_id'),
                event_type,
                terminal_type,
                team_id,
                group_id,
            )
            return True
        except Exception:
            self.__logger.exception(
                "runtime stage event 发送失败: event_id=%s event_type=%s terminal_type=%s "
                "team_id=%s group_id=%s",
                runtime_stage_event_payload.get('event_id'),
                event_type,
                terminal_type,
                team_id,
                group_id,
            )
            return False

    async def __emit_team_run_finalized_event(self, terminal_run_status: str) -> bool:
        terminal_run_status_text = str(terminal_run_status or '').strip().lower()
        if terminal_run_status_text not in self.__TERMINAL_RUN_STATUS_SET:
            return False
        if self.__terminal_run_event_sent:
            return True
        self.__terminal_run_event_sent = True
        return await self.__send_runtime_stage_event(
            {
                'event_type': 'team_run_finalized',
                'team_id': self.__team_id,
                'group_id': self.__group_id,
                'processor_component_id': self.__processor_component_id,
                'collector_component_id': self.__collector_component_id,
                'stage_context': {
                    'subject_id': self.__current_subject_id or '',
                    'exp_name': self.__current_exp_name or '',
                    'exp_task': self.__current_exp_task or '',
                    'session_id': self.__current_session_id or '',
                },
                'terminal_run_status': terminal_run_status_text,
                'finalized_wallclock': time.time(),
            }
        )

    @staticmethod
    def __parse_json_payload(raw_data):
        if isinstance(raw_data, (bytes, bytearray)):
            try:
                raw_data = raw_data.decode('utf-8')
            except UnicodeDecodeError:
                return None
        if not isinstance(raw_data, str):
            return None
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return None

    async def __refresh_team_live_score_fields(self) -> None:
        if self.__current_challenge is None or self.__team_id is None:
            return
        try:
            score_package_model_list = await self.__current_challenge.get_score()
        except Exception:
            self.__logger.exception("刷新赛队实时分数失败: team_id=%s", self.__team_id)
            return

        latest_score_package = score_package_model_list[-1] if score_package_model_list else None
        current_total_score = 0.0
        if latest_score_package is not None and latest_score_package.score is not None:
            current_total_score = float(latest_score_package.score)
        if isinstance(self.__final_score_result, dict):
            current_total_score = float(self.__final_score_result.get('total_score', current_total_score) or 0.0)

        live_task_metrics = {}
        get_live_task_metrics = getattr(self.__current_challenge, 'get_live_task_metrics', None)
        if callable(get_live_task_metrics):
            try:
                live_task_metrics = dict(get_live_task_metrics() or {})
            except Exception:
                self.__logger.exception('读取当前任务实时指标失败: team_id=%s', self.__team_id)
                live_task_metrics = {}
        current_task_score = self.__safe_float(live_task_metrics.get('current_task_score'))
        current_task_accuracy_percent = self.__safe_float(live_task_metrics.get('current_task_accuracy_percent'))
        current_trial_score = self.__safe_float(live_task_metrics.get('current_trial_score'))
        judge_message = live_task_metrics.get('judge_message')
        is_invalid_output = bool(live_task_metrics.get('is_invalid_output'))

        self.__publish_team_live_status(
            current_total_score=current_total_score,
            current_trial_score=current_trial_score,
            current_task_score=current_task_score,
            current_task_accuracy_percent=current_task_accuracy_percent,
            judge_message=judge_message,
            is_invalid_output=is_invalid_output,
            observed_trial_count=len(score_package_model_list),
            latest_score_trial_id=latest_score_package.trial_id if latest_score_package is not None else None,
            latest_score_block_id=latest_score_package.block_id if latest_score_package is not None else None,
            latest_score_subject_id=latest_score_package.subject_id if latest_score_package is not None else None,
        )

    def __publish_team_live_status(self, **updates) -> None:
        if self.__team_id is None:
            return
        payload = dict(self.__team_live_status_payload or {})
        if not payload:
            payload = {
                'team_id': self.__team_id,
                'team_display_name': self.__team_display_name or self.__team_id,
                'group_id': self.__group_id,
                'processor_component_id': self.__processor_component_id,
                'collector_component_id': self.__collector_component_id,
                'run_status': 'idle',
                'connection_status': 'disconnected',
                'calibration_status': 'pending',
                'calibration_ready': False,
                'current_total_score': 0.0,
                'current_trial_score': 0.0,
                'current_task_score': 0.0,
                'current_task_accuracy_percent': 0.0,
                'observed_trial_count': 0,
                'predict_label': None,
                'true_label': None,
                'predict_time_ms': None,
                'is_timeout': None,
                'is_invalid_output': False,
                'judge_message': None,
                'last_terminal_type': None,
                'last_terminal_wallclock': None,
                'last_disconnect_at': None,
                'last_disconnect_reason': None,
                'recovery_advice': None,
                'forfeit_current_task': False,
                'forfeit_task_signature': None,
                'reconnected_at': None,
            }

        payload.update(
            {
                'team_id': self.__team_id,
                'team_display_name': self.__team_display_name or self.__team_id,
                'group_id': self.__group_id,
                'processor_component_id': self.__processor_component_id,
                'collector_component_id': self.__collector_component_id,
                'service_status': self.__task_status.name.lower() if self.__task_status is not None else None,
            }
        )
        previous_run_status = str(payload.get('run_status') or '').strip().lower()
        incoming_run_status = updates.get('run_status')
        incoming_run_status_text = str(incoming_run_status or '').strip().lower()
        if (
            previous_run_status in self.__TERMINAL_RUN_STATUS_SET
            and incoming_run_status_text
            and incoming_run_status_text not in self.__TERMINAL_RUN_STATUS_SET
        ):
            updates = dict(updates)
            updates.pop('run_status', None)
            self.__logger.warning(
                "忽略终态后的非终态 run_status 回退: team_id=%s previous_run_status=%s incoming_run_status=%s",
                self.__team_id,
                previous_run_status,
                incoming_run_status_text,
            )
        payload.update(updates)
        payload['updated_at'] = time.time()
        self.__team_live_status_payload = payload
        self.__write_live_state_json(Path('teams') / f'{self.__team_id}.json', payload)

    @staticmethod
    def __extract_predict_label_from_result_package(result_package_model: ResultPackageModel):
        payload = BCICompetitionTaskFinal.__parse_json_payload(result_package_model.result)
        if isinstance(payload, dict):
            predict_label = payload.get('predict_label')
            return None if predict_label is None else str(predict_label)
        raw_result = result_package_model.result
        return None if raw_result is None else str(raw_result)

    @staticmethod
    def __resolve_live_state_root_dir() -> Path:
        return Path(__file__).resolve().parents[5] / 'results' / 'live'

    def __write_live_state_json(self, relative_file_path: Path | str, payload: dict) -> None:
        relative_path = Path(relative_file_path)
        if relative_path.parent == Path('teams') and relative_path.suffix == '.json':
            state_key = f'{TEAM_STATE_KEY_PREFIX}{relative_path.stem}'
            try:
                write_json_state(
                    resolve_runtime_state_db_path(PROJECT_ROOT),
                    state_key,
                    payload,
                )
            except OSError:
                self.__logger.exception(
                    "写入 SQLite 赛队 live 状态失败: state_key=%s",
                    state_key,
                )
