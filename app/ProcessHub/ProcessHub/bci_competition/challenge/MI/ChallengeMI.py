import copy
import csv
from datetime import datetime
import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from statistics import mean, pstdev
from typing import Optional, Union
import logging
import logging.config
import numpy as np
import yaml
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel, AlgorithmReportMessageModel
from Common.model.CommonMessageModel import (
    ControlPackageModel,
    DataPackageModel,
    DevicePackageModel,
    EventPackageModel,
    InformationPackageModel,
    ReportSourceInformationModel,
    ResultPackageModel,
    ScorePackageModel,
)
from ProcessHub.bci_competition.challenge.interface.ChallengeInterface import ChallengeInterface
from ProcessHub.common.utils.seed import DEFAULT_GLOBAL_SEED
from ProcessHub.orchestrator.model.SourceModel import SourceModel

PROJECT_ROOT = Path(__file__).resolve().parents[6]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from tools.runtime_state_sqlite import (  # noqa: E402
    export_team_score_overview_csv,
    replace_team_subject_task_overview_rows,
    replace_team_task_overview_rows,
    replace_team_trial_record_rows,
    resolve_runtime_state_db_path,
    upsert_team_subject_task_overview_rows,
    upsert_team_task_overview_rows,
    upsert_team_trial_record_rows,
    write_team_overview_row,
    write_team_score_overview_row,
)



class ChallengeMI(ChallengeInterface):
    __DEFAULT_TEAM_ID = 'team_1'
    __DEFAULT_CHANNEL_COUNT = 8
    __DEFAULT_CALIBRATION_TRIALS_PER_CLASS = 10
    __DEFAULT_TIMEOUT_PREDICT_LABEL = 'wrong'
    __INCREMENTAL_SUMMARY_FLUSH_TRIAL_INTERVAL = 5
    __INCREMENTAL_SUMMARY_FLUSH_INTERVAL_SECONDS = 1.0
    __APPLIED_RECOVERY_FILE_NAME = 'applied_recovery.json'

    __TRIAL_START_TRIGGER = '101'
    __TRIAL_END_TRIGGER = '241'
    __BLOCK_START_TRIGGER = '242'
    __BLOCK_END_TRIGGER = '243'

    def __init__(self):
        super().__init__()
        self.is_closed = False
        self.__source_list: list[SourceModel] = []
        self.__config_dict: dict[str, Union[str, dict]] = {}
        self.__algorithm_metadata: dict[str, Union[str, dict, float, int]] = {}
        self.__requested_channel_count: Optional[int] = None
        self.__requested_calibration_trial_count: Optional[int] = None

        self.__current_subject_id: Optional[str] = None
        self.__current_block_id: Optional[str] = None
        self.__current_exp_name: Optional[str] = None
        self.__current_exp_task: Optional[str] = None
        self.__current_session_id: Optional[str] = None
        self.__current_stream_role: str = 'unknown'
        self.__current_stage_trial_id: int = 0
        self.__current_global_trial_id: int = 0

        self.__score_package_list: list[ScorePackageModel] = []
        self.__trial_record_list: list[dict] = []
        self.__record_key_set: set[tuple[str, str, str, str]] = set()
        self.__task_static_score_snapshot_dict: dict[str, dict[str, Union[str, float, int]]] = {}
        self.__label_cache_dict: dict[tuple[str, str, str, str], list[str]] = {}
        self.__score_context_cache: Optional[dict] = None
        self.__final_score_result_cache: Optional[dict] = None
        self.__prepared_result_team_id: Optional[str] = None
        self.__virtual_receiver_config_dict: dict[str, Union[str, dict]] = {}
        # 修改原因：
        # 原实现只缓存 VirtualReceiverConfig.yml 的内容，后面解析 data_files 里的相对路径时只能
        # 假定它们一律相对 Path(__file__).resolve().parents[5]。这个假设在单进程 debug 场景里
        # 不稳定，因为 data_files 实际由 Collector 侧配置维护，所以这里把配置文件真实位置也保存下来。
        self.__virtual_receiver_config_path: Optional[Path] = None
        self.__persisted_trial_record_db_row_count = 0
        self.__persisted_trial_record_db_team_id: Optional[str] = None
        self.__last_incremental_summary_flush_record_count = 0
        self.__last_incremental_summary_flush_monotonic = 0.0
        self.__logger = logging.getLogger("processHubLogger")

    async def initial(self):
        #添加日志初始化，统一日志使用:
        current_file_path = os.path.abspath(__file__)
        log_config_file_directory_path = Path(current_file_path).resolve().parents[3] / "config"
        log_config_file_path = os.path.join(log_config_file_directory_path, 'LoggingConfig.yml')
        with open(log_config_file_path, 'r', encoding='utf-8') as logging_file:
            logging_config = yaml.safe_load(logging_file)

        self.__ensure_logging_targets(
            logging_config,
            base_dir=Path(current_file_path).resolve().parents[3],
        )

        # 应用配置到logging模块
        logging.config.dictConfig(logging_config)


        challenge_config_path = Path(__file__).with_name('ChallengeMI.yml')
        with challenge_config_path.open('r', encoding='utf-8') as file:
            self.__config_dict = yaml.safe_load(file)

        virtual_receiver_config_path = (
            Path(__file__).resolve().parents[5]
            / 'Collector'
            / 'Collector'
            / 'receiver'
            / 'virtual_receiver'
            / 'VirtualReceiverConfig.yml'
        )
        # 原逻辑到这里为止只会 open 配置文件，不会记录配置文件自身路径。
        # 这次额外缓存路径，后面的 __resolve_virtual_receiver_data_file_path() 就能优先按
        # VirtualReceiverConfig.yml 所在目录系去解析 data_files 里的相对路径。
        self.__virtual_receiver_config_path = virtual_receiver_config_path
        with virtual_receiver_config_path.open('r', encoding='utf-8') as file:
            self.__virtual_receiver_config_dict = yaml.safe_load(file)

        for source_label, source_topic in (self.__config_dict.get('sources') or {}).items():
            self.__source_list.append(SourceModel(source_label, source_topic))

    async def startup(self):
        return

    async def shutdown(self):
        return

    async def update(self, config_dict: dict[str, Union[str, dict]]):
        return

    async def get_to_algorithm_config(self) -> dict[str, Union[str, dict]]:
        algorithm_config = copy.deepcopy(self.__config_dict.get('challenge_to_algorithm_config', {}) or {})
        algorithm_config['predict_timeout_seconds'] = self.__resolve_timeout_seconds()
        return algorithm_config

    async def get_source_list(self) -> list[SourceModel]:
        return self.__source_list

    async def get_to_strategy_config(self) -> dict[str, Union[str, dict]]:
        return self.__config_dict.get('strategy_config', {}) or {}


    async def receive_message(
        self, algorithm_data_message_model: AlgorithmDataMessageModel
    ) -> Union[AlgorithmDataMessageModel, None]:
        # 这里本应做 challenge 级别的数据预处理。
        # 当前实现仍保持透传，但补充完整上下文日志，便于排查 session / exp_task / trial 边界问题。
        package = algorithm_data_message_model.package
        if isinstance(package, DevicePackageModel):
            other_information = package.other_information or {}
            self.__current_subject_id = other_information.get('subject_id', self.__current_subject_id)
            self.__current_exp_name = other_information.get('exp_name', self.__current_exp_name)
            self.__current_exp_task = other_information.get('exp_task', self.__current_exp_task)
            self.__current_session_id = other_information.get('session_id', self.__current_session_id)
            self.__current_stream_role = other_information.get('stream_role', self.__current_stream_role)
            self.__logger.info(
                "device_update %s requested_channel_count=%s requested_calibration_trials=%s",
                self.__format_runtime_context(),
                self.__requested_channel_count,
                self.__requested_calibration_trial_count,
            )
        elif isinstance(package, InformationPackageModel):
            self.__current_subject_id = package.subject_id or self.__current_subject_id
            if package.block_id and package.block_id != self.__current_block_id:
                self.__current_stage_trial_id = 0
            self.__current_block_id = package.block_id or self.__current_block_id
            self.__logger.info("block_info %s", self.__format_runtime_context())
        elif isinstance(package, EventPackageModel):
            self.__log_event_package(package)
        elif isinstance(package, (DataPackageModel, ControlPackageModel)):
            pass
        else:
            self.__logger.info(
                "receive_message %s %s",
                type(package).__name__,
                self.__format_runtime_context(),
            )
        # 返回值依然是 AlgorithmDataMessageModel，task 会继续把它发给算法。
        return algorithm_data_message_model

    async def receive_report(self, algorithm_report_message_model: AlgorithmReportMessageModel) -> None:
        package = algorithm_report_message_model.package
        if isinstance(package, ControlPackageModel):
            # 需要处理ControlpackageModel类型数据包
            self.__logger.info(
                "report_control %s end_flag=%s",
                self.__format_runtime_context(),
                package.end_flag,
            )
        if not isinstance(package, ResultPackageModel):
            return
        # 只需要处理ResultPackageModel类型数据包即可
        # result_package_model中数据类型会被自动转换成发送时对应的数据类型
        self.__current_global_trial_id += 1
        self.__current_stage_trial_id += 1
        record = self.__build_trial_record(package)
        self.__logger.info(
            "result_ready %s platform_trial_id=%s local_result_index_in_block=%s local_result_index_total=%s predict_label=%s true_label=%s timeout=%s",
            self.__format_runtime_context(),
            record.get('trial_id'),
            self.__current_stage_trial_id,
            self.__current_global_trial_id,
            record.get('predict_label'),
            record.get('true_label'),
            record.get('is_timeout'),
        )
        self.__append_record_and_score(record)

    async def receive_timeout_trial(self, timeout_context: dict) -> None:
        if not isinstance(timeout_context, dict) or not timeout_context:
            return

        timeout_payload = copy.deepcopy(timeout_context)
        report_source_information = timeout_payload.pop('report_source_information', None)
        timeout_payload.setdefault('platform_timeout', True)
        timeout_payload.setdefault('is_timeout', True)
        if timeout_payload.get('predict_label') is None:
            timeout_payload['predict_label'] = self.__resolve_timeout_predict_label()
        if (
            timeout_payload.get('predict_time_ms') is None
            and timeout_payload.get('platform_runtime_ms') is None
        ):
            timeout_payload['predict_time_ms'] = self.__resolve_timeout_seconds() * 1000.0

        self.__current_global_trial_id += 1
        self.__current_stage_trial_id += 1
        record = self.__build_trial_record_from_payload(timeout_payload, report_source_information)
        self.__logger.info(
            "result_ready %s platform_trial_id=%s local_result_index_in_block=%s local_result_index_total=%s predict_label=%s true_label=%s timeout=%s",
            self.__format_runtime_context(),
            record.get('trial_id'),
            self.__current_stage_trial_id,
            self.__current_global_trial_id,
            record.get('predict_label'),
            record.get('true_label'),
            record.get('is_timeout'),
        )
        self.__append_record_and_score(record)


    async def receive_algorithm_connector_closed_event(self, algorithm_address: str):
        score_context = self.get_final_score_context()
        self.__persist_trial_record_files(incremental_db_write=False)
        self.__logger.info(
            "algorithm_closed address=%s %s",
            algorithm_address,
            self.__format_runtime_context(),
        )
        self.__logger.info("MI final score context: %s", score_context)

    async def receive_algorithm_config(self, algorithm_config: dict[str, Union[str, dict]]):
        # 修改原因：
        # 1. 原文件这里前后出现过两套 receive_algorithm_config 处理方式：一套直接解析并记录日志，
        #    后面又定义了一次同名方法继续处理同样的字段；职责分散后，读代码时很难一眼看出最终生效逻辑。
        # 2. 原始写法里 requested_channel_count / calibration_trials_per_class_requested 都是各自
        #    try: int(...) except ...，重复且容易和后续评分逻辑的取值优先级脱节。
        # 3. 现在先统一缓存 self.__algorithm_metadata，再从同一份 metadata 派生两个“申请值”，
        #    所有“能转 int 就转，不能转就记 None”的规则都收口到 __coerce_optional_int()。
        self.__algorithm_metadata = algorithm_config or {}

        # 原始思路示意：
        # requested_channel_labels_dict = algorithm_config.get('requested_channel_labels', {}) or {}
        # self.__requested_channel_count = algorithm_config.get(...)
        # self.__requested_calibration_trial_count = algorithm_config.get(...)
        # 这里改成全部从 self.__algorithm_metadata 读取，保证缓存、日志、后续 resolve_* 看到的是同一份输入。
        requested_channel_labels_dict = self.__algorithm_metadata.get('requested_channel_labels', {}) or {}
        requested_channel_count = self.__algorithm_metadata.get(
            'requested_channel_count',
            len(requested_channel_labels_dict.get('eeg_1', [])),
        )
        requested_calibration_trial_count = self.__algorithm_metadata.get(
            'calibration_trials_per_class_requested'
        )

        self.__requested_channel_count = self.__coerce_optional_int(requested_channel_count)
        self.__requested_calibration_trial_count = self.__coerce_optional_int(requested_calibration_trial_count)
        self.__task_static_score_snapshot_dict.clear()

        preserved_trial_row_list = self.__read_preserved_trial_rows_for_applied_recovery()

        self.__logger.info(
            "receive_algorithm_config metadata=%s requested_channel_count=%s requested_calibration_trials=%s platform_model_size_mb=%s",
            self.__algorithm_metadata,
            self.__requested_channel_count,
            self.__requested_calibration_trial_count,
            self.__algorithm_metadata.get('platform_model_size_mb'),
        )
        self.__prepare_result_dir(force_cleanup=True)
        if preserved_trial_row_list:
            self.__hydrate_preserved_trial_records(preserved_trial_row_list)
            self.__persist_trial_record_files(incremental_db_write=False)
            current_score_result = self.__build_incremental_score_result()
            if current_score_result is not None:
                self.__persist_score_result_file(
                    current_score_result,
                    incremental_db_write=False,
                )


    async def timeout_trigger(self, algorithm_data_message_model: AlgorithmDataMessageModel):
        package = algorithm_data_message_model.package
        if not isinstance(package, DataPackageModel):
            return None

        timeout_payload = self.__parse_result_payload(package.data)
        if not timeout_payload:
            return None

        report_source_label = str(timeout_payload.get('report_source_label') or 'eeg_1')
        report_source_position = timeout_payload.get('report_source_position')
        report_source_information = []
        if report_source_position is not None:
            report_source_information.append(
                {
                    'source_label': report_source_label,
                    'position': report_source_position,
                }
            )

        timeout_payload['report_source_information'] = report_source_information
        await self.receive_timeout_trial(timeout_payload)
        return None

    async def get_score(self) -> list[ScorePackageModel]:
        return list(self.__score_package_list)

    def get_final_total_score(self) -> float:
        if self.__final_score_result_cache is None:
            return 0.0
        return float(self.__final_score_result_cache.get('total_score', 0.0))

    def get_live_task_metrics(self) -> dict[str, Union[str, float, int, None]]:
        if not self.__trial_record_list:
            return {
                'current_trial_score': 0.0,
                'current_task_score': 0.0,
                'current_task_accuracy_percent': 0.0,
                'judge_message': None,
                'is_invalid_output': False,
                'current_task_trial_count': 0,
                'task_id': None,
            }
        latest_record = self.__trial_record_list[-1] or {}
        score_snapshot = latest_record.get('score_snapshot') or {}
        task_id = latest_record.get('task_id')
        current_task_trial_count = sum(
            1
            for record in self.__trial_record_list
            if str(record.get('task_id') or '') == str(task_id or '')
        )
        cumulative_task_score = self.__safe_float(score_snapshot.get('cumulative_score'))
        return {
            'current_trial_score': self.__safe_float(latest_record.get('trial_score')),
            'current_task_score': cumulative_task_score,
            'current_task_accuracy_percent': self.__safe_float(score_snapshot.get('cumulative_accuracy_percent')),
            'judge_message': latest_record.get('judge_message'),
            'is_invalid_output': bool(latest_record.get('is_invalid_output')),
            'current_task_trial_count': current_task_trial_count,
            'task_id': task_id,
        }

    def build_metric_summary(self) -> dict[str, Union[str, float, int, dict, list]]:
        return self.get_final_score_context()

    def get_final_score_context(self) -> dict[str, Union[str, dict, float, int, list]] | None:
        if self.__score_context_cache is None:
            self.__score_context_cache = self.__build_score_context()
        return copy.deepcopy(self.__score_context_cache)

    def finalize_score_result(self, final_score_result: dict[str, Union[str, dict, float, int, list]]) -> None:
        self.is_closed = True
        self.__final_score_result_cache = copy.deepcopy(final_score_result)
        self.__persist_score_result_file(
            self.__final_score_result_cache,
        )

    def __build_score_context(self) -> dict[str, Union[str, dict, float, int, list]]:
        task_summary_dict = self.__build_task_summary_dict()
        task_order = self.__resolve_configured_task_order()
        model_size_mb = self.__resolve_model_size_mb()
        size_score_enabled = model_size_mb is not None
        runtime_ms_list = [
            float(record['predict_time_ms'])
            for record in self.__trial_record_list
            if record.get('predict_time_ms') is not None
        ]
        avg_runtime_ms = mean(runtime_ms_list) if runtime_ms_list else self.__resolve_timeout_seconds() * 1000.0

        return {
            'team_id': self.__resolve_team_id(),
            'record_count': len(self.__trial_record_list),
            'hierarchy': ['subject_id', 'task_id', 'session_id', 'trial_id'],
            'task_field': 'task',
            'subtask_field': 'exam',
            'task_count': len(task_order),
            'task_order': task_order,
            'task_summary': task_summary_dict,
            'task_baseline_score_dict': self.__resolve_task_baseline_score_dict(),
            'avg_runtime_ms': avg_runtime_ms,
            'avg_reaction_time_ms': avg_runtime_ms,
            'runtime_definition': 'trial_end_to_result_report_ms',
            'channel_count': self.__resolve_channel_count(),
            'calibration_trials_per_class': self.__resolve_calibration_trials_per_class(),
            'model_size_mb': model_size_mb,
            'size_score_enabled': size_score_enabled,
            'window_length_seconds': 4.0,
        }

    def __append_record_and_score(self, record: dict) -> None:
        record_key = self.__build_record_key(record)
        if record_key in self.__record_key_set:
            self.__logger.warning("duplicate MI trial record ignored: %s", record_key)
            return

        self.__trial_record_list.append(record)
        self.__record_key_set.add(record_key)
        record['score_snapshot'] = self.__build_record_score_snapshot(record)
        self.__score_package_list.append(self.__build_trial_score_package(record))
        self.__score_context_cache = None
        self.__final_score_result_cache = None
        self.__logger.info("MI trial record: %s", record.get('score_snapshot',None))
        self.__persist_incremental_result_snapshot()

    def __persist_incremental_result_snapshot(self) -> None:
        try:
            self.__persist_trial_record_files(incremental_db_write=True)
        except Exception:
            self.__logger.exception("persist incremental MI trial files failed")

        current_score_result = self.__build_incremental_score_result()
        if current_score_result is None:
            return
        if not self.__should_flush_incremental_score_summary():
            return

        try:
            self.__persist_score_result_file(
                current_score_result,
                incremental_db_write=True,
            )
            self.__mark_incremental_summary_flush()
        except Exception:
            self.__logger.exception("persist incremental MI score summary failed")

    def __read_preserved_trial_rows_for_applied_recovery(self) -> list[dict]:
        applied_recovery_manifest = self.__load_applied_recovery_manifest()
        if not applied_recovery_manifest:
            return []

        trial_record_csv_path = self.__resolve_result_dir() / '03_trial_records.csv'
        if not trial_record_csv_path.exists():
            self.__logger.warning(
                "applied recovery manifest exists but preserved trial csv is missing: %s",
                trial_record_csv_path,
            )
            return []

        try:
            with trial_record_csv_path.open('r', encoding='utf-8-sig', newline='') as file:
                return [row for row in csv.DictReader(file) if isinstance(row, dict)]
        except OSError:
            self.__logger.exception(
                "read preserved MI trial rows for recovery failed: %s",
                trial_record_csv_path,
            )
            return []

    def __load_applied_recovery_manifest(self) -> Optional[dict]:
        manifest_path = (
            PROJECT_ROOT
            / 'results'
            / 'control'
            / self.__APPLIED_RECOVERY_FILE_NAME
        )
        if not manifest_path.exists():
            return None
        try:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            self.__logger.exception("read applied recovery manifest failed: %s", manifest_path)
            return None
        if not isinstance(manifest, dict):
            return None
        if manifest.get('recovery_mode') != 'restart_from_stage':
            return None
        return manifest

    def __hydrate_preserved_trial_records(self, row_list: list[dict]) -> None:
        self.__trial_record_list = []
        self.__record_key_set.clear()
        self.__score_package_list = []
        self.__score_context_cache = None
        self.__final_score_result_cache = None
        self.__persisted_trial_record_db_row_count = 0
        self.__persisted_trial_record_db_team_id = None

        for row in row_list:
            record = self.__build_trial_record_from_export_row(row)
            record_key = self.__build_record_key(record)
            if record_key in self.__record_key_set:
                self.__logger.warning("duplicate preserved MI trial record ignored: %s", record_key)
                continue
            self.__trial_record_list.append(record)
            self.__record_key_set.add(record_key)
            record['score_snapshot'] = self.__build_record_score_snapshot(record)

        self.__current_global_trial_id = len(self.__trial_record_list)
        self.__current_stage_trial_id = 0
        self.__score_context_cache = None
        self.__final_score_result_cache = None
        self.__logger.info(
            "hydrated preserved MI trial records for recovery: team_id=%s record_count=%s",
            self.__resolve_team_id(),
            len(self.__trial_record_list),
        )

    def __build_trial_record_from_export_row(self, row: dict) -> dict:
        exp_name = self.__coerce_optional_text(row.get('exp_name'))
        exp_task = self.__coerce_optional_text(row.get('exp_task'))
        task_id = self.__coerce_optional_text(row.get('task_id'))
        if task_id is None and exp_name is not None and exp_task is not None:
            task_id = self.__resolve_task_id(exp_name=exp_name, exp_task=exp_task)
        if (exp_name is None or exp_task is None) and task_id is not None:
            split_exp_name, split_exp_task = self.__split_task_id(task_id)
            exp_name = exp_name or split_exp_name
            exp_task = exp_task or split_exp_task

        exp_name = exp_name or 'unknown_exp'
        exp_task = exp_task or 'unknown_task'
        task_id = task_id or self.__resolve_task_id(exp_name=exp_name, exp_task=exp_task)
        subject_id = self.__coerce_optional_text(row.get('subject_id')) or 'unknown_subject'
        session_id = self.__coerce_optional_text(row.get('session_id')) or 'unknown_session'
        block_id = self.__coerce_optional_text(row.get('block_id')) or session_id
        trial_id = self.__coerce_optional_text(row.get('trial_id')) or '0'
        predict_label = self.__coerce_optional_text(row.get('predict_label'))
        raw_predict_label = self.__coerce_optional_text(row.get('raw_predict_label')) or predict_label
        true_label = self.__coerce_optional_text(row.get('true_label'))
        is_timeout = self.__coerce_bool(row.get('is_timeout'), default=False)
        is_invalid_output = self.__coerce_bool(row.get('is_invalid_output'), default=False)
        is_correct = self.__coerce_optional_bool(row.get('is_correct'))
        trial_score = self.__coerce_optional_float(row.get('trial_score'))
        predict_time_ms = self.__coerce_optional_float(row.get('predict_time_ms'))

        return {
            'subject_id': subject_id,
            'task_id': task_id,
            'task': exp_name,
            'exam': exp_task,
            'stage_id': f'{task_id}|{session_id}',
            'transport_block_id': block_id,
            'block_id': block_id,
            'exp_name': exp_name,
            'exp_task': exp_task,
            'session_id': session_id,
            'trial_id': trial_id,
            'raw_label': true_label,
            'raw_predict_label': raw_predict_label,
            'true_label': true_label,
            'predict_label': predict_label,
            'is_correct': is_correct,
            'trial_score': trial_score,
            'is_timeout': is_timeout,
            'is_invalid_output': is_invalid_output,
            'judge_message': self.__coerce_optional_text(row.get('judge_message')),
            'platform_trial_start_position': None,
            'platform_trial_end_position': None,
            'platform_trial_ready_wallclock': None,
            'platform_report_receive_wallclock': None,
            'predict_time_ms': predict_time_ms,
            'platform_raw_trigger_value': None,
            'platform_true_label': true_label,
            'report_source_information': self.__parse_report_position_text(
                row.get('report_position')
            ),
        }

    @staticmethod
    def __coerce_optional_text(value) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text if text != '' else None

    @classmethod
    def __coerce_optional_float(cls, value) -> Optional[float]:
        text = cls.__coerce_optional_text(value)
        if text is None:
            return None
        try:
            return float(text)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def __coerce_optional_bool(value) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        text = str(value).strip().lower()
        if text in {'true', '1', 'yes', 'y'}:
            return True
        if text in {'false', '0', 'no', 'n'}:
            return False
        return None

    @classmethod
    def __coerce_bool(cls, value, *, default: bool = False) -> bool:
        parsed_value = cls.__coerce_optional_bool(value)
        return default if parsed_value is None else parsed_value

    @classmethod
    def __parse_report_position_text(cls, value) -> list[dict]:
        text = cls.__coerce_optional_text(value)
        if text is None:
            return []
        report_source_information = []
        for item_text in text.split('|'):
            if ':' not in item_text:
                continue
            source_label, position_text = item_text.split(':', 1)
            source_label = source_label.strip()
            position = cls.__coerce_optional_float(position_text)
            if source_label == '' or position is None:
                continue
            report_source_information.append(
                {
                    'source_label': source_label,
                    'position': position,
                }
            )
        return report_source_information

    def __build_incremental_score_result(self) -> Optional[dict]:
        score_context = self.get_final_score_context()
        if not isinstance(score_context, dict) or not score_context:
            return None

        try:
            from ProcessHub.bci_competition.task.BCICompetitionTaskFinal import (
                BCICompetitionTaskFinal,
            )

            return (
                BCICompetitionTaskFinal
                ._BCICompetitionTaskFinal__build_final_score_result(score_context)
            )
        except Exception:
            self.__logger.exception("build incremental MI score result failed")
            return None

    @staticmethod
    def __build_record_key(record: dict) -> tuple[str, str, str, str]:
        return (
            str(record.get('subject_id') or 'unknown_subject'),
            str(record.get('task_id') or 'unknown_task'),
            str(record.get('session_id') or 'unknown_session'),
            str(record.get('trial_id') or '0'),
        )

    def __build_record_score_snapshot(self, record: dict) -> dict[str, Union[str, float, int, bool, None]]:
        task_id = str(record.get('task_id') or 'unknown_task')
        task_record_list = [
            item for item in self.__trial_record_list if str(item.get('task_id') or 'unknown_task') == task_id
        ]

        task_static_score_snapshot = self.__resolve_task_static_score_snapshot(task_id)

        predict_time_list = [
            self.__safe_float(item.get('predict_time_ms'))
            for item in task_record_list
            if item.get('predict_time_ms') is not None
        ]
        cumulative_avg_reaction_time_ms = mean(predict_time_list) if predict_time_list else 0.0
        cumulative_avg_reaction_time_score = self.__compute_reaction_time_score(
            cumulative_avg_reaction_time_ms
        )

        correctness_list = [
            1 if item.get('is_correct') is True else 0
            for item in task_record_list
            if item.get('is_correct') is not None
        ]
        cumulative_accuracy = (
            sum(correctness_list) / len(correctness_list) if correctness_list else 0.0
        )
        cumulative_accuracy_percent = cumulative_accuracy * 100.0
        cumulative_accuracy_history_percent_list = [
            sum(correctness_list[: index + 1]) * 100.0 / float(index + 1)
            for index in range(len(correctness_list))
        ]
        cumulative_accuracy_std_percent = (
            pstdev(cumulative_accuracy_history_percent_list)
            if cumulative_accuracy_history_percent_list
            else 0.0
        )
        cumulative_accuracy_score = self.__compute_accuracy_score(
            cumulative_accuracy_percent,
            cumulative_accuracy_std_percent,
        )
        cumulative_score = (
            self.__compute_static_component_score(task_static_score_snapshot)
            + cumulative_avg_reaction_time_score
            + cumulative_accuracy_score
        )

        return {
            'task': record.get('task'),
            'exam': record.get('exam'),
            'trial_id': record.get('trial_id'),
            'trial_score': record.get('trial_score'),
            'calibration_rounds': task_static_score_snapshot.get('calibration_rounds'),
            'calibration_score': task_static_score_snapshot.get('calibration_score'),
            'channel_rounds': task_static_score_snapshot.get('channel_rounds'),
            'channel_score': task_static_score_snapshot.get('channel_score'),
            'model_size_mb': task_static_score_snapshot.get('model_size_mb'),
            'model_size_score': task_static_score_snapshot.get('model_size_score'),
            'current_reaction_time_ms': self.__safe_float(record.get('predict_time_ms')),
            'cumulative_avg_reaction_time_ms': cumulative_avg_reaction_time_ms,
            'cumulative_avg_reaction_time_score': cumulative_avg_reaction_time_score,
            'true_label': record.get('true_label'),
            'predict_label': record.get('predict_label'),
            'cumulative_accuracy': cumulative_accuracy,
            'cumulative_accuracy_percent': cumulative_accuracy_percent,
            'cumulative_accuracy_std_percent': cumulative_accuracy_std_percent,
            'cumulative_accuracy_score': cumulative_accuracy_score,
            'cumulative_score': cumulative_score,
            'is_timeout': bool(record.get('is_timeout')),
        }

    def __build_task_summary_dict(self) -> dict[str, dict[str, Union[str, float, int, dict]]]:
        task_record_dict: dict[str, list[dict]] = {
            task_id: [] for task_id in self.__resolve_configured_task_order()
        }
        for record in self.__trial_record_list:
            task_id = str(record.get('task_id') or 'unknown_task')
            task_record_dict.setdefault(task_id, []).append(record)

        task_baseline_score_dict = self.__resolve_task_baseline_score_dict()
        task_summary_dict: dict[str, dict[str, Union[str, float, int, dict]]] = {}
        for task_id in self.__resolve_configured_task_order():
            task_record_list = task_record_dict.get(task_id, [])
            task_subject_correctness_dict: dict[str, list[int]] = {}
            for record in task_record_list:
                if record.get('is_correct') is None:
                    continue
                subject_id = str(record.get('subject_id') or 'unknown_subject')
                task_subject_correctness_dict.setdefault(subject_id, []).append(
                    1 if record.get('is_correct') is True else 0
                )

            per_subject_accuracy_percent = {
                subject_id: mean(task_subject_correctness_dict[subject_id]) * 100.0
                for subject_id in sorted(task_subject_correctness_dict)
                if task_subject_correctness_dict[subject_id]
            }
            per_subject_trial_count = {
                subject_id: len(task_subject_correctness_dict[subject_id])
                for subject_id in sorted(task_subject_correctness_dict)
            }
            final_score_snapshot = (
                task_record_list[-1].get('score_snapshot') if task_record_list else None
            ) or self.__build_empty_score_snapshot(task_id)
            task_score = self.__safe_float(final_score_snapshot.get('cumulative_score'))
            baseline_score = self.__safe_float(task_baseline_score_dict.get(task_id))
            cumulative_accuracy_percent = self.__safe_float(
                final_score_snapshot.get('cumulative_accuracy_percent')
            )
            cumulative_accuracy_std_percent = self.__safe_float(
                final_score_snapshot.get('cumulative_accuracy_std_percent')
            )
            task_summary_dict[task_id] = {
                'task_name': task_id,
                'exp_name': self.__split_task_id(task_id)[0],
                'exp_task': self.__split_task_id(task_id)[1],
                'subject_count': len(per_subject_accuracy_percent),
                'trial_count': len(task_record_list),
                'mu_accuracy_percent': cumulative_accuracy_percent,
                'sigma_accuracy_percent': cumulative_accuracy_std_percent,
                'per_subject_accuracy_percent': per_subject_accuracy_percent,
                'per_subject_trial_count': per_subject_trial_count,
                'cumulative_accuracy_percent': cumulative_accuracy_percent,
                'cumulative_accuracy_std_percent': cumulative_accuracy_std_percent,
                'avg_reaction_time_ms': self.__safe_float(
                    final_score_snapshot.get('cumulative_avg_reaction_time_ms')
                ),
                'accuracy_score': self.__safe_float(final_score_snapshot.get('cumulative_accuracy_score')),
                'reaction_time_score': self.__safe_float(
                    final_score_snapshot.get('cumulative_avg_reaction_time_score')
                ),
                'channel_count': int(final_score_snapshot.get('channel_rounds') or 0),
                'channel_score': self.__safe_float(final_score_snapshot.get('channel_score')),
                'calibration_trials_per_class': int(final_score_snapshot.get('calibration_rounds') or 0),
                'calibration_score': self.__safe_float(final_score_snapshot.get('calibration_score')),
                'model_size_mb': self.__safe_float(final_score_snapshot.get('model_size_mb')),
                'model_size_score': self.__safe_float(final_score_snapshot.get('model_size_score')),
                'task_score': task_score,
                'baseline_score': baseline_score,
                'adjusted_task_score': task_score if task_score >= baseline_score else 0.0,
            }
        return task_summary_dict

    def __build_empty_score_snapshot(self, task_id: str) -> dict[str, Union[str, float, int, bool, None]]:
        exp_name, exp_task = self.__split_task_id(task_id)
        return {
            'task': exp_name,
            'exam': exp_task,
            'trial_id': None,
            'calibration_rounds': self.__resolve_calibration_trials_per_class(),
            'calibration_score': 0.0,
            'channel_rounds': self.__resolve_channel_count(),
            'channel_score': 0.0,
            'model_size_mb': self.__resolve_model_size_mb(),
            'model_size_score': 0.0,
            'current_reaction_time_ms': 0.0,
            'cumulative_avg_reaction_time_ms': 0.0,
            'cumulative_avg_reaction_time_score': 0.0,
            'true_label': None,
            'predict_label': None,
            'cumulative_accuracy': 0.0,
            'cumulative_accuracy_percent': 0.0,
            'cumulative_accuracy_std_percent': 0.0,
            'cumulative_accuracy_score': 0.0,
            'cumulative_score': 0.0,
            'is_timeout': False,
        }

    def __resolve_task_static_score_snapshot(self, task_id: str) -> dict[str, Union[str, float, int]]:
        task_id_text = str(task_id or 'unknown_task')
        if task_id_text not in self.__task_static_score_snapshot_dict:
            exp_name, exp_task = self.__split_task_id(task_id_text)
            calibration_rounds = self.__resolve_calibration_trials_per_class()
            channel_rounds = self.__resolve_channel_count()
            model_size_mb = self.__resolve_model_size_mb()
            self.__task_static_score_snapshot_dict[task_id_text] = {
                'task': exp_name,
                'exam': exp_task,
                'calibration_rounds': calibration_rounds,
                'calibration_score': self.__compute_calibration_score(calibration_rounds),
                'channel_rounds': channel_rounds,
                'channel_score': self.__compute_channel_score(channel_rounds),
                'model_size_mb': model_size_mb,
                'size_score_enabled': model_size_mb is not None,
                'model_size_score': self.__compute_model_size_score(model_size_mb),
            }
        return self.__task_static_score_snapshot_dict[task_id_text]

    def __compute_static_component_score(self, task_static_score_snapshot: dict[str, Union[str, float, int, bool, None]]) -> float:
        static_score = (
            self.__safe_float(task_static_score_snapshot.get('calibration_score'))
            + self.__safe_float(task_static_score_snapshot.get('channel_score'))
        )
        if bool(task_static_score_snapshot.get('size_score_enabled')):
            static_score += self.__safe_float(task_static_score_snapshot.get('model_size_score'))
        return static_score

    def __build_trial_record(self, result_package_model: ResultPackageModel) -> dict:
        report_source_information = [
            {
                'source_label': item.source_label,
                'position': item.position,
            }
            for item in (result_package_model.report_source_information or [])
        ]
        return self.__build_trial_record_from_payload(
            payload=self.__parse_result_payload(result_package_model.result),
            report_source_information=report_source_information,
        )

    def __build_trial_record_from_payload(
        self,
        payload: dict,
        report_source_information: Optional[list[dict]] = None,
    ) -> dict:
        subject_id = str(payload.get('platform_subject_id') or self.__current_subject_id or 'unknown_subject')
        exp_name = str(payload.get('platform_exp_name') or self.__current_exp_name or 'unknown_exp')
        exp_task = str(payload.get('platform_exp_task') or self.__current_exp_task or 'unknown_task')
        session_id = str(payload.get('platform_session_id') or self.__current_session_id or 'unknown_session')
        block_id = str(payload.get('platform_block_id') or self.__current_block_id or session_id)
        trial_id = str(payload.get('platform_trial_id') or '0')
        task_id = self.__resolve_task_id(exp_name=exp_name, exp_task=exp_task)
        stage_id = f'{task_id}|{session_id}'

        raw_trigger_value = self.__normalize_raw_trigger_value(payload.get('platform_raw_trigger_value'))
        true_label_from_payload = self.__normalize_binary_label(payload.get('platform_true_label'))
        if true_label_from_payload is not None:
            raw_label = str(raw_trigger_value) if raw_trigger_value is not None else None
            true_label = true_label_from_payload
        elif raw_trigger_value is not None:
            raw_label = str(raw_trigger_value)
            true_label = self.__map_label_value(exp_task=exp_task, raw_label_text=raw_label)
        else:
            raw_label, true_label = self.__resolve_labels(
                subject_id=subject_id,
                exp_name=exp_name,
                exp_task=exp_task,
                session_id=session_id,
                trial_id=trial_id,
            )

        is_timeout = bool(payload.get('platform_timeout') or payload.get('is_timeout'))
        predict_output_resolution = self.__resolve_predict_output(
            payload.get('predict_label'),
            is_timeout=is_timeout,
        )
        predict_label = predict_output_resolution.get('predict_label')
        raw_predict_label = predict_output_resolution.get('raw_predict_label')
        is_invalid_output = bool(predict_output_resolution.get('is_invalid_output'))
        judge_message = predict_output_resolution.get('judge_message')
        if is_timeout or is_invalid_output:
            is_correct = False
        elif predict_label is None or true_label is None:
            is_correct = None
        else:
            is_correct = predict_label == str(true_label)
        predict_time_ms = self.__resolve_predict_time_ms(payload)
        if is_correct is True:
            trial_score = 1.0
        elif is_correct is False or is_timeout or is_invalid_output:
            trial_score = 0.0
        else:
            trial_score = None

        return {
            'subject_id': subject_id,
            'task_id': task_id,
            'task': exp_name,
            'exam': exp_task,
            'stage_id': stage_id,
            'transport_block_id': block_id,
            'block_id': block_id,
            'exp_name': exp_name,
            'exp_task': exp_task,
            'session_id': session_id,
            'trial_id': trial_id,
            'raw_label': raw_label,
            'raw_predict_label': raw_predict_label,
            'true_label': true_label,
            'predict_label': predict_label,
            'is_correct': is_correct,
            'trial_score': trial_score,
            'is_timeout': is_timeout,
            'is_invalid_output': is_invalid_output,
            'judge_message': judge_message,
            'platform_trial_start_position': payload.get('platform_trial_start_position'),
            'platform_trial_end_position': payload.get('platform_trial_end_position'),
            'platform_trial_ready_wallclock': payload.get('platform_trial_ready_wallclock'),
            'platform_report_receive_wallclock': payload.get('platform_report_receive_wallclock'),
            'predict_time_ms': predict_time_ms,
            'platform_raw_trigger_value': raw_trigger_value,
            'platform_true_label': true_label_from_payload,
            'report_source_information': self.__normalize_report_source_information(
                report_source_information,
                payload,
            ),
        }

    @staticmethod
    def __normalize_report_source_information(
        report_source_information: Optional[list[dict]],
        payload: dict,
    ) -> list[dict]:
        normalized_report_source_information = []
        for item in report_source_information or []:
            if isinstance(item, ReportSourceInformationModel):
                normalized_report_source_information.append(
                    {
                        'source_label': item.source_label,
                        'position': item.position,
                    }
                )
                continue
            if isinstance(item, dict):
                normalized_report_source_information.append(
                    {
                        'source_label': item.get('source_label'),
                        'position': item.get('position'),
                    }
                )

        if normalized_report_source_information:
            return normalized_report_source_information

        if payload.get('report_source_position') is None:
            return []

        return [
            {
                'source_label': str(payload.get('report_source_label') or 'eeg_1'),
                'position': payload.get('report_source_position'),
            }
        ]

    def __build_trial_score_package(self, record: dict) -> ScorePackageModel:
        score_snapshot = record.get('score_snapshot') or {}
        score = self.__safe_float(score_snapshot.get('cumulative_score'))
        trial_score = self.__safe_float(record.get('trial_score'))
        show_text = (
            f"task={record.get('task')} "
            f"exam={record.get('exam')} "
            f"trial={record.get('trial_id')} "
            f"pred={record.get('predict_label')} "
            f"true={record.get('true_label')} "
            f"trial_score={trial_score:.3f} "
            f"rt={self.__safe_float(record.get('predict_time_ms')):.1f}ms "
            f"acc={self.__safe_float(score_snapshot.get('cumulative_accuracy_percent')):.2f}% "
            f"score={score:.3f}"
        )
        return ScorePackageModel(
            show_text=show_text,
            score=score,
            trial_time=self.__safe_float(record.get('predict_time_ms')),
            trial_id=str(record.get('trial_id') or '0'),
            block_id=str(record.get('stage_id') or record.get('block_id') or '0'),
            subject_id=str(record.get('subject_id') or '0'),
        )

    @staticmethod
    def __parse_result_payload(raw_result: Union[str, bytes, list[int], list[str], None]) -> dict:
        if raw_result is None:
            return {}
        if isinstance(raw_result, str):
            try:
                payload = json.loads(raw_result)
                if isinstance(payload, dict):
                    return payload
                return {'predict_label': str(payload)}
            except json.JSONDecodeError:
                return {'predict_label': raw_result}
        return {'predict_label': str(raw_result)}

    def __resolve_labels(
        self,
        subject_id: str,
        exp_name: str,
        exp_task: str,
        session_id: str,
        trial_id: str,
    ) -> tuple[Optional[str], Optional[str]]:
        label_list = self.__load_label_list((subject_id, exp_name, exp_task, session_id))
        try:
            trial_index = int(trial_id) - 1
        except (TypeError, ValueError):
            return None, None
        if trial_index < 0 or trial_index >= len(label_list):
            return None, None

        raw_label_text = str(label_list[trial_index]).strip()
        mapped_label = self.__map_label_value(exp_task=exp_task, raw_label_text=raw_label_text)
        if raw_label_text in {'1', '2', '3'}:
            return raw_label_text, mapped_label
        return None, mapped_label

    def __load_label_list(self, stage_signature: tuple[str, str, str, str]) -> list[str]:
        if stage_signature in self.__label_cache_dict:
            return self.__label_cache_dict[stage_signature]
        label_list = self.__build_online_label_list(stage_signature)
        self.__label_cache_dict[stage_signature] = label_list
        return label_list

    def __build_online_label_list(self, stage_signature: tuple[str, str, str, str]) -> list[str]:
        subject_id, exp_name, exp_task, session_id = stage_signature
        data_files_dict = self.__virtual_receiver_config_dict.get('data_files', {}) or {}
        exp_files_dict = data_files_dict.get(subject_id, {}) or {}
        session_file_path_list = exp_files_dict.get(exp_name, []) or []

        raw_trigger_list: list[int] = []
        for relative_file_path in session_file_path_list:
            # 原代码：
            # workspace_root = Path(__file__).resolve().parents[5]
            # file_path = workspace_root / str(relative_file_path)
            # 问题在于 data_files 里的相对路径并不一定是相对 ChallengeMI 模块，而是相对
            # VirtualReceiverConfig.yml 所在的 Collector 目录维护的。统一走下面的 helper，
            # 才能兼容正式流程和 debug/debug_pipeline.py 单进程调试流程。
            file_path = self.__resolve_virtual_receiver_data_file_path(relative_file_path)
            if session_id not in file_path.as_posix():
                continue
            raw_trigger_list.extend(self.__extract_raw_trigger_list(file_path))

        filtered_trigger_list = [
            trigger_value
            for trigger_value in raw_trigger_list
            if self.__is_allowed_trigger(exp_task, trigger_value)
        ]

        calibrate_trials_per_class = self.__resolve_calibration_trials_per_class()
        if calibrate_trials_per_class > 0:
            trigger_counter_dict = {}
            online_trigger_list: list[int] = []
            for trigger_value in filtered_trigger_list:
                current_count = trigger_counter_dict.get(trigger_value, 0)
                if current_count < calibrate_trials_per_class:
                    trigger_counter_dict[trigger_value] = current_count + 1
                    continue
                online_trigger_list.append(trigger_value)
        else:
            online_trigger_list = filtered_trigger_list

        shuffle_seed_source = f"{subject_id}|{exp_name}|{exp_task}|{session_id}"
        shuffle_seed = self.__build_session_shuffle_seed(shuffle_seed_source)
        shuffle_rng = np.random.default_rng(shuffle_seed)
        if online_trigger_list:
            shuffled_index_array = shuffle_rng.permutation(len(online_trigger_list))
            online_trigger_list = [online_trigger_list[index] for index in shuffled_index_array.tolist()]

        return [self.__map_label_value(exp_task, str(trigger_value)) for trigger_value in online_trigger_list]

    def __resolve_virtual_receiver_data_file_path(self, file_path_text: Union[str, Path]) -> Path:
        # 修改原因：
        # 原实现实际上只有一条解析规则：
        # workspace_root = Path(__file__).resolve().parents[5]
        # file_path = workspace_root / str(relative_file_path)
        # 这要求 ChallengeMI 与 VirtualReceiverConfig 的 data_files 必须共享同一个基准目录。
        # 但现在 data_files 来自 Collector 侧配置，debug 入口复用该配置时很容易拼错根目录并触发
        # FileNotFoundError，所以这里按“配置文件相关目录 -> 仓库根”依次尝试，并把尝试过的路径带进异常里。
        candidate_path = Path(file_path_text)
        if candidate_path.is_absolute():
            if candidate_path.exists():
                return candidate_path
            raise FileNotFoundError(f'virtual receiver data file not found: {candidate_path}')

        candidate_root_list: list[Path] = []
        if self.__virtual_receiver_config_path is not None:
            # 原假设只有“ChallengeMI 所在仓库根目录”这一种。
            # 现在把和 VirtualReceiverConfig.yml 紧邻的几个候选根目录都纳入尝试范围：
            # 1. app/Collector
            # 2. app
            # 3. virtual_receiver 当前目录
            # 这样既兼容配置里写 app/Collector 相对路径，也兼容更近的局部相对路径。
            candidate_root_list.extend(
                [
                    self.__virtual_receiver_config_path.parents[3],
                    self.__virtual_receiver_config_path.parents[4],
                    self.__virtual_receiver_config_path.parent,
                ]
            )
        candidate_root_list.append(Path(__file__).resolve().parents[5])

        resolved_candidate_path_list: list[Path] = []
        seen_root_set: set[Path] = set()
        for candidate_root in candidate_root_list:
            if candidate_root in seen_root_set:
                continue
            seen_root_set.add(candidate_root)
            resolved_candidate_path = candidate_root / candidate_path
            resolved_candidate_path_list.append(resolved_candidate_path)
            if resolved_candidate_path.exists():
                return resolved_candidate_path

        candidate_path_text_list = ', '.join(str(path) for path in resolved_candidate_path_list)
        raise FileNotFoundError(
            f'virtual receiver data file not found for {candidate_path}: {candidate_path_text_list}'
        )

    def __extract_raw_trigger_list(self, data_file_path: Path) -> list[int]:
        metadata_dict = self.__load_metadata(data_file_path)
        total_channel_number = int(metadata_dict.get('channels', 65))
        sample_matrix = np.fromfile(data_file_path, dtype=np.float32)
        if sample_matrix.size % total_channel_number != 0:
            self.__logger.warning('unexpected dat size for %s', data_file_path)
            return []
        sample_matrix = sample_matrix.reshape(-1, total_channel_number)
        trigger_array = np.rint(sample_matrix[:, total_channel_number - 1]).astype(np.int32)
        valid_trigger_array = np.where(np.isin(trigger_array, [1, 2, 3]), trigger_array, 0)
        previous_trigger_array = np.concatenate(([0], valid_trigger_array[:-1]))
        trial_start_position_array = np.where((valid_trigger_array != 0) & (previous_trigger_array == 0))[0]
        return [int(valid_trigger_array[position]) for position in trial_start_position_array.tolist()]

    @staticmethod
    def __load_metadata(data_file_path: Path) -> dict[str, str]:
        metadata_path = data_file_path.with_name(f'{data_file_path.stem}_meta.txt')
        metadata_dict: dict[str, str] = {}
        if not metadata_path.exists():
            return metadata_dict
        with metadata_path.open('r', encoding='utf-8') as metadata_file:
            for line in metadata_file:
                if ':' not in line:
                    continue
                key, value = line.split(':', 1)
                metadata_dict[key.strip()] = value.strip()
        return metadata_dict

    @staticmethod
    def __is_allowed_trigger(exp_task: str, trigger_value: int) -> bool:
        if exp_task == 'left_vs_rest':
            return trigger_value in {1, 3}
        if exp_task == 'right_vs_rest':
            return trigger_value in {2, 3}
        return False

    @staticmethod
    def __map_label_value(exp_task: str, raw_label_text: str) -> Optional[str]:
        if raw_label_text in {'0', '1'}:
            return raw_label_text
        if exp_task == 'left_vs_rest':
            if raw_label_text == '1':
                return '1'
            if raw_label_text == '3':
                return '0'
        if exp_task == 'right_vs_rest':
            if raw_label_text == '2':
                return '1'
            if raw_label_text == '3':
                return '0'
        return None

    def __resolve_predict_time_ms(self, payload: dict) -> Optional[float]:
        runtime_ms = payload.get('predict_time_ms', payload.get('platform_runtime_ms'))
        if runtime_ms is None:
            return None
        return self.__safe_float(runtime_ms)

    def __resolve_predict_output(
        self,
        raw_predict_label,
        *,
        is_timeout: bool,
    ) -> dict[str, Union[str, bool, None]]:
        raw_predict_label_text = None if raw_predict_label is None else str(raw_predict_label).strip()
        if raw_predict_label_text == '':
            raw_predict_label_text = None

        if is_timeout:
            return {
                'predict_label': self.__resolve_timeout_predict_label(),
                'raw_predict_label': raw_predict_label_text,
                'is_invalid_output': False,
                'judge_message': '算法超时，按错误计分，当前 trial 分数记为 0',
            }

        normalized_predict_label = self.__normalize_binary_label(raw_predict_label_text)
        if normalized_predict_label is not None:
            return {
                'predict_label': normalized_predict_label,
                'raw_predict_label': raw_predict_label_text,
                'is_invalid_output': False,
                'judge_message': None,
            }

        if raw_predict_label_text is None:
            judge_message = '算法输出缺少 predict_label，按错误计分，当前 trial 分数记为 0'
        else:
            judge_message = (
                f"算法输出 predict_label={raw_predict_label_text} 超出允许范围，仅允许 0/1，"
                "按错误计分，当前 trial 分数记为 0"
            )
        self.__logger.warning(judge_message)
        return {
            'predict_label': raw_predict_label_text,
            'raw_predict_label': raw_predict_label_text,
            'is_invalid_output': True,
            'judge_message': judge_message,
        }

    def __resolve_score_config(self) -> dict[str, Union[str, float, int, dict]]:
        return self.__config_dict.get('score_config', {}) or {}

    def __resolve_configured_task_order(self) -> list[str]:
        configured_task_baseline_score_dict = self.__resolve_score_config().get('task_baseline_score', {}) or {}
        task_order = []
        for record in self.__trial_record_list:
            task_id_text = str(record.get('task_id') or '').strip()
            if task_id_text != '' and task_id_text not in task_order:
                task_order.append(task_id_text)
        for task_id in configured_task_baseline_score_dict:
            task_id_text = str(task_id).strip()
            if task_id_text != '' and task_id_text not in task_order:
                task_order.append(task_id_text)
        return task_order

    def __resolve_task_baseline_score_dict(self) -> dict[str, float]:
        configured_task_baseline_score_dict = self.__resolve_score_config().get('task_baseline_score', {}) or {}
        resolved_task_baseline_score_dict: dict[str, float] = {}
        for task_id in self.__resolve_configured_task_order():
            resolved_task_baseline_score_dict[task_id] = self.__safe_float(
                configured_task_baseline_score_dict.get(task_id)
            )
        return resolved_task_baseline_score_dict

    def __resolve_timeout_seconds(self) -> float:
        strategy_config = self.__config_dict.get('strategy_config', {}) or {}
        timeout_setting_dict = strategy_config.get('timeout_setting', {}) or {}
        for timeout_parameter in timeout_setting_dict.values():
            if not isinstance(timeout_parameter, dict):
                continue
            timeout_limit = timeout_parameter.get('timeout_limit')
            if timeout_limit is not None:
                return self.__safe_float(timeout_limit)
        return 2.0

    def __resolve_timeout_predict_label(self) -> str:
        strategy_config = self.__config_dict.get('strategy_config', {}) or {}
        timeout_setting_dict = strategy_config.get('timeout_setting', {}) or {}
        for timeout_parameter in timeout_setting_dict.values():
            if not isinstance(timeout_parameter, dict):
                continue
            timeout_predict_label = timeout_parameter.get('timeout_predict_label')
            if timeout_predict_label is not None and str(timeout_predict_label).strip() != '':
                return str(timeout_predict_label).strip()
        timeout_predict_label = self.__resolve_score_config().get('timeout_predict_label')
        if timeout_predict_label is not None and str(timeout_predict_label).strip() != '':
            return str(timeout_predict_label).strip()
        return self.__DEFAULT_TIMEOUT_PREDICT_LABEL

    def __compute_accuracy_score(
        self,
        mu_accuracy_percent: float,
        sigma_accuracy_percent: float,
    ) -> float:
        score_config = self.__resolve_score_config()
        accuracy_score_max = self.__safe_float(score_config.get('accuracy_score_max', 80.0))
        stability_penalty_lambda = self.__safe_float(
            score_config.get('accuracy_stability_penalty_lambda', 0.5)
        )
        accuracy_score = accuracy_score_max * max(
            0.0,
            (
                self.__safe_float(mu_accuracy_percent)
                - stability_penalty_lambda * self.__safe_float(sigma_accuracy_percent)
            )
            / 100.0,
        )
        return max(0.0, min(accuracy_score_max, accuracy_score))

    def __compute_reaction_time_score(self, average_reaction_time_ms: float) -> float:
        score_config = self.__resolve_score_config()
        reaction_time_score_max = self.__safe_float(score_config.get('reaction_time_score_max', 2.0))
        reaction_time_reference_ms = self.__safe_float(
            score_config.get('reaction_time_reference_ms', 1000.0)
        )
        if reaction_time_reference_ms <= 0:
            return 0.0
        return max(
            0.0,
            min(
                reaction_time_score_max,
                reaction_time_score_max
                * (1.0 - self.__safe_float(average_reaction_time_ms) / reaction_time_reference_ms),
            ),
        )

    def __compute_channel_score(self, channel_count: int) -> float:
        score_config = self.__resolve_score_config()
        channel_score_max = self.__safe_float(score_config.get('channel_score_max', 8.0))
        channel_reference_count = max(1, int(score_config.get('channel_reference_count', 8) or 8))
        if channel_reference_count <= 1:
            return 0.0
        return max(
            0.0,
            min(
                channel_score_max,
                channel_score_max
                * (channel_reference_count - self.__safe_float(channel_count))
                / (channel_reference_count - 1.0),
            ),
        )

    def __compute_calibration_score(self, calibration_trials_per_class: int) -> float:
        score_config = self.__resolve_score_config()
        calibration_score_max = self.__safe_float(score_config.get('calibration_score_max', 7.0))
        calibration_reference_trials_per_class = max(
            1,
            int(score_config.get('calibration_reference_trials_per_class', 10) or 10),
        )
        return max(
            0.0,
            min(
                calibration_score_max,
                calibration_score_max
                * (
                    1.0
                    - self.__safe_float(calibration_trials_per_class)
                    / calibration_reference_trials_per_class
                ),
            ),
        )

    def __compute_model_size_score(self, model_size_mb: Optional[float]) -> float:
        if model_size_mb is None:
            return 0.0
        score_config = self.__resolve_score_config()
        model_size_score_max = self.__safe_float(score_config.get('model_size_score_max', 3.0))
        model_size_reference_mb = self.__safe_float(score_config.get('model_size_reference_mb', 150.0))
        if model_size_reference_mb <= 0:
            return 0.0
        return max(
            0.0,
            min(
                model_size_score_max,
                model_size_score_max
                * max(0.0, 1.0 - self.__safe_float(model_size_mb) / model_size_reference_mb),
            ),
        )

    def __prepare_result_dir(self, force_cleanup: bool = False) -> Path:
        result_dir = self.__resolve_result_dir()
        result_dir.mkdir(parents=True, exist_ok=True)
        current_team_id = self.__resolve_team_id()
        if force_cleanup or self.__prepared_result_team_id != current_team_id:
            self.__cleanup_legacy_result_files(result_dir)
            self.__prepared_result_team_id = current_team_id
        return result_dir

    @staticmethod
    def __ensure_logging_targets(logging_config: dict | None, base_dir: Path) -> None:
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

    @staticmethod
    def __write_csv_atomically(
        csv_path: Path,
        fieldnames: list[str],
        row_list: list[dict],
    ) -> None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_file_path: Optional[Path] = None
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
                writer.writerows(row_list)
            os.replace(tmp_file_path, csv_path)
        finally:
            if tmp_file_path is not None and tmp_file_path.exists():
                tmp_file_path.unlink(missing_ok=True)

    def __persist_trial_record_files(self, incremental_db_write: bool = False) -> None:
        result_dir = self.__prepare_result_dir()
        trial_record_csv_path = result_dir / '03_trial_records.csv'
        csv_fieldnames = self.__build_trial_record_export_fieldnames()
        row_list = self.__build_trial_record_export_row_list()
        self.__write_csv_atomically(trial_record_csv_path, csv_fieldnames, row_list)
        self.__sync_trial_record_rows_to_runtime_state(
            row_list,
            incremental_db_write=incremental_db_write,
        )

        persisted_task_id_list = []
        for record in self.__trial_record_list:
            task_id = str(record.get('task_id') or '').strip()
            if task_id != '' and task_id not in persisted_task_id_list:
                persisted_task_id_list.append(task_id)

        for task_id in persisted_task_id_list:
            self.__persist_task_trial_summary_file(result_dir, task_id)

    def __persist_task_trial_summary_file(self, result_dir: Path, task_id: str) -> None:
        task_trial_summary_csv_path = result_dir / 'task_trials' / f'{task_id}_trial_records.csv'
        csv_fieldnames = self.__build_trial_record_export_fieldnames()
        row_list = self.__build_trial_record_export_row_list(task_id=task_id)
        self.__write_csv_atomically(task_trial_summary_csv_path, csv_fieldnames, row_list)
        return


    def __persist_score_result_file(
        self,
        final_score_result: dict,
        incremental_db_write: bool = False,
    ) -> None:
        result_dir = self.__resolve_result_dir()
        result_dir.mkdir(parents=True, exist_ok=True)
        for legacy_file_name in (
            'team_total_score.csv',
            'score_result.csv',
            'score_task_summary.csv',
            'score_subject_task_summary.csv',
            'team_summary.csv',
        ):
            legacy_file_path = result_dir / legacy_file_name
            if legacy_file_path.exists():
                legacy_file_path.unlink()

        task_metric_list = final_score_result.get('task_metric_list') or []
        started_task_metric_list = [
            task_metric
            for task_metric in task_metric_list
            if self.__safe_float(task_metric.get('trial_count')) > 0
        ]

        team_summary_csv_path = result_dir / '00_team_overview.csv'
        team_summary_fieldnames = self.__build_team_overview_fieldnames()
        team_overview_row = self.__build_team_overview_row(final_score_result, task_metric_list, started_task_metric_list)
        team_summary_row_list = [
            team_overview_row
        ]
        self.__write_csv_atomically(team_summary_csv_path, team_summary_fieldnames, team_summary_row_list)
        write_team_overview_row(
            resolve_runtime_state_db_path(PROJECT_ROOT),
            team_overview_row,
        )
        self.__upsert_results_root_team_overview_row(team_overview_row)

        task_summary_csv_path = result_dir / '01_task_overview.csv'
        task_summary_fieldnames = [
            'team_id',
            'task_id',
            'exp_name',
            'exp_task',
            'task_status',
            'updated_at',
            'subject_count',
            'observed_trial_count',
            'accuracy_percent',
            'avg_reaction_time_ms',
            'task_score',
        ]
        task_summary_row_list = []
        for task_metric in task_metric_list:
            task_summary_row_list.append(
                {
                    'team_id': final_score_result.get('team_id'),
                    'task_id': task_metric.get('task_name'),
                    'exp_name': task_metric.get('exp_name'),
                    'exp_task': task_metric.get('exp_task'),
                    'task_status': self.__resolve_task_status(task_metric),
                    'updated_at': self.__resolve_result_updated_at(),
                    'subject_count': task_metric.get('subject_count'),
                    'observed_trial_count': task_metric.get('trial_count'),
                    'accuracy_percent': task_metric.get('cumulative_accuracy_percent'),
                    'avg_reaction_time_ms': task_metric.get('avg_reaction_time_ms'),
                    'task_score': task_metric.get('task_score'),
                }
            )
        self.__write_csv_atomically(
            task_summary_csv_path,
            task_summary_fieldnames,
            task_summary_row_list,
        )
        if incremental_db_write:
            upsert_team_task_overview_rows(
                resolve_runtime_state_db_path(PROJECT_ROOT),
                self.__resolve_team_id(),
                task_summary_row_list,
            )
        else:
            replace_team_task_overview_rows(
                resolve_runtime_state_db_path(PROJECT_ROOT),
                self.__resolve_team_id(),
                task_summary_row_list,
            )

        subject_task_summary_csv_path = result_dir / '02_subject_task_overview.csv'
        subject_task_summary_fieldnames = [
            'team_id',
            'subject_id',
            'task_id',
            'exp_name',
            'exp_task',
            'task_status',
            'updated_at',
            'observed_trial_count',
            'accuracy_percent',
        ]
        subject_task_summary_row_list = []
        task_summary_dict = final_score_result.get('task_summary') or {}
        for task_name in sorted(task_summary_dict):
            task_summary = task_summary_dict.get(task_name) or {}
            per_subject_accuracy_percent = task_summary.get('per_subject_accuracy_percent') or {}
            per_subject_trial_count = task_summary.get('per_subject_trial_count') or {}
            for subject_id in sorted(set(per_subject_accuracy_percent) | set(per_subject_trial_count)):
                subject_task_summary_row_list.append(
                    {
                        'team_id': final_score_result.get('team_id'),
                        'subject_id': subject_id,
                        'task_id': task_name,
                        'exp_name': task_summary.get('exp_name'),
                        'exp_task': task_summary.get('exp_task'),
                        'task_status': self.__resolve_task_status(task_summary),
                        'updated_at': self.__resolve_result_updated_at(),
                        'observed_trial_count': per_subject_trial_count.get(subject_id),
                        'accuracy_percent': per_subject_accuracy_percent.get(subject_id),
                    }
                )
        self.__write_csv_atomically(
            subject_task_summary_csv_path,
            subject_task_summary_fieldnames,
            subject_task_summary_row_list,
        )
        if incremental_db_write:
            upsert_team_subject_task_overview_rows(
                resolve_runtime_state_db_path(PROJECT_ROOT),
                self.__resolve_team_id(),
                subject_task_summary_row_list,
            )
        else:
            replace_team_subject_task_overview_rows(
                resolve_runtime_state_db_path(PROJECT_ROOT),
                self.__resolve_team_id(),
                subject_task_summary_row_list,
            )
        self.__persist_results_root_team_overview_file()

    def __build_trial_record_export_fieldnames(self) -> list[str]:
        return [
            'team_id',
            'team_trial_index',
            'task_trial_index',
            'subject_id',
            'task_id',
            'exp_name',
            'exp_task',
            'session_id',
            'block_id',
            'trial_id',
            'true_label',
            'raw_predict_label',
            'predict_label',
            'is_correct',
            'trial_score',
            'is_timeout',
            'is_invalid_output',
            'judge_message',
            'predict_time_ms',
            'cumulative_accuracy_percent',
            'cumulative_score',
            'report_position',
        ]

    def __build_trial_record_export_row_list(self, task_id: Optional[str] = None) -> list[dict]:
        row_list: list[dict] = []
        task_trial_index_by_task_id: dict[str, int] = {}
        for team_trial_index, record in enumerate(self.__trial_record_list, start=1):
            current_task_id = str(record.get('task_id') or '').strip()
            if task_id is not None and current_task_id != str(task_id):
                continue
            task_trial_index = task_trial_index_by_task_id.get(current_task_id, 0) + 1
            task_trial_index_by_task_id[current_task_id] = task_trial_index
            score_snapshot = record.get('score_snapshot') or {}
            row_list.append(
                {
                    'team_id': self.__resolve_team_id(),
                    'team_trial_index': team_trial_index,
                    'task_trial_index': task_trial_index,
                    'subject_id': record.get('subject_id'),
                    'task_id': current_task_id,
                    'exp_name': record.get('exp_name'),
                    'exp_task': record.get('exp_task'),
                    'session_id': record.get('session_id'),
                    'block_id': record.get('block_id'),
                    'trial_id': record.get('trial_id'),
                    'true_label': record.get('true_label'),
                    'raw_predict_label': record.get('raw_predict_label'),
                    'predict_label': record.get('predict_label'),
                    'is_correct': record.get('is_correct'),
                    'trial_score': record.get('trial_score'),
                    'is_timeout': record.get('is_timeout'),
                    'is_invalid_output': record.get('is_invalid_output'),
                    'judge_message': record.get('judge_message'),
                    'predict_time_ms': record.get('predict_time_ms'),
                    'cumulative_accuracy_percent': score_snapshot.get('cumulative_accuracy_percent'),
                    'cumulative_score': score_snapshot.get('cumulative_score'),
                    'report_position': self.__build_report_source_positions_text(
                        record.get('report_source_information') or []
                    ),
                }
            )
        return row_list

    @staticmethod
    def __build_report_source_positions_text(report_source_information: list[dict]) -> str:
        return '|'.join(
            f"{item.get('source_label')}:{item.get('position')}"
            for item in report_source_information
            if item.get('source_label') is not None and item.get('position') is not None
        )

    def __build_collector_session_shuffle_seed_text(self) -> str:
        seed_text_list: list[str] = []
        for subject_id, exp_name, exp_task, session_id in self.__collect_observed_stage_signature_list():
            seed_source = f'{subject_id}|{exp_name}|{exp_task}|{session_id}'
            seed_text_list.append(f'{seed_source}:{self.__build_session_shuffle_seed(seed_source)}')
        return ';'.join(seed_text_list)

    def __collect_observed_stage_signature_list(self) -> list[tuple[str, str, str, str]]:
        stage_signature_set: set[tuple[str, str, str, str]] = set()
        for record in self.__trial_record_list:
            stage_signature = (
                str(record.get('subject_id') or '').strip(),
                str(record.get('exp_name') or record.get('task') or '').strip(),
                str(record.get('exp_task') or record.get('exam') or '').strip(),
                str(record.get('session_id') or record.get('transport_block_id') or record.get('block_id') or '').strip(),
            )
            if all(stage_signature):
                stage_signature_set.add(stage_signature)
        return sorted(stage_signature_set)

    @staticmethod
    def __build_session_shuffle_seed(seed_source: str) -> int:
        return int.from_bytes(hashlib.sha256(seed_source.encode('utf-8')).digest()[:8], 'big')

    @staticmethod
    def __build_team_overview_fieldnames() -> list[str]:
        return [
            'team_id',
            'total_score',
            'run_status',
            'updated_at',
            'global_seed',
            'collector_session_shuffle_seed',
            'observed_trial_count',
            'configured_task_count',
            'started_task_count',
            'mean_accuracy_percent',
            'avg_reaction_time_ms',
            'started_task_names',
        ]

    def __build_team_overview_row(
        self,
        final_score_result: dict,
        task_metric_list: list[dict],
        started_task_metric_list: list[dict],
    ) -> dict:
        return {
            'team_id': final_score_result.get('team_id'),
            'total_score': final_score_result.get('total_score'),
            'run_status': self.__resolve_run_status(),
            'updated_at': self.__resolve_result_updated_at(),
            'global_seed': DEFAULT_GLOBAL_SEED,
            'collector_session_shuffle_seed': self.__build_collector_session_shuffle_seed_text(),
            'observed_trial_count': final_score_result.get('record_count'),
            'configured_task_count': len(task_metric_list),
            'started_task_count': len(started_task_metric_list),
            'mean_accuracy_percent': final_score_result.get('mean_accuracy_percent'),
            'avg_reaction_time_ms': final_score_result.get('avg_reaction_time_ms'),
            'started_task_names': '|'.join(
                str(task_metric.get('task_name') or '') for task_metric in started_task_metric_list
            ),
        }

    def __resolve_run_status(self) -> str:
        return 'finished' if self.is_closed else 'running'

    def __resolve_task_status(self, task_metric: dict) -> str:
        observed_trial_count = int(task_metric.get('trial_count') or 0)
        if observed_trial_count <= 0:
            return 'not_started'
        return 'finished' if self.is_closed else 'running'

    @staticmethod
    def __resolve_result_updated_at() -> str:
        return datetime.now().isoformat(timespec='seconds')

    def __persist_results_root_team_overview_file(self) -> None:
        results_root_dir = self.__resolve_results_root_dir()
        results_root_dir.mkdir(parents=True, exist_ok=True)
        team_overview_csv_path = results_root_dir / '00_team_score_overview.csv'
        csv_fieldnames = self.__build_team_overview_fieldnames()
        export_team_score_overview_csv(
            resolve_runtime_state_db_path(PROJECT_ROOT),
            team_overview_csv_path,
            csv_fieldnames,
        )

    def __upsert_results_root_team_overview_row(self, team_overview_row: dict) -> None:
        write_team_score_overview_row(
            resolve_runtime_state_db_path(PROJECT_ROOT),
            team_overview_row,
        )

    def __sync_trial_record_rows_to_runtime_state(
        self,
        row_list: list[dict],
        *,
        incremental_db_write: bool,
    ) -> None:
        runtime_state_db_path = resolve_runtime_state_db_path(PROJECT_ROOT)
        team_id = self.__resolve_team_id()
        row_count = len(row_list)
        if (
            not incremental_db_write
            or self.__persisted_trial_record_db_team_id != team_id
            or row_count < self.__persisted_trial_record_db_row_count
        ):
            replace_team_trial_record_rows(runtime_state_db_path, team_id, row_list)
            self.__persisted_trial_record_db_team_id = team_id
            self.__persisted_trial_record_db_row_count = row_count
            return

        pending_row_list = row_list[self.__persisted_trial_record_db_row_count:]
        if not pending_row_list:
            return
        upsert_team_trial_record_rows(runtime_state_db_path, team_id, pending_row_list)
        self.__persisted_trial_record_db_team_id = team_id
        self.__persisted_trial_record_db_row_count = row_count

    def __should_flush_incremental_score_summary(self) -> bool:
        record_count = len(self.__trial_record_list)
        if record_count <= 1:
            return True
        if (
            record_count - self.__last_incremental_summary_flush_record_count
            >= self.__INCREMENTAL_SUMMARY_FLUSH_TRIAL_INTERVAL
        ):
            return True
        now_monotonic = time.monotonic()
        return (
            self.__last_incremental_summary_flush_monotonic <= 0.0
            or now_monotonic - self.__last_incremental_summary_flush_monotonic
            >= self.__INCREMENTAL_SUMMARY_FLUSH_INTERVAL_SECONDS
        )

    def __mark_incremental_summary_flush(self) -> None:
        self.__last_incremental_summary_flush_record_count = len(self.__trial_record_list)
        self.__last_incremental_summary_flush_monotonic = time.monotonic()

    @staticmethod
    def __cleanup_legacy_result_files(result_dir: Path) -> None:
        for legacy_file_name in (
            'score_raw_data.json',
            'score_result.json',
            'score_raw_data.csv',
            'team_total_score.csv',
            'score_result.csv',
            'score_task_summary.csv',
            'score_subject_task_summary.csv',
            '00_team_overview.csv',
            '01_task_overview.csv',
            '02_subject_task_overview.csv',
            '03_trial_records.csv',
            'team_score.csv',
            'team_summary.csv',
            'task_summary.csv',
            'subject_task_summary.csv',
        ):
            legacy_file_path = result_dir / legacy_file_name
            if legacy_file_path.exists():
                legacy_file_path.unlink()

        for task_result_path in result_dir.glob('*_result.csv'):
            if task_result_path.exists():
                task_result_path.unlink()

        for task_score_path in result_dir.glob('*_score.csv'):
            if task_score_path.exists():
                task_score_path.unlink()

        for task_trial_summary_path in result_dir.glob('*_trial_summary.csv'):
            if task_trial_summary_path.exists():
                task_trial_summary_path.unlink()

        task_trials_dir = result_dir / 'task_trials'
        if task_trials_dir.exists():
            for task_trial_file_path in task_trials_dir.glob('*.csv'):
                if task_trial_file_path.exists():
                    task_trial_file_path.unlink()

    def __resolve_result_dir(self) -> Path:
        return self.__resolve_results_root_dir() / self.__resolve_team_id()

    @staticmethod
    def __resolve_results_root_dir() -> Path:
        return Path(__file__).resolve().parents[6] / 'results'

    def __resolve_team_id(self) -> str:
        platform_team_id = self.__algorithm_metadata.get('platform_team_id')
        if platform_team_id is not None and str(platform_team_id).strip() != '':
            return str(platform_team_id).strip()
        for key in ('team_id', 'team_name'):
            value = self.__algorithm_metadata.get(key)
            if value is not None and str(value).strip() != '':
                return str(value).strip()
        team_id = os.environ.get('TEAM_ID')
        if team_id is not None and team_id.strip() != '':
            return team_id.strip()
        return self.__DEFAULT_TEAM_ID

    def __resolve_channel_count(self) -> int:
        # 修改原因：
        # 原逻辑只优先看 used_channel_count，不存在时再从 required_channel_labels 推导长度。
        # 这次 receive_algorithm_config 已经开始显式缓存 requested_channel_count；如果评分要按
        # “算法申请的通道数”计分，旧逻辑会漏掉这个新字段，所以这里把优先级扩展成：
        # requested_channel_count -> used_channel_count -> 缓存的申请值 -> label 长度 -> 默认值。
        for key in ('requested_channel_count', 'used_channel_count'):
            channel_count = self.__coerce_optional_int(self.__algorithm_metadata.get(key))
            if channel_count is not None and channel_count > 0:
                return channel_count

        if self.__requested_channel_count is not None and self.__requested_channel_count > 0:
            return self.__requested_channel_count

        for key in ('requested_channel_labels', 'required_channel_labels'):
            required_channel_labels = self.__algorithm_metadata.get(key)
            if isinstance(required_channel_labels, dict):
                eeg_channel_label_list = required_channel_labels.get('eeg_1')
                if isinstance(eeg_channel_label_list, list) and len(eeg_channel_label_list) > 0:
                    return len(eeg_channel_label_list)

        return self.__DEFAULT_CHANNEL_COUNT

    def __resolve_calibration_trials_per_class(self) -> int:
        # 修改原因：
        # 原逻辑只读取 calibration_trials_per_class，无法覆盖这次新增的
        # calibration_trials_per_class_requested。现在优先尊重“申请值”，并统一裁剪到
        # 0~__DEFAULT_CALIBRATION_TRIALS_PER_CLASS，避免异常配置把评分范围撑坏。
        for key in ('calibration_trials_per_class_requested', 'calibration_trials_per_class'):
            calibration_trials_per_class = self.__coerce_optional_int(self.__algorithm_metadata.get(key))
            if calibration_trials_per_class is not None:
                return max(0, min(self.__DEFAULT_CALIBRATION_TRIALS_PER_CLASS, calibration_trials_per_class))

        if self.__requested_calibration_trial_count is not None:
            return max(
                0,
                min(self.__DEFAULT_CALIBRATION_TRIALS_PER_CLASS, self.__requested_calibration_trial_count),
            )

        return self.__DEFAULT_CALIBRATION_TRIALS_PER_CLASS

    def __resolve_model_size_mb(self) -> Optional[float]:
        platform_model_size_mb = self.__algorithm_metadata.get('platform_model_size_mb')
        if platform_model_size_mb not in (None, ''):
            try:
                return float(platform_model_size_mb)
            except (TypeError, ValueError):
                self.__logger.warning(
                    "platform_model_size_mb 非法，忽略该值: raw_value=%s",
                    platform_model_size_mb,
                )

        self.__logger.warning(
            "未收到平台侧 model_artifacts 目录统计结果，模型大小项将不纳入计分: team_id=%s metadata=%s",
            self.__resolve_team_id(),
            self.__algorithm_metadata,
        )
        return None

    @classmethod
    def __resolve_task_id(cls, exp_name: str, exp_task: str) -> str:
        exp_name_text = str(exp_name).strip()
        exp_task_text = str(exp_task).strip()
        if exp_name_text == '' or exp_name_text == 'unknown_exp':
            exp_name_text = 'unknown_exp'
        if exp_task_text == '' or exp_task_text == 'unknown_task':
            exp_task_text = 'unknown_task'
        return f'{exp_name_text}_{exp_task_text}'

    @staticmethod
    def __split_task_id(task_id: str) -> tuple[str, str]:
        task_id_text = str(task_id or '').strip()
        for suffix in ('left_vs_rest', 'right_vs_rest'):
            suffix_text = f'_{suffix}'
            if task_id_text.endswith(suffix_text):
                return task_id_text[: -len(suffix_text)], suffix
        return task_id_text, 'unknown_task'

    @staticmethod
    def __safe_float(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def __safe_mean(cls, value_list: list[float]) -> float:
        normalized_value_list = [cls.__safe_float(value) for value in value_list]
        return mean(normalized_value_list) if normalized_value_list else 0.0

    @staticmethod
    def __normalize_raw_trigger_value(raw_trigger_value) -> Optional[int]:
        try:
            normalized_value = int(raw_trigger_value)
        except (TypeError, ValueError):
            return None
        if normalized_value in {1, 2, 3}:
            return normalized_value
        return None

    @staticmethod
    def __normalize_binary_label(true_label) -> Optional[str]:
        if true_label is None:
            return None
        true_label_text = str(true_label).strip()
        if true_label_text in {'0', '1'}:
            return true_label_text
        return None

    @staticmethod
    def __coerce_optional_int(value) -> Optional[int]:
        # 修改原因：
        # receive_algorithm_config / __resolve_channel_count / __resolve_calibration_trials_per_class
        # 原来都各写一套 try: int(value) except ...。抽成公共方法后，非法值统一回退为 None，
        # 避免重复代码，也保证评分链路遇到脏配置时行为一致。
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def __log_event_package(self, event_package_model: EventPackageModel) -> None:
        # 修改原因：
        # receive_message() 里已经存在 self.__log_event_package(package) 这次调用，但原文件缺少
        # 这个实现，事件包一进来就会 AttributeError。这里把 trial/block 边界日志补齐，
        # 方便结合 subject/session/exp/task 上下文排查试次切换问题。
        for _, event_data in zip(event_package_model.event_position, event_package_model.event_data):
            event_data_str = str(event_data)
            if event_data_str == self.__TRIAL_START_TRIGGER:
                self.__logger.info(
                    "trial_start %s next_local_result_index_in_block=%s",
                    self.__format_runtime_context(),
                    self.__current_stage_trial_id + 1,
                )
            elif event_data_str == self.__TRIAL_END_TRIGGER:
                self.__logger.info(
                    "trial_end %s current_local_result_index_in_block=%s",
                    self.__format_runtime_context(),
                    self.__current_stage_trial_id,
                )
            elif event_data_str == self.__BLOCK_START_TRIGGER:
                self.__logger.info(
                    "block_start %s",
                    self.__format_runtime_context(),
                )
            elif event_data_str == self.__BLOCK_END_TRIGGER:
                self.__logger.info(
                    "block_end %s total_local_result_index_in_block=%s",
                    self.__format_runtime_context(),
                    self.__current_stage_trial_id,
                )
            else:
                self.__logger.info(
                    "event %s event_data=%s",
                    self.__format_runtime_context(),
                    event_data_str,
                )

    def __format_runtime_context(self) -> str:
        # 修改原因：
        # device_update / block_info / result_ready / algorithm_closed 这些日志已经在调用
        # self.__format_runtime_context()；原文件同样缺少实现。现在统一在这里集中拼装最小排障上下文，
        # 避免每个 self.__logger.info(...) 各自重复拼字符串，也避免再次出现“调用了不存在的方法”。
        return (
            f"subject={self.__current_subject_id} block={self.__current_block_id} "
            f"session={self.__current_session_id} exp={self.__current_exp_name} "
            f"task={self.__current_exp_task} stream_role={self.__current_stream_role}"
        )


