import copy
import time
import logging
from pathlib import Path
from typing import Union

import numpy as np
import torch
from torch import nn
import json

from baseline_EEGNet import EEGNet
from baseline_preprocessing import EEGNetPreprocessor
from Algorithm.method.interface.AlgorithmInterface import AlgorithmInterface
from Algorithm.method.model.AlgorithmObject import (
    AlgorithmCalibrationObject,
    AlgorithmCalibrationProgressObject,
    AlgorithmContinuousDataObject,
    AlgorithmDeviceObject,
    AlgorithmResultObject,
    CalibrationStageResultObject,
    StageContextObject,
)
from Algorithm.service.SourceReceiver.ContinuousDataSourceReceiver import ContinuousDataSourceReceiver
from Algorithm.method.worker.PredictWorkerManager import PredictWorkerManager, PredictWorkerTimeoutError


class AlgorithmImplement(AlgorithmInterface):
    """
    默认 baseline 算法实现。

    这是参赛选手应该重点阅读和修改的文件。
    ==================== 比赛规则：模型目录约束与风险控制 ====================
    所有参与在线推理的已学习参数、模板、权重、统计量，必须全部存放于指定模型目录；
    目录外参数一律视为违规。

    风险控制要求：
    1. 不允许使用软链接、junction、快捷方式将指定模型目录指向目录外部。
    2. 不允许通过指定模型目录之外的隐藏参数参与在线推理。
    3. 不允许依赖在线下载的模型文件参与比赛。
    =====================================================================

    当前逻辑是按 session 分阶段运行：
    1. calibrate() 读取一个 session 的校准数据；
    2. 针对该 session 初始化/微调模型；
    3. predict() 处理切好的单个 online trial；
    4. 继续下一轮。
    注意，calibrate 是按时间顺序发送 trial。
    框架默认每类最多提供 10 个校准 trial，实际发送数量由
    `calibration_trials_per_class_requested` 决定，允许范围是 0~10。
    而predict中trial间的顺序是打乱后发送的
    建议把它先当成一个“三段式状态机”来理解：
    1. `__ensure_runtime_initialized()` 负责拿到数据源和设备信息；
    2. `calibrate()` / `__calibrate()` 负责读取并训练当前 session 的模型；
    3. `predict()` 负责处理单个完整 trial。

    对参赛选手而言，建议只把下面三个方法当成主要改动入口：
    1. `__init__()`
    2. `calibrate()`
    3. `predict()`
    `run()` 更像框架在线循环包装层，通常不建议直接修改。
    """
    __DEFAULT_MODEL_ARTIFACT_ROOT = 'method/model_artifacts'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 算法必须显式声明所需通道，通道数量小于等于8，框架会据此做筛选和通道重排。
        self.__required_channel_labels = {
            'eeg_1': ['C3', 'C4', 'FC3', 'FC4', 'CP3', 'CP4', 'CZ', 'PZ']
        }
        self.__trial_start_trigger = 101
        self.__trial_end_trigger = 241
        self.__trial_duration = 4
        self.__logger = logging.getLogger("algorithmLogger")
        self.__preprocessor = EEGNetPreprocessor()
        self.__algorithm_config: dict[str, object] = {
            # 选手通过 pull_algorithm_config() 暴露“算法主动申请”的比赛参数。
            # 当前只允许这里申请“每类校准 trial 数量”。
            # 通道声明统一通过 get_required_channel_labels() 返回，
            # 框架会在完成校验后，自动计算 requested_channel_labels / requested_channel_count
            # 并转发给 ProcessHub / Challenge，避免选手自行上报数量造成不一致。
            'calibration_trials_per_class_requested': 7, # 申请的校准数量（每类别）
            'baseline_model': {
                'device': 'cuda:0',
                # ==================== 比赛规则：指定模型目录 ====================
                # 所有参与在线推理的已学习参数、模板、权重、统计量，必须全部存放于
                # 指定模型目录；目录外参数一律视为违规。
                #
                # 1. 不允许使用软链接、junction、快捷方式将该目录映射到目录外部。
                # 2. 不允许通过该目录之外的隐藏参数参与在线推理。
                # 3. 不允许依赖在线下载的模型文件参与比赛。
                #
                # baseline 默认只从 method/model_artifacts 目录树内加载静态模型工件。
                # ===============================================================
                'weight_root': self.__DEFAULT_MODEL_ARTIFACT_ROOT,
                'calibration_epochs': 100,
                'calibration_learning_rate': 1e-4,
                'classifier_only_finetune': True,
            }
        }

        self.__runtime_config: dict[str, Union[str, dict]] = copy.deepcopy(self.__algorithm_config)
        self.__torch_device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        self.__sample_rate = 0
        self.__current_model: Union[EEGNet, None] = None
        # 模型实例属于哪个“被试/范式/任务/session”阶段。
        # 这里必须带上 subject_id，否则两个被试如果 session_id 同名，会错误复用同一轮模型实例。
        self.__current_model_signature: Union[tuple[str, str, str, str], None] = None
        self.__current_model_loaded_weight_flag = False
        self.__base_state_dict_cache_dict: dict[tuple[str, str], Union[dict, None]] = {}
        self.__random_initial_state_dict_cache_dict: dict[tuple[str, str], dict] = {}
        self.__source_eeg = None
        self.__source_eeg_device: Union[AlgorithmDeviceObject, None] = None
        self.__channel_number = 0
        self.__trial_point = 0
        # 当前算法正在处理的数据阶段四元组：
        # (subject_id, exp_name, exp_task, session_id)
        # exp_name: vmi/vme
        # exp_task: left_vs_rest / right_vs_rest
        self.__current_stage_signature: Union[tuple[str, str, str, str], None] = None
        self.__pending_data_object: Union[AlgorithmContinuousDataObject, None] = None
        self.__data_finished_flag = False
        self.__predict_timeout_seconds: float | None = None
        self.__predict_worker_manager = PredictWorkerManager()

    def set_proxy(self, proxy):
        super().set_proxy(proxy)
        self.__sync_algorithm_config_to_proxy()

    def __reset_runtime_state(self) -> None:
        self.__source_eeg = None
        self.__source_eeg_device = None
        self.__channel_number = 0
        self.__sample_rate = 0
        self.__trial_point = 0
        self.__current_stage_signature = None
        self.__pending_data_object = None
        self.__data_finished_flag = False

    def get_required_channel_labels(self) -> dict[str, list[str]]:
        # baseline示例明确声明所需通道，框架会在算法初始化后完成校验与重排。
        return copy.deepcopy(self.__required_channel_labels)

    async def calibrate(self) -> CalibrationStageResultObject:
        # 这是每一轮 session 的“准备阶段”。
        # 外层中控会循环执行：
        # while await algorithm.calibrate():
        #     await algorithm.run()
        #
        # 因此 calibrate() 的职责不是只训练模型，而是：
        # 1. 判断还有没有下一个 session；
        # 2. 若有，则把当前 session 的模型和训练数据都准备好；
        # 3. 若没有，则返回 False 告诉外层流程结束。
        # 第一次进入时，先完成 source 和 device 的初始化。
        ##
        # 如果你的算法完全不需要校准，正确做法是“保留 calibrate() 作为阶段同步入口”，
        # 而不是删除它或跳过 get_calibration()：
        #
        #   1. 仍然执行 __ensure_runtime_initialized()
        #   2. 仍然 await self.__source_eeg.get_calibration()，让流程正常推进到下一个 session
        #   3. 如果 calibration_object.finish_flag 为 True，返回 False，表示流程结束
        #   4. 否则记录当前 subject_id / exp_name / exp_task / session_id
        #   5. 如有需要，初始化当前 session 要使用的模型
        #   6. 不训练，直接返回 True，让外层继续进入 run()
        #
        # 框架在选手申请 0 个校准 trial 时，也会把当前 session 的校准包正常送达；
        # 此时 session_data.shape[0] 会是 0
        if self.__data_finished_flag:
            self.__reset_runtime_state()
        await self.__ensure_runtime_initialized()
        await self.__report_calibration_progress(
            status='waiting',
            progress=0.0,
            message='等待校准数据',
        ) # 这个函数用来统一记录当前校准阶段进度，便于日志和后续监控扩展

        calibration_object = await self.__source_eeg.get_calibration()

        if calibration_object.finish_flag:
            # 没有新的校准阶段了，整个算法流程结束。
            await self.__report_calibration_progress(
                status='finished',
                progress=100.0,
                message='数据源校准阶段已全部结束',
                subject_id=self.__current_stage_signature[0] if self.__current_stage_signature is not None else None,
                exp_name=self.__current_stage_signature[1] if self.__current_stage_signature is not None else None,
                exp_task=self.__current_stage_signature[2] if self.__current_stage_signature is not None else None,
                session_id=self.__current_stage_signature[3] if self.__current_stage_signature is not None else None,
            )
            return CalibrationStageResultObject(
                stage_context=None,
                calibration_ready=False,
            )

        # stage_signature 唯一标识当前阶段：subject_id + exp_name + exp_task + session_id。
        self.__current_stage_signature = (
            calibration_object.subject_id,
            calibration_object.exp_name,
            calibration_object.exp_task,
            calibration_object.session_id,
        )
        # 这四个字段共同定义“当前算法正在处理哪一个阶段”。
        # run() 之后收到的 online 数据也会用同样的 signature 做匹配，
        # 一旦切到新 signature，就说明该进入下一轮 calibrate() 了。
        await self.__report_calibration_progress(
            status='preparing',
            progress=5.0,
            message='收到校准数据，准备初始化模型',
            subject_id=calibration_object.subject_id,
            exp_name=calibration_object.exp_name,
            exp_task=calibration_object.exp_task,
            session_id=calibration_object.session_id,
        )
        self.__logger.info(
            "进入任务阶段: %s/%s/%s",
            calibration_object.exp_name,
            calibration_object.exp_task,
            calibration_object.session_id,
        )
        # 根据当前 session 的阶段信息准备模型。
        self.__ensure_model_ready(
            subject_id=calibration_object.subject_id,
            exp_name=calibration_object.exp_name,
            exp_task=calibration_object.exp_task,
            session_id=calibration_object.session_id,
            channel_number=self.__channel_number,
            trial_point=self.__trial_point,
        )
        await self.__calibrate(calibration_object, channel_number=self.__channel_number)
        return CalibrationStageResultObject(
            stage_context=StageContextObject(
                subject_id=calibration_object.subject_id,
                exp_name=calibration_object.exp_name,
                exp_task=calibration_object.exp_task,
                session_id=calibration_object.session_id,
            ),
            calibration_ready=True,
        )

    async def run(self) -> bool:
        # 这是框架在线阶段的包装层：
        # 1. 从连续流中切出完整 trial；
        # 2. 调用公开 predict() 接口；
        # 3. 统一处理 timeout、阶段切换和结果上报。
        # 参赛选手通常不需要直接改这里，而是修改 predict()。
        if self.__current_stage_signature is None:
            raise RuntimeError("run() 调用前必须先完成 calibrate()")
        # online 阶段是一个连续流。
        # 这里用 data_cache 把多包数据拼起来，再根据 trigger 切出完整 trial。
        #
        # 你可以把 run() 当成“在线切窗器 + 推理器”：
        # 1. 不断从 source_eeg 拿连续数据包；
        # 2. 把包拼进 data_cache；
        # 3. 在 trigger 通道里搜索 trial_start；
        # 4. 一旦凑够一个完整 trial，就立即推理；
        # 5. 如果发现数据已经切到下一个 session，就把这包暂存起来并返回 True。
        #
        # 计时部分数据流从发送trial结束trigger， 到收到结果为止
        data_cache: Union[np.ndarray, None] = None
        data_cache_start_point: Union[int, None] = None
        while not self.__data_finished_flag and not self._end_flag:
            if self.__pending_data_object is not None:
                algorithm_data_object = self.__pending_data_object
                self.__pending_data_object = None
            else:
                algorithm_data_object = await self.__source_eeg.get_data()
            packet_receive_wallclock = time.time()

            self.__data_finished_flag = bool(algorithm_data_object.finish_flag)
            if self.__data_finished_flag:
                return False
            if ContinuousDataSourceReceiver.is_return_to_calibration_marker(algorithm_data_object):
                self.__logger.info(
                    "online阶段收到 return-to-calibration marker，退出 run() 并回到 calibrate(): current_stage=%s marker_other_information=%s",
                    self.__current_stage_signature,
                    algorithm_data_object.other_information,
                )
                return True

            data_other_information = algorithm_data_object.other_information or {}
            data_signature = (
                algorithm_data_object.subject_id,
                data_other_information.get('exp_name'),
                data_other_information.get('exp_task'),
                data_other_information.get('session_id'),
            )
            if data_signature != self.__current_stage_signature:
                # online 流已经切到下一 session，先把当前包存起来，交给下一轮 calibrate()/run() 使用。
                # 这一步很关键，它保证 session 边界不会被吞掉。
                self.__pending_data_object = algorithm_data_object
                return True

            # 连续流分包到达时，这里负责把它们重新拼成一段更长的缓存。
            # ---
            new_data = algorithm_data_object.data
            if new_data is None:
                continue
            packet_start_position = algorithm_data_object.start_position
            packet_end_position = (
                packet_start_position + new_data.shape[1]
                if packet_start_position is not None else None
            )
            receiver_queue_wait_ms = getattr(algorithm_data_object, '_receiver_queue_wait_ms', None)
            receiver_enqueue_wallclock = getattr(algorithm_data_object, '_receiver_enqueue_wallclock', None)
            self.__logger.debug(
                "算法侧run收到在线数据包: stage_signature=%s packet_start_position=%s packet_end_position=%s "
                "sample_count=%s receiver_queue_wait_ms=%s trigger_summary=%s data_cache_start_before=%s "
                "data_cache_width_before=%s",
                self.__current_stage_signature,
                packet_start_position,
                packet_end_position,
                new_data.shape[1],
                f"{receiver_queue_wait_ms:.3f}" if receiver_queue_wait_ms is not None else "unknown",
                ContinuousDataSourceReceiver._ContinuousDataSourceReceiver__summarize_trigger_channel(new_data),
                data_cache_start_point,
                data_cache.shape[1] if data_cache is not None else 0,
            )
            if data_cache is None:
                data_cache = new_data
                data_cache_start_point = algorithm_data_object.start_position
            else:
                data_cache = np.concatenate((data_cache, new_data), axis=1)
            # ---
            current_data_position = algorithm_data_object.start_position + new_data.shape[1]
            # latest_complete_trial_start_point 表示：
            # “按当前已经收到的在线数据长度，最晚允许被视为完整trial起点的绝对位置”。
            latest_complete_trial_start_point = current_data_position - self.__trial_point
            # 再把“绝对位置上的最晚完整trial起点”换算成当前缓存内的右边界偏移。
            # 这里继续保留“+1”的包含边界语义：
            # 如果某个 trial_start 恰好等于 latest_complete_trial_start_point，
            # 说明从它开始刚好能截满一个完整 trial，这种情况必须被纳入检索。
            search_end_offset = max(0, latest_complete_trial_start_point - data_cache_start_point + 1)
            # 最后一行是 trigger 通道。
            search_trigger_data = data_cache[self.__channel_number, :search_end_offset]
            trial_start_index_list = np.where(np.isin(search_trigger_data, self.__trial_start_trigger))[0].tolist()
            trial_data_list = list[np.ndarray]()
            trial_context_list = list[dict]()
            for trial_start_index in trial_start_index_list:
                # 只要从起点往后能截满一个完整 trial_point，就交给模型。
                trial_data = data_cache[:, trial_start_index:trial_start_index + self.__trial_point]
                if trial_data.shape[1] == self.__trial_point:
                    trial_data_list.append(trial_data)
                    trial_context_list.append(
                        {
                            'trial_start_point': data_cache_start_point + trial_start_index,
                            'trial_end_point': data_cache_start_point + trial_start_index + self.__trial_point,
                        }
                    )

            if len(trial_data_list) > 1:
                skipped_trial_context_list = trial_context_list[:-1]
                kept_trial_context = trial_context_list[-1]
                self.__logger.warning(
                    "检测到在线推理积压，跳过已过时完整 trial，避免慢 trial 持续拖累后续 trial: "
                    "stage_signature=%s skipped_trial_count=%s skipped_trial_end_point_list=%s "
                    "kept_trial_end_point=%s data_cache_start_point=%s data_cache_width=%s",
                    self.__current_stage_signature,
                    len(skipped_trial_context_list),
                    [context.get('trial_end_point') for context in skipped_trial_context_list],
                    kept_trial_context.get('trial_end_point'),
                    data_cache_start_point,
                    data_cache.shape[1],
                )
                trial_data_list = [trial_data_list[-1]]
                trial_context_list = [kept_trial_context]

            # 每切出一个完整 trial，就立即预测并上报一次结果。
            for trial_data, trial_context in zip(trial_data_list, trial_context_list):
                trial_pipeline_start_wallclock = time.time()
                predict_dispatch_wallclock = trial_pipeline_start_wallclock
                self.__logger.debug(
                    "算法侧检测到完整 trial，准备提交 predict worker: stage_signature=%s trial_start_point=%s "
                    "trial_end_point=%s data_cache_start_point=%s data_cache_width=%s "
                    "packet_start_position=%s packet_end_position=%s receiver_queue_wait_ms=%s "
                    "packet_enqueue_to_detect_ms=%s packet_dequeue_to_detect_ms=%.3f",
                    self.__current_stage_signature,
                    trial_context.get('trial_start_point'),
                    trial_context.get('trial_end_point'),
                    data_cache_start_point,
                    data_cache.shape[1],
                    packet_start_position,
                    packet_end_position,
                    f"{receiver_queue_wait_ms:.3f}" if receiver_queue_wait_ms is not None else "unknown",
                    (
                        f"{max(0.0, (trial_pipeline_start_wallclock - float(receiver_enqueue_wallclock)) * 1000.0):.3f}"
                        if receiver_enqueue_wallclock is not None else "unknown"
                    ),
                    (trial_pipeline_start_wallclock - packet_receive_wallclock) * 1000.0,
                )
                try:
                    result = await self.__predict_with_worker(trial_data=trial_data)
                except PredictWorkerTimeoutError:
                    self.__logger.warning(
                        "predict worker timeout，当前 trial 不上报结果，等待平台侧 timeout 计分: stage_signature=%s "
                        "trial_start_point=%s trial_end_point=%s timeout_seconds=%s worker_wait_ms=%.3f "
                        "total_pipeline_ms=%.3f",
                        self.__current_stage_signature,
                        trial_context.get('trial_start_point'),
                        trial_context.get('trial_end_point'),
                        self.__predict_timeout_seconds,
                        (time.time() - predict_dispatch_wallclock) * 1000.0,
                        (time.time() - trial_pipeline_start_wallclock) * 1000.0,
                    )
                    continue
                predict_complete_wallclock = time.time()
                self.__logger.info(
                    "predict worker 返回结果: stage_signature=%s trial_start_point=%s trial_end_point=%s "
                    "worker_wait_ms=%.3f result=%s",
                    self.__current_stage_signature,
                    trial_context.get('trial_start_point'),
                    trial_context.get('trial_end_point'),
                    (predict_complete_wallclock - predict_dispatch_wallclock) * 1000.0,
                    result,
                )
                report_dispatch_wallclock = time.time()
                await self._proxy.report(AlgorithmResultObject(result=result))
                report_complete_wallclock = time.time()
                self.__logger.debug(
                    "算法侧 trial 已完成 report 调用: stage_signature=%s trial_start_point=%s trial_end_point=%s "
                    "predict_ms=%.3f report_rpc_ms=%.3f total_pipeline_ms=%.3f "
                    "packet_start_position=%s packet_end_position=%s packet_enqueue_to_report_ms=%s",
                    self.__current_stage_signature,
                    trial_context.get('trial_start_point'),
                    trial_context.get('trial_end_point'),
                    (predict_complete_wallclock - predict_dispatch_wallclock) * 1000.0,
                    (report_complete_wallclock - report_dispatch_wallclock) * 1000.0,
                    (report_complete_wallclock - trial_pipeline_start_wallclock) * 1000.0,
                    packet_start_position,
                    packet_end_position,
                    (
                        f"{max(0.0, (report_complete_wallclock - float(receiver_enqueue_wallclock)) * 1000.0):.3f}"
                        if receiver_enqueue_wallclock is not None else "unknown"
                    ),
                )
            # 缓存裁剪必须与上面的检索窗口保持完全一致：
            # 1. 上面已经把 latest_complete_trial_start_point 作为“包含边界”的完整trial起点上限；
            # 2. 因此这里也必须同步丢弃到同一个边界位置为止的缓存；
            # 3. 如果检索和裁剪的边界不一致，就会出现两种经典错误：
            #    - 裁得过多：首个合法trial或新阶段首个trial被提前丢掉；
            #    - 裁得过少：边界trial在下一轮再次进入检索窗口，形成重复计分。
            #
            # 这里直接复用 search_end_offset 作为裁剪长度，
            # 等价于“把已经确认不可能再产生新完整trial起点的那一段缓存全部丢掉”。
            # 前面已经检查过的那段缓存可以丢掉，避免 data_cache 无限增长。
            data_cache = data_cache[:, search_end_offset:]
            data_cache_start_point += search_end_offset
        return False
    async def __ensure_runtime_initialized(self) -> None:
        if self.__source_eeg is not None and self.__source_eeg_device is not None:
            return

        # 这一步拿到的 source_eeg 为框架提供的数据入口。
        # 它背后并不直接等于 Collector，而是经过了 ProcessHub/BusinessManager 的再封装。
        # 从 BusinessManager 中拿到 source receiver。
        self.__source_eeg = self._proxy.get_source('eeg_1')
        # 框架会把默认配置和外部传入配置合并。
        # 所以你改超参数时，既可以直接改本文件默认值，也可以走 challenge 的 push config 机制。
        self.__runtime_config = self.__merge_dict(
            copy.deepcopy(self.__algorithm_config),
            self._proxy.get_algorithm_config() or {},
        )
        runtime_baseline_model_config = self.__runtime_config.setdefault('baseline_model', {})
        runtime_baseline_model_config['weight_root'] = self.__DEFAULT_MODEL_ARTIFACT_ROOT
        self.__sync_algorithm_config_to_proxy(self.__runtime_config)
        runtime_predict_timeout_seconds = self.__runtime_config.get('predict_timeout_seconds')
        if runtime_predict_timeout_seconds not in (None, ''):
            self.__predict_worker_manager.set_timeout_seconds(float(runtime_predict_timeout_seconds))
        self.__predict_timeout_seconds = self.__predict_worker_manager.get_timeout_seconds()
        self.__torch_device = self.__resolve_torch_device()
        self.__log_torch_device_usage(log_context='runtime_initialized')
        # Device 信息必须在算法开始前先拿到。
        self.__source_eeg_device = await self.__source_eeg.get_device()
        self.__channel_number = self.__source_eeg_device.channel_number
        sample_rate = int(self.__source_eeg_device.sample_rate)
        self.__sample_rate = sample_rate
        self.__trial_point = int(self.__trial_duration * sample_rate)
        self.__logger.info(
            "算法收到设备信息: channel_number=%s channel_label=%s sample_rate=%s",
            self.__channel_number,
            self.__source_eeg_device.channel_label,
            sample_rate,
        )
        # 后面所有 trial 切分长度都基于这里的 sample_rate：
        # trial_point = 4 秒 * sample_rate。

    def __ensure_model_ready(
        self,
        subject_id: str,
        exp_name: str,
        exp_task: str,
        session_id: str,
        channel_number: int,
        trial_point: int,
    ) -> None:
        # 这个函数的目标是：
        # “确保当前 session 有一份可以立即训练/推理的模型实例”。
        #
        # 注意它不训练，只做准备：
        # 1. 判断当前模型是否已经对应这个 session；
        # 2. 如果不是，就重新实例化模型；
        # 3. 加载基线权重或回退到固定随机初始权重。
        stage_signature = (subject_id, exp_name, exp_task, session_id)
        if self.__current_model is not None and self.__current_model_signature == stage_signature:
            return

        # 选手如果希望针对不同范式(exp_name)或不同二分类任务(exp_task)切换模型结构/权重，
        # 1. 先根据当前阶段重新实例化目标模型；
        # 2. 再调用 __load_base_state_dict() 或自定义加载逻辑读取对应权重；
        # 3. 后续 calibrate()/run() 就会自动复用当前阶段已经准备好的模型。
        # 当前 baseline 默认对每个 subject/session 都重新实例化模型，
        # 但初始权重仍按 (exp_name, exp_task) 共享。
        # 当前 baseline 按“每个 session 初始化一份模型”的思路处理。
        self.__current_model = EEGNet(
            channel_number=channel_number,
            sample_number=trial_point,
            class_number=2,
        ).to(self.__torch_device)
        self.__current_model_signature = stage_signature
        # 这里每个 subject/session 都重新 new 一次模型，
        # 所以默认 baseline 不会把上一个被试或上一个 session 的训练结果延续过来。

        base_state_dict = self.__load_base_state_dict(exp_name=exp_name, exp_task=exp_task)
        if base_state_dict is not None:
            self.__current_model.load_state_dict(copy.deepcopy(base_state_dict), strict=True)
            self.__current_model_loaded_weight_flag = True
            self.__logger.info("已加载EEGNet基线权重并用于session独立初始化: %s/%s/%s", exp_name, exp_task, session_id)
        else:
            random_initial_state_key = (exp_name, exp_task)
            if random_initial_state_key not in self.__random_initial_state_dict_cache_dict:
                # 没有预训练权重时，缓存当前随机初始化参数，保证同一exp/task下不同session的初始状态一致。
                self.__random_initial_state_dict_cache_dict[random_initial_state_key] = copy.deepcopy(
                    self.__current_model.state_dict()
                )
            self.__current_model.load_state_dict(
                copy.deepcopy(self.__random_initial_state_dict_cache_dict[random_initial_state_key]),
                strict=True,
            )
            self.__current_model_loaded_weight_flag = False
            self.__logger.warning(
                "未找到EEGNet基线权重，将使用缓存随机初始权重做session独立初始化: %s/%s/%s",
                exp_name,
                exp_task,
                session_id,
            )
        self.__current_model.eval()

    def __load_base_state_dict(self, exp_name: str, exp_task: str) -> Union[dict, None]:
        # 这里负责寻找“当前范式/任务对应的预训练初始权重”。
        # 找到后会缓存到 __base_state_dict_cache_dict，避免重复读盘。
        # 比赛规则要求：所有参与在线推理的已学习参数、模板、权重、统计量，
        # 必须全部存放于指定模型目录 method/model_artifacts；目录外参数一律视为违规。
        # 因此 baseline 只允许从指定模型目录树内搜索静态模型工件。
        stage_signature = (exp_name, exp_task)
        if stage_signature in self.__base_state_dict_cache_dict:
            return self.__base_state_dict_cache_dict[stage_signature]

        weight_root = self.__resolve_model_artifact_root_path(self.__runtime_config)
        candidate_path_list = [
            weight_root / f'{exp_name}_{exp_task}_eegnet.pth',
            weight_root / f'{exp_name}_{exp_task}.pth',
            weight_root / f'{exp_name}_{exp_task}.pt',
            weight_root / 'baseline' / f'{exp_name}_{exp_task}_eegnet.pth',
            weight_root / 'baseline' / f'{exp_name}_{exp_task}.pth',
            weight_root / 'baseline' / f'{exp_name}_{exp_task}.pt',
        ]
        if weight_root.exists():
            candidate_name_set = {candidate_path.name for candidate_path in candidate_path_list}
            for candidate_path in weight_root.rglob('*'):
                if not candidate_path.is_file() or candidate_path.name not in candidate_name_set:
                    continue
                candidate_path_list.append(candidate_path)
        for candidate_path in candidate_path_list:
            if candidate_path.exists():
                base_state_dict = torch.load(candidate_path, map_location=self.__torch_device)
                self.__base_state_dict_cache_dict[stage_signature] = base_state_dict
                return base_state_dict

        self.__base_state_dict_cache_dict[stage_signature] = None
        return None

    def __sync_algorithm_config_to_proxy(self, algorithm_config: dict[str, object] | None = None) -> None:
        if self._proxy is None:
            return
        self._proxy.set_algorithm_config(copy.deepcopy(algorithm_config or self.__algorithm_config))

    @classmethod
    def __resolve_model_artifact_root_path(cls, config_dict: dict[str, object] | None) -> Path:
        baseline_model_config = {}
        if isinstance(config_dict, dict):
            baseline_model_config = config_dict.get('baseline_model', {}) or {}
        weight_root = Path(str(baseline_model_config.get('weight_root', cls.__DEFAULT_MODEL_ARTIFACT_ROOT)))
        if weight_root.is_absolute():
            return weight_root
        current_file_path = Path(__file__).resolve()
        package_root = next(
            (
                candidate_path
                for candidate_path in current_file_path.parents
                if (candidate_path / 'method').is_dir() and (candidate_path / 'service').is_dir()
            ),
            current_file_path.parents[3],
        )
        return (package_root / weight_root).resolve()

    async def __calibrate(self, calibration_object: AlgorithmCalibrationObject, channel_number: int) -> None:
        # 这里才是真正的“训练/微调”逻辑。
        # 进入这个函数时，默认前提已经成立：
        # 1. device 信息已获取；
        # 2. 当前 session 的模型实例已准备好；
        # 3. calibration_object 只包含当前 session 的校准数据。
        session_id = calibration_object.session_id
        session_data_dict = calibration_object.session_data_dict.get(session_id, {})
        session_data = np.asarray(session_data_dict.get('data'), dtype=np.float32)
        session_label = np.asarray(session_data_dict.get('label'), dtype=np.int64)
        # 当前框架为 session-by-session 独立阶段：
        # 1. Collector 只发送当前session的校准trial；
        # 2. baseline 会在进入该session前回到同一份初始权重；
        # 3. 该session训练完成后，仅使用该session对应模型处理后续online trial。
        calibration_summary_dict = {
            session_id: {
                'trial_count': int(session_label.shape[0]),
                'positive_count': int(np.sum(session_label == 1)), # left 和 right 已经转化为1
                'negative_count': int(np.sum(session_label == 0)), # rest 已经转化为0
            }
        }
        self.__logger.info(
            "收到校准数据 %s/%s/%s: %s",
            calibration_object.exp_name,
            calibration_object.exp_task,
            session_id,
            calibration_summary_dict,
        )
        await self.__report_calibration_progress(
            status='received',
            progress=10.0,
            message=f"已收到校准数据: {calibration_summary_dict}",
            subject_id=calibration_object.subject_id,
            exp_name=calibration_object.exp_name,
            exp_task=calibration_object.exp_task,
            session_id=session_id,
        )

        if session_data.shape[0] == 0:
            # 没有校准 trial 时，不报错，而是直接使用当前初始化模型进入 online。
            # 这保证了即使某个 session 没训练数据，流程也不会断。
            self.__logger.warning(
                "任务 %s/%s/%s 没有收到校准trial，直接使用基线模型推理",
                calibration_object.exp_name,
                calibration_object.exp_task,
                session_id,
            )
            self.__current_model.eval()
            await self.__sync_predict_worker_model()
            await self.__report_calibration_progress(
                status='ready',
                progress=100.0,
                message='没有可用校准trial，直接进入online推理',
                subject_id=calibration_object.subject_id,
                exp_name=calibration_object.exp_name,
                exp_task=calibration_object.exp_task,
                session_id=session_id,
            )
            return

        # 先把校准 trial 做预处理，变成模型输入。
        train_x = self.__preprocessor.preprocess_trial_batch(
            trial_batch=session_data,
            sample_rate=self.__sample_rate,
            device=self.__torch_device,
        )
        train_y = torch.as_tensor(session_label, dtype=torch.long, device=self.__torch_device)
        # preprocess_trial_batch() 负责把 numpy trial 批次变成 EEGNet 所需张量形状。
        # 如果你替换模型，通常这里和 predict() 里的预处理要一起改。

        baseline_model_config = self.__runtime_config.get('baseline_model', {})
        calibration_epochs = int(baseline_model_config.get('calibration_epochs', 8))
        calibration_learning_rate = float(baseline_model_config.get('calibration_learning_rate', 1e-3))
        classifier_only_finetune = bool(baseline_model_config.get('classifier_only_finetune', True))
        if not self.__current_model_loaded_weight_flag:
            classifier_only_finetune = False

        if classifier_only_finetune:
            for parameter in self.__current_model.parameters():
                parameter.requires_grad = False
            for parameter in self.__current_model.classifier.parameters():
                parameter.requires_grad = True
        else:
            # 没有预训练权重时，只有分类头可训练通常不够，因此改成全量训练。
            for parameter in self.__current_model.parameters():
                parameter.requires_grad = True

        optimizer = torch.optim.Adam(
            params=[parameter for parameter in self.__current_model.parameters() if parameter.requires_grad],
            lr=calibration_learning_rate,
        )
        criterion = nn.CrossEntropyLoss()

        self.__current_model.train()
        await self.__report_calibration_progress(
            status='training',
            progress=15.0,
            message='开始执行模型校准训练',
            subject_id=calibration_object.subject_id,
            exp_name=calibration_object.exp_name,
            exp_task=calibration_object.exp_task,
            session_id=session_id,
            current_epoch=0,
            total_epoch=calibration_epochs,
        )
        # finetune 部分。
        for epoch_index in range(calibration_epochs):
            optimizer.zero_grad()
            logits = self.__current_model(train_x)
            loss = criterion(logits, train_y)
            loss.backward()
            optimizer.step()
            await self.__report_calibration_progress(
                status='training',
                progress=15.0 + 80.0 * float(epoch_index + 1) / float(max(calibration_epochs, 1)),
                message=f'校准训练中，loss={float(loss.item()):.6f}',
                subject_id=calibration_object.subject_id,
                exp_name=calibration_object.exp_name,
                exp_task=calibration_object.exp_task,
                session_id=session_id,
                current_epoch=epoch_index + 1,
                total_epoch=calibration_epochs,
            )
        self.__current_model.eval()
        await self.__sync_predict_worker_model()
        await self.__report_calibration_progress(
            status='ready',
            progress=100.0,
            message='模型校准完成，等待online数据',
            subject_id=calibration_object.subject_id,
            exp_name=calibration_object.exp_name,
            exp_task=calibration_object.exp_task,
            session_id=session_id,
            current_epoch=calibration_epochs,
            total_epoch=calibration_epochs,
        )

    def predict(self, trial_data: np.ndarray) -> str:
        # 这是参赛选手应该直接修改的在线预测入口。
        # 输入是“已经切好的单个完整 trial”，最后一行仍包含 trigger 通道。
        # 输入的 trial_data 已经是一个完整 trial，最后一行仍包含 trigger 通道。
        # preprocess_single_trial() 会把它整理成模型输入张量。
        input_tensor = self.__preprocessor.preprocess_single_trial(
            trial_data=trial_data,
            channel_number=self.__channel_number,
            sample_rate=self.__sample_rate,
            device=self.__torch_device,
        )
        with torch.no_grad():
            logits = self.__current_model(input_tensor)
            predict_label = int(torch.argmax(logits, dim=1).item())
        payload = {
            "predict_label": predict_label,
        }
        result_str = json.dumps(payload, ensure_ascii=False)
        return result_str

    async def __predict_with_worker(self, trial_data: np.ndarray) -> str:
        return await self.__predict_worker_manager.predict(trial_data=trial_data)

    def load_predict_session(
        self,
        runtime_config: dict,
        stage_signature: tuple[str, str, str, str],
        sample_rate: int,
        channel_number: int,
        trial_point: int,
        model_state_dict: dict,
    ) -> str:
        self.__runtime_config = self.__merge_dict(
            copy.deepcopy(self.__algorithm_config),
            copy.deepcopy(runtime_config or {}),
        )
        self.__sample_rate = int(sample_rate)
        self.__channel_number = int(channel_number)
        self.__trial_point = int(trial_point)
        self.__current_stage_signature = tuple(stage_signature or ())
        self.__torch_device = self.__resolve_torch_device()
        self.__log_torch_device_usage(
            log_context='predict_worker_load_session',
            stage_signature=self.__current_stage_signature,
        )
        if len(self.__current_stage_signature) != 4:
            raise ValueError(f'invalid stage_signature: {self.__current_stage_signature}')
        subject_id, exp_name, exp_task, session_id = self.__current_stage_signature
        self.__ensure_model_ready(
            subject_id=subject_id,
            exp_name=exp_name,
            exp_task=exp_task,
            session_id=session_id,
            channel_number=self.__channel_number,
            trial_point=self.__trial_point,
        )
        self.__current_model.load_state_dict(copy.deepcopy(model_state_dict or {}), strict=True)
        self.__current_model.eval()
        return str(self.__torch_device)

    async def __sync_predict_worker_model(self) -> None:
        if self.__current_model is None or self.__current_stage_signature is None:
            raise RuntimeError('predict worker sync requires current model and stage signature')
        await self.__predict_worker_manager.sync_session(
            runtime_config=self.__runtime_config,
            stage_signature=self.__current_stage_signature,
            sample_rate=self.__sample_rate,
            channel_number=self.__channel_number,
            trial_point=self.__trial_point,
            model_state_dict=self.__build_predict_worker_state_dict(),
        )

    def __build_predict_worker_state_dict(self) -> dict:
        if self.__current_model is None:
            raise RuntimeError('current model is not initialized')
        return {
            key: value.detach().cpu().clone()
            for key, value in self.__current_model.state_dict().items()
        }

    async def shutdown_runtime(self) -> None:
        await self.__predict_worker_manager.shutdown()
        self.__reset_runtime_state()

    def __resolve_torch_device(self) -> torch.device:
        baseline_model_config = self.__runtime_config.get('baseline_model', {})
        configured_device = str(baseline_model_config.get('device', 'cpu')).strip().lower()
        if configured_device.startswith('cuda') and torch.cuda.is_available():
            return torch.device(configured_device)
        return torch.device('cpu')

    def __log_torch_device_usage(
        self,
        log_context: str,
        stage_signature: tuple[str, str, str, str] | None = None,
    ) -> None:
        baseline_model_config = self.__runtime_config.get('baseline_model', {})
        configured_device = str(baseline_model_config.get('device', 'cpu')).strip().lower()
        resolved_device = str(self.__torch_device)
        cuda_available = torch.cuda.is_available()
        log_payload = {
            'log_context': log_context,
            'configured_device': configured_device,
            'resolved_device': resolved_device,
            'cuda_available': cuda_available,
            'cuda_device_count': torch.cuda.device_count() if cuda_available else 0,
            'stage_signature': list(stage_signature) if stage_signature is not None else None,
        }
        if cuda_available and self.__torch_device.type == 'cuda':
            try:
                device_index = self.__torch_device.index if self.__torch_device.index is not None else torch.cuda.current_device()
                log_payload['cuda_device_index'] = device_index
                log_payload['cuda_device_name'] = torch.cuda.get_device_name(device_index)
            except Exception as exc:
                log_payload['cuda_device_name_error'] = str(exc)
        self.__logger.info("当前 torch 设备信息: %s", json.dumps(log_payload, ensure_ascii=False))

    @staticmethod
    def __merge_dict(base_dict: dict, update_dict: dict) -> dict:
        for key, value in update_dict.items():
            if isinstance(value, dict) and isinstance(base_dict.get(key), dict):
                AlgorithmImplement.__merge_dict(base_dict[key], value)
            else:
                base_dict[key] = value
        return base_dict

    async def __report_calibration_progress(
        self,
        status: str,
        progress: float,
        message: str,
        subject_id: str = None,
        exp_name: str = None,
        exp_task: str = None,
        session_id: str = None,
        current_epoch: int = None,
        total_epoch: int = None,
    ) -> None:
        # 这部分最后用于外部显示校准进度条。
        await self._proxy.report_calibration_progress(
            AlgorithmCalibrationProgressObject(
                subject_id=subject_id,
                exp_name=exp_name,
                exp_task=exp_task,
                session_id=session_id,
                status=status,
                progress=progress,
                message=message,
                current_epoch=current_epoch,
                total_epoch=total_epoch,
            )
        )

