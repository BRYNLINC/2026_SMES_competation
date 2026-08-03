import asyncio
import importlib
import json
import logging
import os
import sys
import time
import traceback
from typing import Union

from injector import inject

from Algorithm.common.enum.AlgorithmEventEnum import AlgorithmEventEnum
from Algorithm.common.enum.ServiceStatusEnum import ServiceStatusEnum
from Algorithm.common.utils.EventManager import EventManager
from Algorithm.method.interface.AlgorithmInterface import AlgorithmInterface
from Algorithm.method.interface.ProxyInterface import ProxyInterface
from Algorithm.method.model.AlgorithmObject import CalibrationStageResultObject
from Algorithm.service.interface.RpcControllerInterface import RpcControllerInterface
from Algorithm.service.interface.ServiceManagerInterface import MethodManagerInterface
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmReportMessageModel
from Common.model.CommonMessageModel import DataPackageModel, ExceptionPackageModel


class MethodManager(MethodManagerInterface):
    # 这个事件名会放进 AlgorithmReportMessage(DataPackage) 中，
    # ProcessHub 收到后会将其升级成 team_calibration_ready runtime event。
    __CALIBRATION_READY_EVENT_TYPE = "calibration_ready"
    __REQUIRED_SOURCE_LABEL_SET = {'eeg_1'}
    __MAX_CHANNEL_COUNT_PER_SOURCE = 8
    """
    算法方法管理器。

    它负责加载并驱动具体算法类：
    1. 动态导入 AlgorithmImplement；
    2. 校验算法声明的所需通道；
    3. 注入代理对象；
    4. 以后台任务形式执行 calibrate()/run() 阶段循环。

    对参赛选手而言：
    - 主要改动入口应集中在 AlgorithmImplement.__init__ / calibrate / predict；
    - run() 更像框架在线循环包装层，不建议直接修改。
    """

    @inject
    def __init__(self, method_proxy: ProxyInterface, event_manager: EventManager):
        self.__method_proxy: ProxyInterface = method_proxy
        self.__rpc_controller: RpcControllerInterface = None
        self.__event_manager: EventManager = event_manager
        self.__logger = logging.getLogger("algorithmLogger") # Algorithm控制台中打印的部分
        self.__method_instance: AlgorithmInterface = None
        self.__method_task: asyncio.tasks = None
        # 服务状态
        self.__service_status: ServiceStatusEnum = ServiceStatusEnum.STOPPED
        self.__original_stderr = sys.stderr
        self.__runtime_cleanup_done: bool = False

    async def initial_system(self, config_dict: dict[str, Union[str, dict]] = None):
        if self.__service_status is not ServiceStatusEnum.STOPPED:
            return
        # 设置服务初始化状态
        self.__service_status = ServiceStatusEnum.INITIALIZING
        self.__runtime_cleanup_done = False
        self.__method_instance = self.__load_algorithm_instance(config_dict)

        # 设置服务就绪状态
        self.__service_status = ServiceStatusEnum.READY

    async def startup(self) -> None:
        if self.__service_status not in [ServiceStatusEnum.READY, ServiceStatusEnum.ERROR]:
            return
        self.__service_status = ServiceStatusEnum.STARTING
        self.__runtime_cleanup_done = False
        if self.__method_instance is not None:
            self.__method_instance.set_end_flag(False)

        self.__method_task = asyncio.create_task(self.__run_algorithm_method())
        self.__logger.info("算法已启动")

        self.__service_status = ServiceStatusEnum.RUNNING

    async def shutdown(self) -> None:
        if self.__service_status is not ServiceStatusEnum.RUNNING:
            return
        self.__service_status = ServiceStatusEnum.STOPPING
        if self.__method_task is not None and not self.__method_task.done():
            self.__logger.info("等待算法结束")
            # 设置算法结束标志，并等待算法停止
            self.__method_instance.set_end_flag(True)
            await self.__method_task
        await self.__shutdown_algorithm_runtime_resources()
        self.__service_status = ServiceStatusEnum.READY

    def __load_algorithm_instance(self, method_config_dict: dict[str, Union[str, dict]]) -> AlgorithmInterface:
        # 按配置动态加载算法类。
        method_class_file = method_config_dict['method_class_file']
        method_class_name = method_config_dict['method_class_name']
        workspace_path = os.getcwd()
        absolute_strategy_class_file = os.path.join(workspace_path, method_class_file)
        module_name = os.path.splitext(os.path.basename(absolute_strategy_class_file))[0]
        # 获取模块所在的目录
        module_dir = os.path.dirname(absolute_strategy_class_file)
        if module_dir not in sys.path:
            sys.path.append(module_dir)
        module = importlib.import_module(module_name)
        method_class = getattr(module, method_class_name)
        instance = method_class()
        # 算法必须显式声明所需通道，框架后面会按这个列表做通道筛选和重排。
        required_channel_labels_dict = instance.get_required_channel_labels()
        self.__validate_required_channel_labels(required_channel_labels_dict)
        instance.set_proxy(self.__method_proxy)
        for source_label, channel_label_list in required_channel_labels_dict.items():
            self.__method_proxy.set_source_required_channel_labels(source_label, channel_label_list)
        return instance

    @classmethod
    def __validate_required_channel_labels(cls, required_channel_labels_dict: dict[str, list[str]]) -> None:
        if required_channel_labels_dict is None:
            raise ValueError("算法必须显式声明所需通道，get_required_channel_labels 不能返回 None")
        if not isinstance(required_channel_labels_dict, dict):
            raise TypeError("get_required_channel_labels 必须返回 dict[source_label, channel_label_list]")
        if len(required_channel_labels_dict) == 0:
            raise ValueError("算法必须显式声明所需通道，get_required_channel_labels 不能为空")

        normalized_source_label_set = {
            str(source_label).strip()
            for source_label in required_channel_labels_dict.keys()
            if str(source_label).strip() != ''
        }
        missing_source_label_list = sorted(
            cls.__REQUIRED_SOURCE_LABEL_SET - normalized_source_label_set
        )
        if missing_source_label_list:
            raise ValueError(
                "算法必须为正式数据源显式声明通道: "
                f"missing_source_labels={missing_source_label_list}"
            )
        unexpected_source_label_list = sorted(
            normalized_source_label_set - cls.__REQUIRED_SOURCE_LABEL_SET
        )
        if unexpected_source_label_list:
            raise ValueError(
                "算法声明了不存在的 source_label，正式运行仅允许固定数据源: "
                f"unexpected_source_labels={unexpected_source_label_list}"
            )

        for source_label, channel_label_list in required_channel_labels_dict.items():
            if not isinstance(source_label, str) or not source_label.strip():
                raise ValueError("source_label 不能为空")
            source_label_text = source_label.strip()
            if not isinstance(channel_label_list, (list, tuple)):
                raise TypeError(f"{source_label_text} 的通道列表必须为 list 或 tuple")
            if len(channel_label_list) == 0:
                raise ValueError(f"{source_label_text} 必须显式声明至少1个通道，不能留空")
            if len(channel_label_list) > cls.__MAX_CHANNEL_COUNT_PER_SOURCE:
                raise ValueError(
                    f"{source_label_text} 请求的通道数量不能超过{cls.__MAX_CHANNEL_COUNT_PER_SOURCE}，"
                    f"当前为 {len(channel_label_list)}"
                )

            normalized_channel_label_set = set()
            for channel_label in channel_label_list:
                normalized_channel_label = cls.__normalize_channel_label(channel_label)
                if not normalized_channel_label:
                    raise ValueError(f"{source_label_text} 的通道名不能为空: {channel_label}")
                if normalized_channel_label in normalized_channel_label_set:
                    raise ValueError(
                        f"{source_label_text} 的通道名存在重复，"
                        f"请去重后重试: {list(channel_label_list)}"
                    )
                normalized_channel_label_set.add(normalized_channel_label)

    @staticmethod
    def __normalize_channel_label(channel_label: str) -> str:
        return ''.join(char for char in str(channel_label).upper() if char.isalnum())

    async def __run_algorithm_method(self):
        try:
            while not self.__method_instance._end_flag:
                # 当前框架采用显式阶段制：
                # 先 calibrate()，再 run()。
                self.__logger.info("框架 calibrate()")
                calibration_stage_result = await self.__method_instance.calibrate()
                if not isinstance(calibration_stage_result, CalibrationStageResultObject):
                    raise TypeError(
                        "calibrate() 必须返回 CalibrationStageResultObject"
                    )
                if (
                    calibration_stage_result.stage_context is None
                    and calibration_stage_result.calibration_ready is False
                ):
                    self.__logger.info("校准阶段结束，未检测到后续online阶段，算法流程退出")
                    break
                if calibration_stage_result.calibration_ready:
                    await self.__report_calibration_ready(calibration_stage_result)

                self.__logger.info("框架 run() 处理online阶段")
                should_continue = await self.__method_instance.run()
                if not should_continue:
                    self.__logger.info("online阶段结束，未检测到后续阶段，算法流程退出")
                    break
        except Exception:
            err_str = traceback.format_exc()
            self.__original_stderr.write("[ERROR]算法执行发生异常" + err_str)
            exc_type, exc_value, exception_traceback = sys.exc_info()
            # 发送算法异常信息
            await self.__rpc_controller.report(
                AlgorithmReportMessageModel(
                    timestamp_ms=int(time.time() * 1000),
                    package=ExceptionPackageModel(
                        exception_type=str(exc_type),
                        # 异常信息被截断到 20 个字符，避免通过异常通道携带过多内容。
                        exception_message=str(exc_value) if len(str(exc_value)) <= 20 else str(exc_value)[:20],
                        exception_stack_trace=traceback.format_tb(exception_traceback)
                    )
                )
            )
        finally:
            await self.__shutdown_algorithm_runtime_resources()
            self.__logger.info("算法执行结束")
            # 发出算法结束事件,以异步方式执行，不用等待到事件处理结束
            asyncio.create_task(self.__event_manager.notify(AlgorithmEventEnum.METHOD_FINISHED.value))

    async def __shutdown_algorithm_runtime_resources(self) -> None:
        if self.__runtime_cleanup_done:
            return
        self.__runtime_cleanup_done = True
        if self.__method_instance is None:
            return
        shutdown_runtime = getattr(self.__method_instance, 'shutdown_runtime', None)
        if shutdown_runtime is None:
            return
        await shutdown_runtime()

    async def __report_calibration_ready(
        self,
        calibration_stage_result: CalibrationStageResultObject,
    ) -> None:
        # 这里不上报评分结果，而是上报“阶段已就绪”的运行时事件。
        # 之所以走 DataPackage 而不是 ResultPackage，是为了把“算法预测结果”和
        # “流程控制事件”彻底分离，避免后续计分链路误消费该消息。
        stage_context = calibration_stage_result.stage_context
        if stage_context is None:
            raise ValueError("calibration_ready=True 时 stage_context 不能为空")
        await self.__rpc_controller.report(
            AlgorithmReportMessageModel(
                timestamp_ms=int(time.time() * 1000),
                package=DataPackageModel(
                    data=json.dumps(
                        {
                            "event_type": self.__CALIBRATION_READY_EVENT_TYPE,
                            "stage_context": {
                                "subject_id": stage_context.subject_id,
                                "exp_name": stage_context.exp_name,
                                "exp_task": stage_context.exp_task,
                                "session_id": stage_context.session_id,
                            },
                            "calibration_ready": bool(calibration_stage_result.calibration_ready),
                        },
                        ensure_ascii=False,
                    )
                ),
            )
        )

    def set_rpc_controller(self, rpc_controller: RpcControllerInterface) -> None:
        self.__rpc_controller = rpc_controller
