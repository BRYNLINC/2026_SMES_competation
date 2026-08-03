import copy
import importlib
import json
import logging
import os
from pathlib import Path
import stat
import sys
import time
from typing import Union

from injector import inject

from Algorithm.common.enum.ServiceStatusEnum import ServiceStatusEnum
from Algorithm.method.interface.ProxyInterface import ProxyInterface
from Algorithm.method.model.AlgorithmObject import AlgorithmCalibrationProgressObject, AlgorithmResultObject
from Algorithm.service.exception.AlgorithmSourceException import AlgorithmSourceReceiverNotFoundException
from Algorithm.service.interface.DataForwarderInterface import DataForwarderInterface
from Algorithm.service.interface.RpcControllerInterface import RpcControllerInterface
from Algorithm.service.interface.ServiceManagerInterface import BusinessManagerInterface
from Algorithm.service.interface.SourceReceiverInterface import SourceReceiverInterface
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel, AlgorithmReportMessageModel
from Common.model.CommonMessageModel import ResultPackageModel, ReportSourceInformationModel


class BusinessManager(ProxyInterface, DataForwarderInterface, BusinessManagerInterface):
    """
    业务模块
    数据收发结果报告等核心业务逻辑处理类

    这一层位于“算法框架”和“具体算法类”之间：
    1. 接收 ProcessHub 转发来的数据；
    2. 分发给对应 source receiver；
    3. 接收算法结果并通过 RPC 返回给 ProcessHub；
    4. 保存算法配置和所需通道信息。
    """

    @inject
    def __init__(self):
        # 根据config_dict中的配置信息初始化数据源对象
        # 源接收器创建器
        self.__source_receiver_dict: dict[str, SourceReceiverInterface] \
            = dict[str, SourceReceiverInterface]()
        self.__rpc_controller: RpcControllerInterface = None
        self.__logger = logging.getLogger("algorithmLogger")

        # 服务状态
        self.__service_status: ServiceStatusEnum = ServiceStatusEnum.STOPPED

        # 源接收器工厂
        self.__source_receiver_factory_dict = dict[str, any]()
        # 源配置信息
        self.__source_config_dict = dict[str, Union[str, dict]]()
        self.__required_channel_label_dict = dict[str, list[str]]()

        self.__algorithm_config_dict = dict[str, Union[str, dict]]()
        self.__platform_model_size_mb: float | None = None
        # 校准阶段会频繁上报 progress。
        # 为避免算法控制台被每个 epoch 的日志刷满，这里只在关键节点打印：
        # 1. 非 training 状态；
        # 2. 首轮；
        # 3. 每 10 轮；
        # 4. 最后一轮。
        self.__last_calibration_progress_log_key = None

    def get_source(self, source_label: str) -> SourceReceiverInterface:
        return self.__source_receiver_dict[source_label]

    async def forward_data(self, algorithm_data_message: AlgorithmDataMessageModel):
        # 按 source_label 分发数据。
        source_label = algorithm_data_message.source_label
        self.__logger.debug(f"forward data to source: {source_label}\ndata:{type(algorithm_data_message.package)}")
        if source_label in self.__source_receiver_dict:
            await self.__source_receiver_dict[source_label].set_message_model(algorithm_data_message)
        else:
            raise AlgorithmSourceReceiverNotFoundException(f"SourceReceiver: {source_label} is not exist")
        return

    async def report(self, algorithm_result_object: AlgorithmResultObject):
        # 把算法内部结果对象包装成 AlgorithmReportMessageModel，再通过 RPCController 送回 ProcessHub。
        algorithm_report_message_model = AlgorithmReportMessageModel(
            timestamp_ms=int(time.time() * 1000),
            package=self.__convert_algorithm_result_object_to_result_package_model(algorithm_result_object)
        )
        # 这里是“predict阶段真正提交结果”的统一出口。
        # 不在 AlgorithmImplement 内打印，避免选手改算法文件时把平台侧调试日志一起改坏。
        # 后续你只看 algorithm 控制台，就能直接知道：
        # 1. 结果在什么时刻提交给 ProcessHub；
        # 2. 提交的原始预测内容是什么；
        # 3. 该结果引用了哪些 source position。
        self.__logger.debug(
            "predict result submitted: submit_timestamp_ms=%s result=%s report_source_information=%s",
            algorithm_report_message_model.timestamp_ms,
            self.__summarize_algorithm_result_for_log(algorithm_result_object.result),
            self.__summarize_report_source_information_for_log(
                algorithm_report_message_model.package.report_source_information
            ),
        )
        rpc_dispatch_wallclock = time.time()
        await self.__rpc_controller.report(algorithm_report_message_model)
        rpc_complete_wallclock = time.time()
        self.__logger.debug(
            "predict result rpc completed: report_timestamp_ms=%s rpc_elapsed_ms=%.3f result=%s report_source_information=%s",
            algorithm_report_message_model.timestamp_ms,
            (rpc_complete_wallclock - rpc_dispatch_wallclock) * 1000.0,
            self.__summarize_algorithm_result_for_log(algorithm_result_object.result),
            self.__summarize_report_source_information_for_log(
                algorithm_report_message_model.package.report_source_information
            ),
        )
        return

    def get_algorithm_config(self) -> dict[str, Union[str, dict]]:
        return self.__algorithm_config_dict

    def set_algorithm_config(self, algorithm_config_dict: dict[str, Union[str, dict]]):
        self.__algorithm_config_dict = algorithm_config_dict

    def set_source_required_channel_labels(self, source_label: str, channel_label_list: list[str]):
        normalized_channel_label_list = list(channel_label_list or [])
        self.__required_channel_label_dict[source_label] = normalized_channel_label_list
        if source_label in self.__source_receiver_dict:
            self.__source_receiver_dict[source_label].set_required_channel_labels(normalized_channel_label_list)

    async def report_calibration_progress(
        self,
        calibration_progress_object: AlgorithmCalibrationProgressObject,
    ):
        if not self.__should_log_calibration_progress(calibration_progress_object):
            return
        self.__logger.info(
            "校准进度 subject=%s exp=%s task=%s session=%s status=%s progress=%.1f message=%s epoch=%s/%s",
            calibration_progress_object.subject_id,
            calibration_progress_object.exp_name,
            calibration_progress_object.exp_task,
            calibration_progress_object.session_id,
            calibration_progress_object.status,
            float(calibration_progress_object.progress or 0.0),
            calibration_progress_object.message,
            calibration_progress_object.current_epoch,
            calibration_progress_object.total_epoch,
        )

    async def initial_system(self, config_dict: dict[str, Union[str, dict]] = None):
        if self.__service_status is not ServiceStatusEnum.STOPPED:
            return
        # 设置服务初始化状态
        self.__service_status = ServiceStatusEnum.INITIALIZING

        # 读取 source_receiver_handlers 和 sources 两部分配置。
        # 前者决定“有哪些接收器类型可以创建”，后者决定“当前实际启用哪些 source”。
        if 'source_receiver_handlers' in config_dict:
            # 加载数据源接收器配置
            source_receiver_config_dict = config_dict['source_receiver_handlers']
            self.__load_source_receiver_handles(source_receiver_config_dict)
            self.__logger.info(f"已经加载数据源处理器{source_receiver_config_dict}")
        if 'sources' in config_dict:
            self.__source_config_dict = config_dict['sources']
            self.__logger.info(f"已缓存源配置信息{self.__source_config_dict}")
        self.__platform_model_size_mb = self.__measure_platform_model_size_mb()

        # 设置服务就绪状态
        self.__service_status = ServiceStatusEnum.READY

    async def receive_config(self, config_dict: dict[str, Union[str, dict]]):
        self.__algorithm_config_dict.update(config_dict)

    async def get_config(self) -> dict[str, Union[str, dict]]:
        config_dict = {}
        calibration_trials_per_class_requested = self.__algorithm_config_dict.get(
            'calibration_trials_per_class_requested'
        )
        if calibration_trials_per_class_requested is not None:
            config_dict['calibration_trials_per_class_requested'] = calibration_trials_per_class_requested
        requested_channel_labels = {
            source_label: list(channel_label_list)
            for source_label, channel_label_list in self.__required_channel_label_dict.items()
        }
        config_dict['requested_channel_labels'] = requested_channel_labels
        config_dict['requested_channel_count'] = sum(
            len(channel_label_list)
            for channel_label_list in requested_channel_labels.values()
        )
        if self.__platform_model_size_mb is not None:
            config_dict['platform_model_size_mb'] = self.__platform_model_size_mb
        return config_dict

    async def startup(self):
        if self.__service_status not in [ServiceStatusEnum.READY, ServiceStatusEnum.ERROR]:
            return
        self.__service_status = ServiceStatusEnum.STARTING
        self.__source_receiver_dict.clear()

        # 根据 sources 配置实例化 source receiver。
        for source_label in self.__source_config_dict:
            source_receiver_dict = self.__source_config_dict[source_label]['source_receiver']
            handler_name = source_receiver_dict['handler']
            configuration_dict = source_receiver_dict['configuration']
            self.__source_receiver_dict[source_label] = self.__get_source_receiver_instance(
                source_label,
                handler_name,
                configuration_dict
            )
        self.__logger.info(f"已经初始化数据源接收器{self.__source_receiver_dict}")

        self.__service_status = ServiceStatusEnum.RUNNING

    async def shutdown(self):
        if self.__service_status is not ServiceStatusEnum.RUNNING:
            return
        self.__service_status = ServiceStatusEnum.STOPPING
        # 清理所有数据源接收器
        self.__source_receiver_dict.clear()
        self.__source_config_dict.clear()
        self.__logger.info("业务管理器已关闭")
        self.__service_status = ServiceStatusEnum.READY

    def __load_source_receiver_handles(self, source_receiver_handlers_dict: dict[str, Union[str, dict]]):
        # 辅助读取源接收器配置信息
        workspace_path = os.getcwd()
        for source_receiver_name in source_receiver_handlers_dict:
            receiver_config_dict = source_receiver_handlers_dict[source_receiver_name]
            receiver_class_file = receiver_config_dict['receiver_class_file']
            receiver_class_name = receiver_config_dict['receiver_class_name']
            absolute_receiver_class_file = os.path.join(workspace_path, receiver_class_file)
            module_name = os.path.splitext(os.path.basename(absolute_receiver_class_file))[0]
            # 获取模块所在的目录
            module_dir = os.path.dirname(absolute_receiver_class_file)
            if module_dir not in sys.path:
                sys.path.append(module_dir)
            module = importlib.import_module(module_name)
            source_receiver_class = getattr(module, receiver_class_name)
            self.__source_receiver_factory_dict[source_receiver_name] = source_receiver_class

    def __get_source_receiver_instance(self,
                                       source_label: str,
                                       source_receiver_handler_name: str,
                                       configuration: dict[str, Union[str, dict]]) -> SourceReceiverInterface:
        # 创建某个 source_label 对应的接收器实例，并把通道筛选条件补进去。
        source_receiver_class = self.__source_receiver_factory_dict[source_receiver_handler_name]
        source_receiver: SourceReceiverInterface = source_receiver_class()
        source_receiver.set_source_label(source_label)
        source_receiver.set_configuration(configuration)
        if source_label in self.__required_channel_label_dict:
            # 算法实例在业务模块启动前就会声明所需通道，这里在source初始化完成后补发配置。
            source_receiver.set_required_channel_labels(self.__required_channel_label_dict[source_label])
        return source_receiver

    def __should_log_calibration_progress(
        self,
        calibration_progress_object: AlgorithmCalibrationProgressObject,
    ) -> bool:
        status = str(calibration_progress_object.status or "").strip().lower()
        current_epoch = int(calibration_progress_object.current_epoch or 0)
        total_epoch = int(calibration_progress_object.total_epoch or 0)

        if status != "training":
            return True

        if current_epoch <= 1 or current_epoch == total_epoch:
            return True

        if current_epoch % 10 == 0:
            return True

        progress_log_key = (
            calibration_progress_object.subject_id,
            calibration_progress_object.exp_name,
            calibration_progress_object.exp_task,
            calibration_progress_object.session_id,
            current_epoch,
            total_epoch,
        )
        if progress_log_key == self.__last_calibration_progress_log_key:
            return False
        self.__last_calibration_progress_log_key = progress_log_key
        return False

    def __convert_algorithm_result_object_to_result_package_model(
            self, algorithm_result_object: AlgorithmResultObject) -> ResultPackageModel:
        result_data = algorithm_result_object.result
        result_package_model = ResultPackageModel(
            result=result_data,
            report_source_information=[
                ReportSourceInformationModel(source_label=source_business_object.get_source_label(),
                                             position=source_business_object.get_used_data_position())
                for source_business_object in self.__source_receiver_dict.values()]
        )
        return result_package_model

    @staticmethod
    def __summarize_algorithm_result_for_log(result_data) -> str:
        if isinstance(result_data, str):
            try:
                parsed_result = json.loads(result_data)
            except json.JSONDecodeError:
                return result_data
            if isinstance(parsed_result, dict):
                return json.dumps(parsed_result, ensure_ascii=False, sort_keys=True)
            return str(parsed_result)
        if isinstance(result_data, bytes):
            return f"<bytes len={len(result_data)}>"
        return str(result_data)

    @staticmethod
    def __summarize_report_source_information_for_log(
        report_source_information_list: list[ReportSourceInformationModel],
    ) -> list[dict]:
        return [
            {
                'source_label': item.source_label,
                'position': item.position,
            }
            for item in (report_source_information_list or [])
        ]

    def __measure_platform_model_size_mb(self) -> float | None:
        model_artifact_root_path = self.__resolve_model_artifact_root_path()
        try:
            model_artifact_size_bytes = self.__measure_directory_size_bytes(model_artifact_root_path)
        except (OSError, ValueError) as exc:
            self.__logger.warning(
                "算法框架统计 model_artifacts 目录大小失败，将不上报 platform_model_size_mb: path=%s error=%s",
                model_artifact_root_path,
                f"{type(exc).__name__}: {exc}",
            )
            return None
        model_artifact_size_mb = model_artifact_size_bytes / (1024.0 * 1024.0)
        self.__logger.info(
            "算法框架已统计 model_artifacts 目录大小: path=%s size_bytes=%s size_mb=%.6f",
            model_artifact_root_path,
            model_artifact_size_bytes,
            model_artifact_size_mb,
        )
        return model_artifact_size_mb

    @staticmethod
    def __resolve_model_artifact_root_path() -> Path:
        package_root = Path(__file__).resolve().parents[1]
        return (package_root / 'method' / 'model_artifacts').resolve()

    @classmethod
    def __measure_directory_size_bytes(cls, root_path: Path) -> int:
        if not root_path.exists():
            raise FileNotFoundError(f"目录不存在: {root_path}")
        if not root_path.is_dir():
            raise NotADirectoryError(f"不是目录: {root_path}")

        total_size_bytes = 0
        pending_path_list = [root_path]
        while pending_path_list:
            current_path = pending_path_list.pop()
            current_stat = current_path.lstat()
            if cls.__is_reparse_point(current_stat):
                raise ValueError(f"目录包含链接或重解析点，拒绝统计: {current_path}")
            with os.scandir(current_path) as entry_iterator:
                for entry in entry_iterator:
                    entry_path = Path(entry.path)
                    entry_stat = entry_path.lstat()
                    if cls.__is_reparse_point(entry_stat):
                        raise ValueError(f"检测到链接或重解析点，拒绝统计: {entry_path}")
                    if stat.S_ISDIR(entry_stat.st_mode):
                        pending_path_list.append(entry_path)
                    elif stat.S_ISREG(entry_stat.st_mode):
                        total_size_bytes += int(entry_stat.st_size)
        return total_size_bytes

    @staticmethod
    def __is_reparse_point(stat_result) -> bool:
        file_attributes = getattr(stat_result, 'st_file_attributes', 0)
        return bool(file_attributes & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0))

    def set_rpc_controller(self, rpc_controller: RpcControllerInterface) -> None:
        self.__rpc_controller = rpc_controller
