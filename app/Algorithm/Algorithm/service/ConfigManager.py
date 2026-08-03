import logging
import os
from typing import Union

import yaml
from injector import inject

from Algorithm.common.enum.ServiceStatusEnum import ServiceStatusEnum
from Algorithm.service.interface.RpcControllerInterface import RpcControllerInterface
from Algorithm.service.interface.ServiceManagerInterface import MethodManagerInterface, \
    BusinessManagerInterface, ConfigManagerInterface


class ConfigManager(ConfigManagerInterface):
    __LOCKED_CONNECTION_CONFIG = {
        'rpc_address': '[::]:9981',
    }
    __LOCKED_METHOD_CONFIG = {
        'method_class_file': 'Algorithm/method/model_artifacts/baseline_example/AlgorithmImplement.py',
        'method_class_name': 'AlgorithmImplement',
    }
    __LOCKED_SOURCE_CONFIG = {
        'eeg_1': {
            'source_receiver': {
                'handler': 'continuous_data_source_receiver',
                'configuration': {
                    'chunk_size': 4000,
                    'device_information_wait_timeout_seconds': 0.0,
                    'calibration_object_wait_timeout_seconds': 0.0,
                    'calibration_chunk_assembly_timeout_seconds': 15.0,
                },
            },
        },
    }
    __LOCKED_SOURCE_RECEIVER_HANDLERS = {
        'continuous_data_source_receiver': {
            'receiver_class_file': 'Algorithm/service/SourceReceiver/ContinuousDataSourceReceiver.py',
            'receiver_class_name': 'ContinuousDataSourceReceiver',
        },
    }

    @inject
    def __init__(self,
                 method_manager: MethodManagerInterface,
                 business_manager: BusinessManagerInterface
                 ):
        self.__config_dict = dict[str, Union[str, dict]]()
        self.__config_file_path: str = None
        self.__logger = logging.getLogger("algorithmLogger")

        self.__rpc_controller: RpcControllerInterface = None
        self.__method_manager: MethodManagerInterface = method_manager
        self.__business_manager: BusinessManagerInterface = business_manager

        # 服务状态
        self.__service_status: ServiceStatusEnum = ServiceStatusEnum.STOPPED

    async def initial_system(self, config_dict: dict[str, Union[str, dict]] = None):
        """
        初始化配置信息
        :return:
        """
        if self.__service_status is not ServiceStatusEnum.STOPPED:
            return
        # 设置服务初始化状态
        self.__service_status = ServiceStatusEnum.INITIALIZING

        self.__logger.info("config_manager启动")
        workspace_path = os.getcwd()
        config_path = os.path.join(workspace_path, self.__config_file_path)
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
            self.__logger.info(f"config_manager启动系统初始化{config_dict}")
            self.__log_ignored_runtime_overrides(config_dict)
            await self.__rpc_controller.initial_system(dict(self.__LOCKED_CONNECTION_CONFIG))
            await self.__method_manager.initial_system(dict(self.__LOCKED_METHOD_CONFIG))

            business_config_dict = {
                'source_receiver_handlers': dict(self.__LOCKED_SOURCE_RECEIVER_HANDLERS),
                'sources': dict(self.__LOCKED_SOURCE_CONFIG),
            }
            await self.__business_manager.initial_system(business_config_dict)
            self.__logger.info(
                "Algorithm 运行期装配已锁定: connection=%s method=%s sources=%s",
                self.__LOCKED_CONNECTION_CONFIG,
                self.__LOCKED_METHOD_CONFIG,
                list(self.__LOCKED_SOURCE_CONFIG.keys()),
            )
        # 设置服务就绪状态
        self.__service_status = ServiceStatusEnum.READY

    def __log_ignored_runtime_overrides(self, config_dict: dict[str, Union[str, dict]] | None) -> None:
        if not isinstance(config_dict, dict):
            return
        locked_section_dict = {
            'connection': self.__LOCKED_CONNECTION_CONFIG,
            'method': self.__LOCKED_METHOD_CONFIG,
            'sources': self.__LOCKED_SOURCE_CONFIG,
            'source_receiver_handlers': self.__LOCKED_SOURCE_RECEIVER_HANDLERS,
        }
        for section_name, locked_value in locked_section_dict.items():
            configured_value = config_dict.get(section_name)
            if configured_value is None:
                continue
            if configured_value != locked_value:
                self.__logger.warning(
                    "AlgorithmConfig.yml 中的 %s 已被忽略，正式运行以框架锁定值为准: configured=%s locked=%s",
                    section_name,
                    configured_value,
                    locked_value,
                )

    async def startup(self) -> None:
        if self.__service_status not in [ServiceStatusEnum.READY, ServiceStatusEnum.ERROR]:
            return
        self.__service_status = ServiceStatusEnum.STARTING

        self.__service_status = ServiceStatusEnum.RUNNING

    async def shutdown(self) -> None:
        if self.__service_status is not ServiceStatusEnum.RUNNING:
            return
        self.__service_status = ServiceStatusEnum.STOPPING

        self.__service_status = ServiceStatusEnum.READY

    def set_config_file_path(self, config_file_path: str) -> None:
        self.__config_file_path = config_file_path

    def set_rpc_controller(self, rpc_controller: RpcControllerInterface) -> None:
        self.__rpc_controller = rpc_controller
