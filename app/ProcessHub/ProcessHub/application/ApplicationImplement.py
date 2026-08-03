import asyncio
import importlib
import logging
import logging.config
import os
import sys
import uuid
import copy
from pathlib import Path

import yaml
from injector import Provider, Injector, T

from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkInterface
from ApplicationFramework.api.model.ComponentModel import ComponentModel
from ApplicationFramework.application.interface.ApplicationInterface import ApplicationInterface
from ApplicationFramework.common.utils.ContextManager import ContextManager
from ProcessHub.algorithm_connector.AlgorithmConnectorFactoryManager import AlgorithmConnectorFactoryManager
from ProcessHub.algorithm_connector.interface.AlgorithmConnectorFactoryInterface import \
    AlgorithmConnectorFactoryManagerInterface, AlgorithmConnectorFactoryInterface
from ProcessHub.common.utils.EventManager import EventManager
from ProcessHub.common.enum.ProcessHubEventEnum import ProcessHubEventEnum
from ProcessHub.control.CommandController import CommandController
from ProcessHub.control.interface.ControllerInterface import CommandControllerInterface

from ProcessHub.algorithm_connector.facade.AlgorithmRPCDataConnectClient import AlgorithmRPCDataConnectClient
from ProcessHub.algorithm_connector.facade.AlgorithmRPCServiceControlClient import AlgorithmRPCServiceControlClient

from ProcessHub.orchestrator.interface.OrchestratorInterface import OrchestratorInterface


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
    """
    ProcessHub 主应用。

    ProcessHub 位于 Collector 和 Algorithm 之间，主要负责“编排”：
    1. 建立到算法进程的连接；
    2. 动态加载 orchestrator；
    3. orchestrator 再去加载 task / challenge；
    4. 把采集数据转发给算法，把算法结果再转回消息总线。
    """

    def __init__(self):
        super().__init__()
        # 无初始配置信息
        self.__finish_event: asyncio.Event = asyncio.Event()
        self.__orchestrator: OrchestratorInterface = None
        self.__component_model: ComponentModel = None
        self.__orchestrator_class_name: str = None
        self.__orchestrator_class_file: str = None
        self.__context_manager = ContextManager()
        self.__logger = logging.getLogger("processHubLogger")

    async def initial(self) -> None:
        # 1. 初始化日志。
        current_file_path = os.path.abspath(__file__)
        log_config_file_directory_path = os.path.join(os.path.dirname(os.path.dirname(current_file_path)), 'config')
        log_config_file_path = os.path.join(log_config_file_directory_path, 'LoggingConfig.yml')
        with open(log_config_file_path, 'r', encoding='utf-8') as logging_file:
            logging_config = yaml.safe_load(logging_file)

        ensure_logging_targets(
            logging_config,
            base_dir=Path(os.path.dirname(os.path.dirname(current_file_path))),
        )

        # 应用配置到logging模块
        logging.config.dictConfig(logging_config)

        class AlgorithmConnectorFactoryManagerProvider(Provider):
            instance: AlgorithmConnectorFactoryManager = None

            @classmethod
            def get(cls, injector: Injector) -> T:
                # 统一构造算法连接器工厂。
                if cls.instance is None:
                    cls.instance = AlgorithmConnectorFactoryManager(
                        injector.get(AlgorithmRPCDataConnectClient),
                        injector.get(AlgorithmRPCServiceControlClient)
                    )
                return cls.instance

        self.__context_manager.bind_class(clazz=ComponentFrameworkInterface, to_target=self._component_framework)
        self.__context_manager.bind_class(clazz=EventManager, to_target=EventManager)

        self.__context_manager.bind_class(clazz=AlgorithmRPCDataConnectClient, to_target=AlgorithmRPCDataConnectClient)
        self.__context_manager.bind_class(clazz=AlgorithmRPCServiceControlClient,
                                          to_target=AlgorithmRPCServiceControlClient)
        self.__context_manager.bind_class(clazz=AlgorithmConnectorFactoryInterface,
                                          to_target=AlgorithmConnectorFactoryManagerProvider())
        self.__context_manager.bind_class(clazz=AlgorithmConnectorFactoryManagerInterface,
                                          to_target=AlgorithmConnectorFactoryManagerProvider())

        self.__context_manager.bind_class(clazz=CommandControllerInterface, to_target=CommandController)

        # 2. 读取 ProcessHub 自身配置。
        current_file_path = os.path.abspath(__file__)
        directory_path = os.path.dirname(current_file_path)
        application_config_file_name = 'ApplicationImplement.yml'
        application_config_path = os.path.join(directory_path, application_config_file_name)

        with open(application_config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
        # 生成组件信息
        component_dict = config_dict.get("component", dict())
        # 组件ID遵循如下规则：
        # 1.如果环境变量中存在COMPONENT_ID字段，则优先使用环境变量中定义的ID
        # 2.如果环境变量中未找到COMPONENT_ID字段，则检查配置文件中的component_id
        # 3.如果环境变量中和配置文件都未提供component_id，则根据component_type字段自动生成随机ID
        component_type = component_dict.get('component_type', "")
        configured_component_id = component_dict.get('component_id', None)
        env_component_id = os.environ.get('COMPONENT_ID', None)
        component_id = (
            env_component_id
            if env_component_id not in (None, '')
            else (
                configured_component_id
                if configured_component_id not in (None, '')
                else component_type + '_' + str(uuid.uuid4())
            )
        )
        component_info = copy.deepcopy(component_dict.get('component_info', dict()) or {})
        component_info = self.__normalize_component_info(
            component_type=component_type,
            component_id=component_id,
            component_info=component_info,
        )
        self.__component_model = ComponentModel(
            component_id=component_id,
            component_type=component_type,
            component_info=component_info,
        )
        self.__logger.info(
            "ProcessHub 组件身份已解析: component_id=%s env_component_id=%s configured_component_id=%s "
            "team_id=%s group_id=%s",
            component_id,
            env_component_id,
            configured_component_id,
            component_info.get('team_id'),
            component_info.get('group_id'),
        )

        # orchestrator 决定当前 ProcessHub 具体加载哪一个编排器实现。
        orchestrator_dict = config_dict.get('orchestrator', dict())
        self.__orchestrator_class_file = orchestrator_dict.get('orchestrator_class_file', "")
        self.__orchestrator_class_name = orchestrator_dict.get('orchestrator_class_name', "")

        algorithm_connector_factory_manager = self.__context_manager.get_instance(
            AlgorithmConnectorFactoryManagerInterface)
        await algorithm_connector_factory_manager.initial()
        command_controller = self.__context_manager.get_instance(CommandControllerInterface)
        await command_controller.initial()

    async def run(self) -> None:
        # 1. 启动算法连接器工厂和命令控制器。
        algorithm_connector_factory_manager = self.__context_manager.get_instance(
            AlgorithmConnectorFactoryManagerInterface)
        await algorithm_connector_factory_manager.startup()
        command_controller = self.__context_manager.get_instance(CommandControllerInterface)
        await command_controller.startup()

        # 2. 注册退出事件。
        event_manager: EventManager = self.__context_manager.get_instance(EventManager)
        event_manager.subscribe(event_name=ProcessHubEventEnum.APPLICATION_EXIT.value,
                                callback=self.__on_application_exit)
        # 3. 动态加载编排器对象。
        self.__orchestrator = self.__load_orchestrator(
            orchestrator_class_file=self.__orchestrator_class_file,
            orchestrator_class_name=self.__orchestrator_class_name
        )
        # 4. 把组件框架和算法连接器工厂注入给编排器。
        self.__orchestrator.set_component_framework(self.__context_manager.get_instance(ComponentFrameworkInterface))
        self.__orchestrator.set_algorithm_connector_factory(algorithm_connector_factory_manager)
        await self.__orchestrator.initial()
        await self.__orchestrator.startup()
        await self.__finish_event.wait()

    async def exit(self) -> None:
        self.__logger.info("收到Application exit请求")
        event_manager: EventManager = self.__context_manager.get_instance(EventManager)
        # 唤醒退出事件
        await event_manager.notify(event_name=ProcessHubEventEnum.APPLICATION_EXIT.value)

    def get_component_model(self) -> ComponentModel:
        return self.__component_model

    async def __on_application_exit(self):
        self.__logger.info("收到Application exit事件")
        await self.__orchestrator.shutdown()
        command_controller: CommandControllerInterface = self.__context_manager.get_instance(CommandControllerInterface)
        await command_controller.shutdown()
        algorithm_connector_factory_manager: AlgorithmConnectorFactoryManagerInterface = (
            self.__context_manager.get_instance(AlgorithmConnectorFactoryManagerInterface))
        await algorithm_connector_factory_manager.shutdown()
        self.__finish_event.set()

    def __load_orchestrator(self, orchestrator_class_file: str, orchestrator_class_name: str) -> OrchestratorInterface:
        self.__logger.debug('加载编排器: ' + orchestrator_class_file + ':' + orchestrator_class_name)
        workspace_path = os.getcwd()
        absolute_orchestrator_class_file = os.path.join(workspace_path, orchestrator_class_file)
        module_name = os.path.splitext(os.path.basename(absolute_orchestrator_class_file))[0]
        # 动态导入时强依赖 cwd，这也是整个工程对启动路径非常敏感的原因之一。
        module_dir = os.path.dirname(absolute_orchestrator_class_file)
        if module_dir not in sys.path:
            sys.path.append(module_dir)
        module = importlib.import_module(module_name)
        orchestrator_class = getattr(module, orchestrator_class_name)
        instance = orchestrator_class()
        return instance

    def __normalize_component_info(
        self,
        component_type: str,
        component_id: str,
        component_info: dict,
    ) -> dict:
        if component_type != 'PROCESSOR':
            return component_info
        if not component_id or '.' not in component_id:
            return component_info

        team_id, group_id = component_id.split('.', 1)
        component_info['team_id'] = team_id
        component_info['group_id'] = group_id
        component_info['processor_component_id'] = component_id
        component_info.setdefault('collector_component_id', f'collector_{group_id}')
        component_info.setdefault(
            'collector_custom_control_topic',
            f"collector_{group_id}.virtual_receiver_custom_control",
        )
        component_info.setdefault('runtime_stage_event_topic', 'runtime_stage.event')
        return component_info
