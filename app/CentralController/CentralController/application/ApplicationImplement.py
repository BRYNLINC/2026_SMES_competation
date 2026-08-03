import asyncio
import json
import logging
import logging.config
import multiprocessing
import os
import platform
import subprocess
import uuid
from pathlib import Path
from typing import Union
import yaml
from injector import Provider, Injector, T

from ApplicationFramework.api.model.ComponentModel import ComponentModel
from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from ApplicationFramework.application.interface.ApplicationInterface import ApplicationInterface
from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkInterface
from ApplicationFramework.api.interface.ComponentFrameworkOperatorInterface import BindMessageOperatorInterface, \
    RegisterComponentOperatorInterface, UnRegisterComponentOperatorInterface
from ApplicationFramework.common.utils.ContextManager import ContextManager
from CentralController.api.exception.CentralControllerException import CentralControllerException
from CentralController.common.enum.CentralControllerEventEnum import CentralControllerEventEnum
from CentralController.common.utils.EventManager import EventManager
from CentralController.control.RPCController import RPCController
from CentralController.control.interface.ControllerInterface import RPCControllerInterface
from CentralController.facade.CollectorConnector import CollectorConnector
from CentralController.facade.DataStorageConnector import DataStorageConnector
from CentralController.facade.DatabaseConnector import DatabaseConnector
from CentralController.facade.ProcessorConnector import ProcessorConnector
from CentralController.facade.StimulatorConnector import StimulatorConnector
from CentralController.facade.interface.SubsystemConnectorInterface import CollectorConnectorInterface, \
    ProcessorConnectorInterface, StimulatorConnectorInterface, DataStorageConnectorInterface, DatabaseConnectorInterface
from CentralController.service.ComponentMonitor import ComponentMonitor
from CentralController.service.ProcessManager import ProcessManager
from CentralController.service.ServiceCoordinator import ServiceCoordinator
from CentralController.service.interface.ComponentMonitorInterface import ComponentMonitorInterface
from CentralController.service.interface.ProcessManagerInterface import ProcessManagerApplicationInterface, \
    ProcessManagerInterface
from CentralController.service.interface.ServiceCoordinatorInterface import ServiceCoordinatorInterface
from CentralControllerView import ViewMain
from componentframework.api.Enum.ComponentStatusEnum import ComponentStatusEnum

from ApplicationFramework.api.interface.ComponentFrameworkOperatorInterface import \
    ReceiveMessageOperatorInterface
from ProcessHub.bci_competition.api.converter.AlgorithmConnectEventMessageConverter import \
    AlgorithmConnectEventMessageConverter
from ProcessHub.bci_competition.api.message.MessageKeyEnum import MessageKeyEnum
from ProcessHub.bci_competition.api.model.AlgorithmConnectEventModel import \
    AlgorithmConnectClosedEventModel
from ProcessHub.bci_competition.api.protobuf.AlgorithmConnectEvent_pb2 import \
    AlgorithmConnectClosedEventMessage as AlgorithmConnectEvent_pb2

from CentralController.common.model.GroupInformationModel import GroupInformationModel


# from Collector.api.protobuf.CollectorControl_pb2 import CollectorControlMessage as CollectorControlMessage_pb2


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
    中控主应用。

    可以把它理解成整个比赛框架的“总导演”：
    1. 接收各子系统注册；
    2. 统一管理 topic 绑定；
    3. 监控关键组件状态；
    4. 在 collector 和 processhub 就绪后触发比赛流程；
    5. 在算法结束后通知全系统退出。
    """

    def __init__(self):
        super().__init__()
        # 这些成员对象会在 initial() 阶段通过依赖注入逐步创建。
        self.__component_monitor = None
        self.__process_manager = None
        self.__event_manager = None
        self.__finish_event: asyncio.Event = asyncio.Event()
        self.__logger = logging.getLogger("centralControllerLogger")
        self.__component_model: ComponentModel = None
        self.__config_dict: dict[str, Union[str, dict]] = None
        self.__context_manager = ContextManager()

        # self._component_id: str = None  可以调用通过接口注入的组件ID

    async def initial(self) -> None:
        # 1. 先加载日志配置文件。
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

        # 2. 加载中控自身的应用配置。
        current_file_path = os.path.abspath(__file__)
        directory_path = os.path.dirname(current_file_path)
        application_config_file_name = 'ApplicationImplement.yml'
        application_config_path = os.path.join(directory_path, application_config_file_name)
        with open(application_config_path, 'r', encoding='utf-8') as f:
            self.__config_dict = yaml.safe_load(f)
        # 3. 生成“当前中控组件”的身份信息，这份信息会被注册到组件框架。
        component_dict = self.__config_dict.get("component", dict())
        # 组件ID遵循如下规则：
        # 1.如果在配置文件中写入，则优先使用配置文件定义的ID
        # 2.如果配置文件中未写入，则检查环境变量中COMPONENT_ID字段，如果存在则使用环境变量中定义的ID
        # 3.如果环境变量中未找到COMPONENT_ID字段，则根据component_type字段自动生成component_type+随机uuid作为component_id
        component_type = component_dict.get('component_type', "")
        component_id = component_dict.get('component_id') \
            if component_dict.get('component_id', None) is not None else \
            (
                os.environ.get('COMPONENT_ID') if os.environ.get('COMPONENT_ID', None) is not None else
                component_type + '_' + str(uuid.uuid4())
            )
        self.__component_model = ComponentModel(
            component_id=component_id,
            component_type=component_type,
            component_info=component_dict.get('component_info', dict())
        )

        class ProcessManagerProvider(Provider):
            instance: ProcessManager = None

            @classmethod
            def get(cls, injector: Injector) -> T:
                # ProcessManager 依赖很多 connector，这里统一在 Provider 中构造。
                if cls.instance is None:
                    cls.instance = ProcessManager(
                        injector.get(ComponentFrameworkInterface),
                        injector.get(ServiceCoordinatorInterface),
                        injector.get(CollectorConnectorInterface),
                        injector.get(ProcessorConnectorInterface),
                        injector.get(StimulatorConnectorInterface),
                        injector.get(DataStorageConnectorInterface),
                        injector.get(DatabaseConnectorInterface),
                    )
                return cls.instance

        # 4. 注册依赖。
        # ContextManager 可以理解成一个轻量的“对象容器”。
        self.__context_manager.bind_class(clazz=ComponentFrameworkInterface, to_target=self._component_framework)
        self.__context_manager.bind_class(clazz=EventManager, to_target=EventManager)
        self.__context_manager.bind_class(CollectorConnectorInterface, to_target=CollectorConnector)
        self.__context_manager.bind_class(ProcessorConnectorInterface, to_target=ProcessorConnector)
        self.__context_manager.bind_class(StimulatorConnectorInterface, to_target=StimulatorConnector)
        self.__context_manager.bind_class(DataStorageConnectorInterface, to_target=DataStorageConnector)
        self.__context_manager.bind_class(DatabaseConnectorInterface, to_target=DatabaseConnector)

        self.__context_manager.bind_class(ServiceCoordinatorInterface, to_target=ServiceCoordinator)
        self.__context_manager.bind_class(ProcessManagerApplicationInterface, to_target=ProcessManagerProvider())
        self.__context_manager.bind_class(ProcessManagerInterface, to_target=ProcessManagerProvider())
        self.__context_manager.bind_class(ComponentMonitorInterface, to_target=ComponentMonitor)

        self.__context_manager.bind_class(RPCControllerInterface, to_target=RPCController)

        self.__component_monitor: ComponentMonitorInterface = self.__context_manager.get_instance(
            ComponentMonitorInterface)

        event_manager: EventManager = self.__context_manager.get_instance(EventManager)
        self.__event_manager = event_manager
        # 应用上下文封装
        service_coordinator: ServiceCoordinatorInterface = \
            self.__context_manager.get_instance(ServiceCoordinatorInterface)
        process_manager: ProcessManagerInterface = \
            self.__context_manager.get_instance(ProcessManagerApplicationInterface)
        rpc_controller: RPCControllerInterface = \
            self.__context_manager.get_instance(RPCControllerInterface)
        processor_connector: ProcessorConnectorInterface \
            = self.__context_manager.get_instance(ProcessorConnectorInterface)

        # 5. 逐个初始化中控的核心服务。
        await processor_connector.initial()
        await service_coordinator.initial()
        await process_manager.initial()
        await rpc_controller.initial(self.__component_model.component_info)
        # 设置停止事件
        self.__finish_event.clear()

        # 注册应用退出响应事件
        event_manager.subscribe(event_name=CentralControllerEventEnum.APPLICATION_EXIT.value, callback=self.exit)

    async def run(self) -> None:
        """
        中控进入运行态后的主循环。

        这里做三类事：
        1. 监听“组件注册/消息绑定”这类框架事件；
        2. 监听 ProcessHub 发来的算法关闭消息；
        3. 启动中控内部服务，并等待退出事件。
        """

        component_framework: ComponentFrameworkInterface = self._component_framework

        process_manager: ProcessManagerInterface = \
            self.__context_manager.get_instance(ProcessManagerInterface)

        service_coordinator: ServiceCoordinatorInterface = \
            self.__context_manager.get_instance(ServiceCoordinatorInterface)

        rpc_controller: RPCControllerInterface = \
            self.__context_manager.get_instance(RPCControllerInterface)

        processor_connector: ProcessorConnectorInterface \
            = self.__context_manager.get_instance(ProcessorConnectorInterface)
        self.__process_manager = process_manager

        # 指令绑定：
        # 下面三个 Operator 都是组件框架事件回调。
        # 其他组件注册、注销、绑定消息时，都会先经过中控这里。
        class BindMessageOperator(BindMessageOperatorInterface):
            def __init__(self):
                self.__logger = logging.getLogger('centralControllerLogger')

            async def on_bind_message(self, message_binding_model: MessageBindingModel) -> MessageBindingModel:
                self.__logger.debug(f"收到消息绑定请求，来自"
                                    f"{message_binding_model.component_id}组件的{message_binding_model.message_key}消息")
                return await service_coordinator.on_bind_message(message_binding_model)

        class RegisterComponentOperator(RegisterComponentOperatorInterface):
            def __init__(self):
                self.__logger = logging.getLogger('centralControllerLogger')

            async def on_register_component(self, component_model: ComponentModel) -> ComponentModel:
                self.__logger.debug(f"收到组件注册请求，{component_model.component_id}组件注册")
                return await service_coordinator.on_register_component(component_model)

        class UnRegisterComponentOperator(UnRegisterComponentOperatorInterface):
            def __init__(self):
                self.__logger = logging.getLogger('centralControllerLogger')

            async def on_unregister_component(self, component_model: ComponentModel) -> None:
                self.__logger.debug(f"收到组件注销请求，{component_model.component_id}组件注销")
                await service_coordinator.on_unregister_component(component_model)

        try:
            # 注册组件框架回调。
            await component_framework.add_listener_on_bind_message(BindMessageOperator())
            await component_framework.add_listener_on_register_component(RegisterComponentOperator())
            await component_framework.add_listener_on_unregister_component(UnRegisterComponentOperator())

            # 中控主动绑定一个固定消息：algorithm_closed。
            # ProcessHub 会在算法退出时发这个消息，中控收到后会开始关机流程。
            await self._component_framework.bind_message(
                MessageBindingModel(
                    message_key=MessageKeyEnum.ALGORITHMCLOSED.value,
                    topic=MessageKeyEnum.ALGORITHMCLOSED.value
                )
            )

            class ReceiveProcessHubMessageOperator(ReceiveMessageOperatorInterface):
                def __init__(self, event_manager: EventManager, on_algorithm_closed):
                    self.__event_manager: EventManager = event_manager
                    self.__on_algorithm_closed = on_algorithm_closed
                    self.__logger = logging.getLogger("centralControllerLogger")

                async def __application_exit_control_func(self):
                    # 统一转成 APPLICATION_EXIT 事件，避免直接在回调里写一堆退出逻辑。
                    await self.__event_manager.notify(CentralControllerEventEnum.APPLICATION_EXIT.value)

                async def receive_message(self, data: bytes) -> None:
                    task_control_model = AlgorithmConnectEventMessageConverter.protobuf_to_model(
                        AlgorithmConnectEvent_pb2.FromString(data)
                    )
                    self.__logger.info(f"收到算法退出消息: {task_control_model}")
                    if isinstance(task_control_model, AlgorithmConnectClosedEventModel):
                        should_exit = await self.__on_algorithm_closed(task_control_model)
                        if should_exit:
                            await self.__application_exit_control_func()

            await self._component_framework.subscribe_message(
                MessageKeyEnum.ALGORITHMCLOSED.value,
                ReceiveProcessHubMessageOperator(
                    event_manager=self.__event_manager,
                    on_algorithm_closed=self.__handle_algorithm_closed_event,
                )
            )

            # 启动中控内部几个关键服务。
            await service_coordinator.startup()

            # 处理容器连接器用于跟外部处理容器控制接口通信。
            await processor_connector.startup()

            # 流程管理器负责给各组件发控制命令。
            await process_manager.startup()

            # RPC 控制器通常给 UI 或外部控制端调用。
            await rpc_controller.startup()
            print("系统启动就绪，请开启界面连接模块")
            ui_config = self.__component_model.component_info.get('ui_config', dict())
            ui_auto_start_flag = ui_config.get('auto_start', False)
            if ui_auto_start_flag:
                ui_process = multiprocessing.Process(target=ViewMain.main)
                ui_process.start()

            # 标记中控自身已经进入 RUNNING。
            await component_framework.update_component_status(ComponentStatusEnum.RUNNING)

            # 开始巡检关键组件是否都已就绪。
            await self.__check_and_execute()
            await self.__finish_event.wait()

        except CentralControllerException as e:
            self.__logger.exception(e)
            await component_framework.update_component_status(ComponentStatusEnum.ERROR)

    async def exit(self) -> None:
        self.__logger.info("收到Application exit请求")
        # 先通知其他组件自行退出。
        await self.__process_manager.close_system()
        await asyncio.sleep(5)
        component_framework: ComponentFrameworkInterface = \
            self.__context_manager.get_instance(ComponentFrameworkInterface)
        rpc_controller: RPCControllerInterface = \
            self.__context_manager.get_instance(RPCControllerInterface)

        try:
            await component_framework.cancel_listener_on_bind_message()
            await component_framework.cancel_listener_on_register_component()
            await component_framework.cancel_listener_on_unregister_component()

            service_coordinator: ServiceCoordinatorInterface = \
                self.__context_manager.get_instance(ServiceCoordinatorInterface)

            # 关闭服务协调器
            await service_coordinator.shutdown()

            # 关闭rpc控制器
            await rpc_controller.shutdown()
            await component_framework.update_component_status(ComponentStatusEnum.STOP)
            # 下面的端口清理是一个很“硬”的兜底操作：
            # 如果某些进程退出不干净，会直接按端口杀进程。
            ports = [9002, 9003]
            self.__kill_process_on_ports(ports)
            await asyncio.sleep(5)
            port = [9000]
            self.__kill_process_on_ports(port)  # 关闭占用这三个端口的进程
            await asyncio.sleep(5)

        except CentralControllerException as e:
            self.__logger.exception(e)
            await component_framework.update_component_status(ComponentStatusEnum.ERROR)
        # 允许程序结束执行
        self.__finish_event.set()

    async def __handle_algorithm_closed_event(
        self,
        algorithm_closed_event_model: AlgorithmConnectClosedEventModel,
    ) -> bool:
        match_control_status = self.__read_json_file(
            self.__resolve_live_state_root_dir() / 'match_control_status.json'
        ) or {}
        if not bool(match_control_status.get('match_finished')):
            self.__logger.info(
                "收到 algorithm_closed，但比赛尚未显式 finished，中控保持运行: closed_event=%s match_control_status=%s",
                algorithm_closed_event_model,
                match_control_status,
            )
            return False

        self.__logger.info(
            "比赛已显式 finished，允许中控执行退出流程: closed_event=%s finished_at=%s",
            algorithm_closed_event_model,
            match_control_status.get('finished_at'),
        )
        return True

    def get_component_model(self) -> ComponentModel:
        return self.__component_model

    @staticmethod
    def __resolve_live_state_root_dir() -> Path:
        return Path(__file__).resolve().parents[4] / 'results' / 'live'

    @staticmethod
    def __read_json_file(file_path: Path):
        if not file_path.exists():
            return None
        try:
            return json.loads(file_path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return None

    @staticmethod
    def __load_expected_processor_team_id_list() -> list[str]:
        config_dict = ApplicationImplement.__load_central_controller_config()
        if not config_dict:
            return []

        team_id_list = []
        component_dict = config_dict.get('components') or {}
        for component_id, component_config in component_dict.items():
            if (component_config or {}).get('component_type') != 'PROCESSOR':
                continue
            component_info = (component_config or {}).get('component_info') or {}
            team_id = component_info.get('team_id') or str(component_id).split('.', 1)[0]
            if team_id is None:
                continue
            team_id_text = str(team_id).strip()
            if team_id_text != '':
                team_id_list.append(team_id_text)
        return sorted(set(team_id_list))

    @staticmethod
    def __load_expected_startup_component_id_list(group_id: str) -> list[str]:
        group_id_text = str(group_id or '').strip()
        if group_id_text == '':
            return []
        config_dict = ApplicationImplement.__load_central_controller_config()
        if not config_dict:
            return []

        component_id_list = []
        component_dict = config_dict.get('components') or {}
        for component_id, component_config in component_dict.items():
            component_config = component_config or {}
            component_group_id = str(component_config.get('component_group_id') or '').strip()
            component_type = str(component_config.get('component_type') or '').strip().upper()
            if component_group_id != group_id_text:
                continue
            if component_type not in {'COLLECTOR', 'PROCESSOR'}:
                continue
            component_id_text = str(component_id).strip()
            if component_id_text != '':
                component_id_list.append(component_id_text)
        return sorted(set(component_id_list))

    @staticmethod
    def __load_central_controller_config() -> dict:
        config_file_path = Path(__file__).resolve().parents[1] / 'config' / 'CentralControllerConfig.yml'
        if not config_file_path.exists():
            return {}
        try:
            return yaml.safe_load(config_file_path.read_text(encoding='utf-8')) or {}
        except (OSError, yaml.YAMLError):
            return {}

    async def __check_and_execute(self) -> None:
        """
        自动启动巡检逻辑。

        当前实现会等待当前 group 下所有 collector / processor 就绪后，
        再统一启动 collector 发数：
        1. 每 10 秒轮询一次；
        2. 默认检查 `group_1` 配置下的所有 `COLLECTOR` + `PROCESSOR`；
        3. 全部到 RUNNING 后，调用 start_group(group_1)。
        """
        target_group_id = "group_1"
        target_component_ids = set(self.__load_expected_startup_component_id_list(target_group_id))
        if not target_component_ids:
            target_component_ids = {"collector_group_1", "team_0.group_1"}

        while True:
            await asyncio.sleep(10)
            # 获取所有组件状态
            component_info_status_model_list = await self.__component_monitor.get_components_status_list()
            self.__logger.debug(f"component_info_status_model_list: {component_info_status_model_list}")
            target_status = "RUNNING"

            # 使用集合记录找到的组件 ID
            found_components = set()

            for component_info_status_model in component_info_status_model_list:
                component_id = component_info_status_model.component_id
                component_status = component_info_status_model.component_status

                # 检查是否是目标组件，并且状态是 running
                if component_id in target_component_ids and component_status == target_status:
                    found_components.add(component_id)

            # 检查是否找到了所有目标组件
            if found_components == target_component_ids:
                # 执行特定操作
                self.__logger.info(
                    "目标组件全部就绪，执行 group 启动: group_id=%s component_count=%s",
                    target_group_id,
                    len(target_component_ids),
                )
                await self.__process_manager.start_group(GroupInformationModel(group_id=target_group_id))
                break

    def __kill_process_on_ports(self, ports: list[int]):
        """
        跨平台终止占用指定端口的进程

        参数:
            ports: 需要检查的端口列表

        返回:
            bool: 是否有进程被成功终止
        """
        os_name = platform.system().lower()
        terminated = False

        for port in ports:
            try:
                if os_name == "windows":
                    # Windows 下通过 netstat 找占用端口的 PID。
                    result = subprocess.run(
                        f'netstat -ano | findstr :{port}',
                        shell=True,
                        capture_output=True,
                        text=True
                    )

                    if result.returncode != 0:
                        print(f"查找端口 {port} 失败: {result.stderr}")
                        continue

                    output = result.stdout.strip()
                    if not output:
                        print(f"未找到占用端口 {port} 的进程")
                        continue

                    # netstat 最后一列通常就是 PID。
                    lines = output.split('\n')
                    pids = set()  # 使用集合去重
                    for line in lines:
                        parts = line.strip().split()
                        if len(parts) >= 5 and parts[4].isdigit():
                            pids.add(int(parts[4]))

                    # 终止所有相关进程
                    for pid in pids:
                        if pid == 0:
                            continue
                        kill_result = subprocess.run(
                            f'taskkill /F /PID {pid}',
                            shell=True,
                            capture_output=True,
                            text=True
                        )

                        if kill_result.returncode == 0:
                            print(f"成功终止占用端口 {port} 的进程 (PID: {pid})")
                            terminated = True
                        else:
                            print(f"终止进程 {pid} 失败: {kill_result.stderr}")
                else:
                    # Linux/macOS 系统
                    # 获取占用端口的 PID
                    result = subprocess.run(
                        f'lsof -ti:{port}',
                        shell=True,
                        capture_output=True,
                        text=True
                    )

                    if result.returncode != 0 and result.stdout.strip() == "":
                        print(f"未找到占用端口 {port} 的进程")
                        continue

                    pids = result.stdout.strip().split('\n')
                    for pid in pids:
                        if pid == 0:
                            continue
                        if pid.isdigit():
                            # 终止进程
                            kill_result = subprocess.run(
                                f'kill -9 {pid}',
                                shell=True,
                                capture_output=True,
                                text=True
                            )
                            if kill_result.returncode == 0:
                                print(f"成功终止占用端口 {port} 的进程 (PID: {pid})")
                                terminated = True
                            else:
                                print(f"终止进程 {pid} 失败: {kill_result.stderr}")
            except Exception as e:
                print(f"处理端口 {port} 时发生错误: {e}")

        return terminated
