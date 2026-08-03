import importlib
import os
import socket
import logging
import sys
import time
from typing import Union

import yaml

from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel, AlgorithmReportMessageModel
from ApplicationFramework.api.interface.ComponentFrameworkOperatorInterface import ReceiveMessageOperatorInterface
from ApplicationFramework.api.model.ComponentEnum import ComponentStatusEnum
from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from Common.converter.CommonMessageConverter import CommonMessageConverter
from ProcessHub.algorithm_connector.interface.AlgorithmConnectorInterface import AlgorithmConnectorInterface, \
    ReceiveAlgorithmReportMessageOperatorInterface, DataConnectClosedEventOperatorInterface
from ProcessHub.api.exception.ProcessHubException import ProcessHubException
from ProcessHub.bci_competition.task.interface.BCICompetitionTaskInterface import BCICompetitionTaskInterface
from ProcessHub.algorithm_connector.model.AlgorithmConnectModel import AlgorithmConnectModel
from ProcessHub.orchestrator.interface.OrchestratorInterface import OrchestratorInterface
from Common.protobuf.CommonMessage_pb2 import DataMessage as DataMessage_pb2


class BciCompetitionOrchestrator(OrchestratorInterface):
    """
    决赛主线编排器。

    它把 ProcessHub 的通用能力，落地成具体比赛流程：
    1. 创建算法连接器；
    2. 动态加载 task；
    3. 订阅数据源；
    4. 把数据转给 task，再由 task 决定如何送给算法；
    5. 接收算法报告和算法断开事件。
    """

    def __init__(self):
        super().__init__()
        # self._component_framework: ComponentFrameworkInterface
        # self._algorithm_connector_factory: AlgorithmConnectorFactoryManagerInterface
        # 只需要一个算法连接器，如果需要多个可以列为list
        self.__algorithm_connector: AlgorithmConnectorInterface = None

        self.__default_component_info: dict[str, Union[str, dict]] = None
        self.__default_algorithm_connection_dict: dict[str, Union[str, float, int]] = {}
        self.__current_task: BCICompetitionTaskInterface = None
        self.__task_class_name: str = None
        self.__task_class_file: str = None
        self.__logger = logging.getLogger("processHubLogger")

    async def initial(self):
        # 编排器自己的配置主要有三类：
        # 1. task 类如何动态加载；
        # 2. 算法服务地址和超时时间；
        # 3. 默认 component_info。
        config_path = os.path.join(os.path.dirname(__file__), 'BciCompetitionOrchestratorConfig.yml')
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict: dict = yaml.safe_load(f)
        task_dict = config_dict.get('task', dict())
        self.__task_class_file = task_dict.get('task_class_file', "")
        self.__task_class_name = task_dict.get('task_class_name', "")

        self.__default_algorithm_connection_dict = dict(config_dict.get('algorithm_connection', dict()) or {})
        self.__default_component_info = config_dict.get('component_info', dict())

    async def startup(self):
        try:
            # 1. 补全当前 ProcessHub 组件自己的 component_info。
            component_model = await self._component_framework.get_component_model()
            component_info = component_model.component_info
            # 插入默认配置信息。
            component_info.update(self.__default_component_info)
            if component_info.get('team_name', None) is None:
                component_info['team_name'] = os.getenv('TEAM_NAME', None)
            if component_info.get('algorithm_number', None) is None:
                component_info['algorithm_number'] = os.getenv('ALGORITHM_NUMBER', None)
            # 获取本机IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(('10.254.254.254', 1))
                ip_address = s.getsockname()[0]
            except Exception as e:
                ip_address = '127.0.0.1'
            finally:
                s.close()
            component_info.update({'ip': ip_address})
            await self._component_framework.update_component_info(component_info)

            effective_algorithm_connection_dict = dict(self.__default_algorithm_connection_dict)
            effective_algorithm_connection_dict.update(component_info.get('algorithm_connection', dict()) or {})
            algorithm_connector_model = AlgorithmConnectModel(
                address=effective_algorithm_connection_dict.get('address', ""),
                max_time_out=effective_algorithm_connection_dict.get(
                    'max_time_out',
                    effective_algorithm_connection_dict.get('max_connection_timeout', 0),
                ),
            )
            self.__algorithm_connector = await self._algorithm_connector_factory.get_algorithm_connector(
                algorithm_connector_model
            )
            self.__logger.info(
                "ProcessHub 连接算法地址: component_id=%s algorithm_address=%s max_connection_timeout=%s",
                component_model.component_id,
                algorithm_connector_model.address,
                algorithm_connector_model.max_time_out,
            )

            # 2. 加载并初始化 task。
            self.__current_task = self.__load_task(self.__task_class_file, self.__task_class_name)
            self.__current_task.set_component_framework(self._component_framework)
            self.__current_task.set_algorithm_connector(self.__algorithm_connector)
            await self.__current_task.initial()

            # 3. 给算法连接器挂上“收到报告时怎么办 / 断开连接时怎么办”的回调。
            await self.__algorithm_connector_setup_process()

            # 4. 根据 task 声明的数据源列表进行订阅。
            await self.__subscribe_source_process()

            # 5. task 自己内部还会继续启动算法连接和配置同步。
            await self.__current_task.startup()

            await self._component_framework.update_component_status(ComponentStatusEnum.RUNNING)
            self.__logger.info("ProcessHub Orchestrator流程启动完成")
        except ProcessHubException as e:
            self.__logger.error(f"ProcessHub Orchestrator流程启动失败，错误信息为{e}")
            await self._component_framework.update_component_status(ComponentStatusEnum.ERROR)

    async def shutdown(self):
        try:
            await self.__current_task.shutdown()
            if self.__algorithm_connector is not None:
                await self.__algorithm_connector.shutdown()
            # 向注册中心发送状态
            await self._component_framework.update_component_status(ComponentStatusEnum.STOP)
        except ProcessHubException as e:
            self.__logger.error(f"ProcessHub Orchestrator流程停止失败，错误信息为{e}")
            await self._component_framework.update_component_status(ComponentStatusEnum.ERROR)

    def __load_task(self, task_class_file: str, task_class_name: str) -> BCICompetitionTaskInterface:
        self.__logger.debug('加载赛题: ' + task_class_file + ':' + task_class_name)
        workspace_path = os.getcwd()
        absolute_task_class_file = os.path.join(workspace_path, task_class_file)
        module_name = os.path.splitext(os.path.basename(absolute_task_class_file))[0]
        # 获取赛题模块所在的目录
        module_dir = os.path.dirname(absolute_task_class_file)
        if module_dir not in sys.path:
            sys.path.append(module_dir)
        module = importlib.import_module(module_name)
        task_class = getattr(module, task_class_name)
        instance = task_class()
        return instance

    async def __subscribe_source_process(self) -> None:
        """
        数据源订阅流程。

        task 会告诉 orchestrator 它想订阅哪些 source。
        orchestrator 负责：
        1. 绑定 source_label 对应的消息；
        2. 订阅消息；
        3. 收到后反序列化为 AlgorithmDataMessageModel；
        4. 再交给 task.receive_message()。
        """

        # 执行算法连接器启动流程
        # 获取数据源并订阅
        class ReceiveDataOperator(ReceiveMessageOperatorInterface):

            def __init__(self, source_label: str, task: BCICompetitionTaskInterface):
                self.__source_label = source_label
                self.__current_task: BCICompetitionTaskInterface = task

            async def receive_message(self, data: bytes) -> None:
                # Collector 发来的是 Common.DataMessage protobuf。
                # 这里先解码成通用模型，再补上 source_label 和 timestamp。
                data_message = DataMessage_pb2()
                data_message.ParseFromString(data)
                data_message_model = CommonMessageConverter.protobuf_to_model(data_message)
                await self.__current_task.receive_message(
                    AlgorithmDataMessageModel(
                        source_label=self.__source_label,
                        timestamp_ms=int(time.time() * 1000),
                        package=data_message_model.package)
                )

        source_list = await self.__current_task.get_source_list()
        for source_model in source_list:
            bound_message = await self._component_framework.bind_message(
                MessageBindingModel(message_key=source_model.source_label, topic=source_model.source_topic))
            source_model.source_topic = bound_message.topic
            await self._component_framework.subscribe_message(
                source_model.source_label,
                ReceiveDataOperator(source_model.source_label, self.__current_task)
            )

    async def __algorithm_connector_setup_process(self) -> None:
        """
        算法连接器回调安装流程。

        算法连接器自己只负责收发消息，不知道收到 report 后该做什么；
        因此这里把回调逻辑交给 task。
        """

        # 获取结果并转发结果
        class ReceiveAlgorithmReportMessageOperator(ReceiveAlgorithmReportMessageOperatorInterface):

            def __init__(self, task: BCICompetitionTaskInterface):
                self.__current_task: BCICompetitionTaskInterface = task

            async def receive_report(self, algorithm_report_message: AlgorithmReportMessageModel) -> None:
                await self.__current_task.receive_report(algorithm_report_message)

        self.__algorithm_connector.set_receive_report_operator(
            ReceiveAlgorithmReportMessageOperator(self.__current_task))

        class DataConnectClosedEventOperator(DataConnectClosedEventOperatorInterface):
            def __init__(self, task: BCICompetitionTaskInterface, algorithm_connector: AlgorithmConnectorInterface):
                self.__current_task: BCICompetitionTaskInterface = task
                self.__algorithm_connector: AlgorithmConnectorInterface = algorithm_connector

            async def on_closed(self, disconnect_reason: str = 'unknown') -> None:
                await self.__current_task.receive_algorithm_connector_closed_event(
                    self.__algorithm_connector,
                    disconnect_reason=disconnect_reason,
                )

        self.__algorithm_connector.set_data_connect_closed_event_operator(
            DataConnectClosedEventOperator(self.__current_task, self.__algorithm_connector)
        )

