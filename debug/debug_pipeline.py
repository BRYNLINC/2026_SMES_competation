'''
1. 这个文件供选手进行算法与数据链路的本地调试，简化了许多进程相关通信（选手一般情况下，不需要修改这个文件，直接使用IDEA的debug此文件即可。）
2. 待选手调试完毕后，请再测试一下startup.bat能否正常使用多进程运行，以连接上大赛框架HTTP上报。
3. 如果出现问题请及时联系出题方，避免成绩无效
'''
import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_COLLECTOR = REPO_ROOT / 'app' / 'Collector'
APP_ALGORITHM = REPO_ROOT / 'app' / 'Algorithm'
APP_PROCESSHUB = REPO_ROOT / 'app' / 'ProcessHub'

for candidate in (APP_COLLECTOR, APP_ALGORITHM, APP_PROCESSHUB):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)

from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel, AlgorithmReportMessageModel
from Algorithm.common.enum.AlgorithmEventEnum import AlgorithmEventEnum
from Algorithm.common.utils.EventManager import EventManager
from Algorithm.service.BusinessManager import BusinessManager
from Algorithm.service.MethodManager import MethodManager
from Algorithm.service.interface.RpcControllerInterface import RpcControllerInterface
from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkInterface
from ApplicationFramework.api.model.ComponentEnum import ComponentStatusEnum
from ApplicationFramework.api.model.ComponentModel import ComponentModel
from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from Collector.common.converter.ReceiverTransferModelToDataMessageModelConverter import (
    ReceiverTransferModelToDataMessageModelConverter,
)
from Collector.datasender.TimingDataSender import TimingDataSender
from Collector.receiver.virtual_receiver.VirtualReceiverImplement import VirtualReceiverImplement
from Collector.receiver.virtual_receiver.api.converter.VirtualReceiverCustomControlMessageConverter import (
    VirtualReceiverCustomControlMessageConverter,
)
from Collector.receiver.virtual_receiver.api.message.VirtualReceiverMessageKeyEnum import (
    VirtualReceiverMessageKeyEnum,
)
from Collector.receiver.virtual_receiver.api.proto.VirtualReceiverCustomControl_pb2 import (
    VirtualReceiverCustomControlMessage as VirtualReceiverCustomControlMessage_pb2,
)
from Collector.service.interface.TransponderInterface import InformationTransponderInterface
from Common.converter.CommonMessageConverter import CommonMessageConverter
from Common.model.CommonMessageModel import ControlPackageModel, DataMessageModel
from Common.protobuf.CommonMessage_pb2 import DataMessage as DataMessage_pb2
from ProcessHub.algorithm_connector.interface.AlgorithmConnectorInterface import (
    AlgorithmConnectorInterface,
    DataConnectClosedEventOperatorInterface,
    ReceiveAlgorithmReportMessageOperatorInterface,
)
from ProcessHub.bci_competition.task.BCICompetitionTaskFinal import (
    BCICompetitionTaskFinal,
)


SEND_DATA_KEY = 'send_data'
REPORT_KEY = 'report'
ALGORITHM_CLOSED_KEY = 'algorithm_closed'
# 修改原因：
# 当前主线 task 内部把 source_label == 'hidden_score' 视为私有评分通道，用来缓存每个 trial
# 的真值/隐藏分数信息。原 debug 入口只转发普通数据流，task 就拿不到这条旁路数据，最终评分会缺上下文。
HIDDEN_SCORE_KEY = 'hidden_score'


class DebugComponentFramework(ComponentFrameworkInterface):
    def __init__(self, component_id: str):
        self._component_model = ComponentModel(
            component_id=component_id,
            component_type='DEBUG',
            component_info={},
        )
        self._global_config: dict = {}
        self._message_topics: dict[str, str] = {}
        self._send_handlers: dict[str, Callable[[bytes], Awaitable[None]]] = {}
        self.report_messages: list[DataMessageModel] = []
        self.algorithm_closed_events: list[bytes] = []

    def set_send_handler(self, message_key: str, handler: Callable[[bytes], Awaitable[None]]) -> None:
        self._send_handlers[message_key] = handler

    async def get_global_config(self) -> dict:
        return self._global_config

    async def update_global_config(self, config_dict: dict) -> None:
        self._global_config.update(config_dict)

    async def add_listener_on_update_global_config(self, operator) -> None:
        return None

    async def cancel_listener_on_update_global_config(self) -> None:
        return None

    async def bind_message(self, message_binding_model: MessageBindingModel) -> MessageBindingModel:
        if message_binding_model.topic is None:
            message_binding_model.topic = message_binding_model.message_key
        self._message_topics[message_binding_model.message_key] = message_binding_model.topic
        return message_binding_model

    async def get_topic_by_message_key(self, message_key: str, component_id: str = None) -> str:
        return self._message_topics.get(message_key, message_key)

    async def subscribe_message(self, message_key: str, operator) -> None:
        return None

    async def unsubscribe_message(self, message_key: str) -> None:
        return None

    async def send_message(self, message_key: str, message: bytes) -> None:
        if message_key == REPORT_KEY:
            pb_message = DataMessage_pb2()
            pb_message.ParseFromString(message)
            self.report_messages.append(CommonMessageConverter.protobuf_to_model(pb_message))
        elif message_key == ALGORITHM_CLOSED_KEY:
            self.algorithm_closed_events.append(message)

        handler = self._send_handlers.get(message_key)
        if handler is not None:
            await handler(message)

    async def register_component(self, component_model: ComponentModel) -> ComponentModel:
        self._component_model = component_model
        return component_model

    async def unregister_component(self) -> None:
        return None

    async def get_component_model(self, component_id: str = None) -> ComponentModel:
        return self._component_model

    async def update_component_info(self, component_info: dict, component_id: str = None) -> None:
        self._component_model.component_info.update(component_info)

    async def add_listener_on_update_component_info(self, operator, component_id: str = None) -> None:
        return None

    async def cancel_listener_on_update_component_info(self, component_id: str = None) -> None:
        return None

    async def update_component_status(
        self, component_status: ComponentStatusEnum, component_id: str = None
    ) -> None:
        self._component_model.component_info['status'] = component_status.value

    async def get_component_status(self, component_id: str = None) -> ComponentStatusEnum:
        status = self._component_model.component_info.get('status', ComponentStatusEnum.STOP.value)
        return ComponentStatusEnum(status)

    async def add_listener_on_update_component_status(self, operator, component_id: str = None) -> None:
        return None

    async def cancel_listener_on_update_component_status(self, component_id: str = None) -> None:
        return None

    async def add_listener_on_request_application_exit(self, operator) -> None:
        return None

    async def initial(self) -> None:
        return None

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    def set_component_startup_configuration(self, daemon_address: str, daemon_port: int) -> None:
        return None

    async def add_listener_on_bind_message(self, operator) -> None:
        return None

    async def cancel_listener_on_bind_message(self) -> None:
        return None

    async def add_listener_on_register_component(self, operator) -> None:
        return None

    async def cancel_listener_on_register_component(self) -> None:
        return None

    async def add_listener_on_unregister_component(self, operator) -> None:
        return None

    async def cancel_listener_on_unregister_component(self) -> None:
        return None

    async def get_all_component_id(self) -> list[str]:
        return [self._component_model.component_id]


###############
# Debug-only replacement for the Java message bus and Java-driven collector flow.
# Keep this block isolated so it can be removed without affecting the formal pipeline.
class DebugCollectorTransponder(InformationTransponderInterface):
    def __init__(self, data_sender: TimingDataSender):
        self._data_sender = data_sender
        self._receiver: Optional[VirtualReceiverImplement] = None

    def set_receiver(self, receiver: VirtualReceiverImplement) -> None:
        self._receiver = receiver

    async def initial(self, config_dict: dict = None) -> None:
        return None

    async def update(self, config_dict: dict = None) -> None:
        return None

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    async def send_data(self, receiver_transfer_model) -> None:
        await self._data_sender.send_data(
            ReceiverTransferModelToDataMessageModelConverter.convert(receiver_transfer_model)
        )

    async def receiver_external_trigger(self, external_trigger_model) -> None:
        await self._data_sender.receiver_external_trigger(external_trigger_model)

    async def start_data_sending(self):
        await self._data_sender.start_data_sending()
        await self._receiver.start_data_sending()

    async def stop_data_sending(self):
        await self._receiver.stop_data_sending()
        await self._data_sender.stop_data_sending()

    async def send_device_info(self):
        await self._receiver.send_device_info()

    async def send_impedance(self):
        await self._receiver.send_impedance()
###############


###############
# Debug-only replacement for the Java/gRPC algorithm connector.
# It keeps the original Algorithm BusinessManager/MethodManager in-process for PyCharm debugging.
class DebugAlgorithmRpcController(RpcControllerInterface):
    def __init__(self, on_report: Callable[[AlgorithmReportMessageModel], Awaitable[None]]):
        self._on_report = on_report

    async def initial_system(self, config_dict: dict) -> None:
        return None

    async def startup(self):
        return None

    async def shutdown(self):
        return None

    def delete(self):
        return None

    async def disconnect(self):
        return None

    async def report(self, algorithm_report_message: AlgorithmReportMessageModel):
        await self._on_report(algorithm_report_message)


class LocalAlgorithmRuntime:
    def __init__(self, business_manager: BusinessManager, method_manager: MethodManager):
        self._business_manager = business_manager
        self._method_manager = method_manager

    @classmethod
    async def create(
        cls,
        algorithm_app_dir: Path,
        report_callback: Callable[[AlgorithmReportMessageModel], Awaitable[None]],
        closed_callback: Callable[[], Awaitable[None]],
        algorithm_config_path: Optional[Path] = None,
    ) -> 'LocalAlgorithmRuntime':
        event_manager = EventManager()
        business_manager = BusinessManager()
        business_manager.set_workspace_path_override(str(algorithm_app_dir))
        rpc_controller = DebugAlgorithmRpcController(report_callback)
        business_manager.set_rpc_controller(rpc_controller)

        method_manager = MethodManager(method_proxy=business_manager, event_manager=event_manager)
        method_manager.set_workspace_path_override(str(algorithm_app_dir))
        method_manager.set_rpc_controller(rpc_controller)

        async def on_method_finished() -> None:
            await closed_callback()

        event_manager.subscribe(AlgorithmEventEnum.METHOD_FINISHED.value, on_method_finished)

        config_path = algorithm_config_path or (algorithm_app_dir / 'Algorithm' / 'config' / 'AlgorithmConfig.yml')
        with config_path.open('r', encoding='utf-8') as file:
            config_dict = yaml.safe_load(file)

        business_config_dict = {}
        if 'source_receiver_handlers' in config_dict:
            business_config_dict['source_receiver_handlers'] = config_dict['source_receiver_handlers']
        if 'sources' in config_dict:
            business_config_dict['sources'] = config_dict['sources']

        await business_manager.initial_system(business_config_dict)
        await method_manager.initial_system(config_dict['method'])

        return cls(business_manager=business_manager, method_manager=method_manager)

    async def startup(self) -> None:
        await self._business_manager.startup()
        await self._method_manager.startup()
        await asyncio.sleep(0.05)

    async def send_data(self, algorithm_data_message_model: AlgorithmDataMessageModel) -> None:
        await self._business_manager.forward_data(algorithm_data_message_model)

    async def push_algorithm_config(self, config_dict: dict) -> None:
        await self._business_manager.receive_config(config_dict)

    async def pull_algorithm_config(self) -> dict:
        return await self._business_manager.get_config()

    async def shutdown(self) -> None:
        await self._method_manager.shutdown()
        await self._business_manager.shutdown()


class InMemoryAlgorithmConnector(AlgorithmConnectorInterface):
    def __init__(self, algorithm_app_dir: Path, algorithm_address: str, algorithm_config_path: Optional[Path] = None):
        self._algorithm_app_dir = algorithm_app_dir
        self._algorithm_address = algorithm_address
        self._algorithm_config_path = algorithm_config_path
        self._max_connection_timeout = 0.0
        self._runtime: Optional[LocalAlgorithmRuntime] = None
        self._receive_report_operator: Optional[ReceiveAlgorithmReportMessageOperatorInterface] = None
        self._closed_event_operator: Optional[DataConnectClosedEventOperatorInterface] = None
        self._closed_event = asyncio.Event()
        self._closed_notified = False

    def set_receive_report_operator(
        self, receive_report_operator: ReceiveAlgorithmReportMessageOperatorInterface
    ) -> None:
        self._receive_report_operator = receive_report_operator

    def set_data_connect_closed_event_operator(
        self, data_connect_closed_event_operator: DataConnectClosedEventOperatorInterface
    ):
        self._closed_event_operator = data_connect_closed_event_operator

    async def _on_report(self, algorithm_report_message: AlgorithmReportMessageModel) -> None:
        if self._receive_report_operator is not None:
            await self._receive_report_operator.receive_report(algorithm_report_message)

    async def _on_runtime_closed(self) -> None:
        if self._closed_notified:
            return
        self._closed_notified = True
        self._closed_event.set()
        if self._closed_event_operator is not None:
            await self._closed_event_operator.on_closed()

    async def send_data(self, algorithm_data_message_model: AlgorithmDataMessageModel):
        await self._runtime.send_data(algorithm_data_message_model)

    async def push_algorithm_config(self, config_dict: dict):
        await self._runtime.push_algorithm_config(config_dict)

    async def pull_algorithm_config(self) -> dict:
        return await self._runtime.pull_algorithm_config()

    def get_algorithm_address(self) -> str:
        return self._algorithm_address

    def get_max_connection_timeout(self) -> float:
        return self._max_connection_timeout

    async def startup(self):
        self._runtime = await LocalAlgorithmRuntime.create(
            algorithm_app_dir=self._algorithm_app_dir,
            report_callback=self._on_report,
            closed_callback=self._on_runtime_closed,
            algorithm_config_path=self._algorithm_config_path,
        )

    async def data_connect(self):
        await self._runtime.startup()

    async def data_disconnect(self):
        if self._runtime is not None:
            await self._runtime.shutdown()

    async def shutdown(self):
        if self._runtime is not None:
            await self._runtime.shutdown()

    async def shutdown_and_close_algorithm_system(self) -> None:
        if self._runtime is not None:
            await self._runtime.shutdown()

    async def wait_closed(self, timeout_seconds: float) -> None:
        await asyncio.wait_for(self._closed_event.wait(), timeout=timeout_seconds)
###############


class DebugReceiveAlgorithmReportOperator(ReceiveAlgorithmReportMessageOperatorInterface):
    # 修改原因：
    # 正式流程里，ProcessHub orchestrator 会把算法 report 回调给 task.receive_report()。
    # 原 debug/debug_pipeline.py 直接 new task + connector，绕过了 orchestrator 这层装配，
    # 导致算法虽然有 report 输出，task 侧却收不到，trial 记录和 score_package 都不会累计。
    def __init__(self, task: BCICompetitionTaskFinal):
        self._task = task

    async def receive_report(self, algorithm_report_message: AlgorithmReportMessageModel) -> None:
        await self._task.receive_report(algorithm_report_message)


class DebugDataConnectClosedEventOperator(DataConnectClosedEventOperatorInterface):
    # 修改原因：
    # 正式流程里 connector 关闭后会继续触发“算法已结束”事件，task 再据此落盘并汇总最终分数。
    # 原 debug 入口缺了这层回调，算法即使正常结束，task 也走不到 receive_algorithm_connector_closed_event()，
    # 所以不会进入最终收尾路径。
    def __init__(self, task: BCICompetitionTaskFinal, connector: AlgorithmConnectorInterface):
        self._task = task
        self._connector = connector

    async def on_closed(self) -> None:
        await self._task.receive_algorithm_connector_closed_event(self._connector)


class DebugHttpResponse:
    status_code = 200
    text = 'debug'


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )


def load_debug_config(config_path: Path) -> dict:
    with config_path.open('r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


class PacketLimitState:
    def __init__(self, max_data_packages: Optional[int]):
        self.max_data_packages = max_data_packages
        self.data_package_count = 0
        self.end_sent = False


async def collector_bytes_to_task(
    task: BCICompetitionTaskFinal,
    source_label: str,
    data: bytes,
    packet_limit_state: Optional[PacketLimitState],
    count_toward_packet_limit: bool = True,
) -> None:
    # 修改原因：
    # 原函数假设所有输入消息都参与 max_data_packages 计数，因此 packet_limit_state 必填。
    # 这次接入 hidden_score 后，这条数据只是给 task 补充真值，不应该把“发送 end_flag 的时机”提前，
    # 所以 packet_limit_state 改成 Optional，并新增 count_toward_packet_limit 开关显式区分两类流。
    if packet_limit_state is not None and packet_limit_state.end_sent:
        return

    pb_message = DataMessage_pb2()
    pb_message.ParseFromString(data)
    data_message_model = CommonMessageConverter.protobuf_to_model(pb_message)
    await task.receive_message(
        AlgorithmDataMessageModel(
            source_label=source_label,
            timestamp=time.time(),
            package=data_message_model.package,
        )
    )

    if not count_toward_packet_limit or packet_limit_state is None:
        # 原逻辑从这里开始会继续累计 data_package_count，并可能提前补发 end_flag。
        # hidden_score 只是旁路元数据，不是比赛数据流本身，所以送到 task 后就应立即返回，
        # 避免它意外消耗 debug 包预算。
        return

    if packet_limit_state.max_data_packages is None:
        return

    if type(data_message_model.package).__name__ != 'DataPackageModel':
        return

    packet_limit_state.data_package_count += 1
    if packet_limit_state.data_package_count < packet_limit_state.max_data_packages:
        return

    packet_limit_state.end_sent = True
    await task.receive_message(
        AlgorithmDataMessageModel(
            source_label=source_label,
            timestamp=time.time(),
            package=ControlPackageModel(end_flag=True),
        )
    )


async def run_pipeline(config: dict) -> DebugComponentFramework:
    collector_config = config['collector']
    runtime_config = config['runtime']
    algorithm_config = config['algorithm']
    group_id = collector_config.get('group_id', 'group_1')
    source_topic = collector_config.get('source_topic', f'{group_id}.data')

    task_framework = DebugComponentFramework(component_id=f'team_0.{group_id}')
    collector_framework = DebugComponentFramework(component_id='collector_debug')
    await task_framework.bind_message(MessageBindingModel(message_key=collector_config['source_label'], topic=source_topic))
    packet_limit_state = PacketLimitState(runtime_config.get('max_data_packages'))
    connector = InMemoryAlgorithmConnector(
        algorithm_app_dir=APP_ALGORITHM,
        algorithm_address=algorithm_config.get('address', 'debug://local-algorithm'),
        algorithm_config_path=resolve_path(algorithm_config['config_path'])
        if algorithm_config.get('config_path')
        else None,
    )
    task = BCICompetitionTaskFinal()
    task.set_component_framework(task_framework)
    task.set_algorithm_connector(connector)
    # 原代码到这里就直接开始喂数据，没有把 orchestrator 平时负责的两个关键回调补回来。
    # 现在显式补齐“算法 report -> task.receive_report()”和“connector closed -> task 收尾”这两条链路，
    # 让 debug 行为尽量贴近正式运行时的装配结果。
    connector.set_receive_report_operator(DebugReceiveAlgorithmReportOperator(task))
    connector.set_data_connect_closed_event_operator(DebugDataConnectClosedEventOperator(task, connector))

    collector_framework.set_send_handler(
        SEND_DATA_KEY,
        lambda data: collector_bytes_to_task(
            task,
            collector_config['source_label'],
            data,
            packet_limit_state,
            count_toward_packet_limit=True,
        ),
    )
    # 原代码只有 SEND_DATA_KEY 这一条 handler：
    # collector_framework.set_send_handler(SEND_DATA_KEY, lambda data: collector_bytes_to_task(...))
    # 现在额外把 hidden_score 也送进 task，使其能命中当前决赛主线 task 里的私有评分缓存；
    # 同时这里明确关闭 packet limit 计数，避免旁路标签流改变停止条件。
    collector_framework.set_send_handler(
        HIDDEN_SCORE_KEY,
        lambda data: collector_bytes_to_task(
            task,
            HIDDEN_SCORE_KEY,
            data,
            None,
            count_toward_packet_limit=False,
        ),
    )

    sender = TimingDataSender()
    receiver = VirtualReceiverImplement()
    sender_started = False
    receiver_started = False
    task_started = False
    try:
        sender.set_component_framework(collector_framework)
        await sender.initial()
        await sender.startup()
        sender_started = True

        receiver.set_component_framework(collector_framework)
        receiver.set_workspace_path_override(str(APP_COLLECTOR))
        receiver.set_config_path_override(str(resolve_path(collector_config['virtual_receiver_config_path'])))
        receiver.set_debug_data_file_filter(
            exp_name=collector_config.get('exp_name'),
            subject_id=collector_config.get('subject_id'),
        )
        receiver.set_debug_calibrate_trials_per_class(
            collector_config.get('calibrate_trials_per_class', 0)
        )

        transponder = DebugCollectorTransponder(sender)
        transponder.set_receiver(receiver)
        receiver.set_receiver_transponder(transponder)

        await receiver.initial()
        await receiver.startup()
        receiver_started = True

        async def forward_virtual_receiver_custom_control(data: bytes) -> None:
            control_model = VirtualReceiverCustomControlMessageConverter.protobuf_to_model(
                VirtualReceiverCustomControlMessage_pb2.FromString(data)
            )
            await receiver.custom_control(control_model)

        task_framework.set_send_handler(
            VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value,
            forward_virtual_receiver_custom_control,
        )

        original_cwd = os.getcwd()
        os.chdir(APP_PROCESSHUB)
        try:
            await task.initial()
        finally:
            os.chdir(original_cwd)
        await task.startup()
        task_started = True

        await transponder.send_device_info()
        await transponder.start_data_sending()

        await connector.wait_closed(runtime_config.get('timeout_seconds', 120))
        await asyncio.sleep(0.1)
    finally:
        if receiver_started:
            await receiver.shutdown()
        if sender_started:
            await sender.shutdown()
        if task_started:
            await task.shutdown()

    return task_framework


async def async_main(config_path: Path) -> int:
    config = load_debug_config(config_path)
    setup_logging()

    requests_post_restore = None
    if config['runtime'].get('patch_score_http', True):
        ###############
        # Debug-only replacement for the Java/online score submission path.
        # Formal runs should continue using the original HTTP submission.
        import requests

        requests_post_restore = requests.post
        requests.post = lambda *args, **kwargs: DebugHttpResponse()
        ###############

    try:
        task_framework = await run_pipeline(config)
    finally:
        if requests_post_restore is not None:
            import requests

            requests.post = requests_post_restore

    print('Debug pipeline finished.')
    print(f'Report packages captured: {len(task_framework.report_messages)}')
    for index, data_message_model in enumerate(task_framework.report_messages, start=1):
        print(f'{index}. {type(data_message_model.package).__name__}: {data_message_model.package}')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Single-process PyCharm debug entry for the BCI pipeline.')
    parser.add_argument(
        '--config',
        default=str(REPO_ROOT / 'debug' / 'pipeline_debug.yml'),
        help='Path to the debug pipeline yaml config.',
    )
    args = parser.parse_args()
    return asyncio.run(async_main(Path(args.config)))


if __name__ == '__main__':
    raise SystemExit(main())






