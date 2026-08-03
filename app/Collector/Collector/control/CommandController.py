import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Union

from injector import inject

from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkInterface
from ApplicationFramework.api.interface.ComponentFrameworkOperatorInterface import ReceiveMessageOperatorInterface
from Collector.api.converter.CollectorControlMessageConverter import CollectorControlMessageConverter
from Collector.api.message.MessageKeyEnum import MessageKeyEnum
from Collector.api.model.CollectorControlModel import StartDataSendingControlModel, StopDataSendingControlModel, \
    SendDeviceInfoControlModel, SendImpedanceControlModel, ApplicationExitControlModel
from Collector.common.enum.CollectorEventEnum import CollectorEventEnum
from Collector.common.utils.EventManager import EventManager
from Collector.control.interface.ControllerInterface import CommandControllerInterface
from Collector.service.exception.BusinessCollectorException import BusinessCollectorException, \
    BusinessStatusesNotSuitableException
from Collector.service.interface.BusinessManagerInterface import BusinessManagerInterface
from Collector.api.protobuf.CollectorControl_pb2 import CollectorControlMessage as CollectorControlMessage_pb2

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from tools.runtime_state_sqlite import (  # noqa: E402
    STATE_KEY_MATCH_CONTROL_STATUS,
    read_json_state,
    resolve_runtime_state_db_path,
)


class CommandController(CommandControllerInterface):

    @inject
    def __init__(self,
                 business_manager: BusinessManagerInterface,
                 component_framework: ComponentFrameworkInterface,
                 event_manager: EventManager):
        self.__business_manager: BusinessManagerInterface = business_manager
        self.__component_framework: ComponentFrameworkInterface = component_framework
        self.__event_manager: EventManager = event_manager
        self.__logger = logging.getLogger("collectorLogger")
        self.__command_control_topic: str = None
        self.__pending_start_data_sending_task: asyncio.Task | None = None
        self.__start_compensation_task: asyncio.Task | None = None
        self.__controller_started_wallclock: float = time.time()
        self.__start_command_received: bool = False
        self.__initial_start_compensation_completed: bool = False

    async def initial(self, config_dict: dict[str, Union[str, dict]] = None):
        message_key_topic_dict = config_dict.get("message", dict[str, str]())
        command_control_message_key = MessageKeyEnum.COMMAND_CONTROL.value
        self.__command_control_topic = message_key_topic_dict.get(command_control_message_key, None)

    async def update(self, config_dict: dict[str, Union[str, dict]] = None) -> None:
        message_key_topic_dict = config_dict.get("message", dict[str, str]())
        command_control_message_key = MessageKeyEnum.COMMAND_CONTROL.value
        self.__command_control_topic = message_key_topic_dict.get(command_control_message_key, None)

    async def startup(self):
        self.__controller_started_wallclock = time.time()
        await self.__component_framework.bind_message(
            MessageBindingModel(
                message_key=MessageKeyEnum.COMMAND_CONTROL.value,
                topic=self.__command_control_topic
            )
        )

        class ReceiveCommandControlMessageOperator(ReceiveMessageOperatorInterface):
            def __init__(self,
                         command_controller: "CommandController",
                         event_manager: EventManager,
                         business_manager: BusinessManagerInterface):
                self.__command_controller = command_controller
                self.__logger = logging.getLogger("collectorLogger")
                self.__business_manager: BusinessManagerInterface = business_manager
                self.__event_manager: EventManager = event_manager

            async def __application_exit_control_func(self):
                self.__logger.info("收到Application exit请求")
                await self.__event_manager.notify(CollectorEventEnum.APPLICATION_EXIT.value)

            async def receive_message(self, data: bytes) -> None:
                collector_control_model = CollectorControlMessageConverter.protobuf_to_model(
                    CollectorControlMessage_pb2.FromString(data)
                )
                self.__logger.info(f"收到命令控制消息: {collector_control_model}")
                try:
                    if isinstance(collector_control_model.package, StartDataSendingControlModel):
                        self.__command_controller._mark_start_command_received()
                        await self.__command_controller._schedule_start_data_sending()
                    elif isinstance(collector_control_model.package, StopDataSendingControlModel):
                        self.__command_controller._disable_initial_start_compensation(
                            reason="received_stop_data_sending_command",
                        )
                        self.__command_controller._cancel_pending_start_wait()
                        await self.__business_manager.stop_data_sending()
                    elif isinstance(collector_control_model.package, SendDeviceInfoControlModel):
                        await self.__business_manager.send_device_info()
                    elif isinstance(collector_control_model.package, SendImpedanceControlModel):
                        await self.__business_manager.send_impedance()
                    elif isinstance(collector_control_model.package, ApplicationExitControlModel):
                        await self.__application_exit_control_func()
                except BusinessCollectorException as e:
                    self.__logger.error(e)

        await self.__component_framework.subscribe_message(
            MessageKeyEnum.COMMAND_CONTROL.value,
            ReceiveCommandControlMessageOperator(
                command_controller=self,
                event_manager=self.__event_manager,
                business_manager=self.__business_manager)
        )
        self.__start_compensation_task = asyncio.create_task(self.__watch_match_started_for_initial_compensation())

    async def shutdown(self):
        self._cancel_start_compensation_task()
        self._cancel_pending_start_wait()
        await self.__component_framework.unsubscribe_message(MessageKeyEnum.COMMAND_CONTROL.value)

    async def _schedule_start_data_sending(self) -> None:
        if self.__pending_start_data_sending_task is not None and not self.__pending_start_data_sending_task.done():
            self.__logger.info("start_data_sending 等待任务已存在，忽略重复请求")
            return
        self.__pending_start_data_sending_task = asyncio.create_task(self.__wait_and_start_data_sending())

    async def __wait_and_start_data_sending(self) -> None:
        try:
            self.__logger.info("收到 start_data_sending 指令，等待前端 start-match 放行")
            while True:
                if self.__is_match_started():
                    try:
                        await self.__business_manager.start_data_sending()
                        self.__logger.info("检测到 match_started=true，Collector 开始正式发数")
                    except BusinessStatusesNotSuitableException as e:
                        if "BusinessStatusEnum.DATASENDING" in str(e):
                            self.__logger.info("Collector 已处于正式发数状态，忽略重复启动: %s", e)
                        else:
                            raise
                    return
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            self.__logger.info("Collector 等待 start-match 的后台任务已取消")
            raise
        except BusinessCollectorException as e:
            self.__logger.error(e)
        finally:
            self.__pending_start_data_sending_task = None

    async def __watch_match_started_for_initial_compensation(self) -> None:
        try:
            while True:
                if self.__start_command_received or self.__initial_start_compensation_completed:
                    return
                if self.__is_match_started(require_current_runtime=True):
                    self.__initial_start_compensation_completed = True
                    self.__logger.warning(
                        "检测到当前运行已 match_started，但 Collector 尚未收到 start_data_sending 指令，执行一次补偿启动"
                    )
                    await self._schedule_start_data_sending()
                    return
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            self.__logger.info("Collector 启动补偿监视任务已取消")
            raise

    def __is_match_started(self, require_current_runtime: bool = False) -> bool:
        status_payload = self.__read_match_control_status_payload()
        if not isinstance(status_payload, dict):
            return False
        if require_current_runtime and not self.__is_current_runtime_status_payload(status_payload):
            return False
        return bool(status_payload.get('match_started'))

    def __read_match_control_status_payload(self) -> dict | None:
        status_payload = read_json_state(
            resolve_runtime_state_db_path(PROJECT_ROOT),
            STATE_KEY_MATCH_CONTROL_STATUS,
        )
        if not isinstance(status_payload, dict):
            status_payload = self.__read_json_file(self.__resolve_match_control_status_file_path())
        return status_payload if isinstance(status_payload, dict) else None

    def __is_current_runtime_status_payload(self, status_payload: dict) -> bool:
        try:
            coordinator_started_at = float(status_payload.get('coordinator_started_at'))
        except (TypeError, ValueError):
            return False
        return coordinator_started_at >= (self.__controller_started_wallclock - 60.0)

    def _cancel_pending_start_wait(self) -> None:
        if self.__pending_start_data_sending_task is None:
            return
        if self.__pending_start_data_sending_task.done():
            self.__pending_start_data_sending_task = None
            return
        self.__pending_start_data_sending_task.cancel()
        self.__pending_start_data_sending_task = None

    def _mark_start_command_received(self) -> None:
        self.__start_command_received = True
        self._cancel_start_compensation_task()

    def _disable_initial_start_compensation(self, reason: str) -> None:
        self.__initial_start_compensation_completed = True
        self.__logger.info("关闭 Collector 初始启动补偿: reason=%s", reason)
        self._cancel_start_compensation_task()

    def _cancel_start_compensation_task(self) -> None:
        if self.__start_compensation_task is None:
            return
        if self.__start_compensation_task.done():
            self.__start_compensation_task = None
            return
        self.__start_compensation_task.cancel()
        self.__start_compensation_task = None

    @staticmethod
    def __resolve_match_control_status_file_path() -> Path:
        return Path(__file__).resolve().parents[4] / 'results' / 'live' / 'match_control_status.json'

    @staticmethod
    def __read_json_file(file_path: Path):
        if not file_path.exists():
            return None
        try:
            return json.loads(file_path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return None

