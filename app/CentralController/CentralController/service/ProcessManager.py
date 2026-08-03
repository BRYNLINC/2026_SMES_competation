import asyncio
import json
import logging
import sys
from pathlib import Path
from random import random
from typing import Union

from injector import inject

from ApplicationFramework.api.interface.ComponentFrameworkInterface import ComponentFrameworkInterface
from ApplicationFramework.api.model.MessageBindingModel import MessageBindingModel
from CentralController.common.model import GroupInformationModel
from CentralController.facade.interface.SubsystemConnectorInterface import CollectorConnectorInterface, \
    ProcessorConnectorInterface, StimulatorConnectorInterface, DataStorageConnectorInterface, DatabaseConnectorInterface
from CentralController.service.interface.ProcessManagerInterface import ProcessManagerInterface
from CentralController.service.interface.ServiceCoordinatorInterface import ServiceCoordinatorInterface


PROJECT_ROOT = Path(__file__).resolve().parents[4]
COLLECTOR_APP_ROOT = PROJECT_ROOT / 'app' / 'Collector'
if str(COLLECTOR_APP_ROOT) not in sys.path:
    sys.path.append(str(COLLECTOR_APP_ROOT))

from Collector.receiver.virtual_receiver.api.proto.VirtualReceiverCustomControl_pb2 import (  # noqa: E402
    VirtualReceiverCustomControlMessage as VirtualReceiverCustomControlMessage_pb2,
)
from Common.protobuf.CommonMessage_pb2 import InformationPackage as InformationPackage_pb2  # noqa: E402


class ProcessManager(ProcessManagerInterface):
    """
    流程管理器。

    它不负责算法细节，也不负责采集细节，而是负责“控制顺序”：
    1. 给 collector 下发发送设备信息/开始发数等命令；
    2. 给 stimulator 下发开始/停止刺激命令；
    3. 在系统退出时给各组件发 application_exit。
    """

    __VIRTUAL_RECEIVER_CUSTOM_CONTROL_MESSAGE_KEY = 'virtual_receiver_custom_control'

    @inject
    def __init__(self,
                 component_framework: ComponentFrameworkInterface,
                 service_coordinator: ServiceCoordinatorInterface,
                 collector_connector: CollectorConnectorInterface,
                 processor_connector: ProcessorConnectorInterface,
                 stimulator_connector: StimulatorConnectorInterface,
                 data_storage_connector: DataStorageConnectorInterface,
                 database_connector: DatabaseConnectorInterface,
                 ):
        self.__service_coordinator: ServiceCoordinatorInterface = service_coordinator
        self.__collector_connector: CollectorConnectorInterface = collector_connector
        self.__processor_connector: ProcessorConnectorInterface = processor_connector
        self.__stimulator_connector: StimulatorConnectorInterface = stimulator_connector
        self.__component_framework: ComponentFrameworkInterface = component_framework
        self.__data_storage_connector: DataStorageConnectorInterface = data_storage_connector
        self.__database_connector: DatabaseConnectorInterface = database_connector

        self.__logger = logging.getLogger('centralControllerLogger')

    async def initial(self):
        pass

    async def startup(self):
        central_controller_component_model = await self.__component_framework.get_component_model()
        component_info = central_controller_component_model.component_info
        message_dict = component_info.get('message', dict())
        for message_key in message_dict:
            await self.__component_framework.bind_message(MessageBindingModel(message_key=message_key))

    async def shutdown(self):
        pass

    async def prepare_system(self):
        await self.__processor_connector.start_processor_container()

        registered_component_dict = self.__service_coordinator.get_registered_component_information_model_dict()
        data_storage_component_list = [
            registered_component_dict[component_id]
            for component_id in registered_component_dict
            if registered_component_dict[component_id].component_type == 'DATASTORAGE'
        ]
        database_component_list = [
            registered_component_dict[component_id]
            for component_id in registered_component_dict
            if registered_component_dict[component_id].component_type == 'DATABASE'
        ]

        for database_component in database_component_list:
            await self.__database_connector.start_receive(database_component.component_id)
        await asyncio.sleep(5)

        for data_storage_component in data_storage_component_list:
            await self.__data_storage_connector.start_receive(data_storage_component.component_id)
        await asyncio.sleep(1)

    async def start_group(self, group_information_model: GroupInformationModel):
        registered_component_dict = self.__service_coordinator.get_registered_component_information_model_dict()
        collector_component_list = [
            registered_component_dict[component_id]
            for component_id in registered_component_dict
            if registered_component_dict[component_id].component_group_id == group_information_model.group_id
               and registered_component_dict[component_id].component_type == 'COLLECTOR'
        ]
        for collector_component in collector_component_list:
            await self.__send_recovery_start_selector_if_needed(collector_component.component_id)
        for collector_component in collector_component_list:
            await self.__collector_connector.send_device_info(collector_component.component_id)
        self.__logger.info(f"{group_information_model.group_id}采集设备信息发送")
        await asyncio.sleep(1)
        for collector_component in collector_component_list:
            await self.__collector_connector.start_data_sending(collector_component.component_id)
        self.__logger.info(f"启动{group_information_model.group_id}采集设备数据发送")
        await asyncio.sleep(1)

    async def reset_group(self, group_information_model: GroupInformationModel):
        registered_component_dict = self.__service_coordinator.get_registered_component_information_model_dict()
        collector_component_list = [
            registered_component_dict[component_id]
            for component_id in registered_component_dict
            if registered_component_dict[component_id].component_group_id == group_information_model.group_id
               and registered_component_dict[component_id].component_type == 'COLLECTOR'
        ]
        stimulator_component_list = [
            registered_component_dict[component_id]
            for component_id in registered_component_dict
            if registered_component_dict[component_id].component_group_id == group_information_model.group_id
               and registered_component_dict[component_id].component_type == 'STIMULATOR'
        ]
        for stimulator_component in stimulator_component_list:
            await self.__stimulator_connector.stop_stimulation(stimulator_component.component_id)
        self.__logger.info(f"{group_information_model.group_id}停止刺激组件")
        await asyncio.sleep(1)
        for collector_component in collector_component_list:
            await self.__collector_connector.stop_data_sending(collector_component.component_id)
        self.__logger.info(f"停止{group_information_model.group_id}采集设备数据发送")
        await asyncio.sleep(1)

    async def close_system(self):
        registered_component_dict = self.__service_coordinator.get_registered_component_information_model_dict()
        for component_id in registered_component_dict:
            match registered_component_dict[component_id].component_type:
                case 'COLLECTOR':
                    await self.__collector_connector.application_exit(component_id)
                    self.__logger.info(f"关闭{component_id}采集组件")
                case 'PROCESSOR':
                    await self.__processor_connector.application_exit(component_id)
                    self.__logger.info(f"关闭{component_id}处理组件")
                case 'STIMULATOR':
                    await self.__stimulator_connector.application_exit(component_id)
                    self.__logger.info(f"关闭{component_id}刺激组件")

        await asyncio.sleep(5)

    async def __send_recovery_start_selector_if_needed(self, collector_component_id: str) -> None:
        launcher_manifest = self.__load_launcher_manifest()
        applied_recovery = (launcher_manifest.get('applied_recovery') or {}) if isinstance(launcher_manifest, dict) else {}
        if str(applied_recovery.get('recovery_mode') or '').strip() != 'restart_from_stage':
            return
        collector_start_selector = applied_recovery.get('collector_start_selector') or {}
        subject_id = str(collector_start_selector.get('subject_id') or '').strip()
        block_id = str(collector_start_selector.get('block_id') or '').strip()
        if subject_id == '' or block_id == '':
            self.__logger.warning(
                '恢复模式为 restart_from_stage，但 launcher_manifest 中缺少 collector_start_selector: collector_component_id=%s payload=%s',
                collector_component_id,
                collector_start_selector,
            )
            return
        topic = f'{collector_component_id}.{self.__VIRTUAL_RECEIVER_CUSTOM_CONTROL_MESSAGE_KEY}'
        await self.__component_framework.bind_message(
            MessageBindingModel(
                message_key=self.__VIRTUAL_RECEIVER_CUSTOM_CONTROL_MESSAGE_KEY,
                topic=topic,
            )
        )
        custom_control_message = VirtualReceiverCustomControlMessage_pb2(
            virtualReceiverStartSendingPointMessage=InformationPackage_pb2(
                subjectId=subject_id,
                blockId=block_id,
            )
        )
        await self.__component_framework.send_message(
            self.__VIRTUAL_RECEIVER_CUSTOM_CONTROL_MESSAGE_KEY,
            custom_control_message.SerializeToString(),
        )
        self.__logger.info(
            '已向 Collector 下发恢复起点: collector_component_id=%s topic=%s subject_id=%s block_id=%s recovery_stage=%s',
            collector_component_id,
            topic,
            subject_id,
            block_id,
            applied_recovery.get('stage'),
        )

    def __load_launcher_manifest(self) -> dict:
        launcher_manifest_path = PROJECT_ROOT / 'results' / 'control' / 'launcher_manifest.json'
        if not launcher_manifest_path.exists():
            return {}
        try:
            return json.loads(launcher_manifest_path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            self.__logger.exception('读取 launcher_manifest 失败: %s', launcher_manifest_path)
            return {}
