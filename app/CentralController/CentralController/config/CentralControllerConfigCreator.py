from pathlib import Path

import yaml

from CentralController.common.enum.ComponentCategoryEnum import ComponentCategoryEnum
from CentralController.common.model.ComponentInformationModel import ComponentInformationModel
from CentralController.common.model.GroupInformationModel import GroupInformationModel
from Collector.api.message.MessageKeyEnum import MessageKeyEnum as CollectorMessageKeyEnum
from Collector.receiver.virtual_receiver.api.message.VirtualReceiverMessageKeyEnum import VirtualReceiverMessageKeyEnum
from Stimulator.api.message.MessageKeyEnum import MessageKeyEnum as StimulatorMessageKeyEnum
from ProcessHub.bci_competition.api.message.MessageKeyEnum import MessageKeyEnum as TaskMessageKeyEnum


class CentralControllerConfigCreator:
    """
    生成静态 CentralControllerConfig.yml。

    本轮改造后，这个生成器除了原有 collector / processor / stimulator 外，
    还会生成 runtime stage coordination 所需的：
    1. ProcessHub 显式 team/group 路由信息；
    2. Collector 的 calibration private topic 映射；
    3. RuntimeStageCoordinator 组件与配套 topic。
    """

    def __init__(self):
        # ==============================
        # 这里是“全局赛队列表”的唯一生成入口。
        #
        # 你后续如果要修改参赛队数量，优先改这里：
        # 1. 删除某个 team_x：对应队伍不会再生成 PROCESSOR / report topic / calibration topic；
        # 2. 新增某个 team_x：会自动补齐该队在 group 下的全部组件配置；
        # 3. 名称必须与后续你希望使用的 COMPONENT_ID / team_id 保持一致。
        #
        # 当前要求：
        # - 只保留 group_1
        # - 但保留 group_1 下全部赛队
        # ==============================
        self.team_config_list = [
            {
                'team_id': 'team_0',
                'team_display_name': 'HUSTUM-BCI-SMES',
                'team_host': '10.11.11.110', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_1',
                'team_display_name': 'TJU_baseline',
                'team_host': '10.11.11.105', # 交换机固定IP
                'enabled': True,
            },

            {
                'team_id': 'team_2',
                'team_display_name': '家中无福贵, 口袋无财宝',
                'team_host': '10.11.11.111',  # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_3',
                'team_display_name': '第七克',
                'team_host': '10.11.11.102', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_4',
                'team_display_name': 'CNN is all you need',
                'team_host': '10.11.11.103', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_5',
                'team_display_name': 'MindLink',
                'team_host': '10.11.11.104', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_6',
                'team_display_name': 'UM-HUST-SMES-MI',
                'team_host': '10.11.11.107', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_7',
                'team_display_name': '一念通天',
                'team_host': '10.11.11.108', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_8',
                'team_display_name': '焚天裂渊寂灭恐惧战马',
                'team_host': '10.11.11.109', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_9',
                'team_display_name': 'YSUBCI',
                'team_host': '10.11.11.112', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_10',
                'team_display_name': '元思科技',
                'team_host': '10.11.11.113', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_11',
                'team_display_name': 'medii2',
                'team_host': '10.11.11.115', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_12',
                'team_display_name': '脑机解码队2',
                'team_host': '10.11.11.118', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_13',
                'team_display_name': '随便都行队',
                'team_host': '10.11.11.116', # 交换机固定IP
                'enabled': True,
            },
            {
                'team_id': 'team_14',
                'team_display_name': 'Physics-BCI',
                'team_host': '10.11.11.119', # 交换机固定IP
                'enabled': True,
            },
            # {
            #     'team_id': 'team_15',
            #     'team_display_name': '二三子',
            #     'team_host': '10.11.11.120', # 交换机固定IP
            #     'enabled': True,
            # },
        ]
        self.team_id_list = [
            team_config['team_id']
            for team_config in self.team_config_list
            if team_config.get('enabled', True)
        ]
        # ==============================
        # 这里是“全局组别列表”的唯一生成入口。
        #
        # 后续如果要扩到更多组：
        # 1. 在这里追加 group_2 / group_3 ...；
        # 2. 生成器会按 team_id_list x group_id_list 的笛卡尔积生成 PROCESSOR；
        # 3. 同时补齐 collector / stimulator / data_storage / database / coordinator 对应配置。
        #
        # 当前仅保留 group_1。
        # ==============================
        self.group_id_list = [
            'group_1',
        ]
        self.components = list[ComponentInformationModel]()

    def run(self):
        # run() 会把上面的 team_id_list / group_id_list 展开成最终静态配置文件。
        #
        # 维护建议：
        # 1. 先改 __init__ 里的 team_id_list / group_id_list；
        # 2. 再运行本生成器；
        # 3. 最后检查 CentralControllerConfig.yml 是否符合预期。
        #
        # 不建议直接手改大段 CentralControllerConfig.yml，
        # 因为这个文件内容很多，手改极易漏掉 database / data_storage / coordinator 的联动部分。
        central_controller_config_dict = {}
        groups_dict = dict()
        group_information_model = CentralControllerConfigCreator.create_group_model('group_base')
        groups_dict['group_base'] = self.group_information_model_to_dict(group_information_model)
        for group_id in self.group_id_list:
            group_information_model = CentralControllerConfigCreator.create_group_model(group_id)
            groups_dict.update(self.group_information_model_to_dict(group_information_model))
            # 构建处理组件
            group_processor_component_model_list = list()
            for team_config in self.team_config_list:
                if not team_config.get('enabled', True):
                    continue
                processor_component_model = CentralControllerConfigCreator.create_processor_model(group_id, team_config)
                self.components.append(processor_component_model)
                group_processor_component_model_list.append(processor_component_model)
            # 构建采集组件
            collector_component_model = self.create_collector_model(group_id, self.team_id_list)
            self.components.append(collector_component_model)
            # 构建刺激组件，依赖对应group的采集组件
            stimulator_component_model = self.create_stimulator_model(
                group_id, group_processor_component_model_list[0].component_id, collector_component_model.component_id)
            self.components.append(stimulator_component_model)

        # 构建其他组件

        data_storage_model = self.create_data_storage_model(self.group_id_list)
        self.components.append(data_storage_model)
        database_model = self.create_database_model(self.group_id_list, self.team_id_list)
        self.components.append(database_model)
        runtime_stage_coordinator_model = self.create_runtime_stage_coordinator_model(
            self.group_id_list,
            self.team_id_list,
        )
        self.components.append(runtime_stage_coordinator_model)
        central_controller_component_model = self.create_central_controller_model()
        self.components.append(central_controller_component_model)

        central_controller_config_dict['groups'] = groups_dict
        central_controller_config_dict['components'] = dict()
        for component in self.components:
            central_controller_config_dict['components'].update(
                self.component_information_model_to_dict(component)
            )

        config_path = Path(__file__).resolve().with_name('CentralControllerConfig.yml')
        with config_path.open('w', encoding='utf-8') as file:
            yaml.dump(central_controller_config_dict, file, sort_keys=False)

    @staticmethod
    def create_group_model(group_id: str) -> GroupInformationModel:
        group_information_model = GroupInformationModel()
        group_information_model.group_id = group_id
        group_information_model.group_info = dict()
        group_information_model.message_key_topic_dict = dict()
        return group_information_model

    def create_stimulator_model(self, group_id: str,
                                feed_back_component_id: str = None,
                                virtual_receiver_component_id: str = None) -> ComponentInformationModel:
        feedback_control_topic = next((component.message_key_topic_dict.get(TaskMessageKeyEnum.REPORT.value)
                                       for component in self.components
                                       if component.component_id == feed_back_component_id), None)

        virtual_receiver_custom_control = next((component.message_key_topic_dict.get(
            VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value)
                                       for component in self.components
                                       if component.component_id == virtual_receiver_component_id), None)

        component_information_model = ComponentInformationModel()
        component_information_model.component_id = 'stimulator_' + group_id
        component_information_model.component_type = ComponentCategoryEnum.STIMULATOR.value
        component_information_model.component_info = {
            'external_trigger_address': '127.0.0.1:8972',
        }
        component_information_model.component_group_id = group_id
        component_information_model.message_key_topic_dict = {
            'command_control': f"{component_information_model.component_id}.command_control",
            'feedback_control': f"{feedback_control_topic}",
            'information': f"{group_id}.data",
            'random_number_seeds': f"{component_information_model.component_id}.random_number_seeds",
            'virtual_receiver_custom_control': f"{virtual_receiver_custom_control}",
        }
        return component_information_model

    @staticmethod
    def create_collector_model(group_id: str, team_id_list: list[str]) -> ComponentInformationModel:
        # Collector 在运行期需要知道：
        # 1. 当前 group 里有哪些队伍；
        # 2. 每个队伍的 calibration 私有 topic；
        # 3. runtime stage event / control / status topic。
        #
        # 这里的 team_id_list 直接决定：
        # - Collector 会等待哪些队伍的校准申请；
        # - Collector 会为哪些队伍建立 calibration_private_topic_by_team；
        # - VirtualReceiver 在校准阶段会给哪些队伍分别发私有数据。
        component_information_model = ComponentInformationModel()
        component_information_model.component_id = 'collector_' + group_id
        component_information_model.component_type = ComponentCategoryEnum.COLLECTOR.value
        component_information_model.component_info = {
            'group_id': group_id,
            'collector_component_id': f'collector_{group_id}',
            'team_id_list': list(team_id_list),
            'calibration_private_topic_by_team': {
                team_id: f'{team_id}.{group_id}.calibration'
                for team_id in team_id_list
            },
            'runtime_stage_event_topic': 'runtime_stage.event',
            'runtime_stage_control_topic': 'runtime_stage.control',
            'runtime_stage_status_topic': 'runtime_stage.status',
        }
        component_information_model.component_group_id = group_id
        component_information_model.message_key_topic_dict = {
            'send_data': f"{group_id}.data",
            'command_control': f"{component_information_model.component_id}.command_control",
            'external_trigger': f"{component_information_model.component_id}.external_trigger",
            'virtual_receiver_custom_control':
                f"{component_information_model.component_id}.virtual_receiver_custom_control",
            'runtime_stage_event': 'runtime_stage.event',
            'runtime_stage_control': 'runtime_stage.control',
        }
        return component_information_model

    @staticmethod
    def create_processor_model(group_id: str, team_config: dict[str, str]) -> ComponentInformationModel:
        # ProcessHub 侧显式保存 team_id/group_id/collector 路由信息，
        # 避免后续再通过 source/topic/component_id 反推。
        #
        # 这里每调用一次，就生成“一个赛队在一个组里的一个 PROCESSOR 组件”。
        # 因此：
        # - 保留多少 team_id，就会生成多少个 team_x.group_1；
        # - 如果未来要缩减为 3 支队伍，优先删 team_id_list，不要在生成结果里零散删除。
        team_id = team_config['team_id']
        team_display_name = team_config.get('team_display_name') or team_id
        team_host = team_config.get('team_host') or '127.0.0.1'
        algorithm_rpc_address = f"{team_host}:9981"
        component_information_model = ComponentInformationModel()
        component_information_model.component_id = team_id + '.' + group_id
        component_information_model.component_type = ComponentCategoryEnum.PROCESSOR.value
        component_information_model.component_info = {
            'algorithm_connection': {
                'address': algorithm_rpc_address
            },
            'team_id': team_id,
            'team_display_name': team_display_name,
            'team_host': team_host,
            'algorithm_rpc_address': algorithm_rpc_address,
            'group_id': group_id,
            'processor_component_id': f'{team_id}.{group_id}',
            'collector_component_id': f'collector_{group_id}',
            'collector_custom_control_topic': f'collector_{group_id}.virtual_receiver_custom_control',
            'runtime_stage_event_topic': 'runtime_stage.event',
        }
        component_information_model.component_group_id = group_id
        component_information_model.message_key_topic_dict = {
            'report': f"{component_information_model.component_id }.report",
            'eeg_1': f"{group_id}.data",
            'eeg_1_calibration_private': f"{team_id}.{group_id}.calibration",
            'eeg_1_online_shared': f"{group_id}.data",
            'command_control': f"{component_information_model.component_id }.command_control",
            'runtime_stage_event': 'runtime_stage.event',
        }
        return component_information_model

    @staticmethod
    def create_runtime_stage_coordinator_model(
        group_id_list: list[str],
        team_id_list: list[str],
    ) -> ComponentInformationModel:
        # 新增的 group_base 协调组件。
        # 当前默认 AUTO_RELEASE_WHEN_ALL_TEAMS_READY；
        # runtime_stage_ui_control_topic 仅做预留，后续可接 UI 手动放行。
        #
        # 这里的 team_id_list_by_group 是 coordinator 判断“某个 group 下应该等哪些队伍 ready”的依据。
        # 因此如果你改了 team_id_list / group_id_list，却没有重新生成配置，
        # coordinator 看到的 waiting roster 就会和实际启动组件不一致。
        component_information_model = ComponentInformationModel()
        component_information_model.component_id = 'runtime_stage_coordinator'
        component_information_model.component_type = ComponentCategoryEnum.CONTROLLER.value
        component_information_model.component_group_id = 'group_base'
        component_information_model.component_info = {
            'release_policy': 'AUTO_RELEASE_WHEN_ALL_TEAMS_READY',
            'trial_release_interval_seconds': 1.3,
            # watchdog 只负责兜底“终态事件未到但不应继续拖慢整组”的场景；
            # 默认对齐 MI 当前 timeout_limit=1.0s，并额外保留 0.3s 上报宽限。
            'trial_terminal_watchdog_base_timeout_seconds': 1.0,
            'trial_terminal_watchdog_grace_seconds': 0.3,
            'enable_runtime_stage_status': False,
            'team_id_list_by_group': {
                group_id: list(team_id_list)
                for group_id in group_id_list
            },
            'runtime_stage_event_topic': 'runtime_stage.event',
            'runtime_stage_control_topic': 'runtime_stage.control',
            'runtime_stage_status_topic': 'runtime_stage.status',
            'runtime_stage_ui_control_topic': 'runtime_stage.ui_control',
        }
        component_information_model.message_key_topic_dict = {
            'runtime_stage_event': 'runtime_stage.event',
            'runtime_stage_control': 'runtime_stage.control',
            'runtime_stage_status': 'runtime_stage.status',
            'runtime_stage_ui_control': 'runtime_stage.ui_control',
        }
        return component_information_model

    @staticmethod
    def create_data_storage_model(group_id_list: list[str]) -> ComponentInformationModel:
        component_information_model = ComponentInformationModel()
        component_information_model.component_id = 'data_storage'
        component_information_model.component_type = ComponentCategoryEnum.DATASTORAGE.value
        # 注册时即填入所需要订阅的message_key
        data_storage_message_key_to_topic_dict = {
            f"{group_id}.data": f"{group_id}.data" for group_id in group_id_list
        }

        component_information_model.component_info = {
            "message": {
                message_key: None
                for message_key in
                data_storage_message_key_to_topic_dict.keys()
            },
            "stimulator_components": {
                f"stimulator_{group_id}": None for group_id in group_id_list
            },
        }
        component_information_model.component_group_id = 'group_base'
        component_information_model.message_key_topic_dict = {
            'command_control': f"{component_information_model.component_id}.command_control"
        }
        component_information_model.message_key_topic_dict.update(data_storage_message_key_to_topic_dict)
        return component_information_model

    @staticmethod
    def create_database_model(group_id_list: list[str], team_id_list: list[str]) -> ComponentInformationModel:
        component_information_model = ComponentInformationModel()
        component_information_model.component_id = 'database'
        component_information_model.component_type = ComponentCategoryEnum.DATABASE.value
        # 注册时即填入所需要订阅的message_key
        data_storage_message_key_to_topic_dict = {
            f"{group_id}.data": f"{group_id}.data" for group_id in group_id_list
        }
        result_storage_message_key_to_topic_dict = {
            f"{team_id}.{group_id}.report": f"{team_id}.{group_id}.report"
            for team_id in team_id_list for group_id in group_id_list
        }
        total_storage_message_key_to_topic_dict = dict()
        total_storage_message_key_to_topic_dict.update(data_storage_message_key_to_topic_dict)
        total_storage_message_key_to_topic_dict.update(result_storage_message_key_to_topic_dict)

        component_information_model.component_info = {
            'message': {
                message_key: None
                for message_key in
                data_storage_message_key_to_topic_dict.keys()
            },
            "process_components": {
                f"{team_id}.{group_id}": f"{team_id}.{group_id}.report"
                for team_id in team_id_list for group_id in group_id_list
            },
            "stimulator_components": {
                f"stimulator_{group_id}": None for group_id in group_id_list
            },
        }

        component_information_model.component_group_id = 'group_base'
        component_information_model.message_key_topic_dict = {
            'command_control': f"{component_information_model.component_id}.command_control"
        }
        component_information_model.message_key_topic_dict.update(total_storage_message_key_to_topic_dict)
        return component_information_model

    def create_central_controller_model(self) -> ComponentInformationModel:
        central_controller_model = ComponentInformationModel()
        central_controller_model.component_id = 'central_controller'
        central_controller_model.component_type = ComponentCategoryEnum.CONTROLLER.value
        central_controller_model.component_info = dict()
        central_controller_model.component_group_id = 'group_base'
        central_controller_model.message_key_topic_dict = {}
        component_info_message_dict = dict()
        for component_information_model in self.components:
            if component_information_model.component_type == 'COLLECTOR':
                key_str = f"{component_information_model.component_id}.{CollectorMessageKeyEnum.COMMAND_CONTROL.value}"
                central_controller_model.message_key_topic_dict[key_str] = key_str
                component_info_message_dict[key_str] = None
            elif component_information_model.component_type == 'STIMULATOR':
                key_str = f"{component_information_model.component_id}.{StimulatorMessageKeyEnum.COMMAND_CONTROL.value}"
                central_controller_model.message_key_topic_dict[key_str] = key_str
                component_info_message_dict[key_str] = None
                key_str = f"{component_information_model.component_id}.{StimulatorMessageKeyEnum.RANDOM_NUMBER_SEEDS.value}"
                central_controller_model.message_key_topic_dict[key_str] = key_str
                component_info_message_dict[key_str] = None
            elif component_information_model.component_type == 'DATASTORAGE':
                key_str = f"{component_information_model.component_id}.command_control"
                central_controller_model.message_key_topic_dict[key_str] = key_str
                component_info_message_dict[key_str] = None
            elif component_information_model.component_type == 'DATABASE':
                key_str = f"{component_information_model.component_id}.command_control"
                central_controller_model.message_key_topic_dict[key_str] = key_str
                component_info_message_dict[key_str] = None
        central_controller_model.component_info['message'] = component_info_message_dict
        return central_controller_model

    @staticmethod
    def component_information_model_to_dict(component_information_model: ComponentInformationModel) -> dict:
        return {
            component_information_model.component_id: {
                'component_type': component_information_model.component_type,
                'component_info': component_information_model.component_info,
                'component_group_id': component_information_model.component_group_id,
                'message_key_topic_dict': component_information_model.message_key_topic_dict,
            }
        }

    @staticmethod
    def group_information_model_to_dict(group_information_model: GroupInformationModel) -> dict:
        return {
            group_information_model.group_id:
                {
                    'group_info': group_information_model.group_info,
                    'message_key_topic_dict': group_information_model.message_key_topic_dict,
                }
        }


if __name__ == '__main__':
    CentralControllerConfigCreator().run()
