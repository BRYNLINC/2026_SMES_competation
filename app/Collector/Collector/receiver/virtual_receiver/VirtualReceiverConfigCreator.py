import os
import re
from pathlib import Path

import yaml

from Collector.receiver.virtual_receiver.api.message.VirtualReceiverMessageKeyEnum import VirtualReceiverMessageKeyEnum


class VirtualReceiverConfigCreator:
    EXP_TASK_ORDER = ['left_vs_rest', 'right_vs_rest']
    EEG_CHANNEL_LABEL_LIST = [
        'FP1', 'FPZ', 'FP2', 'AF3', 'AF4', 'F7', 'F5', 'F3', 'F1', 'FZ', 'F2', 'F4', 'F6', 'F8',
        'FT7', 'FC5', 'FC3', 'FC1', 'FCZ', 'FC2', 'FC4', 'FC6', 'FT8', 'T7', 'C5', 'C3', 'C1', 'CZ',
        'C2', 'C4', 'C6', 'T8', 'M1', 'TP7', 'CP5', 'CP3', 'CP1', 'CPZ', 'CP2', 'CP4', 'CP6', 'TP8',
        'M2', 'P7', 'P5', 'P3', 'P1', 'PZ', 'P2', 'P4', 'P6', 'P8', 'PO7', 'PO5', 'PO3', 'POZ',
        'PO4', 'PO6', 'PO8', 'CB1', 'O1', 'OZ', 'O2', 'CB2',
    ]
    AUX_CHANNEL_LABEL_LIST = ['HEO', 'VEO', 'EKG', 'EMG']
    TRIGGER_CHANNEL_ALIAS_LIST = ['TRIGGER', 'TRIG', 'STATUS', 'EVENT', 'MARKER', 'STI014']

    def run(self):
        virtual_receiver_config_dict = {}
        virtual_receiver_config_dict.update(self.create_message())
        virtual_receiver_config_dict.update(self.create_send_config())
        virtual_receiver_config_dict.update(self.create_device_info())
        # prefix_dir 前缀一定不要加/ 否则表示相对根目录的绝对路径
        virtual_receiver_config_dict.update(
            self.create_data_files(r'C:\BCI_competation_2026\final_code6\final_code\app\Collector\Collector\receiver\virtual_receiver\data', # 请替换为当前决赛工程的数据目录
                                   'Collector/receiver/virtual_receiver/data',
                                   # ['set', 'fdt']
                                   ['dat']
                                   )
        )
        with open('VirtualReceiverConfig.yml', 'w', encoding='utf-8') as file:
            yaml.dump(virtual_receiver_config_dict, file, sort_keys=False)

    @staticmethod
    def create_send_config():
        send_config = {
            'send_config':
                {
                    'send_package_points': 4000,  # 决赛 online 每个 trial 尽量整段发送，减少分包转发开销
                    'online_replay_mode': 'burst',  # 决赛 online 使用 burst 模式快速发送整段 trial 数据
                }
        }
        return send_config

    @staticmethod
    def create_device_info():
        eeg_channel_label_dict = {
            channel_label: None for channel_label in VirtualReceiverConfigCreator.EEG_CHANNEL_LABEL_LIST
        }
        device_info = {
            'device_info': {
                'channel_number': len(VirtualReceiverConfigCreator.EEG_CHANNEL_LABEL_LIST),
                'sample_rate': 1000,
                'channel_label': eeg_channel_label_dict,
                'other_information': {
                    'aux_channel_alias': VirtualReceiverConfigCreator.AUX_CHANNEL_LABEL_LIST,
                    'trigger_channel_alias': VirtualReceiverConfigCreator.TRIGGER_CHANNEL_ALIAS_LIST,
                    'exp_task_order': VirtualReceiverConfigCreator.EXP_TASK_ORDER,
                }
                }
        }
        return device_info

    def create_data_files(self, target_dir: str, prefix_dir: str, extensions: list[str]):
        # 按照 被试者:block方式构建
        relative_paths = self.__collect_files_by_subdirectories(target_dir, prefix_dir, extensions)
        data_files = {
            'data_files': relative_paths
        }
        return data_files

    @staticmethod
    def create_message():
        message_dict = {
            'message':
                {
                    VirtualReceiverMessageKeyEnum.VIRTUAL_RECEIVER_CUSTOM_CONTROL.value: None
                }
        }
        return message_dict

    @staticmethod
    def __collect_files_by_subdirectories(target_dir: str, prefix_dir: str, extensions: list[str]):
        """
        递归地收集每个被试目录下所有具有指定扩展名的文件的相对路径，
        并按实验范式分组。在每个路径前添加预定义的前缀。

        参数:
        target_dir (str): 根目录的路径。
        prefix_dir (str): 添加到每个相对路径前的前缀。
        extensions (list): 文件扩展名列表。

        返回:
        dict: 字典，结构为 {被试名: {实验范式名: [文件路径, ...]}}。
             例如 {'A': {'vmi': [...], 'vme': [...]}}。
        字典内元素形式
        A:
            vmi:
                - file_1
                - file_2
            vme:
                - file_3
                - file_4
        """
        results = {}
        root_path = Path(target_dir)
        normalized_extensions = {ext.lstrip('.').lower() for ext in extensions}
        paradigm_pattern = re.compile(r'_([a-zA-Z0-9]+)_run\d+$', re.IGNORECASE)

        for subject_dir in sorted(root_path.iterdir()):
            if not subject_dir.is_dir():
                continue

            subject_key = str(subject_dir.relative_to(root_path))
            subject_data = {}

            for file_path in sorted(subject_dir.rglob('*')):
                if not file_path.is_file():
                    continue
                if file_path.suffix.lstrip('.').lower() not in normalized_extensions:
                    continue

                paradigm_match = paradigm_pattern.search(file_path.stem)
                if paradigm_match is None:
                    continue

                paradigm_key = paradigm_match.group(1).lower()
                relative_file = f"{prefix_dir}/{str(file_path.relative_to(root_path)).replace(os.path.sep, '/')}"
                subject_data.setdefault(paradigm_key, []).append(relative_file)

            if subject_data:
                results[subject_key] = {
                    paradigm_key: sorted(file_paths)
                    for paradigm_key, file_paths in sorted(subject_data.items())
                }

        return results


if __name__ == '__main__':
    VirtualReceiverConfigCreator().run()
