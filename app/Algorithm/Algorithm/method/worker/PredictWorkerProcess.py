import copy
import importlib.util
import logging
import os
import sys
import traceback
from multiprocessing.queues import Queue
from pathlib import Path

import numpy as np

from Algorithm.common.utils.seed import (
    DEFAULT_GLOBAL_SEED,
    seed_everything,
    seed_everything_for_stage,
)

DEFAULT_METHOD_CLASS_FILE = 'Algorithm/method/model_artifacts/baseline_example/AlgorithmImplement.py'
DEFAULT_METHOD_CLASS_NAME = 'AlgorithmImplement'


def _resolve_default_workspace_path() -> Path:
    current_working_path = Path(os.getcwd()).resolve()
    if (current_working_path / 'Algorithm' / 'method').is_dir():
        return current_working_path
    app_algorithm_path = current_working_path / 'app' / 'Algorithm'
    if (app_algorithm_path / 'Algorithm' / 'method').is_dir():
        return app_algorithm_path
    pyd_algorithm_path = current_working_path / 'pyd_app' / 'Algorithm'
    if (pyd_algorithm_path / 'Algorithm' / 'method').is_dir():
        return pyd_algorithm_path
    return current_working_path


def _load_algorithm_instance(method_config: dict | None):
    method_config = dict(method_config or {})
    method_class_file_is_explicit = bool(
        method_config.get('method_class_file') or method_config.get('algorithm_class_file')
    )
    method_class_file = (
        method_config.get('method_class_file')
        or method_config.get('algorithm_class_file')
        or DEFAULT_METHOD_CLASS_FILE
    )
    method_class_name = (
        method_config.get('method_class_name')
        or method_config.get('algorithm_class_name')
        or DEFAULT_METHOD_CLASS_NAME
    )
    workspace_path = Path(str(method_config.get('workspace_path') or _resolve_default_workspace_path()))
    method_class_path = Path(str(method_class_file))
    if not method_class_path.is_absolute():
        method_class_path = workspace_path / method_class_path
    method_class_path = method_class_path.resolve()
    if not method_class_path.exists() and not method_class_file_is_explicit:
        model_artifacts_root = workspace_path / 'Algorithm' / 'method' / 'model_artifacts'
        candidate_path_list = sorted(model_artifacts_root.glob('*/AlgorithmImplement.py'))
        if len(candidate_path_list) == 1:
            method_class_path = candidate_path_list[0].resolve()
        elif len(candidate_path_list) > 1:
            raise RuntimeError(
                'predict worker algorithm entry is ambiguous under model_artifacts; '
                f'candidates={candidate_path_list}'
            )
    if not method_class_path.exists():
        raise FileNotFoundError(f'predict worker algorithm file not found: {method_class_path}')

    module_dir = str(method_class_path.parent)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    module_name = f'_bci_predict_worker_{method_class_path.stem}_{abs(hash(str(method_class_path)))}'
    spec = importlib.util.spec_from_file_location(module_name, method_class_path)
    if spec is None or spec.loader is None:
        raise ImportError(f'cannot load predict worker algorithm module: {method_class_path}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    method_class = getattr(module, method_class_name)
    return method_class()


def predict_worker_main(command_queue: Queue, result_queue: Queue, method_config: dict | None = None) -> None:
    logger = logging.getLogger('algorithmLogger')
    seed_everything(DEFAULT_GLOBAL_SEED)
    algorithm_instance = _load_algorithm_instance(method_config)
    current_device = 'cpu'
    current_stage_signature = None

    while True:
        command = command_queue.get()
        command_type = command.get('command')
        try:
            if command_type == 'shutdown':
                result_queue.put({'type': 'shutdown_ack'})
                return

            if command_type == 'load_session':
                runtime_config = copy.deepcopy(command.get('runtime_config') or {})
                current_stage_signature = tuple(command.get('stage_signature') or ())
                if len(current_stage_signature) != 4:
                    raise ValueError(
                        'predict worker stage_signature must contain '
                        'subject_id, exp_name, exp_task and session_id'
                    )
                stage_seed = seed_everything_for_stage(*current_stage_signature)
                current_device = algorithm_instance.load_predict_session(
                    runtime_config=runtime_config,
                    stage_signature=current_stage_signature,
                    sample_rate=int(command.get('sample_rate') or 0),
                    channel_number=int(command.get('channel_number') or 0),
                    trial_point=int(command.get('trial_point') or 0),
                    model_state_dict=command.get('model_state_dict') or {},
                )
                result_queue.put(
                    {
                        'type': 'load_session_ready',
                        'session_token': command.get('session_token'),
                        'stage_signature': list(current_stage_signature),
                        'stage_seed': stage_seed,
                        'device': current_device,
                    }
                )
                continue

            if command_type == 'predict':
                if current_stage_signature is None:
                    raise RuntimeError('predict worker has no loaded session model')
                trial_data = np.asarray(command.get('trial_data'), dtype=np.float32)
                result_queue.put(
                    {
                        'type': 'predict_result',
                        'request_id': command.get('request_id'),
                        'result': algorithm_instance.predict(trial_data),
                        'stage_signature': list(current_stage_signature or ()),
                    }
                )
                continue

            raise ValueError(f'unsupported worker command: {command_type}')
        except Exception as exc:
            logger.exception('predict worker command failed: command_type=%s', command_type)
            result_queue.put(
                {
                    'type': 'worker_error',
                    'command': command_type,
                    'request_id': command.get('request_id'),
                    'session_token': command.get('session_token'),
                    'error': str(exc),
                    'traceback': traceback.format_exc(),
                }
            )

