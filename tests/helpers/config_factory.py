from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

import yaml

from tests.helpers.project_paths import project_root


def _ensure_component_import_paths() -> None:
    root = project_root()
    for app_root in (
        root / "app" / "Collector",
        root / "app" / "CentralController",
    ):
        path_text = str(app_root)
        if path_text not in sys.path:
            sys.path.insert(0, path_text)


_ensure_component_import_paths()

from Collector.receiver.virtual_receiver.VirtualReceiverConfigCreator import VirtualReceiverConfigCreator
from CentralController.config.CentralControllerConfigCreator import CentralControllerConfigCreator


DEFAULT_GROUP_ID_LIST = ["group_1"]
DEFAULT_RUNTIME_STAGE_TIMINGS = {
    "release_policy": "AUTO_RELEASE_WHEN_ALL_TEAMS_READY",
    "trial_release_interval_seconds": 0.05,
    "trial_terminal_watchdog_base_timeout_seconds": 0.20,
    "trial_terminal_watchdog_grace_seconds": 0.05,
    "enable_runtime_stage_status": True,
}
DEFAULT_JUDGE_DASHBOARD_ORIGINS = [
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:4173",
    "http://localhost:4173",
]


def _write_yaml(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return path


def _build_central_controller_payload(creator: CentralControllerConfigCreator) -> dict[str, Any]:
    groups_dict: dict[str, Any] = {}
    group_base_model = CentralControllerConfigCreator.create_group_model("group_base")
    groups_dict.update(creator.group_information_model_to_dict(group_base_model))

    for group_id in creator.group_id_list:
        group_model = CentralControllerConfigCreator.create_group_model(group_id)
        groups_dict.update(creator.group_information_model_to_dict(group_model))

        group_processor_component_model_list = []
        for team_config in creator.team_config_list:
            if not team_config.get("enabled", True):
                continue
            processor_component_model = CentralControllerConfigCreator.create_processor_model(group_id, team_config)
            creator.components.append(processor_component_model)
            group_processor_component_model_list.append(processor_component_model)

        collector_component_model = creator.create_collector_model(group_id, creator.team_id_list)
        creator.components.append(collector_component_model)
        if group_processor_component_model_list:
            stimulator_component_model = creator.create_stimulator_model(
                group_id,
                group_processor_component_model_list[0].component_id,
                collector_component_model.component_id,
            )
            creator.components.append(stimulator_component_model)

    creator.components.append(creator.create_data_storage_model(creator.group_id_list))
    creator.components.append(creator.create_database_model(creator.group_id_list, creator.team_id_list))
    creator.components.append(
        creator.create_runtime_stage_coordinator_model(
            creator.group_id_list,
            creator.team_id_list,
        )
    )
    creator.components.append(creator.create_central_controller_model())

    payload: dict[str, Any] = {
        "groups": groups_dict,
        "components": {},
    }
    for component in creator.components:
        payload["components"].update(creator.component_information_model_to_dict(component))
    return payload


def build_team_config(
    team_count: int,
    base_port: int,
    profiles: list[str] | dict[int | str, str] | None = None,
) -> list[dict[str, Any]]:
    team_total = max(0, int(team_count))
    starting_port = int(base_port)
    resolved_profiles: dict[int, str] = {}
    if isinstance(profiles, list):
        resolved_profiles = {index: profile for index, profile in enumerate(profiles)}
    elif isinstance(profiles, dict):
        for key, profile in profiles.items():
            if isinstance(key, int):
                resolved_profiles[int(key)] = str(profile)
                continue
            key_text = str(key).strip().lower()
            if key_text.startswith("team_") and key_text.split("_")[-1].isdigit():
                resolved_profiles[int(key_text.split("_")[-1])] = str(profile)

    team_config_list: list[dict[str, Any]] = []
    for index in range(team_total):
        team_id = f"team_{index}"
        algorithm_port = starting_port + index
        profile_name = str(resolved_profiles.get(index, "normal"))
        team_config_list.append(
            {
                "team_id": team_id,
                "team_display_name": f"Auto Team {index}",
                "team_host": "127.0.0.1",
                "enabled": True,
                "algorithm_profile": profile_name,
                "algorithm_port": algorithm_port,
                "algorithm_rpc_address": f"127.0.0.1:{algorithm_port}",
            }
        )
    return team_config_list


def write_central_controller_config(
    sandbox_root: str | Path,
    team_config_list: list[dict[str, Any]],
) -> Path:
    sandbox_path = Path(sandbox_root)
    config_path = sandbox_path / "app" / "CentralController" / "CentralController" / "config" / "CentralControllerConfig.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    creator = CentralControllerConfigCreator()
    creator.team_config_list = [copy.deepcopy(team_config) for team_config in team_config_list]
    creator.team_id_list = [
        str(team_config["team_id"])
        for team_config in creator.team_config_list
        if team_config.get("enabled", True)
    ]
    creator.group_id_list = list(DEFAULT_GROUP_ID_LIST)
    creator.components = []

    payload = _build_central_controller_payload(creator)
    _write_yaml(config_path, payload)

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    components = payload.setdefault("components", {})

    for team_config in creator.team_config_list:
        if not team_config.get("enabled", True):
            continue
        team_id = str(team_config["team_id"])
        algorithm_rpc_address = str(
            team_config.get("algorithm_rpc_address")
            or f"{team_config.get('team_host', '127.0.0.1')}:9981"
        )
        algorithm_profile = str(team_config.get("algorithm_profile", "normal"))
        for group_id in creator.group_id_list:
            component_id = f"{team_id}.{group_id}"
            component = components.get(component_id)
            if not isinstance(component, dict):
                continue
            component_info = component.setdefault("component_info", {})
            component_info["algorithm_rpc_address"] = algorithm_rpc_address
            component_info["algorithm_profile"] = algorithm_profile
            component_info.setdefault("algorithm_connection", {})
            component_info["algorithm_connection"]["address"] = algorithm_rpc_address

    _write_yaml(config_path, payload)
    return config_path


def write_runtime_stage_config(
    sandbox_root: str | Path,
    team_id_list_by_group: dict[str, list[str]],
    timings: dict[str, Any] | None = None,
) -> Path:
    sandbox_path = Path(sandbox_root)
    config_path = (
        sandbox_path
        / "app"
        / "ProcessHub"
        / "ApplicationFramework"
        / "config"
        / "RuntimeStageCoordinatorLauncherConfig.yml"
    )
    runtime_stage_component_info = dict(DEFAULT_RUNTIME_STAGE_TIMINGS)
    if timings:
        runtime_stage_component_info.update(copy.deepcopy(timings))
    runtime_stage_component_info["team_id_list_by_group"] = {
        str(group_id): [str(team_id) for team_id in team_id_list]
        for group_id, team_id_list in team_id_list_by_group.items()
    }
    runtime_stage_component_info.setdefault("runtime_stage_event_topic", "runtime_stage.event")
    runtime_stage_component_info.setdefault("runtime_stage_control_topic", "runtime_stage.control")
    runtime_stage_component_info.setdefault("runtime_stage_status_topic", "runtime_stage.status")
    runtime_stage_component_info.setdefault("runtime_stage_ui_control_topic", "runtime_stage.ui_control")

    payload = {
        "version": 1,
        "application": {
            "application_class_file": "RuntimeStageCoordinator/application/ApplicationImplement.py",
            "application_class_name": "ApplicationImplement",
        },
        "runtime_stage_coordinator_component_info": runtime_stage_component_info,
    }
    return _write_yaml(config_path, payload)


def _resolve_dataset_entry(
    sandbox_path: Path,
    entry: str | dict[str, Any],
) -> tuple[Path, str]:
    if isinstance(entry, dict):
        source_path_text = str(entry.get("source_path") or entry.get("path") or "").strip()
        yaml_path = str(entry.get("yaml_path") or entry.get("path") or "").strip()
    else:
        source_path_text = str(entry).strip()
        yaml_path = source_path_text
    if not source_path_text:
        raise ValueError("dataset entry source_path is required")
    source_path = Path(source_path_text)
    if not source_path.is_absolute():
        source_path = sandbox_path / source_path
    if not source_path.exists():
        raise FileNotFoundError(f"dataset file does not exist: {source_path}")
    if not yaml_path:
        raise ValueError("dataset entry yaml_path is required")
    return source_path, yaml_path.replace("\\", "/")


def write_virtual_receiver_config(
    sandbox_root: str | Path,
    dataset_spec: dict[str, Any],
) -> Path:
    sandbox_path = Path(sandbox_root)
    config_path = (
        sandbox_path
        / "app"
        / "Collector"
        / "Collector"
        / "receiver"
        / "virtual_receiver"
        / "VirtualReceiverConfig.yml"
    )
    payload: dict[str, Any] = {}
    payload.update(VirtualReceiverConfigCreator.create_message())
    payload.update(
        copy.deepcopy(
            dataset_spec.get("send_config")
            or VirtualReceiverConfigCreator.create_send_config()
        )
    )
    payload.update(VirtualReceiverConfigCreator.create_device_info())

    data_files_spec = dataset_spec.get("data_files") or {}
    normalized_data_files: dict[str, dict[str, list[str]]] = {}
    for subject_id, subject_payload in data_files_spec.items():
        normalized_subject_payload: dict[str, list[str]] = {}
        for paradigm_key, file_entries in dict(subject_payload or {}).items():
            yaml_path_list: list[str] = []
            for file_entry in list(file_entries or []):
                _, yaml_path = _resolve_dataset_entry(sandbox_path, file_entry)
                yaml_path_list.append(yaml_path)
            if yaml_path_list:
                normalized_subject_payload[str(paradigm_key)] = sorted(yaml_path_list)
        if normalized_subject_payload:
            normalized_data_files[str(subject_id)] = normalized_subject_payload

    if not normalized_data_files:
        raise ValueError("dataset_spec.data_files must include at least one existing file")

    payload["data_files"] = normalized_data_files
    return _write_yaml(config_path, payload)


def patch_judge_web_config(
    sandbox_root: str | Path,
    host: str,
    port: int,
    local_only: bool,
) -> Path:
    sandbox_path = Path(sandbox_root)
    config_path = sandbox_path / "app" / "JudgeWeb" / "JudgeWeb" / "config" / "JudgeWebConfig.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    else:
        source_config_path = project_root() / "app" / "JudgeWeb" / "JudgeWeb" / "config" / "JudgeWebConfig.yml"
        payload = yaml.safe_load(source_config_path.read_text(encoding="utf-8")) or {}

    server_payload = payload.setdefault("server", {})
    server_payload["host"] = str(host)
    server_payload["port"] = int(port)
    server_payload["local_only"] = bool(local_only)
    cors_allow_origins = [f"http://{host}:{int(port)}", f"http://localhost:{int(port)}", *DEFAULT_JUDGE_DASHBOARD_ORIGINS]
    deduplicated_origins: list[str] = []
    for origin in cors_allow_origins:
        if origin not in deduplicated_origins:
            deduplicated_origins.append(origin)
    server_payload["cors_allow_origins"] = deduplicated_origins
    return _write_yaml(config_path, payload)
