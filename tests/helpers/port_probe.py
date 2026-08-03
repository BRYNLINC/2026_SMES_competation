from __future__ import annotations

from collections.abc import Iterable


def normalize_port_list(port_list: Iterable[int | str]) -> list[int]:
    normalized_port_list: list[int] = []
    for raw_port in port_list:
        port = int(str(raw_port).strip())
        if 1 <= port <= 65535 and port not in normalized_port_list:
            normalized_port_list.append(port)
    return normalized_port_list


def classify_port_conflicts(required_port_list: Iterable[int | str], listening_port_pid_map: dict[int, list[int]]) -> dict[int, list[int]]:
    conflict_map: dict[int, list[int]] = {}
    for port in normalize_port_list(required_port_list):
        pid_list = list(listening_port_pid_map.get(port) or [])
        if pid_list:
            conflict_map[port] = pid_list
    return conflict_map


def is_port_set_clean(required_port_list: Iterable[int | str], listening_port_pid_map: dict[int, list[int]]) -> bool:
    return classify_port_conflicts(required_port_list, listening_port_pid_map) == {}
