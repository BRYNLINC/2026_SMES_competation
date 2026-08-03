from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class HttpExpectation:
    method: str
    path: str
    expected_status: int
    required_keys: tuple[str, ...] = ()


def build_local_url(host: str, port: int, path: str) -> str:
    normalized_host = str(host or "127.0.0.1").strip()
    normalized_path = "/" + str(path or "").lstrip("/")
    return f"http://{normalized_host}:{int(port)}{normalized_path}"


def validate_status_code(status_code: int, expected_status: int) -> None:
    if int(status_code) != int(expected_status):
        raise AssertionError(f"unexpected status code: observed={status_code}, expected={expected_status}")


def validate_json_keys(payload: dict[str, Any], required_keys: tuple[str, ...]) -> None:
    missing_key_list = [key for key in required_keys if key not in payload]
    if missing_key_list:
        raise AssertionError(f"missing json keys: {missing_key_list}")
