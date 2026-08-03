from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any


DEFAULT_ALGORITHM_RPC_PORT = 9981
CONTROL_METHOD_NAME_SET = {"getStatus", "sendConfig", "getConfig", "shutdown"}
STREAM_METHOD_NAME_SET = {"connect", "calibrate", "predict"}


@dataclass(frozen=True)
class GrpcAvailability:
    available: bool
    reason: str = ""
    grpc_version: str = ""


@dataclass(frozen=True)
class RpcCallContract:
    method: str
    timeout_seconds: float
    channel_kind: str
    idempotent: bool
    expected_request: str
    expected_response: str


def probe_grpc_availability() -> GrpcAvailability:
    try:
        grpc_module = importlib.import_module("grpc")
    except Exception as exc:
        return GrpcAvailability(available=False, reason=summarize_grpc_unavailable(exc))
    return GrpcAvailability(
        available=True,
        grpc_version=str(getattr(grpc_module, "__version__", "unknown")),
    )


def is_grpc_available() -> bool:
    return probe_grpc_availability().available


def build_algorithm_endpoint(host: str = "127.0.0.1", port: int = DEFAULT_ALGORITHM_RPC_PORT) -> str:
    normalized_host = str(host or "").strip()
    if not normalized_host:
        raise ValueError("host must not be empty")
    normalized_port = int(port)
    if normalized_port < 1 or normalized_port > 65535:
        raise ValueError(f"port out of range: {normalized_port}")
    if ":" in normalized_host and not normalized_host.startswith("["):
        normalized_host = f"[{normalized_host}]"
    return f"{normalized_host}:{normalized_port}"


def build_control_call_contract(method: str, timeout_seconds: float = 1.0) -> RpcCallContract:
    normalized_method = _normalize_method(method)
    if normalized_method not in CONTROL_METHOD_NAME_SET:
        raise ValueError(f"unknown control RPC method: {method}")
    timeout = _normalize_timeout(timeout_seconds)
    response_map = {
        "getStatus": "AlgorithmStatusMessage",
        "sendConfig": "EmptyMessage",
        "getConfig": "StringMessage",
        "shutdown": "BooleanMessage",
    }
    request_map = {
        "getStatus": "EmptyMessage",
        "sendConfig": "StringMessage(yaml_config)",
        "getConfig": "EmptyMessage",
        "shutdown": "EmptyMessage",
    }
    return RpcCallContract(
        method=normalized_method,
        timeout_seconds=timeout,
        channel_kind="unary",
        idempotent=normalized_method in {"getStatus", "getConfig"},
        expected_request=request_map[normalized_method],
        expected_response=response_map[normalized_method],
    )


def build_stream_call_contract(method: str, timeout_seconds: float = 1.0) -> RpcCallContract:
    normalized_method = _normalize_method(method)
    if normalized_method not in STREAM_METHOD_NAME_SET:
        raise ValueError(f"unknown stream RPC method: {method}")
    timeout = _normalize_timeout(timeout_seconds)
    request_response_map: dict[str, tuple[str, str]] = {
        "connect": ("AlgorithmDataStreamRequest", "AlgorithmDataStreamResponse"),
        "calibrate": ("AlgorithmCalibrationRequest", "AlgorithmCalibrationReady"),
        "predict": ("AlgorithmPredictionRequest", "AlgorithmPredictionResult"),
    }
    expected_request, expected_response = request_response_map[normalized_method]
    return RpcCallContract(
        method=normalized_method,
        timeout_seconds=timeout,
        channel_kind="stream",
        idempotent=False,
        expected_request=expected_request,
        expected_response=expected_response,
    )


def summarize_grpc_unavailable(exc: BaseException | Any) -> str:
    exc_type = type(exc).__name__
    message = str(exc).strip() or repr(exc)
    lower_message = message.lower()
    if "winerror 10106" in lower_message or "base_events" in lower_message:
        return f"{exc_type}: system_network_stack_unavailable: {message}"
    if "no module named" in lower_message:
        return f"{exc_type}: grpc_dependency_missing: {message}"
    return f"{exc_type}: {message}"


def _normalize_method(method: str) -> str:
    normalized = str(method or "").strip()
    if not normalized:
        raise ValueError("method must not be empty")
    return normalized


def _normalize_timeout(timeout_seconds: float) -> float:
    timeout = float(timeout_seconds)
    if timeout <= 0:
        raise ValueError(f"timeout_seconds must be positive: {timeout_seconds}")
    return timeout
