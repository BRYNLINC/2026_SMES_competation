from __future__ import annotations

import ast
import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest


FINAL_CODE_ROOT = Path(__file__).resolve().parents[2]
COMPETITION_ROOT = FINAL_CODE_ROOT.parents[1]
FINAL_PROCESS_HUB_APP_ROOT = FINAL_CODE_ROOT / "app" / "ProcessHub"
if str(FINAL_PROCESS_HUB_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(FINAL_PROCESS_HUB_APP_ROOT))

from Algorithm.api.proto import AlgorithmRPCService_pb2 as final_algorithm_rpc_pb2
from Common.protobuf import CommonMessage_pb2 as final_common_message_pb2


VIRTUAL_RECEIVER_PATH = (
    FINAL_CODE_ROOT
    / "app"
    / "Collector"
    / "Collector"
    / "receiver"
    / "virtual_receiver"
    / "VirtualReceiverImplement.py"
)
def _resolve_preliminary_pyd_root() -> Path:
    configured_root = os.environ.get("BCI_PRELIMINARY_PYD_ROOT")
    candidate_list = [
        Path(configured_root) if configured_root else None,
        FINAL_CODE_ROOT / "preliminary_pyd",
        COMPETITION_ROOT / "初赛" / "preliminary_pyd",
    ]
    for candidate in candidate_list:
        if candidate is not None and (candidate / "pyd_app").is_dir():
            return candidate.resolve()
    return (FINAL_CODE_ROOT / "preliminary_pyd").resolve()


PRELIMINARY_PYD_ROOT = _resolve_preliminary_pyd_root()
PRELIMINARY_PYD_APP_ROOT = PRELIMINARY_PYD_ROOT / "pyd_app"


def _find_async_method(module_node: ast.Module, method_name: str) -> ast.AsyncFunctionDef:
    for node in ast.walk(module_node):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == method_name:
            return node
    raise AssertionError(f"async method not found: {method_name}")


def _awaited_method_call_list(method_node: ast.AsyncFunctionDef) -> list[ast.Call]:
    call_list: list[ast.Call] = []
    for node in ast.walk(method_node):
        if not isinstance(node, ast.Await) or not isinstance(node.value, ast.Call):
            continue
        if isinstance(node.value.func, ast.Attribute):
            call_list.append(node.value)
    return sorted(call_list, key=lambda call: (call.lineno, call.col_offset))


def _method_call_name(call_node: ast.Call) -> str:
    assert isinstance(call_node.func, ast.Attribute)
    return call_node.func.attr


def _assert_force_online_device_call(call_node: ast.Call) -> None:
    assert len(call_node.args) >= 2
    assert isinstance(call_node.args[1], ast.Constant)
    assert call_node.args[1].value == "online"
    force_resend_keyword = next(
        (keyword for keyword in call_node.keywords if keyword.arg == "force_resend"),
        None,
    )
    assert force_resend_keyword is not None
    assert isinstance(force_resend_keyword.value, ast.Constant)
    assert force_resend_keyword.value.value is True


def _assert_trial_sender_builds_float32_data_transfer(method_node: ast.AsyncFunctionDef) -> None:
    transfer_data_is_float32 = False
    numeric_data_transfer_exists = False
    for node in ast.walk(method_node):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "transfer_data"
            for target in node.targets
        ):
            value = node.value
            if (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Attribute)
                and value.func.attr == "astype"
                and value.args
                and isinstance(value.args[0], ast.Attribute)
                and isinstance(value.args[0].value, ast.Name)
                and value.args[0].value.id == "np"
                and value.args[0].attr == "float32"
            ):
                transfer_data_is_float32 = True
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "DataTransferModel"
        ):
            data_keyword = next(
                (keyword for keyword in node.keywords if keyword.arg == "data"),
                None,
            )
            if (
                data_keyword is not None
                and isinstance(data_keyword.value, ast.Name)
                and data_keyword.value.id == "transfer_data"
            ):
                numeric_data_transfer_exists = True
    assert transfer_data_is_float32
    assert numeric_data_transfer_exists


def _resolve_python310() -> Path | None:
    candidate_list = [
        Path(os.environ["BCI_PYTHON_EXE"])
        if os.environ.get("BCI_PYTHON_EXE")
        else None,
        Path(r"D:\anaconda3\envs\BCI_competation_2026\python.exe"),
        Path(sys.executable),
    ]
    for candidate in candidate_list:
        if candidate is None or not candidate.is_file():
            continue
        completed = subprocess.run(
            [str(candidate), "-c", "import sys; print(sys.version_info[:2])"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.returncode == 0 and "(3, 10)" in completed.stdout:
            return candidate
    return None


def _extract_json_payload(stdout: str) -> dict:
    for line in reversed(stdout.splitlines()):
        candidate = line.strip()
        if candidate.startswith("{") and candidate.endswith("}"):
            return json.loads(candidate)
    raise AssertionError(f"old PYD probe did not emit JSON:\n{stdout}")


def _hash_preliminary_pyd_tree() -> tuple[int, str]:
    digest = hashlib.sha256()
    pyd_path_list = sorted(PRELIMINARY_PYD_ROOT.rglob("*.pyd"))
    for pyd_path in pyd_path_list:
        relative_path = pyd_path.relative_to(PRELIMINARY_PYD_ROOT).as_posix()
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(pyd_path.read_bytes())
        digest.update(b"\0")
    return len(pyd_path_list), digest.hexdigest()


def _assert_online_device_precedes_numeric_online_data(event_name_list: Sequence[str]) -> None:
    online_device_index = event_name_list.index("online_device")
    numeric_online_data_index = event_name_list.index("numeric_online_data")
    assert online_device_index < numeric_online_data_index, (
        "stream_role=online DevicePackage must precede numeric online DataPackage"
    )


def test_order_guard_rejects_numeric_online_data_before_online_device() -> None:
    with pytest.raises(
        AssertionError,
        match="stream_role=online DevicePackage must precede numeric online DataPackage",
    ):
        _assert_online_device_precedes_numeric_online_data(
            ["numeric_online_data", "online_device"]
        )


def test_final_collector_forces_online_device_before_numeric_online_data() -> None:
    module_node = ast.parse(VIRTUAL_RECEIVER_PATH.read_text(encoding="utf-8"))
    runtime_sender = _find_async_method(module_node, "__send_runtime_task_group")
    awaited_call_list = _awaited_method_call_list(runtime_sender)
    call_name_list = [_method_call_name(call) for call in awaited_call_list]

    release_index = call_name_list.index("__wait_until_online_stage_allowed")
    online_device_index = call_name_list.index("__ensure_runtime_device_info_sent")
    numeric_online_data_index = call_name_list.index("__send_trial_data")
    assert release_index < online_device_index
    _assert_online_device_precedes_numeric_online_data(
        [
            event_name
            for _, event_name in sorted(
                [
                    (online_device_index, "online_device"),
                    (numeric_online_data_index, "numeric_online_data"),
                ]
            )
        ]
    )
    _assert_force_online_device_call(awaited_call_list[online_device_index])

    trial_sender = _find_async_method(module_node, "__send_trial_data")
    _assert_trial_sender_builds_float32_data_transfer(trial_sender)


def test_deployed_preliminary_pyd_accepts_final_judge_online_transition() -> None:
    receiver_module_dir = (
        PRELIMINARY_PYD_APP_ROOT
        / "Algorithm"
        / "Algorithm"
        / "service"
        / "SourceReceiver"
    )
    if not list(receiver_module_dir.glob("ContinuousDataSourceReceiver*.pyd")):
        pytest.skip("deployed preliminary ContinuousDataSourceReceiver PYD is unavailable")

    python310 = _resolve_python310()
    if python310 is None:
        pytest.skip("Python 3.10 is unavailable for the deployed preliminary PYD")

    pyd_count_before, pyd_digest_before = _hash_preliminary_pyd_tree()

    probe_code = f'''
import asyncio
import hashlib
import importlib
import io
import json
import struct
import sys

import numpy as np

pyd_app_root = r"{PRELIMINARY_PYD_APP_ROOT}"
for import_root in reversed([
    pyd_app_root + r"\\Algorithm",
    pyd_app_root + r"\\Collector",
    pyd_app_root + r"\\ProcessHub",
]):
    sys.path.insert(0, import_root)

receiver_module = importlib.import_module(
    "Algorithm.service.SourceReceiver.ContinuousDataSourceReceiver"
)
algorithm_rpc_pb2 = importlib.import_module("Algorithm.api.proto.AlgorithmRPCService_pb2")
common_message_pb2 = importlib.import_module("Common.protobuf.CommonMessage_pb2")
from Algorithm.api.model.AlgorithmRPCServiceModel import AlgorithmDataMessageModel
from Algorithm.service.SourceReceiver.ContinuousDataSourceReceiver import ContinuousDataSourceReceiver
from Common.model.CommonMessageModel import DataPackageModel, DevicePackageModel
from componentframework.common.enum.DataTypeEnum import DataTypeEnum


def device_message(stream_role, session_id="session1"):
    return AlgorithmDataMessageModel(
        source_label="eeg_1",
        timestamp_ms=1,
        package=DevicePackageModel(
            data_type=DataTypeEnum.EEG,
            channel_number=2,
            sample_rate=1000.0,
            channel_label=["C3", "C4"],
            other_information={{
                "stream_role": stream_role,
                "subject_id": "sub_1",
                "exp_name": "vme",
                "exp_task": "left_vs_rest",
                "session_id": session_id,
            }},
        ),
    )


async def main():
    receiver = ContinuousDataSourceReceiver()
    receiver.set_source_label("eeg_1")
    receiver.set_required_channel_labels(["C3", "C4"])

    await receiver.set_message_model(device_message("calibration"))
    calibration_buffer = io.BytesIO()
    np.savez_compressed(
        calibration_buffer,
        subject_id=np.array("sub_1"),
        exp_name=np.array("vme"),
        exp_task=np.array("left_vs_rest"),
        session_id=np.array("session1"),
        data=np.empty((0, 2, 4000), dtype=np.float32),
        label=np.empty((0,), dtype=np.int64),
    )
    calibration_payload = calibration_buffer.getvalue()
    calibration_chunk = struct.pack(">4sIII", b"CAL1", 1, 0, len(calibration_payload)) + calibration_payload
    pending_calibration = asyncio.create_task(receiver.get_calibration())
    await asyncio.sleep(0)
    await receiver.set_message_model(
        AlgorithmDataMessageModel(
            source_label="eeg_1",
            timestamp_ms=1,
            package=DataPackageModel(data=calibration_chunk, data_position=0),
        )
    )
    calibration_object = await asyncio.wait_for(pending_calibration, timeout=1.0)
    await receiver.set_message_model(device_message("online"))

    pending_data = asyncio.create_task(receiver.get_data())
    await asyncio.sleep(0)
    await receiver.set_message_model(
        AlgorithmDataMessageModel(
            source_label="eeg_1",
            timestamp_ms=2,
            package=DataPackageModel(
                data=np.asarray([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
                data_position=100,
            ),
        )
    )
    data_object = await asyncio.wait_for(pending_data, timeout=1.0)

    return_to_calibration = asyncio.create_task(receiver.get_data())
    await asyncio.sleep(0)
    await receiver.set_message_model(device_message("calibration", session_id="session2"))
    transition_marker = await asyncio.wait_for(return_to_calibration, timeout=1.0)
    print(json.dumps({{
        "accepted": True,
        "module_file": receiver_module.__file__,
        "algorithm_rpc_proto_file": algorithm_rpc_pb2.__file__,
        "common_message_proto_file": common_message_pb2.__file__,
        "algorithm_rpc_descriptor_sha256": hashlib.sha256(
            algorithm_rpc_pb2.DESCRIPTOR.serialized_pb
        ).hexdigest(),
        "common_message_descriptor_sha256": hashlib.sha256(
            common_message_pb2.DESCRIPTOR.serialized_pb
        ).hexdigest(),
        "start_position": int(data_object.start_position),
        "stream_role": data_object.other_information.get("stream_role"),
        "zero_calibration_trial_count": int(
            calibration_object.session_data_dict["session1"]["data"].shape[0]
        ),
        "returned_to_calibration": ContinuousDataSourceReceiver.is_return_to_calibration_marker(
            transition_marker
        ),
        "next_session_id": transition_marker.other_information.get("session_id"),
    }}, ensure_ascii=True))


asyncio.run(main())
'''
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    completed = subprocess.run(
        [str(python310), "-I", "-c", probe_code],
        cwd=PRELIMINARY_PYD_ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, (
        f"old preliminary PYD rejected the judge transition:\n{completed.stdout}\n{completed.stderr}"
    )
    payload = _extract_json_payload(completed.stdout)
    assert payload["accepted"] is True
    assert Path(payload["module_file"]).suffix.lower() == ".pyd"
    assert Path(payload["module_file"]).resolve().is_relative_to(
        PRELIMINARY_PYD_APP_ROOT.resolve()
    )
    assert payload["start_position"] == 100
    assert payload["stream_role"] == "online"
    assert payload["zero_calibration_trial_count"] == 0
    assert payload["returned_to_calibration"] is True
    assert payload["next_session_id"] == "session2"
    assert payload["algorithm_rpc_descriptor_sha256"] == hashlib.sha256(
        final_algorithm_rpc_pb2.DESCRIPTOR.serialized_pb
    ).hexdigest()
    assert payload["common_message_descriptor_sha256"] == hashlib.sha256(
        final_common_message_pb2.DESCRIPTOR.serialized_pb
    ).hexdigest()
    for module_file_key in ("algorithm_rpc_proto_file", "common_message_proto_file"):
        module_path = Path(payload[module_file_key])
        assert module_path.suffix.lower() == ".pyd"
        assert module_path.resolve().is_relative_to(PRELIMINARY_PYD_APP_ROOT.resolve())
    pyd_count_after, pyd_digest_after = _hash_preliminary_pyd_tree()
    assert pyd_count_before == pyd_count_after == 548
    assert pyd_digest_before == pyd_digest_after
