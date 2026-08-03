from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MESSAGE_MANAGER_FILE = (
    PROJECT_ROOT
    / "app"
    / "ProcessHub"
    / "componentframework"
    / "facadeImpl"
    / "MessageManagerGrpcFacadeImpl.py"
)


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("message_payload_security")]


def _read_message_manager_file() -> str:
    return MESSAGE_MANAGER_FILE.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("SEC-MSG-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("消息调试摘要必须记录 payload_size 和截断预览，避免超大输出直接刷爆日志")
@pytest.mark.tested(
    file="app/ProcessHub/componentframework/facadeImpl/MessageManagerGrpcFacadeImpl.py",
    function="__build_request_debug_summary",
)
def test_message_manager_debug_summary_records_payload_size_and_truncated_preview() -> None:
    content = _read_message_manager_file()

    assert "payload_size = len(request.value or b\"\")" in content
    assert "payload_preview = (request.value or b\"\")[:180].decode(\"utf-8\", errors=\"ignore\")" in content
    assert "payload_preview = payload_preview.replace(\"\\n\", \"\\\\n\").replace(\"\\r\", \"\\\\r\")" in content
    assert 'f"message_key={request.messageKey} payload_size={payload_size} "' in content


@pytest.mark.test_id("SEC-MSG-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("关键控制消息必须走单消息单 channel 的 single_shot_control 路径，避免 timeout 终态被共享 stream 状态污染")
@pytest.mark.tested(
    file="app/ProcessHub/componentframework/facadeImpl/MessageManagerGrpcFacadeImpl.py",
    function="__send_message_via_single_shot_control_rpc",
)
def test_message_manager_uses_single_shot_control_transport_for_sensitive_control_messages() -> None:
    content = _read_message_manager_file()

    assert "rpc_channel = self.__create_single_shot_control_channel()" in content
    assert "rpc_stub = MessageManager_pb2_grpc.MessageManagerServiceStub(rpc_channel)" in content
    assert "rpc_call = rpc_stub.SendMessage()" in content
    assert "send_mode=single_shot_control transport=dedicated_channel" in content
    assert "self.__start_single_shot_control_response_watch(" in content


@pytest.mark.test_id("SEC-MSG-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("单次控制消息响应超时后必须取消调用并关闭独立 channel，避免半开连接长期残留")
@pytest.mark.tested(
    file="app/ProcessHub/componentframework/facadeImpl/MessageManagerGrpcFacadeImpl.py",
    function="__wait_single_shot_control_response/__close_single_shot_control_channel",
)
def test_message_manager_closes_single_shot_channel_after_timeout_or_rpc_error() -> None:
    content = _read_message_manager_file()

    assert "rpc_call.cancel()" in content
    assert "response_timeout_seconds" in content
    assert "await self.__close_single_shot_control_channel(" in content
    assert 'close_reason="response_watch_finished"' in content
