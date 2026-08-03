from __future__ import annotations

import socket

import pytest

from tests.helpers import process_runner


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("test_infra")]


@pytest.mark.test_id("PROC-RUNNER-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("wait_for_port 只要 netstat 已确认端口处于 LISTENING，就不能因为 127.0.0.1 主动 connect 失败而误判未就绪")
@pytest.mark.tested(file="tests/helpers/process_runner.py", function="wait_for_port")
def test_wait_for_port_accepts_netstat_listening_without_successful_ipv4_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(process_runner, "_is_port_listening_by_netstat", lambda port: port == 29981)

    class _FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def settimeout(self, timeout: float) -> None:
            return None

        def connect(self, address) -> None:
            raise OSError("ipv4 connect refused")

    monkeypatch.setattr(socket, "socket", lambda *args, **kwargs: _FakeSocket())

    assert process_runner.wait_for_port("127.0.0.1", 29981, timeout=0.1) is True
