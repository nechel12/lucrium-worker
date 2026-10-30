"""Тесты WSS-сервера панели: bearer-аутентификация, rate-limit, envelope-цикл."""

from __future__ import annotations

import socket
from dataclasses import replace
from typing import Any

import pytest
from lucrium_protocol import Command, Event, PingParams, Response, ServerEvent, build_request, parse_envelope
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from lucrium_worker.app import WorkerApp
from lucrium_worker.server import PanelServer, TokenAuth


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_token_auth_rate_limit() -> None:
    auth = TokenAuth(lambda: "secret-token", max_failures=3, base_delay=10.0)
    assert auth.check("1.1.1.1", "secret-token") is True
    for _ in range(3):
        assert auth.check("1.1.1.1", "wrong") is False
    # IP заблокирован даже с верным токеном
    assert auth.check("1.1.1.1", "secret-token") is False
    # другой IP не страдает
    assert auth.check("2.2.2.2", "secret-token") is True


async def test_unauthorized_rejected(app: WorkerApp) -> None:
    port = free_port()
    server = PanelServer(replace(app.config, port=port), app, app.logs)
    await server.start()
    try:
        with pytest.raises(InvalidStatus) as excinfo:
            async with connect(f"ws://127.0.0.1:{port}", additional_headers={"Authorization": "Bearer nope"}):
                pass
        assert excinfo.value.response.status_code == 401
    finally:
        await server.stop()


async def test_authorized_gets_state_and_ping(app: WorkerApp) -> None:
    port = free_port()
    server = PanelServer(replace(app.config, port=port), app, app.logs)
    await server.start()
    try:
        async with connect(
            f"ws://127.0.0.1:{port}",
            additional_headers={"Authorization": f"Bearer {app.token}"},
        ) as websocket:
            initial: Any = parse_envelope(await websocket.recv())
            assert isinstance(initial, Event)
            assert initial.payload.event == ServerEvent.STATE

            await websocket.send(build_request(Command.PING, PingParams(nonce="rtt")).model_dump_json())
            response = parse_envelope(await websocket.recv())
            assert isinstance(response, Response)
            assert response.payload.error is None
            assert response.payload.result is not None
            assert response.payload.result["nonce"] == "rtt"
    finally:
        await server.stop()
