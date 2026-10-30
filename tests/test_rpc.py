"""Тесты host-стороны JSON-RPC (NDJSON) поверх TCP loopback.

В проде — unix socket; на Windows asyncio не умеет unix-сокеты, но фрейминг
и корреляция платформо-независимы.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import pytest
from lucrium_protocol.ipc import RpcErrorCode

from lucrium_worker.plugins.rpc import IpcError, PluginRpcError, RpcPeer

HOST = "127.0.0.1"


async def _start_server(handler: Any) -> tuple[asyncio.AbstractServer, int]:
    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                raw = await reader.readline()
                if not raw:
                    break
                message = json.loads(raw)
                response = await handler(message, writer)
                if response is not None:
                    writer.write((json.dumps(response) + "\n").encode())
                    await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    server = await asyncio.start_server(_handle, HOST, 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


async def _connect(port: int, handler: Any = None) -> RpcPeer:
    reader, writer = await asyncio.open_connection(HOST, port)
    return RpcPeer(reader, writer, on_request=handler)


async def test_request_response() -> None:
    async def handler(message: dict[str, Any], writer: asyncio.StreamWriter) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": message["id"], "result": {"echo": message["params"]}}

    server, port = await _start_server(handler)
    peer = await _connect(port)
    try:
        assert await peer.request("plugin.start", {"x": 1}, timeout=5) == {"echo": {"x": 1}}
    finally:
        await peer.close()
        server.close()
        await server.wait_closed()


async def test_remote_error_raises() -> None:
    async def handler(message: dict[str, Any], writer: asyncio.StreamWriter) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32001, "message": "forbidden"}}

    server, port = await _start_server(handler)
    peer = await _connect(port)
    try:
        with pytest.raises(PluginRpcError) as excinfo:
            await peer.request("x", timeout=5)
        assert excinfo.value.code == RpcErrorCode.FORBIDDEN
    finally:
        await peer.close()
        server.close()
        await server.wait_closed()


async def test_incoming_request_handled() -> None:
    """Host шлёт запрос плагину; плагин отвечает по тому же соединению."""
    seen: list[tuple[str, dict[str, Any]]] = []
    responses: list[dict[str, Any]] = []

    async def on_request(method: str, params: dict[str, Any]) -> Any:
        seen.append((method, params))
        return {"value": 42}

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b'{"jsonrpc":"2.0","id":1,"method":"storage.get","params":{"key":"k"}}\n')
        await writer.drain()
        responses.append(json.loads(await asyncio.wait_for(reader.readline(), timeout=5)))
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()

    server = await asyncio.start_server(_handle, HOST, 0)
    port = server.sockets[0].getsockname()[1]
    peer = await _connect(port, on_request)
    try:
        deadline = asyncio.get_running_loop().time() + 5
        while not responses:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert responses[0]["result"] == {"value": 42}
        assert seen == [("storage.get", {"key": "k"})]
    finally:
        await peer.close()
        server.close()
        await server.wait_closed()


async def test_notification_handled_without_response() -> None:
    seen: list[str] = []

    async def on_request(method: str, params: dict[str, Any]) -> Any:
        seen.append(method)
        return None

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b'{"jsonrpc":"2.0","method":"log.write","params":{"message":"hi"}}\n')
        await writer.drain()
        # уведомление не получает ответ: даём время обработчику и закрываем
        await asyncio.sleep(0.2)
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()

    server = await asyncio.start_server(_handle, HOST, 0)
    port = server.sockets[0].getsockname()[1]
    peer = await _connect(port, on_request)
    try:
        deadline = asyncio.get_running_loop().time() + 5
        while not seen:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert seen == ["log.write"]
    finally:
        await peer.close()
        server.close()
        await server.wait_closed()


async def test_close_fails_pending() -> None:
    async def handler(message: dict[str, Any], writer: asyncio.StreamWriter) -> None:
        return None

    server, port = await _start_server(handler)
    peer = await _connect(port)
    try:
        pending = asyncio.create_task(peer.request("slow", timeout=30))
        await asyncio.sleep(0.05)
        await peer.close()
        with pytest.raises(IpcError):
            await asyncio.wait_for(pending, timeout=5)
    finally:
        server.close()
        await server.wait_closed()
