"""Host-сторона JSON-RPC 2.0 (NDJSON) для IPC «воркер ↔ плагин».

Симметричен ``lucrium_plugin_sdk.transport.JsonRpcClient``: умеет и слать
запросы плагину (``plugin.start/event/stop``), и обслуживать запросы плагина
(``storage.*``, ``session.call``, ``log.write``, ``http.request``).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

from lucrium_protocol.ipc import RpcError, RpcErrorCode, RpcNotification, RpcRequest, RpcResponse

MAX_LINE_BYTES = 16 * 1024 * 1024
MAX_CONCURRENT_REQUESTS = 64
DEFAULT_TIMEOUT = 30.0

RequestHandler = Callable[[str, dict[str, Any]], Awaitable[Any]]


class IpcError(Exception):
    """Ошибка IPC-соединения с плагином."""


class PluginRpcError(IpcError):
    """Плагин ответил ``error`` на запрос воркера."""

    def __init__(self, error: RpcError) -> None:
        super().__init__(f"{error.code}: {error.message}")
        self.code = error.code
        self.message = error.message
        self.data = error.data


class RpcPeer:
    """Одно NDJSON-соединение с плагином."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        on_request: RequestHandler | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._on_request = on_request
        self._pending: dict[int | str, asyncio.Future[Any]] = {}
        self._ids = itertools.count(1)
        self._closed = False
        self._closing = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._read_task = asyncio.create_task(self._read_loop(), name="lucrium-worker-rpc-read")

    @property
    def closed(self) -> bool:
        return self._closed

    async def request(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float = DEFAULT_TIMEOUT
    ) -> Any:
        request_id = next(self._ids)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        await self._send(RpcRequest(id=request_id, method=method, params=params))
        try:
            return await asyncio.wait_for(future, timeout)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            future.cancel()
            raise IpcError(f"rpc timeout waiting for {method!r} after {timeout}s") from exc
        except Exception:
            self._pending.pop(request_id, None)
            raise

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        await self._send(RpcNotification(method=method, params=params))

    async def respond(
        self,
        request: RpcRequest,
        *,
        result: Any = None,
        error: RpcError | None = None,
    ) -> None:
        await self._send(RpcResponse(id=request.id, result=result, error=error))

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._closed = True
        self._read_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._read_task
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._writer.close()
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()
        self._fail_pending(IpcError("connection closed"))

    async def _send(self, message: RpcRequest | RpcNotification | RpcResponse) -> None:
        if self._closed:
            raise IpcError("connection closed")
        line = message.model_dump_json(exclude_none=True) + "\n"
        try:
            self._writer.write(line.encode())
            await self._writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            raise IpcError(f"cannot send to plugin: {exc}") from exc

    async def _read_loop(self) -> None:
        try:
            while True:
                raw = await self._reader.readline()
                if not raw:
                    break
                if len(raw) > MAX_LINE_BYTES:
                    raise IpcError("plugin message exceeds 16 MiB limit")
                self._dispatch(raw)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_pending(IpcError(f"malformed stream from plugin: {exc}"))
            self._closed = True
            return
        self._closed = True
        self._fail_pending(IpcError("plugin closed the connection"))

    def _dispatch(self, raw: bytes) -> None:
        try:
            data: Any = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise IpcError(f"invalid JSON from plugin: {exc}") from exc
        if not isinstance(data, dict):
            raise IpcError("plugin message must be a JSON object")

        if "method" in data:
            if len(self._tasks) >= MAX_CONCURRENT_REQUESTS:
                raise IpcError("too many concurrent requests from plugin")
            if "id" in data:
                request = RpcRequest.model_validate(data)
                self._spawn(self._handle_request(request), f"lucrium-rpc-{request.method}")
            else:
                notification = RpcNotification.model_validate(data)
                self._spawn(self._handle_notification(notification), f"lucrium-rpc-n-{notification.method}")
            return

        response = RpcResponse.model_validate(data)
        if response.id is None:
            raise IpcError("response without id")
        future = self._pending.pop(response.id, None)
        if future is None or future.done():
            return
        if response.error is not None:
            future.set_exception(PluginRpcError(response.error))
        else:
            future.set_result(response.result)

    async def _handle_request(self, request: RpcRequest) -> None:
        if self._on_request is None:
            await self.respond(
                request,
                error=RpcError(code=RpcErrorCode.METHOD_NOT_FOUND, message=f"unknown method {request.method!r}"),
            )
            return
        try:
            result = await self._on_request(request.method, request.params or {})
        except PluginRpcError as exc:
            await self.respond(request, error=RpcError(code=exc.code, message=exc.message, data=exc.data))
        except Exception as exc:
            await self.respond(
                request,
                error=RpcError(code=RpcErrorCode.INTERNAL_ERROR, message=f"{type(exc).__name__}: {exc}"),
            )
        else:
            await self.respond(request, result=result)

    async def _handle_notification(self, notification: RpcNotification) -> None:
        if self._on_request is None:
            return
        with contextlib.suppress(Exception):
            await self._on_request(notification.method, notification.params or {})

    def _fail_pending(self, exc: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()

    def _spawn(self, coroutine: Coroutine[Any, Any, None], name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


__all__ = [
    "DEFAULT_TIMEOUT",
    "MAX_CONCURRENT_REQUESTS",
    "MAX_LINE_BYTES",
    "IpcError",
    "PluginRpcError",
    "RequestHandler",
    "RpcPeer",
]
