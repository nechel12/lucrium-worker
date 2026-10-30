"""WSS-сервер для панели: bearer-аутентификация, rate-limit, envelope-цикл."""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import ssl
import time
from collections.abc import Callable
from http import HTTPStatus
from typing import Any

from lucrium_protocol import (
    ErrorCode,
    Event,
    LogLevel,
    ProtocolError,
    Request,
    Response,
    ServerEvent,
    build_error_response,
    build_event,
    parse_envelope,
)
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed

from .app import WorkerApp
from .config import WorkerConfig
from .logs import LogBuffer

MAX_MESSAGE_BYTES = 64 * 1024 * 1024


class TokenAuth:
    """Bearer-токен + backoff на IP после неудачных попыток (спека §3.1)."""

    def __init__(
        self,
        token_getter: Callable[[], str],
        *,
        max_failures: int = 5,
        base_delay: float = 2.0,
        max_delay: float = 300.0,
    ) -> None:
        self._token_getter = token_getter
        self._max_failures = max_failures
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._failures: dict[str, int] = {}
        self._blocked_until: dict[str, float] = {}

    def check(self, ip: str, token: str | None) -> bool:
        now = time.monotonic()
        until = self._blocked_until.get(ip)
        if until is not None and until > now:
            return False
        expected = self._token_getter()
        if token is not None and hmac.compare_digest(token, expected):
            self._failures.pop(ip, None)
            self._blocked_until.pop(ip, None)
            return True
        failures = self._failures.get(ip, 0) + 1
        self._failures[ip] = failures
        if failures >= self._max_failures:
            delay = min(self._base_delay * (2 ** (failures - self._max_failures)), self._max_delay)
            self._blocked_until[ip] = now + delay
        return False


class PanelServer:
    def __init__(self, config: WorkerConfig, app: WorkerApp, logs: LogBuffer) -> None:
        self._config = config
        self._app = app
        self._logs = logs
        self._clients: set[ServerConnection] = set()
        self._server: Any = None
        self._auth = TokenAuth(lambda: self._app.token)

    async def start(self) -> None:
        ssl_context = _ssl_context(self._config)
        self._server = await serve(
            self._handler,
            self._config.host,
            self._config.port,
            ssl=ssl_context,
            process_request=self._process_request,
            max_size=MAX_MESSAGE_BYTES,
            ping_interval=20,
            ping_timeout=20,
        )
        self._app.set_broadcaster(self.broadcast)
        self._logs.add(
            LogLevel.INFO,
            "worker",
            f"panel server listening on {self._config.host}:{self._config.port}",
            {"tls": ssl_context is not None},
        )

    async def stop(self) -> None:
        for client in list(self._clients):
            with contextlib.suppress(Exception):
                await client.close(code=1001, reason="worker shutting down")
        self._clients.clear()
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None

    async def broadcast(self, event: Event) -> None:
        if not self._clients:
            return
        payload = event.model_dump_json()
        results = await asyncio.gather(
            *(client.send(payload) for client in list(self._clients)),
            return_exceptions=True,
        )
        for client, result in zip(list(self._clients), results, strict=False):
            if isinstance(result, BaseException):
                self._clients.discard(client)

    # --- websockets -----------------------------------------------------------

    def _process_request(self, connection: ServerConnection, request: Any) -> Any:
        ip = connection.remote_address[0] if connection.remote_address else "unknown"
        token = _bearer(request.headers)
        if not self._auth.check(ip, token):
            return connection.respond(HTTPStatus.UNAUTHORIZED, "unauthorized\n")
        return None

    async def _handler(self, connection: ServerConnection) -> None:
        self._clients.add(connection)
        self._app.connected_panels = len(self._clients)
        try:
            snapshot = await self._app.state_snapshot()
            await connection.send(build_event(ServerEvent.STATE, snapshot).model_dump_json())
            async for raw in connection:
                await self._on_message(connection, raw)
        except ConnectionClosed:
            pass
        finally:
            self._clients.discard(connection)
            self._app.connected_panels = len(self._clients)

    async def _on_message(self, connection: ServerConnection, raw: str | bytes) -> None:
        try:
            envelope = parse_envelope(raw)
        except ProtocolError as exc:
            await connection.send(_error_response("0", exc).model_dump_json())
            return
        if not isinstance(envelope, Request):
            await connection.send(
                _error_response(
                    envelope.id, ProtocolError(ErrorCode.INVALID_REQUEST, "expected request")
                ).model_dump_json()
            )
            return
        response = await self._app.handle_request(envelope)
        await connection.send(response.model_dump_json())


def _bearer(headers: Headers) -> str | None:
    value = headers.get("Authorization")
    if not value:
        return None
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


def _error_response(request_id: str, error: ProtocolError) -> Response:
    return build_error_response(request_id or "0", error)


def _ssl_context(config: WorkerConfig) -> ssl.SSLContext | None:
    if config.tls is None:
        return None
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=config.tls.certfile, keyfile=config.tls.keyfile)
    return context


__all__ = ["MAX_MESSAGE_BYTES", "PanelServer", "TokenAuth"]
