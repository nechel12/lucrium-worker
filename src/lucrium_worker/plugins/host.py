"""Host плагинов: процесс на плагин, IPC, таймауты, авто-restart (спека §6.3)."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import inspect
import ipaddress
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from http.client import HTTPMessage
from pathlib import Path
from typing import IO, Any
from urllib.parse import urlsplit

from lucrium_plugin_sdk import PLUGIN_DIR_ENV, PLUGIN_SOCKET_ENV, load_manifest
from lucrium_protocol import (
    HubEvent,
    LogLevel,
    Platform,
    PluginError,
    PluginManifest,
    PluginStatus,
    redact_exception_text,
)
from lucrium_protocol.ipc import (
    HTTP_REQUEST,
    LOG_WRITE,
    PLUGIN_EVENT,
    PLUGIN_START,
    PLUGIN_STOP,
    SESSION_CALL,
    STORAGE_DELETE,
    STORAGE_GET,
    STORAGE_LIST,
    STORAGE_SET,
    EventParams,
    HttpRequestParams,
    HttpResponseResult,
    LogWriteParams,
    RpcError,
    RpcErrorCode,
    SessionCallParams,
    StartParams,
    StopParams,
    StorageDeleteParams,
    StorageGetParams,
    StorageListParams,
    StorageSetParams,
)
from lucrium_protocol.manifest import ManifestPermissions
from pydantic import ValidationError

from ..logs import LogBuffer
from ..sessions import SessionError, SessionManager
from ..storage import PluginRecord, Storage
from .installer import validate_plugin_id
from .rpc import MAX_LINE_BYTES, IpcError, PluginRpcError, RpcPeer

START_TIMEOUT = 30.0
STOP_TIMEOUT = 15.0
EVENT_TIMEOUT = 30.0
SESSION_CALL_TIMEOUT = 60.0
MAX_HTTP_BODY = 8 * 1024 * 1024
MAX_KV_VALUE_BYTES = 1024 * 1024
EVENT_QUEUE_SIZE = 1000
BACKOFF_START = 1.0
BACKOFF_MAX = 60.0
HEALTHY_AFTER = 300.0

ErrorCallback = Callable[[PluginError], None]
ChangeCallback = Callable[[], None]


def permission_changes(stored: ManifestPermissions, actual: ManifestPermissions) -> list[str]:
    """Чем манифест на диске отличается от зафиксированного при установке."""
    changes: list[str] = []
    installed_accounts = sorted(platform.value for platform in stored.accounts) or ["all"]
    actual_accounts = sorted(platform.value for platform in actual.accounts) or ["all"]
    if installed_accounts != actual_accounts:
        changes.append(f"accounts: installed {installed_accounts}, files declare {actual_accounts}")
    if actual.network != stored.network:
        changes.append(f"network: installed {stored.network}, files declare {actual.network}")
    if actual.finance != stored.finance:
        changes.append(f"finance: installed {stored.finance}, files declare {actual.finance}")
    return changes


class PluginHost:
    """Управляет процессами плагинов и проксирует их запросы к воркеру."""

    def __init__(
        self,
        *,
        plugins_dir: Path,
        sockets_dir: Path,
        sessions: SessionManager,
        storage: Storage,
        logs: LogBuffer,
        memory_limit_bytes: int = 512 * 1024 * 1024,
        on_error: ErrorCallback | None = None,
        on_change: ChangeCallback | None = None,
        sandbox: bool = True,
        sandbox_required: bool = True,
        data_dir: Path | None = None,
        config_dir: Path | None = None,
    ) -> None:
        self._plugins_dir = plugins_dir
        self._sockets_dir = sockets_dir
        self._sessions = sessions
        self._storage = storage
        self._logs = logs
        self.memory_limit_bytes = memory_limit_bytes
        self._on_error = on_error
        self._on_change = on_change
        self._data_dir = data_dir or plugins_dir.parent
        self._config_dir = config_dir
        self._processes: dict[str, _PluginProcess] = {}
        self._restart_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_errors: dict[str, str] = {}
        # хост, явно отключивший песочницу, не может её требовать
        self.sandbox_required = sandbox_required and sandbox
        self.sandbox_available = sandbox and _sandbox_available()
        if sandbox and not self.sandbox_available:
            if sandbox_required:
                self._logs.add(
                    LogLevel.ERROR,
                    "worker",
                    "plugin sandbox is required but bubblewrap/userns is unavailable; plugins will not start",
                )
            else:
                self._logs.add(
                    LogLevel.WARNING,
                    "worker",
                    "plugin sandbox requested but bubblewrap/userns unavailable; plugins run without OS isolation",
                )

    # --- запросы --------------------------------------------------------------

    def status(self, plugin_id: str) -> PluginStatus:
        process = self._processes.get(plugin_id)
        return PluginStatus.STOPPED if process is None else process.status

    def restarts(self, plugin_id: str) -> int:
        process = self._processes.get(plugin_id)
        return 0 if process is None else process.restarts

    def last_error(self, plugin_id: str) -> str | None:
        process = self._processes.get(plugin_id)
        if process is not None and process.last_error:
            return process.last_error[:2000]
        return self._last_errors.get(plugin_id, "")[:2000] or None

    # --- управление -----------------------------------------------------------

    async def start(self, record: PluginRecord) -> None:
        if record.id in self._processes:
            return
        if self.sandbox_required and not self.sandbox_available:
            message = "plugin sandbox is required but bubblewrap/userns is unavailable"
            self._last_errors[record.id] = message
            raise RuntimeError(message)
        try:
            validate_plugin_id(record.id)
        except Exception as exc:
            self._last_errors[record.id] = f"{type(exc).__name__}: {exc}"[:2000]
            raise
        directory = self._plugins_dir / record.id
        try:
            manifest = load_manifest(directory)
        except Exception as exc:
            self._last_errors[record.id] = f"{type(exc).__name__}: {exc}"[:2000]
            raise
        if manifest.id != record.id:
            message = f"manifest declares id {manifest.id!r}, installed as {record.id!r}"
            self._last_errors[record.id] = message
            raise RuntimeError(message)
        if record.permissions is None:
            # запись оставлена старой версией воркера: фиксируем то, что на диске
            record = replace(record, permissions=manifest.permissions)
            await self._storage.upsert_plugin(record)
        else:
            changes = permission_changes(record.permissions, manifest.permissions)
            if changes:
                message = "plugin files changed permissions without reinstall: " + "; ".join(changes)
                self._last_errors[record.id] = message
                raise RuntimeError(message)
        process = _PluginProcess(
            record=record,
            manifest=manifest,
            directory=directory,
            socket_path=self._sockets_dir / record.id / "plugin.sock",
            host=self,
        )
        self._processes[record.id] = process
        try:
            await process.start()
        except Exception as exc:
            error = process.last_error or f"{type(exc).__name__}: {exc}"
            self._last_errors[record.id] = error[:2000]
            process.last_error = error[:2000]
            process.status = PluginStatus.CRASHED
            # Не теряем процесс: on_start может упасть из-за временной причины
            # или конфига, который пользователь сейчас исправит. Ретраим с backoff.
            self._schedule_restart(process, error)
            self._notify_change()
            raise
        self._last_errors.pop(record.id, None)
        self._notify_change()

    async def stop(self, plugin_id: str, *, reason: str = "panel request") -> None:
        restart = self._restart_tasks.pop(plugin_id, None)
        if restart is not None:
            restart.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await restart
        process = self._processes.pop(plugin_id, None)
        if process is not None:
            await process.stop(reason=reason)
        self._last_errors.pop(plugin_id, None)
        self._notify_change()

    async def stop_all(self) -> None:
        await asyncio.gather(*(self.stop(plugin_id) for plugin_id in list(self._processes)), return_exceptions=True)

    async def restart(self, record: PluginRecord) -> None:
        await self.stop(record.id, reason="restart")
        await self.start(record)

    async def dispatch(self, event: HubEvent) -> None:
        for process in list(self._processes.values()):
            if process.status is PluginStatus.RUNNING and process.allows(event.platform):
                process.enqueue(event)

    # --- внутреннее -----------------------------------------------------------

    def _notify_change(self) -> None:
        if self._on_change is not None:
            self._on_change()

    def _schedule_restart(self, process: _PluginProcess, error: str) -> None:
        plugin_id = process.record.id
        if plugin_id in self._restart_tasks or process.stopping:
            return
        process.last_error = error[:2000]
        if process.running_since is not None and time.monotonic() - process.running_since >= HEALTHY_AFTER:
            # плагин отработал пять минут стабильно: новая серия сбоев начинается сначала
            process.restarts = 0
            process.running_since = None
        process.restarts += 1
        delay = min(BACKOFF_START * (2 ** (process.restarts - 1)), BACKOFF_MAX)
        if self._on_error is not None:
            self._on_error(
                PluginError(
                    plugin_id=plugin_id,
                    ts=datetime.now(UTC),
                    error=error[:2000],
                    will_restart=True,
                    restart_in_seconds=delay,
                )
            )
        self._logs.add(
            LogLevel.ERROR,
            plugin_id,
            f"plugin crashed: {error}; restart in {delay:.0f}s",
            {"restarts": process.restarts},
        )

        async def _restart() -> None:
            await asyncio.sleep(delay)
            self._restart_tasks.pop(plugin_id, None)
            if process.stopping or self._processes.get(plugin_id) is not process:
                return
            await process.cleanup()
            try:
                await process.start()
            except Exception as exc:
                self._schedule_restart(process, f"{type(exc).__name__}: {exc}")
            self._notify_change()

        self._restart_tasks[plugin_id] = asyncio.create_task(_restart(), name=f"plugin-restart-{plugin_id}")
        self._notify_change()


class _PluginProcess:
    """Один плагин: unix-socket сервер, subprocess, очередь событий."""

    def __init__(
        self,
        *,
        record: PluginRecord,
        manifest: PluginManifest,
        directory: Path,
        socket_path: Path,
        host: PluginHost,
    ) -> None:
        self.record = record
        self.manifest = manifest
        self.directory = directory
        self.socket_path = socket_path
        self.host = host
        self.status = PluginStatus.STOPPED
        self.restarts = 0
        self.running_since: float | None = None
        self.last_error: str | None = None
        self.stopping = False
        self._process: asyncio.subprocess.Process | None = None
        self._server: asyncio.AbstractServer | None = None
        self._peer: RpcPeer | None = None
        self._connected: asyncio.Future[tuple[asyncio.StreamReader, asyncio.StreamWriter]] | None = None
        self._queue: asyncio.Queue[HubEvent] = asyncio.Queue(maxsize=EVENT_QUEUE_SIZE)
        self._tasks: list[asyncio.Task[None]] = []

    # --- права ----------------------------------------------------------------

    def allows(self, platform: Platform) -> bool:
        allowed = self.manifest.permissions.accounts
        return not allowed or platform in allowed

    # --- запуск/остановка -----------------------------------------------------

    async def start(self) -> None:
        self.stopping = False
        self.status = PluginStatus.STARTING
        self.last_error = None
        # После краха/рестарта в очереди могут остаться события прошлого
        # процесса: не отдаём их новому экземпляру плагина.
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            self.socket_path.parent.chmod(0o700)
        with contextlib.suppress(FileNotFoundError):
            self.socket_path.unlink()
        self._connected = asyncio.get_running_loop().create_future()
        self._server = await asyncio.start_unix_server(
            self._on_connection,
            path=str(self.socket_path),
            limit=MAX_LINE_BYTES,
        )
        with contextlib.suppress(OSError):
            self.socket_path.chmod(0o600)
        env = _plugin_env(self.directory, self.socket_path)
        subprocess_kwargs: dict[str, Any] = {}
        if sys.platform != "win32" and not _supports_prlimit():
            # Fallback для не-Linux POSIX: RLIMIT_AS до exec.
            subprocess_kwargs["preexec_fn"] = _memory_limiter(self.host.memory_limit_bytes)
        command = (
            _sandbox_command(
                python=sys.executable,
                plugin_dir=self.directory,
                sockets_dir=self.socket_path.parent,
                data_dir=self.host._data_dir,
                config_dir=self.host._config_dir,
            )
            if self.host.sandbox_available
            else [sys.executable, "-m", "lucrium_plugin_sdk"]
        )
        try:
            self._process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.directory),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **subprocess_kwargs,
            )
            if sys.platform != "win32" and _supports_prlimit():
                _apply_memory_limit(self._process.pid, self.host.memory_limit_bytes)
            self._tasks = [
                asyncio.create_task(self._pump_output(self._process.stdout, "stdout"), name="plugin-stdout"),
                asyncio.create_task(self._pump_output(self._process.stderr, "stderr"), name="plugin-stderr"),
                asyncio.create_task(self._watch_exit(), name="plugin-exit"),
            ]
            reader, writer = await asyncio.wait_for(self._connected, timeout=START_TIMEOUT)
            self._peer = RpcPeer(reader, writer, on_request=self._handle_request)
            await self._peer.request(
                PLUGIN_START,
                StartParams(plugin_id=self.record.id, config=self.record.config).model_dump(mode="json"),
                timeout=START_TIMEOUT,
            )
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"[:2000]
            await self.cleanup()
            raise RuntimeError(self.last_error) from exc
        self.status = PluginStatus.RUNNING
        self.running_since = time.monotonic()
        self._tasks.append(asyncio.create_task(self._event_worker(), name=f"plugin-events-{self.record.id}"))
        self.host._logs.add(LogLevel.INFO, self.record.id, "plugin started", {"version": self.record.version})

    async def stop(self, *, reason: str = "panel request") -> None:
        self.stopping = True
        if self._peer is not None:
            with contextlib.suppress(IpcError, PluginRpcError):
                await self._peer.request(
                    PLUGIN_STOP,
                    StopParams(reason=reason).model_dump(mode="json"),
                    timeout=STOP_TIMEOUT,
                )
        await self.cleanup()
        self.status = PluginStatus.STOPPED

    async def cleanup(self) -> None:
        """Закрывает IPC/процесс/сокет; не меняет ``stopping`` (нужно для restart)."""
        self.status = PluginStatus.STOPPING
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._peer is not None:
            with contextlib.suppress(Exception):
                await self._peer.close()
            self._peer = None
        process = self._process
        self._process = None
        if process is not None:
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        process.kill()
                    with contextlib.suppress(Exception):
                        await process.wait()
            # communicate() drains and closes stdout/stderr pipes; process.wait()
            # alone leaves the transports open on Python 3.14.
            with contextlib.suppress(Exception):
                await process.communicate()
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None
        with contextlib.suppress(FileNotFoundError):
            self.socket_path.unlink()
        with contextlib.suppress(OSError):
            self.socket_path.parent.rmdir()
        self._connected = None

    # --- события --------------------------------------------------------------

    def enqueue(self, event: HubEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.host._logs.add(
                LogLevel.WARNING,
                self.record.id,
                "event queue is full; dropping event",
                {"kind": event.kind.value, "session_id": event.session_id},
            )

    async def _event_worker(self) -> None:
        while True:
            event = await self._queue.get()
            peer = self._peer
            if peer is None or self.status is not PluginStatus.RUNNING:
                continue
            try:
                await peer.request(
                    PLUGIN_EVENT,
                    EventParams(event=event).model_dump(mode="json"),
                    timeout=EVENT_TIMEOUT,
                )
            except PluginRpcError as exc:
                self.host._logs.add(
                    LogLevel.ERROR,
                    self.record.id,
                    f"plugin failed to handle event: {exc.message}",
                    {"kind": event.kind.value, "session_id": event.session_id, "code": exc.code},
                )
            except IpcError as exc:
                message = f"plugin IPC error: {exc}"[:2000]
                self.host._logs.add(LogLevel.ERROR, self.record.id, message)
                if not self.stopping:
                    self.status = PluginStatus.CRASHED
                    self.last_error = message
                    self.host._schedule_restart(self, message)
                return

    # --- IPC ------------------------------------------------------------------

    async def _on_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._connected is None or self._connected.done():
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            return
        # Первое подключившееся соединение не может быть принято без проверки:
        # плагин — чужой код, а сокет доступен любому процессу того же uid.
        if not self._peer_is_plugin_process(writer):
            self.host._logs.add(
                LogLevel.WARNING, self.record.id, "rejected a connection to the plugin socket from another process"
            )
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            return
        self._connected.set_result((reader, writer))

    def _peer_is_plugin_process(self, writer: asyncio.StreamWriter) -> bool:
        process = self._process
        if process is None or sys.platform not in {"linux", "linux2"}:
            return True
        credentials = _peer_credentials(writer)
        if credentials is None:
            return False
        pid, uid = credentials
        if uid != os.getuid():
            return False
        # без песочницы точка входа — наш прямой ребёнок; под bwrap она на
        # уровень глубже, поэтому поднимаемся по цепочке предков
        current: int | None = pid
        for _ in range(4):
            if current is None:
                return False
            if current == process.pid:
                return True
            current = _parent_pid(current)
        return False

    async def _handle_request(self, method: str, params: dict[str, Any]) -> Any:
        try:
            return await self._dispatch_request(method, params)
        except ValidationError as exc:
            raise PluginRpcError(_rpc_error(RpcErrorCode.INVALID_PARAMS, redact_exception_text(exc))) from exc

    async def _dispatch_request(self, method: str, params: dict[str, Any]) -> Any:
        storage = self.host._storage
        plugin_id = self.record.id
        if method == STORAGE_GET:
            parsed_get = StorageGetParams.model_validate(params)
            return {"value": await storage.kv_get(plugin_id, parsed_get.key)}
        if method == STORAGE_SET:
            parsed_set = StorageSetParams.model_validate(params)
            if len(json.dumps(parsed_set.value, ensure_ascii=False).encode("utf-8")) > MAX_KV_VALUE_BYTES:
                raise PluginRpcError(
                    _rpc_error(RpcErrorCode.INVALID_PARAMS, f"KV value exceeds {MAX_KV_VALUE_BYTES} bytes")
                )
            await storage.kv_set(plugin_id, parsed_set.key, parsed_set.value)
            return {}
        if method == STORAGE_DELETE:
            parsed_delete = StorageDeleteParams.model_validate(params)
            await storage.kv_delete(plugin_id, parsed_delete.key)
            return {}
        if method == STORAGE_LIST:
            parsed_list = StorageListParams.model_validate(params)
            return {"keys": await storage.kv_keys(plugin_id, parsed_list.prefix)}
        if method == LOG_WRITE:
            parsed = LogWriteParams.model_validate(params)
            self.host._logs.add(parsed.level, plugin_id, parsed.message, parsed.fields)
            return {}
        if method == SESSION_CALL:
            return await self._session_call(SessionCallParams.model_validate(params))
        if method == HTTP_REQUEST:
            return await self._http_request(HttpRequestParams.model_validate(params))
        raise PluginRpcError(_rpc_error(RpcErrorCode.METHOD_NOT_FOUND, f"unknown method {method!r}"))

    async def _session_call(self, call: SessionCallParams) -> dict[str, Any]:
        try:
            runtime = self.host._sessions.resolve(call.session_id)
        except SessionError as exc:
            raise PluginRpcError(_rpc_error(RpcErrorCode.INVALID_PARAMS, str(exc))) from exc
        if not self.allows(runtime.platform):
            raise PluginRpcError(
                _rpc_error(RpcErrorCode.FORBIDDEN, f"plugin has no access to platform {runtime.platform.value}")
            )
        try:
            data = await asyncio.wait_for(
                self.host._sessions.call(
                    runtime.account_id,
                    call.method,
                    call.params,
                    allow_finance=self.manifest.permissions.finance,
                ),
                timeout=SESSION_CALL_TIMEOUT,
            )
        except TimeoutError as exc:
            raise PluginRpcError(_rpc_error(RpcErrorCode.TIMEOUT, f"session.call timed out: {call.method}")) from exc
        except (SessionError, AttributeError, TypeError) as exc:
            raise PluginRpcError(_rpc_error(RpcErrorCode.INVALID_PARAMS, str(exc))) from exc
        return {"data": data}

    async def _http_request(self, params: HttpRequestParams) -> dict[str, Any]:
        if not self.manifest.permissions.network:
            raise PluginRpcError(_rpc_error(RpcErrorCode.FORBIDDEN, "network permission is not granted"))
        result = await asyncio.to_thread(_http_request_sync, params)
        payload: dict[str, Any] = dict(result.model_dump(mode="json"))
        return payload

    # --- диагностика ----------------------------------------------------------

    async def _pump_output(self, stream: asyncio.StreamReader | None, name: str) -> None:
        if stream is None:
            return
        while True:
            try:
                raw = await stream.readline()
            except (ValueError, OSError):
                return
            if not raw:
                return
            text = raw.decode("utf-8", errors="replace").rstrip()
            if text:
                self.host._logs.add(LogLevel.DEBUG, self.record.id, f"[{name}] {text}")

    async def _watch_exit(self) -> None:
        process = self._process
        if process is None:
            return
        code = await process.wait()
        if self.stopping:
            return
        self.status = PluginStatus.CRASHED
        self.last_error = f"plugin exited with code {code}"
        self.host._schedule_restart(self, self.last_error)


def _sandbox_command(
    *,
    python: str,
    plugin_dir: Path,
    sockets_dir: Path,
    data_dir: Path,
    config_dir: Path | None,
) -> list[str]:
    """bubblewrap: скрыть data/config, оставить только код плагина и сокет.

    Сеть песочнице не выдаётся вовсе (``--unshare-all`` без ``--share-net``):
    ``permissions.network`` означает доступ к ``ctx.http`, который делает воркер
    со своей egress-политикой. Иначе флаг превращался бы в обход всей модели
    разрешений — raw-сокеты, listening-порты, внутренний сегмент VPS.
    """
    command = [
        "bwrap",
        "--unshare-all",
        "--die-with-parent",
        "--new-session",
        "--cap-drop",
        "ALL",
        "--ro-bind",
        "/",
        "/",
        "--dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
    ]
    if not data_dir.is_dir():
        raise RuntimeError(f"plugin sandbox: data directory {data_dir} does not exist")
    command += [
        "--tmpfs",
        str(data_dir),
        "--dir",
        str(plugin_dir.parent),
        "--dir",
        str(plugin_dir),
        "--dir",
        str(sockets_dir),
        "--ro-bind",
        str(plugin_dir),
        str(plugin_dir),
        "--bind",
        str(sockets_dir),
        str(sockets_dir),
    ]
    if config_dir is not None:
        if not config_dir.is_dir():
            raise RuntimeError(f"plugin sandbox: config directory {config_dir} does not exist")
        command += ["--tmpfs", str(config_dir)]
    command += [
        "--chdir",
        str(plugin_dir),
        "--",
        python,
        "-m",
        "lucrium_plugin_sdk",
    ]
    return command


def _sandbox_available() -> bool:
    if sys.platform != "linux" or shutil.which("bwrap") is None:
        return False
    try:
        probe = subprocess.run(
            [
                "bwrap",
                "--unshare-all",
                "--die-with-parent",
                "--cap-drop",
                "ALL",
                "--ro-bind",
                "/",
                "/",
                "--dev",
                "/dev",
                "--proc",
                "/proc",
                "--",
                "true",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return probe.returncode == 0


def _plugin_env(directory: Path, socket_path: Path) -> dict[str, str]:
    """Минимальное окружение: не отдаём плагину токены/секреты воркера."""
    allowed_exact = {
        "PATH",
        "HOME",
        "LANG",
        "LANGUAGE",
        "TZ",
        "PYTHONUNBUFFERED",
        "PYTHONIOENCODING",
        "PYTHONPATH",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "ALL_PROXY",
    }
    env = {key: value for key, value in os.environ.items() if key in allowed_exact or key.startswith(("LC_", "PYTHON"))}
    env[PLUGIN_DIR_ENV] = str(directory)
    env[PLUGIN_SOCKET_ENV] = str(socket_path)
    return env


def _rpc_error(code: int, message: str) -> RpcError:
    return RpcError(code=code, message=message)


def _peer_credentials(writer: asyncio.StreamWriter) -> tuple[int, int] | None:
    """``(pid, uid)`` процесса на другом конце unix-сокета."""
    sock = writer.get_extra_info("socket")
    if sock is None:
        return None
    try:
        raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.pack("3i", 0, 0, 0))
    except OSError:
        return None
    pid, uid, _gid = struct.unpack("3i", raw)
    return pid, uid


def _parent_pid(pid: int) -> int | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return None
    # имя в скобках может содержать пробелы — поля считаем после последней скобки
    fields = stat.rsplit(")", 1)[-1].split()
    return int(fields[1]) if len(fields) > 1 else None


def _supports_prlimit() -> bool:
    if sys.platform == "win32":
        return False
    try:
        import resource
    except ImportError:
        return False
    return hasattr(resource, "prlimit")


def _apply_memory_limit(pid: int, limit_bytes: int) -> None:
    """Ставит RLIMIT_AS уже запущенному процессу, без preexec_fn (thread-safe)."""
    try:
        import resource

        resource.prlimit(pid, resource.RLIMIT_AS, (limit_bytes, limit_bytes))
    except (ImportError, OSError, ValueError):
        return


def _memory_limiter(limit_bytes: int) -> Callable[[], None]:
    """Fallback ``preexec_fn`` для POSIX без ``resource.prlimit``."""

    def _limit() -> None:
        import resource

        resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))

    return _limit


def _assert_public_url(url: str) -> None:
    """SSRF-гейт: запрос делает воркер, поэтому внутренность VPS недоступна плагину.

    Остаточный риск: между проверкой и соединением DNS может ответить другим
    адресом (rebinding). Полностью это закрывает только отдельный netns/прокси
    с фиксированным IP — в v1 осознанно принято как ограничение.
    """
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("url must be http(s)")
    host = parts.hostname
    if not host:
        raise ValueError("url has no host")
    try:
        addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = {ipaddress.ip_address(host)}
    except ValueError:
        port = parts.port or (443 if scheme == "https" else 80)
        try:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise ValueError(f"cannot resolve {host}: {exc}") from exc
        addresses = {ipaddress.ip_address(info[4][0]) for info in infos}
    for address in addresses:
        mapped = getattr(address, "ipv4_mapped", None)
        if mapped is not None:
            address = mapped
        if not address.is_global:
            raise ValueError(f"{host} resolves to {address}, which is not a public address")


class _ValidatedRedirects(urllib.request.HTTPRedirectHandler):
    """Ревалидирует адрес на каждой ступени редиректа, а не только стартовый URL."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        _assert_public_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _http_request_sync(params: HttpRequestParams) -> HttpResponseResult:
    try:
        body = base64.b64decode(params.body_b64, validate=True) if params.body_b64 else None
    except (ValueError, TypeError) as exc:
        raise ValueError(f"body_b64 is not valid base64: {exc}") from exc
    _assert_public_url(params.url)
    request = urllib.request.Request(params.url, data=body, method=params.method, headers=params.headers)
    opener = urllib.request.build_opener(_ValidatedRedirects())
    try:
        with opener.open(request, timeout=params.timeout_seconds) as response:
            raw = response.read(MAX_HTTP_BODY + 1)
            if len(raw) > MAX_HTTP_BODY:
                raise ValueError(f"http response exceeds {MAX_HTTP_BODY} bytes")
            return HttpResponseResult(
                status=response.status,
                headers=dict(response.headers.items()),
                body_b64=base64.b64encode(raw).decode(),
            )
    except urllib.error.HTTPError as exc:
        raw = exc.read(MAX_HTTP_BODY)
        return HttpResponseResult(
            status=exc.code,
            headers=dict(exc.headers.items()) if exc.headers is not None else {},
            body_b64=base64.b64encode(raw).decode(),
        )
    except urllib.error.URLError as exc:
        raise ValueError(f"http request failed: {exc.reason}") from exc


# aiosellers-клиент вызывает методы через dotted-путь; проверяем, что это не данные
async def _resolve_call(client: object, method: str, params: dict[str, Any]) -> Any:
    target: Any = client
    for part in method.split("."):
        target = getattr(target, part)
    if not callable(target):
        raise TypeError(f"{method} is not callable")
    result = target(**params)
    if inspect.isawaitable(result):
        result = await result
    return result


__all__ = [
    "BACKOFF_MAX",
    "BACKOFF_START",
    "EVENT_QUEUE_SIZE",
    "EVENT_TIMEOUT",
    "MAX_HTTP_BODY",
    "START_TIMEOUT",
    "STOP_TIMEOUT",
    "PluginHost",
    "permission_changes",
]
