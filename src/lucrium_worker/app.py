"""Ядро воркера: состояние, команды панели, события, жизненный цикл.

``WorkerApp`` — единственное место, где сходятся аккаунты, сессии, плагины и
протокол. Транспорт (WSS) — в :mod:`lucrium_worker.server`; здесь только
``handle_request`` и широковещательные события.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import platform as platform_module
import sys
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jsonschema
from lucrium_plugin_sdk.manifest import load_config_schema
from lucrium_protocol import (
    PROTOCOL_VERSION,
    AccountInput,
    AccountRef,
    AccountWithCredentials,
    AddAccountParams,
    CallClientParams,
    CallClientResult,
    Command,
    ErrorCode,
    Event,
    ExportStateParams,
    GetEventsParams,
    GetEventsResult,
    GetLogsParams,
    GetLogsResult,
    GetPluginSchemaParams,
    GetPluginSchemaResult,
    GetStateParams,
    ImportStateParams,
    ImportStateResult,
    InstallPluginParams,
    LogLevel,
    PingParams,
    PingResult,
    PluginConfigResult,
    PluginError,
    PluginRef,
    PluginSource,
    PluginState,
    PluginStatus,
    ProtocolError,
    RemoveAccountParams,
    RemovePluginParams,
    Request,
    Response,
    RestartParams,
    RestartResult,
    RestartScope,
    RotateTokenParams,
    RotateTokenResult,
    ServerEvent,
    SessionRef,
    SessionStatus,
    SetPluginConfigParams,
    StartSessionParams,
    StateSnapshot,
    StateWithCredentials,
    StatusCounts,
    StopSessionParams,
    UpdateAccountParams,
    UpdatePluginParams,
    WorkerStatus,
    build_error_response,
    build_event,
    build_response,
    decode_b64,
)

from . import __version__
from .accounts import AccountError, AccountService
from .config import WorkerConfig, ensure_directories
from .export import ExportError, export_state, import_state, parse_state_dump
from .logs import LogBuffer
from .plugins.host import PluginHost
from .plugins.installer import InstallError, PluginInstaller
from .sessions import SessionError, SessionManager
from .storage import AccountRecord, PluginRecord, Storage

Broadcaster = Callable[[Event], Awaitable[None]]
LogListener = Callable[[Any], None]


class WorkerApp:
    def __init__(self, config: WorkerConfig, *, config_path: Path | None = None) -> None:
        self.config = config
        self.config_path = config_path
        self.storage = Storage(config.database_path, config.secret_key_path)
        self.logs = LogBuffer()
        self.accounts = AccountService(self.storage)
        self.sessions = SessionManager(data_dir=config.data_dir, on_event=self._on_hub_event, logs=self.logs)
        self.installer = PluginInstaller(config.plugins_dir)
        self.plugins = PluginHost(
            plugins_dir=config.plugins_dir,
            sockets_dir=config.data_dir / "sockets",
            sessions=self.sessions,
            storage=self.storage,
            logs=self.logs,
            on_error=self._on_plugin_error,
            on_change=self._touch,
            memory_limit_bytes=self.config.plugin_memory_limit_mb * 1024 * 1024,
            sandbox=self.config.plugin_sandbox,
            data_dir=self.config.data_dir,
            config_dir=config_path.parent if config_path is not None else None,
        )
        self.connected_panels = 0
        self._started_at = datetime.now(UTC)
        self._revision = 0
        self._token = config.token
        self._broadcaster: Broadcaster | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._retention_task: asyncio.Task[None] | None = None
        self._state_task: asyncio.Task[None] | None = None
        self._background: set[asyncio.Task[None]] = set()
        self._shutdown = asyncio.Event()
        self._closed = False

    # --- жизненный цикл -------------------------------------------------------

    @property
    def token(self) -> str:
        return self._token

    @property
    def revision(self) -> int:
        return self._revision

    def set_broadcaster(self, broadcaster: Broadcaster) -> None:
        self._broadcaster = broadcaster

    async def start(self) -> None:
        ensure_directories(self.config)
        await self.storage.open()
        self.logs.set_listener(self._on_log_entry)
        self.logs.set_file(self.config.logs_dir / "worker.log")
        await self._restore_sessions()
        await self._restore_plugins()
        await self._purge_events()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="worker-heartbeat")
        self._retention_task = asyncio.create_task(self._retention_loop(), name="worker-retention")
        self.logs.add(LogLevel.INFO, "worker", f"worker {__version__} started", {"data_dir": str(self.config.data_dir)})
        self._touch()

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        if self._retention_task is not None:
            self._retention_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._retention_task
        if self._state_task is not None:
            self._state_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._state_task
        for task in list(self._background):
            task.cancel()
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        await self.plugins.stop_all()
        await self.sessions.stop_all()
        await self.storage.close()
        self.logs.close_file()
        self._shutdown.set()

    async def wait_closed(self) -> None:
        await self._shutdown.wait()

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def _restore_plugins(self) -> None:
        for record in await self.storage.list_plugins():
            if not record.enabled:
                continue
            try:
                await self.plugins.start(record)
            except Exception as exc:
                self.logs.add(LogLevel.ERROR, record.id, f"cannot start plugin: {type(exc).__name__}: {exc}")

    async def _restore_sessions(self) -> None:
        for record in await self.storage.list_accounts():
            if not record.desired_running:
                continue
            try:
                await self.sessions.start(record)
            except Exception as exc:
                self.logs.add(LogLevel.ERROR, "worker", f"cannot start session {record.id}: {exc}")

    # --- команды --------------------------------------------------------------

    async def handle_request(self, request: Request) -> Response:
        try:
            result = await self._dispatch(request)
        except ProtocolError as exc:
            return build_error_response(request, exc)
        except Exception as exc:
            self.logs.add(LogLevel.ERROR, "worker", f"command {request.payload.method} failed: {exc}")
            return build_error_response(request, ProtocolError.internal(exc))
        return build_response(request, result)

    async def _dispatch(self, request: Request) -> Any:
        payload = request.payload
        method = str(payload.method)
        if method == Command.GET_STATE:
            assert isinstance(payload, GetStateParams)
            return await self.state_with_credentials()
        if method == Command.ADD_ACCOUNT:
            assert isinstance(payload, AddAccountParams)
            return await self._add_account(payload)
        if method == Command.UPDATE_ACCOUNT:
            assert isinstance(payload, UpdateAccountParams)
            return await self._update_account(payload)
        if method == Command.REMOVE_ACCOUNT:
            assert isinstance(payload, RemoveAccountParams)
            return await self._remove_account(payload)
        if method == Command.START_SESSION:
            assert isinstance(payload, StartSessionParams)
            return await self._start_session(payload)
        if method == Command.STOP_SESSION:
            assert isinstance(payload, StopSessionParams)
            return await self._stop_session(payload)
        if method == Command.INSTALL_PLUGIN:
            assert isinstance(payload, InstallPluginParams)
            return await self._install_plugin(payload)
        if method == Command.UPDATE_PLUGIN:
            assert isinstance(payload, UpdatePluginParams)
            return await self._update_plugin(payload)
        if method == Command.REMOVE_PLUGIN:
            assert isinstance(payload, RemovePluginParams)
            return await self._remove_plugin(payload)
        if method == Command.SET_PLUGIN_CONFIG:
            assert isinstance(payload, SetPluginConfigParams)
            return await self._set_plugin_config(payload)
        if method == Command.GET_PLUGIN_SCHEMA:
            assert isinstance(payload, GetPluginSchemaParams)
            return await self._get_plugin_schema(payload)
        if method == Command.CALL_CLIENT:
            assert isinstance(payload, CallClientParams)
            return await self._call_client(payload)
        if method == Command.EXPORT_STATE:
            assert isinstance(payload, ExportStateParams)
            try:
                return await export_state(
                    self.storage,
                    payload.passphrase.get_secret_value(),
                    include_history=payload.include_history,
                    plugins_dir=self.config.plugins_dir,
                )
            except ExportError as exc:
                raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc
        if method == Command.IMPORT_STATE:
            assert isinstance(payload, ImportStateParams)
            return await self._import_state(payload)
        if method == Command.GET_LOGS:
            assert isinstance(payload, GetLogsParams)
            return self._get_logs(payload)
        if method == Command.GET_EVENTS:
            assert isinstance(payload, GetEventsParams)
            return await self._get_events(payload)
        if method == Command.ROTATE_TOKEN:
            assert isinstance(payload, RotateTokenParams)
            return self._rotate_token()
        if method == Command.RESTART:
            assert isinstance(payload, RestartParams)
            return await self._restart(payload)
        if method == Command.PING:
            assert isinstance(payload, PingParams)
            return PingResult(nonce=payload.nonce, server_time=datetime.now(UTC), revision=self._revision)
        raise ProtocolError(ErrorCode.METHOD_NOT_FOUND, f"unknown method {method!r}")

    # --- аккаунты -------------------------------------------------------------

    async def _add_account(self, payload: AddAccountParams) -> AccountRef:
        record = await self.accounts.create(payload.account)
        await self.sessions.start(record)
        self._touch()
        return AccountRef(account_id=record.id)

    async def _update_account(self, payload: UpdateAccountParams) -> AccountRef:
        try:
            record = await self.accounts.update(
                payload.account_id,
                label=payload.label,
                credentials=payload.credentials,
                proxy=payload.proxy,
                clear_proxy=payload.clear_proxy,
            )
        except AccountError as exc:
            raise ProtocolError(ErrorCode.NOT_FOUND, str(exc)) from exc
        # Перезапускаем и RUNNING, и ERROR/STARTING: supervisor держит старый
        # record и иначе продолжит ретраиться со старыми кредами.
        if self.sessions.runtime(record.id) is not None:
            await self.sessions.stop(record.id)
            await self.sessions.start(record)
        self._touch()
        return AccountRef(account_id=record.id)

    async def _remove_account(self, payload: RemoveAccountParams) -> AccountRef:
        try:
            record = await self.accounts.delete(payload.account_id)
        except AccountError as exc:
            raise ProtocolError(ErrorCode.NOT_FOUND, str(exc)) from exc
        await self.sessions.stop(record.id)
        if Path(record.id).name == record.id:
            for suffix in ("", "-wal", "-shm"):
                (self.config.data_dir / "sessions" / f"{record.id}.db{suffix}").unlink(missing_ok=True)
        self._touch()
        return AccountRef(account_id=record.id)

    async def _start_session(self, payload: StartSessionParams) -> SessionRef:
        try:
            record = await self.accounts.get(payload.account_id)
        except AccountError as exc:
            raise ProtocolError(ErrorCode.NOT_FOUND, str(exc)) from exc
        await self.storage.set_account_desired_running(record.id, True)
        await self.sessions.start(record)
        self._touch()
        return SessionRef(account_id=record.id, status=self.sessions.status(record.id))

    async def _stop_session(self, payload: StopSessionParams) -> SessionRef:
        try:
            record = await self.accounts.get(payload.account_id)
        except AccountError as exc:
            raise ProtocolError(ErrorCode.NOT_FOUND, str(exc)) from exc
        await self.storage.set_account_desired_running(record.id, False)
        await self.sessions.stop(record.id)
        self._touch()
        return SessionRef(account_id=record.id, status=self.sessions.status(record.id))

    # --- плагины --------------------------------------------------------------

    async def _install_plugin(self, payload: InstallPluginParams) -> PluginState:
        archive = decode_b64(payload.archive_b64, field="archive_b64")
        _verify_sha256(archive, payload.sha256)
        try:
            installed = self.installer.install(archive, overwrite=payload.overwrite)
        except InstallError as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc
        existing = await self.storage.get_plugin(installed.manifest.id)
        now = datetime.now(UTC)
        record = PluginRecord(
            id=installed.manifest.id,
            name=installed.manifest.name,
            version=installed.manifest.version,
            api=installed.manifest.api,
            source=PluginSource.LOCAL,
            enabled=True if existing is None else existing.enabled,
            config={} if existing is None else existing.config,
            installed_at=now if existing is None else existing.installed_at,
            updated_at=now,
        )
        await self.storage.upsert_plugin(record)
        if payload.auto_start and record.enabled:
            try:
                if self.plugins.status(record.id) in {
                    PluginStatus.RUNNING,
                    PluginStatus.STARTING,
                    PluginStatus.CRASHED,
                    PluginStatus.RESTARTING,
                }:
                    await self.plugins.restart(record)
                else:
                    await self.plugins.start(record)
            except Exception as exc:
                self.logs.add(
                    LogLevel.ERROR,
                    record.id,
                    f"plugin installed but cannot start: {type(exc).__name__}: {exc}",
                )
        self._touch()
        return await self._plugin_state(record)

    async def _update_plugin(self, payload: UpdatePluginParams) -> PluginState:
        existing = await self.storage.get_plugin(payload.plugin_id)
        if existing is None:
            raise ProtocolError(ErrorCode.NOT_FOUND, f"plugin {payload.plugin_id!r} is not installed")
        archive = decode_b64(payload.archive_b64, field="archive_b64")
        _verify_sha256(archive, payload.sha256)
        try:
            incoming = self.installer.inspect(archive)
        except InstallError as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc
        if incoming.id != existing.id:
            raise ProtocolError(
                ErrorCode.INVALID_PARAMS,
                f"archive contains {incoming.id!r}, expected {existing.id!r}",
            )
        try:
            self.installer.install(archive, overwrite=True)
        except InstallError as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc
        record = PluginRecord(
            id=existing.id,
            name=incoming.name,
            version=incoming.version,
            api=incoming.api,
            source=existing.source,
            enabled=existing.enabled,
            config=existing.config,
            installed_at=existing.installed_at,
            updated_at=datetime.now(UTC),
        )
        await self.storage.upsert_plugin(record)
        was_running = self.plugins.status(record.id) in {
            PluginStatus.RUNNING,
            PluginStatus.STARTING,
            PluginStatus.CRASHED,
            PluginStatus.RESTARTING,
        }
        if payload.auto_restart and was_running:
            await self.plugins.restart(record)
        self._touch()
        return await self._plugin_state(record)

    async def _remove_plugin(self, payload: RemovePluginParams) -> PluginRef:
        existing = await self.storage.get_plugin(payload.plugin_id)
        if existing is None:
            raise ProtocolError(ErrorCode.NOT_FOUND, f"plugin {payload.plugin_id!r} is not installed")
        await self.plugins.stop(existing.id, reason="plugin removed")
        try:
            self.installer.remove(existing.id)
        except InstallError as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc
        await self.storage.delete_plugin(existing.id, purge_data=payload.purge_data)
        self._touch()
        return PluginRef(plugin_id=existing.id)

    async def _set_plugin_config(self, payload: SetPluginConfigParams) -> PluginConfigResult:
        existing = await self.storage.get_plugin(payload.plugin_id)
        if existing is None:
            raise ProtocolError(ErrorCode.NOT_FOUND, f"plugin {payload.plugin_id!r} is not installed")
        self._validate_config(existing, payload.config)
        record = PluginRecord(
            id=existing.id,
            name=existing.name,
            version=existing.version,
            api=existing.api,
            source=existing.source,
            enabled=existing.enabled,
            config=dict(payload.config),
            installed_at=existing.installed_at,
            updated_at=datetime.now(UTC),
        )
        await self.storage.upsert_plugin(record)
        restart_triggered = self.plugins.status(record.id) in {
            PluginStatus.RUNNING,
            PluginStatus.STARTING,
            PluginStatus.CRASHED,
            PluginStatus.RESTARTING,
        }
        if restart_triggered:
            await self.plugins.restart(record)
        self._touch()
        return PluginConfigResult(plugin_id=record.id, restart_triggered=restart_triggered)

    async def _call_client(self, payload: CallClientParams) -> CallClientResult:
        """Вызов клиента aiosellers из панели (лоты/товары, §9)."""
        try:
            runtime = self.sessions.resolve(payload.session_id)
        except SessionError as exc:
            raise ProtocolError(ErrorCode.NOT_FOUND, str(exc)) from exc
        try:
            data = await asyncio.wait_for(
                self.sessions.call(runtime.account_id, payload.client_method, payload.params),
                timeout=60,
            )
        except TimeoutError as exc:
            raise ProtocolError(ErrorCode.UNAVAILABLE, f"call {payload.client_method} timed out") from exc
        except (SessionError, AttributeError, TypeError) as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc
        return CallClientResult(data=data)

    async def _get_plugin_schema(self, payload: GetPluginSchemaParams) -> GetPluginSchemaResult:
        record = await self.storage.get_plugin(payload.plugin_id)
        if record is None:
            raise ProtocolError(ErrorCode.NOT_FOUND, f"plugin {payload.plugin_id!r} is not installed")
        manifest = self.installer.read_manifest(record.id)
        schema = load_config_schema(self.installer.directory_for(record.id), manifest)
        return GetPluginSchemaResult(plugin_id=record.id, config_schema=schema, config=record.config)

    def _validate_config(self, record: PluginRecord, config: dict[str, Any]) -> None:
        manifest = self.installer.read_manifest(record.id)
        schema = load_config_schema(self.installer.directory_for(record.id), manifest)
        if schema is None:
            return
        try:
            validator = jsonschema.Draft202012Validator(schema)
            validator.validate(config)
        except (jsonschema.ValidationError, jsonschema.SchemaError) as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, f"invalid config: {exc}") from exc
        except Exception as exc:  # неразрешимый $ref/битая схема плагина
            raise ProtocolError(ErrorCode.INVALID_PARAMS, f"invalid config schema: {exc}") from exc

    # --- состояние ------------------------------------------------------------

    async def state_snapshot(self) -> StateSnapshot:
        accounts = await self.accounts.list()
        plugins = await self.storage.list_plugins()
        return StateSnapshot(
            revision=self._revision,
            generated_at=datetime.now(UTC),
            status=self.worker_status(accounts=accounts, plugins=plugins),
            accounts=[self._account_state(record) for record in accounts],
            plugins=[await self._plugin_state(record, include_config=False) for record in plugins],
        )

    async def state_with_credentials(self) -> StateWithCredentials:
        accounts = await self.accounts.list()
        plugins = await self.storage.list_plugins()
        return StateWithCredentials(
            revision=self._revision,
            generated_at=datetime.now(UTC),
            status=self.worker_status(accounts=accounts, plugins=plugins),
            accounts=[
                AccountWithCredentials(
                    id=record.id,
                    account=AccountInput(
                        platform=record.platform,
                        label=record.label,
                        credentials=record.credentials,
                        proxy=record.proxy,
                    ),
                )
                for record in accounts
            ],
            plugins=[await self._plugin_state(record, include_config=False) for record in plugins],
        )

    def _account_state(self, record: AccountRecord) -> Any:
        runtime = self.sessions.runtime(record.id)
        return AccountService.to_state(
            record,
            status=self.sessions.status(record.id),
            last_error=None if runtime is None or runtime.last_error is None else runtime.last_error[:2000],
            last_event_at=None if runtime is None else runtime.last_event_at,
            restarts=0 if runtime is None else runtime.restarts,
        )

    async def _plugin_state(self, record: PluginRecord, *, include_config: bool = True) -> PluginState:
        status = self.plugins.status(record.id)
        if not record.enabled:
            status = PluginStatus.STOPPED
        has_schema = False
        with contextlib.suppress(Exception):
            has_schema = self.installer.read_manifest(record.id).schema_path is not None
        return PluginState(
            id=record.id,
            name=record.name,
            version=record.version,
            api=record.api,
            source=record.source,
            enabled=record.enabled,
            status=status,
            restarts=self.plugins.restarts(record.id),
            last_error=(self.plugins.last_error(record.id) or "")[:2000] or None,
            has_config_schema=has_schema,
            config=record.config if include_config else {},
            installed_at=record.installed_at,
            updated_at=record.updated_at,
        )

    def worker_status(self, *, accounts: list[AccountRecord], plugins: list[PluginRecord]) -> WorkerStatus:
        running_sessions = sum(1 for account in accounts if self.sessions.is_running(account.id))
        error_sessions = sum(1 for account in accounts if self.sessions.status(account.id) is SessionStatus.ERROR)
        running_plugins = sum(1 for record in plugins if self.plugins.status(record.id) is PluginStatus.RUNNING)
        error_plugins = sum(1 for record in plugins if self.plugins.status(record.id) is PluginStatus.CRASHED)
        return WorkerStatus(
            version=__version__,
            protocol=PROTOCOL_VERSION,
            started_at=self._started_at,
            uptime_seconds=(datetime.now(UTC) - self._started_at).total_seconds(),
            python=platform_module.python_version(),
            os=sys.platform,
            sessions=StatusCounts(total=len(accounts), running=running_sessions, error=error_sessions),
            plugins=StatusCounts(total=len(plugins), running=running_plugins, error=error_plugins),
            connected_panels=self.connected_panels,
        )

    # --- события --------------------------------------------------------------

    async def _on_hub_event(self, event: Any) -> None:
        try:
            await self.storage.add_event(event)
        except Exception as exc:
            self.logs.add(LogLevel.ERROR, "worker", f"cannot persist event: {exc}")
        await self.broadcast(build_event(ServerEvent.HUB, event))
        await self.plugins.dispatch(event)

    def _on_log_entry(self, entry: Any) -> None:
        if entry.source == "worker":
            return
        self._schedule(self.broadcast(build_event(ServerEvent.PLUGIN_LOG, entry)))

    def _on_plugin_error(self, error: PluginError) -> None:
        self._schedule(self.broadcast(build_event(ServerEvent.PLUGIN_ERROR, error)))
        self._touch()

    async def broadcast(self, event: Event) -> None:
        if self._broadcaster is not None:
            await self._broadcaster(event)

    def _schedule(self, awaitable: Coroutine[Any, Any, None]) -> None:
        async def _run() -> None:
            with contextlib.suppress(Exception):
                await awaitable

        self._spawn(_run(), "worker-event")

    def _spawn(self, coroutine: Coroutine[Any, Any, None], name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def _touch(self) -> None:
        self._revision += 1
        if self._state_task is not None and not self._state_task.done():
            return
        self._state_task = asyncio.create_task(self._broadcast_state(), name="worker-state")

    async def _broadcast_state(self) -> None:
        await asyncio.sleep(0)
        if self._broadcaster is None:
            return
        snapshot = await self.state_snapshot()
        await self.broadcast(build_event(ServerEvent.STATE, snapshot))

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_seconds)
            try:
                accounts = await self.accounts.list()
                plugins = await self.storage.list_plugins()
                status = self.worker_status(accounts=accounts, plugins=plugins)
                await self.broadcast(build_event(ServerEvent.STATUS, status))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logs.add(LogLevel.WARNING, "worker", f"heartbeat failed: {type(exc).__name__}: {exc}")

    async def _retention_loop(self) -> None:
        while True:
            await asyncio.sleep(24 * 3600)
            await self._purge_events()

    async def _purge_events(self) -> None:
        before = datetime.now(UTC) - timedelta(days=self.config.event_retention_days)
        try:
            removed = await self.storage.purge_events(before)
        except Exception as exc:
            self.logs.add(LogLevel.WARNING, "worker", f"event retention failed: {exc}")
            return
        if removed:
            self.logs.add(LogLevel.INFO, "worker", f"event retention removed {removed} records")

    # --- прочее ---------------------------------------------------------------

    def _get_logs(self, payload: GetLogsParams) -> GetLogsResult:
        # Без cursor панель хочет «последние N», а не первые N из буфера.
        if payload.cursor is None:
            entries = self.logs.tail(
                scope=payload.scope,
                plugin_id=payload.plugin_id,
                level=payload.level,
                limit=payload.limit,
            )
            return GetLogsResult(entries=entries, next_cursor=None)
        entries, next_cursor = self.logs.query(
            scope=payload.scope,
            plugin_id=payload.plugin_id,
            level=payload.level,
            limit=payload.limit,
            cursor=payload.cursor,
        )
        return GetLogsResult(entries=entries, next_cursor=next_cursor)

    async def _get_events(self, payload: GetEventsParams) -> GetEventsResult:
        events, next_cursor = await self.storage.list_events(
            session_id=payload.session_id,
            kind=payload.kind,
            limit=payload.limit,
            cursor=payload.cursor,
        )
        return GetEventsResult(events=events, next_cursor=next_cursor)

    def _rotate_token(self) -> RotateTokenResult:
        from .config import generate_token

        token = generate_token()
        self._token = token
        if self.config_path is not None:
            from dataclasses import replace

            self.config = replace(self.config, token=token)
            self.config.save(self.config_path)
        self.logs.add(LogLevel.WARNING, "worker", "pairing token rotated")
        return RotateTokenResult(token=token, rotated_at=datetime.now(UTC))

    async def _restart(self, payload: RestartParams) -> RestartResult:
        if payload.scope is RestartScope.SESSIONS:
            records = await self.storage.list_accounts()
            await self.sessions.stop_all()
            for record in records:
                await self.sessions.start(record)
            self._touch()
            return RestartResult(scheduled=True, delay_seconds=0.0)

        async def _shutdown() -> None:
            await asyncio.sleep(payload.delay_seconds)
            self.request_shutdown()

        self._spawn(_shutdown(), "worker-restart")
        return RestartResult(scheduled=True, delay_seconds=payload.delay_seconds)

    async def _import_state(self, payload: ImportStateParams) -> ImportStateResult:
        raw = decode_b64(payload.blob_b64, field="blob_b64")
        passphrase = payload.passphrase.get_secret_value()
        # Сначала полностью проверяем дамп. Неверный пароль/битый файл не должен
        # останавливать работающие сессии и плагины.
        try:
            parsed = await asyncio.to_thread(parse_state_dump, raw, passphrase)
        except ExportError as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc

        await self.plugins.stop_all()
        await self.sessions.stop_all()
        try:
            result = await import_state(
                self.storage,
                raw,
                passphrase,
                mode=payload.mode,
                plugins_dir=self.config.plugins_dir,
                parsed=parsed,
            )
        except ExportError as exc:
            raise ProtocolError(ErrorCode.INVALID_PARAMS, str(exc)) from exc
        finally:
            # Даже при ошибке импорта возвращаем воркер в рабочее состояние
            # (БД откатывается транзакцией, файлы плагинов — backup'ом).
            await self._restore_sessions()
            await self._restore_plugins()
            self._touch()
        return result.model_copy(
            update={
                "sessions_started": len(
                    [record for record in await self.storage.list_accounts() if record.desired_running]
                )
            }
        )


def _verify_sha256(archive: bytes, expected: str | None) -> None:
    if expected is None:
        return
    actual = hashlib.sha256(archive).hexdigest()
    if actual != expected.lower():
        raise ProtocolError(ErrorCode.INVALID_PARAMS, "archive sha256 mismatch")


__all__ = ["WorkerApp"]
