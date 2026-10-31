"""Сессии площадок: один клиент aiosellers на аккаунт + авто-restart.

Каждая сессия живёт в своей задаче: ``start()`` → ``listen()`` → при обрыве
перезапуск с экспоненциальным backoff (1 → 30 c), пока сессию не остановят
явно. Крах одной сессии не влияет на остальные (спека §5, §6.3).
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from aiosellers import (
    FunPay,
    FunPayClientConfig,
    GGsel,
    GGselClientConfig,
    Playerok,
    PlayerokClientConfig,
    SellerClient,
    SellerHub,
    SQLiteStateStore,
)
from lucrium_protocol import HubEvent, LogLevel, Platform, SessionStatus

from .client_policy import ALLOWED_CLIENT_ROOTS, is_financial_method
from .logs import LogBuffer
from .serialization import hub_event_to_protocol, to_jsonable
from .storage import AccountRecord

BACKOFF_START = 1.0
BACKOFF_MAX = 30.0
BACKOFF_RESET_AFTER = 60.0

EventHandler = Callable[[HubEvent], Awaitable[None]]


class SessionError(Exception):
    """Сессия не найдена/не запущена или вызов невозможен."""


@dataclass(slots=True)
class SessionRuntime:
    """Текущее состояние сессии (не хранится в БД — эфемерное)."""

    account_id: str
    platform: Platform
    status: SessionStatus = SessionStatus.STOPPED
    last_error: str | None = None
    last_event_at: datetime | None = None
    restarts: int = 0
    task: asyncio.Task[None] | None = None
    stopping: bool = False
    client: SellerClient | None = field(default=None, repr=False)
    state_store: SQLiteStateStore | None = field(default=None, repr=False)


def build_client(record: AccountRecord, state: SQLiteStateStore) -> SellerClient:
    """Создаёт клиент площадки с кредами, прокси и durable state."""
    credentials = record.credentials
    proxy = record.proxy.to_url() if record.proxy is not None else None
    if record.platform is Platform.FUNPAY:
        return cast(
            SellerClient, FunPay(FunPayClientConfig(golden_key=credentials.secret_value(), proxy=proxy, state=state))
        )
    if record.platform is Platform.PLAYEROK:
        return cast(
            SellerClient,
            Playerok(PlayerokClientConfig(access_token=credentials.secret_value(), proxy=proxy, state=state)),
        )
    if credentials.seller_id is None:
        raise ValueError("ggsel account requires seller_id")
    return cast(
        SellerClient,
        GGsel(
            GGselClientConfig(
                api_key=credentials.secret_value(),
                seller_id=int(credentials.seller_id),
                proxy=proxy,
                state=state,
            )
        ),
    )


class SessionManager:
    """Держит по одной сессии на аккаунт и раздаёт нормализованные события."""

    def __init__(
        self,
        *,
        data_dir: Path,
        on_event: EventHandler,
        logs: LogBuffer,
        on_change: Callable[[], None] | None = None,
    ) -> None:
        self._data_dir = data_dir
        self._on_event = on_event
        self._logs = logs
        self._on_change = on_change
        self._runtimes: dict[str, SessionRuntime] = {}

    def _notify_change(self) -> None:
        if self._on_change is not None:
            self._on_change()

    # --- запросы --------------------------------------------------------------

    def runtime(self, account_id: str) -> SessionRuntime | None:
        return self._runtimes.get(account_id)

    def is_running(self, account_id: str) -> bool:
        runtime = self._runtimes.get(account_id)
        return runtime is not None and runtime.status is SessionStatus.RUNNING

    def status(self, account_id: str) -> SessionStatus:
        runtime = self._runtimes.get(account_id)
        return SessionStatus.STOPPED if runtime is None else runtime.status

    def resolve(self, session_id: str | None) -> SessionRuntime:
        """Сессия по id; ``None`` допустим только при ровно одной запущенной."""
        if session_id is not None:
            runtime = self._runtimes.get(session_id)
            if runtime is None or runtime.client is None or runtime.status is not SessionStatus.RUNNING:
                raise SessionError(f"session {session_id!r} is not running")
            return runtime
        running = [item for item in self._runtimes.values() if item.status is SessionStatus.RUNNING]
        if len(running) != 1:
            raise SessionError("session_id is required when the plugin has no single running session")
        return running[0]

    async def call(
        self,
        session_id: str | None,
        method: str,
        params: dict[str, Any],
        *,
        allow_finance: bool = False,
    ) -> Any:
        """Вызов метода клиента aiosellers через dotted-путь (``chats.send_message``).

        Финансовые методы (``finance.*``, ``transactions.*``, ``orders.refund``,
        ``deals.cancel``) доступны только при ``allow_finance=True`` — это
        разрешение выдаётся плагину манифестом, панель его не получает.
        """
        parts = method.split(".")
        root = parts[0]
        if root not in ALLOWED_CLIENT_ROOTS:
            raise SessionError(f"method {method!r} is not allowed")
        if is_financial_method(method) and not allow_finance:
            raise SessionError(f"method {method!r} requires permissions.finance = true")
        if any(part.startswith("_") or part in {"client", "transport", "raw"} for part in parts[1:]):
            raise SessionError(f"private client path {method!r} is not allowed")
        runtime = self.resolve(session_id)
        target: Any = runtime.client
        for part in parts:
            target = getattr(target, part)
        if not callable(target):
            raise SessionError(f"{method!r} is not callable on {runtime.platform.value} client")
        result = target(**params)
        if inspect.isawaitable(result):
            result = await result
        return to_jsonable(result)

    # --- управление -----------------------------------------------------------

    async def start(self, record: AccountRecord) -> SessionRuntime:
        runtime = self._runtimes.get(record.id)
        if (
            runtime is not None
            and runtime.status in {SessionStatus.RUNNING, SessionStatus.STARTING}
            and runtime.task is not None
            and not runtime.task.done()
        ):
            return runtime
        if runtime is not None:
            # ERROR/STOPPED/умершая задача: гасим прежнюю, чтобы не было двух сессий
            await self.stop(record.id)
        runtime = SessionRuntime(account_id=record.id, platform=record.platform)
        self._runtimes[record.id] = runtime
        runtime.task = asyncio.create_task(self._supervise(record, runtime), name=f"session-{record.id}")
        return runtime

    async def stop(self, account_id: str) -> None:
        runtime = self._runtimes.get(account_id)
        if runtime is None:
            return
        runtime.stopping = True
        runtime.status = SessionStatus.STOPPING
        task = runtime.task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self._logs.add(
                    LogLevel.WARNING,
                    "worker",
                    f"session {account_id} task failed during stop: {type(exc).__name__}: {exc}"[:2000],
                )
        runtime.status = SessionStatus.STOPPED
        self._runtimes.pop(account_id, None)
        self._notify_change()

    async def stop_all(self) -> None:
        await asyncio.gather(*(self.stop(account_id) for account_id in list(self._runtimes)), return_exceptions=True)

    # --- надзор ---------------------------------------------------------------

    async def _supervise(self, record: AccountRecord, runtime: SessionRuntime) -> None:
        backoff = BACKOFF_START
        while not runtime.stopping:
            state: SQLiteStateStore | None = None
            hub = SellerHub()
            running_since: float | None = None
            try:
                state = SQLiteStateStore(self._data_dir / "sessions" / f"{record.id}.db")
                runtime.state_store = state
                runtime.status = SessionStatus.STARTING
                self._notify_change()
                client = build_client(record, state)
                runtime.client = client
                hub.add(client, platform=record.platform.value, session_id=record.id)
                await hub.start()
                runtime.status = SessionStatus.RUNNING
                runtime.last_error = None
                running_since = time.monotonic()
                self._notify_change()
                self._logs.add(
                    LogLevel.INFO, "worker", f"session {record.id} started", {"platform": record.platform.value}
                )
                async for event in hub.stream():
                    runtime.last_event_at = datetime.now(UTC)
                    await self._on_event(hub_event_to_protocol(event))
                raise SessionError("platform event stream ended unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if running_since is not None and time.monotonic() - running_since >= BACKOFF_RESET_AFTER:
                    # Сессия была стабильной: следующую серию ошибок начинаем
                    # снова с короткой паузы.
                    backoff = BACKOFF_START
                runtime.status = SessionStatus.ERROR
                runtime.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                runtime.restarts += 1
                self._notify_change()
                self._logs.add(
                    LogLevel.ERROR,
                    "worker",
                    f"session {record.id} failed: {runtime.last_error}",
                    {"platform": record.platform.value, "restart_in": backoff},
                )
            finally:
                runtime.client = None
                with contextlib.suppress(Exception):
                    await hub.close()
                if state is not None:
                    with contextlib.suppress(Exception):
                        await state.close()
                runtime.state_store = None
            if runtime.stopping:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)


__all__ = [
    "ALLOWED_CLIENT_ROOTS",
    "BACKOFF_MAX",
    "BACKOFF_START",
    "SessionError",
    "SessionManager",
    "SessionRuntime",
    "build_client",
]
