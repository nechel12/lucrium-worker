"""Общие фикстуры воркера: фейковые сессии, zip-архивы плагинов, приложение."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pytest
from lucrium_protocol import AccountCredentials, Platform, SessionStatus
from pydantic import SecretStr

from lucrium_worker.app import WorkerApp
from lucrium_worker.config import WorkerConfig
from lucrium_worker.storage import AccountRecord


class FakeSessions:
    """Подмена SessionManager: без сети, площадок и aiosellers-клиентов."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.stopped: list[str] = []
        self.calls: list[tuple[str | None, str, dict[str, Any]]] = []
        self.platforms: dict[str, Platform] = {}
        self.statuses: dict[str, SessionStatus] = {}

    async def start(self, record: AccountRecord) -> Any:
        self.started.append(record.id)
        self.platforms[record.id] = record.platform
        self.statuses[record.id] = SessionStatus.RUNNING
        return self.runtime(record.id)

    async def stop(self, account_id: str) -> None:
        self.stopped.append(account_id)
        self.statuses[account_id] = SessionStatus.STOPPED

    async def stop_all(self) -> None:
        for account_id in list(self.statuses):
            await self.stop(account_id)

    def is_running(self, account_id: str) -> bool:
        return self.statuses.get(account_id) is SessionStatus.RUNNING

    def status(self, account_id: str) -> SessionStatus:
        return self.statuses.get(account_id, SessionStatus.STOPPED)

    def runtime(self, account_id: str) -> Any:
        if account_id not in self.platforms:
            return None
        from types import SimpleNamespace

        return SimpleNamespace(
            account_id=account_id,
            platform=self.platforms[account_id],
            status=self.status(account_id),
            last_error=None,
            last_event_at=None,
            restarts=0,
        )

    def resolve(self, session_id: str | None) -> Any:
        if session_id is None:
            running = [item for item in self.platforms if self.is_running(item)]
            if len(running) != 1:
                raise RuntimeError("session_id is required")
            session_id = running[0]
        runtime = self.runtime(session_id)
        if runtime is None or not self.is_running(session_id):
            raise RuntimeError(f"session {session_id!r} is not running")
        return runtime

    async def call(
        self,
        session_id: str | None,
        method: str,
        params: dict[str, Any],
        *,
        allow_finance: bool = False,
    ) -> Any:
        self.calls.append((session_id, method, params))
        return {"ok": True, "method": method}


def make_account(
    account_id: str = "acc-test",
    *,
    platform: Platform = Platform.FUNPAY,
    label: str = "Тестовый",
) -> AccountRecord:
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    if platform is Platform.FUNPAY:
        credentials = AccountCredentials(platform=platform, golden_key=SecretStr("gk-secret-value"))
    elif platform is Platform.PLAYEROK:
        credentials = AccountCredentials(platform=platform, access_token=SecretStr("po-secret-value"))
    else:
        credentials = AccountCredentials(platform=platform, api_key=SecretStr("gg-secret-value"), seller_id="1")
    return AccountRecord(
        id=account_id,
        platform=platform,
        label=label,
        credentials=credentials,
        proxy=None,
        created_at=now,
        updated_at=now,
    )


def make_zip(files: dict[str, str], *, root: str = "") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            path = f"{root}/{name}" if root else name
            archive.writestr(path, content)
    return buffer.getvalue()


PLUGIN_TOML = """
[plugin]
id = "test-plugin"
name = "Тестовый плагин"
version = "1.0.0"
api = "1"
entry = "plugin_main"

[config]
schema = "config_schema.json"
"""

PLUGIN_SCHEMA = """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "properties": {"greeting": {"type": "string", "minLength": 1}},
  "required": ["greeting"]
}
"""

PLUGIN_MAIN = """
from __future__ import annotations

from lucrium_plugin_sdk import PluginContext, PluginEvent


async def on_start(ctx: PluginContext) -> None:
    # >64 KiB: регрессия на host-side asyncio limit в NDJSON IPC.
    await ctx.storage.set("big", "x" * 200_000)
    size = len(await ctx.storage.get("big", ""))
    ctx.log.info("test plugin started", greeting=ctx.config.get("greeting"), big=size)


async def on_event(event: PluginEvent, ctx: PluginContext) -> None:
    if event.kind == "message":
        ctx.log.info("got message", session_id=event.session_id)
"""


def plugin_zip(*, root: str = "") -> bytes:
    return make_zip(
        {
            "plugin.toml": PLUGIN_TOML,
            "config_schema.json": PLUGIN_SCHEMA,
            "plugin_main.py": PLUGIN_MAIN,
        },
        root=root,
    )


@pytest.fixture
def config(tmp_path: Path) -> WorkerConfig:
    return WorkerConfig(
        data_dir=tmp_path / "data",
        host="127.0.0.1",
        port=8443,
        token="a" * 64,
        heartbeat_seconds=3600,
    )


@pytest.fixture
async def app(config: WorkerConfig) -> Any:
    instance = WorkerApp(config)
    instance.sessions = FakeSessions()  # type: ignore[assignment]
    await instance.start()
    try:
        yield instance
    finally:
        await instance.stop()
