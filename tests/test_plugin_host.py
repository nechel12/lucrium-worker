"""Интеграционный тест host плагинов: реальный subprocess + unix socket.

Только Linux: на Windows asyncio не поддерживает unix-сокеты. CI гоняет
этот тест на ubuntu-latest.
"""

from __future__ import annotations

import asyncio
import shutil
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from lucrium_protocol import EventKind, HubEvent, LogLevel, LogScope, Platform, PluginSource

from lucrium_worker.logs import LogBuffer
from lucrium_worker.plugins.host import PluginHost
from lucrium_worker.plugins.installer import PluginInstaller
from lucrium_worker.storage import PluginRecord, Storage

from .conftest import FakeSessions, make_account, plugin_zip

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="unix sockets are Linux-only")


async def test_plugin_subprocess_lifecycle(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    await storage.open()
    logs = LogBuffer()
    sessions = FakeSessions()
    await sessions.start(make_account("acc-1"))

    installer = PluginInstaller(tmp_path / "plugins")
    installed = installer.install(plugin_zip())
    now = datetime.now(UTC)
    record = PluginRecord(
        id=installed.manifest.id,
        name=installed.manifest.name,
        version=installed.manifest.version,
        api=installed.manifest.api,
        source=PluginSource.LOCAL,
        enabled=True,
        config={"greeting": "Привет"},
        installed_at=now,
        updated_at=now,
    )
    # короткий путь сокета: лимит unix socket ~108 символов
    socket_dir = Path(tempfile.mkdtemp(prefix=f"luc-{uuid.uuid4().hex[:8]}-"))
    host = PluginHost(
        plugins_dir=tmp_path / "plugins",
        sockets_dir=socket_dir,
        sessions=sessions,  # type: ignore[arg-type]
        storage=storage,
        logs=logs,
    )
    try:
        await host.start(record)
        await _wait_until(lambda: host.status(record.id).value == "running", timeout=15)

        event = HubEvent(
            platform=Platform.FUNPAY,
            session_id="acc-1",
            kind=EventKind.MESSAGE,
            payload={"message": {"text": "hi"}},
            ts=now,
        )
        await host.dispatch(event)
        await _wait_until(lambda: _has_log(logs, record.id, "got message"), timeout=10)

        started = _logs(logs, record.id)
        started_entry = next(entry for entry in started if "test plugin started" in entry.message)
        assert started_entry.fields["greeting"] == "Привет"
        assert started_entry.fields["big"] == 200_000
        assert any("got message" in entry.message for entry in started)
    finally:
        await host.stop(record.id)
        await storage.close()
        shutil.rmtree(socket_dir, ignore_errors=True)


async def _wait_until(predicate: object, *, timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():  # type: ignore[operator]
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.05)


def _logs(logs: LogBuffer, plugin_id: str) -> list[object]:
    entries, _ = logs.query(scope=LogScope.PLUGIN, plugin_id=plugin_id, level=LogLevel.DEBUG, limit=1000)
    return list(entries)


def _has_log(logs: LogBuffer, plugin_id: str, needle: str) -> bool:
    return any(needle in entry.message for entry in _logs(logs, plugin_id))  # type: ignore[attr-defined]
