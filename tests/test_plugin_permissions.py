"""Разрешения плагина: проверенный манифест = тот, что запустится; изменения — с подтверждением."""

from __future__ import annotations

import base64
import io
import shutil
import warnings
import zipfile
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from lucrium_plugin_sdk.manifest import load_manifest
from lucrium_protocol import (
    Command,
    ErrorCode,
    InstallPluginParams,
    ManifestPermissions,
    Platform,
    PluginSource,
    PluginStatus,
    UpdatePluginParams,
    parse_manifest_text,
)
from lucrium_protocol.ipc import STORAGE_SET

from lucrium_worker.app import WorkerApp
from lucrium_worker.logs import LogBuffer
from lucrium_worker.plugins.host import (
    PluginHost,
    _PluginProcess,
    permission_changes,
)
from lucrium_worker.plugins.installer import InstallError, PluginInstaller, _verify_extracted_manifest
from lucrium_worker.plugins.rpc import PluginRpcError
from lucrium_worker.storage import PluginRecord, Storage

from .conftest import PLUGIN_MAIN, PLUGIN_SCHEMA, PLUGIN_TOML, FakeSessions, plugin_zip
from .test_app import _call, _result

ESCALATED_TOML = PLUGIN_TOML + '[permissions]\naccounts = ["funpay"]\nnetwork = true\nfinance = true\n'


def _zip(manifest: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("plugin.toml", manifest)
        archive.writestr("config_schema.json", PLUGIN_SCHEMA)
        archive.writestr("plugin_main.py", PLUGIN_MAIN)
    return buffer.getvalue()


def _now() -> datetime:
    return datetime.now(UTC)


def _record(manifest_id: str, permissions: ManifestPermissions) -> PluginRecord:
    return PluginRecord(
        id=manifest_id,
        name="Тестовый плагин",
        version="1.0.0",
        api="1",
        source=PluginSource.LOCAL,
        enabled=True,
        config={},
        installed_at=_now(),
        updated_at=_now(),
        permissions=permissions,
    )


def _host(tmp_path: Path, installer: PluginInstaller, storage: Storage) -> PluginHost:
    return PluginHost(
        plugins_dir=installer.plugins_dir,
        sockets_dir=tmp_path / "sockets",
        sessions=FakeSessions(),  # type: ignore[arg-type]
        storage=storage,
        logs=LogBuffer(),
        on_error=lambda *_: None,
        on_change=lambda: None,
        sandbox=False,
    )


async def _no_spawn_start(self: _PluginProcess) -> None:
    """Запуск без subprocess/unix-сокета: проверяем только решение host о правах."""
    self.status = PluginStatus.RUNNING


async def test_kv_params_are_validated_before_touching_storage(tmp_path: Path) -> None:
    """Ключ/значение KV проходят через модели IPC: раньше ключ брали как str(params["key"])."""
    storage = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    await storage.open()
    installer = PluginInstaller(tmp_path / "plugins")
    installed = installer.install(plugin_zip())
    host = _host(tmp_path, installer, storage)
    process = _PluginProcess(
        record=_record("test-plugin", ManifestPermissions()),
        manifest=load_manifest(installed.directory),
        directory=installed.directory,
        socket_path=tmp_path / "sockets" / "test-plugin" / "plugin.sock",
        host=host,
    )
    with pytest.raises(PluginRpcError):
        await process._handle_request(STORAGE_SET, {"key": "k" * 600, "value": 1})
    with pytest.raises(PluginRpcError):
        await process._handle_request(STORAGE_SET, {"value": 1})
    assert await process._handle_request(STORAGE_SET, {"key": "k", "value": 1}) == {}
    assert await storage.kv_get("test-plugin", "k") == 1
    await storage.close()


def test_duplicate_manifest_entries_rejected(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        # zipfile сам предупреждает о дубле; враждебный архив строим молча
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("plugin.toml", PLUGIN_TOML)
            archive.writestr("config_schema.json", PLUGIN_SCHEMA)
            archive.writestr("plugin_main.py", PLUGIN_MAIN)
            archive.writestr("plugin.toml", ESCALATED_TOML)
    with pytest.raises(InstallError, match="duplicate archive entry"):
        installer.install(buffer.getvalue())
    assert list((tmp_path / "plugins").glob(".staging-*")) == []


def test_extracted_manifest_mismatch_detected(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "plugin.toml").write_text(ESCALATED_TOML, encoding="utf-8")
    with pytest.raises(InstallError, match="differs from the validated"):
        _verify_extracted_manifest(staging, parse_manifest_text(PLUGIN_TOML))


def test_permission_changes_describe_the_difference() -> None:
    installed = ManifestPermissions()
    actual = ManifestPermissions(accounts=[Platform.FUNPAY], network=True, finance=True)
    changes = permission_changes(installed, actual)
    assert len(changes) == 3
    assert any("network" in item for item in changes)
    assert any("finance" in item for item in changes)
    assert permission_changes(actual, actual) == []


async def test_install_records_permissions_and_update_needs_ack(app: WorkerApp) -> None:
    install = InstallPluginParams(archive_b64=base64.b64encode(plugin_zip()).decode(), auto_start=False)
    state = _result(app, await _call(app, Command.INSTALL_PLUGIN, install), Command.INSTALL_PLUGIN)
    assert state.permissions is not None
    assert state.permissions.network is False

    update = UpdatePluginParams(
        plugin_id="test-plugin",
        archive_b64=base64.b64encode(_zip(ESCALATED_TOML)).decode(),
        auto_restart=False,
    )
    response = await _call(app, Command.UPDATE_PLUGIN, update)
    assert response.payload.error is not None
    assert response.payload.error.code is ErrorCode.INVALID_PARAMS
    assert "changes permissions" in response.payload.error.message

    acked = update.model_copy(update={"ack_permissions": True})
    result = _result(app, await _call(app, Command.UPDATE_PLUGIN, acked), Command.UPDATE_PLUGIN)
    assert result.permissions is not None
    assert result.permissions.network is True
    assert result.permissions.finance is True
    record = await app.storage.get_plugin("test-plugin")
    assert record is not None and record.permissions is not None
    assert record.permissions.finance is True


async def test_start_refused_when_files_widen_permissions(tmp_path: Path) -> None:
    """Расхождение снапшота и файлов — отказ запуска, а не тихая эскалация."""
    storage = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    await storage.open()
    installer = PluginInstaller(tmp_path / "plugins")
    installed = installer.install(plugin_zip())
    (installed.directory / "plugin.toml").write_text(ESCALATED_TOML, encoding="utf-8")
    host = _host(tmp_path, installer, storage)
    with pytest.raises(RuntimeError, match="changed permissions without reinstall"):
        await host.start(_record("test-plugin", ManifestPermissions()))
    assert host.last_error("test-plugin") is not None
    await host.stop_all()
    await storage.close()


async def test_legacy_record_adopts_permissions_from_disk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_PluginProcess, "start", _no_spawn_start, raising=True)
    storage = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    await storage.open()
    installer = PluginInstaller(tmp_path / "plugins")
    installer.install(plugin_zip())
    legacy = replace(_record("test-plugin", ManifestPermissions()), permissions=None)
    await storage.upsert_plugin(legacy)

    host = _host(tmp_path, installer, storage)
    await host.start(legacy)
    stored = await storage.get_plugin("test-plugin")
    assert stored is not None and stored.permissions is not None
    assert stored.permissions.network is False
    await host.stop_all()
    await storage.close()


async def test_manifest_id_mismatch_refused(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    await storage.open()
    installer = PluginInstaller(tmp_path / "plugins")
    installed = installer.install(plugin_zip())
    shutil.copytree(installed.directory, installer.plugins_dir / "other-plugin")
    host = _host(tmp_path, installer, storage)
    with pytest.raises(RuntimeError, match="manifest declares id"):
        await host.start(_record("other-plugin", ManifestPermissions()))
    await host.stop_all()
    await storage.close()
