"""Тесты экспорта/импорта состояния (спека §4)."""

from __future__ import annotations

import base64
import gzip
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest
from lucrium_protocol import EventKind, HubEvent, ImportMode, Platform, PluginSource

from lucrium_worker.export import (
    ExportError,
    decrypt_blob,
    encrypt_blob,
    export_state,
    import_state,
    parse_state_dump,
)
from lucrium_worker.plugins.installer import PluginInstaller
from lucrium_worker.storage import PluginRecord, Storage

from .conftest import make_account, plugin_zip

PASSPHRASE = "correct-horse-battery-staple"


@pytest.fixture
async def storage(tmp_path: Path) -> Storage:
    instance = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


async def _seed(storage: Storage) -> None:
    from datetime import UTC, datetime

    await storage.upsert_account(make_account())
    now = datetime.now(UTC)
    await storage.upsert_plugin(
        PluginRecord(
            id="test-plugin",
            name="Тест",
            version="1.0.0",
            api="1",
            source=PluginSource.LOCAL,
            enabled=True,
            config={"greeting": "Привет"},
            installed_at=now,
            updated_at=now,
        )
    )
    await storage.kv_set("test-plugin", "counter", 7)


async def test_export_import_roundtrip(storage: Storage) -> None:
    await _seed(storage)
    result = await export_state(storage, PASSPHRASE)
    assert result.size_bytes > 0
    assert len(result.sha256) == 64
    assert result.filename.endswith(".lcs")

    await storage.clear_all()
    imported = await import_state(storage, base64.b64decode(result.blob_b64), PASSPHRASE)
    assert imported.accounts == 1
    assert imported.plugins == 1

    account = await storage.get_account("acc-test")
    assert account is not None
    assert account.credentials.secret_value() == "gk-secret-value"
    plugin = await storage.get_plugin("test-plugin")
    assert plugin is not None
    assert plugin.config == {"greeting": "Привет"}
    assert await storage.kv_get("test-plugin", "counter") == 7


async def test_wrong_passphrase(storage: Storage) -> None:
    await _seed(storage)
    result = await export_state(storage, PASSPHRASE)
    with pytest.raises(ExportError, match="wrong passphrase"):
        await import_state(storage, base64.b64decode(result.blob_b64), "wrong-passphrase")


async def test_import_replace_mode(storage: Storage) -> None:
    await _seed(storage)
    result = await export_state(storage, PASSPHRASE)
    await storage.upsert_account(make_account("acc-extra"))
    imported = await import_state(storage, base64.b64decode(result.blob_b64), PASSPHRASE, mode=ImportMode.REPLACE)
    assert imported.mode is ImportMode.REPLACE
    accounts = await storage.list_accounts()
    assert [item.id for item in accounts] == ["acc-test"]


async def test_malicious_ids_and_labels_rejected(storage: Storage) -> None:
    for account_id, label, plugin_id in (
        ("../../escape", "ok", "test-plugin"),
        ("acc-1", "", "test-plugin"),
        ("acc-1", "ok", "../../victim"),
    ):
        payload = {
            "format": 2,
            "accounts": [
                {
                    "id": account_id,
                    "platform": "funpay",
                    "label": label,
                    "credentials": {"platform": "funpay", "golden_key": "gk"},
                    "created_at": datetime.now(UTC).isoformat(),
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            ],
            "plugins": [
                {
                    "id": plugin_id,
                    "name": "P",
                    "version": "1.0.0",
                    "api": "1",
                    "source": "local",
                    "enabled": True,
                    "config": {},
                    "installed_at": datetime.now(UTC).isoformat(),
                    "updated_at": datetime.now(UTC).isoformat(),
                    "data": {},
                }
            ],
        }
        blob = encrypt_blob(gzip.compress(json.dumps(payload).encode()), PASSPHRASE)
        with pytest.raises(ExportError):
            parse_state_dump(blob, PASSPHRASE)


async def test_truncated_gzip_is_export_error(storage: Storage) -> None:
    payload = gzip.compress(json.dumps({"format": 2, "accounts": [], "plugins": []}).encode())
    blob = encrypt_blob(payload[:-5], PASSPHRASE)
    with pytest.raises(ExportError):
        await import_state(storage, blob, PASSPHRASE)


async def test_format_v1_dump_is_still_accepted(storage: Storage) -> None:
    payload = {
        "format": 1,
        "accounts": [
            {
                "id": "acc-1",
                "platform": "funpay",
                "label": "Old",
                "credentials": {"platform": "funpay", "golden_key": "gk"},
                "created_at": datetime.now(UTC).isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
            }
        ],
        "plugins": [],
    }
    blob = encrypt_blob(gzip.compress(json.dumps(payload).encode()), PASSPHRASE)
    result = await import_state(storage, blob, PASSPHRASE)
    assert result.accounts == 1
    loaded = await storage.get_account("acc-1")
    assert loaded is not None and loaded.desired_running is True


async def test_export_and_restore_plugin_artifacts(storage: Storage, tmp_path: Path) -> None:
    now = datetime.now(UTC)
    installer = PluginInstaller(tmp_path / "plugins")
    installer.install(plugin_zip())
    await storage.upsert_plugin(
        PluginRecord(
            id="test-plugin",
            name="Тест",
            version="1.0.0",
            api="1",
            source=PluginSource.LOCAL,
            enabled=True,
            config={"greeting": "Привет"},
            installed_at=now,
            updated_at=now,
        )
    )
    result = await export_state(storage, PASSPHRASE, plugins_dir=tmp_path / "plugins")
    parsed = parse_state_dump(base64.b64decode(result.blob_b64), PASSPHRASE)
    assert "test-plugin" in parsed.artifacts

    for path in sorted((tmp_path / "plugins").glob("*")):
        if path.is_dir():
            shutil.rmtree(path)
    await storage.delete_plugin("test-plugin", purge_data=True)
    await import_state(
        storage,
        base64.b64decode(result.blob_b64),
        PASSPHRASE,
        plugins_dir=tmp_path / "plugins",
    )
    assert (tmp_path / "plugins" / "test-plugin" / "plugin.toml").is_file()


async def test_include_history_roundtrip(storage: Storage) -> None:
    now = datetime.now(UTC)
    await storage.add_event(
        HubEvent(platform=Platform.FUNPAY, session_id="acc-1", kind=EventKind.MESSAGE, payload={"n": 1}, ts=now)
    )
    result = await export_state(storage, PASSPHRASE, include_history=True)
    await storage.clear_all()
    await import_state(storage, base64.b64decode(result.blob_b64), PASSPHRASE)
    events = await storage.list_all_events()
    assert len(events) == 1 and events[0].payload["n"] == 1


async def test_corrupted_blob(storage: Storage) -> None:
    with pytest.raises(ExportError):
        await import_state(storage, b"garbage", PASSPHRASE)


def test_blob_encryption_roundtrip() -> None:
    blob = encrypt_blob(b"secret data", PASSPHRASE)
    assert blob.startswith(b"LCS2")
    assert b"secret data" not in blob
    assert decrypt_blob(blob, PASSPHRASE) == b"secret data"
    with pytest.raises(ExportError):
        decrypt_blob(blob, "other-passphrase")
