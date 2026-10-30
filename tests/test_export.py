"""Тесты экспорта/импорта состояния (спека §4)."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest
from lucrium_protocol import ImportMode, PluginSource

from lucrium_worker.export import ExportError, decrypt_blob, encrypt_blob, export_state, import_state
from lucrium_worker.storage import PluginRecord, Storage

from .conftest import make_account

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


async def test_corrupted_blob(storage: Storage) -> None:
    with pytest.raises(ExportError):
        await import_state(storage, b"garbage", PASSPHRASE)


def test_blob_encryption_roundtrip() -> None:
    blob = encrypt_blob(b"secret data", PASSPHRASE)
    assert blob.startswith(b"LCS1")
    assert b"secret data" not in blob
    assert decrypt_blob(blob, PASSPHRASE) == b"secret data"
    with pytest.raises(ExportError):
        decrypt_blob(blob, "other-passphrase")
