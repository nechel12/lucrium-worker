"""Тесты SQLite-хранилища: креды шифруются at-rest, KV и CRUD."""

from __future__ import annotations

from pathlib import Path

import pytest

from lucrium_worker.storage import PluginRecord, Storage, StorageError

from .conftest import make_account


@pytest.fixture
async def storage(tmp_path: Path) -> Storage:
    instance = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


async def test_account_credentials_encrypted_at_rest(storage: Storage, tmp_path: Path) -> None:
    await storage.upsert_account(make_account())
    cursor = await storage.db.execute("SELECT credentials_enc FROM accounts")
    row = await cursor.fetchone()
    assert row is not None
    assert "gk-secret-value" not in row["credentials_enc"]

    loaded = await storage.get_account("acc-test")
    assert loaded is not None
    assert loaded.credentials.secret_value() == "gk-secret-value"
    assert loaded.platform.value == "funpay"


async def test_account_crud(storage: Storage) -> None:
    await storage.upsert_account(make_account("acc-1"))
    await storage.upsert_account(make_account("acc-2", label="Второй"))
    accounts = await storage.list_accounts()
    assert [item.id for item in accounts] == ["acc-1", "acc-2"]
    assert await storage.delete_account("acc-1") is True
    assert await storage.get_account("acc-1") is None
    assert await storage.delete_account("acc-1") is False


async def test_proxy_and_plugin_secrets_are_encrypted_at_rest(storage: Storage) -> None:
    from dataclasses import replace
    from datetime import UTC, datetime

    from lucrium_protocol import PluginSource, ProxyConfig

    now = datetime.now(UTC)
    account = replace(
        make_account("acc-proxy"),
        proxy=ProxyConfig(host="1.2.3.4", port=8080, username="u", password="proxy-secret"),
    )
    await storage.upsert_account(account)
    await storage.upsert_plugin(
        PluginRecord(
            id="test-plugin",
            name="Тест",
            version="1.0.0",
            api="1",
            source=PluginSource.LOCAL,
            enabled=True,
            config={"api_key": "plugin-secret"},
            installed_at=now,
            updated_at=now,
        )
    )
    await storage.kv_set("test-plugin", "token", "kv-secret")

    cursor = await storage.db.execute("SELECT proxy_json FROM accounts WHERE id = ?", ("acc-proxy",))
    proxy_row = await cursor.fetchone()
    cursor = await storage.db.execute("SELECT config_json FROM plugins WHERE id = ?", ("test-plugin",))
    config_row = await cursor.fetchone()
    cursor = await storage.db.execute("SELECT value_json FROM plugin_kv WHERE plugin_id = ?", ("test-plugin",))
    kv_row = await cursor.fetchone()
    assert proxy_row is not None and "proxy-secret" not in proxy_row["proxy_json"]
    assert config_row is not None and "plugin-secret" not in config_row["config_json"]
    assert kv_row is not None and "kv-secret" not in kv_row["value_json"]

    loaded = await storage.get_account("acc-proxy")
    assert loaded is not None and loaded.proxy is not None
    assert loaded.proxy.password is not None
    assert loaded.proxy.password.get_secret_value() == "proxy-secret"
    assert await storage.kv_get("test-plugin", "token") == "kv-secret"


async def test_desired_running_and_kv_prefix_are_strict(storage: Storage) -> None:
    await storage.upsert_account(make_account("acc-1"))
    await storage.set_account_desired_running("acc-1", False)
    loaded = await storage.get_account("acc-1")
    assert loaded is not None and loaded.desired_running is False

    await storage.kv_set("p", "a%b", 1)
    await storage.kv_set("p", "axb", 2)
    assert await storage.kv_keys("p", "a%") == ["a%b"]


async def test_plugin_crud_and_kv(storage: Storage, tmp_path: Path) -> None:
    from datetime import UTC, datetime

    from lucrium_protocol import PluginSource

    now = datetime.now(UTC)
    record = PluginRecord(
        id="test-plugin",
        name="Тест",
        version="1.0.0",
        api="1",
        source=PluginSource.LOCAL,
        enabled=True,
        config={"a": 1},
        installed_at=now,
        updated_at=now,
    )
    await storage.upsert_plugin(record)
    loaded = await storage.get_plugin("test-plugin")
    assert loaded is not None
    assert loaded.config == {"a": 1}

    await storage.kv_set("test-plugin", "counter", 41)
    await storage.kv_set("test-plugin", "cooldown:x", 1.5)
    assert await storage.kv_get("test-plugin", "counter") == 41
    assert await storage.kv_keys("test-plugin", "cooldown:") == ["cooldown:x"]
    assert [key for key, _ in await storage.kv_items("test-plugin")] == ["cooldown:x", "counter"]

    await storage.kv_delete("test-plugin", "counter")
    assert await storage.kv_get("test-plugin", "counter") is None

    await storage.delete_plugin("test-plugin", purge_data=True)
    assert await storage.get_plugin("test-plugin") is None
    assert await storage.kv_keys("test-plugin") == []


async def test_event_history_and_retention(storage: Storage) -> None:
    from datetime import UTC, datetime, timedelta

    from lucrium_protocol import EventKind, HubEvent, Platform

    now = datetime.now(UTC)
    await storage.add_event(
        HubEvent(platform=Platform.FUNPAY, session_id="acc-1", kind=EventKind.MESSAGE, payload={"n": 1}, ts=now)
    )
    await storage.add_event(
        HubEvent(
            platform=Platform.GGSEL,
            session_id="acc-2",
            kind=EventKind.ORDER,
            payload={"n": 2},
            ts=now - timedelta(days=40),
        )
    )

    events, _ = await storage.list_events(session_id="acc-1")
    assert [event.payload["n"] for event in events] == [1]
    events, _ = await storage.list_events(kind=EventKind.ORDER)
    assert [event.payload["n"] for event in events] == [2]

    first, cursor = await storage.list_events(limit=1)
    assert len(first) == 1 and cursor is not None
    rest, _ = await storage.list_events(limit=10, cursor=cursor)
    assert len(rest) == 1

    removed = await storage.purge_events(now - timedelta(days=30))
    assert removed == 1
    remaining, _ = await storage.list_events()
    assert [event.payload["n"] for event in remaining] == [1]


async def test_wrong_secret_key_fails_loudly(tmp_path: Path) -> None:
    db = tmp_path / "lucrium.db"
    first = Storage(db, tmp_path / "secret.key")
    await first.open()
    await first.upsert_account(make_account())
    await first.close()

    second = Storage(db, tmp_path / "other.key")
    await second.open()
    try:
        with pytest.raises(StorageError, match="cannot decrypt"):
            await second.get_account("acc-test")
    finally:
        await second.close()


async def test_meta_json(storage: Storage) -> None:
    await storage.set_meta("schema_version", 1)
    assert await storage.get_meta("schema_version") == 1
    assert await storage.get_meta("missing") is None


async def test_clear_all(storage: Storage) -> None:
    from datetime import UTC, datetime

    from lucrium_protocol import EventKind, HubEvent, Platform

    await storage.upsert_account(make_account())
    await storage.kv_set("test-plugin", "k", "v")
    await storage.add_event(
        HubEvent(
            platform=Platform.FUNPAY,
            session_id="acc-1",
            kind=EventKind.MESSAGE,
            payload={},
            ts=datetime.now(UTC),
        )
    )
    await storage.clear_all()
    assert await storage.list_accounts() == []
    assert await storage.kv_keys("test-plugin") == []
    assert await storage.list_all_events() == []
