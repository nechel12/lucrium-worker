"""SQLite-хранилище воркера: аккаунты, плагины, KV плагинов, метаданные.

Секреты аккаунтов шифруются at-rest ключом из ``data_dir/secret.key``
(Fernet, AES-128-CBC + HMAC). На провод креды уходят только в ответе
``get_state`` авторизованной панели (см. ``lucrium_protocol``).
"""

from __future__ import annotations

import contextlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite
from cryptography.fernet import Fernet, InvalidToken
from lucrium_protocol import AccountCredentials, EventKind, HubEvent, Platform, PluginSource, ProxyConfig, to_wire

SCHEMA_VERSION = 1


class StorageError(Exception):
    """Ошибка чтения/записи состояния."""


@dataclass(frozen=True, slots=True)
class AccountRecord:
    id: str
    platform: Platform
    label: str
    credentials: AccountCredentials
    proxy: ProxyConfig | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PluginRecord:
    id: str
    name: str
    version: str
    api: str
    source: PluginSource
    enabled: bool
    config: dict[str, Any]
    installed_at: datetime
    updated_at: datetime

    @property
    def path(self) -> str:
        """Каталог плагина относительно ``plugins_dir`` (id)."""
        return self.id


_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    label TEXT NOT NULL,
    credentials_enc TEXT NOT NULL,
    proxy_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugins (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    api TEXT NOT NULL,
    source TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    config_json TEXT NOT NULL,
    installed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_kv (
    plugin_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (plugin_id, key)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    platform TEXT NOT NULL,
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, id);
"""


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


class Storage:
    """Асинхронное хранилище (aiosqlite, WAL)."""

    def __init__(self, db_path: str | Path, secret_key_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._secret_key_path = Path(secret_key_path)
        self._db: aiosqlite.Connection | None = None
        self._fernet: Fernet | None = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise StorageError("storage is not open")
        return self._db

    @property
    def fernet(self) -> Fernet:
        if self._fernet is None:
            raise StorageError("storage is not open")
        return self._fernet

    async def open(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._fernet = Fernet(_load_or_create_key(self._secret_key_path))
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        await self.set_meta("schema_version", SCHEMA_VERSION)

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None
        self._fernet = None

    # --- meta -----------------------------------------------------------------

    async def get_meta(self, key: str) -> Any | None:
        cursor = await self.db.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return None if row is None else json.loads(row["value"])

    async def set_meta(self, key: str, value: Any) -> None:
        payload = json.dumps(value, ensure_ascii=False)
        await self.db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, payload),
        )
        await self.db.commit()

    # --- accounts -------------------------------------------------------------

    async def list_accounts(self) -> list[AccountRecord]:
        cursor = await self.db.execute("SELECT * FROM accounts ORDER BY created_at")
        return [self._account_from_row(row) for row in await cursor.fetchall()]

    async def get_account(self, account_id: str) -> AccountRecord | None:
        cursor = await self.db.execute("SELECT * FROM accounts WHERE id = ?", (account_id,))
        row = await cursor.fetchone()
        return None if row is None else self._account_from_row(row)

    async def upsert_account(self, record: AccountRecord) -> None:
        credentials = self._encrypt(json.dumps(to_wire(record.credentials), ensure_ascii=False))
        proxy_json = None if record.proxy is None else json.dumps(to_wire(record.proxy), ensure_ascii=False)
        await self.db.execute(
            """
            INSERT INTO accounts (id, platform, label, credentials_enc, proxy_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                platform = excluded.platform,
                label = excluded.label,
                credentials_enc = excluded.credentials_enc,
                proxy_json = excluded.proxy_json,
                updated_at = excluded.updated_at
            """,
            (
                record.id,
                record.platform.value,
                record.label,
                credentials,
                proxy_json,
                _iso(record.created_at),
                _iso(record.updated_at),
            ),
        )
        await self.db.commit()

    async def delete_account(self, account_id: str) -> bool:
        cursor = await self.db.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        await self.db.commit()
        return cursor.rowcount > 0

    def _account_from_row(self, row: aiosqlite.Row) -> AccountRecord:
        proxy_raw = row["proxy_json"]
        return AccountRecord(
            id=row["id"],
            platform=Platform(row["platform"]),
            label=row["label"],
            credentials=AccountCredentials.model_validate(json.loads(self._decrypt(row["credentials_enc"]))),
            proxy=None if proxy_raw is None else ProxyConfig.model_validate(json.loads(proxy_raw)),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    # --- plugins --------------------------------------------------------------

    async def list_plugins(self) -> list[PluginRecord]:
        cursor = await self.db.execute("SELECT * FROM plugins ORDER BY installed_at")
        return [self._plugin_from_row(row) for row in await cursor.fetchall()]

    async def get_plugin(self, plugin_id: str) -> PluginRecord | None:
        cursor = await self.db.execute("SELECT * FROM plugins WHERE id = ?", (plugin_id,))
        row = await cursor.fetchone()
        return None if row is None else self._plugin_from_row(row)

    async def upsert_plugin(self, record: PluginRecord) -> None:
        await self.db.execute(
            """
            INSERT INTO plugins (id, name, version, api, source, enabled, config_json, installed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                version = excluded.version,
                api = excluded.api,
                source = excluded.source,
                enabled = excluded.enabled,
                config_json = excluded.config_json,
                updated_at = excluded.updated_at
            """,
            (
                record.id,
                record.name,
                record.version,
                record.api,
                record.source.value,
                int(record.enabled),
                json.dumps(record.config, ensure_ascii=False),
                _iso(record.installed_at),
                _iso(record.updated_at),
            ),
        )
        await self.db.commit()

    async def delete_plugin(self, plugin_id: str, *, purge_data: bool) -> bool:
        cursor = await self.db.execute("DELETE FROM plugins WHERE id = ?", (plugin_id,))
        if purge_data:
            await self.db.execute("DELETE FROM plugin_kv WHERE plugin_id = ?", (plugin_id,))
        await self.db.commit()
        return cursor.rowcount > 0

    def _plugin_from_row(self, row: aiosqlite.Row) -> PluginRecord:
        return PluginRecord(
            id=row["id"],
            name=row["name"],
            version=row["version"],
            api=row["api"],
            source=PluginSource(row["source"]),
            enabled=bool(row["enabled"]),
            config=json.loads(row["config_json"]),
            installed_at=_parse_dt(row["installed_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    # --- plugin KV ------------------------------------------------------------

    async def kv_get(self, plugin_id: str, key: str) -> Any | None:
        cursor = await self.db.execute(
            "SELECT value_json FROM plugin_kv WHERE plugin_id = ? AND key = ?", (plugin_id, key)
        )
        row = await cursor.fetchone()
        return None if row is None else json.loads(row["value_json"])

    async def kv_set(self, plugin_id: str, key: str, value: Any) -> None:
        payload = json.dumps(value, ensure_ascii=False)
        await self.db.execute(
            "INSERT INTO plugin_kv (plugin_id, key, value_json) VALUES (?, ?, ?) "
            "ON CONFLICT(plugin_id, key) DO UPDATE SET value_json = excluded.value_json",
            (plugin_id, key, payload),
        )
        await self.db.commit()

    async def kv_delete(self, plugin_id: str, key: str) -> None:
        await self.db.execute("DELETE FROM plugin_kv WHERE plugin_id = ? AND key = ?", (plugin_id, key))
        await self.db.commit()

    async def kv_keys(self, plugin_id: str, prefix: str = "") -> list[str]:
        cursor = await self.db.execute(
            "SELECT key FROM plugin_kv WHERE plugin_id = ? AND key LIKE ? ORDER BY key",
            (plugin_id, f"{prefix}%"),
        )
        return [row["key"] for row in await cursor.fetchall()]

    async def kv_items(self, plugin_id: str) -> list[tuple[str, Any]]:
        cursor = await self.db.execute(
            "SELECT key, value_json FROM plugin_kv WHERE plugin_id = ? ORDER BY key", (plugin_id,)
        )
        return [(row["key"], json.loads(row["value_json"])) for row in await cursor.fetchall()]

    # --- история событий ------------------------------------------------------

    async def add_event(self, event: HubEvent) -> int:
        cursor = await self.db.execute(
            "INSERT INTO events (ts, platform, session_id, kind, payload_json) VALUES (?, ?, ?, ?, ?)",
            (
                _iso(event.ts),
                event.platform.value,
                event.session_id,
                event.kind.value,
                event.model_dump_json(),
            ),
        )
        await self.db.commit()
        return int(cursor.lastrowid or 0)

    async def list_events(
        self,
        *,
        session_id: str | None = None,
        kind: EventKind | None = None,
        limit: int = 100,
        cursor: int | None = None,
    ) -> tuple[list[HubEvent], int | None]:
        query = "SELECT id, payload_json FROM events WHERE 1=1"
        args: list[Any] = []
        if session_id is not None:
            query += " AND session_id = ?"
            args.append(session_id)
        if kind is not None:
            query += " AND kind = ?"
            args.append(kind.value)
        if cursor is not None:
            query += " AND id > ?"
            args.append(cursor)
        query += " ORDER BY id LIMIT ?"
        args.append(limit)
        cursor_obj = await self.db.execute(query, args)
        rows = list(await cursor_obj.fetchall())
        events = [HubEvent.model_validate_json(row["payload_json"]) for row in rows]
        next_cursor = int(rows[-1]["id"]) if len(rows) == limit else None
        return events, next_cursor

    async def purge_events(self, before: datetime) -> int:
        cursor = await self.db.execute("DELETE FROM events WHERE ts < ?", (_iso(before),))
        await self.db.commit()
        return cursor.rowcount

    async def clear_all(self) -> None:
        """Очищает аккаунты/плагины/KV (режим ``import_state`` = replace)."""
        await self.db.execute("DELETE FROM accounts")
        await self.db.execute("DELETE FROM plugins")
        await self.db.execute("DELETE FROM plugin_kv")
        await self.db.commit()

    # --- crypto ---------------------------------------------------------------

    def _encrypt(self, plaintext: str) -> str:
        return self.fernet.encrypt(plaintext.encode()).decode()

    def _decrypt(self, token: str) -> str:
        try:
            return self.fernet.decrypt(token.encode()).decode()
        except InvalidToken as exc:
            raise StorageError("cannot decrypt credentials: secret.key changed or database corrupted") from exc


def _load_or_create_key(path: Path) -> bytes:
    if path.is_file():
        return path.read_bytes().strip()
    path.parent.mkdir(parents=True, exist_ok=True)
    key = Fernet.generate_key()
    path.write_bytes(key + b"\n")
    with contextlib.suppress(OSError):  # Windows
        os.chmod(path, 0o600)
    return key


__all__ = [
    "SCHEMA_VERSION",
    "AccountRecord",
    "PluginRecord",
    "Storage",
    "StorageError",
]
