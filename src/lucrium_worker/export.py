"""Экспорт/импорт состояния воркера (спека §4, §8).

Формат ``.lcs``::

    b"LCS1" | salt(16) | Fernet(PBKDF2(passphrase, salt, 200_000) → key, gzip(json))

В дамп попадают аккаунты (креды), прокси, плагины с конфигами и KV плагинов —
это позволяет поднять новый VPS из одного файла.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from lucrium_protocol import (
    AccountCredentials,
    ExportStateResult,
    ImportMode,
    ImportStateResult,
    Platform,
    PluginSource,
    ProxyConfig,
    to_wire,
)

from .storage import AccountRecord, PluginRecord, Storage

MAGIC = b"LCS1"
SALT_BYTES = 16
PBKDF2_ITERATIONS = 200_000
FORMAT_VERSION = 1


class ExportError(Exception):
    """Дамп повреждён, пароль неверен или формат не поддерживается."""


async def export_state(storage: Storage, passphrase: str) -> ExportStateResult:
    accounts = await storage.list_accounts()
    plugins = await storage.list_plugins()
    plugin_data: dict[str, dict[str, Any]] = {}
    for plugin in plugins:
        plugin_data[plugin.id] = dict(await storage.kv_items(plugin.id))

    payload = {
        "format": FORMAT_VERSION,
        "exported_at": datetime.now(UTC).isoformat(),
        "accounts": [_account_payload(account) for account in accounts],
        "plugins": [_plugin_payload(plugin, plugin_data.get(plugin.id, {})) for plugin in plugins],
    }
    raw = gzip.compress(json.dumps(payload, ensure_ascii=False).encode("utf-8"), mtime=0)
    blob = encrypt_blob(raw, passphrase)
    filename = f"lucrium-state-{datetime.now(UTC):%Y-%m-%d}.lcs"
    return ExportStateResult(
        filename=filename,
        blob_b64=base64.b64encode(blob).decode(),
        sha256=hashlib.sha256(blob).hexdigest(),
        size_bytes=len(blob),
    )


async def import_state(
    storage: Storage,
    blob: bytes,
    passphrase: str,
    *,
    mode: ImportMode = ImportMode.MERGE,
) -> ImportStateResult:
    try:
        payload = json.loads(gzip.decompress(decrypt_blob(blob, passphrase)))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ExportError(f"invalid state dump: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("format") != FORMAT_VERSION:
        raise ExportError("unsupported state dump format")

    accounts = payload.get("accounts", [])
    plugins = payload.get("plugins", [])
    if not isinstance(accounts, list) or not isinstance(plugins, list):
        raise ExportError("state dump must contain accounts/plugins lists")

    if mode is ImportMode.REPLACE:
        await storage.clear_all()

    imported_accounts = 0
    for item in accounts:
        account_record = _parse_account(item)
        await storage.upsert_account(account_record)
        imported_accounts += 1

    imported_plugins = 0
    for item in plugins:
        plugin_record, data = _parse_plugin(item)
        await storage.upsert_plugin(plugin_record)
        for key, value in data.items():
            await storage.kv_set(plugin_record.id, key, value)
        imported_plugins += 1

    return ImportStateResult(
        mode=mode,
        accounts=imported_accounts,
        plugins=imported_plugins,
        sessions_started=0,
    )


def encrypt_blob(raw: bytes, passphrase: str) -> bytes:
    salt = os.urandom(SALT_BYTES)
    cipher = Fernet(_derive_key(passphrase, salt))
    return MAGIC + salt + cipher.encrypt(raw)


def decrypt_blob(blob: bytes, passphrase: str) -> bytes:
    if not blob.startswith(MAGIC):
        raise ExportError("not a Lucrium state dump")
    salt = blob[len(MAGIC) : len(MAGIC) + SALT_BYTES]
    if len(salt) != SALT_BYTES:
        raise ExportError("truncated state dump")
    cipher = Fernet(_derive_key(passphrase, salt))
    try:
        return cipher.decrypt(blob[len(MAGIC) + SALT_BYTES :])
    except InvalidToken as exc:
        raise ExportError("wrong passphrase or corrupted state dump") from exc


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=PBKDF2_ITERATIONS,
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))


def _account_payload(account: AccountRecord) -> dict[str, Any]:
    return {
        "id": account.id,
        "platform": account.platform.value,
        "label": account.label,
        "credentials": to_wire(account.credentials),
        "proxy": None if account.proxy is None else to_wire(account.proxy),
        "created_at": account.created_at.isoformat(),
        "updated_at": account.updated_at.isoformat(),
    }


def _plugin_payload(plugin: PluginRecord, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": plugin.id,
        "name": plugin.name,
        "version": plugin.version,
        "api": plugin.api,
        "source": plugin.source.value,
        "enabled": plugin.enabled,
        "config": plugin.config,
        "installed_at": plugin.installed_at.isoformat(),
        "updated_at": plugin.updated_at.isoformat(),
        "data": data,
    }


def _parse_account(item: Any) -> AccountRecord:
    if not isinstance(item, dict):
        raise ExportError("account entry must be an object")
    try:
        return AccountRecord(
            id=str(item["id"]),
            platform=Platform(item["platform"]),
            label=str(item["label"]),
            credentials=AccountCredentials.model_validate(item["credentials"]),
            proxy=None if item.get("proxy") is None else ProxyConfig.model_validate(item["proxy"]),
            created_at=datetime.fromisoformat(str(item["created_at"])),
            updated_at=datetime.fromisoformat(str(item["updated_at"])),
        )
    except (KeyError, ValueError) as exc:
        raise ExportError(f"invalid account entry: {exc}") from exc


def _parse_plugin(item: Any) -> tuple[PluginRecord, dict[str, Any]]:
    if not isinstance(item, dict):
        raise ExportError("plugin entry must be an object")
    try:
        record = PluginRecord(
            id=str(item["id"]),
            name=str(item["name"]),
            version=str(item["version"]),
            api=str(item["api"]),
            source=PluginSource(item["source"]),
            enabled=bool(item["enabled"]),
            config=dict(item.get("config") or {}),
            installed_at=datetime.fromisoformat(str(item["installed_at"])),
            updated_at=datetime.fromisoformat(str(item["updated_at"])),
        )
        data = item.get("data") or {}
        if not isinstance(data, dict):
            raise ExportError("plugin data must be an object")
        return record, dict(data)
    except (KeyError, ValueError) as exc:
        raise ExportError(f"invalid plugin entry: {exc}") from exc


__all__ = [
    "FORMAT_VERSION",
    "MAGIC",
    "ExportError",
    "decrypt_blob",
    "encrypt_blob",
    "export_state",
    "import_state",
]
