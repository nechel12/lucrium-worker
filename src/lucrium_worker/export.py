"""Экспорт/импорт состояния воркера (спека §4, §8).

Формат ``.lcs``::

    b"LCS2" | iterations(4, BE) | salt(16) |
        Fernet(PBKDF2-SHA256(passphrase, salt, iterations) → key, gzip(json))

Старые ``b"LCS1" | salt(16) | ...`` (200k итераций) продолжают читаться.

В дамп попадают аккаунты (креды), прокси, плагины с конфигами, KV, а также —
если возможно — файлы плагинов и история событий. Импорт сначала полностью
расшифровывает и валидирует дамп, и только потом применяет его.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import uuid
import zipfile
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from lucrium_protocol import (
    SUPPORTED_PLUGIN_API_VERSIONS,
    AccountInput,
    ExportStateResult,
    HubEvent,
    ImportMode,
    ImportStateResult,
    PluginSource,
    to_wire,
)
from lucrium_protocol.errors import redact_exception_text
from lucrium_protocol.manifest import ManifestPermissions
from pydantic import ValidationError

from .plugins.installer import InstallError, PluginInstaller
from .storage import AccountRecord, PluginRecord, Storage

MAGIC = b"LCS1"  # legacy: 200k iterations, соль сразу после magic
MAGIC_V2 = b"LCS2"
SALT_BYTES = 16
PBKDF2_ITERATIONS = 600_000
LEGACY_PBKDF2_ITERATIONS = 200_000
FORMAT_VERSION = 2
MAX_EXPORT_B64 = 64 * 1024 * 1024
MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_ARTIFACT_TOTAL = 96 * 1024 * 1024
MAX_EVENTS = 500_000

_ENTITY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_PLUGIN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{2,63}$")
_CACHE_PARTS = {"__pycache__", ".git", ".idea", ".vscode", ".mypy_cache", ".ruff_cache"}


class ExportError(Exception):
    """Дамп повреждён, пароль неверен или формат не поддерживается."""


@dataclass(slots=True)
class ParsedState:
    accounts: list[AccountRecord]
    plugins: list[tuple[PluginRecord, dict[str, Any]]]
    events: list[HubEvent]
    artifacts: dict[str, bytes]


async def export_state(
    storage: Storage,
    passphrase: str,
    *,
    include_history: bool = False,
    plugins_dir: Path | None = None,
) -> ExportStateResult:
    accounts = await storage.list_accounts()
    plugins = await storage.list_plugins()
    plugin_data: dict[str, dict[str, Any]] = {}
    for plugin in plugins:
        plugin_data[plugin.id] = dict(await storage.kv_items(plugin.id))

    artifacts: dict[str, str] = {}
    if plugins_dir is not None:
        for plugin in plugins:
            directory = plugins_dir / plugin.id
            if not directory.is_dir():
                continue
            # упаковка до 128 МиБ на плагин — в event loop она вешает heartbeat
            # и приём событий всех площадок
            archive = await asyncio.to_thread(_zip_plugin_dir, directory)
            if archive:
                artifacts[plugin.id] = base64.b64encode(archive).decode()

    events = await storage.list_all_events(limit=MAX_EVENTS + 1) if include_history else []
    if len(events) > MAX_EVENTS:
        raise ExportError(f"history is larger than {MAX_EVENTS} events; export without history")
    payload: dict[str, Any] = {
        "format": FORMAT_VERSION,
        "exported_at": datetime.now(UTC).isoformat(),
        "accounts": [_account_payload(account) for account in accounts],
        "plugins": [_plugin_payload(plugin, plugin_data.get(plugin.id, {})) for plugin in plugins],
        "artifacts": artifacts,
    }
    if include_history:
        payload["events"] = [_event_payload(event) for event in events]

    return await asyncio.to_thread(_encode_state_dump, payload, passphrase)


def _encode_state_dump(payload: dict[str, Any], passphrase: str) -> ExportStateResult:
    """CPU-тяжёлая часть экспорта (gzip/PBKDF2/Fernet) вне event loop."""
    raw = gzip.compress(json.dumps(payload, ensure_ascii=False).encode("utf-8"), mtime=0)
    blob = encrypt_blob(raw, passphrase)
    blob_b64 = base64.b64encode(blob).decode()
    if len(blob_b64) > MAX_EXPORT_B64:
        raise ExportError("state dump is too large; reduce plugin KV/history or export without artifacts/history")
    filename = f"lucrium-state-{datetime.now(UTC):%Y-%m-%d}.lcs"
    return ExportStateResult(
        filename=filename,
        blob_b64=blob_b64,
        sha256=hashlib.sha256(blob).hexdigest(),
        size_bytes=len(blob),
    )


def parse_state_dump(blob: bytes, passphrase: str) -> ParsedState:
    """Расшифровывает и валидирует дамп, не изменяя состояние воркера."""
    try:
        compressed = decrypt_blob(blob, passphrase)
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as gz:
            raw = gz.read(MAX_DECOMPRESSED_BYTES + 1)
        if len(raw) > MAX_DECOMPRESSED_BYTES:
            raise ExportError("state dump is too large when decompressed")
    except ExportError:
        raise
    except (OSError, EOFError, zlib.error, ValueError) as exc:
        raise ExportError(f"invalid state dump: {exc}") from exc
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ExportError(f"invalid state dump JSON: {exc}") from exc
    if not isinstance(payload, dict) or type(payload.get("format")) is not int:
        raise ExportError("unsupported state dump format")
    if payload["format"] not in {1, FORMAT_VERSION}:
        raise ExportError("unsupported state dump format")

    accounts_raw = payload.get("accounts", [])
    plugins_raw = payload.get("plugins", [])
    if not isinstance(accounts_raw, list) or not isinstance(plugins_raw, list):
        raise ExportError("state dump must contain accounts/plugins lists")

    accounts = [_parse_account(item) for item in accounts_raw]
    plugins: list[tuple[PluginRecord, dict[str, Any]]] = []
    plugin_ids: set[str] = set()
    for item in plugins_raw:
        record, data = _parse_plugin(item)
        if record.id in plugin_ids:
            raise ExportError(f"duplicate plugin id in state dump: {record.id}")
        plugin_ids.add(record.id)
        plugins.append((record, data))

    artifacts_raw = payload.get("artifacts", {})
    if not isinstance(artifacts_raw, dict):
        raise ExportError("artifacts must be an object")
    artifacts: dict[str, bytes] = {}
    total_artifact_bytes = 0
    for plugin_id, encoded in artifacts_raw.items():
        if not isinstance(plugin_id, str) or plugin_id not in plugin_ids:
            raise ExportError(f"artifact for unknown plugin: {plugin_id!r}")
        if not isinstance(encoded, str):
            raise ExportError(f"artifact for {plugin_id!r} must be base64 string")
        try:
            archive_bytes = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ExportError(f"artifact for {plugin_id!r} is not valid base64") from exc
        if len(archive_bytes) > MAX_ARTIFACT_BYTES:
            raise ExportError(f"artifact for {plugin_id!r} is too large")
        total_artifact_bytes += len(archive_bytes)
        if total_artifact_bytes > MAX_ARTIFACT_TOTAL:
            raise ExportError("plugin artifacts are too large")
        artifacts[plugin_id] = archive_bytes

    events_raw = payload.get("events", [])
    if not isinstance(events_raw, list):
        raise ExportError("events must be a list")
    if len(events_raw) > MAX_EVENTS:
        raise ExportError(f"too many events in state dump: {len(events_raw)}")
    events = [_parse_event(item) for item in events_raw]

    return ParsedState(accounts=accounts, plugins=plugins, events=events, artifacts=artifacts)


async def import_state(
    storage: Storage,
    blob: bytes,
    passphrase: str,
    *,
    mode: ImportMode = ImportMode.MERGE,
    plugins_dir: Path | None = None,
    parsed: ParsedState | None = None,
) -> ImportStateResult:
    state = parsed or parse_state_dump(blob, passphrase)
    installer = PluginInstaller(plugins_dir) if plugins_dir is not None and state.artifacts else None
    if installer is not None:
        for plugin_id, archive in state.artifacts.items():
            try:
                manifest = installer.inspect(archive)
            except InstallError as exc:
                raise ExportError(f"invalid plugin artifact {plugin_id!r}: {exc}") from exc
            if manifest.id != plugin_id:
                raise ExportError(f"plugin artifact id mismatch: archive has {manifest.id!r}, dump has {plugin_id!r}")

    backup_root = plugins_dir / f".import-backup-{uuid.uuid4().hex}" if installer is not None and plugins_dir else None
    moved: list[tuple[Path, Path]] = []
    installed: list[Path] = []
    try:
        if backup_root is not None and plugins_dir is not None:
            backup_root.mkdir(parents=True, exist_ok=False)
            for plugin_id in state.artifacts:
                target = plugins_dir / plugin_id
                if target.exists():
                    destination = backup_root / plugin_id
                    shutil.move(str(target), str(destination))
                    moved.append((target, destination))

        if installer is not None and plugins_dir is not None:
            for plugin_id, archive in state.artifacts.items():
                try:
                    installed_plugin = installer.install(archive, overwrite=True)
                except InstallError as exc:
                    raise ExportError(f"cannot restore plugin {plugin_id!r}: {exc}") from exc
                installed.append(installed_plugin.directory)

        try:
            async with storage.transaction():
                if mode is ImportMode.REPLACE:
                    await storage.clear_all(commit=False)
                for account in state.accounts:
                    await storage.upsert_account(account, commit=False)
                for record, data in state.plugins:
                    await storage.upsert_plugin(record, commit=False)
                    for key, value in data.items():
                        await storage.kv_set(record.id, key, value, commit=False)
                for event in state.events:
                    await storage.add_event(event, commit=False)
        except ExportError:
            raise
        except Exception as exc:
            raise ExportError(f"cannot apply state dump: {type(exc).__name__}") from exc
    except BaseException:
        # Откатываем файловую часть, чтобы не оставить полуустановленный плагин.
        for directory in installed:
            shutil.rmtree(directory, ignore_errors=True)
        for target, backup in moved:
            if backup.exists():
                shutil.move(str(backup), str(target))
        raise
    finally:
        if backup_root is not None:
            shutil.rmtree(backup_root, ignore_errors=True)

    return ImportStateResult(
        mode=mode,
        accounts=len(state.accounts),
        plugins=len(state.plugins),
        sessions_started=0,
    )


def encrypt_blob(raw: bytes, passphrase: str) -> bytes:
    salt = os.urandom(SALT_BYTES)
    cipher = Fernet(_derive_key(passphrase, salt, PBKDF2_ITERATIONS))
    return MAGIC_V2 + PBKDF2_ITERATIONS.to_bytes(4, "big") + salt + cipher.encrypt(raw)


def decrypt_blob(blob: bytes, passphrase: str) -> bytes:
    if blob.startswith(MAGIC_V2):
        header_size = len(MAGIC_V2) + 4
        if len(blob) < header_size + SALT_BYTES:
            raise ExportError("truncated state dump")
        iterations = int.from_bytes(blob[len(MAGIC_V2) : header_size], "big")
        if not 100_000 <= iterations <= 2_000_000:
            raise ExportError("invalid state dump KDF parameters")
        salt = blob[header_size : header_size + SALT_BYTES]
        ciphertext = blob[header_size + SALT_BYTES :]
    elif blob.startswith(MAGIC):
        header_size = len(MAGIC)
        iterations = LEGACY_PBKDF2_ITERATIONS
        salt = blob[header_size : header_size + SALT_BYTES]
        if len(salt) != SALT_BYTES:
            raise ExportError("truncated state dump")
        ciphertext = blob[header_size + SALT_BYTES :]
    else:
        raise ExportError("not a Lucrium state dump")
    cipher = Fernet(_derive_key(passphrase, salt, iterations))
    try:
        return cipher.decrypt(ciphertext)
    except InvalidToken as exc:
        raise ExportError("wrong passphrase or corrupted state dump") from exc


def _derive_key(passphrase: str, salt: bytes, iterations: int) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=iterations,
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))


def _zip_plugin_dir(directory: Path) -> bytes:
    buffer = io.BytesIO()
    total = 0
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(directory.rglob("*")):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(directory)
            if any(part in _CACHE_PARTS for part in relative.parts):
                continue
            if path.suffix in {".pyc", ".pyo"}:
                continue
            total += path.stat().st_size
            if total > MAX_ARTIFACT_BYTES * 4:
                raise ExportError(f"plugin {directory.name!r} is too large to export")
            archive.write(path, arcname=relative.as_posix())
    return buffer.getvalue()


def _account_payload(account: AccountRecord) -> dict[str, Any]:
    return {
        "id": account.id,
        "platform": account.platform.value,
        "label": account.label,
        "credentials": to_wire(account.credentials),
        "proxy": None if account.proxy is None else to_wire(account.proxy),
        "created_at": account.created_at.isoformat(),
        "updated_at": account.updated_at.isoformat(),
        "desired_running": account.desired_running,
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
        "permissions": None if plugin.permissions is None else plugin.permissions.model_dump(mode="json"),
        "data": data,
    }


def _event_payload(event: HubEvent) -> dict[str, Any]:
    return event.model_dump(mode="json")


def _parse_account(item: Any) -> AccountRecord:
    if not isinstance(item, dict):
        raise ExportError("account entry must be an object")
    try:
        account = AccountInput.model_validate(
            {
                "platform": item["platform"],
                "label": item["label"],
                "credentials": item["credentials"],
                "proxy": item.get("proxy"),
            }
        )
        account_id = str(item["id"])
        _validate_entity_id(account_id, "account")
        desired_running = item.get("desired_running", True)
        if not isinstance(desired_running, bool):
            raise ExportError("account.desired_running must be boolean")
        return AccountRecord(
            id=account_id,
            platform=account.platform,
            label=account.label,
            credentials=account.credentials,
            proxy=account.proxy,
            created_at=_aware_datetime(item["created_at"], "account.created_at"),
            updated_at=_aware_datetime(item["updated_at"], "account.updated_at"),
            desired_running=desired_running,
        )
    except (KeyError, TypeError, ValueError, ValidationError) as exc:
        raise ExportError(f"invalid account entry: {redact_exception_text(exc)}") from exc


def _parse_plugin(item: Any) -> tuple[PluginRecord, dict[str, Any]]:
    if not isinstance(item, dict):
        raise ExportError("plugin entry must be an object")
    try:
        plugin_id = str(item["id"])
        _validate_plugin_id(plugin_id)
        name = str(item["name"])
        if not name.strip() or len(name) > 120:
            raise ExportError("plugin.name must be 1-120 characters")
        version = str(item["version"])
        if not version.strip() or len(version) > 64:
            raise ExportError("plugin.version must be 1-64 characters")
        api = str(item["api"])
        if api not in SUPPORTED_PLUGIN_API_VERSIONS:
            raise ExportError(f"unsupported plugin api {api!r}")
        source = PluginSource(item["source"])
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ExportError("plugin.enabled must be boolean")
        config = item.get("config") or {}
        if not isinstance(config, dict):
            raise ExportError("plugin.config must be an object")
        data = item.get("data") or {}
        if not isinstance(data, dict):
            raise ExportError("plugin.data must be an object")
        permissions = item.get("permissions")
        record = PluginRecord(
            id=plugin_id,
            name=name,
            version=version,
            api=api,
            source=source,
            enabled=enabled,
            config=dict(config),
            installed_at=_aware_datetime(item["installed_at"], "plugin.installed_at"),
            updated_at=_aware_datetime(item["updated_at"], "plugin.updated_at"),
            permissions=None if permissions is None else ManifestPermissions.model_validate(permissions),
        )
        return record, dict(data)
    except (KeyError, TypeError, ValueError) as exc:
        raise ExportError(f"invalid plugin entry: {redact_exception_text(exc)}") from exc


def _parse_event(item: Any) -> HubEvent:
    try:
        event = HubEvent.model_validate(item)
    except (ValueError, ValidationError) as exc:
        raise ExportError(f"invalid event entry: {redact_exception_text(exc)}") from exc
    if event.ts.tzinfo is None:
        raise ExportError("event.ts must be timezone-aware")
    return event


def _aware_datetime(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ExportError(f"{field} must be ISO 8601") from exc
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _validate_entity_id(value: str, field: str) -> None:
    if not _ENTITY_ID_RE.fullmatch(value):
        raise ExportError(f"{field}.id must match {_ENTITY_ID_RE.pattern}")


def _validate_plugin_id(value: str) -> None:
    if not _PLUGIN_ID_RE.fullmatch(value):
        raise ExportError(f"plugin.id must match {_PLUGIN_ID_RE.pattern}")


__all__ = [
    "FORMAT_VERSION",
    "MAGIC",
    "ExportError",
    "ParsedState",
    "decrypt_blob",
    "encrypt_blob",
    "export_state",
    "import_state",
    "parse_state_dump",
]
