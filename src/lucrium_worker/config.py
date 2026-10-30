"""Конфигурация воркера: TOML-файл + генерация pairing-токена (спека §3.1, §4).

Пример ``worker.toml``::

    [worker]
    host = "0.0.0.0"
    port = 8443
    token = "<64 hex>"

    [storage]
    data_dir = "/var/lib/lucrium"

    [tls]                     # опционально; без секции — plain ws:// (за TLS-прокси)
    certfile = "/etc/lucrium/cert.pem"
    keyfile = "/etc/lucrium/key.pem"

    [heartbeat]
    seconds = 15
"""

from __future__ import annotations

import contextlib
import os
import secrets
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path

DEFAULT_CONFIG_PATH = Path("/etc/lucrium/worker.toml")
DEFAULT_DATA_DIR = Path("/var/lib/lucrium")
TOKEN_BYTES = 32  # 256 бит


class ConfigError(Exception):
    """Конфиг отсутствует, не парсится или содержит некорректные значения."""


@dataclass(frozen=True, slots=True)
class TlsConfig:
    certfile: Path
    keyfile: Path


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    data_dir: Path = DEFAULT_DATA_DIR
    host: str = "0.0.0.0"
    port: int = 8443
    token: str = ""
    tls: TlsConfig | None = None
    heartbeat_seconds: float = 15.0
    log_level: str = "info"
    event_retention_days: int = 30
    plugin_memory_limit_mb: int = 512
    extra: dict[str, object] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not 1 <= self.port <= 65535:
            raise ConfigError(f"port out of range: {self.port}")
        if len(self.token) < 32:
            raise ConfigError("pairing token must be at least 32 characters (128+ bits)")
        if self.heartbeat_seconds <= 0:
            raise ConfigError("heartbeat.seconds must be positive")
        if self.event_retention_days <= 0:
            raise ConfigError("storage.event_retention_days must be positive")
        if self.plugin_memory_limit_mb <= 0:
            raise ConfigError("plugins.memory_limit_mb must be positive")

    @property
    def database_path(self) -> Path:
        return self.data_dir / "lucrium.db"

    @property
    def plugins_dir(self) -> Path:
        return self.data_dir / "plugins"

    @property
    def plugin_data_dir(self) -> Path:
        return self.data_dir / "plugin-data"

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def secret_key_path(self) -> Path:
        return self.data_dir / "secret.key"

    def with_data_dir(self, data_dir: Path) -> WorkerConfig:
        return replace(self, data_dir=data_dir)

    @classmethod
    def load(cls, path: str | Path) -> WorkerConfig:
        file = Path(path)
        try:
            raw = tomllib.loads(file.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ConfigError(f"config not found: {file}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"invalid TOML in {file}: {exc}") from exc

        worker = _table(raw, "worker")
        storage = _table(raw, "storage")
        heartbeat = _table(raw, "heartbeat")
        plugins = _table(raw, "plugins")
        tls_raw = raw.get("tls")

        data_dir = Path(os.environ.get("LUCRIUM_DATA_DIR", str(storage.get("data_dir", DEFAULT_DATA_DIR))))
        tls: TlsConfig | None = None
        if isinstance(tls_raw, dict):
            certfile = tls_raw.get("certfile")
            keyfile = tls_raw.get("keyfile")
            if not certfile or not keyfile:
                raise ConfigError("tls.certfile and tls.keyfile are both required")
            tls = TlsConfig(certfile=Path(str(certfile)), keyfile=Path(str(keyfile)))

        token = os.environ.get("LUCRIUM_TOKEN", str(worker.get("token", "")))
        try:
            return cls(
                data_dir=data_dir,
                host=str(worker.get("host", "0.0.0.0")),
                port=_as_int(worker.get("port", 8443), "worker.port"),
                token=token,
                tls=tls,
                heartbeat_seconds=_as_float(heartbeat.get("seconds", 15.0), "heartbeat.seconds"),
                log_level=str(worker.get("log_level", "info")),
                event_retention_days=_as_int(storage.get("event_retention_days", 30), "storage.event_retention_days"),
                plugin_memory_limit_mb=_as_int(plugins.get("memory_limit_mb", 512), "plugins.memory_limit_mb"),
            )
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"invalid config values in {file}: {exc}") from exc

    def save(self, path: str | Path) -> Path:
        """Пишет конфиг с правами 0600 (в нём pairing-токен)."""
        file = Path(path)
        file.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "[worker]",
            f'host = "{self.host}"',
            f"port = {self.port}",
            f'token = "{self.token}"',
            f'log_level = "{self.log_level}"',
            "",
            "[storage]",
            f'data_dir = "{self.data_dir.as_posix()}"',
            f"event_retention_days = {self.event_retention_days}",
            "",
            "[heartbeat]",
            f"seconds = {self.heartbeat_seconds}",
            "",
            "[plugins]",
            f"memory_limit_mb = {self.plugin_memory_limit_mb}",
        ]
        if self.tls is not None:
            lines += [
                "",
                "[tls]",
                f'certfile = "{self.tls.certfile.as_posix()}"',
                f'keyfile = "{self.tls.keyfile.as_posix()}"',
            ]
        file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        _chmod_600(file)
        return file


def generate_token() -> str:
    """Криптостойкий pairing-токен: 256 бит в hex (спека §3.1 — 128+ бит)."""
    return secrets.token_hex(TOKEN_BYTES)


def create_config(
    *,
    data_dir: Path = DEFAULT_DATA_DIR,
    host: str = "0.0.0.0",
    port: int = 8443,
    tls: TlsConfig | None = None,
) -> WorkerConfig:
    return WorkerConfig(data_dir=data_dir, host=host, port=port, token=generate_token(), tls=tls)


def ensure_directories(config: WorkerConfig) -> None:
    for directory in (config.data_dir, config.plugins_dir, config.plugin_data_dir, config.logs_dir):
        directory.mkdir(parents=True, exist_ok=True)


def _table(raw: dict[str, object], name: str) -> dict[str, object]:
    value = raw.get(name, {})
    if not isinstance(value, dict):
        raise ConfigError(f"[{name}] must be a table")
    return value


def _chmod_600(path: Path) -> None:
    with contextlib.suppress(OSError):  # Windows/FS без POSIX-прав
        path.chmod(0o600)


def _as_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ConfigError(f"{field} must be an integer")
    return int(value)


def _as_float(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise ConfigError(f"{field} must be a number")
    return float(value)


__all__ = [
    "DEFAULT_CONFIG_PATH",
    "DEFAULT_DATA_DIR",
    "ConfigError",
    "TlsConfig",
    "WorkerConfig",
    "create_config",
    "ensure_directories",
    "generate_token",
]
