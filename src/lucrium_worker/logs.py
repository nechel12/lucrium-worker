"""Кольцевой буфер журнала воркера для ``get_logs`` и события ``plugin_log``."""

from __future__ import annotations

import contextlib
import json
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

from lucrium_protocol import LogEntry, LogLevel, LogScope

_LEVEL_ORDER = [LogLevel.DEBUG, LogLevel.INFO, LogLevel.WARNING, LogLevel.ERROR]


class LogBuffer:
    """Потокобезопасный в рамках event loop буфер последних записей."""

    def __init__(self, maxlen: int = 2000, *, max_file_bytes: int = 10 * 1024 * 1024) -> None:
        self._entries: deque[LogEntry] = deque(maxlen=maxlen)
        self._seq = 0
        self._listener: Callable[[LogEntry], None] | None = None
        self._file: TextIO | None = None
        self._file_path: Path | None = None
        self._file_bytes = 0
        self._max_file_bytes = max(1024, max_file_bytes)

    def set_listener(self, listener: Callable[[LogEntry], None] | None) -> None:
        """Колбэк на каждую запись (воркер шлёт ``plugin_log`` в панель)."""
        self._listener = listener

    def set_file(self, path: Path) -> None:
        """Дублирует журнал в локальный файл воркера (спека §6.3)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            path.parent.chmod(0o700)
        self.close_file()
        self._file_path = path
        self._file = path.open("a", encoding="utf-8", buffering=1)
        with contextlib.suppress(OSError):
            path.chmod(0o600)
            self._file_bytes = path.stat().st_size

    def close_file(self) -> None:
        if self._file is not None:
            with contextlib.suppress(OSError):
                self._file.close()
            self._file = None
        self._file_path = None
        self._file_bytes = 0

    def add(
        self,
        level: LogLevel,
        source: str,
        message: str,
        fields: dict[str, Any] | None = None,
    ) -> LogEntry:
        self._seq += 1
        entry = LogEntry(
            seq=self._seq,
            ts=datetime.now(UTC),
            level=level,
            source=source,
            message=message[:8192],
            fields=dict(fields or {}),
        )
        self._entries.append(entry)
        if self._file is not None:
            line = json.dumps(entry.model_dump(mode="json"), ensure_ascii=False) + "\n"
            with contextlib.suppress(OSError):
                if self._file_bytes and self._file_bytes + len(line.encode()) > self._max_file_bytes:
                    self._rotate_file()
                if self._file is not None:
                    self._file.write(line)
                    self._file_bytes += len(line.encode())
            if self._file_path is not None:
                with contextlib.suppress(OSError):
                    self._file_path.chmod(0o600)
        if self._listener is not None:
            self._listener(entry)
        return entry

    def _rotate_file(self) -> None:
        if self._file_path is None:
            return
        if self._file is not None:
            with contextlib.suppress(OSError):
                self._file.close()
        backup = self._file_path.with_name(f"{self._file_path.name}.1")
        with contextlib.suppress(OSError):
            backup.unlink()
        with contextlib.suppress(OSError):
            self._file_path.rename(backup)
        self._file = self._file_path.open("a", encoding="utf-8", buffering=1)
        self._file_bytes = 0
        with contextlib.suppress(OSError):
            self._file_path.chmod(0o600)

    def tail(
        self,
        *,
        scope: LogScope | None = None,
        plugin_id: str | None = None,
        level: LogLevel | None = None,
        limit: int = 100,
    ) -> list[LogEntry]:
        """Последние ``limit`` записей (в хронологическом порядке) без cursor."""
        min_level = None if level is None else _LEVEL_ORDER.index(level)
        selected: list[LogEntry] = []
        for entry in reversed(self._entries):
            if scope is LogScope.WORKER and entry.source != "worker":
                continue
            if scope is LogScope.PLUGIN and entry.source != plugin_id:
                continue
            if min_level is not None and _LEVEL_ORDER.index(entry.level) < min_level:
                continue
            selected.append(entry)
            if len(selected) >= limit:
                break
        selected.reverse()
        return selected

    def query(
        self,
        *,
        scope: LogScope | None = None,
        plugin_id: str | None = None,
        level: LogLevel | None = None,
        limit: int = 100,
        cursor: int | None = None,
    ) -> tuple[list[LogEntry], int | None]:
        """Возвращает ``(entries, next_cursor)``; ``scope=None`` — все источники."""
        min_level = None if level is None else _LEVEL_ORDER.index(level)
        selected: list[LogEntry] = []
        for entry in self._entries:
            if cursor is not None and entry.seq <= cursor:
                continue
            if scope is LogScope.WORKER and entry.source != "worker":
                continue
            if scope is LogScope.PLUGIN and entry.source != plugin_id:
                continue
            if min_level is not None and _LEVEL_ORDER.index(entry.level) < min_level:
                continue
            selected.append(entry)
            if len(selected) >= limit:
                break
        next_cursor = selected[-1].seq if len(selected) == limit else None
        return selected, next_cursor

    def __len__(self) -> int:
        return len(self._entries)


__all__ = ["LogBuffer"]
