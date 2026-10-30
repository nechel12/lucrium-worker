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

    def __init__(self, maxlen: int = 2000) -> None:
        self._entries: deque[LogEntry] = deque(maxlen=maxlen)
        self._seq = 0
        self._listener: Callable[[LogEntry], None] | None = None
        self._file: TextIO | None = None

    def set_listener(self, listener: Callable[[LogEntry], None] | None) -> None:
        """Колбэк на каждую запись (воркер шлёт ``plugin_log`` в панель)."""
        self._listener = listener

    def set_file(self, path: Path) -> None:
        """Дублирует журнал в локальный файл воркера (спека §6.3)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        self.close_file()
        self._file = path.open("a", encoding="utf-8", buffering=1)

    def close_file(self) -> None:
        if self._file is not None:
            with contextlib.suppress(OSError):
                self._file.close()
            self._file = None

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
            with contextlib.suppress(OSError):
                self._file.write(json.dumps(entry.model_dump(mode="json"), ensure_ascii=False) + "\n")
        if self._listener is not None:
            self._listener(entry)
        return entry

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
