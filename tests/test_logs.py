"""Тесты журнала: кольцевой буфер, фильтры и файловый дубль (спека §6.3)."""

from __future__ import annotations

import json
from pathlib import Path

from lucrium_protocol import LogLevel, LogScope

from lucrium_worker.logs import LogBuffer


def test_file_sink_writes_json_lines(tmp_path: Path) -> None:
    logs = LogBuffer()
    path = tmp_path / "logs" / "worker.log"
    logs.set_file(path)
    try:
        logs.add(LogLevel.INFO, "worker", "hello", {"a": 1})
        logs.add(LogLevel.ERROR, "test-plugin", "boom")
    finally:
        logs.close_file()
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["message"] == "hello"
    assert json.loads(lines[1])["level"] == "error"


def test_tail_returns_latest_entries() -> None:
    logs = LogBuffer()
    for index in range(500):
        logs.add(LogLevel.INFO, "worker", f"line {index}")
    tail = logs.tail(scope=LogScope.WORKER, limit=200)
    assert tail[0].message == "line 300"
    assert tail[-1].message == "line 499"


def test_file_rotation(tmp_path: Path) -> None:
    logs = LogBuffer(max_file_bytes=1024)
    path = tmp_path / "logs" / "worker.log"
    logs.set_file(path)
    try:
        for index in range(100):
            logs.add(LogLevel.INFO, "worker", f"line {index} " + "x" * 50)
    finally:
        logs.close_file()
    assert path.is_file()
    assert path.with_name("worker.log.1").is_file()


def test_query_filters_and_cursor() -> None:
    logs = LogBuffer()
    logs.add(LogLevel.DEBUG, "worker", "d")
    logs.add(LogLevel.INFO, "worker", "i")
    logs.add(LogLevel.WARNING, "test-plugin", "w")
    logs.add(LogLevel.ERROR, "test-plugin", "e")

    worker, _ = logs.query(scope=LogScope.WORKER)
    assert [entry.message for entry in worker] == ["d", "i"]

    plugin, _ = logs.query(scope=LogScope.PLUGIN, plugin_id="test-plugin")
    assert [entry.message for entry in plugin] == ["w", "e"]

    errors, _ = logs.query(level=LogLevel.WARNING)
    assert [entry.message for entry in errors] == ["w", "e"]

    _first, cursor = logs.query(limit=2)
    assert cursor is not None
    rest, _ = logs.query(limit=10, cursor=cursor)
    assert [entry.message for entry in rest] == ["w", "e"]
