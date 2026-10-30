"""Тесты SessionManager: вызов клиента по dotted-пути и резолв сессии."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from lucrium_protocol import HubEvent, Platform, SessionStatus

from lucrium_worker.logs import LogBuffer
from lucrium_worker.sessions import SessionError, SessionManager, SessionRuntime


class FakeChats:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def send_message(self, **params: Any) -> dict[str, Any]:
        self.calls.append(params)
        return {"id": 1, **params}


@pytest.fixture
def manager(tmp_path: Path) -> SessionManager:
    async def on_event(event: HubEvent) -> None:
        return None

    instance = SessionManager(data_dir=tmp_path, on_event=on_event, logs=LogBuffer())
    chats = FakeChats()
    instance._runtimes["acc-1"] = SessionRuntime(
        account_id="acc-1",
        platform=Platform.FUNPAY,
        status=SessionStatus.RUNNING,
        client=SimpleNamespace(chats=chats),  # type: ignore[arg-type]
    )
    return instance


async def test_call_resolves_client_and_serializes_result(manager: SessionManager) -> None:
    result = await manager.call("acc-1", "chats.send_message", {"chat_id": 42, "text": "hi"})
    assert result == {"id": 1, "chat_id": 42, "text": "hi"}
    assert manager.resolve("acc-1").client is not None


async def test_call_without_session_id_uses_single_running(manager: SessionManager) -> None:
    result = await manager.call(None, "chats.send_message", {"chat_id": 1, "text": "hi"})
    assert result["chat_id"] == 1


async def test_unknown_session_rejected(manager: SessionManager) -> None:
    with pytest.raises(SessionError, match="not running"):
        await manager.call("acc-missing", "chats.send_message", {})


async def test_private_and_unknown_roots_rejected(manager: SessionManager) -> None:
    with pytest.raises(SessionError, match="not allowed"):
        await manager.call("acc-1", "_require", {})
    with pytest.raises(SessionError, match="not allowed"):
        await manager.call("acc-1", "__class__", {})
    with pytest.raises(SessionError, match="private"):
        await manager.call("acc-1", "chats._client", {})
    with pytest.raises(SessionError, match="private"):
        await manager.call("acc-1", "chats._client._transport.request", {})


async def test_non_callable_method_rejected(manager: SessionManager) -> None:
    with pytest.raises(SessionError, match="not callable"):
        await manager.call("acc-1", "chats.calls", {})
