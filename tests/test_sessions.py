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


async def test_stop_notifies_status_change(tmp_path: Path) -> None:
    changes: list[bool] = []

    async def on_event(event: HubEvent) -> None:
        return None

    manager = SessionManager(
        data_dir=tmp_path,
        on_event=on_event,
        logs=LogBuffer(),
        on_change=lambda: changes.append(True),
    )
    manager._runtimes["acc-1"] = SessionRuntime(
        account_id="acc-1",
        platform=Platform.FUNPAY,
        status=SessionStatus.RUNNING,
    )
    await manager.stop("acc-1")
    assert changes == [True]


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


async def test_media_params_reject_paths_on_worker_disk(manager: SessionManager) -> None:
    """``image``/``photo`` как путь прочитал бы файл VPS и отправил его в чат."""
    chats = manager.resolve("acc-1").client.chats  # type: ignore[attr-defined]
    for params in (
        {"chat_id": 1, "image": "/var/lib/lucrium/secret.key"},
        {"chat_id": 1, "image": "worker.toml"},
        {"chat_id": 1, "image": r"C:\Windows\system32\config\sam"},
        {"chat_id": 1, "photo": "~/id_rsa"},
        {"chat_id": 1, "attachments": [{"path": "/etc/passwd"}]},
    ):
        with pytest.raises(SessionError, match="uploaded-file id or an http"):
            await manager.call("acc-1", "chats.send_message", params)
    assert chats.calls == []


async def test_media_params_accept_ids_and_urls(manager: SessionManager) -> None:
    await manager.call("acc-1", "chats.send_message", {"chat_id": 1, "image": 77})
    await manager.call("acc-1", "chats.send_message", {"chat_id": 1, "image": None})
    await manager.call("acc-1", "chats.send_message", {"chat_id": 1, "image": "HTTPS://example.com/a.png"})
    # текстовые параметры не режутся по форме значения: «/help» — легитимное сообщение
    await manager.call("acc-1", "chats.send_message", {"chat_id": 1, "text": "/help /var/log"})
    with pytest.raises(SessionError, match="NUL"):
        await manager.call("acc-1", "chats.send_message", {"chat_id": 1, "text": "a\x00b"})


async def test_financial_methods_require_opt_in(manager: SessionManager) -> None:
    client = manager.resolve("acc-1").client
    assert client is not None
    client.finance = SimpleNamespace(
        withdraw=lambda **_: {"withdrawn": True},
        save_wallets=lambda **_: {"saved": True},
    )
    client.orders = SimpleNamespace(refund=lambda **_: {"refunded": True})
    client.deals = SimpleNamespace(cancel=lambda **_: {"cancelled": True})

    for method in ("finance.withdraw", "finance.save_wallets", "orders.refund", "deals.cancel"):
        with pytest.raises(SessionError, match=r"permissions\.finance"):
            await manager.call("acc-1", method, {})

    assert await manager.call("acc-1", "finance.withdraw", {}, allow_finance=True) == {"withdrawn": True}
    assert await manager.call("acc-1", "orders.refund", {}, allow_finance=True) == {"refunded": True}


async def test_paid_and_destructive_methods_require_finance(manager: SessionManager) -> None:
    """Плагин без finance не тратит баланс и не сносит витрину."""
    client = manager.resolve("acc-1").client
    assert client is not None
    client.lots = SimpleNamespace(
        raise_category=lambda **_: {"raised": True},
        hide_all=lambda **_: {"hidden": True},
        delete=lambda **_: {"deleted": True},
    )
    client.items = SimpleNamespace(
        publish=lambda **_: {"published": True},
        set_premium_priority=lambda **_: {"premium": True},
    )
    client.games = SimpleNamespace(accept_agreement=lambda **_: {"accepted": True})

    for method in (
        "lots.raise_category",
        "lots.hide_all",
        "lots.delete",
        "items.publish",
        "items.set_premium_priority",
        "games.accept_agreement",
    ):
        with pytest.raises(SessionError, match=r"permissions\.finance"):
            await manager.call("acc-1", method, {})

    assert await manager.call("acc-1", "lots.raise_category", {}, allow_finance=True) == {"raised": True}


async def test_panel_listing_ops_do_not_need_finance(manager: SessionManager) -> None:
    """Панель по §9 правит свои лоты, но платные операции ей не открываются."""
    client = manager.resolve("acc-1").client
    assert client is not None
    client.items = SimpleNamespace(publish=lambda **_: {"ok": True}, update=lambda **_: {"ok": True})
    client.products = SimpleNamespace(update_price=lambda **_: {"ok": True})
    client.lots = SimpleNamespace(delete=lambda **_: {"ok": True}, raise_category=lambda **_: {"ok": True})

    for method in ("items.publish", "items.update", "products.update_price", "lots.delete"):
        assert await manager.call("acc-1", method, {}, panel=True) == {"ok": True}

    with pytest.raises(SessionError, match=r"permissions\.finance"):
        await manager.call("acc-1", "lots.raise_category", {}, panel=True)


async def test_rejected_credentials_stop_the_supervisor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Мёртвый golden_key не должен превращаться в бесконечный шторм запросов."""
    import asyncio

    from aiosellers.funpay.exceptions import UnauthorizedError

    from .conftest import make_account

    attempts: list[str] = []

    def reject_credentials(record: Any, state: Any) -> Any:
        del state
        attempts.append(record.id)
        raise UnauthorizedError("golden key rejected")

    monkeypatch.setattr("lucrium_worker.sessions.build_client", reject_credentials)

    async def on_event(event: HubEvent) -> None:
        return None

    manager = SessionManager(data_dir=tmp_path, on_event=on_event, logs=LogBuffer())
    runtime = await manager.start(make_account("acc-1"))
    for _ in range(20):
        await asyncio.sleep(0.05)
        if runtime.status is SessionStatus.ERROR:
            break

    assert runtime.status is SessionStatus.ERROR
    assert attempts == ["acc-1"]  # одна попытка, без ретраев
    assert "отвергла" in (runtime.last_error or "")
    await manager.stop("acc-1")
