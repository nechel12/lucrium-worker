"""Тесты сериализации событий aiosellers в протокол (docs/platforms.md §5)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from aiosellers import EventKind as AioEventKind
from aiosellers import HubEvent as AioHubEvent
from aiosellers import Platform as AioPlatform

from lucrium_worker.serialization import hub_event_to_protocol, to_jsonable


class Direction(StrEnum):
    IN = "IN"
    OUT = "OUT"


@dataclass(slots=True)
class Message:
    id: int
    text: str | None
    direction: Direction
    sent_at: datetime
    _client: object = field(default=None, repr=False)


@dataclass(slots=True)
class NewMessage:
    client: object
    chat: dict[str, object]
    message: Message
    type: str = "NEW_MESSAGE"


def test_to_jsonable_strips_client_and_converts_types() -> None:
    event = NewMessage(
        client=object(),
        chat={"id": 42, "path": Path("x")},
        message=Message(
            id=7,
            text="Привет",
            direction=Direction.IN,
            sent_at=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
        ),
    )
    payload = to_jsonable(event)
    assert "client" not in payload
    assert "type" not in payload["message"]
    assert payload["message"]["direction"] == "IN"
    assert payload["message"]["sent_at"] == "2026-01-15T12:00:00+00:00"
    assert payload["chat"]["path"] == "x"


def test_hub_event_conversion() -> None:
    raw = NewMessage(
        client=object(),
        chat={"id": 42},
        message=Message(id=7, text="Привет", direction=Direction.IN, sent_at=datetime.now(UTC)),
    )
    hub = AioHubEvent(platform=AioPlatform.FUNPAY, session_id="acc-1", kind=AioEventKind.MESSAGE, event=raw)
    converted = hub_event_to_protocol(hub)
    assert converted.platform.value == "funpay"
    assert converted.kind.value == "message"
    assert converted.session_id == "acc-1"
    assert converted.payload["message"]["text"] == "Привет"
    assert "client" not in converted.payload
