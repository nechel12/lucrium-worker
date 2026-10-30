"""Сериализация событий aiosellers в ``lucrium_protocol.HubEvent``.

Правила (docs/platforms.md §5): поля dataclass'а без ``client``/``_client``,
рекурсивно; enum → значение; datetime → ISO 8601; bytes → base64.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from aiosellers import HubEvent as AioHubEvent
from lucrium_protocol import EventKind, HubEvent, Platform

_SKIP_FIELDS = {"client", "_client"}


def to_jsonable(value: Any) -> Any:
    """Приводит произвольное значение из aiosellers к JSON-совместимому виду."""
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: to_jsonable(getattr(value, item.name))
            for item in fields(value)
            if item.name not in _SKIP_FIELDS and not item.name.startswith("_")
        }
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [to_jsonable(item) for item in value]
    return str(value)


def hub_event_to_protocol(hub: AioHubEvent) -> HubEvent:
    """``aiosellers.HubEvent`` → ``lucrium_protocol.HubEvent`` (payload без client)."""
    payload = to_jsonable(hub.event)
    if not isinstance(payload, dict):
        payload = {"value": payload}
    payload.pop("client", None)
    return HubEvent(
        platform=Platform(hub.platform.value),
        session_id=hub.session_id,
        kind=EventKind(hub.kind.value),
        payload=payload,
        ts=datetime.now(UTC),
    )


__all__ = ["hub_event_to_protocol", "to_jsonable"]
