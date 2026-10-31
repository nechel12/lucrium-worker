"""Политика вызовов клиента aiosellers для панели и плагинов.

Панель — доверенный, но ограниченный UI: ей нужны только лоты/товары.
Плагины получают более широкий доступ, однако любые операции, двигающие
деньги (вывод, возвраты, кошельки), требуют явного ``permissions.finance``
в манифесте.
"""

from __future__ import annotations

ALLOWED_CLIENT_ROOTS = frozenset(
    {
        "account",
        "categories",
        "chats",
        "deals",
        "finance",
        "games",
        "items",
        "lots",
        "orders",
        "products",
        "reviews",
        "sales",
        "transactions",
    }
)

FINANCIAL_ROOTS = frozenset({"finance", "transactions"})
FINANCIAL_METHODS = frozenset({"orders.refund", "deals.cancel"})

_PANEL_METHODS = frozenset(
    {
        "lots.calc",
        "lots.chip_fields",
        "lots.delete",
        "lots.fields",
        "lots.hide_all",
        "lots.list_self",
        "lots.raise_category",
        "lots.save",
        "lots.upload_offer_image",
        "items.get",
        "items.list_self",
        "items.publish",
        "items.remove",
        "items.update",
        "products.get",
        "products.list",
        "products.my",
        "products.update_price",
    }
)
"""Только методы, которые реально нужны панели (лоты/товары)."""


def is_financial_method(method: str) -> bool:
    """True, если вызов двигает деньги/реквизиты и требует finance-opt-in."""
    return method.split(".", 1)[0] in FINANCIAL_ROOTS or method in FINANCIAL_METHODS


MEDIA_PARAMS = frozenset(
    {
        "attachment",
        "attachments",
        "audio",
        "avatar",
        "cover",
        "document",
        "file",
        "filename",
        "file_name",
        "file_path",
        "files",
        "filepath",
        "image",
        "images",
        "local_path",
        "path",
        "paths",
        "photo",
        "pictures",
        "photos",
        "video",
        "voice",
    }
)
"""Параметры aiosellers, которые библиотека открывает как файл на диске воркера.

``funpay/api/chats.py`` читает ``str``/``Path`` через ``read_bytes()``, ``playerok``
открывает ``str`` через ``open()`` (URL — только со схемой ``://``). Плагин, которому
выдаётся аккаунт, может прочитать любой файл VPS и отправить его картинкой в чат,
не имея разрешения ``network``: трафик идёт с легитимной сессии продавца.
"""


def unsafe_call_params(value: object, name: str | None = None) -> str | None:
    """Первая причина отклонить параметры вызова, либо ``None``.

    Медианные параметры принимают только числовой id уже загруженного файла и
    http(s)-URL: локальный путь указывал бы файл на диске воркера.
    """
    if isinstance(value, dict):
        for key, item in value.items():
            reason = unsafe_call_params(item, str(key))
            if reason is not None:
                return reason
        return None
    if isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            reason = unsafe_call_params(item, name)
            if reason is not None:
                return reason
        return None
    if isinstance(value, str):
        label = name if name is not None else "<root>"
        if "\x00" in value:
            return f"parameter {label!r} contains a NUL byte"
        if name is not None and name.lower() in MEDIA_PARAMS and not value.lower().startswith(("http://", "https://")):
            return f"parameter {label!r} must be an uploaded-file id or an http(s) URL, not a path on the worker's disk"
    return None


def is_panel_client_method(method: str) -> bool:
    return method in _PANEL_METHODS


__all__ = [
    "ALLOWED_CLIENT_ROOTS",
    "FINANCIAL_METHODS",
    "FINANCIAL_ROOTS",
    "MEDIA_PARAMS",
    "is_financial_method",
    "is_panel_client_method",
    "unsafe_call_params",
]
