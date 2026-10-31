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


def is_panel_client_method(method: str) -> bool:
    return method in _PANEL_METHODS


__all__ = [
    "ALLOWED_CLIENT_ROOTS",
    "FINANCIAL_METHODS",
    "FINANCIAL_ROOTS",
    "is_financial_method",
    "is_panel_client_method",
]
