"""Аккаунты: CRUD поверх :class:`~lucrium_worker.storage.Storage`.

Валидация — моделями ``lucrium_protocol`` (``AccountInput``/``AccountCredentials``):
воркер не доверяет панели и перепроверяет всё на своей стороне.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime

from aiosellers import Capability as AioCapability
from aiosellers import capabilities_for
from lucrium_protocol import (
    AccountCredentials,
    AccountInput,
    AccountState,
    Capability,
    Platform,
    ProxyConfig,
    SessionStatus,
)

from .storage import AccountRecord, Storage


class AccountError(Exception):
    """Аккаунт не найден или запрос противоречит состоянию."""


def capabilities_of(platform: Platform) -> list[Capability]:
    """``aiosellers`` Flag → список capability протокола (для панели)."""
    flags = capabilities_for(platform.value)
    result: list[Capability] = []
    for member in AioCapability:
        if member is AioCapability.NONE or member not in flags:
            continue
        try:
            result.append(Capability(str(member.name).lower()))
        except ValueError:  # pragma: no cover - новая capability форка
            continue
    return result


class AccountService:
    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    async def list(self) -> list[AccountRecord]:
        return await self._storage.list_accounts()

    async def get(self, account_id: str) -> AccountRecord:
        record = await self._storage.get_account(account_id)
        if record is None:
            raise AccountError(f"account {account_id!r} not found")
        return record

    async def create(self, payload: AccountInput) -> AccountRecord:
        now = datetime.now(UTC)
        record = AccountRecord(
            id=f"acc-{secrets.token_hex(8)}",
            platform=payload.platform,
            label=payload.label,
            credentials=payload.credentials,
            proxy=payload.proxy,
            created_at=now,
            updated_at=now,
            desired_running=True,
        )
        await self._storage.upsert_account(record)
        return record

    async def update(
        self,
        account_id: str,
        *,
        label: str | None = None,
        credentials: AccountCredentials | None = None,
        proxy: ProxyConfig | None = None,
        clear_proxy: bool = False,
    ) -> AccountRecord:
        current = await self.get(account_id)
        if credentials is not None and credentials.platform is not current.platform:
            raise AccountError("credentials.platform must match account platform")
        updated = AccountRecord(
            id=current.id,
            platform=current.platform,
            label=label if label is not None else current.label,
            credentials=credentials if credentials is not None else current.credentials,
            proxy=None if clear_proxy else (proxy if proxy is not None else current.proxy),
            created_at=current.created_at,
            updated_at=datetime.now(UTC),
            desired_running=current.desired_running,
        )
        await self._storage.upsert_account(updated)
        return updated

    async def delete(self, account_id: str) -> AccountRecord:
        record = await self.get(account_id)
        await self._storage.delete_account(account_id)
        return record

    @staticmethod
    def to_state(
        record: AccountRecord,
        *,
        status: SessionStatus = SessionStatus.STOPPED,
        last_error: str | None = None,
        last_event_at: datetime | None = None,
        restarts: int = 0,
    ) -> AccountState:
        return AccountState(
            id=record.id,
            platform=record.platform,
            label=record.label,
            status=status,
            capabilities=capabilities_of(record.platform),
            proxy=record.proxy,
            last_error=last_error,
            created_at=record.created_at,
            updated_at=record.updated_at,
            last_event_at=last_event_at,
            restarts=restarts,
        )


__all__ = ["AccountError", "AccountService", "capabilities_of"]
