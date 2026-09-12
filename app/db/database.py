from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PortalsAmount = str | int | float
logger = logging.getLogger(__name__)


class ActiveTransferConflictError(RuntimeError):
    """An exact asset already has a non-terminal transfer."""


@dataclass(frozen=True, slots=True)
class Account:
    id: int
    owner_telegram_id: int
    telegram_account_id: int
    session_key: str
    username: str | None
    first_name: str | None
    phone: str | None
    created_at: str
    role: str = "TARGET"


@dataclass(frozen=True, slots=True)
class ActiveAccountPair:
    owner: Account | None
    target: Account | None
    owner_count: int
    target_count: int


@dataclass(frozen=True, slots=True)
class TransferJob:
    id: int
    owner_telegram_id: int
    market: str
    owner_account_id: int
    target_account_id: int
    asset_id: str | int | None
    display_name: str | None
    amount_text: str
    amount_atomic: int | None
    external_ref: str | int | None
    status: str
    phase: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    error_message: str | None
    result_metadata: dict[str, Any]
    batch_id: int | None = None


@dataclass(frozen=True, slots=True)
class PortalsTransferBatch:
    id: int
    owner_telegram_id: int
    owner_account_id: int
    target_account_id: int
    status: str
    total_count: int
    success_count: int
    failed_count: int
    ambiguous_count: int
    created_at: str
    started_at: str | None
    finished_at: str | None
    result_metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TonnelTransferBatch:
    id: int
    owner_telegram_id: int
    owner_account_id: int
    target_account_id: int
    status: str
    total_count: int
    success_count: int
    failed_count: int
    ambiguous_count: int
    created_at: str
    started_at: str | None
    finished_at: str | None
    progress_chat_id: int | None
    progress_message_id: int | None
    final_notification_sent: bool
    result_metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class MrktJob:
    id: int
    owner_telegram_id: int
    account_id: int
    market: str
    gift_id: str | int | None
    display_name: str | None
    price_ton: str
    price_nanotons: int | None
    status: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    error_message: str | None
    result_metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PortalsJob:
    id: int
    owner_telegram_id: int
    account_id: int
    market: str
    offer_id: str | int | None
    nft_id: str | int | None
    amount: PortalsAmount | None
    display_name: str | None
    status: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    error_message: str | None
    result_metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class OperationHistoryItem:
    operation_id: int
    market: str
    account_id: int
    display_name: str | None
    value_text: str | None
    status: str
    timestamp: str


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._path, timeout=10)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            with connection:
                yield connection
        finally:
            connection.close()

    async def initialize(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.parent.chmod(0o700)
        async with self._lock:
            await asyncio.to_thread(self._initialize_sync)
        self._path.chmod(0o600)

    def _initialize_sync(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_telegram_id INTEGER NOT NULL,
                    telegram_account_id INTEGER NOT NULL,
                    session_key TEXT NOT NULL UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    phone TEXT,
                    role TEXT NOT NULL DEFAULT 'TARGET',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (owner_telegram_id, telegram_account_id)
                )
                """
            )
            self._ensure_column(
                connection, "accounts", "role", "TEXT NOT NULL DEFAULT 'TARGET'"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS active_account_pairs (
                    owner_telegram_id INTEGER PRIMARY KEY,
                    owner_account_id INTEGER,
                    target_account_id INTEGER,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS portals_transfer_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_telegram_id INTEGER NOT NULL,
                    owner_account_id INTEGER NOT NULL,
                    target_account_id INTEGER NOT NULL,
                    confirmation_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_count INTEGER NOT NULL,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    ambiguous_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TEXT,
                    finished_at TEXT,
                    result_metadata TEXT NOT NULL DEFAULT '{}',
                    UNIQUE (owner_telegram_id, confirmation_key)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portals_transfer_batches_owner
                ON portals_transfer_batches (owner_telegram_id, created_at, id)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tonnel_transfer_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_telegram_id INTEGER NOT NULL,
                    owner_account_id INTEGER NOT NULL,
                    target_account_id INTEGER NOT NULL,
                    confirmation_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_count INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    ambiguous_count INTEGER NOT NULL DEFAULT 0,
                    progress_chat_id INTEGER,
                    progress_message_id INTEGER,
                    final_notification_sent INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TEXT,
                    finished_at TEXT,
                    result_metadata TEXT NOT NULL DEFAULT '{}',
                    UNIQUE (owner_telegram_id, confirmation_key)
                )
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tonnel_active_batch_pair
                ON tonnel_transfer_batches (
                    owner_telegram_id, owner_account_id, target_account_id
                )
                WHERE status IN ('PENDING', 'RUNNING')
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tonnel_transfer_batches_owner
                ON tonnel_transfer_batches (owner_telegram_id, created_at, id)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS transfer_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_telegram_id INTEGER NOT NULL,
                    market TEXT NOT NULL,
                    owner_account_id INTEGER NOT NULL,
                    target_account_id INTEGER NOT NULL,
                    asset_id_json TEXT,
                    display_name TEXT,
                    amount_text TEXT NOT NULL,
                    amount_atomic INTEGER,
                    external_ref_json TEXT,
                    status TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TEXT,
                    finished_at TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    result_metadata TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_transfer_jobs_owner
                ON transfer_jobs (owner_telegram_id, created_at, id)
                """
            )
            self._ensure_column(connection, "transfer_jobs", "confirmation_key", "TEXT")
            self._ensure_column(connection, "transfer_jobs", "batch_id", "INTEGER")
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_transfer_jobs_confirmation
                ON transfer_jobs (owner_telegram_id, market, confirmation_key)
                WHERE confirmation_key IS NOT NULL
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_transfer_jobs_batch
                ON transfer_jobs (batch_id, id)
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_portals_active_transfer_asset
                ON transfer_jobs (
                    owner_telegram_id, owner_account_id, target_account_id,
                    asset_id_json
                )
                WHERE market = 'portals'
                  AND status IN ('QUEUED', 'PENDING', 'RUNNING')
                  AND asset_id_json IS NOT NULL
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tonnel_active_transfer_asset
                ON transfer_jobs (
                    owner_telegram_id, owner_account_id, target_account_id,
                    asset_id_json
                )
                WHERE market = 'tonnel'
                  AND status IN ('QUEUED', 'PENDING', 'RUNNING')
                  AND asset_id_json IS NOT NULL
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS mrkt_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_telegram_id INTEGER NOT NULL,
                    account_id INTEGER NOT NULL,
                    market TEXT NOT NULL DEFAULT 'mrkt',
                    gift_id_json TEXT,
                    display_name TEXT,
                    price_ton TEXT NOT NULL,
                    price_nanotons INTEGER,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TEXT,
                    finished_at TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    result_metadata TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_mrkt_jobs_owner
                ON mrkt_jobs (owner_telegram_id, created_at, id)
                """
            )
            self._ensure_column(connection, "mrkt_jobs", "display_name", "TEXT")
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_mrkt_jobs_account_status
                ON mrkt_jobs (owner_telegram_id, account_id, status)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS portals_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_telegram_id INTEGER NOT NULL,
                    account_id INTEGER NOT NULL,
                    market TEXT NOT NULL DEFAULT 'portals',
                    offer_id_json TEXT,
                    nft_id_json TEXT,
                    amount_json TEXT,
                    display_name TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TEXT,
                    finished_at TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    result_metadata TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portals_jobs_owner
                ON portals_jobs (owner_telegram_id, created_at, id)
                """
            )
            self._ensure_column(connection, "portals_jobs", "display_name", "TEXT")
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portals_jobs_account_status
                ON portals_jobs (owner_telegram_id, account_id, status)
                """
            )

    async def save_account(
        self,
        *,
        owner_telegram_id: int,
        telegram_account_id: int,
        session_key: str,
        username: str | None,
        first_name: str | None,
        phone: str | None,
        role: str = "TARGET",
    ) -> str | None:
        """Insert/update an account and return the replaced session key, if any."""
        async with self._lock:
            return await asyncio.to_thread(
                self._save_account_sync,
                owner_telegram_id,
                telegram_account_id,
                session_key,
                username,
                first_name,
                phone,
                self._validate_account_role(role),
            )

    def _save_account_sync(
        self,
        owner_telegram_id: int,
        telegram_account_id: int,
        session_key: str,
        username: str | None,
        first_name: str | None,
        phone: str | None,
        role: str,
    ) -> str | None:
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT session_key FROM accounts
                WHERE owner_telegram_id = ? AND telegram_account_id = ?
                """,
                (owner_telegram_id, telegram_account_id),
            ).fetchone()
            connection.execute(
                """
                INSERT INTO accounts (
                    owner_telegram_id, telegram_account_id, session_key,
                    username, first_name, phone, role
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(owner_telegram_id, telegram_account_id) DO UPDATE SET
                    session_key = excluded.session_key,
                    username = excluded.username,
                    first_name = excluded.first_name,
                    phone = excluded.phone,
                    role = excluded.role,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    owner_telegram_id,
                    telegram_account_id,
                    session_key,
                    username,
                    first_name,
                    phone,
                    role,
                ),
            )
            account_row = connection.execute(
                """
                SELECT id FROM accounts
                WHERE owner_telegram_id = ? AND telegram_account_id = ?
                """,
                (owner_telegram_id, telegram_account_id),
            ).fetchone()
            assert account_row is not None
            account_id = int(account_row["id"])
            pair = connection.execute(
                """
                SELECT owner_account_id, target_account_id
                FROM active_account_pairs WHERE owner_telegram_id = ?
                """,
                (owner_telegram_id,),
            ).fetchone()
            active_owner_id = (
                account_id
                if role == "OWNER"
                else (
                    None
                    if pair is not None and pair["owner_account_id"] == account_id
                    else pair["owner_account_id"] if pair is not None else None
                )
            )
            active_target_id = (
                account_id
                if role == "TARGET"
                else (
                    None
                    if pair is not None and pair["target_account_id"] == account_id
                    else pair["target_account_id"] if pair is not None else None
                )
            )
            connection.execute(
                """
                INSERT INTO active_account_pairs (
                    owner_telegram_id, owner_account_id, target_account_id
                ) VALUES (?, ?, ?)
                ON CONFLICT(owner_telegram_id) DO UPDATE SET
                    owner_account_id = excluded.owner_account_id,
                    target_account_id = excluded.target_account_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (owner_telegram_id, active_owner_id, active_target_id),
            )
            return str(existing["session_key"]) if existing else None

    async def list_accounts(self, owner_telegram_id: int) -> list[Account]:
        async with self._lock:
            rows = await asyncio.to_thread(self._list_accounts_sync, owner_telegram_id)
        return [self._row_to_account(row) for row in rows]

    async def list_accounts_by_role(
        self, owner_telegram_id: int, role: str
    ) -> list[Account]:
        normalized_role = self._validate_account_role(role)
        async with self._lock:
            rows = await asyncio.to_thread(
                self._list_accounts_by_role_sync,
                owner_telegram_id,
                normalized_role,
            )
        return [self._row_to_account(row) for row in rows]

    def _list_accounts_by_role_sync(
        self, owner_telegram_id: int, role: str
    ) -> list[sqlite3.Row]:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT * FROM accounts
                WHERE owner_telegram_id = ? AND role = ?
                ORDER BY created_at, id
                """,
                (owner_telegram_id, role),
            ).fetchall()

    def _list_accounts_sync(self, owner_telegram_id: int) -> list[sqlite3.Row]:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT * FROM accounts
                WHERE owner_telegram_id = ?
                ORDER BY created_at, id
                """,
                (owner_telegram_id,),
            ).fetchall()

    async def list_session_keys(self) -> set[str]:
        async with self._lock:
            rows = await asyncio.to_thread(self._list_session_keys_sync)
        return {str(row["session_key"]) for row in rows}

    def _list_session_keys_sync(self) -> list[sqlite3.Row]:
        with self._connect() as connection:
            return connection.execute("SELECT session_key FROM accounts").fetchall()

    async def get_account(
        self, account_id: int, owner_telegram_id: int
    ) -> Account | None:
        async with self._lock:
            row = await asyncio.to_thread(
                self._get_account_sync, account_id, owner_telegram_id
            )
        return self._row_to_account(row) if row else None

    def _get_account_sync(
        self, account_id: int, owner_telegram_id: int
    ) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                "SELECT * FROM accounts WHERE id = ? AND owner_telegram_id = ?",
                (account_id, owner_telegram_id),
            ).fetchone()

    async def delete_account(self, account_id: int, owner_telegram_id: int) -> bool:
        async with self._lock:
            return await asyncio.to_thread(
                self._delete_account_sync, account_id, owner_telegram_id
            )

    def _delete_account_sync(self, account_id: int, owner_telegram_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM accounts WHERE id = ? AND owner_telegram_id = ?",
                (account_id, owner_telegram_id),
            )
            if cursor.rowcount == 1:
                connection.execute(
                    """
                    UPDATE active_account_pairs SET
                        owner_account_id = CASE
                            WHEN owner_account_id = ? THEN NULL
                            ELSE owner_account_id END,
                        target_account_id = CASE
                            WHEN target_account_id = ? THEN NULL
                            ELSE target_account_id END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE owner_telegram_id = ?
                    """,
                    (account_id, account_id, owner_telegram_id),
                )
            return cursor.rowcount == 1

    async def get_active_account_pair(
        self, owner_telegram_id: int
    ) -> ActiveAccountPair:
        async with self._lock:
            owner_row, target_row, owner_count, target_count = await asyncio.to_thread(
                self._get_active_account_pair_sync, owner_telegram_id
            )
        return ActiveAccountPair(
            owner=self._row_to_account(owner_row) if owner_row is not None else None,
            target=self._row_to_account(target_row) if target_row is not None else None,
            owner_count=owner_count,
            target_count=target_count,
        )

    def _get_active_account_pair_sync(
        self, owner_telegram_id: int
    ) -> tuple[sqlite3.Row | None, sqlite3.Row | None, int, int]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM accounts WHERE owner_telegram_id = ?
                ORDER BY created_at DESC, id DESC
                """,
                (owner_telegram_id,),
            ).fetchall()
            owners = [row for row in rows if row["role"] == "OWNER"]
            targets = [row for row in rows if row["role"] == "TARGET"]
            pair = connection.execute(
                """
                SELECT owner_account_id, target_account_id
                FROM active_account_pairs WHERE owner_telegram_id = ?
                """,
                (owner_telegram_id,),
            ).fetchone()

            def selected(
                candidates: list[sqlite3.Row], column: str
            ) -> sqlite3.Row | None:
                selected_id = pair[column] if pair is not None else None
                explicit = next(
                    (row for row in candidates if row["id"] == selected_id), None
                )
                if explicit is not None:
                    return explicit
                return candidates[0] if len(candidates) == 1 else None

            owner = selected(owners, "owner_account_id")
            target = selected(targets, "target_account_id")
            if owner is not None or target is not None:
                connection.execute(
                    """
                    INSERT INTO active_account_pairs (
                        owner_telegram_id, owner_account_id, target_account_id
                    ) VALUES (?, ?, ?)
                    ON CONFLICT(owner_telegram_id) DO UPDATE SET
                        owner_account_id = excluded.owner_account_id,
                        target_account_id = excluded.target_account_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        owner_telegram_id,
                        owner["id"] if owner is not None else None,
                        target["id"] if target is not None else None,
                    ),
                )
            return owner, target, len(owners), len(targets)

    async def set_active_account(
        self, account_id: int, owner_telegram_id: int, role: str
    ) -> bool:
        normalized_role = self._validate_account_role(role)
        async with self._lock:
            return await asyncio.to_thread(
                self._set_active_account_sync,
                account_id,
                owner_telegram_id,
                normalized_role,
            )

    def _set_active_account_sync(
        self, account_id: int, owner_telegram_id: int, role: str
    ) -> bool:
        with self._connect() as connection:
            account = connection.execute(
                """
                SELECT id FROM accounts
                WHERE id = ? AND owner_telegram_id = ? AND role = ?
                """,
                (account_id, owner_telegram_id, role),
            ).fetchone()
            if account is None:
                return False
            pair = connection.execute(
                """
                SELECT owner_account_id, target_account_id
                FROM active_account_pairs WHERE owner_telegram_id = ?
                """,
                (owner_telegram_id,),
            ).fetchone()
            owner_id = account_id if role == "OWNER" else (
                pair["owner_account_id"] if pair is not None else None
            )
            target_id = account_id if role == "TARGET" else (
                pair["target_account_id"] if pair is not None else None
            )
            connection.execute(
                """
                INSERT INTO active_account_pairs (
                    owner_telegram_id, owner_account_id, target_account_id
                ) VALUES (?, ?, ?)
                ON CONFLICT(owner_telegram_id) DO UPDATE SET
                    owner_account_id = excluded.owner_account_id,
                    target_account_id = excluded.target_account_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (owner_telegram_id, owner_id, target_id),
            )
            return True

    async def create_mrkt_job(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        gift_id: str | int | None,
        price_ton: str,
    ) -> MrktJob:
        gift_id_json = self._encode_gift_id(gift_id)
        async with self._lock:
            row = await asyncio.to_thread(
                self._create_mrkt_job_sync,
                owner_telegram_id,
                account_id,
                gift_id_json,
                price_ton,
            )
        return self._row_to_mrkt_job(row)

    def _create_mrkt_job_sync(
        self,
        owner_telegram_id: int,
        account_id: int,
        gift_id_json: str | None,
        price_ton: str,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO mrkt_jobs (
                    owner_telegram_id, account_id, market, gift_id_json,
                    price_ton, status
                ) VALUES (?, ?, 'mrkt', ?, ?, 'PENDING')
                """,
                (owner_telegram_id, account_id, gift_id_json, price_ton),
            )
            row = connection.execute(
                "SELECT * FROM mrkt_jobs WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
            assert row is not None
            return row

    async def create_portals_transfer_batch(
        self,
        *,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        assets: list[tuple[str | int, str]],
        amount_text: str,
        confirmation_key: str,
    ) -> tuple[PortalsTransferBatch, list[TransferJob], bool]:
        if not assets:
            raise ValueError("A Portals batch requires at least one NFT")
        encoded_assets: list[tuple[str, str]] = []
        seen: set[tuple[type[object], object]] = set()
        for asset_id, display_name in assets:
            encoded = self._encode_opaque_id(asset_id, "Transfer asset")
            assert encoded is not None
            key = (type(asset_id), asset_id)
            if key in seen:
                raise ValueError("A Portals batch contains a duplicate NFT")
            seen.add(key)
            encoded_assets.append((encoded, display_name[:80]))
        async with self._lock:
            batch_row, job_rows, created = await asyncio.to_thread(
                self._create_portals_transfer_batch_sync,
                owner_telegram_id,
                owner_account_id,
                target_account_id,
                encoded_assets,
                amount_text[:40],
                confirmation_key,
            )
        return (
            self._row_to_portals_transfer_batch(batch_row),
            [self._row_to_transfer_job(row) for row in job_rows],
            created,
        )

    def _create_portals_transfer_batch_sync(
        self,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        assets: list[tuple[str, str]],
        amount_text: str,
        confirmation_key: str,
    ) -> tuple[sqlite3.Row, list[sqlite3.Row], bool]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT * FROM portals_transfer_batches
                WHERE owner_telegram_id = ? AND confirmation_key = ?
                LIMIT 1
                """,
                (owner_telegram_id, confirmation_key),
            ).fetchone()
            if existing is not None:
                rows = connection.execute(
                    "SELECT * FROM transfer_jobs WHERE batch_id = ? ORDER BY id",
                    (existing["id"],),
                ).fetchall()
                return existing, rows, False

            for asset_json, _ in assets:
                conflict = connection.execute(
                    """
                    SELECT id FROM transfer_jobs
                    WHERE owner_telegram_id = ? AND market = 'portals'
                      AND owner_account_id = ? AND target_account_id = ?
                      AND asset_id_json = ?
                      AND status IN ('QUEUED', 'PENDING', 'RUNNING')
                    LIMIT 1
                    """,
                    (
                        owner_telegram_id,
                        owner_account_id,
                        target_account_id,
                        asset_json,
                    ),
                ).fetchone()
                if conflict is not None:
                    raise ActiveTransferConflictError(
                        "The selected NFT already has an active transfer"
                    )

            cursor = connection.execute(
                """
                INSERT INTO portals_transfer_batches (
                    owner_telegram_id, owner_account_id, target_account_id,
                    confirmation_key, status, total_count
                ) VALUES (?, ?, ?, ?, 'PENDING', ?)
                """,
                (
                    owner_telegram_id,
                    owner_account_id,
                    target_account_id,
                    confirmation_key,
                    len(assets),
                ),
            )
            assert cursor.lastrowid is not None
            batch_id = int(cursor.lastrowid)
            for asset_json, display_name in assets:
                child_key = hashlib.sha256(
                    f"{confirmation_key}:{asset_json}".encode()
                ).hexdigest()
                connection.execute(
                    """
                    INSERT INTO transfer_jobs (
                        owner_telegram_id, market, owner_account_id,
                        target_account_id, asset_id_json, display_name,
                        amount_text, confirmation_key, batch_id, status, phase
                    ) VALUES (?, 'portals', ?, ?, ?, ?, ?, ?, ?, 'QUEUED', 'VALIDATING')
                    """,
                    (
                        owner_telegram_id,
                        owner_account_id,
                        target_account_id,
                        asset_json,
                        display_name,
                        amount_text,
                        child_key,
                        batch_id,
                    ),
                )
            batch_row = connection.execute(
                "SELECT * FROM portals_transfer_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            assert batch_row is not None
            job_rows = connection.execute(
                "SELECT * FROM transfer_jobs WHERE batch_id = ? ORDER BY id",
                (batch_id,),
            ).fetchall()
            return batch_row, job_rows, True

    async def get_portals_transfer_batch(
        self, batch_id: int, owner_telegram_id: int | None = None
    ) -> PortalsTransferBatch | None:
        async with self._lock:
            row = await asyncio.to_thread(
                self._get_portals_transfer_batch_sync,
                batch_id,
                owner_telegram_id,
            )
        return self._row_to_portals_transfer_batch(row) if row is not None else None

    def _get_portals_transfer_batch_sync(
        self, batch_id: int, owner_telegram_id: int | None
    ) -> sqlite3.Row | None:
        with self._connect() as connection:
            if owner_telegram_id is None:
                return connection.execute(
                    "SELECT * FROM portals_transfer_batches WHERE id = ?",
                    (batch_id,),
                ).fetchone()
            return connection.execute(
                """
                SELECT * FROM portals_transfer_batches
                WHERE id = ? AND owner_telegram_id = ?
                """,
                (batch_id, owner_telegram_id),
            ).fetchone()

    async def list_transfer_jobs_by_batch(self, batch_id: int) -> list[TransferJob]:
        async with self._lock:
            rows = await asyncio.to_thread(
                self._list_transfer_jobs_by_batch_sync, batch_id
            )
        return [self._row_to_transfer_job(row) for row in rows]

    def _list_transfer_jobs_by_batch_sync(self, batch_id: int) -> list[sqlite3.Row]:
        with self._connect() as connection:
            return connection.execute(
                "SELECT * FROM transfer_jobs WHERE batch_id = ? ORDER BY id",
                (batch_id,),
            ).fetchall()

    async def claim_portals_transfer_batch(
        self, batch_id: int
    ) -> PortalsTransferBatch | None:
        async with self._lock:
            row = await asyncio.to_thread(
                self._claim_portals_transfer_batch_sync, batch_id
            )
        return self._row_to_portals_transfer_batch(row) if row is not None else None

    def _claim_portals_transfer_batch_sync(self, batch_id: int) -> sqlite3.Row | None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE portals_transfer_batches
                SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'PENDING'
                """,
                (batch_id,),
            )
            if cursor.rowcount != 1:
                return None
            return connection.execute(
                "SELECT * FROM portals_transfer_batches WHERE id = ?", (batch_id,)
            ).fetchone()

    async def finish_portals_transfer_batch(
        self,
        batch_id: int,
        *,
        status: str,
        success_count: int,
        failed_count: int,
        ambiguous_count: int,
        result_metadata: dict[str, Any],
    ) -> PortalsTransferBatch:
        metadata_json = json.dumps(
            result_metadata, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        async with self._lock:
            row = await asyncio.to_thread(
                self._finish_portals_transfer_batch_sync,
                batch_id,
                status,
                success_count,
                failed_count,
                ambiguous_count,
                metadata_json,
            )
        return self._row_to_portals_transfer_batch(row)

    def _finish_portals_transfer_batch_sync(
        self,
        batch_id: int,
        status: str,
        success_count: int,
        failed_count: int,
        ambiguous_count: int,
        result_metadata: str,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE portals_transfer_batches
                SET status = ?, success_count = ?, failed_count = ?,
                    ambiguous_count = ?, finished_at = CURRENT_TIMESTAMP,
                    result_metadata = ?
                WHERE id = ? AND status IN ('PENDING', 'RUNNING')
                """,
                (
                    status[:20],
                    success_count,
                    failed_count,
                    ambiguous_count,
                    result_metadata,
                    batch_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM portals_transfer_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("Portals transfer batch disappeared")
            return row

    async def finish_queued_transfer_job(
        self, job_id: int, *, error_code: str, error_message: str
    ) -> TransferJob:
        async with self._lock:
            row = await asyncio.to_thread(
                self._finish_queued_transfer_job_sync,
                job_id,
                error_code,
                error_message,
            )
        return self._row_to_transfer_job(row)

    def _finish_queued_transfer_job_sync(
        self, job_id: int, error_code: str, error_message: str
    ) -> sqlite3.Row:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE transfer_jobs
                SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP,
                    error_code = ?, error_message = ?
                WHERE id = ? AND status = 'QUEUED'
                """,
                (error_code[:40], error_message[:240], job_id),
            )
            row = connection.execute(
                "SELECT * FROM transfer_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("Transfer job disappeared")
            return row

    async def interrupt_portals_transfer_batch(self, batch_id: int) -> None:
        async with self._lock:
            await asyncio.to_thread(
                self._interrupt_portals_transfer_batch_sync, batch_id
            )

    def _interrupt_portals_transfer_batch_sync(self, batch_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE transfer_jobs
                SET status = 'AMBIGUOUS', finished_at = CURRENT_TIMESTAMP,
                    error_code = 'INTERRUPTED_MUTATION',
                    error_message =
                        'Процесс остановился во время операции. Проверьте Portals вручную.'
                WHERE batch_id = ? AND status = 'RUNNING'
                """,
                (batch_id,),
            )
            connection.execute(
                """
                UPDATE transfer_jobs
                SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP,
                    error_code = 'BATCH_INTERRUPTED',
                    error_message =
                        'Пакетная операция была остановлена до запуска этого подарка.'
                WHERE batch_id = ? AND status = 'QUEUED'
                """,
                (batch_id,),
            )
            counts = connection.execute(
                """
                SELECT
                    SUM(CASE WHEN status IN ('SUCCESS', 'DRY_RUN') THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
                    SUM(CASE WHEN status = 'AMBIGUOUS' THEN 1 ELSE 0 END) AS ambiguous_count
                FROM transfer_jobs WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
            assert counts is not None
            success = int(counts["success_count"] or 0)
            failed = int(counts["failed_count"] or 0)
            ambiguous = int(counts["ambiguous_count"] or 0)
            connection.execute(
                """
                UPDATE portals_transfer_batches
                SET status = ?, success_count = ?, failed_count = ?,
                    ambiguous_count = ?, finished_at = CURRENT_TIMESTAMP,
                    result_metadata = ?
                WHERE id = ? AND status IN ('PENDING', 'RUNNING')
                """,
                (
                    "PARTIAL" if success else "AMBIGUOUS" if ambiguous else "FAILED",
                    success,
                    failed,
                    ambiguous,
                    json.dumps(
                        {"interrupted": True},
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                    batch_id,
                ),
            )

    async def get_mrkt_job(
        self,
        job_id: int,
        owner_telegram_id: int | None = None,
    ) -> MrktJob | None:
        async with self._lock:
            row = await asyncio.to_thread(
                self._get_mrkt_job_sync, job_id, owner_telegram_id
            )
        return self._row_to_mrkt_job(row) if row else None

    def _get_mrkt_job_sync(
        self, job_id: int, owner_telegram_id: int | None
    ) -> sqlite3.Row | None:
        with self._connect() as connection:
            if owner_telegram_id is None:
                return connection.execute(
                    "SELECT * FROM mrkt_jobs WHERE id = ?", (job_id,)
                ).fetchone()
            return connection.execute(
                """
                SELECT * FROM mrkt_jobs
                WHERE id = ? AND owner_telegram_id = ?
                """,
                (job_id, owner_telegram_id),
            ).fetchone()

    async def claim_mrkt_job(self, job_id: int) -> MrktJob | None:
        """Atomically change one PENDING job to RUNNING."""
        async with self._lock:
            row = await asyncio.to_thread(self._claim_mrkt_job_sync, job_id)
        return self._row_to_mrkt_job(row) if row else None

    async def recover_running_mrkt_jobs(self) -> int:
        """Make interrupted jobs terminal without ever retrying their sale."""
        async with self._lock:
            return await asyncio.to_thread(self._recover_running_mrkt_jobs_sync)

    def _recover_running_mrkt_jobs_sync(self) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE mrkt_jobs
                SET status = 'AMBIGUOUS', finished_at = CURRENT_TIMESTAMP,
                    error_code = 'SALE_AMBIGUOUS',
                    error_message =
                        'Процесс остановился во время выполнения задания. Проверьте MRKT перед повторной попыткой.'
                WHERE status = 'RUNNING'
                """
            )
            return cursor.rowcount

    def _claim_mrkt_job_sync(self, job_id: int) -> sqlite3.Row | None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE mrkt_jobs
                SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP,
                    finished_at = NULL, error_code = NULL, error_message = NULL
                WHERE id = ? AND status = 'PENDING'
                """,
                (job_id,),
            )
            if cursor.rowcount != 1:
                return None
            return connection.execute(
                "SELECT * FROM mrkt_jobs WHERE id = ?", (job_id,)
            ).fetchone()

    async def update_mrkt_job_target(
        self,
        job_id: int,
        *,
        gift_id: str | int,
        price_ton: str,
        price_nanotons: int,
        display_name: str,
    ) -> None:
        gift_id_json = self._encode_gift_id(gift_id)
        async with self._lock:
            await asyncio.to_thread(
                self._update_mrkt_job_target_sync,
                job_id,
                gift_id_json,
                price_ton,
                price_nanotons,
                display_name,
            )

    def _update_mrkt_job_target_sync(
        self,
        job_id: int,
        gift_id_json: str | None,
        price_ton: str,
        price_nanotons: int,
        display_name: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE mrkt_jobs
                SET gift_id_json = ?, price_ton = ?, price_nanotons = ?,
                    display_name = ?
                WHERE id = ? AND status = 'RUNNING'
                """,
                (gift_id_json, price_ton, price_nanotons, display_name[:80], job_id),
            )

    async def finish_mrkt_job(
        self,
        job_id: int,
        *,
        status: str,
        error_code: str | None,
        error_message: str | None,
        result_metadata: dict[str, Any],
    ) -> MrktJob:
        metadata_json = json.dumps(
            result_metadata, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        async with self._lock:
            row = await asyncio.to_thread(
                self._finish_mrkt_job_sync,
                job_id,
                status,
                error_code,
                error_message,
                metadata_json,
            )
        return self._row_to_mrkt_job(row)

    def _finish_mrkt_job_sync(
        self,
        job_id: int,
        status: str,
        error_code: str | None,
        error_message: str | None,
        result_metadata: str,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE mrkt_jobs
                SET status = ?, finished_at = CURRENT_TIMESTAMP,
                    error_code = ?, error_message = ?, result_metadata = ?
                WHERE id = ? AND status = 'RUNNING'
                """,
                (status, error_code, error_message, result_metadata, job_id),
            )
            row = connection.execute(
                "SELECT * FROM mrkt_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("MRKT job disappeared while it was running")
            return row

    async def create_portals_job(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        offer_id: str | int | None,
    ) -> PortalsJob:
        offer_id_json = self._encode_opaque_id(offer_id, "Portals offer")
        async with self._lock:
            row = await asyncio.to_thread(
                self._create_portals_job_sync,
                owner_telegram_id,
                account_id,
                offer_id_json,
            )
        return self._row_to_portals_job(row)

    def _create_portals_job_sync(
        self,
        owner_telegram_id: int,
        account_id: int,
        offer_id_json: str | None,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO portals_jobs (
                    owner_telegram_id, account_id, market, offer_id_json, status
                ) VALUES (?, ?, 'portals', ?, 'PENDING')
                """,
                (owner_telegram_id, account_id, offer_id_json),
            )
            row = connection.execute(
                "SELECT * FROM portals_jobs WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
            assert row is not None
            return row

    async def get_portals_job(
        self,
        job_id: int,
        owner_telegram_id: int | None = None,
    ) -> PortalsJob | None:
        async with self._lock:
            row = await asyncio.to_thread(
                self._get_portals_job_sync, job_id, owner_telegram_id
            )
        return self._row_to_portals_job(row) if row else None

    def _get_portals_job_sync(
        self, job_id: int, owner_telegram_id: int | None
    ) -> sqlite3.Row | None:
        with self._connect() as connection:
            if owner_telegram_id is None:
                return connection.execute(
                    "SELECT * FROM portals_jobs WHERE id = ?", (job_id,)
                ).fetchone()
            return connection.execute(
                """
                SELECT * FROM portals_jobs
                WHERE id = ? AND owner_telegram_id = ?
                """,
                (job_id, owner_telegram_id),
            ).fetchone()

    async def claim_portals_job(self, job_id: int) -> PortalsJob | None:
        """Atomically change one PENDING Portals job to RUNNING."""
        async with self._lock:
            row = await asyncio.to_thread(self._claim_portals_job_sync, job_id)
        return self._row_to_portals_job(row) if row else None

    def _claim_portals_job_sync(self, job_id: int) -> sqlite3.Row | None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE portals_jobs
                SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP,
                    finished_at = NULL, error_code = NULL, error_message = NULL
                WHERE id = ? AND status = 'PENDING'
                """,
                (job_id,),
            )
            if cursor.rowcount != 1:
                return None
            return connection.execute(
                "SELECT * FROM portals_jobs WHERE id = ?", (job_id,)
            ).fetchone()

    async def update_portals_job_target(
        self,
        job_id: int,
        *,
        offer_id: str | int,
        nft_id: str | int,
        amount: PortalsAmount,
        display_name: str,
    ) -> None:
        offer_id_json = self._encode_opaque_id(offer_id, "Portals offer")
        nft_id_json = self._encode_opaque_id(nft_id, "Portals NFT")
        amount_json = self._encode_portals_amount(amount)
        assert offer_id_json is not None
        assert nft_id_json is not None
        async with self._lock:
            await asyncio.to_thread(
                self._update_portals_job_target_sync,
                job_id,
                offer_id_json,
                nft_id_json,
                amount_json,
                display_name,
            )

    def _update_portals_job_target_sync(
        self,
        job_id: int,
        offer_id_json: str,
        nft_id_json: str,
        amount_json: str,
        display_name: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE portals_jobs
                SET offer_id_json = ?, nft_id_json = ?, amount_json = ?,
                    display_name = ?
                WHERE id = ? AND status = 'RUNNING'
                """,
                (
                    offer_id_json,
                    nft_id_json,
                    amount_json,
                    display_name[:80],
                    job_id,
                ),
            )

    async def list_operation_history(
        self, owner_telegram_id: int, *, limit: int = 20, offset: int = 0
    ) -> list[OperationHistoryItem]:
        safe_limit = max(1, min(limit, 50))
        safe_offset = max(0, offset)
        async with self._lock:
            rows = await asyncio.to_thread(
                self._list_operation_history_sync,
                owner_telegram_id,
                safe_limit,
                safe_offset,
            )
        items: list[OperationHistoryItem] = []
        for row in rows:
            raw_value = row["value_text"]
            value_text: str | None
            if row["market"] == "portals" and raw_value is not None:
                decoded = json.loads(str(raw_value))
                value_text = str(decoded)[:40]
            else:
                value_text = str(raw_value)[:40] if raw_value is not None else None
            items.append(
                OperationHistoryItem(
                    operation_id=int(row["operation_id"]),
                    market=str(row["market"]),
                    account_id=int(row["account_id"]),
                    display_name=(
                        str(row["display_name"]) if row["display_name"] else None
                    ),
                    value_text=value_text,
                    status=str(row["status"]),
                    timestamp=str(row["timestamp"]),
                )
            )
        return items

    def _list_operation_history_sync(
        self, owner_telegram_id: int, limit: int, offset: int
    ) -> list[sqlite3.Row]:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT id AS operation_id, market, account_id, display_name,
                       price_ton AS value_text, status,
                       COALESCE(finished_at, started_at, created_at) AS timestamp
                FROM mrkt_jobs
                WHERE owner_telegram_id = ?
                UNION ALL
                SELECT id AS operation_id, market, account_id, display_name,
                       amount_json AS value_text, status,
                       COALESCE(finished_at, started_at, created_at) AS timestamp
                FROM portals_jobs
                WHERE owner_telegram_id = ?
                UNION ALL
                SELECT id AS operation_id, market || '_transfer' AS market,
                       target_account_id AS account_id, display_name,
                       amount_text AS value_text, status,
                       COALESCE(finished_at, started_at, created_at) AS timestamp
                FROM transfer_jobs
                WHERE owner_telegram_id = ?
                ORDER BY timestamp DESC, operation_id DESC
                LIMIT ? OFFSET ?
                """,
                (
                    owner_telegram_id,
                    owner_telegram_id,
                    owner_telegram_id,
                    limit,
                    offset,
                ),
            ).fetchall()

    async def count_operation_history(self, owner_telegram_id: int) -> int:
        async with self._lock:
            return await asyncio.to_thread(
                self._count_operation_history_sync, owner_telegram_id
            )

    def _count_operation_history_sync(self, owner_telegram_id: int) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM mrkt_jobs WHERE owner_telegram_id = ?)
                    +
                    (SELECT COUNT(*) FROM portals_jobs WHERE owner_telegram_id = ?)
                    +
                    (SELECT COUNT(*) FROM transfer_jobs WHERE owner_telegram_id = ?)
                    AS total
                """,
                (owner_telegram_id, owner_telegram_id, owner_telegram_id),
            ).fetchone()
        return int(row["total"] if row is not None else 0)

    async def finish_portals_job(
        self,
        job_id: int,
        *,
        status: str,
        error_code: str | None,
        error_message: str | None,
        result_metadata: dict[str, Any],
    ) -> PortalsJob:
        metadata_json = json.dumps(
            result_metadata, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        async with self._lock:
            row = await asyncio.to_thread(
                self._finish_portals_job_sync,
                job_id,
                status,
                error_code,
                error_message,
                metadata_json,
            )
        return self._row_to_portals_job(row)

    def _finish_portals_job_sync(
        self,
        job_id: int,
        status: str,
        error_code: str | None,
        error_message: str | None,
        result_metadata: str,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE portals_jobs
                SET status = ?, finished_at = CURRENT_TIMESTAMP,
                    error_code = ?, error_message = ?, result_metadata = ?
                WHERE id = ? AND status = 'RUNNING'
                """,
                (status, error_code, error_message, result_metadata, job_id),
            )
            row = connection.execute(
                "SELECT * FROM portals_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("Portals job disappeared while it was running")
            return row

    async def recover_running_portals_jobs(self) -> int:
        """Make interrupted jobs terminal without replaying acceptance."""
        async with self._lock:
            return await asyncio.to_thread(self._recover_running_portals_jobs_sync)

    def _recover_running_portals_jobs_sync(self) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE portals_jobs
                SET status = 'AMBIGUOUS', finished_at = CURRENT_TIMESTAMP,
                    error_code = 'ACCEPT_AMBIGUOUS',
                    error_message =
                        'Процесс остановился во время задания. Проверьте Portals вручную перед новой попыткой.'
                WHERE status = 'RUNNING'
                """
            )
            return cursor.rowcount

    async def reserve_tonnel_transfer_batch(
        self,
        *,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        confirmation_key: str,
        progress_chat_id: int | None = None,
        progress_message_id: int | None = None,
    ) -> TonnelTransferBatch:
        async with self._lock:
            row = await asyncio.to_thread(
                self._reserve_tonnel_transfer_batch_sync,
                owner_telegram_id,
                owner_account_id,
                target_account_id,
                confirmation_key,
                progress_chat_id,
                progress_message_id,
            )
        return self._row_to_tonnel_transfer_batch(row)

    def _reserve_tonnel_transfer_batch_sync(
        self,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        confirmation_key: str,
        progress_chat_id: int | None,
        progress_message_id: int | None,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                """
                SELECT id FROM tonnel_transfer_batches
                WHERE owner_telegram_id = ? AND owner_account_id = ?
                  AND target_account_id = ?
                  AND status IN ('PENDING', 'RUNNING')
                LIMIT 1
                """,
                (owner_telegram_id, owner_account_id, target_account_id),
            ).fetchone()
            if active is not None:
                raise ActiveTransferConflictError(
                    "A Tonnel batch for this account pair is already running"
                )
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO tonnel_transfer_batches (
                        owner_telegram_id, owner_account_id, target_account_id,
                        confirmation_key, status, started_at,
                        progress_chat_id, progress_message_id
                    ) VALUES (?, ?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP, ?, ?)
                    """,
                    (
                        owner_telegram_id,
                        owner_account_id,
                        target_account_id,
                        confirmation_key,
                        progress_chat_id,
                        progress_message_id,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ActiveTransferConflictError(
                    "A Tonnel batch for this account pair is already running"
                ) from exc
            assert cursor.lastrowid is not None
            row = connection.execute(
                "SELECT * FROM tonnel_transfer_batches WHERE id = ?",
                (int(cursor.lastrowid),),
            ).fetchone()
            assert row is not None
            return row

    async def get_tonnel_transfer_batch(
        self, batch_id: int, owner_telegram_id: int | None = None
    ) -> TonnelTransferBatch | None:
        async with self._lock:
            row = await asyncio.to_thread(
                self._get_tonnel_transfer_batch_sync, batch_id, owner_telegram_id
            )
        return self._row_to_tonnel_transfer_batch(row) if row is not None else None

    def _get_tonnel_transfer_batch_sync(
        self, batch_id: int, owner_telegram_id: int | None
    ) -> sqlite3.Row | None:
        with self._connect() as connection:
            if owner_telegram_id is None:
                return connection.execute(
                    "SELECT * FROM tonnel_transfer_batches WHERE id = ?", (batch_id,)
                ).fetchone()
            return connection.execute(
                """
                SELECT * FROM tonnel_transfer_batches
                WHERE id = ? AND owner_telegram_id = ?
                """,
                (batch_id, owner_telegram_id),
            ).fetchone()

    async def finish_tonnel_transfer_batch(
        self,
        batch_id: int,
        *,
        status: str,
        total_count: int,
        success_count: int,
        failed_count: int,
        ambiguous_count: int,
        result_metadata: dict[str, Any],
    ) -> TonnelTransferBatch:
        metadata_json = json.dumps(
            result_metadata, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        async with self._lock:
            row = await asyncio.to_thread(
                self._finish_tonnel_transfer_batch_sync,
                batch_id,
                status,
                total_count,
                success_count,
                failed_count,
                ambiguous_count,
                metadata_json,
            )
        return self._row_to_tonnel_transfer_batch(row)

    def _finish_tonnel_transfer_batch_sync(
        self,
        batch_id: int,
        status: str,
        total_count: int,
        success_count: int,
        failed_count: int,
        ambiguous_count: int,
        result_metadata: str,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE tonnel_transfer_batches
                SET status = ?, total_count = ?, success_count = ?,
                    failed_count = ?, ambiguous_count = ?,
                    finished_at = CURRENT_TIMESTAMP, result_metadata = ?
                WHERE id = ? AND status IN ('PENDING', 'RUNNING')
                """,
                (
                    status[:20],
                    total_count,
                    success_count,
                    failed_count,
                    ambiguous_count,
                    result_metadata,
                    batch_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM tonnel_transfer_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("Tonnel transfer batch disappeared")
            return row

    async def claim_tonnel_batch_final_notification(self, batch_id: int) -> bool:
        async with self._lock:
            return await asyncio.to_thread(
                self._claim_tonnel_batch_final_notification_sync, batch_id
            )

    def _claim_tonnel_batch_final_notification_sync(self, batch_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tonnel_transfer_batches
                SET final_notification_sent = 1
                WHERE id = ? AND final_notification_sent = 0
                  AND status NOT IN ('PENDING', 'RUNNING')
                """,
                (batch_id,),
            )
            return cursor.rowcount == 1

    async def create_transfer_job(
        self,
        *,
        owner_telegram_id: int,
        market: str,
        owner_account_id: int,
        target_account_id: int,
        asset_id: str | int | None,
        amount_text: str,
        amount_atomic: int | None = None,
        confirmation_key: str | None = None,
        batch_id: int | None = None,
    ) -> TransferJob:
        if market not in {"mrkt", "portals", "tonnel"}:
            raise ValueError("Unsupported transfer market")
        asset_json = self._encode_opaque_id(asset_id, "Transfer asset")
        async with self._lock:
            row = await asyncio.to_thread(
                self._create_transfer_job_sync,
                owner_telegram_id,
                market,
                owner_account_id,
                target_account_id,
                asset_json,
                amount_text,
                amount_atomic,
                confirmation_key,
                batch_id,
            )
        return self._row_to_transfer_job(row)

    async def create_transfer_job_batch(
        self,
        *,
        owner_telegram_id: int,
        market: str,
        owner_account_id: int,
        target_account_id: int,
        assets: list[tuple[str | int, str]],
        amount_text: str,
        amount_atomic: int | None,
        confirmation_key: str,
        batch_id: int | None = None,
    ) -> list[TransferJob]:
        if market not in {"portals", "tonnel"}:
            raise ValueError("Unsupported transfer batch market")
        if not assets:
            raise ValueError("A transfer batch requires at least one asset")
        encoded: list[tuple[str, str, str]] = []
        seen: set[tuple[type[object], object]] = set()
        for asset_id, display_name in assets:
            key = (type(asset_id), asset_id)
            if key in seen:
                raise ValueError("A transfer batch contains duplicate assets")
            seen.add(key)
            asset_json = self._encode_opaque_id(asset_id, "Transfer asset")
            assert asset_json is not None
            child_key = hashlib.sha256(
                f"{confirmation_key}:{asset_json}".encode()
            ).hexdigest()
            encoded.append((asset_json, display_name[:80], child_key))
        async with self._lock:
            rows = await asyncio.to_thread(
                self._create_transfer_job_batch_sync,
                owner_telegram_id,
                market,
                owner_account_id,
                target_account_id,
                encoded,
                amount_text[:40],
                amount_atomic,
                batch_id,
            )
        return [self._row_to_transfer_job(row) for row in rows]

    def _create_transfer_job_batch_sync(
        self,
        owner_telegram_id: int,
        market: str,
        owner_account_id: int,
        target_account_id: int,
        assets: list[tuple[str, str, str]],
        amount_text: str,
        amount_atomic: int | None,
        batch_id: int | None,
    ) -> list[sqlite3.Row]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if market == "tonnel":
                if batch_id is None:
                    raise ValueError("A Tonnel transfer batch ID is required")
                batch = connection.execute(
                    """
                    SELECT id FROM tonnel_transfer_batches
                    WHERE id = ? AND owner_telegram_id = ?
                      AND owner_account_id = ? AND target_account_id = ?
                      AND status = 'RUNNING'
                    """,
                    (
                        batch_id,
                        owner_telegram_id,
                        owner_account_id,
                        target_account_id,
                    ),
                ).fetchone()
                if batch is None:
                    raise ActiveTransferConflictError(
                        "The Tonnel batch reservation is not active"
                    )
            child_keys = [item[2] for item in assets]
            placeholders = ",".join("?" for _ in child_keys)
            existing = connection.execute(
                f"""
                SELECT * FROM transfer_jobs
                WHERE owner_telegram_id = ? AND market = ?
                  AND confirmation_key IN ({placeholders})
                ORDER BY id
                """,
                (owner_telegram_id, market, *child_keys),
            ).fetchall()
            if existing:
                if len(existing) == len(assets):
                    return existing
                raise ActiveTransferConflictError(
                    "The transfer batch was only partially reserved"
                )
            for asset_json, _, _ in assets:
                conflict = connection.execute(
                    """
                    SELECT id FROM transfer_jobs
                    WHERE owner_telegram_id = ? AND market = ?
                      AND owner_account_id = ? AND target_account_id = ?
                      AND asset_id_json = ?
                      AND status IN ('QUEUED', 'PENDING', 'RUNNING')
                    LIMIT 1
                    """,
                    (
                        owner_telegram_id,
                        market,
                        owner_account_id,
                        target_account_id,
                        asset_json,
                    ),
                ).fetchone()
                if conflict is not None:
                    raise ActiveTransferConflictError(
                        "The selected asset already has an active transfer"
                    )
            for asset_json, display_name, child_key in assets:
                connection.execute(
                    """
                    INSERT INTO transfer_jobs (
                        owner_telegram_id, market, owner_account_id,
                        target_account_id, asset_id_json, display_name,
                        amount_text, amount_atomic, confirmation_key,
                        batch_id, status, phase
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 'VALIDATING')
                    """,
                    (
                        owner_telegram_id,
                        market,
                        owner_account_id,
                        target_account_id,
                        asset_json,
                        display_name,
                        amount_text,
                        amount_atomic,
                        child_key,
                        batch_id,
                    ),
                )
            if market == "tonnel" and batch_id is not None:
                connection.execute(
                    """
                    UPDATE tonnel_transfer_batches SET total_count = ?
                    WHERE id = ? AND status = 'RUNNING'
                    """,
                    (len(assets), batch_id),
                )
            return connection.execute(
                f"""
                SELECT * FROM transfer_jobs
                WHERE owner_telegram_id = ? AND market = ?
                  AND confirmation_key IN ({placeholders})
                ORDER BY id
                """,
                (owner_telegram_id, market, *child_keys),
            ).fetchall()

    async def list_active_transfer_asset_ids(
        self,
        *,
        owner_telegram_id: int,
        market: str,
        owner_account_id: int,
        target_account_id: int,
    ) -> list[str | int]:
        async with self._lock:
            rows = await asyncio.to_thread(
                self._list_active_transfer_asset_ids_sync,
                owner_telegram_id,
                market,
                owner_account_id,
                target_account_id,
            )
        result: list[str | int] = []
        for row in rows:
            value = json.loads(str(row["asset_id_json"]))
            if isinstance(value, bool) or not isinstance(value, (str, int)):
                raise TypeError("Stored transfer asset ID has an invalid type")
            result.append(value)
        return result

    def _list_active_transfer_asset_ids_sync(
        self,
        owner_telegram_id: int,
        market: str,
        owner_account_id: int,
        target_account_id: int,
    ) -> list[sqlite3.Row]:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT asset_id_json FROM transfer_jobs
                WHERE owner_telegram_id = ? AND market = ?
                  AND owner_account_id = ? AND target_account_id = ?
                  AND status IN ('QUEUED', 'PENDING', 'RUNNING')
                  AND asset_id_json IS NOT NULL
                """,
                (
                    owner_telegram_id,
                    market,
                    owner_account_id,
                    target_account_id,
                ),
            ).fetchall()

    def _create_transfer_job_sync(
        self,
        owner_telegram_id: int,
        market: str,
        owner_account_id: int,
        target_account_id: int,
        asset_id_json: str | None,
        amount_text: str,
        amount_atomic: int | None,
        confirmation_key: str | None,
        batch_id: int | None,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            # Atomic reservation: the same confirmation or an overlapping active
            # operation cannot enqueue another mutation.
            connection.execute("BEGIN IMMEDIATE")
            if confirmation_key is not None:
                existing = connection.execute(
                    """
                    SELECT * FROM transfer_jobs
                    WHERE owner_telegram_id = ? AND market = ?
                      AND confirmation_key = ?
                    LIMIT 1
                    """,
                    (owner_telegram_id, market, confirmation_key),
                ).fetchone()
                if existing is None:
                    existing = connection.execute(
                        """
                        SELECT * FROM transfer_jobs
                        WHERE owner_telegram_id = ? AND market = ?
                          AND status IN ('QUEUED', 'PENDING', 'RUNNING')
                          AND owner_account_id = ?
                          AND target_account_id = ?
                          AND asset_id_json IS ?
                        ORDER BY id DESC LIMIT 1
                        """,
                        (
                            owner_telegram_id,
                            market,
                            owner_account_id,
                            target_account_id,
                            asset_id_json,
                        ),
                    ).fetchone()
            else:
                existing = connection.execute(
                    """
                    SELECT * FROM transfer_jobs
                    WHERE owner_telegram_id = ? AND market = ? AND (
                        (status IN ('QUEUED', 'PENDING', 'RUNNING')
                         AND owner_account_id = ?
                         AND target_account_id = ?
                         AND asset_id_json IS ?) OR (
                            target_account_id = ?
                            AND (
                                asset_id_json = ?
                                OR asset_id_json IS NULL
                                OR ? IS NULL
                            )
                            AND (
                                status IN ('AMBIGUOUS', 'SUCCESS')
                                OR (status = 'FAILED' AND phase != 'VALIDATING')
                            )
                        )
                    ) ORDER BY id DESC LIMIT 1
                    """,
                    (
                        owner_telegram_id,
                        market,
                        owner_account_id,
                        target_account_id,
                        asset_id_json,
                        target_account_id,
                        asset_id_json,
                        asset_id_json,
                    ),
                ).fetchone()
            if existing is not None:
                return existing
            cursor = connection.execute(
                """
                INSERT INTO transfer_jobs (
                    owner_telegram_id, market, owner_account_id,
                    target_account_id, asset_id_json, amount_text,
                    amount_atomic, confirmation_key, batch_id, status, phase
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 'VALIDATING')
                """,
                (
                    owner_telegram_id,
                    market,
                    owner_account_id,
                    target_account_id,
                    asset_id_json,
                    amount_text[:40],
                    amount_atomic,
                    confirmation_key,
                    batch_id,
                ),
            )
            if market == "portals":
                logger.info("PORTALS_FLOW job_created id=%d", cursor.lastrowid)
            row = connection.execute(
                "SELECT * FROM transfer_jobs WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
            assert row is not None
            return row

    async def get_transfer_job(
        self, job_id: int, owner_telegram_id: int | None = None
    ) -> TransferJob | None:
        async with self._lock:
            row = await asyncio.to_thread(
                self._get_transfer_job_sync, job_id, owner_telegram_id
            )
        return self._row_to_transfer_job(row) if row else None

    def _get_transfer_job_sync(
        self, job_id: int, owner_telegram_id: int | None
    ) -> sqlite3.Row | None:
        with self._connect() as connection:
            if owner_telegram_id is None:
                return connection.execute(
                    "SELECT * FROM transfer_jobs WHERE id = ?", (job_id,)
                ).fetchone()
            return connection.execute(
                """
                SELECT * FROM transfer_jobs
                WHERE id = ? AND owner_telegram_id = ?
                """,
                (job_id, owner_telegram_id),
            ).fetchone()

    async def claim_transfer_job(self, job_id: int) -> TransferJob | None:
        async with self._lock:
            row = await asyncio.to_thread(self._claim_transfer_job_sync, job_id)
        return self._row_to_transfer_job(row) if row else None

    def _claim_transfer_job_sync(self, job_id: int) -> sqlite3.Row | None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE transfer_jobs
                SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP,
                    finished_at = NULL, error_code = NULL, error_message = NULL
                WHERE id = ? AND status IN ('QUEUED', 'PENDING')
                """,
                (job_id,),
            )
            if cursor.rowcount != 1:
                return None
            return connection.execute(
                "SELECT * FROM transfer_jobs WHERE id = ?", (job_id,)
            ).fetchone()

    async def update_transfer_job(
        self,
        job_id: int,
        *,
        phase: str,
        asset_id: str | int | None = None,
        display_name: str | None = None,
        external_ref: str | int | None = None,
    ) -> None:
        asset_json = self._encode_opaque_id(asset_id, "Transfer asset")
        external_json = self._encode_opaque_id(external_ref, "External reference")
        async with self._lock:
            await asyncio.to_thread(
                self._update_transfer_job_sync,
                job_id,
                phase,
                asset_json,
                display_name,
                external_json,
            )

    def _update_transfer_job_sync(
        self,
        job_id: int,
        phase: str,
        asset_id_json: str | None,
        display_name: str | None,
        external_ref_json: str | None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE transfer_jobs
                SET phase = ?,
                    asset_id_json = COALESCE(?, asset_id_json),
                    display_name = COALESCE(?, display_name),
                    external_ref_json = COALESCE(?, external_ref_json)
                WHERE id = ? AND status = 'RUNNING'
                """,
                (
                    phase[:40],
                    asset_id_json,
                    display_name[:80] if display_name else None,
                    external_ref_json,
                    job_id,
                ),
            )

    async def finish_transfer_job(
        self,
        job_id: int,
        *,
        status: str,
        phase: str,
        error_code: str | None,
        error_message: str | None,
        result_metadata: dict[str, Any],
    ) -> TransferJob:
        metadata_json = json.dumps(
            result_metadata, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        async with self._lock:
            row = await asyncio.to_thread(
                self._finish_transfer_job_sync,
                job_id,
                status,
                phase,
                error_code,
                error_message,
                metadata_json,
            )
        return self._row_to_transfer_job(row)

    def _finish_transfer_job_sync(
        self,
        job_id: int,
        status: str,
        phase: str,
        error_code: str | None,
        error_message: str | None,
        result_metadata: str,
    ) -> sqlite3.Row:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE transfer_jobs
                SET status = ?, phase = ?, finished_at = CURRENT_TIMESTAMP,
                    error_code = ?, error_message = ?, result_metadata = ?
                WHERE id = ? AND status = 'RUNNING'
                """,
                (
                    status,
                    phase[:40],
                    error_code,
                    error_message,
                    result_metadata,
                    job_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM transfer_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("Transfer job disappeared while it was running")
            return row

    async def recover_running_transfer_jobs(self) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._recover_running_transfer_jobs_sync)

    def _recover_running_transfer_jobs_sync(self) -> int:
        with self._connect() as connection:
            running = connection.execute(
                """
                UPDATE transfer_jobs
                SET status = 'AMBIGUOUS', finished_at = CURRENT_TIMESTAMP,
                    error_code = 'INTERRUPTED_MUTATION',
                    error_message =
                        'Процесс остановился во время операции. Проверьте площадку вручную.'
                WHERE status = 'RUNNING'
                """
            )
            queued = connection.execute(
                """
                UPDATE transfer_jobs
                SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP,
                    error_code = 'BATCH_INTERRUPTED',
                    error_message =
                        'Пакетная операция была остановлена до запуска этого подарка.'
                WHERE status = 'QUEUED'
                """
            )
            return running.rowcount + queued.rowcount

    async def recover_portals_transfer_batches(self) -> int:
        async with self._lock:
            return await asyncio.to_thread(
                self._recover_portals_transfer_batches_sync
            )

    def _recover_portals_transfer_batches_sync(self) -> int:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id FROM portals_transfer_batches
                WHERE status IN ('PENDING', 'RUNNING')
                """
            ).fetchall()
            for row in rows:
                batch_id = int(row["id"])
                counts = connection.execute(
                    """
                    SELECT
                        SUM(CASE WHEN status IN ('SUCCESS', 'DRY_RUN') THEN 1 ELSE 0 END) AS success_count,
                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
                        SUM(CASE WHEN status = 'AMBIGUOUS' THEN 1 ELSE 0 END) AS ambiguous_count
                    FROM transfer_jobs WHERE batch_id = ?
                    """,
                    (batch_id,),
                ).fetchone()
                success = int(counts["success_count"] or 0)
                failed = int(counts["failed_count"] or 0)
                ambiguous = int(counts["ambiguous_count"] or 0)
                status = "PARTIAL" if success else "AMBIGUOUS" if ambiguous else "FAILED"
                connection.execute(
                    """
                    UPDATE portals_transfer_batches
                    SET status = ?, success_count = ?, failed_count = ?,
                        ambiguous_count = ?, finished_at = CURRENT_TIMESTAMP,
                        result_metadata = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        success,
                        failed,
                        ambiguous,
                        json.dumps(
                            {"interrupted": True},
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                        batch_id,
                    ),
                )
            return len(rows)

    async def recover_tonnel_transfer_batches(self) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._recover_tonnel_transfer_batches_sync)

    def _recover_tonnel_transfer_batches_sync(self) -> int:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id FROM tonnel_transfer_batches
                WHERE status IN ('PENDING', 'RUNNING')
                """
            ).fetchall()
            for row in rows:
                batch_id = int(row["id"])
                connection.execute(
                    """
                    UPDATE transfer_jobs
                    SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP,
                        error_code = 'BATCH_INTERRUPTED',
                        error_message =
                            'Пакетная операция была остановлена до запуска этого подарка.'
                    WHERE batch_id = ? AND status = 'PENDING'
                    """,
                    (batch_id,),
                )
                counts = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS total_count,
                        SUM(CASE WHEN status IN ('SUCCESS', 'DRY_RUN') THEN 1 ELSE 0 END) AS success_count,
                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
                        SUM(CASE WHEN status = 'AMBIGUOUS' THEN 1 ELSE 0 END) AS ambiguous_count
                    FROM transfer_jobs WHERE batch_id = ?
                    """,
                    (batch_id,),
                ).fetchone()
                assert counts is not None
                total = int(counts["total_count"] or 0)
                success = int(counts["success_count"] or 0)
                failed = int(counts["failed_count"] or 0)
                ambiguous = int(counts["ambiguous_count"] or 0)
                status = "PARTIAL" if success else "AMBIGUOUS" if ambiguous else "FAILED"
                connection.execute(
                    """
                    UPDATE tonnel_transfer_batches
                    SET status = ?, total_count = ?, success_count = ?,
                        failed_count = ?, ambiguous_count = ?,
                        finished_at = CURRENT_TIMESTAMP, result_metadata = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        total,
                        success,
                        failed,
                        ambiguous,
                        json.dumps(
                            {"interrupted": True},
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                        batch_id,
                    ),
                )
            return len(rows)

    @staticmethod
    def _row_to_account(row: sqlite3.Row) -> Account:
        values: dict[str, Any] = dict(row)
        values["role"] = Database._validate_account_role(str(values.get("role")))
        return Account(**values)

    @staticmethod
    def _validate_account_role(role: str) -> str:
        normalized = role.strip().upper()
        if normalized not in {"OWNER", "TARGET"}:
            raise ValueError("Account role must be OWNER or TARGET")
        return normalized

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection,
        table: str,
        column: str,
        declaration: str,
    ) -> None:
        existing = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    @staticmethod
    def _encode_gift_id(gift_id: str | int | None) -> str | None:
        if gift_id is None:
            return None
        if isinstance(gift_id, bool) or not isinstance(gift_id, (str, int)):
            raise TypeError("MRKT gift ID must be a string, integer, or None")
        return json.dumps(gift_id, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _encode_opaque_id(value: str | int | None, label: str) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise TypeError(f"{label} ID must be a string, integer, or None")
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _encode_portals_amount(value: PortalsAmount) -> str:
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise TypeError("Portals amount must be a string or number")
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _row_to_mrkt_job(row: sqlite3.Row) -> MrktJob:
        gift_id_json = row["gift_id_json"]
        gift_id = json.loads(gift_id_json) if gift_id_json is not None else None
        if isinstance(gift_id, bool) or not isinstance(gift_id, (str, int, type(None))):
            raise TypeError("Stored MRKT gift ID has an invalid type")
        raw_metadata = json.loads(str(row["result_metadata"]))
        if not isinstance(raw_metadata, dict):
            raise TypeError("Stored MRKT result metadata is invalid")
        return MrktJob(
            id=int(row["id"]),
            owner_telegram_id=int(row["owner_telegram_id"]),
            account_id=int(row["account_id"]),
            market=str(row["market"]),
            gift_id=gift_id,
            display_name=(str(row["display_name"]) if row["display_name"] else None),
            price_ton=str(row["price_ton"]),
            price_nanotons=(
                int(row["price_nanotons"])
                if row["price_nanotons"] is not None
                else None
            ),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            started_at=(str(row["started_at"]) if row["started_at"] else None),
            finished_at=(str(row["finished_at"]) if row["finished_at"] else None),
            error_code=(str(row["error_code"]) if row["error_code"] else None),
            error_message=(str(row["error_message"]) if row["error_message"] else None),
            result_metadata={str(key): value for key, value in raw_metadata.items()},
        )

    @staticmethod
    def _row_to_portals_job(row: sqlite3.Row) -> PortalsJob:
        def decode_id(column: str) -> str | int | None:
            encoded = row[column]
            value = json.loads(encoded) if encoded is not None else None
            if isinstance(value, bool) or not isinstance(value, (str, int, type(None))):
                raise TypeError(f"Stored Portals {column} has an invalid type")
            return value

        amount_json = row["amount_json"]
        amount = json.loads(amount_json) if amount_json is not None else None
        if isinstance(amount, bool) or not isinstance(
            amount, (str, int, float, type(None))
        ):
            raise TypeError("Stored Portals amount has an invalid type")
        raw_metadata = json.loads(str(row["result_metadata"]))
        if not isinstance(raw_metadata, dict):
            raise TypeError("Stored Portals result metadata is invalid")
        return PortalsJob(
            id=int(row["id"]),
            owner_telegram_id=int(row["owner_telegram_id"]),
            account_id=int(row["account_id"]),
            market=str(row["market"]),
            offer_id=decode_id("offer_id_json"),
            nft_id=decode_id("nft_id_json"),
            amount=amount,
            display_name=(str(row["display_name"]) if row["display_name"] else None),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            started_at=(str(row["started_at"]) if row["started_at"] else None),
            finished_at=(str(row["finished_at"]) if row["finished_at"] else None),
            error_code=(str(row["error_code"]) if row["error_code"] else None),
            error_message=(str(row["error_message"]) if row["error_message"] else None),
            result_metadata={str(key): value for key, value in raw_metadata.items()},
        )

    @staticmethod
    def _row_to_transfer_job(row: sqlite3.Row) -> TransferJob:
        def decode_id(column: str) -> str | int | None:
            encoded = row[column]
            value = json.loads(encoded) if encoded is not None else None
            if isinstance(value, bool) or not isinstance(value, (str, int, type(None))):
                raise TypeError(f"Stored transfer {column} has an invalid type")
            return value

        raw_metadata = json.loads(str(row["result_metadata"]))
        if not isinstance(raw_metadata, dict):
            raise TypeError("Stored transfer result metadata is invalid")
        return TransferJob(
            id=int(row["id"]),
            owner_telegram_id=int(row["owner_telegram_id"]),
            market=str(row["market"]),
            owner_account_id=int(row["owner_account_id"]),
            target_account_id=int(row["target_account_id"]),
            asset_id=decode_id("asset_id_json"),
            display_name=(str(row["display_name"]) if row["display_name"] else None),
            amount_text=str(row["amount_text"]),
            amount_atomic=(
                int(row["amount_atomic"]) if row["amount_atomic"] is not None else None
            ),
            external_ref=decode_id("external_ref_json"),
            status=str(row["status"]),
            phase=str(row["phase"]),
            created_at=str(row["created_at"]),
            started_at=(str(row["started_at"]) if row["started_at"] else None),
            finished_at=(str(row["finished_at"]) if row["finished_at"] else None),
            error_code=(str(row["error_code"]) if row["error_code"] else None),
            error_message=(str(row["error_message"]) if row["error_message"] else None),
            result_metadata={str(key): value for key, value in raw_metadata.items()},
            batch_id=(int(row["batch_id"]) if row["batch_id"] is not None else None),
        )

    @staticmethod
    def _row_to_portals_transfer_batch(row: sqlite3.Row) -> PortalsTransferBatch:
        raw_metadata = json.loads(str(row["result_metadata"]))
        if not isinstance(raw_metadata, dict):
            raise TypeError("Stored Portals batch metadata is invalid")
        return PortalsTransferBatch(
            id=int(row["id"]),
            owner_telegram_id=int(row["owner_telegram_id"]),
            owner_account_id=int(row["owner_account_id"]),
            target_account_id=int(row["target_account_id"]),
            status=str(row["status"]),
            total_count=int(row["total_count"]),
            success_count=int(row["success_count"]),
            failed_count=int(row["failed_count"]),
            ambiguous_count=int(row["ambiguous_count"]),
            created_at=str(row["created_at"]),
            started_at=(str(row["started_at"]) if row["started_at"] else None),
            finished_at=(str(row["finished_at"]) if row["finished_at"] else None),
            result_metadata={str(key): value for key, value in raw_metadata.items()},
        )

    @staticmethod
    def _row_to_tonnel_transfer_batch(row: sqlite3.Row) -> TonnelTransferBatch:
        raw_metadata = json.loads(str(row["result_metadata"]))
        if not isinstance(raw_metadata, dict):
            raise TypeError("Stored Tonnel batch metadata is invalid")
        return TonnelTransferBatch(
            id=int(row["id"]),
            owner_telegram_id=int(row["owner_telegram_id"]),
            owner_account_id=int(row["owner_account_id"]),
            target_account_id=int(row["target_account_id"]),
            status=str(row["status"]),
            total_count=int(row["total_count"]),
            success_count=int(row["success_count"]),
            failed_count=int(row["failed_count"]),
            ambiguous_count=int(row["ambiguous_count"]),
            created_at=str(row["created_at"]),
            started_at=(str(row["started_at"]) if row["started_at"] else None),
            finished_at=(str(row["finished_at"]) if row["finished_at"] else None),
            progress_chat_id=(
                int(row["progress_chat_id"])
                if row["progress_chat_id"] is not None
                else None
            ),
            progress_message_id=(
                int(row["progress_message_id"])
                if row["progress_message_id"] is not None
                else None
            ),
            final_notification_sent=bool(row["final_notification_sent"]),
            result_metadata={str(key): value for key, value in raw_metadata.items()},
        )
