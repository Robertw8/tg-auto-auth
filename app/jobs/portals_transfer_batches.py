from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from app.db import (
    Account,
    ActiveTransferConflictError,
    Database,
    PortalsTransferBatch,
    TransferJob,
)
from app.telegram_client import PortalsNft

from .portals_transfer_jobs import PortalsTransferJobService

logger = logging.getLogger(__name__)
BatchProgressCallback = Callable[["PortalsBatchProgress"], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class PortalsBatchItemProgress:
    display_name: str
    status: str


@dataclass(frozen=True, slots=True)
class PortalsBatchProgress:
    total_count: int
    success_count: int
    failed_count: int
    ambiguous_count: int
    running_count: int
    queued_count: int
    items: tuple[PortalsBatchItemProgress, ...]


class PortalsTransferBatchService:
    """Bounded orchestration over isolated, persisted single-NFT jobs."""

    def __init__(
        self,
        db: Database,
        child_jobs: PortalsTransferJobService,
        *,
        max_concurrency: int = 5,
    ) -> None:
        if not 1 <= max_concurrency <= 5:
            raise ValueError("Portals batch concurrency must be between 1 and 5")
        self._db = db
        self._child_jobs = child_jobs
        self._offer_amount = getattr(child_jobs, "offer_amount", "0.53")
        self._max_concurrency = max_concurrency
        self._active_guard = asyncio.Lock()
        self._active_batches: set[int] = set()
        self._active_pairs: set[tuple[int, int, int]] = set()

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    async def execute_batch(
        self,
        *,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        nfts: Sequence[PortalsNft],
        confirmation_nonce: str,
        progress: BatchProgressCallback | None = None,
    ) -> PortalsTransferBatch:
        pair_key = (owner_telegram_id, owner_account_id, target_account_id)
        async with self._active_guard:
            if pair_key in self._active_pairs:
                raise ActiveTransferConflictError(
                    "A Portals batch for this account pair is already running"
                )
            self._active_pairs.add(pair_key)
        try:
            return await self._execute_batch(
                owner_telegram_id=owner_telegram_id,
                owner_account_id=owner_account_id,
                target_account_id=target_account_id,
                nfts=nfts,
                confirmation_nonce=confirmation_nonce,
                progress=progress,
            )
        finally:
            async with self._active_guard:
                self._active_pairs.discard(pair_key)

    async def _execute_batch(
        self,
        *,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        nfts: Sequence[PortalsNft],
        confirmation_nonce: str,
        progress: BatchProgressCallback | None,
    ) -> PortalsTransferBatch:
        owner, target = await asyncio.gather(
            self._db.get_account(owner_account_id, owner_telegram_id),
            self._db.get_account(target_account_id, owner_telegram_id),
        )
        self._validate_accounts(owner, target)
        assets = self._validate_nfts(nfts)
        if not confirmation_nonce:
            raise ValueError("A batch confirmation nonce is required")
        confirmation_key = hashlib.sha256(confirmation_nonce.encode()).hexdigest()
        batch, children, created = await self._db.create_portals_transfer_batch(
            owner_telegram_id=owner_telegram_id,
            owner_account_id=owner_account_id,
            target_account_id=target_account_id,
            assets=assets,
            amount_text=self._offer_amount,
            confirmation_key=confirmation_key,
        )
        if not created:
            return batch
        claimed = await self._db.claim_portals_transfer_batch(batch.id)
        if claimed is None:
            current = await self._db.get_portals_transfer_batch(
                batch.id, owner_telegram_id
            )
            return current or batch
        async with self._active_guard:
            if batch.id in self._active_batches:
                return claimed
            self._active_batches.add(batch.id)
        started_at = time.monotonic()
        try:
            scope_entered = False
            auth_started_at = time.monotonic()
            try:
                async with self._child_jobs.batch_session_scope(
                    owner_telegram_id,
                    owner_account_id,
                    target_account_id,
                ):
                    scope_entered = True
                    return await self._run(
                        claimed,
                        children,
                        progress,
                        started_at=started_at,
                        auth_preparation_duration_ms=max(
                            0, round((time.monotonic() - auth_started_at) * 1000)
                        ),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - persisted batch boundary
                if scope_entered:
                    logger.warning(
                        "Portals batch runner failed batch_id=%d exception=%s",
                        batch.id,
                        type(exc).__name__,
                    )
                    await self._db.interrupt_portals_transfer_batch(batch.id)
                    current = await self._db.get_portals_transfer_batch(
                        batch.id, owner_telegram_id
                    )
                    return current or claimed
                logger.warning(
                    "Portals batch auth failed batch_id=%d exception=%s",
                    batch.id,
                    type(exc).__name__,
                )
                for child in children:
                    await self._db.finish_queued_transfer_job(
                        child.id,
                        error_code="BATCH_AUTH_FAILED",
                        error_message="Не удалось подготовить аккаунты Portals.",
                    )
                return await self._db.finish_portals_transfer_batch(
                    batch.id,
                    status="FAILED",
                    success_count=0,
                    failed_count=len(children),
                    ambiguous_count=0,
                    result_metadata={
                        "batch_total_duration_ms": max(
                            0, round((time.monotonic() - started_at) * 1000)
                        ),
                        "auth_preparation_duration_ms": max(
                            0, round((time.monotonic() - auth_started_at) * 1000)
                        ),
                        "max_concurrency_observed": 0,
                        "queued_count": len(children),
                        "success_count": 0,
                        "failed_count": len(children),
                        "ambiguous_count": 0,
                        "dry_run_count": 0,
                        "rate_limited": False,
                    },
                )
        except asyncio.CancelledError:
            await self._db.interrupt_portals_transfer_batch(batch.id)
            raise
        finally:
            async with self._active_guard:
                self._active_batches.discard(batch.id)

    async def _run(
        self,
        batch: PortalsTransferBatch,
        children: list[TransferJob],
        progress: BatchProgressCallback | None,
        *,
        started_at: float,
        auth_preparation_duration_ms: int,
    ) -> PortalsTransferBatch:
        queue: asyncio.Queue[TransferJob | None] = asyncio.Queue()
        for child in children:
            queue.put_nowait(child)
        worker_count = min(self._max_concurrency, len(children))
        states = {child.id: "QUEUED" for child in children}
        display_names = {
            child.id: child.display_name or "Подарок" for child in children
        }
        state_lock = asyncio.Lock()
        notify_lock = asyncio.Lock()
        rate_limited = asyncio.Event()
        max_running = 0

        async def notify() -> None:
            if progress is None:
                return
            async with notify_lock:
                async with state_lock:
                    snapshot = self._progress_snapshot(states, display_names)
                try:
                    await progress(snapshot)
                except Exception as exc:  # noqa: BLE001 - UI must not stop jobs
                    logger.warning(
                        "Portals batch progress failed batch_id=%d exception=%s",
                        batch.id,
                        type(exc).__name__,
                    )

        async def worker() -> None:
            nonlocal max_running
            while True:
                child = await queue.get()
                try:
                    if child is None:
                        return
                    if rate_limited.is_set():
                        result = await self._db.finish_queued_transfer_job(
                            child.id,
                            error_code="BATCH_RATE_LIMITED",
                            error_message=(
                                "Portals ограничил частоту запросов. "
                                "Этот подарок не запускался."
                            ),
                        )
                    else:
                        async with state_lock:
                            states[child.id] = "RUNNING"
                            running = sum(
                                status == "RUNNING" for status in states.values()
                            )
                            max_running = max(max_running, running)
                        await notify()
                        try:
                            result = (
                                await self._child_jobs.execute_reserved_portals_transfer(
                                    child.id
                                )
                            )
                        except Exception as exc:
                            logger.warning(
                                "Portals batch child failed batch_id=%d job_id=%d "
                                "exception=%s",
                                batch.id,
                                child.id,
                                type(exc).__name__,
                            )
                            current = await self._db.get_transfer_job(child.id)
                            if current is not None and current.status == "RUNNING":
                                result = await self._db.finish_transfer_job(
                                    child.id,
                                    status="AMBIGUOUS",
                                    phase=current.phase,
                                    error_code="CHILD_UNEXPECTED_FAILURE",
                                    error_message=(
                                        "Результат операции требует ручной проверки."
                                    ),
                                    result_metadata={},
                                )
                            elif current is not None:
                                result = current
                            else:
                                raise
                        if result.error_code in {
                            "RATE_LIMITED",
                            "RATE_LIMITED_AFTER_CREATE",
                        }:
                            rate_limited.set()
                    async with state_lock:
                        states[child.id] = result.status
                    await notify()
                finally:
                    queue.task_done()

        await notify()
        workers = [
            asyncio.create_task(worker(), name=f"portals-batch-{batch.id}-{index}")
            for index in range(worker_count)
        ]
        try:
            await queue.join()
        except asyncio.CancelledError:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        for _ in workers:
            queue.put_nowait(None)
        await asyncio.gather(*workers)

        current = await self._db.list_transfer_jobs_by_batch(batch.id)
        success_count = sum(job.status == "SUCCESS" for job in current)
        dry_run_count = sum(job.status == "DRY_RUN" for job in current)
        failed_count = sum(job.status == "FAILED" for job in current)
        ambiguous_count = sum(job.status == "AMBIGUOUS" for job in current)
        create_mutation_count = self._mutation_count(
            current, "portals_create_offer"
        )
        accept_mutation_count = self._mutation_count(current, "portals_accept")
        if dry_run_count == len(current):
            status = "DRY_RUN"
        elif success_count == len(current):
            status = "SUCCESS"
        elif success_count or dry_run_count:
            status = "PARTIAL"
        elif ambiguous_count:
            status = "AMBIGUOUS"
        else:
            status = "FAILED"
        metadata = {
            "batch_total_duration_ms": max(
                0, round((time.monotonic() - started_at) * 1000)
            ),
            "auth_preparation_duration_ms": auth_preparation_duration_ms,
            "max_concurrency_observed": max_running,
            "queued_count": max(0, len(children) - worker_count),
            "success_count": success_count,
            "failed_count": failed_count,
            "ambiguous_count": ambiguous_count,
            "dry_run_count": dry_run_count,
            "create_mutation_count": create_mutation_count,
            "accept_mutation_count": accept_mutation_count,
            "rate_limited": rate_limited.is_set(),
        }
        return await self._db.finish_portals_transfer_batch(
            batch.id,
            status=status,
            success_count=success_count,
            failed_count=failed_count,
            ambiguous_count=ambiguous_count,
            result_metadata=metadata,
        )

    @staticmethod
    def _validate_accounts(owner: Account | None, target: Account | None) -> None:
        if owner is None or owner.role != "OWNER":
            raise ValueError("Owner account is unavailable")
        if target is None or target.role != "TARGET":
            raise ValueError("Target account is unavailable")

    @staticmethod
    def _validate_nfts(nfts: Sequence[PortalsNft]) -> list[tuple[str | int, str]]:
        if not nfts:
            raise ValueError("Select at least one NFT")
        result: list[tuple[str | int, str]] = []
        seen: set[tuple[type[object], object]] = set()
        for nft in nfts:
            if not nft.eligible or isinstance(nft.nft_id, bool) or not isinstance(
                nft.nft_id, (str, int)
            ):
                raise ValueError("A selected NFT is unavailable")
            key = (type(nft.nft_id), nft.nft_id)
            if key in seen:
                raise ValueError("A selected NFT is duplicated")
            seen.add(key)
            result.append((nft.nft_id, nft.display_name))
        return result

    @staticmethod
    def _progress_snapshot(
        states: dict[int, str], display_names: dict[int, str]
    ) -> PortalsBatchProgress:
        completed = {"SUCCESS", "DRY_RUN"}
        return PortalsBatchProgress(
            total_count=len(states),
            success_count=sum(value in completed for value in states.values()),
            failed_count=sum(value == "FAILED" for value in states.values()),
            ambiguous_count=sum(
                value == "AMBIGUOUS" for value in states.values()
            ),
            running_count=sum(value == "RUNNING" for value in states.values()),
            queued_count=sum(value == "QUEUED" for value in states.values()),
            items=tuple(
                PortalsBatchItemProgress(display_names[job_id], status)
                for job_id, status in states.items()
            ),
        )

    @staticmethod
    def _mutation_count(children: Sequence[TransferJob], stage: str) -> int:
        total = 0
        for child in children:
            report = child.result_metadata.get("live_verify")
            if not isinstance(report, dict):
                continue
            counts = report.get("mutation_counts")
            if not isinstance(counts, dict):
                continue
            value = counts.get(stage)
            if isinstance(value, int) and not isinstance(value, bool) and value > 0:
                total += value
        return total

    async def shutdown(self) -> None:
        async with self._active_guard:
            active = tuple(self._active_batches)
        for batch_id in active:
            await self._db.interrupt_portals_transfer_batch(batch_id)


__all__ = [
    "ActiveTransferConflictError",
    "PortalsBatchItemProgress",
    "PortalsBatchProgress",
    "PortalsTransferBatchService",
]
