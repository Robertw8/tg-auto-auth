from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from app.db import Account, ActiveTransferConflictError, Database, TransferJob
from app.telegram_client import (
    TonnelBatchAuthContext,
    TonnelMarketGift,
    TonnelService,
    TonnelTransferMode,
)

from .tonnel_transfer_jobs import TonnelTransferJobService

logger = logging.getLogger(__name__)
TonnelBatchProgressCallback = Callable[["TonnelBatchProgress"], Awaitable[None]]
TonnelBatchReservedCallback = Callable[[int], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class TonnelBatchItemProgress:
    display_name: str
    status: str
    safe_reason: str | None = None


@dataclass(frozen=True, slots=True)
class TonnelBatchProgress:
    total_count: int
    success_count: int
    failed_count: int
    ambiguous_count: int
    dry_run_count: int
    running_count: int
    queued_count: int
    items: tuple[TonnelBatchItemProgress, ...]


@dataclass(frozen=True, slots=True)
class TonnelTransferBatchResult:
    batch_id: int
    status: str
    children: tuple[TransferJob, ...]
    progress: TonnelBatchProgress
    batch_reference: str
    offer_amount: Decimal
    per_gift_required: Decimal
    total_required: Decimal
    buyer_balance: Decimal
    error_code: str | None = None
    error_message: str | None = None
    batch_auth: Mapping[str, object] = field(default_factory=dict)


class TonnelTransferBatchService:
    """Run isolated BUY_OFFER jobs for every exact eligible N2 gift."""

    def __init__(
        self,
        db: Database,
        tonnel: TonnelService,
        child_jobs: TonnelTransferJobService,
        *,
        offer_amount: str | Decimal,
        max_concurrency: int = 5,
    ) -> None:
        if not 1 <= max_concurrency <= 5:
            raise ValueError("Tonnel batch concurrency must be between 1 and 5")
        if child_jobs.offer_accept_delay_ms != tonnel.offer_accept_delay_ms:
            raise ValueError(
                "Tonnel batch and child jobs must share the configured accept delay"
            )
        self._db = db
        self._tonnel = tonnel
        self._child_jobs = child_jobs
        self._offer_amount = tonnel.normalize_market_price(offer_amount)
        self._max_concurrency = max_concurrency
        self._active_guard = asyncio.Lock()
        self._active_pairs: set[tuple[int, int, int]] = set()

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @property
    def offer_amount(self) -> Decimal:
        return self._offer_amount

    @property
    def offer_accept_delay_ms(self) -> int:
        return self._child_jobs.offer_accept_delay_ms

    async def execute_batch(
        self,
        *,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        progress: TonnelBatchProgressCallback | None = None,
        reserved: TonnelBatchReservedCallback | None = None,
        progress_chat_id: int | None = None,
        progress_message_id: int | None = None,
    ) -> TonnelTransferBatchResult:
        if self._child_jobs.transfer_mode is not TonnelTransferMode.BUY_OFFER:
            raise ValueError("Tonnel automatic batches require BUY_OFFER mode")
        pair_key = (owner_telegram_id, owner_account_id, target_account_id)
        async with self._active_guard:
            if pair_key in self._active_pairs:
                raise ActiveTransferConflictError(
                    "A Tonnel batch for this account pair is already running"
                )
            self._active_pairs.add(pair_key)
        batch = None
        try:
            confirmation_material = secrets.token_urlsafe(32)
            confirmation_key = hashlib.sha256(
                confirmation_material.encode()
            ).hexdigest()
            del confirmation_material
            batch = await self._db.reserve_tonnel_transfer_batch(
                owner_telegram_id=owner_telegram_id,
                owner_account_id=owner_account_id,
                target_account_id=target_account_id,
                confirmation_key=confirmation_key,
                progress_chat_id=progress_chat_id,
                progress_message_id=progress_message_id,
            )
            if reserved is not None:
                try:
                    await reserved(batch.id)
                except Exception as exc:  # noqa: BLE001 - UI cannot stop batch
                    logger.warning(
                        "Tonnel batch reserved callback failed batch_id=%d exception=%s",
                        batch.id,
                        type(exc).__name__,
                    )
            return await self._execute_batch(
                batch_id=batch.id,
                owner_telegram_id=owner_telegram_id,
                owner_account_id=owner_account_id,
                target_account_id=target_account_id,
                progress=progress,
            )
        except asyncio.CancelledError:
            if batch is not None:
                await self._finish_unexpected_batch(batch.id, ambiguous=True)
            raise
        except ActiveTransferConflictError:
            raise
        except Exception:
            if batch is not None:
                await self._finish_unexpected_batch(batch.id, ambiguous=False)
            raise
        finally:
            async with self._active_guard:
                self._active_pairs.discard(pair_key)

    async def _execute_batch(
        self,
        *,
        batch_id: int,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        progress: TonnelBatchProgressCallback | None,
    ) -> TonnelTransferBatchResult:
        owner, target = await asyncio.gather(
            self._db.get_account(owner_account_id, owner_telegram_id),
            self._db.get_account(target_account_id, owner_telegram_id),
        )
        self._validate_accounts(owner, target)
        assert owner is not None and target is not None
        async with self._tonnel.batch_auth_context(
            owner_telegram_id=owner_telegram_id,
            n1_account_id=owner.id,
            n2_account_id=target.id,
        ) as batch_auth:
            return await self._execute_authenticated_batch(
                batch_id=batch_id,
                owner_telegram_id=owner_telegram_id,
                owner=owner,
                target=target,
                progress=progress,
                batch_auth=batch_auth,
            )

    async def _execute_authenticated_batch(
        self,
        *,
        batch_id: int,
        owner_telegram_id: int,
        owner: Account,
        target: Account,
        progress: TonnelBatchProgressCallback | None,
        batch_auth: TonnelBatchAuthContext,
    ) -> TonnelTransferBatchResult:
        owner_account_id = owner.id
        target_account_id = target.id
        raw_gifts, buyer_balance = await asyncio.gather(
            self._tonnel.get_market_inventory(
                target.id, owner_telegram_id=owner_telegram_id
            ),
            self._tonnel.get_balance(
                owner.id, owner_telegram_id=owner_telegram_id
            ),
        )
        active_ids = await self._db.list_active_transfer_asset_ids(
            owner_telegram_id=owner_telegram_id,
            market="tonnel",
            owner_account_id=owner_account_id,
            target_account_id=target_account_id,
        )
        gifts = self._eligible_gifts(raw_gifts, target, active_ids)
        batch_auth.set_child_count(len(gifts))
        batch_auth_metadata = batch_auth.safe_metadata()
        logger.info(
            "Tonnel batch auth prepared n1_account_id=%d n2_account_id=%d "
            "child_count=%d telethon_launch_count_n1=1 telethon_launch_count_n2=1",
            owner.id,
            target.id,
            len(gifts),
        )
        quote = self._tonnel.buy_offer_quote(self._offer_amount)
        total_required = quote.required_buyer_balance * len(gifts)
        batch_reference = self._batch_reference()
        if not gifts:
            empty = self._snapshot((), {})
            await self._db.finish_tonnel_transfer_batch(
                batch_id,
                status="EMPTY",
                total_count=0,
                success_count=0,
                failed_count=0,
                ambiguous_count=0,
                result_metadata={"batch_reference": batch_reference},
            )
            return TonnelTransferBatchResult(
                batch_id=batch_id,
                status="EMPTY",
                children=(),
                progress=empty,
                batch_reference=batch_reference,
                offer_amount=quote.offer_amount,
                per_gift_required=quote.required_buyer_balance,
                total_required=total_required,
                buyer_balance=buyer_balance.ton,
                error_code="NO_ELIGIBLE_GIFTS",
                error_message="Нет подарков, доступных для передачи.",
                batch_auth=batch_auth_metadata,
            )
        if buyer_balance.ton < total_required:
            blocked = self._snapshot(
                gifts, {index: "BLOCKED" for index in range(len(gifts))}
            )
            await self._db.finish_tonnel_transfer_batch(
                batch_id,
                status="FAILED",
                total_count=len(gifts),
                success_count=0,
                failed_count=len(gifts),
                ambiguous_count=0,
                result_metadata={
                    "batch_reference": batch_reference,
                    "error_code": "INSUFFICIENT_BATCH_BALANCE",
                },
            )
            return TonnelTransferBatchResult(
                batch_id=batch_id,
                status="FAILED",
                children=(),
                progress=blocked,
                batch_reference=batch_reference,
                offer_amount=quote.offer_amount,
                per_gift_required=quote.required_buyer_balance,
                total_required=total_required,
                buyer_balance=buyer_balance.ton,
                error_code="INSUFFICIENT_BATCH_BALANCE",
                error_message="Недостаточно баланса N1 для передачи всех подарков.",
                batch_auth=batch_auth_metadata,
            )

        child_confirmation_key = hashlib.sha256(
            f"tonnel-batch:{batch_id}:{batch_reference}".encode()
        ).hexdigest()
        children = await self._db.create_transfer_job_batch(
            owner_telegram_id=owner_telegram_id,
            market="tonnel",
            owner_account_id=owner_account_id,
            target_account_id=target_account_id,
            assets=[(gift.gift_id, gift.display_name) for gift in gifts],
            amount_text=self._tonnel.format_market_price(self._offer_amount),
            amount_atomic=quote.offer_amount_nanotons,
            confirmation_key=child_confirmation_key,
            batch_id=batch_id,
        )
        states = {job.id: "QUEUED" for job in children}
        reasons: dict[int, str | None] = {job.id: None for job in children}
        state_lock = asyncio.Lock()
        notify_lock = asyncio.Lock()
        queue: asyncio.Queue[TransferJob | None] = asyncio.Queue()
        for child in children:
            queue.put_nowait(child)

        async def notify() -> None:
            if progress is None:
                return
            async with notify_lock:
                async with state_lock:
                    snapshot = self._progress_from_jobs(children, states, reasons)
                try:
                    await progress(snapshot)
                except Exception as exc:  # noqa: BLE001 - UI cannot stop mutations
                    logger.warning(
                        "Tonnel batch progress failed batch_ref=%s exception=%s",
                        batch_reference,
                        type(exc).__name__,
                    )

        async def worker() -> None:
            while True:
                child = await queue.get()
                try:
                    if child is None:
                        return
                    async with state_lock:
                        states[child.id] = "RUNNING"
                    await notify()
                    try:
                        result = await self._child_jobs.execute_reserved_tonnel_transfer(
                            child.id,
                            batch_auth_metadata=batch_auth_metadata,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Tonnel batch child failed batch_ref=%s job_id=%d "
                            "exception=%s",
                            batch_reference,
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
                                error_message="Результат операции требует проверки.",
                                result_metadata={},
                            )
                        elif current is not None:
                            result = current
                        else:
                            raise
                    async with state_lock:
                        states[child.id] = result.status
                        reasons[child.id] = result.error_message
                    await notify()
                finally:
                    queue.task_done()

        await notify()
        worker_count = min(self._max_concurrency, len(children))
        workers = [
            asyncio.create_task(worker(), name=f"tonnel-batch-{batch_reference}-{i}")
            for i in range(worker_count)
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
        completed: list[TransferJob] = []
        for child in children:
            current_job = await self._db.get_transfer_job(child.id)
            if current_job is not None:
                completed.append(current_job)
        current = tuple(completed)
        final = self._progress_from_jobs(current, states, reasons)
        if final.dry_run_count == final.total_count:
            status = "DRY_RUN"
        elif final.success_count == final.total_count:
            status = "SUCCESS"
        elif final.success_count or final.dry_run_count:
            status = "PARTIAL"
        elif final.ambiguous_count:
            status = "AMBIGUOUS"
        else:
            status = "FAILED"
        await self._db.finish_tonnel_transfer_batch(
            batch_id,
            status=status,
            total_count=final.total_count,
            success_count=final.success_count + final.dry_run_count,
            failed_count=final.failed_count,
            ambiguous_count=final.ambiguous_count,
            result_metadata={
                "batch_reference": batch_reference,
                "batch_auth": batch_auth_metadata,
            },
        )
        return TonnelTransferBatchResult(
            batch_id=batch_id,
            status=status,
            children=current,
            progress=final,
            batch_reference=batch_reference,
            offer_amount=quote.offer_amount,
            per_gift_required=quote.required_buyer_balance,
            total_required=total_required,
            buyer_balance=buyer_balance.ton,
            batch_auth=batch_auth_metadata,
        )

    async def _finish_unexpected_batch(self, batch_id: int, *, ambiguous: bool) -> None:
        current = await self._db.get_tonnel_transfer_batch(batch_id)
        if current is None or current.status not in {"PENDING", "RUNNING"}:
            return
        children = await self._db.list_transfer_jobs_by_batch(batch_id)
        has_started_child = any(
            child.status in {"RUNNING", "SUCCESS", "AMBIGUOUS"} for child in children
        )
        status = "AMBIGUOUS" if ambiguous or has_started_child else "FAILED"
        await self._db.finish_tonnel_transfer_batch(
            batch_id,
            status=status,
            total_count=len(children),
            success_count=sum(
                child.status in {"SUCCESS", "DRY_RUN"} for child in children
            ),
            failed_count=sum(child.status == "FAILED" for child in children),
            ambiguous_count=sum(child.status == "AMBIGUOUS" for child in children),
            result_metadata={"unexpected_stop": True},
        )

    @staticmethod
    def _validate_accounts(owner: Account | None, target: Account | None) -> None:
        if owner is None or owner.role != "OWNER":
            raise ValueError("Owner account is unavailable")
        if target is None or target.role != "TARGET":
            raise ValueError("Target account is unavailable")

    @classmethod
    def _eligible_gifts(
        cls,
        gifts: Sequence[TonnelMarketGift],
        target: Account,
        active_ids: Sequence[str | int],
    ) -> list[TonnelMarketGift]:
        active = {(type(value), value) for value in active_ids}
        id_counts = Counter((type(gift.gift_id), gift.gift_id) for gift in gifts)
        sale_counts = Counter(gift.sale_id for gift in gifts)
        return [
            gift
            for gift in gifts
            if gift.eligible
            and gift.sale_id
            and id_counts[(type(gift.gift_id), gift.gift_id)] == 1
            and sale_counts[gift.sale_id] == 1
            and (type(gift.gift_id), gift.gift_id) not in active
            and (
                gift.seller_telegram_id is None
                or gift.seller_telegram_id == target.telegram_account_id
            )
        ]

    @staticmethod
    def _progress_from_jobs(
        jobs: Sequence[TransferJob],
        states: dict[int, str],
        reasons: dict[int, str | None],
    ) -> TonnelBatchProgress:
        allowed = {
            "QUEUED",
            "PENDING",
            "RUNNING",
            "SUCCESS",
            "DRY_RUN",
            "FAILED",
            "BLOCKED",
            "AMBIGUOUS",
        }
        unknown = {
            states.get(job.id, job.status)
            for job in jobs
            if states.get(job.id, job.status) not in allowed
        }
        if unknown:
            raise AssertionError(f"Unknown Tonnel batch child states: {sorted(unknown)}")
        result = TonnelBatchProgress(
            total_count=len(jobs),
            success_count=sum(states.get(job.id) == "SUCCESS" for job in jobs),
            failed_count=sum(states.get(job.id) in {"FAILED", "BLOCKED"} for job in jobs),
            ambiguous_count=sum(states.get(job.id) == "AMBIGUOUS" for job in jobs),
            dry_run_count=sum(states.get(job.id) == "DRY_RUN" for job in jobs),
            running_count=sum(states.get(job.id) == "RUNNING" for job in jobs),
            queued_count=sum(
                states.get(job.id) in {"QUEUED", "PENDING"} for job in jobs
            ),
            items=tuple(
                TonnelBatchItemProgress(
                    display_name=job.display_name or "Подарок",
                    status=states.get(job.id, job.status),
                    safe_reason=reasons.get(job.id),
                )
                for job in jobs
            ),
        )
        classified = (
            result.success_count
            + result.dry_run_count
            + result.failed_count
            + result.ambiguous_count
            + result.running_count
            + result.queued_count
        )
        if classified != result.total_count:
            raise AssertionError("Tonnel batch child classification is incomplete")
        return result

    @staticmethod
    def _snapshot(
        gifts: Sequence[TonnelMarketGift], states: dict[int, str]
    ) -> TonnelBatchProgress:
        return TonnelBatchProgress(
            total_count=len(gifts),
            success_count=0,
            failed_count=sum(state == "BLOCKED" for state in states.values()),
            ambiguous_count=0,
            dry_run_count=0,
            running_count=0,
            queued_count=sum(state == "QUEUED" for state in states.values()),
            items=tuple(
                TonnelBatchItemProgress(
                    gift.display_name,
                    states.get(index, "QUEUED"),
                )
                for index, gift in enumerate(gifts)
            ),
        )

    @staticmethod
    def _batch_reference() -> str:
        return hashlib.sha256(secrets.token_bytes(32)).hexdigest()[:12]


__all__ = [
    "TonnelBatchItemProgress",
    "TonnelBatchProgress",
    "TonnelTransferBatchResult",
    "TonnelTransferBatchService",
]
