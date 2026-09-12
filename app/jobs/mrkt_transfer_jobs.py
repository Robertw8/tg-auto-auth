from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from enum import StrEnum

from app.db import Database, TransferJob
from app.live_verify import final_verification, selected_asset, verification_scope
from app.telegram_client import (
    MiniAppAccountNotFoundError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    MrktApiError,
    MrktFastTransferStatus,
    MrktGiftUnavailableError,
    MrktMutationNetworkError,
    MrktNetworkError,
    MrktPriceError,
    MrktService,
    MrktServiceError,
    MrktTransferMode,
)

logger = logging.getLogger(__name__)
ProgressCallback = Callable[[str], Awaitable[None]]


class MrktTransferStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"
    DRY_RUN = "DRY_RUN"


class MrktTransferPhase(StrEnum):
    VALIDATING = "VALIDATING"
    LISTING = "LISTING"
    LOCATING_LISTING = "LOCATING_LISTING"
    BUYING = "BUYING"
    COMPLETED = "COMPLETED"


class MrktTransferJobService:
    """Persist and execute one exact list-then-buy flow."""

    def __init__(
        self,
        db: Database,
        mrkt: MrktService,
        *,
        dry_run: bool = True,
        live_verify: bool = False,
        speculative_buy: bool = False,
        speculative_buy_delay_ms: int = 40,
        transfer_mode: MrktTransferMode | str | None = None,
        listed_trigger_max_wait_ms: int = 1_000,
        listed_trigger_poll_interval_ms: int = 10,
    ) -> None:
        self._db = db
        self._mrkt = mrkt
        self._dry_run = dry_run
        self._live_verify = live_verify
        self._transfer_mode = (
            MrktTransferMode(transfer_mode)
            if transfer_mode is not None
            else MrktTransferMode.SPECULATIVE
            if speculative_buy
            else MrktTransferMode.FAST_CONFIRMED
        )
        self._speculative_buy = self._transfer_mode is MrktTransferMode.SPECULATIVE
        self._speculative_buy_delay_ms = speculative_buy_delay_ms
        self._listed_trigger_max_wait_ms = listed_trigger_max_wait_ms
        self._listed_trigger_poll_interval_ms = listed_trigger_poll_interval_ms
        self._locks_guard = asyncio.Lock()
        self._account_locks: dict[tuple[int, int, int], asyncio.Lock] = {}

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    async def execute_mrkt_transfer(
        self,
        owner_telegram_id: int,
        buyer_account_id: int,
        seller_account_id: int,
        gift_id: str | int | None,
        price_ton: str,
        *,
        confirmation_nonce: str,
        progress: ProgressCallback | None = None,
    ) -> TransferJob:
        if not confirmation_nonce:
            raise ValueError("A MRKT confirmation nonce is required")
        price_nanotons = self._mrkt.ton_to_nanotons(price_ton)
        price_text = self._mrkt.format_ton(price_ton)
        confirmation_key = hashlib.sha256(confirmation_nonce.encode()).hexdigest()
        job = await self._db.create_transfer_job(
            owner_telegram_id=owner_telegram_id,
            market="mrkt",
            owner_account_id=buyer_account_id,
            target_account_id=seller_account_id,
            asset_id=gift_id,
            amount_text=price_text,
            amount_atomic=price_nanotons,
            confirmation_key=confirmation_key,
        )
        claimed = await self._db.claim_transfer_job(job.id)
        if claimed is None:
            return job
        lock = await self._lock_for_accounts(
            claimed.owner_telegram_id,
            claimed.owner_account_id,
            claimed.target_account_id,
        )
        async with lock:
            with verification_scope(claimed, self._live_verify):
                return await self._run(
                    claimed, gift_id=claimed.asset_id, progress=progress
                )

    async def _run(
        self,
        job: TransferJob,
        *,
        gift_id: str | int | None,
        progress: ProgressCallback | None,
    ) -> TransferJob:
        buyer = await self._db.get_account(job.owner_account_id, job.owner_telegram_id)
        seller = await self._db.get_account(
            job.target_account_id, job.owner_telegram_id
        )
        if buyer is None or buyer.role != "OWNER":
            return await self._fail(job, "NO_OWNER_ACCOUNT")
        if seller is None or seller.role != "TARGET":
            return await self._fail(job, "NO_TARGET_ACCOUNT")

        try:
            assert job.amount_atomic is not None
            await self._notify(progress, "1/3 Подготавливаем оба аккаунта…")
            await self._db.update_transfer_job(
                job.id,
                phase=MrktTransferPhase.LISTING,
            )
            await self._notify(progress, "2/3 Размещаем и сразу покупаем…")
            result = await self._mrkt.execute_fast_transfer(
                owner_telegram_id=job.owner_telegram_id,
                buyer_account_id=buyer.id,
                seller_account_id=seller.id,
                gift_id=gift_id,
                price_nanotons=job.amount_atomic,
                dry_run=self._dry_run,
                speculative_buy=self._speculative_buy,
                speculative_buy_delay_ms=self._speculative_buy_delay_ms,
                observe_listing_activation=self._live_verify,
                transfer_mode=self._transfer_mode,
                listed_trigger_max_wait_ms=self._listed_trigger_max_wait_ms,
                listed_trigger_poll_interval_ms=(
                    self._listed_trigger_poll_interval_ms
                ),
            )
            selected_asset(result.gift_id)
            await self._db.update_transfer_job(
                job.id,
                phase=(
                    MrktTransferPhase.BUYING
                    if result.buy_request_sent
                    else MrktTransferPhase.LISTING
                ),
                asset_id=result.gift_id,
                display_name=result.display_name,
            )
            await self._notify(progress, "3/3 Проверяем результат…")
            metadata: dict[str, object] = {
                "listing_sent": result.listing_request_sent,
                "buy_sent": result.buy_request_sent,
                "fast_path": True,
                "seller_price_nanotons": result.price_nanotons,
                "buyer_price_nanotons": result.buyer_price_nanotons,
                "commission_percent": result.commission_percent,
                "observed_listing_price_nanotons": (
                    result.listed_trigger.get("mrkt_price_correlation", {}).get(
                        "observed_listing_price_nanotons"
                    )
                    if isinstance(
                        result.listed_trigger.get("mrkt_price_correlation"), dict
                    )
                    else None
                ),
                "transfer_mode": self._transfer_mode.value,
                "speculative_buy_delay_ms": (
                    self._speculative_buy_delay_ms if self._speculative_buy else None
                ),
                "connection_reuse": result.verification.get("connection_reuse"),
                "mrkt_listing_observation": result.listing_observation,
                "mrkt_listed_trigger": result.listed_trigger,
                "mrkt_listed_trigger_probe_rtts_ms": result.listed_trigger.get(
                    "probe_rtts_ms", []
                ),
                **result.timings,
            }
            if result.status is MrktFastTransferStatus.DRY_RUN:
                return await self._finish(
                    job,
                    MrktTransferStatus.DRY_RUN,
                    MrktTransferPhase.COMPLETED,
                    None,
                    "Тестовый запуск завершён. Размещение и покупка не выполнялись.",
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.SUCCESS:
                return await self._finish(
                    job,
                    MrktTransferStatus.SUCCESS,
                    MrktTransferPhase.COMPLETED,
                    None,
                    "Подарок выставлен рабочим аккаунтом и куплен вашим аккаунтом.",
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.LISTING_AMBIGUOUS:
                return await self._finish(
                    job,
                    MrktTransferStatus.AMBIGUOUS,
                    MrktTransferPhase.LISTING,
                    "LISTING_AMBIGUOUS",
                    "Результат размещения не определён. Покупка не выполнялась.",
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.LISTING_REJECTED:
                sale_failed = (
                    self._transfer_mode is MrktTransferMode.LISTED_TRIGGERED
                    and result.listed_trigger.get("stopped_reason") == "SALE_FAILED"
                )
                return await self._finish(
                    job,
                    MrktTransferStatus.FAILED,
                    MrktTransferPhase.LISTING,
                    "SALE_FAILED" if sale_failed else "LISTING_FAILED",
                    (
                        "MRKT отклонил размещение. Покупка не выполнялась."
                        if sale_failed
                        else "Не удалось разместить подарок."
                    ),
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.LISTING_NOT_OBSERVED:
                return await self._finish(
                    job,
                    MrktTransferStatus.FAILED,
                    MrktTransferPhase.LOCATING_LISTING,
                    "LISTING_NOT_OBSERVED",
                    "MRKT не подтвердил появление точного размещения. Покупка не выполнялась.",
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.LISTING_RATE_LIMITED:
                return await self._finish(
                    job,
                    MrktTransferStatus.FAILED,
                    MrktTransferPhase.LOCATING_LISTING,
                    "RATE_LIMITED",
                    "MRKT ограничил проверки размещения. Покупка не выполнялась.",
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.EXTERNAL_SALE:
                return await self._finish(
                    job,
                    MrktTransferStatus.FAILED,
                    MrktTransferPhase.BUYING,
                    "GIFT_INTERCEPTED_OR_SOLD_EXTERNALLY",
                    "Подарок успел уйти другому покупателю. Размещение было создано, но наш аккаунт не успел выполнить покупку.",
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.SPECULATIVE_BUY_TOO_EARLY:
                return await self._finish(
                    job,
                    MrktTransferStatus.FAILED,
                    MrktTransferPhase.BUYING,
                    "SPECULATIVE_BUY_TOO_EARLY",
                    "Экспериментальная покупка началась слишком рано. Повторная покупка не выполнялась.",
                    metadata,
                    final_read=result.verification,
                )
            if result.status is MrktFastTransferStatus.BUY_REJECTED:
                return await self._finish(
                    job,
                    MrktTransferStatus.FAILED,
                    MrktTransferPhase.BUYING,
                    "BUY_FAILED",
                    "Покупка не выполнена. Подарок остался у рабочего аккаунта или в активном размещении.",
                    metadata,
                    final_read=result.verification,
                )
            return await self._finish(
                job,
                MrktTransferStatus.AMBIGUOUS,
                MrktTransferPhase.BUYING,
                "BUY_AMBIGUOUS",
                "Результат покупки не определён. Проверьте MRKT вручную.",
                metadata,
                final_read=result.verification,
            )
        except MrktMutationNetworkError:
            phase = await self._current_phase(job)
            code = (
                "BUY_AMBIGUOUS"
                if phase == MrktTransferPhase.BUYING
                else "LISTING_AMBIGUOUS"
            )
            return await self._ambiguous(job, code)
        except Exception as exc:  # noqa: BLE001 - safe boundary
            phase = await self._current_phase(job)
            if (
                isinstance(exc, MrktApiError)
                and exc.status >= 500
                and phase == MrktTransferPhase.LISTING
            ):
                return await self._ambiguous(job, "LISTING_AMBIGUOUS")
            logger.warning(
                "MRKT transfer failed job_id=%d phase=%s exception=%s",
                job.id,
                await self._current_phase(job),
                type(exc).__name__,
            )
            return await self._fail(job, self._error_code(exc))

    @staticmethod
    def _error_code(exc: Exception) -> str:
        if isinstance(exc, MiniAppAccountNotFoundError):
            return "NO_TARGET_ACCOUNT"
        if isinstance(
            exc, (MiniAppSessionMissingError, MiniAppSessionUnauthorizedError)
        ):
            return "SESSION_REVOKED"
        if isinstance(exc, MrktPriceError):
            return "LISTING_FAILED"
        if isinstance(exc, MrktGiftUnavailableError):
            return "AMBIGUOUS_LISTING"
        if isinstance(exc, MrktApiError):
            if exc.status == 429:
                return "RATE_LIMITED"
            if exc.code in {"INSUFFICIENT_FUNDS", "INSUFFICIENT_BALANCE"}:
                return "INSUFFICIENT_BALANCE"
            if exc.status in {404, 409}:
                return "ALREADY_SOLD"
            return "BUY_FAILED"
        if isinstance(exc, (MrktNetworkError, MrktServiceError, OSError)):
            return "BUY_FAILED"
        return "BUY_FAILED"

    async def _fail(self, job: TransferJob, code: str) -> TransferJob:
        messages = {
            "NO_OWNER_ACCOUNT": "Мой аккаунт не найден или имеет неверный тип.",
            "NO_TARGET_ACCOUNT": "Рабочий аккаунт не найден или имеет неверный тип.",
            "EMPTY_STORAGE": "В рабочем аккаунте нет доступных подарков.",
            "AMBIGUOUS_GIFT": "Найдено несколько подарков. Выберите один явно.",
            "GIFT_NOT_FOUND": "Выбранный подарок не найден.",
            "LISTING_FAILED": "Не удалось разместить подарок.",
            "LISTING_NOT_FOUND": "Созданное размещение не найдено. Покупка не выполнялась.",
            "AMBIGUOUS_LISTING": "Размещение нельзя однозначно сопоставить. Покупка не выполнялась.",
            "BUY_FAILED": "Не удалось купить подарок.",
            "INSUFFICIENT_BALANCE": "Недостаточно средств для покупки.",
            "ALREADY_SOLD": "Размещение уже продано или недоступно.",
            "RATE_LIMITED": "MRKT ограничил частоту запросов. Попробуйте позже.",
            "SESSION_REVOKED": "Авторизация одного из аккаунтов больше не действует.",
        }
        return await self._finish(
            job,
            MrktTransferStatus.FAILED,
            await self._current_phase(job),
            code,
            messages.get(code, "Не удалось выполнить операцию MRKT."),
            {},
        )

    async def _ambiguous(self, job: TransferJob, code: str) -> TransferJob:
        message = (
            "Результат покупки не определён. Проверьте MRKT вручную."
            if code == "BUY_AMBIGUOUS"
            else "Результат размещения не определён. Покупка не выполнялась."
        )
        return await self._finish(
            job,
            MrktTransferStatus.AMBIGUOUS,
            await self._current_phase(job),
            code,
            message,
            {},
        )

    async def _finish(
        self,
        job: TransferJob,
        status: str,
        phase: str,
        code: str | None,
        message: str,
        metadata: dict[str, object],
        *,
        final_read: dict[str, object] | None = None,
    ) -> TransferJob:
        report = await final_verification(
            job, self._mrkt, status, known_result=final_read
        )
        if report is not None:
            metadata = {**metadata, "live_verify": report}
        return await self._db.finish_transfer_job(
            job.id,
            status=status,
            phase=phase,
            error_code=code,
            error_message=message,
            result_metadata=metadata,
        )

    async def _lock_for_accounts(
        self, owner_id: int, buyer_id: int, seller_id: int
    ) -> asyncio.Lock:
        key = (owner_id, min(buyer_id, seller_id), max(buyer_id, seller_id))
        async with self._locks_guard:
            return self._account_locks.setdefault(key, asyncio.Lock())

    async def _current_phase(self, job: TransferJob) -> str:
        current = await self._db.get_transfer_job(job.id)
        return current.phase if current is not None else job.phase

    @staticmethod
    async def _notify(progress: ProgressCallback | None, text: str) -> None:
        if progress is not None:
            try:
                await progress(text)
            except Exception as exc:  # noqa: BLE001 - notification must not interrupt mutations
                logger.warning(
                    "Transfer progress delivery failed exception=%s", type(exc).__name__
                )

    async def shutdown(self) -> None:
        async with self._locks_guard:
            self._account_locks.clear()
