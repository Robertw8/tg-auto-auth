from __future__ import annotations

import asyncio
import html
import logging
from collections.abc import Awaitable, Callable
from enum import StrEnum

from telethon.errors import FloodWaitError

from app.db import Database, MrktJob
from app.telegram_client import (
    MiniAppAccountNotFoundError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    MrktApiError,
    MrktAuthenticationError,
    MrktGift,
    MrktGiftUnavailableError,
    MrktListingInFlightError,
    MrktListingStatus,
    MrktNetworkError,
    MrktPriceError,
    MrktService,
    MrktServiceError,
)

logger = logging.getLogger(__name__)


class MrktJobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"
    DRY_RUN = "DRY_RUN"


class MrktJobErrorCode(StrEnum):
    ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
    SESSION_REVOKED = "SESSION_REVOKED"
    MRKT_AUTH_FAILED = "MRKT_AUTH_FAILED"
    EMPTY_STORAGE = "EMPTY_STORAGE"
    AMBIGUOUS_GIFT = "AMBIGUOUS_GIFT"
    GIFT_NOT_FOUND = "GIFT_NOT_FOUND"
    GIFT_NOT_ELIGIBLE = "GIFT_NOT_ELIGIBLE"
    INVALID_PRICE = "INVALID_PRICE"
    SALE_REJECTED = "SALE_REJECTED"
    SALE_AMBIGUOUS = "SALE_AMBIGUOUS"
    RATE_LIMITED = "RATE_LIMITED"
    SERVER_ERROR = "SERVER_ERROR"
    SUCCESS = "SUCCESS"


JobNotifier = Callable[[int, str], Awaitable[None]]


class MrktJobService:
    """Persist and execute one safe MRKT listing attempt per job."""

    def __init__(
        self,
        db: Database,
        mrkt_service: MrktService,
        *,
        notifier: JobNotifier | None = None,
    ) -> None:
        self._db = db
        self._mrkt_service = mrkt_service
        self._notifier = notifier
        self._locks_guard = asyncio.Lock()
        self._job_locks: dict[int, asyncio.Lock] = {}
        self._account_locks: dict[tuple[int, int], asyncio.Lock] = {}

    async def execute_mrkt_job(
        self,
        owner_telegram_id: int,
        account_id: int,
        price_ton: str,
        gift_id: str | int | None = None,
    ) -> MrktJob:
        job = await self._db.create_mrkt_job(
            owner_telegram_id=owner_telegram_id,
            account_id=account_id,
            gift_id=gift_id,
            price_ton=price_ton,
        )
        return await self.execute_existing_job(job.id, owner_telegram_id)

    async def execute_existing_job(
        self,
        job_id: int,
        owner_telegram_id: int,
    ) -> MrktJob:
        job_lock = await self._lock_for_job(job_id)
        async with job_lock:
            current = await self._db.get_mrkt_job(job_id, owner_telegram_id)
            if current is None:
                raise LookupError("MRKT job not found")
            if current.status not in {
                MrktJobStatus.PENDING,
                MrktJobStatus.RUNNING,
            }:
                return current
            claimed = await self._db.claim_mrkt_job(job_id)
            if claimed is None:
                # Another process claimed it. Never execute or retry a RUNNING job.
                latest = await self._db.get_mrkt_job(job_id, owner_telegram_id)
                if latest is None:
                    raise LookupError("MRKT job not found")
                return latest

            account_lock = await self._lock_for_account(
                claimed.owner_telegram_id, claimed.account_id
            )
            async with account_lock:
                completed = await self._run_claimed_job(claimed)
            await self._notify(completed)
            return completed

    async def _run_claimed_job(self, job: MrktJob) -> MrktJob:
        account = await self._db.get_account(job.account_id, job.owner_telegram_id)
        if account is None:
            return await self._fail(
                job,
                MrktJobErrorCode.ACCOUNT_NOT_FOUND,
                "Выбранный подключённый аккаунт больше не существует.",
            )

        try:
            price_nanotons = self._mrkt_service.ton_to_nanotons(job.price_ton)
            formatted_price = self._mrkt_service.format_ton(job.price_ton)
        except (MrktPriceError, ArithmeticError, ValueError):
            return await self._fail(
                job,
                MrktJobErrorCode.INVALID_PRICE,
                "Введите положительную цену в TON, не более двух знаков после точки.",
            )

        try:
            inventory = await self._mrkt_service.get_inventory(
                job.account_id,
                owner_telegram_id=job.owner_telegram_id,
                is_listed=False,
            )
        except Exception as exc:  # noqa: BLE001 - map to safe persisted code
            return await self._fail_for_exception(job, exc, stage="inventory")

        selected, selection_error = self._select_gift(inventory, job.gift_id)
        if selection_error is not None:
            code, message = selection_error
            return await self._fail(job, code, message)
        assert selected is not None
        if isinstance(selected.gift_id, bool) or not isinstance(
            selected.gift_id, (str, int)
        ):
            return await self._fail(
                job,
                MrktJobErrorCode.GIFT_NOT_FOUND,
                "MRKT вернул неподдерживаемый идентификатор подарка.",
            )

        await self._db.update_mrkt_job_target(
            job.id,
            gift_id=selected.gift_id,
            price_ton=formatted_price,
            price_nanotons=price_nanotons,
            display_name=selected.display_name,
        )
        try:
            # This method performs a fresh launch/auth and a final inventory fetch.
            # Its sale boundary sends POST /gifts/sale no more than once.
            result = await self._mrkt_service.execute_job_listing(
                owner_telegram_id=job.owner_telegram_id,
                account_id=job.account_id,
                gift_id=selected.gift_id,
                display_name=selected.display_name,
                price_ton=formatted_price,
            )
        except MrktGiftUnavailableError:
            return await self._fail(
                job,
                MrktJobErrorCode.GIFT_NOT_FOUND,
                "Перед продажей выбранный подарок исчез или стал недоступен.",
            )
        except Exception as exc:  # noqa: BLE001 - map to safe persisted code
            return await self._fail_for_exception(job, exc, stage="sale")

        metadata: dict[str, object] = {
            "dry_run": result.status is MrktListingStatus.DRY_RUN,
            "sale_request_sent": result.sale_request_sent,
            "response_fields": list(result.response_fields),
            "response_status": result.response_status,
        }
        if result.status is MrktListingStatus.SUCCESS:
            return await self._finish(
                job,
                MrktJobStatus.SUCCESS,
                MrktJobErrorCode.SUCCESS,
                "MRKT подтвердил одно размещение.",
                metadata,
            )
        if result.status is MrktListingStatus.DRY_RUN:
            return await self._finish(
                job,
                MrktJobStatus.DRY_RUN,
                MrktJobErrorCode.SUCCESS,
                "Тестовый запуск завершён; запрос на продажу не отправлялся.",
                metadata,
            )
        if result.status is MrktListingStatus.AMBIGUOUS:
            return await self._finish(
                job,
                MrktJobStatus.AMBIGUOUS,
                MrktJobErrorCode.SALE_AMBIGUOUS,
                "Не удалось подтвердить результат размещения. Проверьте MRKT перед повторной попыткой.",
                metadata,
            )
        return await self._fail(
            job,
            MrktJobErrorCode.SALE_REJECTED,
            "MRKT не подтвердил размещение.",
            metadata,
        )

    @staticmethod
    def _select_gift(
        inventory: list[MrktGift], requested_id: str | int | None
    ) -> tuple[
        MrktGift | None,
        tuple[MrktJobErrorCode, str] | None,
    ]:
        if requested_id is not None:
            selected = next(
                (
                    gift
                    for gift in inventory
                    if type(gift.gift_id) is type(requested_id)
                    and gift.gift_id == requested_id
                ),
                None,
            )
            if selected is None:
                return None, (
                    MrktJobErrorCode.GIFT_NOT_FOUND,
                    "Запрошенного подарка нет среди неразмещённых подарков MRKT.",
                )
            if not selected.eligible:
                return None, (
                    MrktJobErrorCode.GIFT_NOT_ELIGIBLE,
                    selected.eligibility_reason
                    or "Запрошенный подарок нельзя разместить.",
                )
            return selected, None

        eligible = [gift for gift in inventory if gift.eligible]
        if not eligible:
            return None, (
                MrktJobErrorCode.EMPTY_STORAGE,
                "В хранилище MRKT нет доступных подарков.",
            )
        if len(eligible) > 1:
            return None, (
                MrktJobErrorCode.AMBIGUOUS_GIFT,
                "Доступно несколько подарков; выберите один явно.",
            )
        return eligible[0], None

    async def _fail_for_exception(
        self, job: MrktJob, exc: Exception, *, stage: str
    ) -> MrktJob:
        logger.warning(
            "MRKT job failed job_id=%d account_db_id=%d stage=%s exception=%s",
            job.id,
            job.account_id,
            stage,
            type(exc).__name__,
        )
        if isinstance(exc, MiniAppAccountNotFoundError):
            return await self._fail(
                job,
                MrktJobErrorCode.ACCOUNT_NOT_FOUND,
                "Выбранный подключённый аккаунт больше не существует.",
            )
        if isinstance(
            exc, (MiniAppSessionMissingError, MiniAppSessionUnauthorizedError)
        ):
            return await self._fail(
                job,
                MrktJobErrorCode.SESSION_REVOKED,
                "Авторизация аккаунта больше не действует. Подключите его заново.",
            )
        if isinstance(exc, MrktAuthenticationError):
            return await self._fail(
                job,
                MrktJobErrorCode.MRKT_AUTH_FAILED,
                "MRKT временно недоступен. Попробуйте позже.",
            )
        if isinstance(exc, FloodWaitError):
            return await self._fail(
                job,
                MrktJobErrorCode.RATE_LIMITED,
                "Слишком много запросов. Попробуйте немного позже.",
            )
        if isinstance(exc, MrktApiError):
            if exc.status == 429:
                code = MrktJobErrorCode.RATE_LIMITED
            elif exc.status >= 500:
                code = MrktJobErrorCode.SERVER_ERROR
            elif stage == "sale":
                code = MrktJobErrorCode.SALE_REJECTED
            else:
                code = MrktJobErrorCode.MRKT_AUTH_FAILED
            if exc.status == 429:
                message = "Слишком много запросов. Попробуйте немного позже."
            elif exc.status >= 500:
                message = "MRKT временно недоступен."
            else:
                message = "MRKT отклонил размещение. Проверьте подарок и цену."
            return await self._fail(job, code, message)
        if isinstance(exc, MrktListingInFlightError):
            return await self._fail(
                job,
                MrktJobErrorCode.SALE_REJECTED,
                "Для этого аккаунта уже выполняется другое размещение.",
            )
        if isinstance(exc, (MrktNetworkError, MrktServiceError, OSError)):
            return await self._fail(
                job,
                MrktJobErrorCode.SERVER_ERROR,
                "Telegram или MRKT временно недоступен.",
            )
        return await self._fail(
            job,
            MrktJobErrorCode.SERVER_ERROR,
            "Что-то пошло не так. Попробуйте позже.",
        )

    async def _fail(
        self,
        job: MrktJob,
        code: MrktJobErrorCode,
        message: str,
        metadata: dict[str, object] | None = None,
    ) -> MrktJob:
        return await self._finish(
            job, MrktJobStatus.FAILED, code, message, metadata or {}
        )

    async def _finish(
        self,
        job: MrktJob,
        status: MrktJobStatus,
        code: MrktJobErrorCode,
        message: str,
        metadata: dict[str, object],
    ) -> MrktJob:
        return await self._db.finish_mrkt_job(
            job.id,
            status=status,
            error_code=code,
            error_message=message[:300],
            result_metadata=metadata,
        )

    async def _notify(self, job: MrktJob) -> None:
        if self._notifier is None:
            return
        gift = html.escape(job.display_name or "Подарок")
        price = html.escape(job.price_ton)
        if job.status == MrktJobStatus.SUCCESS.value:
            text = f"<b>✅ Подарок размещён</b>\n\nПодарок: {gift}\nЦена: {price} TON"
        elif job.status == MrktJobStatus.DRY_RUN.value:
            text = (
                "<b>🧪 Проверка завершена</b>\n\n"
                "Все данные корректны.\n"
                "Реальное размещение не выполнялось."
            )
        elif job.status == MrktJobStatus.AMBIGUOUS.value:
            text = (
                "<b>⚠️ Требуется проверка</b>\n\n"
                "Не удалось точно определить результат операции.\n\n"
                "Не запускайте размещение повторно, пока не проверите подарок в MRKT."
            )
        else:
            text = (
                "<b>❌ Не удалось разместить подарок</b>\n\n"
                f"{html.escape(job.error_message or 'Попробуйте позже.')}"
            )
        try:
            await self._notifier(job.owner_telegram_id, text)
        except Exception as exc:  # noqa: BLE001 - notification cannot change job result
            logger.warning(
                "MRKT job notification failed job_id=%d exception=%s",
                job.id,
                type(exc).__name__,
            )

    async def _lock_for_job(self, job_id: int) -> asyncio.Lock:
        async with self._locks_guard:
            return self._job_locks.setdefault(job_id, asyncio.Lock())

    async def _lock_for_account(
        self, owner_telegram_id: int, account_id: int
    ) -> asyncio.Lock:
        async with self._locks_guard:
            return self._account_locks.setdefault(
                (owner_telegram_id, account_id), asyncio.Lock()
            )

    async def shutdown(self) -> None:
        async with self._locks_guard:
            self._job_locks.clear()
            self._account_locks.clear()
