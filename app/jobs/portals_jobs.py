from __future__ import annotations

import asyncio
import html
import logging
from collections.abc import Awaitable, Callable
from enum import StrEnum

from telethon.errors import FloodWaitError

from app.db import Database, PortalsJob
from app.telegram_client import (
    MiniAppAccountNotFoundError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    PortalsAcceptStatus,
    PortalsApiError,
    PortalsAuthenticationError,
    PortalsMutationNetworkError,
    PortalsNetworkError,
    PortalsOffer,
    PortalsOfferChangedError,
    PortalsOfferNotFoundError,
    PortalsOfferUnavailableError,
    PortalsService,
    PortalsServiceError,
)

logger = logging.getLogger(__name__)


class PortalsJobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"
    DRY_RUN = "DRY_RUN"


class PortalsJobErrorCode(StrEnum):
    ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
    SESSION_REVOKED = "SESSION_REVOKED"
    PORTALS_AUTH_FAILED = "PORTALS_AUTH_FAILED"
    EMPTY_OFFERS = "EMPTY_OFFERS"
    AMBIGUOUS_OFFER = "AMBIGUOUS_OFFER"
    OFFER_NOT_FOUND = "OFFER_NOT_FOUND"
    OFFER_CHANGED = "OFFER_CHANGED"
    OFFER_NOT_ELIGIBLE = "OFFER_NOT_ELIGIBLE"
    ACCEPT_REJECTED = "ACCEPT_REJECTED"
    ACCEPT_AMBIGUOUS = "ACCEPT_AMBIGUOUS"
    RATE_LIMITED = "RATE_LIMITED"
    SERVER_ERROR = "SERVER_ERROR"
    SUCCESS = "SUCCESS"


JobNotifier = Callable[[int, str], Awaitable[None]]


class PortalsJobService:
    """Persist and execute one non-replayable Portals accept attempt per job."""

    def __init__(
        self,
        db: Database,
        portals_service: PortalsService,
        *,
        notifier: JobNotifier | None = None,
    ) -> None:
        self._db = db
        self._portals_service = portals_service
        self._notifier = notifier
        self._locks_guard = asyncio.Lock()
        self._job_locks: dict[int, asyncio.Lock] = {}
        self._account_locks: dict[tuple[int, int], asyncio.Lock] = {}

    async def execute_portals_job(
        self,
        owner_telegram_id: int,
        account_id: int,
        offer_id: str | int | None = None,
    ) -> PortalsJob:
        job = await self._db.create_portals_job(
            owner_telegram_id=owner_telegram_id,
            account_id=account_id,
            offer_id=offer_id,
        )
        return await self.execute_existing_job(job.id, owner_telegram_id)

    async def execute_existing_job(
        self, job_id: int, owner_telegram_id: int
    ) -> PortalsJob:
        job_lock = await self._lock_for_job(job_id)
        async with job_lock:
            current = await self._db.get_portals_job(job_id, owner_telegram_id)
            if current is None:
                raise LookupError("Portals job not found")
            if current.status not in {
                PortalsJobStatus.PENDING,
                PortalsJobStatus.RUNNING,
            }:
                return current
            claimed = await self._db.claim_portals_job(job_id)
            if claimed is None:
                latest = await self._db.get_portals_job(job_id, owner_telegram_id)
                if latest is None:
                    raise LookupError("Portals job not found")
                return latest

            account_lock = await self._lock_for_account(
                claimed.owner_telegram_id, claimed.account_id
            )
            async with account_lock:
                completed = await self._run_claimed_job(claimed)
            await self._notify(completed)
            return completed

    async def _run_claimed_job(self, job: PortalsJob) -> PortalsJob:
        account = await self._db.get_account(job.account_id, job.owner_telegram_id)
        if account is None:
            return await self._fail(
                job,
                PortalsJobErrorCode.ACCOUNT_NOT_FOUND,
                "Выбранный подключённый аккаунт больше не существует.",
            )

        try:
            offers = await self._portals_service.get_received_offers(
                job.account_id,
                owner_telegram_id=job.owner_telegram_id,
            )
        except Exception as exc:  # noqa: BLE001 - sanitized persistence boundary
            return await self._fail_for_exception(job, exc, stage="offers")

        selected, selection_error = self._select_offer(offers, job.offer_id)
        if selection_error is not None:
            code, message = selection_error
            return await self._fail(job, code, message)
        assert selected is not None

        try:
            await self._db.update_portals_job_target(
                job.id,
                offer_id=selected.offer_id,
                nft_id=selected.nft_id,
                amount=selected.amount,
                display_name=selected.display_name,
            )
        except (TypeError, ValueError):
            return await self._fail(
                job,
                PortalsJobErrorCode.OFFER_NOT_FOUND,
                "Portals вернул неподдерживаемые данные оффера.",
            )

        try:
            result = await self._portals_service.accept_offer(
                owner_telegram_id=job.owner_telegram_id,
                account_id=job.account_id,
                offer_id=selected.offer_id,
                nft_id=selected.nft_id,
                amount=selected.amount,
                display_name=selected.display_name,
            )
        except Exception as exc:  # noqa: BLE001 - sanitized persistence boundary
            return await self._fail_for_exception(job, exc, stage="accept")

        metadata: dict[str, object] = {
            "dry_run": result.status is PortalsAcceptStatus.DRY_RUN,
            "accept_request_sent": result.accept_request_sent,
            "response_status": result.response_status,
            "response_fields": list(result.response_fields),
            "verification_fields": list(result.verification_fields),
        }
        if result.status is PortalsAcceptStatus.SUCCESS:
            return await self._finish(
                job,
                PortalsJobStatus.SUCCESS,
                PortalsJobErrorCode.SUCCESS,
                "Portals подтвердил принятие оффера.",
                metadata,
            )
        if result.status is PortalsAcceptStatus.DRY_RUN:
            return await self._finish(
                job,
                PortalsJobStatus.DRY_RUN,
                PortalsJobErrorCode.SUCCESS,
                "Проверка завершена. Оффер не был принят.",
                metadata,
            )
        return await self._finish(
            job,
            PortalsJobStatus.AMBIGUOUS,
            PortalsJobErrorCode.ACCEPT_AMBIGUOUS,
            (
                "Результат принятия оффера не определён. Проверьте Portals "
                "вручную перед новой попыткой."
            ),
            metadata,
        )

    @staticmethod
    def _select_offer(
        offers: list[PortalsOffer], requested_id: str | int | None
    ) -> tuple[
        PortalsOffer | None,
        tuple[PortalsJobErrorCode, str] | None,
    ]:
        if requested_id is not None:
            selected = next(
                (
                    offer
                    for offer in offers
                    if type(offer.offer_id) is type(requested_id)
                    and offer.offer_id == requested_id
                ),
                None,
            )
            if selected is None:
                return None, (
                    PortalsJobErrorCode.OFFER_NOT_FOUND,
                    "Запрошенный оффер больше не найден в Portals.",
                )
            if not selected.eligible:
                return None, (
                    PortalsJobErrorCode.OFFER_NOT_ELIGIBLE,
                    selected.eligibility_reason or "Этот оффер нельзя принять.",
                )
            return selected, None

        eligible = [offer for offer in offers if offer.eligible]
        if not eligible:
            return None, (
                PortalsJobErrorCode.EMPTY_OFFERS,
                "В Portals нет доступных полученных офферов.",
            )
        if len(eligible) > 1:
            return None, (
                PortalsJobErrorCode.AMBIGUOUS_OFFER,
                "Доступно несколько офферов; выберите один явно.",
            )
        return eligible[0], None

    async def _fail_for_exception(
        self, job: PortalsJob, exc: Exception, *, stage: str
    ) -> PortalsJob:
        logger.warning(
            "Portals job failed job_id=%d account_db_id=%d stage=%s exception=%s",
            job.id,
            job.account_id,
            stage,
            type(exc).__name__,
        )
        if isinstance(exc, MiniAppAccountNotFoundError):
            return await self._fail(
                job,
                PortalsJobErrorCode.ACCOUNT_NOT_FOUND,
                "Выбранный подключённый аккаунт больше не существует.",
            )
        if isinstance(
            exc, (MiniAppSessionMissingError, MiniAppSessionUnauthorizedError)
        ):
            return await self._fail(
                job,
                PortalsJobErrorCode.SESSION_REVOKED,
                "Авторизация аккаунта больше не действует. Подключите его заново.",
            )
        if isinstance(exc, PortalsAuthenticationError):
            return await self._fail(
                job,
                PortalsJobErrorCode.PORTALS_AUTH_FAILED,
                "Portals временно недоступен. Попробуйте позже.",
            )
        if isinstance(exc, FloodWaitError):
            return await self._fail(
                job,
                PortalsJobErrorCode.RATE_LIMITED,
                "Слишком много запросов. Попробуйте немного позже.",
            )
        if isinstance(exc, PortalsOfferChangedError):
            return await self._fail(
                job,
                PortalsJobErrorCode.OFFER_CHANGED,
                "Оффер изменился перед подтверждением и не был принят.",
            )
        if isinstance(exc, PortalsOfferNotFoundError):
            return await self._fail(
                job,
                PortalsJobErrorCode.OFFER_NOT_FOUND,
                "Оффер исчез перед подтверждением; запрос принятия не отправлялся.",
            )
        if isinstance(exc, PortalsOfferUnavailableError):
            return await self._fail(
                job,
                PortalsJobErrorCode.OFFER_NOT_ELIGIBLE,
                "Оффер больше недоступен; запрос принятия не отправлялся.",
            )
        if isinstance(exc, PortalsApiError):
            if exc.status == 429:
                code = PortalsJobErrorCode.RATE_LIMITED
            elif exc.status >= 500:
                code = PortalsJobErrorCode.SERVER_ERROR
            elif stage == "accept":
                code = PortalsJobErrorCode.ACCEPT_REJECTED
            else:
                code = PortalsJobErrorCode.PORTALS_AUTH_FAILED
            if exc.status == 429:
                message = "Слишком много запросов. Попробуйте немного позже."
            elif exc.status >= 500:
                message = "Portals временно недоступен."
            else:
                message = "Portals отклонил принятие оффера. Проверьте его статус."
            return await self._fail(job, code, message)
        if isinstance(exc, PortalsMutationNetworkError):
            return await self._finish(
                job,
                PortalsJobStatus.AMBIGUOUS,
                PortalsJobErrorCode.ACCEPT_AMBIGUOUS,
                "Результат принятия не определён. Проверьте Portals вручную.",
                {"accept_request_sent": True},
            )
        if isinstance(exc, (PortalsNetworkError, PortalsServiceError, OSError)):
            return await self._fail(
                job,
                PortalsJobErrorCode.SERVER_ERROR,
                "Telegram или Portals временно недоступен.",
            )
        return await self._fail(
            job,
            PortalsJobErrorCode.SERVER_ERROR,
            "Что-то пошло не так. Попробуйте позже.",
        )

    async def _fail(
        self,
        job: PortalsJob,
        code: PortalsJobErrorCode,
        message: str,
        metadata: dict[str, object] | None = None,
    ) -> PortalsJob:
        return await self._finish(
            job, PortalsJobStatus.FAILED, code, message, metadata or {}
        )

    async def _finish(
        self,
        job: PortalsJob,
        status: PortalsJobStatus,
        code: PortalsJobErrorCode,
        message: str,
        metadata: dict[str, object],
    ) -> PortalsJob:
        return await self._db.finish_portals_job(
            job.id,
            status=status,
            error_code=code,
            error_message=message[:300],
            result_metadata=metadata,
        )

    async def _notify(self, job: PortalsJob) -> None:
        if self._notifier is None:
            return
        amount_text = (
            html.escape(str(job.amount)[:40]) if job.amount is not None else "—"
        )
        nft = html.escape(job.display_name or "NFT")
        if job.status == PortalsJobStatus.SUCCESS.value:
            text = f"<b>✅ Оффер принят</b>\n\nNFT: {nft}\nСумма: {amount_text} TON"
        elif job.status == PortalsJobStatus.DRY_RUN.value:
            text = (
                "<b>🧪 Проверка завершена</b>\n\n"
                "Оффер доступен для принятия.\n"
                "Реальное принятие не выполнялось."
            )
        elif job.status == PortalsJobStatus.AMBIGUOUS.value:
            text = (
                "<b>⚠️ Требуется проверка</b>\n\n"
                "Результат операции нельзя подтвердить автоматически.\n\n"
                "Проверьте оффер в Portals перед повторной попыткой."
            )
        else:
            text = (
                "<b>❌ Не удалось принять оффер</b>\n\n"
                f"{html.escape(job.error_message or 'Попробуйте позже.')}"
            )
        try:
            await self._notifier(job.owner_telegram_id, text)
        except Exception as exc:  # noqa: BLE001 - notification cannot alter result
            logger.warning(
                "Portals job notification failed job_id=%d exception=%s",
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


async def execute_portals_job(
    service: PortalsJobService,
    owner_telegram_id: int,
    account_id: int,
    offer_id: str | int | None = None,
) -> PortalsJob:
    """Callable entry point retained for future in-process automation."""
    return await service.execute_portals_job(
        owner_telegram_id=owner_telegram_id,
        account_id=account_id,
        offer_id=offer_id,
    )
