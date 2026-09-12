from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from decimal import Decimal
from enum import StrEnum

from app.db import Account, Database, TransferJob
from app.live_verify import final_verification, selected_asset, verification_scope
from app.telegram_client import (
    MiniAppAccountNotFoundError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    PortalsAcceptStatus,
    PortalsApiError,
    PortalsCreateOfferUnconfirmedError,
    PortalsMutationNetworkError,
    PortalsNetworkError,
    PortalsNft,
    PortalsOfferChangedError,
    PortalsOfferNotFoundError,
    PortalsService,
    PortalsServiceError,
)

logger = logging.getLogger(__name__)
ProgressCallback = Callable[[str], Awaitable[None]]
PORTALS_TRANSFER_AMOUNT = Decimal("0.53")


class PortalsTransferStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"
    DRY_RUN = "DRY_RUN"


class PortalsTransferPhase(StrEnum):
    VALIDATING = "VALIDATING"
    CREATING_OFFER = "CREATING_OFFER"
    LOCATING_OFFER = "LOCATING_OFFER"
    ACCEPTING = "ACCEPTING"
    COMPLETED = "COMPLETED"


class PortalsTransferJobService:
    """Persist and execute one tightly correlated owner-to-target offer flow."""

    def __init__(
        self,
        db: Database,
        portals: PortalsService,
        *,
        dry_run: bool = True,
        live_verify: bool = False,
        offer_amount: str | Decimal = PORTALS_TRANSFER_AMOUNT,
    ) -> None:
        self._db = db
        self._portals = portals
        self._dry_run = dry_run
        self._live_verify = live_verify
        self._offer_amount = PortalsService.normalize_offer_amount(
            format(Decimal(offer_amount), "f")
        )
        self._locks_guard = asyncio.Lock()
        self._account_locks: dict[
            tuple[int, int, int, str, str | int | None], asyncio.Lock
        ] = {}

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    @property
    def offer_amount(self) -> str:
        return self._offer_amount

    @asynccontextmanager
    async def batch_session_scope(
        self,
        owner_telegram_id: int,
        source_account_id: int,
        target_account_id: int,
    ) -> AsyncIterator[None]:
        async with self._portals.transfer_session_scope(
            owner_telegram_id, source_account_id, target_account_id
        ):
            yield

    async def execute_portals_transfer(
        self,
        owner_telegram_id: int,
        source_account_id: int,
        target_account_id: int,
        nft_id: str | int | None,
        *,
        progress: ProgressCallback | None = None,
        confirmation_nonce: str | None = None,
    ) -> TransferJob:
        offer_amount = self._offer_amount
        confirmation_key = (
            hashlib.sha256(confirmation_nonce.encode()).hexdigest()
            if confirmation_nonce
            else None
        )
        job = await self._db.create_transfer_job(
            owner_telegram_id=owner_telegram_id,
            market="portals",
            owner_account_id=source_account_id,
            target_account_id=target_account_id,
            asset_id=nft_id,
            amount_text=offer_amount,
            confirmation_key=confirmation_key,
        )
        return await self.execute_reserved_portals_transfer(job.id, progress=progress)

    async def execute_reserved_portals_transfer(
        self,
        job_id: int,
        *,
        progress: ProgressCallback | None = None,
    ) -> TransferJob:
        job = await self._db.get_transfer_job(job_id)
        if job is None or job.market != "portals":
            raise ValueError("Portals transfer job not found")
        claimed = await self._db.claim_transfer_job(job.id)
        if claimed is None:
            return job
        lock = await self._lock_for_accounts(
            claimed.owner_telegram_id,
            claimed.owner_account_id,
            claimed.target_account_id,
            claimed.asset_id,
        )
        async with lock:
            with verification_scope(claimed, self._live_verify or not self._dry_run):
                source, target = await asyncio.gather(
                    self._db.get_account(
                        claimed.owner_account_id, claimed.owner_telegram_id
                    ),
                    self._db.get_account(
                        claimed.target_account_id, claimed.owner_telegram_id
                    ),
                )
                if source is None or source.role != "OWNER":
                    return await self._fail(claimed, "NO_OWNER_ACCOUNT")
                if target is None or target.role != "TARGET":
                    return await self._fail(claimed, "NO_TARGET_ACCOUNT")
                try:
                    async with self._portals.transfer_session_scope(
                        claimed.owner_telegram_id,
                        claimed.owner_account_id,
                        claimed.target_account_id,
                    ):
                        return await self._run(
                            claimed,
                            source=source,
                            target=target,
                            nft_id=claimed.asset_id,
                            progress=progress,
                        )
                except Exception as exc:  # noqa: BLE001 - safe job boundary
                    logger.warning(
                        "Portals transfer auth preparation failed job_id=%d "
                        "exception=%s",
                        claimed.id,
                        type(exc).__name__,
                    )
                    return await self._fail(
                        claimed,
                        self._error_code(exc, PortalsTransferPhase.VALIDATING),
                    )

    async def _run(
        self,
        job: TransferJob,
        *,
        source: Account,
        target: Account,
        nft_id: str | int | None,
        progress: ProgressCallback | None,
    ) -> TransferJob:
        try:
            await self._notify(progress, "1/4 Проверяем NFT…")
            nfts = await self._portals.get_owned_nfts(
                target.id, owner_telegram_id=job.owner_telegram_id
            )
            selected, error = self._select_nft(nfts, nft_id)
            if error:
                return await self._fail(job, error)
            assert selected is not None
            selected_asset(selected.nft_id)
            await self._db.update_transfer_job(
                job.id,
                phase=PortalsTransferPhase.CREATING_OFFER,
                asset_id=selected.nft_id,
                display_name=selected.display_name,
            )

            await self._notify(progress, "2/4 Отправляем оффер…")
            created = await self._portals.create_offer(
                owner_telegram_id=job.owner_telegram_id,
                account_id=source.id,
                nft_id=selected.nft_id,
                amount=job.amount_text,
                dry_run=self._dry_run,
                reconciliation_account_id=target.id,
            )
            if self._dry_run:
                return await self._finish(
                    job,
                    PortalsTransferStatus.DRY_RUN,
                    PortalsTransferPhase.COMPLETED,
                    None,
                    "Тестовый запуск завершён. Оффер не отправлялся.",
                    {"create_sent": False, "accept_sent": False},
                )
            if created.offer_id is None:
                return await self._ambiguous(job, "CREATE_OFFER_UNCONFIRMED")
            await self._db.update_transfer_job(
                job.id,
                phase=PortalsTransferPhase.LOCATING_OFFER,
                external_ref=created.offer_id,
            )

            await self._notify(progress, "3/4 Проверяем оффер…")
            await self._db.update_transfer_job(
                job.id, phase=PortalsTransferPhase.ACCEPTING
            )
            await self._notify(progress, "4/4 Принимаем оффер…")
            accepted = await self._portals.accept_transfer_offer(
                owner_telegram_id=job.owner_telegram_id,
                source_account_id=source.id,
                target_account_id=target.id,
                offer_id=created.offer_id,
                nft_id=selected.nft_id,
                amount=created.amount,
                display_name=selected.display_name,
                expected_sender_id=created.sender_id,
            )
            if accepted.status is PortalsAcceptStatus.SUCCESS:
                return await self._finish(
                    job,
                    PortalsTransferStatus.SUCCESS,
                    PortalsTransferPhase.COMPLETED,
                    None,
                    "Оффер отправлен вашим аккаунтом и принят рабочим аккаунтом.",
                    {"create_sent": True, "accept_sent": True},
                )
            return await self._ambiguous(job, "ACCEPT_AMBIGUOUS")
        except PortalsCreateOfferUnconfirmedError as exc:
            phase = await self._current_phase(job)
            code = (
                "ACCEPT_AMBIGUOUS"
                if phase == PortalsTransferPhase.ACCEPTING
                else exc.reason
            )
            return await self._ambiguous(job, code)
        except PortalsMutationNetworkError:
            phase = await self._current_phase(job)
            code = (
                "ACCEPT_AMBIGUOUS"
                if phase == PortalsTransferPhase.ACCEPTING
                else "CREATE_OFFER_AMBIGUOUS"
            )
            return await self._ambiguous(job, code)
        except Exception as exc:  # noqa: BLE001 - safe boundary
            phase = await self._current_phase(job)
            if (
                isinstance(exc, PortalsApiError)
                and exc.status >= 500
                and phase == PortalsTransferPhase.ACCEPTING
            ):
                return await self._ambiguous(job, "ACCEPT_AMBIGUOUS")
            logger.warning(
                "Portals transfer failed job_id=%d phase=%s exception=%s",
                job.id,
                phase,
                type(exc).__name__,
            )
            return await self._fail(job, self._error_code(exc, phase))

    @staticmethod
    def _select_nft(
        nfts: list[PortalsNft], requested: str | int | None
    ) -> tuple[PortalsNft | None, str | None]:
        eligible = [nft for nft in nfts if nft.eligible]
        if requested is not None:
            matches = [
                nft
                for nft in eligible
                if PortalsTransferJobService._same(nft.nft_id, requested)
            ]
            if len(matches) > 1:
                return None, "AMBIGUOUS_NFT"
            return (matches[0], None) if matches else (None, "NFT_NOT_FOUND")
        if not eligible:
            return None, "EMPTY_NFTS"
        if len(eligible) != 1:
            return None, "AMBIGUOUS_NFT"
        return eligible[0], None

    @staticmethod
    def _same(left: object, right: object) -> bool:
        return type(left) is type(right) and left == right

    @staticmethod
    def _error_code(exc: Exception, phase: str) -> str:
        if isinstance(exc, PortalsOfferChangedError):
            return "OFFER_CHANGED"
        if isinstance(exc, PortalsOfferNotFoundError):
            return "OFFER_NOT_FOUND"
        if isinstance(exc, (MiniAppAccountNotFoundError,)):
            return "NO_TARGET_ACCOUNT"
        if isinstance(
            exc, (MiniAppSessionMissingError, MiniAppSessionUnauthorizedError)
        ):
            return "SESSION_REVOKED"
        if isinstance(exc, PortalsApiError):
            if exc.status == 429:
                return "RATE_LIMITED"
            if exc.code == "INSUFFICIENT_BALANCE":
                return "INSUFFICIENT_BALANCE"
            return (
                "ACCEPT_FAILED"
                if phase == PortalsTransferPhase.ACCEPTING
                else "CREATE_OFFER_FAILED"
            )
        if isinstance(exc, (PortalsNetworkError, PortalsServiceError, OSError)):
            return (
                "ACCEPT_FAILED"
                if phase == PortalsTransferPhase.ACCEPTING
                else "CREATE_OFFER_FAILED"
            )
        return (
            "ACCEPT_FAILED"
            if phase == PortalsTransferPhase.ACCEPTING
            else "CREATE_OFFER_FAILED"
        )

    async def _fail(self, job: TransferJob, code: str) -> TransferJob:
        messages = {
            "NO_OWNER_ACCOUNT": "Мой аккаунт не найден или имеет неверный тип.",
            "NO_TARGET_ACCOUNT": "Рабочий аккаунт не найден или имеет неверный тип.",
            "EMPTY_NFTS": "В рабочем аккаунте нет доступных NFT.",
            "AMBIGUOUS_NFT": "Найдено несколько NFT. Выберите одно явно.",
            "NFT_NOT_FOUND": "Выбранное NFT не найдено.",
            "OFFER_NOT_FOUND": "Созданный оффер не найден у рабочего аккаунта.",
            "OFFER_CHANGED": "Данные созданного оффера не совпали. Операция остановлена.",
            "INSUFFICIENT_BALANCE": "Недостаточно средств для отправки оффера.",
            "RATE_LIMITED": "Portals ограничил частоту запросов. Попробуйте позже.",
            "SESSION_REVOKED": "Авторизация одного из аккаунтов больше не действует.",
            "CREATE_OFFER_FAILED": "Не удалось отправить оффер.",
            "ACCEPT_FAILED": "Не удалось принять созданный оффер.",
        }
        return await self._finish(
            job,
            PortalsTransferStatus.FAILED,
            await self._current_phase(job),
            code,
            messages.get(code, "Не удалось выполнить операцию Portals."),
            {},
        )

    async def _ambiguous(self, job: TransferJob, code: str) -> TransferJob:
        messages = {
            "ACCEPT_AMBIGUOUS": (
                "Результат принятия оффера не определён. "
                "Проверьте Portals вручную."
            ),
            "CREATE_OFFER_CANCELLED": (
                "Созданный оффер был отменён до принятия. Принятие не выполнялось."
            ),
            "CREATE_OFFER_EXPIRED": (
                "Созданный оффер истёк до принятия. Принятие не выполнялось."
            ),
            "CREATE_OFFER_REJECTED": (
                "Созданный оффер был отклонён до принятия. Принятие не выполнялось."
            ),
            "CREATE_OFFER_INACTIVE": (
                "Созданный оффер стал недоступен до принятия. "
                "Принятие не выполнялось."
            ),
            "CREATE_OFFER_DISAPPEARED": (
                "Созданный оффер появился и исчез до принятия. "
                "Принятие не выполнялось."
            ),
            "CREATE_OFFER_MULTIPLE_MATCHES": (
                "Найдено несколько совпадающих офферов. "
                "Принятие не выполнялось."
            ),
        }
        message = messages.get(
            code,
            "Результат отправки оффера не определён. Принятие не выполнялось.",
        )
        return await self._finish(
            job,
            PortalsTransferStatus.AMBIGUOUS,
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
    ) -> TransferJob:
        report = await final_verification(job, self._portals, status)
        if report is not None:
            metadata = {**metadata, "live_verify": report}
            final_read = report.get("final_read")
            outcome = (
                final_read.get("outcome") if isinstance(final_read, dict) else None
            )
            mutation_counts = report.get("mutation_counts")
            mutations_observed = (
                isinstance(mutation_counts, dict)
                and mutation_counts.get("portals_create_offer") == 1
                and mutation_counts.get("portals_accept") == 1
            )
            if (
                status == PortalsTransferStatus.SUCCESS
                and mutations_observed
                and outcome != "consistent"
            ):
                status = PortalsTransferStatus.AMBIGUOUS
                code = "FINAL_STATE_UNCONFIRMED"
                message = (
                    "Принятие отправлено, но переход NFT ещё не подтверждён. "
                    "Проверьте Portals вручную."
                )
        return await self._db.finish_transfer_job(
            job.id,
            status=status,
            phase=phase,
            error_code=code,
            error_message=message,
            result_metadata=metadata,
        )

    async def _lock_for_accounts(
        self,
        owner_id: int,
        source_id: int,
        target_id: int,
        asset_id: str | int | None,
    ) -> asyncio.Lock:
        kind = type(asset_id).__name__
        key = (
            owner_id,
            min(source_id, target_id),
            max(source_id, target_id),
            kind,
            asset_id,
        )
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
