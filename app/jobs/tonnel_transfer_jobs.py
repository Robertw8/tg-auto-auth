from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal

from app.db import Account, Database, TransferJob
from app.telegram_client import (
    TonnelDuplicateMutationError,
    TonnelExistingOfferDiagnosticResult,
    TonnelGift,
    TonnelGiftUnavailableError,
    TonnelInsufficientBalanceError,
    TonnelInventoryUnavailableError,
    TonnelMarketGift,
    TonnelMutationNetworkError,
    TonnelNetworkError,
    TonnelPreflightFailure,
    TonnelService,
    TonnelServiceError,
    TonnelTransferMode,
    TonnelTransferStatus,
)

logger = logging.getLogger(__name__)
ProgressCallback = Callable[[str], Awaitable[None]]


class TonnelTransferJobService:
    """Persist one recipient-bound Tonnel transfer with no mutation retry."""

    def __init__(
        self,
        db: Database,
        tonnel: TonnelService,
        *,
        dry_run: bool = True,
        transfer_mode: str | TonnelTransferMode = TonnelTransferMode.DIRECT_RECIPIENT,
    ) -> None:
        self._db = db
        self._tonnel = tonnel
        self._dry_run = dry_run
        self._transfer_mode = TonnelTransferMode(transfer_mode)
        self._locks_guard = asyncio.Lock()
        self._asset_locks: dict[tuple[int, int, int, str], asyncio.Lock] = {}

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    @property
    def transfer_mode(self) -> TonnelTransferMode:
        return self._transfer_mode

    @property
    def offer_accept_delay_ms(self) -> int:
        """Configured readiness delay used by every BUY_OFFER child job."""
        return self._tonnel.offer_accept_delay_ms

    async def execute_tonnel_transfer(
        self,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        gift_id: str | int,
        price_ton: str = "",
        *,
        confirmation_nonce: str,
        progress: ProgressCallback | None = None,
    ) -> TransferJob:
        confirmation_key = hashlib.sha256(confirmation_nonce.encode()).hexdigest()
        amount_atomic = (
            self._tonnel.ton_to_nanotons(price_ton)
            if self._transfer_mode
            in {TonnelTransferMode.MARKET_SALE, TonnelTransferMode.BUY_OFFER}
            else None
        )
        job = await self._db.create_transfer_job(
            owner_telegram_id=owner_telegram_id,
            market="tonnel",
            owner_account_id=owner_account_id,
            target_account_id=target_account_id,
            asset_id=gift_id,
            amount_text=price_ton,
            amount_atomic=amount_atomic,
            confirmation_key=confirmation_key,
        )
        return await self.execute_reserved_tonnel_transfer(job.id, progress=progress)

    async def execute_reserved_tonnel_transfer(
        self,
        job_id: int,
        *,
        progress: ProgressCallback | None = None,
        batch_auth_metadata: Mapping[str, object] | None = None,
    ) -> TransferJob:
        job = await self._db.get_transfer_job(job_id)
        if job is None or job.market != "tonnel":
            raise ValueError("Tonnel transfer job not found")
        claimed = await self._db.claim_transfer_job(job.id)
        if claimed is None:
            return job
        lock = await self._lock_for(claimed)
        async with lock:
            owner, target = await asyncio.gather(
                self._db.get_account(
                    claimed.owner_account_id, claimed.owner_telegram_id
                ),
                self._db.get_account(
                    claimed.target_account_id, claimed.owner_telegram_id
                ),
            )
            if owner is None or owner.role != "OWNER":
                return await self._fail(claimed, "NO_OWNER_ACCOUNT")
            if target is None or target.role != "TARGET":
                return await self._fail(claimed, "NO_TARGET_ACCOUNT")
            return await self._run(
                claimed,
                owner=owner,
                target=target,
                progress=progress,
                batch_auth_metadata=batch_auth_metadata,
            )

    async def execute_existing_offer_diagnostic(
        self,
        *,
        owner_telegram_id: int,
        owner_account_id: int,
        target_account_id: int,
        gift_id: str | int,
        offer_amount: str,
        expected_offer_fingerprint: str,
        confirm: bool,
    ) -> TransferJob:
        random_material = secrets.token_urlsafe(32)
        confirmation_key = hashlib.sha256(random_material.encode()).hexdigest()
        del random_material
        job = await self._db.create_transfer_job(
            owner_telegram_id=owner_telegram_id,
            market="tonnel",
            owner_account_id=owner_account_id,
            target_account_id=target_account_id,
            asset_id=gift_id,
            amount_text=offer_amount,
            amount_atomic=self._tonnel.ton_to_nanotons(offer_amount),
            confirmation_key=confirmation_key,
        )
        claimed = await self._db.claim_transfer_job(job.id)
        if claimed is None:
            return job
        owner, target = await asyncio.gather(
            self._db.get_account(owner_account_id, owner_telegram_id),
            self._db.get_account(target_account_id, owner_telegram_id),
        )
        if owner is None or owner.role != "OWNER":
            return await self._finish_existing_diagnostic_failure(
                claimed, "NO_OWNER_ACCOUNT", "Аккаунт N1 не найден."
            )
        if target is None or target.role != "TARGET":
            return await self._finish_existing_diagnostic_failure(
                claimed, "NO_TARGET_ACCOUNT", "Аккаунт N2 не найден."
            )
        return await self._run_existing_offer_diagnostic(
            claimed,
            owner=owner,
            target=target,
            expected_offer_fingerprint=expected_offer_fingerprint,
            confirm=confirm,
        )

    async def _run_existing_offer_diagnostic(
        self,
        job: TransferJob,
        *,
        owner: Account,
        target: Account,
        expected_offer_fingerprint: str,
        confirm: bool,
    ) -> TransferJob:
        await self._db.update_transfer_job(
            job.id,
            phase="EXISTING_OFFER_DIAGNOSTIC",
            asset_id=job.asset_id,
        )
        if job.asset_id is None:
            return await self._finish_existing_diagnostic_failure(
                job, "GIFT_UNAVAILABLE", "Идентификатор подарка отсутствует."
            )
        try:
            result = await self._tonnel.accept_existing_buy_offer_diagnostic(
                owner_telegram_id=job.owner_telegram_id,
                source_account_id=target.id,
                destination_account_id=owner.id,
                gift_id=job.asset_id,
                offer_amount=job.amount_text,
                expected_offer_fingerprint=expected_offer_fingerprint,
                confirm=confirm,
            )
        except Exception as exc:  # noqa: BLE001 - sanitized diagnostic boundary
            logger.warning(
                "Tonnel existing-offer diagnostic failed job_id=%d exception=%s",
                job.id,
                type(exc).__name__,
            )
            metadata: dict[str, object] = {
                "strategy": "ACCEPT_EXISTING_OFFER_DIAGNOSTIC",
                "mutation_count": 0,
                "mutation_counts": {
                    "tonnel_offer_create": 0,
                    "tonnel_offer_accept": 0,
                },
            }
            preflight = self._preflight_metadata(exc)
            if preflight is not None:
                metadata["tonnel_preflight_error"] = preflight
            return await self._finish(
                job,
                "FAILED",
                self._error_code(exc),
                "Диагностика существующего оффера не выполнена.",
                metadata,
            )
        return await self._persist_existing_offer_diagnostic(job, result)

    async def _persist_existing_offer_diagnostic(
        self,
        job: TransferJob,
        result: TonnelExistingOfferDiagnosticResult,
    ) -> TransferJob:
        accept = result.accept_result
        metadata: dict[str, object] = {
            "strategy": "ACCEPT_EXISTING_OFFER_DIAGNOSTIC",
            "offer_age_ms": result.offer_age_ms,
            "candidate_count": result.candidate_count,
            "offer_id_fingerprint": (
                TonnelService.fingerprint(
                    f"{type(result.offer_id).__name__}:{result.offer_id}"
                )
                if result.offer_id is not None
                else None
            ),
            "offer_id_type": (
                type(result.offer_id).__name__
                if result.offer_id is not None
                else None
            ),
            **dict(result.validation),
            "tonnel_offer_accept_request": dict(result.accept_request_metadata),
            "tonnel_offer_accept_result": {
                "request_sent": accept.request_sent,
                "http_status": accept.http_status,
                "body_type": accept.body_type,
                "status": accept.status,
                "message": accept.message,
                "safe_field_names": list(accept.safe_field_names),
                "duration_ms": accept.duration_ms,
            },
            "mutation_count": 1 if accept.request_sent else 0,
            "mutation_counts": {
                "tonnel_offer_create": 0,
                "tonnel_offer_accept": 1 if accept.request_sent else 0,
            },
            "source_owns_after": result.source_owns_after,
            "destination_owns_after": result.destination_owns_after,
            "offer_active_after": result.offer_active_after,
        }
        status = result.status.value
        return await self._finish(
            job,
            status,
            result.error_code,
            result.message,
            metadata,
        )

    async def _finish_existing_diagnostic_failure(
        self, job: TransferJob, error_code: str, message: str
    ) -> TransferJob:
        return await self._finish(
            job,
            "FAILED",
            error_code,
            message,
            {
                "strategy": "ACCEPT_EXISTING_OFFER_DIAGNOSTIC",
                "mutation_count": 0,
                "mutation_counts": {
                    "tonnel_offer_create": 0,
                    "tonnel_offer_accept": 0,
                },
            },
        )

    async def _run(
        self,
        job: TransferJob,
        *,
        owner: Account,
        target: Account,
        progress: ProgressCallback | None,
        batch_auth_metadata: Mapping[str, object] | None = None,
    ) -> TransferJob:
        if self._transfer_mode is TonnelTransferMode.BUY_OFFER:
            return await self._run_buy_offer(
                job,
                owner=owner,
                target=target,
                progress=progress,
                batch_auth_metadata=batch_auth_metadata,
            )
        if self._transfer_mode is TonnelTransferMode.MARKET_SALE:
            return await self._run_market(
                job, owner=owner, target=target, progress=progress
            )
        try:
            await self._notify(progress, "1/3 Проверяем точный подарок…")
            gifts = await self._tonnel.get_inventory(
                target.id, owner_telegram_id=job.owner_telegram_id
            )
            gift = self._select_exact(gifts, job.asset_id)
            await self._db.update_transfer_job(
                job.id,
                phase="TRANSFERRING",
                asset_id=gift.gift_id,
                display_name=gift.display_name,
            )
            await self._notify(progress, "2/3 Проверяем получателя…")
            await self._notify(
                progress,
                "3/3 Выполняем тестовую проверку…"
                if self._dry_run
                else "3/3 Передаём подарок…",
            )
            result = await self._tonnel.transfer_exact_gift(
                owner_telegram_id=job.owner_telegram_id,
                source_account_id=target.id,
                destination_account_id=owner.id,
                identity=gift.identity,
                dry_run=self._dry_run,
            )
            metadata: dict[str, object] = {
                "strategy": result.strategy.value,
                "mutation_count": 1 if result.mutation_sent else 0,
                "response_status": result.response_status,
                "response_fields": list(result.response_fields),
                "source_owns_after": result.source_owns_after,
                "destination_owns_after": result.destination_owns_after,
                "gift_ref_fingerprint": TonnelService.fingerprint(
                    f"{type(gift.gift_id).__name__}:{gift.gift_id}"
                ),
            }
            if result.status is TonnelTransferStatus.SUCCESS:
                return await self._finish(
                    job, "SUCCESS", None, result.message, metadata
                )
            if result.status is TonnelTransferStatus.DRY_RUN:
                return await self._finish(
                    job, "DRY_RUN", None, result.message, metadata
                )
            if result.status is TonnelTransferStatus.AMBIGUOUS:
                return await self._finish(
                    job,
                    "AMBIGUOUS",
                    "TRANSFER_AMBIGUOUS",
                    result.message,
                    metadata,
                )
            return await self._finish(
                job,
                "FAILED",
                "NOT_TRANSFERRED",
                result.message,
                metadata,
            )
        except Exception as exc:  # noqa: BLE001 - sanitized job boundary
            logger.warning(
                "Tonnel transfer failed job_id=%d exception=%s",
                job.id,
                type(exc).__name__,
            )
            return await self._fail(
                job,
                self._error_code(exc),
                preflight_error=self._preflight_metadata(exc),
            )

    async def _run_buy_offer(
        self,
        job: TransferJob,
        *,
        owner: Account,
        target: Account,
        progress: ProgressCallback | None,
        batch_auth_metadata: Mapping[str, object] | None = None,
    ) -> TransferJob:
        try:
            await self._notify(progress, "Проверяем точный подарок и баланс…")
            gifts = await self._tonnel.get_market_inventory(
                target.id, owner_telegram_id=job.owner_telegram_id
            )
            gift = self._select_exact_market(gifts, job.asset_id)
            amount = self._tonnel.format_market_price(job.amount_text)
            await self._db.update_transfer_job(
                job.id,
                phase="PREPARE",
                asset_id=gift.gift_id,
                display_name=gift.display_name,
            )
            await self._notify(
                progress,
                (
                    "Проверяем создание оффера…\n"
                    "Проверяем точный оффер…\n"
                    "Проверяем принятие оффера…\n"
                    "Проверяем владение…"
                )
                if self._dry_run
                else (
                    "Создание оффера…\n"
                    "Проверка оффера…\n"
                    "Принятие оффера…\n"
                    "Проверка владения…"
                ),
            )
            result = await self._tonnel.transfer_buy_offer(
                owner_telegram_id=job.owner_telegram_id,
                source_account_id=target.id,
                destination_account_id=owner.id,
                identity=gift.identity,
                offer_amount=amount,
                dry_run=self._dry_run,
                mutation_guard_key=f"transfer-job:{job.id}",
                child_job_id=job.id,
            )
            quote = result.quote
            create = result.create_result
            accept = result.accept_result
            metadata: dict[str, object] = {
                "strategy": TonnelTransferMode.BUY_OFFER.value,
                "source_gift_id_type": type(result.gift_id).__name__,
                "source_gift_ref_fingerprint": TonnelService.fingerprint(
                    f"{type(result.gift_id).__name__}:{result.gift_id}"
                ),
                "offer_id_type": (
                    type(result.offer_id).__name__
                    if result.offer_id is not None
                    else None
                ),
                "offer_id_fingerprint": (
                    TonnelService.fingerprint(
                        f"{type(result.offer_id).__name__}:{result.offer_id}"
                    )
                    if result.offer_id is not None
                    else None
                ),
                "offer_amount": self._decimal_text(quote.offer_amount),
                "offer_amount_nanotons": quote.offer_amount_nanotons,
                "seller_proceeds": self._decimal_text(quote.seller_proceeds),
                "seller_proceeds_nanotons": quote.seller_proceeds_nanotons,
                "seller_fee": self._decimal_text(quote.seller_fee),
                "seller_fee_nanotons": quote.seller_fee_nanotons,
                "create_fee": self._decimal_text(quote.create_fee),
                "create_fee_nanotons": quote.create_fee_nanotons,
                "buyer_balance_required": self._decimal_text(
                    quote.required_buyer_balance
                ),
                "buyer_balance": (
                    self._decimal_text(quote.buyer_balance)
                    if quote.buyer_balance is not None
                    else None
                ),
                "balance_sufficient": (
                    quote.buyer_balance >= quote.required_buyer_balance
                    if quote.buyer_balance is not None
                    else None
                ),
                "tonnel_offer_create_result": {
                    "request_sent": create.request_sent,
                    "http_status": create.http_status,
                    "body_type": create.body_type,
                    "status": create.status,
                    "message": create.message,
                    "safe_field_names": list(create.safe_field_names),
                    "duration_ms": create.duration_ms,
                },
                "tonnel_offer_create_request": dict(result.create_request_metadata),
                "tonnel_offer_accept_result": {
                    "request_sent": accept.request_sent,
                    "http_status": accept.http_status,
                    "body_type": accept.body_type,
                    "status": accept.status,
                    "message": accept.message,
                    "safe_field_names": list(accept.safe_field_names),
                    "duration_ms": accept.duration_ms,
                },
                "tonnel_offer_accept_request": dict(result.accept_request_metadata),
                "offer_correlation": dict(result.offer_correlation),
                "ownership_verification": dict(result.ownership_verification),
                "timings": dict(result.timings_ms),
                "mutation_counts": {
                    "tonnel_offer_create": 1 if create.request_sent else 0,
                    "tonnel_offer_accept": 1 if accept.request_sent else 0,
                },
                "source_owns_after": result.source_owns_after,
                "destination_owns_after": result.destination_owns_after,
                "offer_active_after": result.offer_active_after,
            }
            if batch_auth_metadata is not None:
                metadata["batch_auth"] = dict(batch_auth_metadata)
            if result.status is TonnelTransferStatus.SUCCESS:
                return await self._finish(
                    job, "SUCCESS", None, result.message, metadata
                )
            if result.status is TonnelTransferStatus.DRY_RUN:
                return await self._finish(
                    job, "DRY_RUN", None, result.message, metadata
                )
            if result.status is TonnelTransferStatus.AMBIGUOUS:
                return await self._finish(
                    job,
                    "AMBIGUOUS",
                    result.error_code or "TRANSFER_AMBIGUOUS",
                    result.message,
                    metadata,
                )
            return await self._finish(
                job,
                "FAILED",
                result.error_code or "NOT_TRANSFERRED",
                result.message,
                metadata,
            )
        except Exception as exc:  # noqa: BLE001 - sanitized job boundary
            logger.warning(
                "Tonnel buy-offer transfer failed job_id=%d exception=%s",
                job.id,
                type(exc).__name__,
            )
            return await self._fail(
                job,
                self._error_code(exc),
                preflight_error=self._preflight_metadata(exc),
                extra_metadata=(
                    {"batch_auth": dict(batch_auth_metadata)}
                    if batch_auth_metadata is not None
                    else None
                ),
            )

    async def _run_market(
        self,
        job: TransferJob,
        *,
        owner: Account,
        target: Account,
        progress: ProgressCallback | None,
    ) -> TransferJob:
        try:
            await self._notify(progress, "1/3 Проверяем внутренний инвентарь…")
            gifts = await self._tonnel.get_market_inventory(
                target.id, owner_telegram_id=job.owner_telegram_id
            )
            gift = self._select_exact_market(gifts, job.asset_id)
            price = self._tonnel.format_market_price(job.amount_text)
            await self._db.update_transfer_job(
                job.id,
                phase="PREPARE",
                asset_id=gift.gift_id,
                display_name=gift.display_name,
                external_ref=gift.sale_id,
            )
            await self._notify(progress, "2/3 Проверяем баланс и точные данные…")
            await self._notify(
                progress,
                "3/3 Выполняем тестовую проверку…"
                if self._dry_run
                else "3/3 Размещаем и покупаем подарок…",
            )
            result = await self._tonnel.transfer_market_sale(
                owner_telegram_id=job.owner_telegram_id,
                source_account_id=target.id,
                destination_account_id=owner.id,
                identity=gift.identity,
                seller_price=price,
                dry_run=self._dry_run,
            )
            metadata: dict[str, object] = {
                "strategy": TonnelTransferMode.MARKET_SALE.value,
                "source_gift_id_type": type(gift.gift_id).__name__,
                "source_gift_ref_fingerprint": TonnelService.fingerprint(
                    f"{type(gift.gift_id).__name__}:{gift.gift_id}"
                ),
                "sale_id_type": type(result.sale_id).__name__,
                "sale_id_fingerprint": TonnelService.fingerprint(result.sale_id),
                "seller_price": self._decimal_text(result.seller_price),
                "buyer_price": self._decimal_text(result.buyer_price),
                "seller_price_nanotons": result.seller_price_nanotons,
                "buyer_price_nanotons": result.buyer_price_nanotons,
                "commission_percent": self._decimal_text(result.commission_percent),
                "buyer_balance_required": self._decimal_text(result.buyer_price),
                "buyer_balance": (
                    self._decimal_text(result.buyer_balance)
                    if result.buyer_balance is not None
                    else None
                ),
                "seller_balance": (
                    self._decimal_text(result.seller_balance)
                    if result.seller_balance is not None
                    else None
                ),
                "required_seller_fee": "0",
                "balance_sufficient": result.balance_sufficient,
                "tonnel_market_listing_result": {
                    "request_sent": result.listing_sent,
                    "http_status": result.listing_status,
                    "safe_field_names": list(result.listing_response_fields),
                    "sale_id_type": type(result.sale_id).__name__,
                    "sale_id_fingerprint": TonnelService.fingerprint(result.sale_id),
                    "seller_price": self._decimal_text(result.seller_price),
                    "public_buyer_price": self._decimal_text(result.buyer_price),
                    "duration_ms": result.timings_ms.get("listing_request_ms"),
                },
                "tonnel_market_buy_result": {
                    "request_sent": result.buy_sent,
                    "http_status": result.buy_status,
                    "body_type": "mapping" if result.buy_response_fields else "none",
                    "safe_field_names": list(result.buy_response_fields),
                    "duration_ms": result.timings_ms.get("buy_request_ms"),
                },
                "timings": dict(result.timings_ms),
                "mutation_counts": {
                    "tonnel_listing": 1 if result.listing_sent else 0,
                    "tonnel_buy": 1 if result.buy_sent else 0,
                },
                "source_owns_after": result.source_owns_after,
                "destination_owns_after": result.destination_owns_after,
                "listing_still_active": result.listing_still_active,
            }
            if result.status is TonnelTransferStatus.SUCCESS:
                return await self._finish(
                    job, "SUCCESS", None, result.message, metadata
                )
            if result.status is TonnelTransferStatus.DRY_RUN:
                return await self._finish(
                    job, "DRY_RUN", None, result.message, metadata
                )
            if result.status is TonnelTransferStatus.AMBIGUOUS:
                return await self._finish(
                    job,
                    "AMBIGUOUS",
                    result.error_code or "TRANSFER_AMBIGUOUS",
                    result.message,
                    metadata,
                )
            return await self._finish(
                job,
                "FAILED",
                result.error_code or "NOT_TRANSFERRED",
                result.message,
                metadata,
            )
        except Exception as exc:  # noqa: BLE001 - sanitized job boundary
            logger.warning(
                "Tonnel market transfer failed job_id=%d exception=%s",
                job.id,
                type(exc).__name__,
            )
            return await self._fail(job, self._error_code(exc))

    @staticmethod
    def _select_exact(
        gifts: Sequence[TonnelGift], gift_id: str | int | None
    ) -> TonnelGift:
        matches = [
            gift
            for gift in gifts
            if type(gift.gift_id) is type(gift_id) and gift.gift_id == gift_id
        ]
        if len(matches) != 1:
            raise TonnelGiftUnavailableError("Выбранный подарок больше не доступен.")
        return matches[0]

    @staticmethod
    def _select_exact_market(
        gifts: Sequence[TonnelMarketGift], gift_id: str | int | None
    ) -> TonnelMarketGift:
        matches = [
            gift
            for gift in gifts
            if type(gift.gift_id) is type(gift_id) and gift.gift_id == gift_id
        ]
        if len(matches) != 1:
            raise TonnelGiftUnavailableError("Выбранный подарок больше не доступен.")
        return matches[0]

    @staticmethod
    def _decimal_text(value: Decimal) -> str:
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text

    async def _lock_for(self, job: TransferJob) -> asyncio.Lock:
        key = (
            job.owner_telegram_id,
            job.owner_account_id,
            job.target_account_id,
            f"{type(job.asset_id).__name__}:{job.asset_id}",
        )
        async with self._locks_guard:
            return self._asset_locks.setdefault(key, asyncio.Lock())

    async def _fail(
        self,
        job: TransferJob,
        code: str,
        *,
        preflight_error: dict[str, object] | None = None,
        extra_metadata: Mapping[str, object] | None = None,
    ) -> TransferJob:
        messages = {
            "NO_OWNER_ACCOUNT": "Аккаунт получателя больше не доступен.",
            "NO_TARGET_ACCOUNT": "Рабочий аккаунт больше не доступен.",
            "INVENTORY_UNAVAILABLE": (
                "Не удалось загрузить подарки Tonnel. Проверьте подключение "
                "@Tonnel_Network_bot к рабочему аккаунту."
            ),
            "GIFT_UNAVAILABLE": "Выбранный подарок больше не доступен.",
            "INSUFFICIENT_BALANCE": (
                "На балансе аккаунта-покупателя в Tonnel недостаточно TON."
            ),
            "NETWORK_ERROR": "Tonnel временно недоступен.",
            "TRANSFER_AMBIGUOUS": (
                "Результат передачи не определён. Повторный запрос не отправлялся."
            ),
            "DUPLICATE_CREATE_BLOCKED": (
                "Повторная отправка оффера для этого задания заблокирована."
            ),
        }
        metadata: dict[str, object] = {"mutation_count": 0}
        if preflight_error is not None:
            metadata["tonnel_preflight_error"] = preflight_error
        if extra_metadata is not None:
            metadata.update(extra_metadata)
        return await self._finish(
            job,
            "AMBIGUOUS" if code == "TRANSFER_AMBIGUOUS" else "FAILED",
            code,
            messages.get(code, "Операция Tonnel не выполнена."),
            metadata,
        )

    async def _finish(
        self,
        job: TransferJob,
        status: str,
        error_code: str | None,
        message: str,
        metadata: dict[str, object],
    ) -> TransferJob:
        return await self._db.finish_transfer_job(
            job.id,
            status=status,
            phase="COMPLETED",
            error_code=error_code,
            error_message=message,
            result_metadata=metadata,
        )

    @staticmethod
    async def _notify(progress: ProgressCallback | None, text: str) -> None:
        if progress is not None:
            await progress(text)

    @staticmethod
    def _error_code(exc: Exception) -> str:
        if isinstance(exc, TonnelPreflightFailure):
            return exc.error_code
        if isinstance(exc, TonnelInventoryUnavailableError):
            return "INVENTORY_UNAVAILABLE"
        if isinstance(exc, TonnelGiftUnavailableError):
            return "GIFT_UNAVAILABLE"
        if isinstance(exc, TonnelInsufficientBalanceError):
            return "INSUFFICIENT_BALANCE"
        if isinstance(exc, TonnelMutationNetworkError):
            return "TRANSFER_AMBIGUOUS"
        if isinstance(exc, TonnelDuplicateMutationError):
            return "DUPLICATE_CREATE_BLOCKED"
        if isinstance(exc, (TonnelNetworkError, TonnelServiceError)):
            return "NETWORK_ERROR"
        return "UNEXPECTED_ERROR"

    @staticmethod
    def _preflight_metadata(exc: Exception) -> dict[str, object] | None:
        diagnostic = getattr(exc, "diagnostic", None)
        return dict(diagnostic) if isinstance(diagnostic, Mapping) else None


__all__ = ["TonnelTransferJobService"]
