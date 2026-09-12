"""Safe verification metadata; never performs a mutation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from app.db import TransferJob
    from app.telegram_client import MrktService, PortalsService

logger = logging.getLogger(__name__)
_SAFE_STATUS_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
_active: ContextVar[LiveVerification | None] = ContextVar(
    "live_verification", default=None
)
# A response can put secrets in keys too. Unknown keys are counted, never emitted.
_FIELDS = frozenset(
    [
        "id",
        "ids",
        "gift_id",
        "nft_id",
        "offer_id",
        "offer_price",
        "amount",
        "price",
        "salePrice",
        "prices",
        "offer",
        "offers",
        "nft",
        "nfts",
        "gifts",
        "items",
        "data",
        "result",
        "results",
        "success",
        "status",
        "code",
        "error",
        "message",
        "detail",
        "details",
        "total",
        "total_count",
        "total_amount",
        "cursor",
        "count",
        "isListed",
        "isMine",
        "isLockedForSale",
        "canBeListed",
        "isListingAvailable",
        "canSell",
        "canAccept",
        "isAcceptable",
        "accepted",
        "cancelled",
        "expired",
        "sender",
        "sender_id",
        "buyer",
        "buyer_id",
        "user",
        "user_id",
        "owner",
        "owner_id",
        "ownerId",
        "seller",
        "seller_id",
        "sellerId",
        "userId",
        "token",
        "purchasedGifts",
        "name",
        "title",
        "number",
        "collection_name",
        "collectionName",
        "created_at",
        "updated_at",
        "expires_at",
    ]
)


def identity(value: object) -> dict[str, object]:
    """Opaque wire type + SHA256, never a raw identifier or its repr."""
    kind = (
        "str"
        if type(value) is str
        else "int"
        if type(value) is int
        else "missing"
        if value is None
        else "invalid"
    )
    if kind in {"missing", "invalid"}:
        return {"type": kind, "fingerprint": None}
    encoded = json.dumps([kind, value], ensure_ascii=True, separators=(",", ":"))
    return {
        "type": kind,
        "fingerprint": hashlib.sha256(encoded.encode()).hexdigest()[:12],
    }


def same(left: object, right: object) -> bool:
    return type(left) is type(right) and left == right


def amount_matches(left: object, right: object) -> bool:
    try:
        return Decimal(str(left)) == Decimal(str(right))
    except (InvalidOperation, ValueError):
        return False


class LiveVerification:
    def __init__(self, job: TransferJob) -> None:
        self.started_at = time.monotonic()
        self.job_id = job.id
        self.market = job.market
        self.owner_account_id = job.owner_account_id
        self.target_account_id = job.target_account_id
        self.asset_id: object = job.asset_id
        self.offer_id: object = job.external_ref
        self.events: list[dict[str, Any]] = []
        self.mutations: dict[str, int] = {}

    def emit(self, event: str, **safe_metadata: object) -> None:
        record = {
            "event": event,
            "job_id": self.job_id,
            "market": self.market,
            "n1_account_db_id": self.owner_account_id,
            "n2_account_db_id": self.target_account_id,
            **safe_metadata,
        }
        if len(self.events) < 512:
            self.events.append(record)
        try:
            logger.info(
                "LIVE_VERIFY %s", json.dumps(record, ensure_ascii=True, sort_keys=True)
            )
        except Exception:  # noqa: BLE001, S110 - the logger itself failed; never interrupt a mutation
            pass


@contextmanager
def verification_scope(job: TransferJob, enabled: bool) -> Iterator[None]:
    trace = LiveVerification(job) if enabled else None
    token = _active.set(trace)
    try:
        if trace is not None:
            trace.emit("started", asset=identity(job.asset_id))
        yield
    finally:
        _active.reset(token)


def selected_asset(value: object) -> None:
    if (trace := _active.get()) is not None:
        trace.asset_id = value
        trace.emit("selected_asset", asset=identity(value))


def mutation_started(stage: str, *, emit_event: bool = True) -> None:
    if (trace := _active.get()) is not None:
        trace.mutations[stage] = trace.mutations.get(stage, 0) + 1
        if emit_event:
            trace.emit("mutation_started", stage=stage, count=trace.mutations[stage])


def stage_timing(stage: str, started_at: float) -> None:
    if (trace := _active.get()) is not None:
        trace.emit(
            "stage_timing",
            stage=stage,
            duration_ms=max(0, round((time.monotonic() - started_at) * 1000)),
        )


def response_metadata(stage: str, status: int, body: object) -> None:
    if (trace := _active.get()) is None:
        return
    keys = set(body) if isinstance(body, dict) else set()
    trace.emit(
        "response",
        stage=stage,
        http_status=status,
        fields=sorted(key for key in keys if key in _FIELDS),
        omitted_field_count=len(keys - _FIELDS),
        body_type="object"
        if isinstance(body, dict)
        else "array"
        if isinstance(body, list)
        else "other",
    )


def created_offer(value: object) -> None:
    if (trace := _active.get()) is not None:
        trace.offer_id = value
        trace.emit(
            "created_offer", offer_id_present=value is not None, offer=identity(value)
        )


def offer_reconciliation(
    *,
    attempt: int,
    placed_result_count: int | None,
    nft_result_count: int | None,
    exact_match_count: int,
    resolved_offer_id: object,
    elapsed_ms: int,
    received_result_count: int | None = None,
    active_match_count: int = 0,
    state_uncertain: bool = False,
) -> None:
    if (trace := _active.get()) is not None:
        trace.emit(
            "offer_reconciliation",
            attempt=attempt,
            placed_result_count=placed_result_count,
            nft_result_count=nft_result_count,
            received_result_count=received_result_count,
            exact_match_count=exact_match_count,
            active_match_count=active_match_count,
            offer_id_resolved=resolved_offer_id is not None,
            offer=identity(resolved_offer_id),
            elapsed_ms=max(0, elapsed_ms),
            state_uncertain=state_uncertain,
        )


def offer_observation(
    *,
    attempt: int,
    endpoint: str,
    offer_id: object,
    status: object,
    previous_status: object,
    created_at: object,
    updated_at: object,
    expires_at: object,
    appeared: bool,
    disappeared: bool,
    created_in_window: bool,
) -> None:
    if (trace := _active.get()) is None:
        return
    safe_endpoint = (
        endpoint
        if endpoint in {"n1_placed", "nft_offers", "n2_received"}
        else "unknown"
    )
    safe_status = (
        status
        if isinstance(status, str) and _SAFE_STATUS_RE.fullmatch(status)
        else None
    )
    safe_previous = (
        previous_status
        if isinstance(previous_status, str)
        and _SAFE_STATUS_RE.fullmatch(previous_status)
        else None
    )
    trace.emit(
        "offer_observation",
        attempt=attempt,
        endpoint=safe_endpoint,
        offer=identity(offer_id),
        status=safe_status,
        previous_status=safe_previous,
        status_changed=(
            safe_previous is not None
            and safe_status is not None
            and safe_previous != safe_status
        ),
        created_at=_safe_timestamp(created_at),
        updated_at=_safe_timestamp(updated_at),
        expires_at=_safe_timestamp(expires_at),
        appeared=appeared,
        disappeared=disappeared,
        created_in_window=created_in_window,
    )


def _safe_timestamp(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    candidate = value.strip()
    if not candidate or any(
        character not in "0123456789-:TZ+." for character in candidate
    ):
        return None
    return candidate


def offer_checks(
    stage: str,
    *,
    actual_id: object,
    expected_id: object,
    actual_nft: object,
    expected_nft: object,
    actual_amount: object,
    expected_amount: object,
    actual_sender: object,
    expected_sender: object,
    match_count: int = 1,
    placed_match: bool | None = None,
    nft_offer_match: bool | None = None,
    received_match: bool | None = None,
    n2_owns_nft: bool | None = None,
    sender_match: bool | None = None,
) -> None:
    if (trace := _active.get()) is not None:
        observed_sender_match = (
            same(actual_sender, expected_sender)
            if actual_sender is not None and expected_sender is not None
            else None
        )
        trace.emit(
            "correlation",
            stage=stage,
            offer_id_match=same(actual_id, expected_id),
            nft_id_match=same(actual_nft, expected_nft),
            amount_match=amount_matches(actual_amount, expected_amount),
            sender_match=(
                observed_sender_match if sender_match is None else sender_match
            ),
            sender_available=actual_sender is not None,
            expected_sender_available=expected_sender is not None,
            match_count=match_count,
            placed_match=placed_match,
            nft_offer_match=nft_offer_match,
            received_match=received_match,
            n2_owns_nft=n2_owns_nft,
        )


def gift_snapshot(
    stage: str,
    raw: Mapping[str, Any],
    seller_id: object,
    *,
    account_id: int | None = None,
) -> None:
    trace = _active.get()
    if trace is None or not same(raw.get("id"), trace.asset_id):
        return
    price = raw.get("salePrice")
    flags = {
        key: raw[key]
        for key in (
            "isListed",
            "isMine",
            "isLockedForSale",
            "canBeListed",
            "isListingAvailable",
            "canSell",
        )
        if type(raw.get(key)) is bool
    }
    trace.emit(
        "gift_snapshot",
        stage=stage,
        asset=identity(raw.get("id")),
        account_db_id=account_id,
        seller=identity(seller_id),
        seller_available=seller_id is not None,
        price_nanotons=price if type(price) is int else None,
        flags=flags,
        fields=sorted(key for key in raw if key in _FIELDS),
    )


async def final_verification(
    job: TransferJob,
    service: MrktService | PortalsService,
    status: str,
    *,
    known_result: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    trace = _active.get()
    if trace is None:
        return None
    verification_started_at = time.monotonic()
    result: dict[str, Any] = (
        dict(known_result) if known_result is not None else {"outcome": "not_run"}
    )
    if known_result is None and trace.mutations and status != "DRY_RUN":
        try:
            async with asyncio.timeout(60):
                if job.market == "mrkt":
                    mrkt = cast("MrktService", service)
                    buyer_owned = False
                    seller_present = False
                    for account_id, label in (
                        (job.owner_account_id, "buyer"),
                        (job.target_account_id, "seller"),
                    ):
                        found = False
                        for listed in (False, True):
                            gifts = await mrkt.get_inventory(
                                account_id,
                                owner_telegram_id=job.owner_telegram_id,
                                is_listed=listed,
                            )
                            present = any(
                                same(g.gift_id, trace.asset_id) for g in gifts
                            )
                            result[
                                f"{label}_{'listed' if listed else 'unlisted'}_present"
                            ] = present
                            found |= present
                        if label == "buyer":
                            buyer_owned = found
                        else:
                            seller_present = found
                    result["outcome"] = (
                        "consistent"
                        if buyer_owned and not seller_present
                        else "inconclusive"
                    )
                else:
                    portals = cast("PortalsService", service)
                    source_nfts, target_nfts = await asyncio.gather(
                        portals.get_owned_nfts(
                            job.owner_account_id,
                            owner_telegram_id=job.owner_telegram_id,
                        ),
                        portals.get_owned_nfts(
                            job.target_account_id,
                            owner_telegram_id=job.owner_telegram_id,
                        ),
                    )
                    source_owns = any(
                        same(n.nft_id, trace.asset_id) for n in source_nfts
                    )
                    target_owns = any(
                        same(n.nft_id, trace.asset_id) for n in target_nfts
                    )
                    result.update(
                        source_owns_nft=source_owns,
                        target_owns_nft=target_owns,
                    )
                    result["outcome"] = (
                        "consistent"
                        if source_owns and not target_owns
                        else "inconclusive"
                    )
        except Exception as exc:  # noqa: BLE001 - reads cannot change the transaction result
            result.update(outcome="unavailable", exception_class=type(exc).__name__)
    stage_timing(
        "portals_final_ownership"
        if job.market == "portals"
        else "mrkt_final_ownership",
        verification_started_at,
    )
    stage_timing(
        "portals_total_job" if job.market == "portals" else "mrkt_total_job",
        trace.started_at,
    )
    trace.emit("final_read_verification", job_status=status, **result)
    return {
        "job_id": job.id,
        "market": job.market,
        "events": trace.events,
        "mutation_counts": dict(trace.mutations),
        "final_read": result,
    }


def russian_summary(metadata: Mapping[str, Any]) -> str:
    report = metadata.get("live_verify")
    if not isinstance(report, dict):
        return ""
    result = report.get("final_read", {})
    outcome = result.get("outcome") if isinstance(result, dict) else None
    text = {
        "consistent": "После операции объект найден у получателя. Проверенные данные согласуются с передачей.",
        "listing_still_active": "Подарок остался у рабочего аккаунта. Покупка не подтверждена.",
        "listing_not_active": "Подарок остался у рабочего аккаунта без активного размещения. Покупка не подтверждена.",
        "external_sale": "Подарок не найден ни у покупателя, ни у рабочего аккаунта. Он мог уйти другому покупателю.",
        "inconclusive": "Переход объекта пока не подтверждён. Проверьте оба аккаунта на площадке вручную.",
        "unavailable": "Не удалось проверить состояние после операции. Нужна ручная проверка площадки.",
        "not_run": "Проверка перехода объекта не выполнялась: запросы изменения не отправлялись.",
    }.get(str(outcome), "Результат проверки недоступен.")
    return (
        "\n\n<b>🔎 Контрольная проверка</b>\n"
        + text
        + "\nАвтоматических повторов нет."
    )
