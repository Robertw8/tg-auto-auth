from __future__ import annotations

import asyncio
import http.cookiejar
import json
import logging
import math
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, Request, build_opener

from app.live_verify import (
    created_offer,
    mutation_started,
    offer_checks,
    offer_observation,
    offer_reconciliation,
    response_metadata,
    stage_timing,
)

from .miniapp_service import MiniAppLaunchResult, MiniAppService

logger = logging.getLogger(__name__)

PORTALS_FRONTEND_ORIGIN = "https://portal-market.com"
PORTALS_API_ORIGIN = "https://portal-market.com/api"
PORTALS_AUTH_PATH = "/users/auth"
PORTALS_OWNED_NFTS_PATH = "/nfts/owned"
PORTALS_RECEIVED_OFFERS_PATH = "/offers/received"
PORTALS_PLACED_OFFERS_PATH = "/offers/placed"
PORTALS_CREATE_OFFER_PATH = "/offers/"
PORTALS_NFT_OFFERS_PATH = "/offers/nft/{nft_id}"
PORTALS_ACCEPT_OFFER_PATH = "/offers/{offer_id}/accept"
PORTALS_CANCEL_OFFER_PATH = "/offers/{offer_id}/cancel"
PORTALS_HTTP_TIMEOUT_SECONDS = 20
PORTALS_SAFE_ATTEMPTS = 2
PORTALS_PAGE_SIZE = 20
PORTALS_MAX_OFFER_PAGES = 5
PORTALS_RECONCILE_DELAYS_SECONDS = (0.0, 0.5, 1.0, 1.5, 2.0, 2.0, 3.0)
PORTALS_CREATE_CLOCK_SKEW_SECONDS = 10
PORTALS_CREATE_FUTURE_TOLERANCE_SECONDS = 5

_GET_PATHS = frozenset(
    {
        PORTALS_AUTH_PATH,
        PORTALS_OWNED_NFTS_PATH,
        PORTALS_RECEIVED_OFFERS_PATH,
        PORTALS_PLACED_OFFERS_PATH,
    }
)
_NFT_OFFERS_RE = re.compile(r"^/offers/nft/[^/?#]+$")
_ACCEPT_OFFER_RE = re.compile(r"^/offers/[^/?#]+/accept$")
_CANCEL_OFFER_RE = re.compile(r"^/offers/[^/?#]+/cancel$")
_OFFER_AMOUNT_RE = re.compile(r"^\d+(?:\.\d{1,9})?$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_. -]{1,64}$")


class PortalsServiceError(RuntimeError):
    """Base class for sanitized Portals failures."""


class PortalsNetworkError(PortalsServiceError):
    """A read-only Portals request failed without exposing credentials."""


class PortalsMutationNetworkError(PortalsServiceError):
    """The accept request may have reached Portals."""


class PortalsAuthenticationError(PortalsServiceError):
    """Portals rejected a fresh Telegram Mini App authorization."""


class PortalsOfferUnavailableError(PortalsServiceError):
    """The selected offer cannot safely be accepted."""


class PortalsOfferChangedError(PortalsOfferUnavailableError):
    """The selected offer changed during final revalidation."""


class PortalsOfferNotFoundError(PortalsOfferUnavailableError):
    """The selected offer disappeared during final revalidation."""


class PortalsCreateOfferUnconfirmedError(PortalsServiceError):
    """Portals did not return a usable ID for the newly created offer."""

    def __init__(
        self,
        message: str,
        reason: str = "CREATE_OFFER_UNCONFIRMED",
    ) -> None:
        self.reason = reason
        super().__init__(message)


class PortalsApiError(PortalsServiceError):
    def __init__(self, status: int, code: str | None = None) -> None:
        self.status = status
        self.code = code
        super().__init__(self.user_message)

    @property
    def user_message(self) -> str:
        messages = {
            400: "Portals отклонил запрос принятия оффера.",
            401: "Авторизация Portals истекла. Запустите задание заново.",
            403: "Portals не разрешает принять этот оффер.",
            404: "Выбранный оффер больше не существует.",
            409: "Состояние оффера изменилось.",
            429: "Portals ограничил частоту запросов. Попробуйте позже.",
        }
        if self.status >= 500:
            return "Portals временно недоступен. Запрос принятия не повторялся."
        return messages.get(
            self.status,
            f"Portals отклонил запрос с кодом HTTP {self.status}.",
        )


class PortalsAcceptStatus(StrEnum):
    SUCCESS = "success"
    DRY_RUN = "dry_run"
    AMBIGUOUS = "ambiguous"


class PortalsCancelStatus(StrEnum):
    SUCCESS = "success"
    ALREADY_INACTIVE = "already_inactive"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class PortalsOffer:
    display_name: str
    amount_text: str
    eligible: bool
    eligibility_reason: str | None
    offer_id: Any = field(repr=False)
    nft_id: Any = field(repr=False)
    amount: Any = field(repr=False)
    sender_id: Any = field(default=None, repr=False)
    status: str | None = field(default=None, repr=False)
    created_at: str | None = field(default=None, repr=False)
    updated_at: str | None = field(default=None, repr=False)
    expires_at: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class PortalsNft:
    display_name: str
    eligible: bool
    eligibility_reason: str | None
    nft_id: Any = field(repr=False)


@dataclass(frozen=True, slots=True)
class PortalsCreateOfferResult:
    offer_id: Any = field(repr=False)
    nft_id: Any = field(repr=False)
    amount: str
    request_sent: bool
    response_status: int | None
    response_fields: tuple[str, ...]
    sender_id: Any = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class PortalsAcceptResult:
    status: PortalsAcceptStatus
    display_name: str
    amount_text: str
    accept_request_sent: bool
    response_status: int | None
    response_fields: tuple[str, ...]
    verification_fields: tuple[str, ...]
    message: str


@dataclass(frozen=True, slots=True)
class PortalsCancelResult:
    status: PortalsCancelStatus
    display_name: str
    amount_text: str
    cancel_request_sent: bool
    response_status: int | None
    response_fields: tuple[str, ...]


@dataclass(slots=True)
class PortalsHttpResponse:
    status: int
    body: Any = field(repr=False)


class PortalsHttpTransport(Protocol):
    def get_json(
        self,
        path: str,
        *,
        query: Mapping[str, str | int] | None,
        authorization: str,
    ) -> PortalsHttpResponse: ...

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any] | None,
        *,
        authorization: str,
    ) -> PortalsHttpResponse: ...

    def close(self) -> None: ...


TransportFactory = Callable[[], PortalsHttpTransport]


@dataclass(slots=True)
class _PortalsSession:
    transport: PortalsHttpTransport = field(repr=False)
    authorization: str = field(repr=False)
    launch_fingerprint: str | None
    user_id: Any = field(default=None, repr=False)
    request_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


class _UrllibPortalsTransport:
    def __init__(self) -> None:
        self._cookies = http.cookiejar.CookieJar()
        self._opener = build_opener(HTTPCookieProcessor(self._cookies))

    def get_json(
        self,
        path: str,
        *,
        query: Mapping[str, str | int] | None,
        authorization: str,
    ) -> PortalsHttpResponse:
        if path not in _GET_PATHS and not _NFT_OFFERS_RE.fullmatch(path):
            raise PortalsServiceError("Неподдерживаемый read-only путь Portals.")
        return self._request(
            method="GET",
            path=path,
            query=query,
            payload=None,
            authorization=authorization,
            mutation=False,
        )

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any] | None,
        *,
        authorization: str,
    ) -> PortalsHttpResponse:
        if (
            path != PORTALS_CREATE_OFFER_PATH
            and not _ACCEPT_OFFER_RE.fullmatch(path)
            and not _CANCEL_OFFER_RE.fullmatch(path)
        ):
            raise PortalsServiceError("Неподдерживаемый изменяющий путь Portals.")
        return self._request(
            method="POST",
            path=path,
            query=None,
            payload=payload,
            authorization=authorization,
            mutation=True,
        )

    def _request(
        self,
        *,
        method: str,
        path: str,
        query: Mapping[str, str | int] | None,
        payload: Mapping[str, Any] | None,
        authorization: str,
        mutation: bool,
    ) -> PortalsHttpResponse:
        query_string = urlencode(query or {})
        url = f"{PORTALS_API_ORIGIN}{path}"
        if query_string:
            url = f"{url}?{query_string}"
        data = (
            json.dumps(payload, separators=(",", ":")).encode()
            if payload is not None
            else None
        )
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Authorization": authorization,
                "Content-Type": "application/json",
                "Origin": PORTALS_FRONTEND_ORIGIN,
                "Referer": f"{PORTALS_FRONTEND_ORIGIN}/",
                "User-Agent": "Mozilla/5.0 Portals-client/1.0",
                "x-request-id": str(uuid.uuid4()),
            },
        )
        try:
            with self._opener.open(
                request, timeout=PORTALS_HTTP_TIMEOUT_SECONDS
            ) as response:
                status = response.status
                raw_body = response.read()
        except HTTPError as exc:
            status = exc.code
            raw_body = exc.read()
        except (URLError, TimeoutError, OSError) as exc:
            error_type = type(exc).__name__
            if mutation:
                raise PortalsMutationNetworkError(
                    f"Portals mutation network failure: {error_type}"
                ) from None
            raise PortalsNetworkError(
                f"Portals network failure: {error_type}"
            ) from None
        try:
            body = json.loads(raw_body) if raw_body else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = None
        return PortalsHttpResponse(status=status, body=body)

    def close(self) -> None:
        self._cookies.clear()


class PortalsService:
    """Use fresh official WebView data for safe Portals offer operations."""

    def __init__(
        self,
        miniapp_service: MiniAppService,
        *,
        dry_run: bool = True,
        transport_factory: TransportFactory | None = None,
        reconciliation_delays: tuple[float, ...] | None = None,
        cancel_verification_delays: tuple[float, ...] = (0.0, 0.5, 1.0),
    ) -> None:
        self._miniapp_service = miniapp_service
        self._dry_run = dry_run
        self._transport_factory = transport_factory or _UrllibPortalsTransport
        self._reconciliation_delays = (
            reconciliation_delays
            if reconciliation_delays is not None
            else PORTALS_RECONCILE_DELAYS_SECONDS
        )
        self._cancel_verification_delays = cancel_verification_delays
        self._job_sessions: ContextVar[
            dict[tuple[int, int], _PortalsSession] | None
        ] = ContextVar("portals_job_sessions", default=None)

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    @asynccontextmanager
    async def transfer_session_scope(
        self,
        owner_telegram_id: int,
        source_account_id: int,
        target_account_id: int,
    ) -> AsyncIterator[None]:
        """Prepare and reuse two fresh Portals sessions for one transfer job."""

        existing = self._job_sessions.get()
        if existing is not None and all(
            (owner_telegram_id, account_id) in existing
            for account_id in (source_account_id, target_account_id)
        ):
            yield
            return

        async def prepare(account_id: int, stage: str) -> _PortalsSession:
            started_at = time.monotonic()
            try:
                return await self._authenticate(account_id, owner_telegram_id)
            finally:
                stage_timing(stage, started_at)

        results = await asyncio.gather(
            prepare(source_account_id, "portals_n1_auth"),
            prepare(target_account_id, "portals_n2_auth"),
            return_exceptions=True,
        )
        sessions = [item for item in results if isinstance(item, _PortalsSession)]
        failure = next(
            (item for item in results if isinstance(item, BaseException)), None
        )
        if failure is not None:
            for session in sessions:
                self._close_session(session)
            raise failure
        source_session, target_session = sessions
        cache = {
            (owner_telegram_id, source_account_id): source_session,
            (owner_telegram_id, target_account_id): target_session,
        }
        token = self._job_sessions.set(cache)
        try:
            yield
        finally:
            self._job_sessions.reset(token)
            self._close_session(target_session)
            self._close_session(source_session)

    async def _acquire_session(
        self, account_id: int, owner_telegram_id: int
    ) -> tuple[_PortalsSession, bool]:
        cached = self._job_sessions.get()
        if cached is not None:
            session = cached.get((owner_telegram_id, account_id))
            if session is not None:
                return session, False
        return await self._authenticate(account_id, owner_telegram_id), True

    def _release_session(self, session: _PortalsSession, owned: bool) -> None:
        if owned:
            self._close_session(session)

    async def get_received_offers(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsOffer]:
        session, owned = await self._acquire_session(account_id, owner_telegram_id)
        try:
            return await self._fetch_received_offers(session)
        finally:
            self._release_session(session, owned)

    async def get_placed_offers(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsOffer]:
        session, owned = await self._acquire_session(account_id, owner_telegram_id)
        try:
            offers = await self._fetch_placed_offers(session)
            return [offer for offer in offers if self._is_active_offer(offer)]
        finally:
            self._release_session(session, owned)

    async def get_owned_nfts(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsNft]:
        session, owned = await self._acquire_session(account_id, owner_telegram_id)
        try:
            return await self._fetch_owned_nfts(session)
        finally:
            self._release_session(session, owned)

    async def create_offer(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        nft_id: Any,
        amount: str,
        dry_run: bool = True,
        reconciliation_account_id: int | None = None,
    ) -> PortalsCreateOfferResult:
        """Create one NFT offer, without retrying the mutation."""
        self._validate_id(nft_id, "NFT")
        normalized_amount = self.normalize_offer_amount(amount)
        session, session_owned = await self._acquire_session(
            account_id, owner_telegram_id
        )
        payload: dict[str, Any] = {
            "offer": {"nft_id": nft_id, "offer_price": normalized_amount}
        }
        try:
            if dry_run:
                logger.info(
                    "Portals transfer dry run account_db_id=%d path=%s "
                    "request_fields=offer offer_fields=nft_id,offer_price",
                    account_id,
                    PORTALS_CREATE_OFFER_PATH,
                )
                return PortalsCreateOfferResult(
                    offer_id=None,
                    nft_id=nft_id,
                    amount=normalized_amount,
                    request_sent=False,
                    response_status=None,
                    response_fields=(),
                )
            logger.info(
                "Portals create-offer started account_db_id=%d path=%s "
                "request_fields=offer offer_fields=nft_id,offer_price",
                account_id,
                PORTALS_CREATE_OFFER_PATH,
            )
            create_started_at = datetime.now(timezone.utc)
            create_timing_started_at = time.monotonic()
            try:
                mutation_started("portals_create_offer")
                response = await self._post_json(
                    session,
                    PORTALS_CREATE_OFFER_PATH,
                    payload,
                )
            except Exception as exc:  # noqa: BLE001 - delivery after POST is uncertain
                logger.warning(
                    "Portals create delivery uncertain exception=%s", type(exc).__name__
                )
                raise PortalsMutationNetworkError(
                    "Результат отправки оффера требует проверки."
                ) from None
            finally:
                stage_timing("portals_create_offer", create_timing_started_at)
            try:
                response_metadata(
                    "portals_create_offer", response.status, response.body
                )
                created_offer(self._created_offer_id(response.body))
                response_fields = self._field_names(response.body)
                if response.status >= 500:
                    raise PortalsCreateOfferUnconfirmedError(
                        "Результат отправки оффера требует проверки."
                    )
                if not 200 <= response.status < 300:
                    raise self._api_error(response)
                offer_id = self._created_offer_id(response.body)
                reconciled: PortalsOffer | None = None
                if response.status == 204 and offer_id is None:
                    target_session: _PortalsSession | None = None
                    target_state_uncertain = False
                    try:
                        if reconciliation_account_id is not None:
                            try:
                                target_session, target_session_owned = (
                                    await self._acquire_session(
                                        reconciliation_account_id,
                                        owner_telegram_id,
                                    )
                                )
                            except Exception as exc:  # noqa: BLE001 - optional read evidence
                                target_state_uncertain = True
                                logger.info(
                                    "Portals create reconciliation target auth failed "
                                    "exception=%s",
                                    type(exc).__name__,
                                )
                        reconciled = await self._reconcile_created_offer(
                            session,
                            target_session=target_session,
                            target_state_uncertain=target_state_uncertain,
                            nft_id=nft_id,
                            amount=normalized_amount,
                            created_after=create_started_at,
                            response_received_at=asyncio.get_running_loop().time(),
                        )
                    finally:
                        if target_session is not None:
                            self._release_session(
                                target_session, target_session_owned
                            )
                    offer_id = reconciled.offer_id
                    created_offer(offer_id)
                if offer_id is None:
                    raise PortalsCreateOfferUnconfirmedError(
                        "Portals не вернул идентификатор созданного оффера."
                    )
                sender_id = (
                    reconciled.sender_id
                    if reconciled is not None
                    else self._created_offer_sender_id(response.body)
                )
                if session.user_id is not None:
                    if sender_id is not None and not self._same_value(
                        sender_id, session.user_id
                    ):
                        raise PortalsCreateOfferUnconfirmedError(
                            "Не удалось подтвердить отправителя оффера."
                        )
                    sender_id = session.user_id
                logger.info(
                    "Portals create-offer confirmed account_db_id=%d status=%d "
                    "response_fields=%s",
                    account_id,
                    response.status,
                    ",".join(response_fields),
                )
                return PortalsCreateOfferResult(
                    offer_id=offer_id,
                    nft_id=nft_id,
                    amount=normalized_amount,
                    request_sent=True,
                    response_status=response.status,
                    response_fields=response_fields,
                    sender_id=sender_id,
                )
            finally:
                response.body = None
        finally:
            nested = payload.get("offer")
            if isinstance(nested, dict):
                nested.clear()
            payload.clear()
            self._release_session(session, session_owned)

    async def accept_transfer_offer(
        self,
        *,
        owner_telegram_id: int,
        source_account_id: int,
        target_account_id: int,
        offer_id: Any,
        nft_id: Any,
        amount: Any,
        display_name: str,
        expected_sender_id: Any = None,
    ) -> PortalsAcceptResult:
        """Freshly correlate one transfer offer without requiring /received."""
        self._validate_id(offer_id, "offer")
        self._validate_id(nft_id, "NFT")
        self._validate_amount(amount)
        source_session: _PortalsSession | None = None
        target_session: _PortalsSession | None = None
        source_session_owned = False
        target_session_owned = False
        verification_started_at = time.monotonic()
        verification_timing_emitted = False
        try:
            source_session, source_session_owned = await self._acquire_session(
                source_account_id, owner_telegram_id
            )
            target_session, target_session_owned = await self._acquire_session(
                target_account_id, owner_telegram_id
            )

            owned_result, placed_result, nft_result = await asyncio.gather(
                self._fetch_owned_nfts(target_session),
                self._fetch_placed_offers(source_session),
                self._fetch_nft_for_reconciliation(target_session, nft_id),
            )
            received: list[PortalsOffer] | None = None
            owned = owned_result
            owned_matches = [
                nft for nft in owned if self._same_value(nft.nft_id, nft_id)
            ]
            n2_owns_nft = len(owned_matches) == 1
            placed = placed_result
            nft_offers, _ = nft_result

            placed_by_id = [
                offer
                for offer in placed
                if self._same_value(offer.offer_id, offer_id)
            ]
            nft_by_id = [
                offer
                for offer in nft_offers
                if self._same_value(offer.offer_id, offer_id)
            ]
            received_by_id = [
                offer
                for offer in (received or [])
                if self._same_value(offer.offer_id, offer_id)
            ]
            placed_exact_active = self._active_transfer_candidates(
                placed, nft_id=nft_id, amount=amount
            )
            nft_exact_active = self._active_transfer_candidates(
                nft_offers, nft_id=nft_id, amount=amount
            )
            placed_current = placed_by_id[0] if len(placed_by_id) == 1 else None
            nft_current = nft_by_id[0] if len(nft_by_id) == 1 else None
            placed_match = (
                placed_current is not None
                and len(placed_exact_active) == 1
                and self._same_value(
                    placed_exact_active[0].offer_id, placed_current.offer_id
                )
                and self._same_value(placed_current.nft_id, nft_id)
                and self._same_amount(placed_current.amount, amount)
                and self._is_active_offer(placed_current)
            )
            nft_offer_match = (
                nft_current is not None
                and len(nft_exact_active) == 1
                and self._same_value(
                    nft_exact_active[0].offer_id, nft_current.offer_id
                )
                and self._same_value(nft_current.nft_id, nft_id)
                and self._same_amount(nft_current.amount, amount)
                and self._is_active_offer(nft_current)
            )
            received_match = (
                len(received_by_id) == 1
                and self._same_value(received_by_id[0].nft_id, nft_id)
                and self._same_amount(received_by_id[0].amount, amount)
                and self._is_active_offer(received_by_id[0])
            )
            sender_expected = (
                source_session.user_id
                if source_session.user_id is not None
                else expected_sender_id
            )
            sender_match = (
                self._same_value(nft_current.sender_id, sender_expected)
                if nft_current is not None
                and nft_current.sender_id is not None
                and sender_expected is not None
                else None
            )
            offer_checks(
                "portals_job_before_accept",
                actual_id=nft_current.offer_id if nft_current else None,
                expected_id=offer_id,
                actual_nft=nft_current.nft_id if nft_current else None,
                expected_nft=nft_id,
                actual_amount=nft_current.amount if nft_current else None,
                expected_amount=amount,
                actual_sender=nft_current.sender_id if nft_current else None,
                expected_sender=sender_expected,
                match_count=len(nft_by_id),
                placed_match=placed_match,
                nft_offer_match=nft_offer_match,
                received_match=received_match if received is not None else None,
                n2_owns_nft=n2_owns_nft,
                sender_match=sender_match,
            )

            if not n2_owns_nft:
                raise PortalsOfferUnavailableError(
                    "Рабочий аккаунт больше не владеет выбранным NFT."
                )
            if len(placed_exact_active) > 1 or len(nft_exact_active) > 1:
                raise PortalsOfferChangedError(
                    "Найдено несколько точных офферов; принятие остановлено."
                )
            if len(placed_by_id) != 1 or len(nft_by_id) != 1:
                raise PortalsOfferNotFoundError(
                    "Точный оффер не найден во всех обязательных источниках."
                )
            if not placed_match or not nft_offer_match:
                raise PortalsOfferChangedError(
                    "Данные оффера изменились перед принятием."
                )
            assert placed_current is not None
            assert nft_current is not None
            if (
                expected_sender_id is not None
                and source_session.user_id is not None
                and not self._same_value(
                    expected_sender_id, source_session.user_id
                )
            ):
                raise PortalsOfferChangedError("Аккаунт отправителя изменился.")
            if (
                placed_current.sender_id is not None
                and source_session.user_id is not None
                and not self._same_value(
                    placed_current.sender_id, source_session.user_id
                )
            ):
                raise PortalsOfferChangedError("Отправитель оффера изменился.")
            if nft_current.sender_id is not None and (
                sender_expected is None
                or not self._same_value(nft_current.sender_id, sender_expected)
            ):
                raise PortalsOfferChangedError("Отправитель оффера изменился.")

            stage_timing("portals_pre_accept_verification", verification_started_at)
            verification_timing_emitted = True
            return await self._send_accept_request(
                target_session,
                nft_current,
                account_id=target_account_id,
                display_name=display_name,
                dry_run=False,
            )
        finally:
            if not verification_timing_emitted:
                stage_timing(
                    "portals_pre_accept_verification", verification_started_at
                )
            if target_session is not None:
                self._release_session(target_session, target_session_owned)
            if source_session is not None:
                self._release_session(source_session, source_session_owned)

    async def accept_offer(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        offer_id: Any,
        nft_id: Any,
        amount: Any,
        display_name: str,
        dry_run: bool | None = None,
        expected_sender_id: Any = None,
    ) -> PortalsAcceptResult:
        self._validate_id(offer_id, "offer")
        self._validate_id(nft_id, "NFT")
        self._validate_amount(amount)
        session = await self._authenticate(account_id, owner_telegram_id)
        try:
            received = await self._fetch_received_offers(session)
            current = self._find_offer(received, offer_id)
            offer_checks(
                "portals_service_before_accept",
                actual_id=current.offer_id if current else None,
                expected_id=offer_id,
                actual_nft=current.nft_id if current else None,
                expected_nft=nft_id,
                actual_amount=current.amount if current else None,
                expected_amount=amount,
                actual_sender=current.sender_id if current else None,
                expected_sender=expected_sender_id,
                match_count=sum(
                    self._same_value(o.offer_id, offer_id) for o in received
                ),
            )
            self._validate_current_offer(current, nft_id=nft_id, amount=amount)
            assert current is not None
            if (
                expected_sender_id is not None
                and current.sender_id is not None
                and not self._same_value(current.sender_id, expected_sender_id)
            ):
                raise PortalsOfferChangedError("Отправитель оффера изменился.")
            await self._validate_offer_details(session, current)
            return await self._send_accept_request(
                session,
                current,
                account_id=account_id,
                display_name=display_name,
                dry_run=self._dry_run if dry_run is None else dry_run,
            )
        finally:
            self._close_session(session)

    async def _send_accept_request(
        self,
        session: _PortalsSession,
        current: PortalsOffer,
        *,
        account_id: int,
        display_name: str,
        dry_run: bool,
    ) -> PortalsAcceptResult:
        path = PORTALS_ACCEPT_OFFER_PATH.format(
            offer_id=quote(str(current.offer_id), safe="")
        )
        payload = {"amount": current.amount}
        if dry_run:
            payload.clear()
            return PortalsAcceptResult(
                status=PortalsAcceptStatus.DRY_RUN,
                display_name=self._safe_display_name(display_name),
                amount_text=current.amount_text,
                accept_request_sent=False,
                response_status=None,
                response_fields=(),
                verification_fields=(),
                message="Проверка завершена. Оффер не был принят.",
            )

        accept_started_at = time.monotonic()
        try:
            mutation_started("portals_accept")
            response = await self._post_json(session, path, payload)
        except (PortalsMutationNetworkError, PortalsNetworkError):
            stage_timing("portals_accept_request", accept_started_at)
            payload.clear()
            verification_fields = await self._verify_after_ambiguous(
                session, current
            )
            logger.warning(
                "Portals accept result ambiguous account_db_id=%d exception=network",
                account_id,
            )
            return PortalsAcceptResult(
                status=PortalsAcceptStatus.AMBIGUOUS,
                display_name=current.display_name,
                amount_text=current.amount_text,
                accept_request_sent=True,
                response_status=None,
                response_fields=(),
                verification_fields=verification_fields,
                message=(
                    "Результат принятия оффера не определён. Проверьте Portals "
                    "вручную перед новой попыткой."
                ),
            )
        else:
            stage_timing("portals_accept_request", accept_started_at)
        finally:
            payload.clear()

        response_metadata("portals_accept", response.status, response.body)
        response_fields = self._field_names(response.body)
        logger.info(
            "Portals accept response account_db_id=%d status=%d fields=%s",
            account_id,
            response.status,
            ",".join(response_fields),
        )
        try:
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            return PortalsAcceptResult(
                status=PortalsAcceptStatus.SUCCESS,
                display_name=current.display_name,
                amount_text=current.amount_text,
                accept_request_sent=True,
                response_status=response.status,
                response_fields=response_fields,
                verification_fields=(),
                message="Portals подтвердил принятие оффера.",
            )
        finally:
            response.body = None

    async def cancel_offer(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        offer_id: Any,
        nft_id: Any,
        amount: Any,
    ) -> PortalsCancelResult:
        """Cancel one exact placed offer without retrying the mutation."""
        self._validate_id(offer_id, "offer")
        self._validate_id(nft_id, "NFT")
        self._validate_amount(amount)
        session = await self._authenticate(account_id, owner_telegram_id)
        try:
            placed = await self._fetch_placed_offers(session)
            current = self._find_offer(placed, offer_id)
            if current is None or not self._is_active_offer(current):
                return PortalsCancelResult(
                    status=PortalsCancelStatus.ALREADY_INACTIVE,
                    display_name="NFT",
                    amount_text=self._safe_amount_text(amount),
                    cancel_request_sent=False,
                    response_status=None,
                    response_fields=(),
                )
            if not self._same_value(current.nft_id, nft_id):
                raise PortalsOfferChangedError("NFT оффера изменился перед отзывом.")
            if not self._same_amount(current.amount, amount):
                raise PortalsOfferChangedError("Сумма оффера изменилась перед отзывом.")

            path = PORTALS_CANCEL_OFFER_PATH.format(
                offer_id=quote(str(current.offer_id), safe="")
            )
            try:
                response = await self._post_json(session, path, None)
            except (PortalsMutationNetworkError, PortalsNetworkError):
                definitely_inactive = await self._verify_cancelled_after_ambiguous(
                    session, current.offer_id
                )
                logger.warning(
                    "Portals cancel result ambiguous account_db_id=%d "
                    "definitely_inactive=%s",
                    account_id,
                    definitely_inactive,
                )
                return PortalsCancelResult(
                    status=(
                        PortalsCancelStatus.SUCCESS
                        if definitely_inactive
                        else PortalsCancelStatus.AMBIGUOUS
                    ),
                    display_name=current.display_name,
                    amount_text=current.amount_text,
                    cancel_request_sent=True,
                    response_status=None,
                    response_fields=(),
                )

            response_metadata("portals_cancel_offer", response.status, response.body)
            response_fields = self._field_names(response.body)
            logger.info(
                "Portals cancel response account_db_id=%d status=%d fields=%s",
                account_id,
                response.status,
                ",".join(response_fields),
            )
            try:
                if not 200 <= response.status < 300:
                    raise self._api_error(response)
                return PortalsCancelResult(
                    status=PortalsCancelStatus.SUCCESS,
                    display_name=current.display_name,
                    amount_text=current.amount_text,
                    cancel_request_sent=True,
                    response_status=response.status,
                    response_fields=response_fields,
                )
            finally:
                response.body = None
        finally:
            self._close_session(session)

    async def _authenticate(
        self, account_id: int, owner_telegram_id: int
    ) -> _PortalsSession:
        launch = await self._miniapp_service.open_miniapp(
            account_id,
            "@portals",
            owner_telegram_id=owner_telegram_id,
        )
        self._validate_launch(launch)
        assert launch.init_data is not None
        authorization = f"tma {launch.init_data}"
        transport = self._transport_factory()
        session = _PortalsSession(
            transport=transport,
            authorization=authorization,
            launch_fingerprint=launch.init_data_fingerprint,
        )
        try:
            response = await self._safe_get(session, PORTALS_AUTH_PATH)
            fields = self._field_names(response.body)
            logger.info(
                "Portals auth response account_db_id=%d status=%d fields=%s",
                account_id,
                response.status,
                ",".join(fields),
            )
            try:
                if not 200 <= response.status < 300:
                    raise self._api_error(response)
                if not isinstance(response.body, dict):
                    raise PortalsAuthenticationError(
                        "Portals вернул некорректный ответ авторизации."
                    )
                user_id = response.body.get("user_id")
                if isinstance(user_id, (str, int)) and not isinstance(user_id, bool):
                    session.user_id = user_id
                response.body.pop("token", None)
            finally:
                response.body = None
            return session
        except Exception:
            self._close_session(session)
            raise

    async def _fetch_received_offers(
        self, session: _PortalsSession
    ) -> list[PortalsOffer]:
        offers: list[PortalsOffer] = []
        offset = 0
        for _ in range(PORTALS_MAX_OFFER_PAGES):
            response = await self._safe_get(
                session,
                PORTALS_RECEIVED_OFFERS_PATH,
                query={"offset": offset, "limit": PORTALS_PAGE_SIZE},
            )
            try:
                if not 200 <= response.status < 300:
                    raise self._api_error(response)
                if not isinstance(response.body, dict):
                    raise PortalsServiceError(
                        "Portals вернул некорректный список офферов."
                    )
                raw_offers = response.body.get("top_offers")
                if not isinstance(raw_offers, list):
                    raise PortalsServiceError(
                        "Ответ Portals не содержит список полученных офферов."
                    )
                parsed = [
                    offer
                    for raw in raw_offers
                    if isinstance(raw, dict)
                    and (offer := self._parse_received_offer(raw)) is not None
                ]
                offers.extend(parsed)
                total_count = response.body.get("total_count")
                if len(raw_offers) < PORTALS_PAGE_SIZE:
                    break
                offset += len(raw_offers)
                if isinstance(total_count, int) and offset >= total_count:
                    break
            finally:
                response.body = None
        return offers

    async def _fetch_owned_nfts(self, session: _PortalsSession) -> list[PortalsNft]:
        nfts: list[PortalsNft] = []
        offset = 0
        for _ in range(PORTALS_MAX_OFFER_PAGES):
            response = await self._safe_get(
                session,
                PORTALS_OWNED_NFTS_PATH,
                query={"offset": offset, "limit": PORTALS_PAGE_SIZE},
            )
            try:
                if not 200 <= response.status < 300:
                    raise self._api_error(response)
                if not isinstance(response.body, dict):
                    raise PortalsServiceError(
                        "Portals вернул некорректный список NFT."
                    )
                raw_nfts = response.body.get("nfts")
                if not isinstance(raw_nfts, list):
                    raise PortalsServiceError(
                        "Ответ Portals не содержит список NFT."
                    )
                for raw in raw_nfts:
                    nft = self._parse_owned_nft(raw) if isinstance(raw, dict) else None
                    if nft is None or any(
                        self._same_value(n.nft_id, nft.nft_id) for n in nfts
                    ):
                        raise PortalsServiceError(
                            "Не удалось однозначно загрузить список NFT."
                        )
                    nfts.append(nft)
                offset += len(raw_nfts)
                total = response.body.get("total_count")
                if isinstance(total, int) and not isinstance(total, bool):
                    if offset >= total:
                        return nfts
                    if not raw_nfts:
                        break
                elif len(raw_nfts) < PORTALS_PAGE_SIZE:
                    return nfts
            finally:
                response.body = None
        raise PortalsServiceError(
            "Не удалось полностью загрузить список NFT. Попробуйте позже."
        )

    async def _fetch_placed_offers(
        self, session: _PortalsSession
    ) -> list[PortalsOffer]:
        offers: list[PortalsOffer] = []
        offset = 0
        for _ in range(PORTALS_MAX_OFFER_PAGES):
            response = await self._safe_get(
                session,
                PORTALS_PLACED_OFFERS_PATH,
                query={"offset": offset, "limit": PORTALS_PAGE_SIZE},
            )
            try:
                if not 200 <= response.status < 300:
                    raise self._api_error(response)
                if not isinstance(response.body, dict):
                    raise PortalsServiceError(
                        "Portals вернул некорректный список отправленных офферов."
                    )
                raw_offers = response.body.get("offers")
                if not isinstance(raw_offers, list):
                    raise PortalsServiceError(
                        "Ответ Portals не содержит список отправленных офферов."
                    )
                offers.extend(
                    offer
                    for raw in raw_offers
                    if isinstance(raw, dict)
                    and (offer := self._parse_reconciliation_offer(raw)) is not None
                )
                total_count = response.body.get("total_count")
                if len(raw_offers) < PORTALS_PAGE_SIZE:
                    break
                offset += len(raw_offers)
                if isinstance(total_count, int) and offset >= total_count:
                    break
            finally:
                response.body = None
        return self._distinct_offers(offers)

    async def _reconcile_created_offer(
        self,
        session: _PortalsSession,
        *,
        target_session: _PortalsSession | None,
        target_state_uncertain: bool,
        nft_id: Any,
        amount: str,
        created_after: datetime,
        response_received_at: float,
    ) -> PortalsOffer:
        """Observe a successful 204 for about ten seconds; never resend the POST."""
        reconciliation_started_at = time.monotonic()
        state_uncertain = target_state_uncertain
        previous: dict[str, dict[tuple[type[Any], Any], PortalsOffer]] = {
            "n1_placed": {},
            "nft_offers": {},
            "n2_received": {},
        }
        multiple_seen = False
        candidate_disappeared = False
        terminal_reason: str | None = None
        resolved: PortalsOffer | None = None
        rate_limited_seen = False

        for attempt, delay in enumerate(self._reconciliation_delays, start=1):
            if delay:
                await asyncio.sleep(delay)

            placed: list[PortalsOffer] | None = None
            nft_offers: list[PortalsOffer] | None = None
            received: list[PortalsOffer] | None = None
            placed_count: int | None = None
            nft_count: int | None = None
            received_count: int | None = None
            async def fetch_placed() -> tuple[list[PortalsOffer], int]:
                return await self._fetch_placed_for_reconciliation(session)

            async def fetch_nft() -> tuple[list[PortalsOffer], int] | None:
                if target_session is None:
                    return None
                return await self._fetch_nft_for_reconciliation(
                    target_session, nft_id
                )

            placed_result, nft_result = await asyncio.gather(
                fetch_placed(),
                fetch_nft(),
                return_exceptions=True,
            )
            if isinstance(placed_result, BaseException):
                state_uncertain = True
                rate_limited_seen |= (
                    isinstance(placed_result, PortalsApiError)
                    and placed_result.status == 429
                )
                logger.info(
                    "Portals create reconciliation placed read failed "
                    "attempt=%d exception=%s",
                    attempt,
                    type(placed_result).__name__,
                )
            else:
                placed, placed_count = placed_result
            if isinstance(nft_result, BaseException) or nft_result is None:
                if target_session is not None:
                    state_uncertain = True
                    rate_limited_seen |= (
                        isinstance(nft_result, PortalsApiError)
                        and nft_result.status == 429
                    )
                    logger.info(
                        "Portals create reconciliation NFT read failed "
                        "attempt=%d exception=%s",
                        attempt,
                        type(nft_result).__name__,
                    )
            else:
                nft_offers, nft_count = nft_result

            exact_by_endpoint = {
                "n1_placed": self._exact_offer_candidates(
                    placed or [], nft_id=nft_id, amount=amount
                ),
                "nft_offers": self._exact_offer_candidates(
                    nft_offers or [], nft_id=nft_id, amount=amount
                ),
                "n2_received": self._exact_offer_candidates(
                    received or [], nft_id=nft_id, amount=amount
                ),
            }
            endpoint_available = {
                "n1_placed": placed is not None,
                "nft_offers": nft_offers is not None,
                "n2_received": target_session is not None and received is not None,
            }
            for endpoint, candidates in exact_by_endpoint.items():
                if not endpoint_available[endpoint]:
                    continue
                current = self._offer_map(candidates)
                missing = set(previous[endpoint]) - set(current)
                if any(
                    (
                        endpoint == "n1_placed"
                        or self._sender_matches_when_available(
                            previous[endpoint][key], session.user_id
                        )
                    )
                    and self._created_in_window(
                        previous[endpoint][key].created_at, created_after
                    )
                    for key in missing
                ):
                    candidate_disappeared = True
                self._observe_offer_candidates(
                    attempt=attempt,
                    endpoint=endpoint,
                    current=current,
                    previous=previous[endpoint],
                    created_after=created_after,
                )
                previous[endpoint] = current

            all_exact = [
                offer for offers in exact_by_endpoint.values() for offer in offers
            ]
            exact = self._distinct_offers(all_exact)
            correlated_by_id: dict[tuple[type[Any], Any], PortalsOffer] = {}
            for endpoint, offers in exact_by_endpoint.items():
                for offer in offers:
                    if not self._created_in_window(offer.created_at, created_after):
                        continue
                    if endpoint != "n1_placed" and not (
                        self._sender_matches_when_available(offer, session.user_id)
                    ):
                        continue
                    key = (type(offer.offer_id), offer.offer_id)
                    correlated_by_id.setdefault(key, offer)
            correlated = list(correlated_by_id.values())
            active = [offer for offer in correlated if self._is_active_offer(offer)]
            placed_active = [
                offer
                for offer in exact_by_endpoint["n1_placed"]
                if self._created_in_window(offer.created_at, created_after)
                and self._is_active_offer(offer)
            ]
            nft_active = [
                offer
                for offer in exact_by_endpoint["nft_offers"]
                if self._is_active_offer(offer)
                and self._sender_matches_when_available(offer, session.user_id)
            ]
            if len(correlated) > 1:
                multiple_seen = True
            for endpoint, offers in exact_by_endpoint.items():
                for offer in offers:
                    if endpoint != "n1_placed" and not (
                        self._sender_matches_when_available(offer, session.user_id)
                    ):
                        continue
                    if not self._created_in_window(offer.created_at, created_after):
                        continue
                    terminal_reason = terminal_reason or self._terminal_create_reason(
                        offer
                    )
            strict_resolved = (
                placed_active[0]
                if len(placed_active) == 1
                and len(nft_active) == 1
                and self._same_value(
                    placed_active[0].offer_id, nft_active[0].offer_id
                )
                and len(active) == 1
                and not multiple_seen
                else None
            )
            resolved = strict_resolved or (
                placed_active[0]
                if len(placed_active) == 1
                and len(active) == 1
                and not multiple_seen
                else None
            )
            elapsed_ms = round(
                (asyncio.get_running_loop().time() - response_received_at) * 1000
            )
            offer_reconciliation(
                attempt=attempt,
                placed_result_count=placed_count,
                nft_result_count=nft_count,
                received_result_count=received_count,
                exact_match_count=len(exact),
                active_match_count=len(active),
                resolved_offer_id=resolved.offer_id if resolved else None,
                elapsed_ms=elapsed_ms,
                state_uncertain=state_uncertain,
            )
            if attempt == 1:
                stage_timing("portals_first_reconciliation", reconciliation_started_at)
            if strict_resolved is not None and not state_uncertain:
                stage_timing("portals_reconciliation_total", reconciliation_started_at)
                logger.info(
                    "Portals create reconciliation resolved attempt=%d elapsed_ms=%d",
                    attempt,
                    elapsed_ms,
                )
                return strict_resolved

        if terminal_reason is not None:
            raise PortalsCreateOfferUnconfirmedError(
                "Созданный оффер перешёл в недоступное состояние.", terminal_reason
            )
        if multiple_seen:
            raise PortalsCreateOfferUnconfirmedError(
                "Найдено несколько похожих офферов; принятие остановлено.",
                "CREATE_OFFER_MULTIPLE_MATCHES",
            )
        if candidate_disappeared:
            raise PortalsCreateOfferUnconfirmedError(
                "Созданный оффер появился и исчез до принятия.",
                "CREATE_OFFER_DISAPPEARED",
            )
        if resolved is not None:
            stage_timing("portals_reconciliation_total", reconciliation_started_at)
            logger.info(
                "Portals create reconciliation resolved elapsed_ms=%d",
                round(
                    (asyncio.get_running_loop().time() - response_received_at) * 1000
                ),
            )
            return resolved
        stage_timing("portals_reconciliation_total", reconciliation_started_at)
        if rate_limited_seen:
            raise PortalsCreateOfferUnconfirmedError(
                "Portals ограничил read-only проверку созданного оффера.",
                "RATE_LIMITED_AFTER_CREATE",
            )
        raise PortalsCreateOfferUnconfirmedError(
            "Созданный оффер не найден в течение контрольного интервала.",
            "CREATE_OFFER_UNCONFIRMED",
        )

    async def _fetch_placed_for_reconciliation(
        self, session: _PortalsSession
    ) -> tuple[list[PortalsOffer], int]:
        response = await self._safe_get(
            session,
            PORTALS_PLACED_OFFERS_PATH,
            query={"offset": 0, "limit": PORTALS_PAGE_SIZE},
        )
        try:
            response_metadata(
                "portals_reconcile_placed", response.status, response.body
            )
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            if not isinstance(response.body, dict) or not isinstance(
                response.body.get("offers"), list
            ):
                raise PortalsServiceError(
                    "Portals вернул некорректный список отправленных офферов."
                )
            raw_offers = response.body["offers"]
            offers = [
                offer
                for raw in raw_offers
                if isinstance(raw, dict)
                and (offer := self._parse_reconciliation_offer(raw)) is not None
            ]
            return offers, len(raw_offers)
        finally:
            response.body = None

    async def _fetch_nft_for_reconciliation(
        self, session: _PortalsSession, nft_id: Any
    ) -> tuple[list[PortalsOffer], int]:
        path = PORTALS_NFT_OFFERS_PATH.format(nft_id=quote(str(nft_id), safe=""))
        response = await self._safe_get(session, path, query={"limit": 10})
        try:
            response_metadata("portals_reconcile_nft", response.status, response.body)
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            if not isinstance(response.body, dict) or not isinstance(
                response.body.get("offers"), list
            ):
                raise PortalsServiceError(
                    "Portals вернул некорректный список офферов NFT."
                )
            raw_offers = response.body["offers"]
            raw_nft = response.body.get("nft")
            response_nft_id = (
                raw_nft.get("id") if isinstance(raw_nft, dict) else None
            )
            offers = [
                offer
                for raw in raw_offers
                if isinstance(raw, dict)
                and (
                    offer := self._parse_reconciliation_offer(
                        raw, fallback_nft_id=response_nft_id
                    )
                )
                is not None
            ]
            return offers, len(raw_offers)
        finally:
            response.body = None

    async def _validate_offer_details(
        self, session: _PortalsSession, expected: PortalsOffer
    ) -> None:
        path = PORTALS_NFT_OFFERS_PATH.format(
            nft_id=quote(str(expected.nft_id), safe="")
        )
        response = await self._safe_get(session, path, query={"limit": 10})
        try:
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            if not isinstance(response.body, dict) or not isinstance(
                response.body.get("offers"), list
            ):
                raise PortalsServiceError(
                    "Portals вернул некорректные сведения об оффере."
                )
            detail = self._find_raw_offer(response.body["offers"], expected.offer_id)
            if detail is None:
                raise PortalsOfferNotFoundError("Оффер исчез перед подтверждением.")
            detail_amount = detail.get("amount")
            if not self._same_value(detail_amount, expected.amount):
                raise PortalsOfferChangedError(
                    "Сумма оффера изменилась перед подтверждением."
                )
            if not self._is_eligible(detail)[0]:
                raise PortalsOfferUnavailableError("Оффер больше нельзя принять.")
            detail_sender = self._offer_sender_id(detail)
            if (
                detail_sender is not None
                and expected.sender_id is not None
                and not self._same_value(detail_sender, expected.sender_id)
            ):
                raise PortalsOfferChangedError("Отправитель оффера изменился.")
        finally:
            response.body = None

    async def _verify_after_ambiguous(
        self, session: _PortalsSession, expected: PortalsOffer
    ) -> tuple[str, ...]:
        fields: set[str] = set()
        try:
            received = await self._fetch_received_offers(session)
            fields.add("received_offers_refreshed")
            if self._find_offer(received, expected.offer_id) is not None:
                fields.add("offer_still_received")
        except Exception as exc:  # noqa: BLE001 - verification is best effort
            logger.info(
                "Portals ambiguous received-offer verification failed exception=%s",
                type(exc).__name__,
            )
        try:
            path = PORTALS_NFT_OFFERS_PATH.format(
                nft_id=quote(str(expected.nft_id), safe="")
            )
            response = await self._safe_get(session, path, query={"limit": 10})
            fields.add("nft_offers_refreshed")
            response.body = None
        except Exception as exc:  # noqa: BLE001 - verification is best effort
            logger.info(
                "Portals ambiguous NFT verification failed exception=%s",
                type(exc).__name__,
            )
        return tuple(sorted(fields))

    async def _verify_cancelled_after_ambiguous(
        self, session: _PortalsSession, offer_id: Any
    ) -> bool:
        consecutive_absent = 0
        for delay in self._cancel_verification_delays:
            if delay:
                await asyncio.sleep(delay)
            try:
                placed = await self._fetch_placed_offers(session)
            except Exception as exc:  # noqa: BLE001 - read-only best effort
                logger.info(
                    "Portals ambiguous cancel verification failed exception=%s",
                    type(exc).__name__,
                )
                return False
            if self._find_offer(placed, offer_id) is None:
                consecutive_absent += 1
            else:
                consecutive_absent = 0
        return consecutive_absent >= 2

    async def _safe_get(
        self,
        session: _PortalsSession,
        path: str,
        *,
        query: Mapping[str, str | int] | None = None,
    ) -> PortalsHttpResponse:
        for attempt in range(PORTALS_SAFE_ATTEMPTS):
            try:
                return await asyncio.to_thread(
                    session.transport.get_json,
                    path,
                    query=query,
                    authorization=session.authorization,
                )
            except PortalsNetworkError:
                if attempt + 1 >= PORTALS_SAFE_ATTEMPTS:
                    raise
            except Exception as exc:
                if isinstance(exc, PortalsServiceError):
                    raise
                logger.warning(
                    "Portals read-only request failed path=%s exception=%s",
                    self._safe_path(path),
                    type(exc).__name__,
                )
                raise PortalsServiceError(
                    f"Ошибка read-only запроса Portals: {type(exc).__name__}"
                ) from None
        raise AssertionError("unreachable")

    @staticmethod
    async def _post_json(
        session: _PortalsSession,
        path: str,
        payload: Mapping[str, Any] | None,
    ) -> PortalsHttpResponse:
        """Send one mutation while serializing a shared cookie-backed session."""
        async with session.request_lock:
            return await asyncio.to_thread(
                session.transport.post_json,
                path,
                payload,
                authorization=session.authorization,
            )

    @classmethod
    def _parse_received_offer(cls, raw: Mapping[str, Any]) -> PortalsOffer | None:
        offer = raw.get("offer")
        nft = raw.get("nft")
        if not isinstance(offer, dict) or not isinstance(nft, dict):
            return None
        if "id" not in offer or "id" not in nft or "amount" not in offer:
            return None
        offer_id = offer["id"]
        nft_id = nft["id"]
        amount = offer["amount"]
        try:
            cls._validate_id(offer_id, "offer")
            cls._validate_id(nft_id, "NFT")
            cls._validate_amount(amount)
        except PortalsServiceError:
            return None
        eligible, reason = cls._is_eligible(offer)
        return PortalsOffer(
            offer_id=offer_id,
            nft_id=nft_id,
            amount=amount,
            amount_text=cls._safe_amount_text(amount),
            display_name=cls._nft_display_name(nft),
            eligible=eligible,
            eligibility_reason=reason,
            sender_id=cls._offer_sender_id(offer),
            status=cls._normalized_offer_status(offer),
            created_at=cls._timestamp_field(offer, raw, "created_at", "createdAt"),
            updated_at=cls._timestamp_field(offer, raw, "updated_at", "updatedAt"),
            expires_at=cls._timestamp_field(offer, raw, "expires_at", "expiresAt"),
        )

    @classmethod
    def _parse_reconciliation_offer(
        cls,
        raw: Mapping[str, Any],
        *,
        fallback_nft_id: Any = None,
    ) -> PortalsOffer | None:
        nested_offer = raw.get("offer")
        offer = nested_offer if isinstance(nested_offer, dict) else raw
        nested_nft = raw.get("nft")
        if not isinstance(nested_nft, dict):
            candidate_nft = offer.get("nft")
            nested_nft = candidate_nft if isinstance(candidate_nft, dict) else None

        offer_id = offer.get("id")
        if offer_id is None:
            offer_id = offer.get("offer_id")
        nft_id = offer.get("nft_id")
        if nft_id is None and nested_nft is not None:
            nft_id = nested_nft.get("id")
        if nft_id is None:
            nft_id = fallback_nft_id
        amount = offer.get("amount")
        if amount is None:
            amount = offer.get("offer_price")
        try:
            cls._validate_id(offer_id, "offer")
            cls._validate_id(nft_id, "NFT")
            cls._validate_amount(amount)
        except PortalsServiceError:
            return None

        eligible, reason = cls._is_eligible(offer)
        sender_id = cls._offer_sender_id(offer)
        if sender_id is None:
            sender_id = cls._offer_sender_id(raw)
        status = cls._normalized_offer_status(offer, raw)
        return PortalsOffer(
            offer_id=offer_id,
            nft_id=nft_id,
            amount=amount,
            sender_id=sender_id,
            display_name=(
                cls._nft_display_name(nested_nft) if nested_nft is not None else "NFT"
            ),
            amount_text=cls._safe_amount_text(amount),
            eligible=eligible,
            eligibility_reason=reason,
            status=status,
            created_at=cls._timestamp_field(offer, raw, "created_at", "createdAt"),
            updated_at=cls._timestamp_field(offer, raw, "updated_at", "updatedAt"),
            expires_at=cls._timestamp_field(offer, raw, "expires_at", "expiresAt"),
        )

    @classmethod
    def _exact_offer_candidates(
        cls,
        offers: list[PortalsOffer],
        *,
        nft_id: Any,
        amount: str,
    ) -> list[PortalsOffer]:
        """Match only immutable transaction inputs; retain every offer state."""
        return [
            offer
            for offer in offers
            if cls._same_value(offer.nft_id, nft_id)
            and cls._same_amount(offer.amount, amount)
        ]

    @classmethod
    def _active_transfer_candidates(
        cls,
        offers: list[PortalsOffer],
        *,
        nft_id: Any,
        amount: Any,
    ) -> list[PortalsOffer]:
        return [
            offer
            for offer in offers
            if cls._same_value(offer.nft_id, nft_id)
            and cls._same_amount(offer.amount, amount)
            and cls._is_active_offer(offer)
        ]

    @staticmethod
    def _offer_map(
        offers: list[PortalsOffer],
    ) -> dict[tuple[type[Any], Any], PortalsOffer]:
        return {(type(offer.offer_id), offer.offer_id): offer for offer in offers}

    @classmethod
    def _observe_offer_candidates(
        cls,
        *,
        attempt: int,
        endpoint: str,
        current: dict[tuple[type[Any], Any], PortalsOffer],
        previous: dict[tuple[type[Any], Any], PortalsOffer],
        created_after: datetime,
    ) -> None:
        for key, offer in current.items():
            prior = previous.get(key)
            offer_observation(
                attempt=attempt,
                endpoint=endpoint,
                offer_id=offer.offer_id,
                status=offer.status,
                previous_status=prior.status if prior is not None else None,
                created_at=offer.created_at,
                updated_at=offer.updated_at,
                expires_at=offer.expires_at,
                appeared=prior is None,
                disappeared=False,
                created_in_window=cls._created_in_window(
                    offer.created_at, created_after
                ),
            )
        for key in previous.keys() - current.keys():
            offer = previous[key]
            offer_observation(
                attempt=attempt,
                endpoint=endpoint,
                offer_id=offer.offer_id,
                status=offer.status,
                previous_status=offer.status,
                created_at=offer.created_at,
                updated_at=offer.updated_at,
                expires_at=offer.expires_at,
                appeared=False,
                disappeared=True,
                created_in_window=cls._created_in_window(
                    offer.created_at, created_after
                ),
            )

    @classmethod
    def _sender_matches_when_available(
        cls, offer: PortalsOffer, expected_sender_id: Any
    ) -> bool:
        return (
            expected_sender_id is None
            or offer.sender_id is None
            or cls._same_value(offer.sender_id, expected_sender_id)
        )

    @staticmethod
    def _is_active_offer(offer: PortalsOffer) -> bool:
        return (
            isinstance(offer.status, str)
            and offer.status.lower() in {"pending", "active"}
            and offer.eligible
        )

    @staticmethod
    def _terminal_create_reason(offer: PortalsOffer) -> str | None:
        status = offer.status.lower() if isinstance(offer.status, str) else ""
        if status in {"cancelled", "canceled"}:
            return "CREATE_OFFER_CANCELLED"
        if status == "expired":
            return "CREATE_OFFER_EXPIRED"
        if status in {"rejected", "declined", "failed"}:
            return "CREATE_OFFER_REJECTED"
        if status in {"accepted", "closed", "inactive"}:
            return "CREATE_OFFER_INACTIVE"
        return None

    @staticmethod
    def _normalized_offer_status(
        *sources: Mapping[str, Any],
    ) -> str | None:
        flags = (
            ("cancelled", "cancelled"),
            ("canceled", "cancelled"),
            ("expired", "expired"),
            ("rejected", "rejected"),
            ("accepted", "accepted"),
        )
        for source in sources:
            for key, flag_status in flags:
                if source.get(key) is True:
                    return flag_status
        for source in sources:
            raw_status = source.get("status")
            if isinstance(raw_status, str):
                return raw_status
        return None

    @staticmethod
    def _timestamp_field(
        primary: Mapping[str, Any],
        fallback: Mapping[str, Any],
        *keys: str,
    ) -> str | None:
        for source in (primary, fallback):
            for key in keys:
                value = source.get(key)
                if isinstance(value, str):
                    return value
        return None

    @staticmethod
    def _distinct_offers(offers: list[PortalsOffer]) -> list[PortalsOffer]:
        distinct: dict[tuple[type[Any], Any], PortalsOffer] = {}
        for offer in offers:
            distinct[(type(offer.offer_id), offer.offer_id)] = offer
        return list(distinct.values())

    @staticmethod
    def _created_in_window(value: str | None, created_after: datetime) -> bool:
        if value is None:
            return False
        try:
            created_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        return (
            created_after - timedelta(seconds=PORTALS_CREATE_CLOCK_SKEW_SECONDS)
            <= created_at
            <= datetime.now(timezone.utc)
            + timedelta(seconds=PORTALS_CREATE_FUTURE_TOLERANCE_SECONDS)
        )

    @classmethod
    def _parse_owned_nft(cls, raw: Mapping[str, Any]) -> PortalsNft | None:
        if "id" not in raw:
            return None
        try:
            cls._validate_id(raw["id"], "NFT")
        except PortalsServiceError:
            return None
        eligible = not any(
            raw.get(key) is False
            for key in ("canReceiveOffers", "isOfferable", "canBeOffered")
            if key in raw
        )
        return PortalsNft(
            nft_id=raw["id"],
            display_name=cls._nft_display_name(raw),
            eligible=eligible,
            eligibility_reason=(
                None
                if eligible
                else "Portals сообщает, что для NFT нельзя создать оффер."
            ),
        )

    @staticmethod
    def _offer_sender_id(raw: Mapping[str, Any]) -> Any:
        for key in ("sender_id", "buyer_id", "user_id", "owner_id"):
            value = raw.get(key)
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                return value
        sender = raw.get("sender") or raw.get("buyer") or raw.get("user")
        if isinstance(sender, dict):
            value = sender.get("id")
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                return value
        return None

    @staticmethod
    def _created_offer_id(body: Any) -> Any:
        candidates: list[Any] = [body]
        for candidate in candidates:
            if isinstance(candidate, dict):
                value = candidate.get("id")
                if value is None:
                    value = candidate.get("offer_id")
                if isinstance(value, (str, int)) and not isinstance(value, bool):
                    return value
                candidates.extend(
                    candidate.get(key) for key in ("offer", "data", "result")
                )
        return None

    @classmethod
    def _created_offer_sender_id(cls, body: Any) -> Any:
        candidates: list[Any] = [body]
        for candidate in candidates:
            if isinstance(candidate, dict):
                value = cls._offer_sender_id(candidate)
                if value is not None:
                    return value
                candidates.extend(
                    candidate.get(key) for key in ("offer", "data", "result")
                )
        return None

    @staticmethod
    def normalize_offer_amount(value: str) -> str:
        raw = value.strip()
        if not _OFFER_AMOUNT_RE.fullmatch(raw):
            raise PortalsServiceError("Введите положительную сумму оффера в TON.")
        try:
            amount = Decimal(raw)
        except InvalidOperation as exc:
            raise PortalsServiceError("Некорректная сумма оффера.") from exc
        if not amount.is_finite() or amount <= 0:
            raise PortalsServiceError("Сумма оффера должна быть больше нуля.")
        if amount < Decimal("0.5"):
            raise PortalsServiceError("Минимальная сумма оффера — 0.5 TON.")
        formatted = format(amount, "f")
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
        return formatted or "0"

    @staticmethod
    def _find_raw_offer(
        raw_offers: list[Any], offer_id: Any
    ) -> Mapping[str, Any] | None:
        for raw in raw_offers:
            candidate = raw.get("offer") if isinstance(raw, dict) else None
            if not isinstance(candidate, dict) and isinstance(raw, dict):
                candidate = raw
            if isinstance(candidate, dict) and PortalsService._same_value(
                candidate.get("id"), offer_id
            ):
                return candidate
        return None

    @staticmethod
    def _find_offer(offers: list[PortalsOffer], offer_id: Any) -> PortalsOffer | None:
        return next(
            (
                offer
                for offer in offers
                if PortalsService._same_value(offer.offer_id, offer_id)
            ),
            None,
        )

    @classmethod
    def _validate_current_offer(
        cls,
        current: PortalsOffer | None,
        *,
        nft_id: Any,
        amount: Any,
    ) -> None:
        if current is None:
            raise PortalsOfferNotFoundError("Оффер исчез перед подтверждением.")
        if not cls._same_value(current.nft_id, nft_id):
            raise PortalsOfferChangedError("NFT оффера изменился перед подтверждением.")
        if not cls._same_value(current.amount, amount):
            raise PortalsOfferChangedError(
                "Сумма оффера изменилась перед подтверждением."
            )
        if not current.eligible:
            raise PortalsOfferUnavailableError(
                current.eligibility_reason or "Оффер больше нельзя принять."
            )

    @staticmethod
    def _is_eligible(raw: Mapping[str, Any]) -> tuple[bool, str | None]:
        if any(raw.get(key) is False for key in ("canAccept", "isAcceptable")):
            return False, "Portals сообщает, что оффер нельзя принять."
        if any(raw.get(key) is True for key in ("accepted", "cancelled", "expired")):
            return False, "Оффер уже закрыт или истёк."
        status = raw.get("status")
        if isinstance(status, str) and status.lower() not in {
            "active",
            "open",
            "pending",
            "created",
        }:
            return False, "Оффер находится в недоступном состоянии."
        return True, None

    @staticmethod
    def _nft_display_name(raw: Mapping[str, Any]) -> str:
        name = next(
            (
                value
                for key in ("name", "title", "collection_name", "collectionName")
                if isinstance((value := raw.get(key)), str) and value.strip()
            ),
            "NFT",
        )
        number = raw.get("number")
        suffix = f" #{number}" if isinstance(number, (str, int)) else ""
        return PortalsService._safe_display_name(f"{name}{suffix}")

    @staticmethod
    def _safe_display_name(value: str) -> str:
        return (" ".join(value.replace("\x00", " ").split()) or "NFT")[:80]

    @staticmethod
    def _safe_amount_text(value: Any) -> str:
        text = " ".join(str(value).replace("\x00", " ").split())
        return (text or "—")[:40]

    @staticmethod
    def _validate_id(value: Any, label: str) -> None:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise PortalsServiceError(
                f"Portals вернул неподдерживаемый идентификатор {label}."
            )
        if isinstance(value, str) and not value:
            raise PortalsServiceError(f"Portals вернул пустой идентификатор {label}.")

    @staticmethod
    def _validate_amount(value: Any) -> None:
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise PortalsServiceError("Portals вернул неподдерживаемую сумму оффера.")
        if isinstance(value, str) and not value:
            raise PortalsServiceError("Portals вернул пустую сумму оффера.")
        if isinstance(value, float) and not math.isfinite(value):
            raise PortalsServiceError("Portals вернул некорректную сумму оффера.")

    @staticmethod
    def _same_value(left: Any, right: Any) -> bool:
        return type(left) is type(right) and left == right

    @staticmethod
    def _same_amount(left: Any, right: Any) -> bool:
        try:
            return Decimal(str(left)) == Decimal(str(right))
        except (InvalidOperation, ValueError):
            return False

    @staticmethod
    def _validate_launch(launch: MiniAppLaunchResult) -> None:
        if launch.bot_username != "@portals":
            raise PortalsAuthenticationError("Выбран неожиданный бот Portals.")
        if not launch.init_data_obtained or not launch.init_data:
            raise PortalsAuthenticationError(
                "Telegram не вернул данные запуска Portals."
            )
        if not launch.webview_url:
            raise PortalsAuthenticationError("Telegram не вернул окно Portals.")
        parsed = urlsplit(launch.webview_url)
        if parsed.scheme != "https" or parsed.hostname != "portal-market.com":
            raise PortalsAuthenticationError("Telegram вернул неожиданный URL Portals.")

    @staticmethod
    def _api_error(response: PortalsHttpResponse) -> PortalsApiError:
        code = None
        if isinstance(response.body, dict):
            for key in ("code", "errorCode", "detail", "title"):
                candidate = response.body.get(key)
                if isinstance(candidate, str) and _SAFE_CODE_RE.fullmatch(candidate):
                    code = candidate
                    break
        return PortalsApiError(response.status, code)

    @staticmethod
    def _field_names(value: Any) -> tuple[str, ...]:
        if not isinstance(value, dict):
            return ()
        return tuple(sorted(str(key) for key in value))

    @staticmethod
    def _safe_path(path: str) -> str:
        if _NFT_OFFERS_RE.fullmatch(path):
            return PORTALS_NFT_OFFERS_PATH
        if _ACCEPT_OFFER_RE.fullmatch(path):
            return PORTALS_ACCEPT_OFFER_PATH
        if _CANCEL_OFFER_RE.fullmatch(path):
            return PORTALS_CANCEL_OFFER_PATH
        if path == PORTALS_CREATE_OFFER_PATH:
            return PORTALS_CREATE_OFFER_PATH
        return path

    @staticmethod
    def _close_session(session: _PortalsSession) -> None:
        session.authorization = ""
        session.transport.close()

    async def shutdown(self) -> None:
        return None
