from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import logging
import re
import secrets
import threading
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from http.cookies import SimpleCookie
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from app.live_verify import gift_snapshot, mutation_started, response_metadata

from .miniapp_service import MiniAppLaunchResult, MiniAppService

logger = logging.getLogger(__name__)

MRKT_FRONTEND_ORIGIN = "https://cdn.tgmrkt.io"
MRKT_API_ORIGIN = "https://api.tgmrkt.io"
MRKT_AUTH_PATH = "/api/v1/auth"
MRKT_INVENTORY_PATH = "/api/v1/gifts"
MRKT_SALE_PATH = "/api/v1/gifts/sale"
MRKT_MARKET_PATH = "/api/v1/gifts/saling"
MRKT_MARKET_BY_IDS_PATH = "/api/v1/gifts/saling/by-ids"
MRKT_BUY_PATH = "/api/v1/gifts/buy"
MRKT_HTTP_TIMEOUT_SECONDS = 20
MRKT_SAFE_ATTEMPTS = 2
MRKT_MAX_INVENTORY_PAGES = 5
MRKT_CONFIRMATION_TTL_SECONDS = 5 * 60
MRKT_LISTED_TRIGGER_MAX_PROBES = 64
MRKT_LISTING_OBSERVATION_SCHEDULE_MS = (
    0,
    25,
    50,
    75,
    100,
    150,
    250,
    400,
    700,
    1_000,
)

_MRKT_HTTP_PATHS = frozenset(
    {
        MRKT_AUTH_PATH,
        MRKT_INVENTORY_PATH,
        MRKT_SALE_PATH,
        MRKT_MARKET_PATH,
        MRKT_MARKET_BY_IDS_PATH,
        MRKT_BUY_PATH,
    }
)
_PRICE_PATTERN = re.compile(r"^\d+(?:\.\d{1,2})?$")
_SAFE_ERROR_CODE_PATTERN = re.compile(r"^[A-Za-z0-9_. -]{1,64}$")
_NANOTONS_PER_TON = Decimal(1_000_000_000)
MRKT_BUYER_COMMISSION_PERCENT = 2
_MRKT_BUYER_PRICE_MULTIPLIER = Decimal(
    100 + MRKT_BUYER_COMMISSION_PERCENT
) / Decimal(100)
_DEFAULT_INVENTORY_FILTERS: dict[str, Any] = {
    "collectionNames": [],
    "modelNames": [],
    "backdropNames": [],
    "symbolNames": [],
    "minPrice": None,
    "maxPrice": None,
    "number": None,
    "isPremarket": None,
    "isNew": None,
    "luckyBuy": None,
    "giftType": None,
    "craftable": None,
    "isCrafted": None,
    "tgCanBeCraftedFrom": None,
    "removeSelfSales": None,
    "isTransferable": None,
    "availableForStaking": None,
    "forGame": None,
    "ordering": "None",
    "lowToHigh": False,
    "query": None,
}


class MrktServiceError(RuntimeError):
    """Base class for safe user-facing MRKT failures."""


class MrktPriceError(MrktServiceError):
    """The supplied TON price is not accepted by the MRKT UI contract."""


class MrktNetworkError(MrktServiceError):
    """A sanitized MRKT network failure."""


class MrktMutationNetworkError(MrktNetworkError):
    """A MRKT mutation may have reached the server."""


class MrktAuthenticationError(MrktServiceError):
    """MRKT did not accept the fresh Telegram Mini App launch data."""


class MrktApiError(MrktServiceError):
    def __init__(self, status: int, code: str | None = None) -> None:
        self.status = status
        self.code = code
        super().__init__(self.user_message)

    @property
    def user_message(self) -> str:
        messages = {
            400: "MRKT отклонил запрос. Проверьте ограничения цены и подарка.",
            401: "Авторизация MRKT истекла. Начните размещение заново.",
            403: "MRKT не разрешает размещение для выбранного аккаунта или подарка.",
            404: "Выбранный подарок больше не доступен в хранилище MRKT.",
            409: "Состояние подарка изменилось или он уже размещён на MRKT.",
            429: "MRKT ограничил частоту запросов. Подождите перед повторной попыткой.",
        }
        if self.status >= 500:
            return "MRKT временно недоступен. Запрос на продажу автоматически не повторялся."
        return messages.get(
            self.status,
            f"MRKT отклонил запрос с кодом HTTP {self.status}.",
        )


class MrktGiftUnavailableError(MrktServiceError):
    """The originally selected gift cannot safely be listed now."""


class MrktConfirmationError(MrktServiceError):
    """A listing confirmation is invalid, expired, or belongs to another user."""


class MrktConfirmationUsedError(MrktConfirmationError):
    """The confirmation token has already been consumed."""


class MrktListingInFlightError(MrktConfirmationError):
    """Another listing confirmation is already running for this account."""


class MrktListingStatus(StrEnum):
    SUCCESS = "success"
    DRY_RUN = "dry_run"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"


class MrktPurchaseStatus(StrEnum):
    SUCCESS = "success"
    DRY_RUN = "dry_run"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"


class MrktFastTransferStatus(StrEnum):
    SUCCESS = "success"
    DRY_RUN = "dry_run"
    LISTING_REJECTED = "listing_rejected"
    LISTING_AMBIGUOUS = "listing_ambiguous"
    BUY_REJECTED = "buy_rejected"
    BUY_AMBIGUOUS = "buy_ambiguous"
    EXTERNAL_SALE = "external_sale"
    SPECULATIVE_BUY_TOO_EARLY = "speculative_buy_too_early"
    LISTING_NOT_OBSERVED = "listing_not_observed"
    LISTING_RATE_LIMITED = "listing_rate_limited"
    VERIFICATION_AMBIGUOUS = "verification_ambiguous"


class MrktTransferMode(StrEnum):
    FAST_CONFIRMED = "FAST_CONFIRMED"
    SPECULATIVE = "SPECULATIVE"
    LISTED_TRIGGERED = "LISTED_TRIGGERED"


@dataclass(frozen=True, slots=True)
class MrktGift:
    display_name: str
    eligible: bool
    eligibility_reason: str | None
    sale_price_nanotons: int | None
    gift_id: Any = field(repr=False)
    seller_id: Any = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class MrktPurchaseResult:
    status: MrktPurchaseStatus
    display_name: str
    price_nanotons: int
    buy_request_sent: bool
    response_status: int | None
    response_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MrktFastTransferResult:
    status: MrktFastTransferStatus
    display_name: str
    price_nanotons: int
    buyer_price_nanotons: int
    commission_percent: int
    listing_request_sent: bool
    buy_request_sent: bool
    listing_response_status: int | None
    listing_response_fields: tuple[str, ...]
    buy_response_status: int | None
    buy_response_fields: tuple[str, ...]
    verification: dict[str, Any]
    timings: dict[str, float | None]
    listing_observation: dict[str, Any]
    listed_trigger: dict[str, Any]
    gift_id: Any = field(repr=False)


@dataclass(frozen=True, slots=True)
class MrktLatencyBenchmarkResult:
    sample_count: int
    auth_and_connection_ms: float
    samples_ms: tuple[float, ...]
    connection_reused: bool | None


@dataclass(frozen=True, slots=True)
class MrktListingConfirmation:
    token: str = field(repr=False)
    display_name: str
    price_ton: str
    expires_in_seconds: int


@dataclass(frozen=True, slots=True)
class MrktListingResult:
    status: MrktListingStatus
    display_name: str
    price_ton: str
    price_nanotons: int
    sale_request_sent: bool
    response_status: int | None
    response_fields: tuple[str, ...]
    message: str

    @property
    def successful(self) -> bool:
        return self.status in {MrktListingStatus.SUCCESS, MrktListingStatus.DRY_RUN}


@dataclass(frozen=True, slots=True)
class MrktHttpResponse:
    status: int
    body: Any = field(repr=False)


class MrktHttpTransport(Protocol):
    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> MrktHttpResponse: ...

    def close(self) -> None: ...


TransportFactory = Callable[[], MrktHttpTransport]


@dataclass(slots=True)
class _MrktAuthSession:
    transport: MrktHttpTransport = field(repr=False)
    token: str = field(repr=False)
    launch_fingerprint: str | None


@dataclass(frozen=True, slots=True)
class _MrktPresenceSnapshot:
    seller_unlisted_present: bool
    seller_listed_present: bool
    buyer_unlisted_present: bool
    buyer_listed_present: bool


@dataclass(slots=True)
class _MrktListedTriggerWatchResult:
    metadata: dict[str, Any]
    buy_task: asyncio.Task[
        tuple[MrktHttpResponse | None, Exception | None, int, int]
    ] | None
    listing_confirmed_ns: int | None


@dataclass(frozen=True, slots=True)
class _MrktExactListingCorrelation:
    exact_listing_present: bool
    exact_price_match: bool
    seller_available: bool
    seller_match: bool | None
    ambiguous: bool
    observed_listing_price_nanotons: int | None
    raw: Mapping[str, Any] | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class _ConfirmationRecord:
    owner_telegram_id: int
    account_id: int
    display_name: str
    price_ton: str
    price_nanotons: int
    expires_at: float
    gift_id: Any = field(repr=False)


class _KeepAliveMrktTransport:
    def __init__(self) -> None:
        self._connection = http.client.HTTPSConnection(
            "api.tgmrkt.io", timeout=MRKT_HTTP_TIMEOUT_SECONDS
        )
        self._cookies: dict[str, str] = {}
        self._lock = threading.Lock()
        self._successful_requests = 0
        self._connection_reused = False

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> MrktHttpResponse:
        if path not in _MRKT_HTTP_PATHS:
            raise MrktServiceError("Неподдерживаемый путь API MRKT.")

        request_body = json.dumps(payload, separators=(",", ":")).encode()
        return self.post_prepared_json(
            path,
            request_body,
            authorization=authorization,
        )

    def post_prepared_json(
        self,
        path: str,
        request_body: bytes,
        *,
        authorization: str | None = None,
    ) -> MrktHttpResponse:
        if path not in _MRKT_HTTP_PATHS:
            raise MrktServiceError("Неподдерживаемый путь API MRKT.")
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": MRKT_FRONTEND_ORIGIN,
            "Referer": f"{MRKT_FRONTEND_ORIGIN}/",
            "User-Agent": "Mozilla/5.0 MRKT-client/1.0",
        }
        if authorization:
            headers["Authorization"] = authorization
        try:
            with self._lock:
                if self._cookies:
                    headers["Cookie"] = "; ".join(
                        f"{name}={value}" for name, value in self._cookies.items()
                    )
                self._connection.request(
                    "POST", path, body=request_body, headers=headers
                )
                response = self._connection.getresponse()
                status = response.status
                raw_body = response.read()
                self._save_cookies(response.getheaders())
                self._connection_reused = self._successful_requests > 0
                self._successful_requests += 1
        except (http.client.HTTPException, TimeoutError, OSError) as exc:
            with self._lock:
                self._connection.close()
            error_type = type(exc).__name__
            if path in {MRKT_SALE_PATH, MRKT_BUY_PATH}:
                raise MrktMutationNetworkError(
                    f"MRKT mutation network failure: {error_type}"
                ) from None
            raise MrktNetworkError(f"MRKT network failure: {error_type}") from None

        try:
            response_body: Any = json.loads(raw_body) if raw_body else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            response_body = None
        return MrktHttpResponse(status=status, body=response_body)

    @property
    def connection_reused(self) -> bool:
        return self._connection_reused

    @property
    def successful_requests(self) -> int:
        return self._successful_requests

    def close(self) -> None:
        with self._lock:
            self._connection.close()
            self._cookies.clear()

    def _save_cookies(self, headers: list[tuple[str, str]]) -> None:
        for name, value in headers:
            if name.lower() != "set-cookie":
                continue
            parsed = SimpleCookie()
            try:
                parsed.load(value)
            except Exception as exc:  # noqa: BLE001 - never log cookie contents
                logger.debug(
                    "MRKT ignored malformed response cookie exception=%s",
                    type(exc).__name__,
                )
                continue
            for cookie_name, morsel in parsed.items():
                if morsel["max-age"] == "0":
                    self._cookies.pop(cookie_name, None)
                else:
                    self._cookies[cookie_name] = morsel.value


class MrktService:
    def __init__(
        self,
        miniapp_service: MiniAppService,
        *,
        dry_run: bool = True,
        transport_factory: TransportFactory | None = None,
        confirmation_ttl_seconds: int = MRKT_CONFIRMATION_TTL_SECONDS,
    ) -> None:
        self._miniapp_service = miniapp_service
        self._dry_run = dry_run
        self._transport_factory = transport_factory or _KeepAliveMrktTransport
        self._confirmation_ttl_seconds = confirmation_ttl_seconds
        self._confirmations: dict[str, _ConfirmationRecord] = {}
        self._spent_tokens: dict[str, float] = {}
        self._in_flight: set[tuple[int, int]] = set()
        self._confirmation_lock = asyncio.Lock()

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    async def execute_fast_transfer(
        self,
        *,
        owner_telegram_id: int,
        buyer_account_id: int,
        seller_account_id: int,
        gift_id: str | int | None,
        price_nanotons: int,
        dry_run: bool,
        speculative_buy: bool = False,
        speculative_buy_delay_ms: int = 40,
        observe_listing_activation: bool = False,
        transfer_mode: MrktTransferMode | str | None = None,
        listed_trigger_max_wait_ms: int = 1_000,
        listed_trigger_poll_interval_ms: int = 10,
    ) -> MrktFastTransferResult:
        """Prepare both accounts, then perform one exact list->buy attempt."""
        if (
            not isinstance(price_nanotons, int)
            or isinstance(price_nanotons, bool)
            or price_nanotons <= 0
        ):
            raise MrktPriceError("Цена покупки должна быть положительной.")
        seller_price_nanotons = price_nanotons
        buyer_price_nanotons = self.seller_to_buyer_price_nanotons(
            seller_price_nanotons
        )
        if not 0 <= speculative_buy_delay_ms <= 250:
            raise MrktServiceError(
                "Задержка экспериментальной покупки должна быть от 0 до 250 мс."
            )
        if not 100 <= listed_trigger_max_wait_ms <= 5_000:
            raise MrktServiceError(
                "Ожидание размещения должно быть от 100 до 5000 мс."
            )
        if not 1 <= listed_trigger_poll_interval_ms <= 250:
            raise MrktServiceError("Интервал проверки должен быть от 1 до 250 мс.")
        if listed_trigger_poll_interval_ms > listed_trigger_max_wait_ms:
            raise MrktServiceError(
                "Интервал проверки не может превышать время ожидания."
            )

        total_started = time.perf_counter_ns()
        try:
            mode = (
                MrktTransferMode(transfer_mode)
                if transfer_mode is not None
                else MrktTransferMode.SPECULATIVE
                if speculative_buy
                else MrktTransferMode.FAST_CONFIRMED
            )
        except ValueError as exc:
            raise MrktServiceError("Неподдерживаемый режим передачи MRKT.") from exc
        timings: dict[str, float | None] = {
            "mrkt_auth_n1_ms": None,
            "mrkt_auth_n2_ms": None,
            "mrkt_preparation_ms": None,
            "mrkt_listing_request_ms": None,
            "mrkt_listing_to_buy_start_ms": None,
            "mrkt_buy_request_ms": None,
            "mrkt_sale_start_to_buy_start_ms": None,
            "mrkt_sale_start_to_sale_response_ms": None,
            "mrkt_buy_start_to_buy_response_ms": None,
            "mrkt_sale_response_relative_to_buy_start_ms": None,
            "mrkt_final_verification_ms": None,
            "mrkt_total_job_ms": None,
            "mrkt_listed_trigger_sale_start_ms": None,
            "mrkt_listed_trigger_first_probe_start_ms": None,
            "mrkt_listed_trigger_listing_confirmed_ms": None,
            "mrkt_listed_trigger_buy_start_ms": None,
            "mrkt_listed_trigger_confirm_to_buy_start_ms": None,
            "mrkt_listed_trigger_probe_count": None,
            "mrkt_listed_trigger_sale_response_ms": None,
        }
        buyer_session: _MrktAuthSession | None = None
        seller_session: _MrktAuthSession | None = None
        listing_payload: dict[str, Any] = {}
        buy_payload: dict[str, Any] = {}
        listing_prepared = b""
        buy_prepared = b""
        connection_diagnostics: dict[str, Any] = {
            "n1_prepared_before_sale": None,
            "n2_prepared_before_sale": None,
            "n1_reused_for_buy": None,
            "n2_reused_for_sale": None,
        }
        listing_observation = self._empty_listing_observation(
            observe_listing_activation
            and mode is MrktTransferMode.SPECULATIVE
            and not dry_run
        )
        observer_buyer_session: _MrktAuthSession | None = None
        observer_seller_session: _MrktAuthSession | None = None
        observer_task: asyncio.Task[tuple[dict[str, Any], int]] | None = None
        listed_lookup_session: _MrktAuthSession | None = None
        listed_trigger = self._empty_listed_trigger(
            mode is MrktTransferMode.LISTED_TRIGGERED and not dry_run,
            max_wait_ms=listed_trigger_max_wait_ms,
            poll_interval_ms=listed_trigger_poll_interval_ms,
        )

        async def authenticate(
            account_id: int, timing_key: str
        ) -> _MrktAuthSession:
            started = time.perf_counter_ns()
            try:
                return await self._authenticate(account_id, owner_telegram_id)
            finally:
                timings[timing_key] = self._elapsed_ms(started)

        async def build_result(
            status: MrktFastTransferStatus,
            *,
            selected_id: Any,
            display_name: str,
            listing_sent: bool,
            buy_sent: bool,
            listing_response: MrktHttpResponse | None = None,
            buy_response: MrktHttpResponse | None = None,
            verification: dict[str, Any] | None = None,
        ) -> MrktFastTransferResult:
            timings["mrkt_total_job_ms"] = self._elapsed_ms(total_started)
            logger.info(
                "MRKT transfer timings mode=%s n1_auth_ms=%s n2_auth_ms=%s "
                "preparation_ms=%s listing_request_ms=%s "
                "listing_to_buy_start_ms=%s sale_to_buy_start_ms=%s "
                "sale_response_ms=%s buy_response_ms=%s "
                "sale_response_relative_to_buy_start_ms=%s "
                "final_verification_ms=%s total_ms=%s",
                mode.value,
                timings["mrkt_auth_n1_ms"],
                timings["mrkt_auth_n2_ms"],
                timings["mrkt_preparation_ms"],
                timings["mrkt_listing_request_ms"],
                timings["mrkt_listing_to_buy_start_ms"],
                timings["mrkt_sale_start_to_buy_start_ms"],
                timings["mrkt_sale_start_to_sale_response_ms"],
                timings["mrkt_buy_start_to_buy_response_ms"],
                timings["mrkt_sale_response_relative_to_buy_start_ms"],
                timings["mrkt_final_verification_ms"],
                timings["mrkt_total_job_ms"],
            )
            safe_verification = dict(verification or {"outcome": "not_run"})
            safe_verification["transfer_mode"] = mode.value
            safe_verification["connection_reuse"] = dict(connection_diagnostics)
            return MrktFastTransferResult(
                status=status,
                display_name=display_name,
                gift_id=selected_id,
                price_nanotons=seller_price_nanotons,
                buyer_price_nanotons=buyer_price_nanotons,
                commission_percent=MRKT_BUYER_COMMISSION_PERCENT,
                listing_request_sent=listing_sent,
                buy_request_sent=buy_sent,
                listing_response_status=(
                    listing_response.status if listing_response is not None else None
                ),
                listing_response_fields=(
                    self._field_names(listing_response.body)
                    if listing_response is not None
                    else ()
                ),
                buy_response_status=(
                    buy_response.status if buy_response is not None else None
                ),
                buy_response_fields=(
                    self._field_names(buy_response.body)
                    if buy_response is not None
                    else ()
                ),
                verification=safe_verification,
                timings=dict(timings),
                listing_observation=dict(listing_observation),
                listed_trigger=dict(listed_trigger),
            )

        async def send_mutation(
            session: _MrktAuthSession,
            path: str,
            payload: Mapping[str, Any],
            prepared: bytes,
        ) -> tuple[MrktHttpResponse | None, Exception | None, int]:
            try:
                response = await asyncio.to_thread(
                    self._post_prepared_mutation,
                    session,
                    path,
                    payload,
                    prepared,
                )
                return response, None, time.perf_counter_ns()
            except Exception as exc:  # noqa: BLE001 - never retry a mutation
                return None, exc, time.perf_counter_ns()

        def start_prepared_buy_immediately(
            session: _MrktAuthSession,
            payload: Mapping[str, Any],
            prepared: bytes,
        ) -> tuple[
            asyncio.Task[
                tuple[MrktHttpResponse | None, Exception | None, int, int]
            ],
            int,
        ]:
            # Deliberately contains no await, logging, callback, or formatting.
            # run_in_executor submits the prepared HTTP operation immediately.
            mutation_started("mrkt_buy", emit_event=False)
            started_ns = time.perf_counter_ns()
            future = asyncio.get_running_loop().run_in_executor(
                None,
                self._post_prepared_mutation,
                session,
                MRKT_BUY_PATH,
                payload,
                prepared,
            )

            async def collect() -> tuple[
                MrktHttpResponse | None, Exception | None, int, int
            ]:
                try:
                    response = await future
                    return response, None, time.perf_counter_ns(), started_ns
                except Exception as exc:  # noqa: BLE001 - never retry a mutation
                    return None, exc, time.perf_counter_ns(), started_ns

            return asyncio.create_task(collect()), started_ns

        try:
            auth_results = await asyncio.gather(
                authenticate(buyer_account_id, "mrkt_auth_n1_ms"),
                authenticate(seller_account_id, "mrkt_auth_n2_ms"),
                return_exceptions=True,
            )
            auth_error = next(
                (item for item in auth_results if isinstance(item, BaseException)),
                None,
            )
            for item in auth_results:
                if isinstance(item, _MrktAuthSession):
                    if buyer_session is None:
                        buyer_session = item
                    else:
                        seller_session = item
            if auth_error is not None:
                raise auth_error
            if buyer_session is None or seller_session is None:
                raise MrktAuthenticationError(
                    "Не удалось подготовить оба аккаунта MRKT."
                )
            if mode is MrktTransferMode.LISTED_TRIGGERED and not dry_run:
                # This N1 context is authenticated and warmed before SALE. It is
                # read-only and owns a distinct serialized HTTP transport, so its
                # exact-listing probes cannot block N2 SALE or N1 BUY.
                listed_lookup_session = await self._authenticate(
                    buyer_account_id, owner_telegram_id
                )

            seller_gifts = await self._fetch_inventory(
                seller_session,
                is_listed=False,
                verification_account_id=seller_account_id,
            )
            eligible = [gift for gift in seller_gifts if gift.eligible]
            if gift_id is None:
                if not eligible:
                    raise MrktGiftUnavailableError(
                        "В рабочем аккаунте нет доступных подарков."
                    )
                if len(eligible) != 1:
                    raise MrktGiftUnavailableError(
                        "Найдено несколько подарков. Выберите один явно."
                    )
                selected = eligible[0]
            else:
                matches = [
                    gift
                    for gift in eligible
                    if self._same_gift_id(gift.gift_id, gift_id)
                ]
                if len(matches) != 1:
                    raise MrktGiftUnavailableError(
                        "Выбранный подарок больше не доступен для размещения."
                    )
                selected = matches[0]

            selected_id = selected.gift_id
            self._validate_gift_id(selected_id)
            buy_id = str(selected_id)
            listing_payload = {
                "ids": [selected_id],
                "price": seller_price_nanotons,
            }
            buy_payload = {
                "ids": [buy_id],
                "prices": {buy_id: buyer_price_nanotons},
            }
            listing_prepared = json.dumps(
                listing_payload, separators=(",", ":")
            ).encode()
            buy_prepared = json.dumps(buy_payload, separators=(",", ":")).encode()
            connection_diagnostics["n1_prepared_before_sale"] = (
                self._connection_prepared(buyer_session)
            )
            connection_diagnostics["n2_prepared_before_sale"] = (
                self._connection_prepared(seller_session)
            )
            if listing_observation["enabled"] is True:
                (
                    observer_buyer_session,
                    observer_seller_session,
                ) = await self._prepare_listing_observer_sessions(
                    owner_telegram_id=owner_telegram_id,
                    buyer_account_id=buyer_account_id,
                    seller_account_id=seller_account_id,
                )
                if (
                    observer_buyer_session is None
                    or observer_seller_session is None
                ):
                    listing_observation["observation_stopped_reason"] = "READ_ERROR"
            timings["mrkt_preparation_ms"] = self._elapsed_ms(total_started)

            if dry_run:
                return await build_result(
                    MrktFastTransferStatus.DRY_RUN,
                    selected_id=selected_id,
                    display_name=selected.display_name,
                    listing_sent=False,
                    buy_sent=False,
                )

            mutation_started("mrkt_listing")
            listing_started = time.perf_counter_ns()
            listing_task = asyncio.create_task(
                send_mutation(
                    seller_session,
                    MRKT_SALE_PATH,
                    listing_payload,
                    listing_prepared,
                )
            )
            listing_response: MrktHttpResponse | None = None
            listing_error: Exception | None = None
            listing_received: int | None = None

            if mode is MrktTransferMode.LISTED_TRIGGERED:
                assert listed_lookup_session is not None
                timings["mrkt_listed_trigger_sale_start_ms"] = 0.0
                trigger_watch = await self._watch_for_exact_listing(
                    lookup_session=listed_lookup_session,
                    listing_task=listing_task,
                    gift_id=selected_id,
                    expected_seller_price_nanotons=seller_price_nanotons,
                    expected_buyer_price_nanotons=buyer_price_nanotons,
                    expected_seller_id=selected.seller_id,
                    sale_started_ns=listing_started,
                    max_wait_ms=listed_trigger_max_wait_ms,
                    poll_interval_ms=listed_trigger_poll_interval_ms,
                    start_buy=lambda: start_prepared_buy_immediately(
                        buyer_session, buy_payload, buy_prepared
                    ),
                )
                listed_trigger.update(trigger_watch.metadata)
                listing_response, listing_error, listing_received = await listing_task
                self._record_sale_timings(timings, listing_started, listing_received)
                timings["mrkt_listed_trigger_sale_response_ms"] = timings[
                    "mrkt_sale_start_to_sale_response_ms"
                ]
                listed_trigger["sale_response_ms"] = timings[
                    "mrkt_listed_trigger_sale_response_ms"
                ]
                connection_diagnostics["n2_reused_for_sale"] = (
                    self._connection_reused(seller_session)
                )
                for source, target in (
                    ("first_probe_start_ms", "mrkt_listed_trigger_first_probe_start_ms"),
                    ("listing_confirmed_ms", "mrkt_listed_trigger_listing_confirmed_ms"),
                    ("buy_started_ms", "mrkt_listed_trigger_buy_start_ms"),
                    (
                        "confirm_to_buy_start_ms",
                        "mrkt_listed_trigger_confirm_to_buy_start_ms",
                    ),
                    ("probe_count", "mrkt_listed_trigger_probe_count"),
                ):
                    value = listed_trigger.get(source)
                    timings[target] = value if isinstance(value, (int, float)) else None

                if trigger_watch.buy_task is None:
                    verification_started = time.perf_counter_ns()
                    verification = await self._verify_fast_transfer_ownership(
                        buyer_session,
                        seller_session,
                        buyer_account_id,
                        seller_account_id,
                        selected_id,
                    )
                    timings["mrkt_final_verification_ms"] = self._elapsed_ms(
                        verification_started
                    )
                    raw_stopped_reason = listed_trigger.get("stopped_reason")
                    stopped_reason = (
                        raw_stopped_reason
                        if isinstance(raw_stopped_reason, str)
                        else "AMBIGUOUS"
                    )
                    if (
                        listing_error is not None
                        or listing_response is None
                        or listing_response.status >= 500
                    ):
                        status = MrktFastTransferStatus.LISTING_AMBIGUOUS
                    elif 400 <= listing_response.status < 500:
                        status = MrktFastTransferStatus.LISTING_REJECTED
                    else:
                        status = {
                            "SALE_FAILED": MrktFastTransferStatus.LISTING_REJECTED,
                            "TIMEOUT": MrktFastTransferStatus.LISTING_NOT_OBSERVED,
                            "RATE_LIMITED": MrktFastTransferStatus.LISTING_RATE_LIMITED,
                        }.get(
                            stopped_reason,
                            MrktFastTransferStatus.LISTING_AMBIGUOUS,
                        )
                    return await build_result(
                        status,
                        selected_id=selected_id,
                        display_name=selected.display_name,
                        listing_sent=True,
                        buy_sent=False,
                        listing_response=listing_response,
                        verification=verification,
                    )

                (
                    trigger_buy_response,
                    trigger_buy_error,
                    trigger_buy_received,
                    trigger_buy_started,
                ) = await trigger_watch.buy_task
                timings["mrkt_sale_start_to_buy_start_ms"] = round(
                    (trigger_buy_started - listing_started) / 1_000_000, 3
                )
                timings["mrkt_listing_to_buy_start_ms"] = round(
                    (trigger_buy_started - listing_received) / 1_000_000, 3
                )
                timings["mrkt_sale_response_relative_to_buy_start_ms"] = round(
                    (listing_received - trigger_buy_started) / 1_000_000, 3
                )
                timings["mrkt_buy_start_to_buy_response_ms"] = round(
                    (trigger_buy_received - trigger_buy_started) / 1_000_000, 3
                )
                timings["mrkt_buy_request_ms"] = timings[
                    "mrkt_buy_start_to_buy_response_ms"
                ]
                connection_diagnostics["n1_reused_for_buy"] = (
                    self._connection_reused(buyer_session)
                )
                if listing_response is not None:
                    response_metadata(
                        "mrkt_listing",
                        listing_response.status,
                        listing_response.body,
                    )
                if trigger_buy_response is not None:
                    response_metadata(
                        "mrkt_buy",
                        trigger_buy_response.status,
                        trigger_buy_response.body,
                    )
                if listing_error is not None:
                    logger.warning(
                        "MRKT listed-trigger sale response ambiguous exception=%s",
                        type(listing_error).__name__,
                    )
                if trigger_buy_error is not None:
                    logger.warning(
                        "MRKT listed-trigger buy response ambiguous exception=%s",
                        type(trigger_buy_error).__name__,
                    )
                verification_started = time.perf_counter_ns()
                verification = await self._verify_fast_transfer_ownership(
                    buyer_session,
                    seller_session,
                    buyer_account_id,
                    seller_account_id,
                    selected_id,
                )
                timings["mrkt_final_verification_ms"] = self._elapsed_ms(
                    verification_started
                )
                status = self._classify_completed_fast_transfer(
                    mode=mode,
                    listing_response=listing_response,
                    listing_error=listing_error,
                    buy_response=trigger_buy_response,
                    buy_error=trigger_buy_error,
                    gift_id=selected_id,
                    verification=verification,
                )
                return await build_result(
                    status,
                    selected_id=selected_id,
                    display_name=selected.display_name,
                    listing_sent=True,
                    buy_sent=True,
                    listing_response=listing_response,
                    buy_response=trigger_buy_response,
                    verification=verification,
                )

            if mode is MrktTransferMode.SPECULATIVE:
                delay_task = asyncio.create_task(
                    asyncio.sleep(speculative_buy_delay_ms / 1000)
                )
                await asyncio.wait(
                    {listing_task, delay_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if listing_task.done():
                    listing_response, listing_error, listing_received = (
                        await listing_task
                    )
                    self._record_sale_timings(
                        timings, listing_started, listing_received
                    )
                    if listing_error is not None or not self._listing_confirms_gift(
                        listing_response, selected_id
                    ):
                        connection_diagnostics["n2_reused_for_sale"] = (
                            self._connection_reused(seller_session)
                        )
                        if listing_error is not None:
                            logger.warning(
                                "MRKT speculative listing response ambiguous exception=%s",
                                type(listing_error).__name__,
                            )
                        elif listing_response is not None:
                            response_metadata(
                                "mrkt_listing",
                                listing_response.status,
                                listing_response.body,
                            )
                        verification_started = time.perf_counter_ns()
                        verification = await self._seller_reconciliation(
                            seller_session, seller_account_id, selected_id
                        )
                        timings["mrkt_final_verification_ms"] = self._elapsed_ms(
                            verification_started
                        )
                        status = (
                            MrktFastTransferStatus.LISTING_REJECTED
                            if listing_error is None
                            and listing_response is not None
                            and listing_response.status < 500
                            else MrktFastTransferStatus.LISTING_AMBIGUOUS
                        )
                        delay_task.cancel()
                        await asyncio.gather(delay_task, return_exceptions=True)
                        return await build_result(
                            status,
                            selected_id=selected_id,
                            display_name=selected.display_name,
                            listing_sent=True,
                            buy_sent=False,
                            listing_response=listing_response,
                            verification=verification,
                        )
                    await delay_task
                else:
                    await delay_task
            else:
                listing_response, listing_error, listing_received = await listing_task
                self._record_sale_timings(timings, listing_started, listing_received)
                connection_diagnostics["n2_reused_for_sale"] = (
                    self._connection_reused(seller_session)
                )
                if listing_error is not None or not self._listing_confirms_gift(
                    listing_response, selected_id
                ):
                    if listing_error is not None:
                        logger.warning(
                            "MRKT fast listing response ambiguous exception=%s",
                            type(listing_error).__name__,
                        )
                    elif listing_response is not None:
                        response_metadata(
                            "mrkt_listing",
                            listing_response.status,
                            listing_response.body,
                        )
                    verification_started = time.perf_counter_ns()
                    verification = await self._seller_reconciliation(
                        seller_session, seller_account_id, selected_id
                    )
                    timings["mrkt_final_verification_ms"] = self._elapsed_ms(
                        verification_started
                    )
                    status = (
                        MrktFastTransferStatus.LISTING_REJECTED
                        if listing_error is None
                        and listing_response is not None
                        and listing_response.status < 500
                        else MrktFastTransferStatus.LISTING_AMBIGUOUS
                    )
                    return await build_result(
                        status,
                        selected_id=selected_id,
                        display_name=selected.display_name,
                        listing_sent=True,
                        buy_sent=False,
                        listing_response=listing_response,
                        verification=verification,
                    )

            # The critical interval contains no reads, database writes, WebView
            # launches, or diagnostic formatting. Both payload bytes already exist.
            mutation_started("mrkt_buy", emit_event=False)
            buy_started = time.perf_counter_ns()
            timings["mrkt_sale_start_to_buy_start_ms"] = round(
                (buy_started - listing_started) / 1_000_000, 3
            )
            if listing_received is not None:
                timings["mrkt_listing_to_buy_start_ms"] = round(
                    (buy_started - listing_received) / 1_000_000, 3
                )
                timings["mrkt_sale_response_relative_to_buy_start_ms"] = round(
                    (listing_received - buy_started) / 1_000_000, 3
                )
            buy_task = asyncio.create_task(
                send_mutation(
                    buyer_session,
                    MRKT_BUY_PATH,
                    buy_payload,
                    buy_prepared,
                )
            )
            buy_response = None
            buy_error = None
            buy_received = None
            # In diagnostic SPECULATIVE mode, consume the one BUY result first so
            # an unsuccessful response can start a read-only observer while the
            # original SALE task is still in flight. The observer uses distinct
            # pre-authenticated transports and therefore cannot acquire either
            # mutation transport's serialized lock.
            if (
                mode is MrktTransferMode.SPECULATIVE
                and listing_observation["enabled"] is True
            ):
                buy_response, buy_error, buy_received = await buy_task
                if (
                    buy_error is None
                    and buy_response is not None
                    and 200 <= buy_response.status < 300
                    and self._purchase_confirms_gift(
                        buy_response.body,
                        selected_id,
                    )
                ):
                    listing_observation["observation_stopped_reason"] = "BUYER_OWNS"
                elif (
                    observer_buyer_session is not None
                    and observer_seller_session is not None
                ):
                    observer_started_ns = time.perf_counter_ns()
                    observer_task = asyncio.create_task(
                        self._observe_listing_activation(
                            buyer_session=observer_buyer_session,
                            seller_session=observer_seller_session,
                            buyer_account_id=buyer_account_id,
                            seller_account_id=seller_account_id,
                            gift_id=selected_id,
                            sale_started_ns=listing_started,
                            observation_started_ns=observer_started_ns,
                        )
                    )
            if listing_received is None:
                listing_response, listing_error, listing_received = await listing_task
                self._record_sale_timings(timings, listing_started, listing_received)
                timings["mrkt_listing_to_buy_start_ms"] = round(
                    (buy_started - listing_received) / 1_000_000, 3
                )
                timings["mrkt_sale_response_relative_to_buy_start_ms"] = round(
                    (listing_received - buy_started) / 1_000_000, 3
                )
            if buy_received is None:
                buy_response, buy_error, buy_received = await buy_task
            assert buy_received is not None
            timings["mrkt_buy_start_to_buy_response_ms"] = round(
                (buy_received - buy_started) / 1_000_000, 3
            )
            timings["mrkt_buy_request_ms"] = timings[
                "mrkt_buy_start_to_buy_response_ms"
            ]
            connection_diagnostics["n1_reused_for_buy"] = self._connection_reused(
                buyer_session
            )
            connection_diagnostics["n2_reused_for_sale"] = self._connection_reused(
                seller_session
            )
            if observer_task is not None:
                try:
                    observed, observer_started_ns = await observer_task
                    listing_observation.update(observed)
                    listing_observation["observation_started_before_sale_response"] = (
                        observer_started_ns < listing_received
                    )
                except Exception as exc:  # noqa: BLE001 - diagnostics are read-only
                    logger.warning(
                        "MRKT listing observer failed exception=%s",
                        type(exc).__name__,
                    )
                    listing_observation["observation_stopped_reason"] = "READ_ERROR"

            # Diagnostics deliberately happen after the buy attempt, never inside
            # the public listing-to-buy race window.
            if listing_response is not None:
                response_metadata(
                    "mrkt_listing", listing_response.status, listing_response.body
                )
            if buy_response is not None:
                response_metadata("mrkt_buy", buy_response.status, buy_response.body)
            if listing_error is not None:
                logger.warning(
                    "MRKT listing response ambiguous after speculative buy exception=%s",
                    type(listing_error).__name__,
                )
            if buy_error is not None:
                logger.warning(
                    "MRKT fast buy response ambiguous exception=%s",
                    type(buy_error).__name__,
                )

            verification_started = time.perf_counter_ns()
            verification = await self._verify_fast_transfer_ownership(
                buyer_session,
                seller_session,
                buyer_account_id,
                seller_account_id,
                selected_id,
            )
            timings["mrkt_final_verification_ms"] = self._elapsed_ms(
                verification_started
            )

            status = self._classify_completed_fast_transfer(
                mode=mode,
                listing_response=listing_response,
                listing_error=listing_error,
                buy_response=buy_response,
                buy_error=buy_error,
                gift_id=selected_id,
                verification=verification,
            )

            return await build_result(
                status,
                selected_id=selected_id,
                display_name=selected.display_name,
                listing_sent=True,
                buy_sent=True,
                listing_response=listing_response,
                buy_response=buy_response,
                verification=verification,
            )
        finally:
            if observer_task is not None and not observer_task.done():
                observer_task.cancel()
                await asyncio.gather(observer_task, return_exceptions=True)
            listing_payload.clear()
            prices = buy_payload.get("prices")
            if isinstance(prices, dict):
                prices.clear()
            buy_payload.clear()
            listing_prepared = b""
            buy_prepared = b""
            for session in (buyer_session, seller_session):
                if session is not None:
                    session.token = ""
                    session.transport.close()
            for session in (observer_buyer_session, observer_seller_session):
                if session is not None:
                    session.token = ""
                    session.transport.close()
            if listed_lookup_session is not None:
                listed_lookup_session.token = ""
                listed_lookup_session.transport.close()

    @staticmethod
    def _post_prepared_mutation(
        session: _MrktAuthSession,
        path: str,
        payload: Mapping[str, Any],
        prepared: bytes,
    ) -> MrktHttpResponse:
        prepared_post = getattr(session.transport, "post_prepared_json", None)
        if callable(prepared_post):
            return prepared_post(path, prepared, authorization=session.token)
        return session.transport.post_json(
            path,
            payload,
            authorization=session.token,
        )

    @staticmethod
    def _connection_prepared(session: _MrktAuthSession) -> bool | None:
        count = getattr(session.transport, "successful_requests", None)
        return count > 0 if isinstance(count, int) else None

    @staticmethod
    def _connection_reused(session: _MrktAuthSession) -> bool | None:
        value = getattr(session.transport, "connection_reused", None)
        return value if isinstance(value, bool) else None

    @staticmethod
    def _record_sale_timings(
        timings: dict[str, float | None],
        started_ns: int,
        received_ns: int,
    ) -> None:
        duration = round((received_ns - started_ns) / 1_000_000, 3)
        timings["mrkt_listing_request_ms"] = duration
        timings["mrkt_sale_start_to_sale_response_ms"] = duration

    @classmethod
    def _listing_confirms_gift(
        cls,
        response: MrktHttpResponse | None,
        gift_id: Any,
    ) -> bool:
        if response is None or not 200 <= response.status < 300:
            return False
        response_ids = (
            response.body.get("ids") if isinstance(response.body, dict) else None
        )
        return isinstance(response_ids, list) and any(
            cls._same_gift_id(item, gift_id) for item in response_ids
        )

    async def benchmark_read_latency(
        self,
        account_id: int,
        *,
        owner_telegram_id: int,
        samples: int = 20,
        spacing_ms: int = 250,
    ) -> MrktLatencyBenchmarkResult:
        """Measure authenticated inventory RTTs without any marketplace mutation."""
        if not 1 <= samples <= 50:
            raise MrktServiceError("Число замеров должно быть от 1 до 50.")
        if not 100 <= spacing_ms <= 5_000:
            raise MrktServiceError("Пауза между замерами должна быть от 100 до 5000 мс.")
        auth_started = time.perf_counter_ns()
        session = await self._authenticate(account_id, owner_telegram_id)
        auth_ms = self._elapsed_ms(auth_started)
        values: list[float] = []
        payload = {
            "isListed": False,
            "count": 1,
            "cursor": "",
            **_DEFAULT_INVENTORY_FILTERS,
        }
        try:
            for index in range(samples):
                started = time.perf_counter_ns()
                response = await self._safe_post(
                    session.transport,
                    MRKT_INVENTORY_PATH,
                    payload,
                    authorization=session.token,
                )
                values.append(self._elapsed_ms(started))
                if not 200 <= response.status < 300:
                    raise self._api_error(response)
                if index + 1 < samples:
                    await asyncio.sleep(spacing_ms / 1000)
            return MrktLatencyBenchmarkResult(
                sample_count=len(values),
                auth_and_connection_ms=auth_ms,
                samples_ms=tuple(values),
                connection_reused=self._connection_reused(session),
            )
        finally:
            payload.clear()
            session.token = ""
            session.transport.close()

    @staticmethod
    def ton_to_nanotons(price_ton: str | Decimal) -> int:
        raw = format(price_ton, "f") if isinstance(price_ton, Decimal) else price_ton
        raw = raw.strip()
        if not _PRICE_PATTERN.fullmatch(raw):
            raise MrktPriceError(
                "Введите положительную сумму в TON, не более двух знаков после точки."
            )
        try:
            price = Decimal(raw)
        except InvalidOperation as exc:
            raise MrktPriceError("Некорректная цена в TON.") from exc
        if not price.is_finite() or price <= 0:
            raise MrktPriceError("Цена в TON должна быть больше нуля.")
        nanotons = price * _NANOTONS_PER_TON
        return int(nanotons)

    @staticmethod
    def seller_to_buyer_price_nanotons(seller_price_nanotons: int) -> int:
        """Apply MRKT's fixed buyer commission without float arithmetic."""
        if (
            not isinstance(seller_price_nanotons, int)
            or isinstance(seller_price_nanotons, bool)
            or seller_price_nanotons <= 0
        ):
            raise MrktPriceError("Цена размещения должна быть положительной.")
        buyer_price = Decimal(seller_price_nanotons) * _MRKT_BUYER_PRICE_MULTIPLIER
        integral_price = buyer_price.to_integral_value()
        if buyer_price != integral_price:
            raise MrktPriceError(
                "Цена размещения не позволяет точно рассчитать комиссию MRKT."
            )
        return int(integral_price)

    @staticmethod
    def format_ton(price_ton: str | Decimal) -> str:
        raw = format(price_ton, "f") if isinstance(price_ton, Decimal) else price_ton
        price = Decimal(raw)
        formatted = format(price, "f")
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
        return formatted

    async def get_inventory(
        self,
        account_id: int,
        *,
        owner_telegram_id: int,
        is_listed: bool = False,
    ) -> list[MrktGift]:
        session = await self._authenticate(account_id, owner_telegram_id)
        try:
            return await self._fetch_inventory(
                session, is_listed=is_listed, verification_account_id=account_id
            )
        finally:
            session.token = ""
            session.transport.close()

    async def get_marketplace_gift_by_id(
        self,
        account_id: int,
        *,
        owner_telegram_id: int,
        gift_id: Any,
    ) -> MrktGift | None:
        self._validate_gift_id(gift_id)
        session = await self._authenticate(account_id, owner_telegram_id)
        payload = {"ids": [gift_id]}
        try:
            response = await self._safe_post(
                session.transport,
                MRKT_MARKET_BY_IDS_PATH,
                payload,
                authorization=session.token,
            )
            response_metadata("mrkt_market_by_ids", response.status, response.body)
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            raw_items = self._market_items(response.body)
            matches = [
                self._parse_market_gift(raw)
                for raw in raw_items
                if isinstance(raw, dict) and self._same_gift_id(raw.get("id"), gift_id)
            ]
            return matches[0] if len(matches) == 1 else None
        finally:
            payload.clear()
            session.token = ""
            session.transport.close()

    async def buy_listing(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        gift_id: Any,
        expected_price_nanotons: int,
        expected_seller_id: Any = None,
        dry_run: bool = True,
    ) -> MrktPurchaseResult:
        """Buy one exactly revalidated listing without retrying the mutation."""
        self._validate_gift_id(gift_id)
        if (
            not isinstance(expected_price_nanotons, int)
            or isinstance(expected_price_nanotons, bool)
            or expected_price_nanotons <= 0
        ):
            raise MrktPriceError("Цена покупки должна быть положительной.")
        session = await self._authenticate(account_id, owner_telegram_id)
        lookup_payload = {"ids": [gift_id]}
        buy_id = str(gift_id)
        buy_payload: dict[str, Any] = {
            "ids": [buy_id],
            "prices": {buy_id: expected_price_nanotons},
        }
        try:
            response = await self._safe_post(
                session.transport,
                MRKT_MARKET_BY_IDS_PATH,
                lookup_payload,
                authorization=session.token,
            )
            response_metadata(
                "mrkt_market_by_ids_before_buy", response.status, response.body
            )
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            raw_items = self._market_items(response.body)
            exact = [
                self._parse_market_gift(raw)
                for raw in raw_items
                if isinstance(raw, dict) and self._same_gift_id(raw.get("id"), gift_id)
            ]
            if len(exact) != 1:
                raise MrktGiftUnavailableError(
                    "Не удалось однозначно найти созданное размещение."
                )
            listing = exact[0]
            if not listing.eligible:
                raise MrktGiftUnavailableError("Размещение недоступно для покупки.")
            if listing.sale_price_nanotons != expected_price_nanotons:
                raise MrktGiftUnavailableError("Цена размещения изменилась.")
            if expected_seller_id is not None and not self._same_gift_id(
                listing.seller_id, expected_seller_id
            ):
                raise MrktGiftUnavailableError("Продавец размещения не совпадает.")
            if dry_run:
                logger.info(
                    "MRKT transfer dry run account_db_id=%d path=%s "
                    "request_fields=ids,prices price_nanotons=%d",
                    account_id,
                    MRKT_BUY_PATH,
                    expected_price_nanotons,
                )
                return MrktPurchaseResult(
                    status=MrktPurchaseStatus.DRY_RUN,
                    display_name=listing.display_name,
                    price_nanotons=expected_price_nanotons,
                    buy_request_sent=False,
                    response_status=None,
                    response_fields=(),
                )

            logger.info(
                "MRKT purchase started account_db_id=%d path=%s "
                "request_fields=ids,prices price_nanotons=%d",
                account_id,
                MRKT_BUY_PATH,
                expected_price_nanotons,
            )
            try:
                mutation_started("mrkt_buy")
                response = await asyncio.to_thread(
                    session.transport.post_json,
                    MRKT_BUY_PATH,
                    buy_payload,
                    authorization=session.token,
                )
            except Exception as exc:  # noqa: BLE001 - delivery after POST is uncertain
                logger.warning(
                    "MRKT purchase delivery uncertain exception=%s", type(exc).__name__
                )
                raise MrktMutationNetworkError(
                    "Результат покупки требует ручной проверки."
                ) from None
            response_metadata("mrkt_buy", response.status, response.body)
            response_fields = self._field_names(response.body)
            if response.status >= 500:
                raise MrktMutationNetworkError(
                    "Результат покупки требует ручной проверки."
                )
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            if not self._purchase_confirms_gift(response.body, gift_id):
                return MrktPurchaseResult(
                    status=MrktPurchaseStatus.AMBIGUOUS,
                    display_name=listing.display_name,
                    price_nanotons=expected_price_nanotons,
                    buy_request_sent=True,
                    response_status=response.status,
                    response_fields=response_fields,
                )
            return MrktPurchaseResult(
                status=MrktPurchaseStatus.SUCCESS,
                display_name=listing.display_name,
                price_nanotons=expected_price_nanotons,
                buy_request_sent=True,
                response_status=response.status,
                response_fields=response_fields,
            )
        finally:
            lookup_payload.clear()
            prices = buy_payload.get("prices")
            if isinstance(prices, dict):
                prices.clear()
            buy_payload.clear()
            session.token = ""
            session.transport.close()

    async def prepare_confirmation(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        gift_id: Any,
        display_name: str,
        price_ton: str | Decimal,
    ) -> MrktListingConfirmation:
        price_nanotons = self.ton_to_nanotons(price_ton)
        formatted_price = self.format_ton(price_ton)
        token = secrets.token_urlsafe(18)
        digest = self._token_digest(token)
        now = time.monotonic()
        record = _ConfirmationRecord(
            owner_telegram_id=owner_telegram_id,
            account_id=account_id,
            gift_id=gift_id,
            display_name=self._safe_display_name(display_name),
            price_ton=formatted_price,
            price_nanotons=price_nanotons,
            expires_at=now + self._confirmation_ttl_seconds,
        )
        async with self._confirmation_lock:
            self._purge_tokens(now)
            for existing_digest, existing in list(self._confirmations.items()):
                if existing.owner_telegram_id == owner_telegram_id:
                    self._confirmations.pop(existing_digest, None)
                    self._spent_tokens[existing_digest] = now + 60
            self._confirmations[digest] = record
        return MrktListingConfirmation(
            token=token,
            display_name=record.display_name,
            price_ton=formatted_price,
            expires_in_seconds=self._confirmation_ttl_seconds,
        )

    async def cancel_confirmation(self, token: str, owner_telegram_id: int) -> bool:
        digest = self._token_digest(token)
        now = time.monotonic()
        async with self._confirmation_lock:
            self._purge_tokens(now)
            record = self._confirmations.get(digest)
            if record is None or record.owner_telegram_id != owner_telegram_id:
                return False
            self._confirmations.pop(digest, None)
            self._spent_tokens[digest] = now + 60
            return True

    async def confirm_listing(
        self,
        token: str,
        *,
        owner_telegram_id: int,
    ) -> MrktListingResult:
        digest = self._token_digest(token)
        account_key: tuple[int, int] | None = None
        now = time.monotonic()
        async with self._confirmation_lock:
            self._purge_tokens(now)
            record = self._confirmations.get(digest)
            if record is None:
                if digest in self._spent_tokens:
                    raise MrktConfirmationUsedError(
                        "Это подтверждение уже использовано. Начните заново."
                    )
                raise MrktConfirmationError(
                    "Подтверждение некорректно или истекло. Начните заново."
                )
            if record.owner_telegram_id != owner_telegram_id:
                raise MrktConfirmationError(
                    "Это подтверждение принадлежит другому пользователю."
                )
            if record.expires_at <= now:
                self._confirmations.pop(digest, None)
                self._spent_tokens[digest] = now + 60
                raise MrktConfirmationError(
                    "Срок подтверждения истёк. Начните размещение заново."
                )
            account_key = (record.owner_telegram_id, record.account_id)
            if account_key in self._in_flight:
                self._confirmations.pop(digest, None)
                self._spent_tokens[digest] = now + 60
                raise MrktListingInFlightError(
                    "Другое подтверждение размещения уже обрабатывается."
                )
            self._confirmations.pop(digest, None)
            self._spent_tokens[digest] = now + 60
            self._in_flight.add(account_key)

        try:
            return await self._create_confirmed_listing(record)
        finally:
            async with self._confirmation_lock:
                if account_key is not None:
                    self._in_flight.discard(account_key)

    async def execute_job_listing(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        gift_id: str | int,
        display_name: str,
        price_ton: str | Decimal,
        dry_run_override: bool | None = None,
    ) -> MrktListingResult:
        """Execute one job-owned listing through the normal revalidation boundary."""
        price_nanotons = self.ton_to_nanotons(price_ton)
        record = _ConfirmationRecord(
            owner_telegram_id=owner_telegram_id,
            account_id=account_id,
            gift_id=gift_id,
            display_name=self._safe_display_name(display_name),
            price_ton=self.format_ton(price_ton),
            price_nanotons=price_nanotons,
            expires_at=time.monotonic() + self._confirmation_ttl_seconds,
        )
        account_key = (owner_telegram_id, account_id)
        async with self._confirmation_lock:
            if account_key in self._in_flight:
                raise MrktListingInFlightError(
                    "Для этого аккаунта уже обрабатывается другое размещение."
                )
            self._in_flight.add(account_key)
        try:
            return await self._create_confirmed_listing(
                record, dry_run_override=dry_run_override
            )
        finally:
            async with self._confirmation_lock:
                self._in_flight.discard(account_key)

    async def shutdown(self) -> None:
        async with self._confirmation_lock:
            self._confirmations.clear()
            self._spent_tokens.clear()
            self._in_flight.clear()

    async def _create_confirmed_listing(
        self,
        record: _ConfirmationRecord,
        *,
        dry_run_override: bool | None = None,
    ) -> MrktListingResult:
        session = await self._authenticate(
            record.account_id,
            record.owner_telegram_id,
        )
        request_body = {
            "ids": [record.gift_id],
            "price": record.price_nanotons,
        }
        try:
            gifts = await self._fetch_inventory(
                session, is_listed=False, verification_account_id=record.account_id
            )
            selected = self._find_gift(gifts, record.gift_id)
            if selected is None:
                raise MrktGiftUnavailableError(
                    "Выбранный подарок исчез из хранилища MRKT. Ничего не размещено."
                )
            if not selected.eligible:
                raise MrktGiftUnavailableError(
                    selected.eligibility_reason
                    or "Выбранный подарок больше нельзя разместить."
                )

            effective_dry_run = (
                self._dry_run if dry_run_override is None else dry_run_override
            )
            if effective_dry_run:
                logger.info(
                    "MRKT dry run complete account_db_id=%d path=%s "
                    "request_fields=ids,price price_nanotons=%d",
                    record.account_id,
                    MRKT_SALE_PATH,
                    record.price_nanotons,
                )
                return MrktListingResult(
                    status=MrktListingStatus.DRY_RUN,
                    display_name=record.display_name,
                    price_ton=record.price_ton,
                    price_nanotons=record.price_nanotons,
                    sale_request_sent=False,
                    response_status=None,
                    response_fields=(),
                    message="Тестовый запуск выполнен успешно. Запрос на размещение не отправлялся.",
                )

            logger.info(
                "MRKT sale request started account_db_id=%d path=%s "
                "request_fields=ids,price price_nanotons=%d",
                record.account_id,
                MRKT_SALE_PATH,
                record.price_nanotons,
            )
            try:
                # Deliberately exactly one call. Never wrap this in safe-read retry logic.
                mutation_started("mrkt_listing")
                response = await asyncio.to_thread(
                    session.transport.post_json,
                    MRKT_SALE_PATH,
                    request_body,
                    authorization=session.token,
                )
            except MrktNetworkError as exc:
                logger.warning(
                    "MRKT sale response ambiguous account_db_id=%d exception=%s",
                    record.account_id,
                    type(exc).__name__,
                )
                return await self._resolve_ambiguous_sale(record)
            except Exception as exc:  # noqa: BLE001 - a sale failure is ambiguous
                logger.warning(
                    "MRKT sale response ambiguous account_db_id=%d exception=%s",
                    record.account_id,
                    type(exc).__name__,
                )
                return await self._resolve_ambiguous_sale(record)

            response_metadata("mrkt_listing", response.status, response.body)
            response_fields = self._field_names(response.body)
            logger.info(
                "MRKT sale response account_db_id=%d status=%d response_fields=%s",
                record.account_id,
                response.status,
                ",".join(response_fields),
            )
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            response_ids = (
                response.body.get("ids") if isinstance(response.body, dict) else None
            )
            if not isinstance(response_ids, list) or not any(
                self._same_gift_id(item, record.gift_id) for item in response_ids
            ):
                return MrktListingResult(
                    status=MrktListingStatus.REJECTED,
                    display_name=record.display_name,
                    price_ton=record.price_ton,
                    price_nanotons=record.price_nanotons,
                    sale_request_sent=True,
                    response_status=response.status,
                    response_fields=response_fields,
                    message=(
                        "MRKT не подтвердил размещение. Возможно, действует задержка "
                        "или ограничение площадки."
                    ),
                )
            return MrktListingResult(
                status=MrktListingStatus.SUCCESS,
                display_name=record.display_name,
                price_ton=record.price_ton,
                price_nanotons=record.price_nanotons,
                sale_request_sent=True,
                response_status=response.status,
                response_fields=response_fields,
                message="MRKT подтвердил размещение подарка.",
            )
        finally:
            request_body.clear()
            session.token = ""
            session.transport.close()

    async def _resolve_ambiguous_sale(
        self,
        record: _ConfirmationRecord,
    ) -> MrktListingResult:
        try:
            listed = await self.get_inventory(
                record.account_id,
                owner_telegram_id=record.owner_telegram_id,
                is_listed=True,
            )
            listed_gift = self._find_gift(listed, record.gift_id)
            if (
                listed_gift is not None
                and listed_gift.sale_price_nanotons
                == self.seller_to_buyer_price_nanotons(record.price_nanotons)
            ):
                return MrktListingResult(
                    status=MrktListingStatus.SUCCESS,
                    display_name=record.display_name,
                    price_ton=record.price_ton,
                    price_nanotons=record.price_nanotons,
                    sale_request_sent=True,
                    response_status=None,
                    response_fields=(),
                    message=(
                        "Ответ на запрос продажи был прерван, но хранилище MRKT "
                        "показывает подарок по запрошенной цене."
                    ),
                )
            unlisted = await self.get_inventory(
                record.account_id,
                owner_telegram_id=record.owner_telegram_id,
                is_listed=False,
            )
            still_unlisted = self._find_gift(unlisted, record.gift_id) is not None
        except Exception as exc:  # noqa: BLE001 - ambiguity must stay sanitized
            logger.warning(
                "MRKT ambiguous sale verification failed account_db_id=%d exception=%s",
                record.account_id,
                type(exc).__name__,
            )
            still_unlisted = False

        suffix = (
            "Подарок по-прежнему не размещён, но запрос продажи не повторялся."
            if still_unlisted
            else "Не удалось достоверно определить текущее состояние размещения."
        )
        return MrktListingResult(
            status=MrktListingStatus.AMBIGUOUS,
            display_name=record.display_name,
            price_ton=record.price_ton,
            price_nanotons=record.price_nanotons,
            sale_request_sent=True,
            response_status=None,
            response_fields=(),
            message=(
                "MRKT мог получить запрос на размещение, но ответ был потерян. "
                f"{suffix} Проверьте хранилище MRKT перед повторной попыткой."
            ),
        )

    async def _seller_reconciliation(
        self,
        session: _MrktAuthSession,
        account_id: int,
        gift_id: Any,
    ) -> dict[str, Any]:
        """Read-only state after an ambiguous listing; never authorizes a buy."""
        result: dict[str, Any] = {
            "outcome": "unavailable",
            "buyer_unlisted_present": None,
            "buyer_listed_present": None,
            "seller_unlisted_present": None,
            "seller_listed_present": None,
        }
        try:
            unlisted = await self._fetch_inventory(
                session, is_listed=False, verification_account_id=account_id
            )
            listed = await self._fetch_inventory(
                session, is_listed=True, verification_account_id=account_id
            )
        except Exception as exc:  # noqa: BLE001 - ambiguity remains sanitized
            logger.warning(
                "MRKT listing reconciliation failed exception=%s",
                type(exc).__name__,
            )
            return result
        seller_unlisted = any(
            self._same_gift_id(gift.gift_id, gift_id) for gift in unlisted
        )
        seller_listed = any(
            self._same_gift_id(gift.gift_id, gift_id) for gift in listed
        )
        result.update(
            outcome="inconclusive",
            seller_unlisted_present=seller_unlisted,
            seller_listed_present=seller_listed,
        )
        return result

    @staticmethod
    def _empty_listed_trigger(
        enabled: bool,
        *,
        max_wait_ms: int,
        poll_interval_ms: int,
    ) -> dict[str, Any]:
        return {
            "mode": "LISTED_TRIGGERED" if enabled else None,
            "enabled": enabled,
            "max_wait_ms": max_wait_ms if enabled else None,
            "poll_interval_ms": poll_interval_ms if enabled else None,
            "probe_count": 0,
            "probes": [],
            "probe_rtts_ms": [],
            "first_probe_start_ms": None,
            "listing_confirmed_ms": None,
            "buy_started_ms": None,
            "confirm_to_buy_start_ms": None,
            "sale_response_ms": None,
            "stopped_reason": None if enabled else "DISABLED",
        }

    async def _watch_for_exact_listing(
        self,
        *,
        lookup_session: _MrktAuthSession,
        listing_task: asyncio.Task[
            tuple[MrktHttpResponse | None, Exception | None, int]
        ],
        gift_id: Any,
        expected_seller_price_nanotons: int,
        expected_buyer_price_nanotons: int,
        expected_seller_id: Any,
        sale_started_ns: int,
        max_wait_ms: int,
        poll_interval_ms: int,
        start_buy: Callable[
            [],
            tuple[
                asyncio.Task[
                    tuple[MrktHttpResponse | None, Exception | None, int, int]
                ],
                int,
            ],
        ],
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> _MrktListedTriggerWatchResult:
        metadata = self._empty_listed_trigger(
            True,
            max_wait_ms=max_wait_ms,
            poll_interval_ms=poll_interval_ms,
        )
        metadata["mrkt_price_correlation"] = self._price_correlation_metadata(
            raw=None,
            seller_price_nanotons=expected_seller_price_nanotons,
            buyer_price_nanotons=expected_buyer_price_nanotons,
        )
        deadline_ns = sale_started_ns + max_wait_ms * 1_000_000
        payload = {"ids": [gift_id]}
        try:
            for probe_number in range(1, MRKT_LISTED_TRIGGER_MAX_PROBES + 1):
                if self._sale_explicitly_failed(listing_task):
                    metadata["stopped_reason"] = "SALE_FAILED"
                    break
                probe_started_ns = clock_ns()
                if probe_number > 1 and probe_started_ns >= deadline_ns:
                    metadata["stopped_reason"] = "TIMEOUT"
                    break
                probe_started_ms = round(
                    (probe_started_ns - sale_started_ns) / 1_000_000, 3
                )
                if metadata["first_probe_start_ms"] is None:
                    metadata["first_probe_start_ms"] = probe_started_ms
                try:
                    response = await asyncio.to_thread(
                        lookup_session.transport.post_json,
                        MRKT_MARKET_BY_IDS_PATH,
                        payload,
                        authorization=lookup_session.token,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - read failure is terminal
                    finished_ns = clock_ns()
                    failed_probe = self._listed_trigger_probe_metadata(
                            number=probe_number,
                            started_ns=probe_started_ns,
                            finished_ns=finished_ns,
                            sale_started_ns=sale_started_ns,
                            read_ok=False,
                            error_type=type(exc).__name__,
                        )
                    metadata["probes"].append(failed_probe)
                    metadata["probe_rtts_ms"].append(failed_probe["duration_ms"])
                    metadata["probe_count"] = probe_number
                    metadata["stopped_reason"] = "READ_ERROR"
                    break
                finished_ns = clock_ns()
                exact = self._exact_trigger_listing(
                    response.body,
                    gift_id=gift_id,
                    expected_buyer_price_nanotons=expected_buyer_price_nanotons,
                    expected_seller_id=expected_seller_id,
                )
                sale_failed = self._sale_explicitly_failed(listing_task)
                exact_ready = (
                    200 <= response.status < 300
                    and not sale_failed
                    and not exact.ambiguous
                    and exact.exact_listing_present
                    and exact.exact_price_match
                    and exact.seller_match is not False
                )
                if exact_ready:
                    listing_confirmed_ns = finished_ns
                    buy_task, buy_started_ns = start_buy()
                    # The HTTP operation has already been submitted at this point.
                    price_correlation = self._price_correlation_metadata(
                        raw=exact.raw,
                        seller_price_nanotons=expected_seller_price_nanotons,
                        buyer_price_nanotons=expected_buyer_price_nanotons,
                    )
                    probe_metadata = self._listed_trigger_probe_metadata(
                        number=probe_number,
                        started_ns=probe_started_ns,
                        finished_ns=finished_ns,
                        sale_started_ns=sale_started_ns,
                        read_ok=True,
                        http_status=response.status,
                        exact_listing_present=exact.exact_listing_present,
                        exact_price_match=exact.exact_price_match,
                        seller_available=exact.seller_available,
                        seller_match=exact.seller_match,
                        ambiguous=False,
                    )
                    probe_metadata["observed_listing_price_nanotons"] = (
                        exact.observed_listing_price_nanotons
                    )
                    probe_metadata["exact_listing_ready"] = True
                    metadata["mrkt_price_correlation"] = price_correlation
                    metadata["probe_count"] = probe_number
                    metadata["probes"].append(probe_metadata)
                    metadata["probe_rtts_ms"].append(probe_metadata["duration_ms"])
                    metadata["listing_confirmed_ms"] = round(
                        (listing_confirmed_ns - sale_started_ns) / 1_000_000, 3
                    )
                    metadata["buy_started_ms"] = round(
                        (buy_started_ns - sale_started_ns) / 1_000_000, 3
                    )
                    metadata["confirm_to_buy_start_ms"] = round(
                        (buy_started_ns - listing_confirmed_ns) / 1_000_000, 3
                    )
                    metadata["stopped_reason"] = "LISTING_CONFIRMED"
                    return _MrktListedTriggerWatchResult(
                        metadata=metadata,
                        buy_task=buy_task,
                        listing_confirmed_ns=listing_confirmed_ns,
                    )

                probe_metadata = self._listed_trigger_probe_metadata(
                    number=probe_number,
                    started_ns=probe_started_ns,
                    finished_ns=finished_ns,
                    sale_started_ns=sale_started_ns,
                    read_ok=200 <= response.status < 300,
                    http_status=response.status,
                    exact_listing_present=exact.exact_listing_present,
                    exact_price_match=exact.exact_price_match,
                    seller_available=exact.seller_available,
                    seller_match=exact.seller_match,
                    ambiguous=exact.ambiguous,
                )
                probe_metadata["observed_listing_price_nanotons"] = (
                    exact.observed_listing_price_nanotons
                )
                if exact.raw is not None:
                    metadata["mrkt_price_correlation"] = (
                        self._price_correlation_metadata(
                            raw=exact.raw,
                            seller_price_nanotons=expected_seller_price_nanotons,
                            buyer_price_nanotons=expected_buyer_price_nanotons,
                        )
                    )
                metadata["probe_count"] = probe_number
                metadata["probes"].append(probe_metadata)
                metadata["probe_rtts_ms"].append(probe_metadata["duration_ms"])
                if response.status == 429:
                    metadata["stopped_reason"] = "RATE_LIMITED"
                    break
                if not 200 <= response.status < 300:
                    metadata["stopped_reason"] = "READ_ERROR"
                    break
                if sale_failed:
                    metadata["stopped_reason"] = "SALE_FAILED"
                    break
                if exact.ambiguous:
                    metadata["stopped_reason"] = "AMBIGUOUS"
                    break
                if finished_ns >= deadline_ns:
                    metadata["stopped_reason"] = "TIMEOUT"
                    break
                await sleeper(poll_interval_ms / 1_000)
            else:
                metadata["stopped_reason"] = "TIMEOUT"
        finally:
            payload.clear()
        return _MrktListedTriggerWatchResult(
            metadata=metadata,
            buy_task=None,
            listing_confirmed_ns=None,
        )

    @classmethod
    def _exact_trigger_listing(
        cls,
        body: Any,
        *,
        gift_id: Any,
        expected_buyer_price_nanotons: int,
        expected_seller_id: Any,
    ) -> _MrktExactListingCorrelation:
        exact = [
            item
            for item in cls._market_items(body)
            if isinstance(item, dict) and cls._same_gift_id(item.get("id"), gift_id)
        ]
        if len(exact) != 1:
            return _MrktExactListingCorrelation(
                exact_listing_present=bool(exact),
                exact_price_match=False,
                seller_available=False,
                seller_match=None,
                ambiguous=len(exact) > 1,
                observed_listing_price_nanotons=None,
            )
        raw = exact[0]
        observed_price = cls.extract_mrkt_listing_price_nanotons(raw)
        price_match = observed_price == expected_buyer_price_nanotons
        seller_id = cls._seller_id(raw)
        seller_available = seller_id is not None
        seller_match = (
            cls._same_gift_id(seller_id, expected_seller_id)
            if seller_available and expected_seller_id is not None
            else None
        )
        ambiguous = raw.get("isMine") is True or seller_match is False
        return _MrktExactListingCorrelation(
            exact_listing_present=True,
            exact_price_match=price_match,
            seller_available=seller_available,
            seller_match=seller_match,
            ambiguous=ambiguous,
            observed_listing_price_nanotons=observed_price,
            raw=raw,
        )

    @staticmethod
    def _sale_explicitly_failed(
        listing_task: asyncio.Task[
            tuple[MrktHttpResponse | None, Exception | None, int]
        ],
    ) -> bool:
        if not listing_task.done() or listing_task.cancelled():
            return False
        response, error, _ = listing_task.result()
        return (
            error is None
            and response is not None
            and 400 <= response.status < 500
        )

    @staticmethod
    def _listed_trigger_probe_metadata(
        *,
        number: int,
        started_ns: int,
        finished_ns: int,
        sale_started_ns: int,
        read_ok: bool,
        http_status: int | None = None,
        exact_listing_present: bool = False,
        exact_price_match: bool = False,
        seller_available: bool = False,
        seller_match: bool | None = None,
        ambiguous: bool = False,
        error_type: str | None = None,
    ) -> dict[str, Any]:
        return {
            "probe": number,
            "started_ms": round((started_ns - sale_started_ns) / 1_000_000, 3),
            "finished_ms": round((finished_ns - sale_started_ns) / 1_000_000, 3),
            "duration_ms": round((finished_ns - started_ns) / 1_000_000, 3),
            "read_ok": read_ok,
            "http_status": http_status,
            "exact_listing_present": exact_listing_present,
            "exact_price_match": exact_price_match,
            "seller_available": seller_available,
            "seller_match": seller_match,
            "ambiguous": ambiguous,
            "error_type": error_type,
        }

    @classmethod
    def _classify_completed_fast_transfer(
        cls,
        *,
        mode: MrktTransferMode,
        listing_response: MrktHttpResponse | None,
        listing_error: Exception | None,
        buy_response: MrktHttpResponse | None,
        buy_error: Exception | None,
        gift_id: Any,
        verification: Mapping[str, Any],
    ) -> MrktFastTransferStatus:
        outcome = verification.get("outcome")
        if outcome == "consistent":
            return MrktFastTransferStatus.SUCCESS
        if outcome == "external_sale":
            return MrktFastTransferStatus.EXTERNAL_SALE
        if (
            listing_error is not None
            or listing_response is None
            or listing_response.status >= 500
            or buy_error is not None
            or buy_response is None
            or buy_response.status >= 500
        ):
            return MrktFastTransferStatus.BUY_AMBIGUOUS
        if outcome == "listing_still_active":
            if (
                mode is MrktTransferMode.SPECULATIVE
                and cls._listing_confirms_gift(listing_response, gift_id)
                and verification.get("seller_listed_present") is True
            ):
                return MrktFastTransferStatus.SPECULATIVE_BUY_TOO_EARLY
            return MrktFastTransferStatus.BUY_REJECTED
        if not 200 <= buy_response.status < 300:
            return MrktFastTransferStatus.BUY_REJECTED
        if cls._purchase_confirms_gift(buy_response.body, gift_id):
            return MrktFastTransferStatus.SUCCESS
        return MrktFastTransferStatus.VERIFICATION_AMBIGUOUS

    @staticmethod
    def _empty_listing_observation(enabled: bool) -> dict[str, Any]:
        return {
            "enabled": enabled,
            "observation_started_relative_to_sale_ms": None,
            "observation_started_before_sale_response": None,
            "first_snapshot_is_upper_bound": False,
            "samples": [],
            "last_observed_seller_unlisted_ms": None,
            "first_observed_seller_listed_ms": None,
            "listing_transition_lower_bound_ms": None,
            "listing_transition_upper_bound_ms": None,
            "observation_stopped_reason": "DISABLED" if not enabled else None,
        }

    async def _prepare_listing_observer_sessions(
        self,
        *,
        owner_telegram_id: int,
        buyer_account_id: int,
        seller_account_id: int,
    ) -> tuple[_MrktAuthSession | None, _MrktAuthSession | None]:
        """Prepare isolated read transports before SALE; failure cannot stop a job."""
        results = await asyncio.gather(
            self._authenticate(buyer_account_id, owner_telegram_id),
            self._authenticate(seller_account_id, owner_telegram_id),
            return_exceptions=True,
        )
        cancelled = next(
            (item for item in results if isinstance(item, asyncio.CancelledError)),
            None,
        )
        if cancelled is not None:
            raise cancelled
        if not all(isinstance(item, _MrktAuthSession) for item in results):
            for item in results:
                if isinstance(item, _MrktAuthSession):
                    item.token = ""
                    item.transport.close()
            error = next(
                (item for item in results if isinstance(item, BaseException)), None
            )
            logger.warning(
                "MRKT listing observer preparation failed exception=%s",
                type(error).__name__ if error is not None else "UnknownError",
            )
            return None, None
        buyer, seller = results
        assert isinstance(buyer, _MrktAuthSession)
        assert isinstance(seller, _MrktAuthSession)
        return buyer, seller

    async def _read_listing_observation_snapshot(
        self,
        *,
        buyer_session: _MrktAuthSession,
        seller_session: _MrktAuthSession,
        buyer_account_id: int,
        seller_account_id: int,
        gift_id: Any,
    ) -> _MrktPresenceSnapshot:
        async def account_presence(
            session: _MrktAuthSession, account_id: int
        ) -> tuple[bool, bool]:
            unlisted = await self._fetch_inventory(
                session, is_listed=False, verification_account_id=account_id
            )
            listed = await self._fetch_inventory(
                session, is_listed=True, verification_account_id=account_id
            )
            return (
                any(self._same_gift_id(gift.gift_id, gift_id) for gift in unlisted),
                any(self._same_gift_id(gift.gift_id, gift_id) for gift in listed),
            )

        observed = await asyncio.gather(
            account_presence(buyer_session, buyer_account_id),
            account_presence(seller_session, seller_account_id),
            return_exceptions=True,
        )
        for item in observed:
            if isinstance(item, MrktApiError) and item.status == 429:
                raise item
        error = next(
            (item for item in observed if isinstance(item, BaseException)), None
        )
        if error is not None:
            if isinstance(error, asyncio.CancelledError):
                raise error
            raise MrktServiceError(
                f"MRKT listing observation read failed: {type(error).__name__}"
            ) from None
        buyer_result, seller_result = observed
        assert isinstance(buyer_result, tuple)
        assert isinstance(seller_result, tuple)
        return _MrktPresenceSnapshot(
            seller_unlisted_present=seller_result[0],
            seller_listed_present=seller_result[1],
            buyer_unlisted_present=buyer_result[0],
            buyer_listed_present=buyer_result[1],
        )

    async def _observe_listing_activation(
        self,
        *,
        buyer_session: _MrktAuthSession,
        seller_session: _MrktAuthSession,
        buyer_account_id: int,
        seller_account_id: int,
        gift_id: Any,
        sale_started_ns: int,
        observation_started_ns: int,
        schedule_ms: tuple[int, ...] = MRKT_LISTING_OBSERVATION_SCHEDULE_MS,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        sample_reader: Callable[[], Awaitable[_MrktPresenceSnapshot]] | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Observe one typed gift with bounded reads and no mutation transport use."""
        result = self._empty_listing_observation(True)
        result["observation_started_relative_to_sale_ms"] = round(
            (observation_started_ns - sale_started_ns) / 1_000_000, 3
        )
        async def default_reader() -> _MrktPresenceSnapshot:
            return await self._read_listing_observation_snapshot(
                buyer_session=buyer_session,
                seller_session=seller_session,
                buyer_account_id=buyer_account_id,
                seller_account_id=seller_account_id,
                gift_id=gift_id,
            )

        read_sample = sample_reader or default_reader

        for index, target_ms in enumerate(schedule_ms):
            target_ns = observation_started_ns + target_ms * 1_000_000
            remaining_ns = target_ns - clock_ns()
            if remaining_ns > 0:
                await sleeper(remaining_ns / 1_000_000_000)
            try:
                snapshot = await read_sample()
            except MrktApiError as exc:
                sample_ns = clock_ns()
                stopped_reason = "RATE_LIMITED" if exc.status == 429 else "READ_ERROR"
                result["samples"].append(
                    self._listing_observation_error_sample(
                        sale_started_ns,
                        observation_started_ns,
                        sample_ns,
                        type(exc).__name__,
                    )
                )
                result["observation_stopped_reason"] = stopped_reason
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - read diagnostics are isolated
                sample_ns = clock_ns()
                result["samples"].append(
                    self._listing_observation_error_sample(
                        sale_started_ns,
                        observation_started_ns,
                        sample_ns,
                        type(exc).__name__,
                    )
                )
                result["observation_stopped_reason"] = "READ_ERROR"
                break

            sample_ns = clock_ns()
            sale_relative_ms = round(
                (sample_ns - sale_started_ns) / 1_000_000, 3
            )
            observer_relative_ms = round(
                (sample_ns - observation_started_ns) / 1_000_000, 3
            )
            sample = {
                "sale_relative_ms": sale_relative_ms,
                "observer_relative_ms": observer_relative_ms,
                "seller_unlisted_present": snapshot.seller_unlisted_present,
                "seller_listed_present": snapshot.seller_listed_present,
                "buyer_unlisted_present": snapshot.buyer_unlisted_present,
                "buyer_listed_present": snapshot.buyer_listed_present,
                "read_ok": True,
            }
            result["samples"].append(sample)
            if snapshot.seller_unlisted_present:
                result["last_observed_seller_unlisted_ms"] = sale_relative_ms
            if (
                snapshot.seller_listed_present
                and result["first_observed_seller_listed_ms"] is None
            ):
                result["first_observed_seller_listed_ms"] = sale_relative_ms
                result["listing_transition_upper_bound_ms"] = sale_relative_ms
                lower = result["last_observed_seller_unlisted_ms"]
                if isinstance(lower, (int, float)) and lower < sale_relative_ms:
                    result["listing_transition_lower_bound_ms"] = lower
                if index == 0:
                    result["first_snapshot_is_upper_bound"] = True

            buyer_owns = (
                snapshot.buyer_unlisted_present or snapshot.buyer_listed_present
            )
            if buyer_owns:
                result["observation_stopped_reason"] = "BUYER_OWNS"
                break
            if snapshot.seller_listed_present:
                result["observation_stopped_reason"] = "LISTED_OBSERVED"
                break
            if not any(
                (
                    snapshot.seller_unlisted_present,
                    snapshot.seller_listed_present,
                    snapshot.buyer_unlisted_present,
                    snapshot.buyer_listed_present,
                )
            ):
                result["observation_stopped_reason"] = "GIFT_ABSENT"
                break
        else:
            result["observation_stopped_reason"] = "TIMEOUT"
        return result, observation_started_ns

    @staticmethod
    def _listing_observation_error_sample(
        sale_started_ns: int,
        observation_started_ns: int,
        sample_ns: int,
        error_type: str,
    ) -> dict[str, Any]:
        return {
            "sale_relative_ms": round(
                (sample_ns - sale_started_ns) / 1_000_000, 3
            ),
            "observer_relative_ms": round(
                (sample_ns - observation_started_ns) / 1_000_000, 3
            ),
            "seller_unlisted_present": None,
            "seller_listed_present": None,
            "buyer_unlisted_present": None,
            "buyer_listed_present": None,
            "read_ok": False,
            "error_type": error_type,
        }

    async def _verify_fast_transfer_ownership(
        self,
        buyer_session: _MrktAuthSession,
        seller_session: _MrktAuthSession,
        buyer_account_id: int,
        seller_account_id: int,
        gift_id: Any,
    ) -> dict[str, Any]:
        async def account_presence(
            session: _MrktAuthSession, account_id: int
        ) -> tuple[bool, bool]:
            unlisted = await self._fetch_inventory(
                session, is_listed=False, verification_account_id=account_id
            )
            listed = await self._fetch_inventory(
                session, is_listed=True, verification_account_id=account_id
            )
            return (
                any(self._same_gift_id(gift.gift_id, gift_id) for gift in unlisted),
                any(self._same_gift_id(gift.gift_id, gift_id) for gift in listed),
            )

        observed = await asyncio.gather(
            account_presence(buyer_session, buyer_account_id),
            account_presence(seller_session, seller_account_id),
            return_exceptions=True,
        )
        buyer_result, seller_result = observed
        buyer_unlisted: bool | None = None
        buyer_listed: bool | None = None
        seller_unlisted: bool | None = None
        seller_listed: bool | None = None
        if isinstance(buyer_result, tuple):
            buyer_unlisted, buyer_listed = buyer_result
        if isinstance(seller_result, tuple):
            seller_unlisted, seller_listed = seller_result
        complete = isinstance(buyer_result, tuple) and isinstance(
            seller_result, tuple
        )
        buyer_owned = (
            buyer_unlisted is True or buyer_listed is True if complete else None
        )
        seller_present = (
            seller_unlisted is True or seller_listed is True if complete else None
        )
        if complete and buyer_owned and not seller_present:
            outcome = "consistent"
        elif (
            complete
            and not buyer_owned
            and seller_listed is True
            and seller_unlisted is False
        ):
            outcome = "listing_still_active"
        elif (
            complete
            and not buyer_owned
            and seller_unlisted is True
            and seller_listed is False
        ):
            outcome = "listing_not_active"
        elif complete and not buyer_owned and not seller_present:
            outcome = "external_sale"
        elif complete:
            outcome = "inconclusive"
        else:
            outcome = "unavailable"
        return {
            "outcome": outcome,
            "buyer_unlisted_present": buyer_unlisted,
            "buyer_listed_present": buyer_listed,
            "seller_unlisted_present": seller_unlisted,
            "seller_listed_present": seller_listed,
        }

    @staticmethod
    def _elapsed_ms(started_ns: int) -> float:
        return round((time.perf_counter_ns() - started_ns) / 1_000_000, 3)

    async def _authenticate(
        self,
        account_id: int,
        owner_telegram_id: int,
    ) -> _MrktAuthSession:
        launch = await self._miniapp_service.open_miniapp(
            account_id,
            "@mrkt",
            owner_telegram_id=owner_telegram_id,
        )
        self._validate_launch(launch)
        assert launch.init_data is not None
        auth_payload = self._auth_payload(launch.init_data)
        transport = self._transport_factory()
        try:
            response = await self._safe_post(
                transport,
                MRKT_AUTH_PATH,
                auth_payload,
            )
            response_fields = self._field_names(response.body)
            logger.info(
                "MRKT auth response account_db_id=%d status=%d response_fields=%s",
                account_id,
                response.status,
                ",".join(response_fields),
            )
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            if not isinstance(response.body, dict):
                raise MrktAuthenticationError(
                    f"Авторизация MRKT завершилась с кодом HTTP {response.status}."
                )
            token = response.body.pop("token", None)
            if not isinstance(token, str) or not token:
                raise MrktAuthenticationError(
                    "Авторизация MRKT не вернула пригодную сессию."
                )
            return _MrktAuthSession(
                transport=transport,
                token=token,
                launch_fingerprint=launch.init_data_fingerprint,
            )
        except MrktServiceError:
            transport.close()
            raise
        except Exception as exc:  # noqa: BLE001 - redact unexpected transport data
            transport.close()
            logger.warning(
                "MRKT authentication failed exception=%s", type(exc).__name__
            )
            raise MrktServiceError(
                f"Ошибка авторизации MRKT: {type(exc).__name__}"
            ) from None
        finally:
            auth_payload.clear()

    async def _fetch_inventory(
        self,
        session: _MrktAuthSession,
        *,
        is_listed: bool,
        verification_account_id: int | None = None,
    ) -> list[MrktGift]:
        gifts: list[MrktGift] = []
        cursor = ""
        seen_cursors: set[str] = set()
        for _ in range(MRKT_MAX_INVENTORY_PAGES):
            payload = {
                "isListed": is_listed,
                "count": 20,
                "cursor": cursor,
                **_DEFAULT_INVENTORY_FILTERS,
            }
            response = await self._safe_post(
                session.transport,
                MRKT_INVENTORY_PATH,
                payload,
                authorization=session.token,
            )
            response_metadata(
                "mrkt_listed_inventory" if is_listed else "mrkt_unlisted_inventory",
                response.status,
                response.body,
            )
            logger.info(
                "MRKT inventory response status=%d listed=%s response_fields=%s",
                response.status,
                is_listed,
                ",".join(self._field_names(response.body)),
            )
            if not 200 <= response.status < 300:
                raise self._api_error(response)
            if not isinstance(response.body, dict):
                raise MrktServiceError("Хранилище MRKT вернуло некорректный ответ.")
            raw_gifts = response.body.get("gifts")
            if not isinstance(raw_gifts, list):
                raise MrktServiceError(
                    "Ответ хранилища MRKT не содержит список подарков."
                )
            for item in raw_gifts:
                if isinstance(item, dict):
                    gift_snapshot(
                        "mrkt_listed_inventory"
                        if is_listed
                        else "mrkt_unlisted_inventory",
                        item,
                        self._seller_id(item),
                        account_id=verification_account_id,
                    )
            gifts.extend(
                self._parse_gift(item, is_listed=is_listed)
                for item in raw_gifts
                if isinstance(item, dict) and "id" in item
            )
            next_cursor = response.body.get("cursor")
            if (
                not isinstance(next_cursor, str)
                or not next_cursor
                or next_cursor in seen_cursors
            ):
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return gifts

    async def _safe_post(
        self,
        transport: MrktHttpTransport,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> MrktHttpResponse:
        for attempt in range(MRKT_SAFE_ATTEMPTS):
            try:
                return await asyncio.to_thread(
                    transport.post_json,
                    path,
                    payload,
                    authorization=authorization,
                )
            except MrktNetworkError:
                if attempt + 1 >= MRKT_SAFE_ATTEMPTS:
                    raise
            except Exception as exc:  # noqa: BLE001 - redact unexpected transport data
                logger.warning(
                    "MRKT safe request failed path=%s exception=%s",
                    path,
                    type(exc).__name__,
                )
                raise MrktServiceError(
                    f"Ошибка запроса MRKT: {type(exc).__name__}"
                ) from None
        raise AssertionError("unreachable")

    @staticmethod
    def _parse_gift(raw: Mapping[str, Any], *, is_listed: bool) -> MrktGift:
        is_locked = raw.get("isLockedForSale") is True
        explicitly_blocked = any(
            raw.get(field_name) is False
            for field_name in ("canBeListed", "isListingAvailable", "canSell")
            if field_name in raw
        )
        eligible = not is_listed and not is_locked and not explicitly_blocked
        reason = None
        if is_listed:
            reason = "Подарок уже размещён."
        elif is_locked:
            reason = "MRKT сообщает, что подарок заблокирован для продажи."
        elif explicitly_blocked:
            reason = "MRKT сообщает, что подарок нельзя разместить."
        return MrktGift(
            gift_id=raw["id"],
            display_name=MrktService._gift_display_name(raw),
            eligible=eligible,
            eligibility_reason=reason,
            sale_price_nanotons=MrktService.extract_mrkt_listing_price_nanotons(raw),
            seller_id=MrktService._seller_id(raw),
        )

    @staticmethod
    def _parse_market_gift(raw: Mapping[str, Any]) -> MrktGift:
        gift_snapshot("mrkt_market", raw, MrktService._seller_id(raw))
        return MrktGift(
            gift_id=raw["id"],
            display_name=MrktService._gift_display_name(raw),
            eligible=raw.get("isMine") is not True,
            eligibility_reason=(
                "Нельзя купить собственное размещение."
                if raw.get("isMine") is True
                else None
            ),
            sale_price_nanotons=MrktService.extract_mrkt_listing_price_nanotons(raw),
            seller_id=MrktService._seller_id(raw),
        )

    @staticmethod
    def _seller_id(raw: Mapping[str, Any]) -> Any:
        for key in ("sellerId", "seller_id", "ownerId", "owner_id", "userId"):
            value = raw.get(key)
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                return value
        seller = raw.get("seller") or raw.get("owner")
        if isinstance(seller, dict):
            value = seller.get("id")
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                return value
        return None

    @staticmethod
    def _market_items(body: Any) -> list[Any]:
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("gifts", "items", "data"):
                value = body.get(key)
                if isinstance(value, list):
                    return value
        return []

    @classmethod
    def _purchase_confirms_gift(cls, body: Any, gift_id: Any) -> bool:
        values: list[Any]
        if isinstance(body, list):
            values = body
        elif isinstance(body, dict):
            candidate: Any = next(
                (
                    body.get(key)
                    for key in ("ids", "gifts", "purchasedGifts", "items")
                    if isinstance(body.get(key), list)
                ),
                [],
            )
            values = candidate if isinstance(candidate, list) else []
        else:
            values = []
        for value in values:
            candidate = value.get("id") if isinstance(value, dict) else value
            if cls._same_gift_id(candidate, gift_id) or str(candidate) == str(gift_id):
                return True
        return False

    @staticmethod
    def _validate_gift_id(value: Any) -> None:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise MrktServiceError(
                "MRKT вернул неподдерживаемый идентификатор подарка."
            )
        if isinstance(value, str) and not value:
            raise MrktServiceError("MRKT вернул пустой идентификатор подарка.")

    @staticmethod
    def _gift_display_name(raw: Mapping[str, Any]) -> str:
        title = next(
            (
                value
                for key in ("title", "name", "collectionName")
                if isinstance((value := raw.get(key)), str) and value.strip()
            ),
            "Telegram gift",
        )
        number = raw.get("number")
        suffix = f" #{number}" if isinstance(number, int | str) else ""
        return MrktService._safe_display_name(f"{title}{suffix}")

    @staticmethod
    def _safe_display_name(value: str) -> str:
        sanitized = " ".join(value.replace("\x00", " ").split())
        return (sanitized or "Telegram gift")[:80]

    @staticmethod
    def _strict_nanotons(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int) and value >= 0:
            return value
        if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
            return int(value)
        return None

    @staticmethod
    def extract_mrkt_listing_price_nanotons(
        raw: Mapping[str, Any],
    ) -> int | None:
        """Extract the public buyer price used by MRKT's official buy payload."""
        return MrktService._strict_nanotons(raw.get("salePrice"))

    @classmethod
    def _price_correlation_metadata(
        cls,
        *,
        raw: Mapping[str, Any] | None,
        seller_price_nanotons: int,
        buyer_price_nanotons: int,
    ) -> dict[str, Any]:
        candidates: dict[str, dict[str, Any]] = {}
        if raw is not None:
            for key in sorted(raw):
                if "price" not in key.casefold():
                    continue
                value = raw.get(key)
                candidates[key] = {
                    "python_type": type(value).__name__,
                    "normalized_nanotons": cls._strict_nanotons(value),
                }
        observed = (
            cls.extract_mrkt_listing_price_nanotons(raw)
            if raw is not None
            else None
        )
        return {
            "seller_price_nanotons": seller_price_nanotons,
            "buyer_price_nanotons": buyer_price_nanotons,
            "commission_percent": MRKT_BUYER_COMMISSION_PERCENT,
            "response_field_names": tuple(sorted(raw)) if raw is not None else (),
            "listing_price_candidates": candidates,
            "selected_field": "salePrice",
            "observed_listing_price_nanotons": observed,
            "exact_price_match": observed == buyer_price_nanotons,
        }

    @staticmethod
    def _find_gift(gifts: list[MrktGift], gift_id: Any) -> MrktGift | None:
        return next(
            (
                gift
                for gift in gifts
                if MrktService._same_gift_id(gift.gift_id, gift_id)
            ),
            None,
        )

    @staticmethod
    def _same_gift_id(left: Any, right: Any) -> bool:
        return type(left) is type(right) and left == right

    @staticmethod
    def _validate_launch(launch: MiniAppLaunchResult) -> None:
        if launch.bot_username != "@mrkt":
            raise MrktAuthenticationError("Выбран неожиданный бот Mini App.")
        if not launch.init_data_obtained or not launch.init_data:
            raise MrktAuthenticationError("Telegram не вернул данные запуска MRKT.")
        if not launch.webview_url:
            raise MrktAuthenticationError("Telegram не вернул WebView MRKT.")
        parsed = urlsplit(launch.webview_url)
        if parsed.scheme != "https" or parsed.hostname != "cdn.tgmrkt.io":
            raise MrktAuthenticationError("Telegram вернул неожиданный URL MRKT.")

    @staticmethod
    def _auth_payload(init_data: str) -> dict[str, Any]:
        payload: dict[str, Any] = {"data": init_data, "appId": None}
        user_values = parse_qs(init_data).get("user")
        encoded_user = user_values[0] if user_values else None
        if encoded_user:
            try:
                user = json.loads(encoded_user)
            except (json.JSONDecodeError, TypeError):
                user = None
            if isinstance(user, dict):
                photo = user.get("photo_url")
                if isinstance(photo, str) and photo:
                    payload["photo"] = photo
        return payload

    @staticmethod
    def _api_error(response: MrktHttpResponse) -> MrktApiError:
        code = None
        if isinstance(response.body, dict):
            for key in ("code", "errorCode", "detail", "title"):
                candidate = response.body.get(key)
                if isinstance(candidate, str) and _SAFE_ERROR_CODE_PATTERN.fullmatch(
                    candidate
                ):
                    code = candidate
                    break
        return MrktApiError(response.status, code)

    @staticmethod
    def _field_names(value: Any) -> tuple[str, ...]:
        if not isinstance(value, dict):
            return ()
        return tuple(sorted(str(key) for key in value))

    @staticmethod
    def _token_digest(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    def _purge_tokens(self, now: float) -> None:
        self._confirmations = {
            digest: record
            for digest, record in self._confirmations.items()
            if record.expires_at > now
        }
        self._spent_tokens = {
            digest: expires_at
            for digest, expires_at in self._spent_tokens.items()
            if expires_at > now
        }
