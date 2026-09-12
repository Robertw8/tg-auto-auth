from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import socket
import sqlite3
import ssl
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import urlsplit

import aiohttp
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from app.db import Account, Database

from .miniapp_service import (
    MiniAppAccountNotFoundError,
    MiniAppBotResolutionError,
    MiniAppService,
    MiniAppSessionConnectionError,
    MiniAppSessionIdentityMismatchError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    MiniAppWebViewError,
)

logger = logging.getLogger(__name__)

TONNEL_BOT_USERNAME = "@Tonnel_Network_bot"
TONNEL_FRONTEND_ORIGIN = "https://marketplace.tonnel.network"
TONNEL_FRONTEND_REFERER = f"{TONNEL_FRONTEND_ORIGIN}/"
TONNEL_AUTH_ORIGIN = "https://gifts.coffin.meme"
TONNEL_API_ORIGIN = "https://gifts.coffin.meme"
TONNEL_REGIONAL_API_ORIGIN = "https://rs-api.tonnel.network"
TONNEL_REGIONAL_DATA_ORIGIN = "https://rs-gifts.tonnel.network"
TONNEL_ALLOWED_API_ORIGINS = frozenset({TONNEL_API_ORIGIN, TONNEL_REGIONAL_API_ORIGIN})
TONNEL_GIFTS2_ORIGIN = "https://gifts2.tonnel.network"
TONNEL_GIFTS3_ORIGIN = "https://gifts3.tonnel.network"
TONNEL_AUTH_PATH = "/api/auth/telegram/miniapp/session"
TONNEL_BALANCE_PATH = "/api/balance/info"
TONNEL_INVENTORY_PATH = "/api/fetchMangedGifts"
TONNEL_USER_INFO_PATH = "/api/userInfo"
TONNEL_RETURN_STATS_PATH = "/api/returnGiftStats"
TONNEL_DIRECT_TRANSFER_PATH = "/api/returnGiftToUser"
TONNEL_MARKET_INVENTORY_PATH = "/api/pageGifts"
TONNEL_GIFT_DATA_PATH = "/api/giftData"
TONNEL_LIST_FOR_SALE_PATH = "/api/listForSale"
TONNEL_BUY_GIFT_PATH = "/api/buyGift"
TONNEL_BUY_OFFER_CREATE_PATH = "/api/buyOffer/create"
TONNEL_BUY_OFFER_GET_PATH = "/api/buyOffer/getOffers"
TONNEL_BUY_OFFER_GET_MINE_PATH = "/api/buyOffer/getMyOffers"
TONNEL_BUY_OFFER_ACCEPT_PATH = "/api/buyOffer/acceptBuyOffer"
TONNEL_BUY_OFFER_CANCEL_PATH = "/api/buyOffer/cancel"
TONNEL_HTTP_TIMEOUT_SECONDS = 20
TONNEL_PAGE_SIZE = 20
TONNEL_MARKET_PAGE_SIZE = 30
TONNEL_MAX_INVENTORY_PAGES = 10
TONNEL_DIRECT_TRANSFER_FEE_BASE = Decimal("0.3")
TONNEL_DIRECT_TRANSFER_FEE_HIGH = Decimal("0.4")
TONNEL_DIRECT_TRANSFER_HIGH_FEE_THRESHOLD = 20
TONNEL_BUYER_FEE_FACTOR = Decimal("1.005")
TONNEL_MIN_MARKET_PRICE = Decimal("0.5")
TONNEL_MAX_MARKET_PRICE = Decimal(20000)
TONNEL_ATOMIC_FACTOR = Decimal(1000000000)
TONNEL_REQUEST_SECRET = b"yowtfisthispieceofshitiiit"
TONNEL_BUY_OFFER_CREATE_FEE = Decimal("0.01")
TONNEL_BUY_OFFER_SELLER_FEE_RATE = Decimal("0.005")
TONNEL_BUY_OFFER_STATUS_ACTIVE = frozenset({"pending", "active"})
TONNEL_OWNERSHIP_VERIFY_POLL_INTERVAL_MS = 1_000

_SAFE_STATUS_RE = re.compile(r"^[A-Za-z0-9_. -]{1,64}$")


class TonnelServiceError(RuntimeError):
    """Base class for a sanitized Tonnel failure."""


class TonnelNetworkError(TonnelServiceError):
    """A read-only Tonnel request failed."""

    def __init__(
        self,
        message: str,
        *,
        failure_kind: str = "network",
        exception_type: str | None = None,
    ) -> None:
        self.failure_kind = failure_kind
        self.exception_type = exception_type
        super().__init__(message)


class TonnelMutationNetworkError(TonnelServiceError):
    """A Tonnel mutation request may have reached the server."""


class TonnelDuplicateMutationError(TonnelServiceError):
    """A child tried to enter the same Tonnel CREATE mutation twice."""


class TonnelAuthenticationError(TonnelServiceError):
    """Tonnel rejected or did not establish the Mini App session."""

    def __init__(
        self,
        message: str,
        *,
        diagnostic: Mapping[str, object] | None = None,
        failure_code: str | None = None,
    ) -> None:
        self.diagnostic = dict(diagnostic) if diagnostic is not None else None
        self.failure_code = failure_code
        super().__init__(message)


class TonnelInventoryUnavailableError(TonnelServiceError):
    """A requested official Tonnel inventory is unavailable."""

    def __init__(
        self,
        message: str,
        *,
        reason: str = "inventory_unavailable",
        http_status: int | None = None,
    ) -> None:
        self.reason = reason
        self.http_status = http_status
        super().__init__(message)


class TonnelGiftUnavailableError(TonnelServiceError):
    """The exact selected gift is no longer transferable."""


class TonnelInsufficientBalanceError(TonnelServiceError):
    """The source account cannot cover the official direct-return fee."""


class TonnelRecipientMismatchError(TonnelServiceError):
    """Tonnel did not resolve the intended recipient exactly."""


class TonnelPreflightFailure(TonnelServiceError):
    """A sanitized, operation-scoped failure before any mutation."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        diagnostic: Mapping[str, object],
    ) -> None:
        self.error_code = error_code
        self.diagnostic = dict(diagnostic)
        super().__init__(message)


class TonnelApiError(TonnelServiceError):
    def __init__(self, status: int, code: str | None = None) -> None:
        self.status = status
        self.code = code if code and _SAFE_STATUS_RE.fullmatch(code) else None
        super().__init__(self.user_message)

    @property
    def user_message(self) -> str:
        if self.status == 401:
            return "Авторизация Tonnel истекла. Начните операцию заново."
        if self.status == 403:
            return "Tonnel не разрешил выполнить эту операцию."
        if self.status == 429:
            return "Tonnel ограничил частоту запросов. Попробуйте позже."
        if self.status >= 500:
            return "Tonnel временно недоступен."
        return "Tonnel отклонил запрос."


class TonnelTransferStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"
    DRY_RUN = "DRY_RUN"


class TonnelTransferStrategy(StrEnum):
    MARKET_SALE = "MARKET_SALE"
    DIRECT_RECIPIENT = "DIRECT_RECIPIENT"
    PRIVATE_RESERVED = "PRIVATE_RESERVED"
    LISTING_BOUND = "LISTING_BOUND"
    OFFER = "OFFER"
    PUBLIC_LISTING = "PUBLIC_LISTING"
    UNKNOWN = "UNKNOWN"


class TonnelTransferMode(StrEnum):
    MARKET_SALE = "MARKET_SALE"
    DIRECT_RECIPIENT = "DIRECT_RECIPIENT"
    BUY_OFFER = "BUY_OFFER"


@dataclass(frozen=True, slots=True)
class TonnelGiftIdentity:
    owned_gift_id: str | int = field(repr=False)
    collectible_slug: str


@dataclass(frozen=True, slots=True)
class TonnelGift:
    display_name: str
    identity: TonnelGiftIdentity
    eligible: bool
    eligibility_reason: str | None
    serial_number: str | int | None = field(default=None, repr=False)
    next_transfer_at: int | None = field(default=None, repr=False)

    @property
    def gift_id(self) -> str | int:
        return self.identity.owned_gift_id


@dataclass(frozen=True, slots=True)
class TonnelMarketGiftIdentity:
    source_gift_id: str | int = field(repr=False)
    sale_id: str = field(repr=False)
    collectible_slug: str


@dataclass(frozen=True, slots=True)
class TonnelMarketGift:
    display_name: str
    identity: TonnelMarketGiftIdentity
    eligible: bool
    eligibility_reason: str | None
    seller_telegram_id: int | None = field(default=None, repr=False)
    buyer_telegram_id: int | None = field(default=None, repr=False)
    asset: str = "TON"
    listing_price: Decimal | None = field(default=None, repr=False)
    status: str | None = None

    @property
    def gift_id(self) -> str | int:
        return self.identity.source_gift_id

    @property
    def sale_id(self) -> str:
        return self.identity.sale_id


@dataclass(frozen=True, slots=True)
class TonnelBalance:
    ton: Decimal
    tonnel: Decimal
    usdt: Decimal
    direct_transfer_enabled: bool | None


@dataclass(frozen=True, slots=True)
class TonnelMarketQuote:
    seller_price: Decimal
    buyer_price: Decimal
    seller_proceeds: Decimal
    fee_rate: Decimal
    cashback_fraction: Decimal
    seller_price_nanotons: int
    buyer_price_nanotons: int
    seller_balance: Decimal | None = None
    buyer_balance: Decimal | None = None


@dataclass(frozen=True, slots=True)
class TonnelBuyOfferQuote:
    offer_amount: Decimal
    seller_proceeds: Decimal
    seller_fee: Decimal
    create_fee: Decimal
    required_buyer_balance: Decimal
    offer_amount_nanotons: int
    seller_proceeds_nanotons: int
    seller_fee_nanotons: int
    create_fee_nanotons: int
    required_buyer_nanotons: int
    buyer_balance: Decimal | None = None


@dataclass(frozen=True, slots=True)
class TonnelBuyOffer:
    offer_id: str | int = field(repr=False)
    gift_id: str | int = field(repr=False)
    amount: Decimal
    asset: str
    status: str
    buyer_telegram_id: int | None = field(default=None, repr=False)
    seller_telegram_id: int | None = field(default=None, repr=False)
    created_at: str | None = None


@dataclass(frozen=True, slots=True)
class TonnelMutationResult:
    request_sent: bool
    http_status: int | None
    body_type: str
    status: str | None
    message: str | None
    safe_field_names: tuple[str, ...]
    duration_ms: int | None


@dataclass(frozen=True, slots=True)
class TonnelBuyOfferTransferResult:
    status: TonnelTransferStatus
    display_name: str
    offer_id: str | int | None = field(repr=False)
    gift_id: str | int = field(repr=False)
    quote: TonnelBuyOfferQuote
    create_result: TonnelMutationResult
    accept_result: TonnelMutationResult
    source_owns_after: bool | None
    destination_owns_after: bool | None
    offer_active_after: bool | None
    error_code: str | None
    timings_ms: Mapping[str, int | None]
    message: str
    offer_correlation: Mapping[str, object] = field(default_factory=dict)
    ownership_verification: Mapping[str, object] = field(default_factory=dict)
    create_request_metadata: Mapping[str, object] = field(default_factory=dict)
    accept_request_metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TonnelExistingOfferDiagnosticResult:
    status: TonnelTransferStatus
    display_name: str
    gift_id: str | int = field(repr=False)
    offer_id: str | int | None = field(repr=False)
    offer_age_ms: int | None
    candidate_count: int
    validation: Mapping[str, object]
    accept_result: TonnelMutationResult
    accept_request_metadata: Mapping[str, object]
    source_owns_after: bool | None
    destination_owns_after: bool | None
    offer_active_after: bool | None
    error_code: str | None
    message: str


@dataclass(frozen=True, slots=True)
class TonnelRecipient:
    telegram_user_id: int
    display_name: str
    username: str | None


@dataclass(frozen=True, slots=True)
class TonnelTransferResult:
    status: TonnelTransferStatus
    display_name: str
    strategy: TonnelTransferStrategy
    mutation_sent: bool
    response_status: int | None
    response_fields: tuple[str, ...]
    source_owns_after: bool | None
    destination_owns_after: bool | None
    message: str


@dataclass(frozen=True, slots=True)
class TonnelMarketTransferResult:
    status: TonnelTransferStatus
    display_name: str
    sale_id: str = field(repr=False)
    seller_price: Decimal
    buyer_price: Decimal
    seller_price_nanotons: int
    buyer_price_nanotons: int
    commission_percent: Decimal
    listing_sent: bool
    buy_sent: bool
    listing_status: int | None
    buy_status: int | None
    listing_response_fields: tuple[str, ...]
    buy_response_fields: tuple[str, ...]
    source_owns_after: bool | None
    destination_owns_after: bool | None
    listing_still_active: bool | None
    error_code: str | None
    timings_ms: Mapping[str, int | None]
    message: str
    seller_balance: Decimal | None = None
    buyer_balance: Decimal | None = None
    balance_sufficient: bool | None = None


@dataclass(frozen=True, slots=True)
class TonnelHttpResponse:
    status: int
    body: Any = field(repr=False)


class TonnelHttpTransport(Protocol):
    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        mutation: bool = False,
    ) -> TonnelHttpResponse: ...

    async def close(self) -> None: ...


def _network_failure_kind(exc: BaseException) -> str:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        name = type(current).__name__.lower()
        if (
            isinstance(current, (asyncio.TimeoutError, TimeoutError))
            or "timeout" in name
        ):
            return "timeout"
        if isinstance(current, socket.gaierror) or "dns" in name:
            return "dns"
        if isinstance(current, ssl.SSLError) or any(
            marker in name for marker in ("ssl", "tls", "certificate")
        ):
            return "tls"
        if isinstance(current, (ConnectionError, OSError)) or "connector" in name:
            return "connect"
        current = current.__cause__ or current.__context__
    return "network"


def _root_exception(exc: BaseException) -> BaseException:
    current = exc
    seen: set[int] = set()
    while id(current) not in seen:
        seen.add(id(current))
        nested = current.__cause__ or current.__context__
        if nested is None:
            return current
        current = nested
    return current


def _safe_sqlite_operational_message(exc: sqlite3.OperationalError) -> str:
    lowered = str(exc).strip().lower()
    for marker in (
        "database is locked",
        "database table is locked",
        "unable to open database file",
        "attempt to write a readonly database",
        "database or disk is full",
        "disk i/o error",
    ):
        if marker in lowered:
            return marker
    return "sqlite operational error"


class _AiohttpTonnelTransport:
    def __init__(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=TONNEL_HTTP_TIMEOUT_SECONDS),
            # Browser fetch() uses credentials="same-origin" by default. These
            # requests are cross-origin, so the official client sends no cookies.
            cookie_jar=aiohttp.DummyCookieJar(),
            headers={
                "Origin": TONNEL_FRONTEND_ORIGIN,
                # strict-origin-when-cross-origin reduces the document URL to
                # its origin for the official cross-origin browser request.
                "Referer": TONNEL_FRONTEND_REFERER,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "cross-site",
                "User-Agent": (
                    "Mozilla/5.0 AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/140 Safari/537.36"
                ),
            },
        )

    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        mutation: bool = False,
    ) -> TonnelHttpResponse:
        try:
            async with self._session.post(
                url,
                data=_exact_json_dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            ) as response:
                try:
                    body = await response.json(
                        content_type=None, loads=_exact_json_loads
                    )
                except (aiohttp.ContentTypeError, ValueError):
                    body = None
                return TonnelHttpResponse(status=response.status, body=body)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            failure_kind = _network_failure_kind(exc)
            safe_message = {
                "timeout": "Время ожидания ответа Tonnel истекло.",
                "dns": "Не удалось определить адрес сервера Tonnel.",
                "connect": "Не удалось подключиться к серверу Tonnel.",
                "tls": "Не удалось установить защищённое соединение с Tonnel.",
            }.get(failure_kind, "Ошибка сети при обращении к Tonnel.")
            if mutation:
                raise TonnelMutationNetworkError(
                    "Результат операции Tonnel не определён."
                ) from exc
            raise TonnelNetworkError(
                safe_message,
                failure_kind=failure_kind,
                exception_type=type(exc).__name__,
            ) from exc

    async def close(self) -> None:
        await self._session.close()


@dataclass(slots=True)
class _TonnelSession:
    account: Account
    transport: TonnelHttpTransport = field(repr=False)
    auth_data: str = field(repr=False)
    authenticated_telegram_id: int
    auth_created_at: str
    launch_fingerprint: str | None = None


class _BorrowedTonnelTransport:
    """Share one batch transport without letting child calls close it."""

    def __init__(self, transport: TonnelHttpTransport) -> None:
        self._transport = transport

    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        mutation: bool = False,
    ) -> TonnelHttpResponse:
        return await self._transport.post_json(url, payload, mutation=mutation)

    async def close(self) -> None:
        return None


@dataclass(slots=True)
class TonnelBatchAuthContext:
    """In-memory N1/N2 Tonnel sessions scoped to one short batch."""

    owner_telegram_id: int
    n1_account_id: int
    n2_account_id: int
    _n1_session: _TonnelSession = field(repr=False)
    _n2_session: _TonnelSession = field(repr=False)
    child_count: int = 0

    def set_child_count(self, child_count: int) -> None:
        if child_count < 0:
            raise ValueError("Tonnel batch child count cannot be negative")
        self.child_count = child_count

    def borrow(self, account_id: int, owner_telegram_id: int) -> _TonnelSession:
        if owner_telegram_id != self.owner_telegram_id:
            raise TonnelAuthenticationError(
                "Контекст Tonnel принадлежит другому пользователю."
            )
        if account_id == self.n1_account_id:
            session = self._n1_session
        elif account_id == self.n2_account_id:
            session = self._n2_session
        else:
            raise TonnelAuthenticationError(
                "Аккаунт не входит в подготовленную пару Tonnel."
            )
        return _TonnelSession(
            account=session.account,
            transport=_BorrowedTonnelTransport(session.transport),
            auth_data=session.auth_data,
            authenticated_telegram_id=session.authenticated_telegram_id,
            auth_created_at=session.auth_created_at,
            launch_fingerprint=session.launch_fingerprint,
        )

    def safe_metadata(self) -> dict[str, object]:
        return {
            "n1_authenticated_once": True,
            "n2_authenticated_once": True,
            "n1_account_id": self.n1_account_id,
            "n2_account_id": self.n2_account_id,
            "child_count": self.child_count,
            "telethon_launch_count_n1": 1,
            "telethon_launch_count_n2": 1,
        }


def _exact_json_dumps(value: Any) -> str:
    """Serialize Decimal as a JSON number without converting it through float."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise TypeError("Non-finite Decimal is not valid JSON")
        return format(value, "f")
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=True)
    if isinstance(value, Mapping):
        return (
            "{"
            + ",".join(
                f"{json.dumps(str(key), ensure_ascii=True)}:{_exact_json_dumps(item)}"
                for key, item in value.items()
            )
            + "}"
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return "[" + ",".join(_exact_json_dumps(item) for item in value) + "]"
    raise TypeError(f"Unsupported JSON value type: {type(value).__name__}")


def _exact_json_loads(value: str) -> Any:
    return json.loads(value, parse_float=Decimal)


class TonnelService:
    """Official Tonnel Mini App client with isolated market/direct strategies."""

    def __init__(
        self,
        miniapps: MiniAppService,
        db: Database,
        *,
        dry_run: bool = True,
        transfer_mode: str | TonnelTransferMode = TonnelTransferMode.MARKET_SALE,
        api_origin: str = TONNEL_API_ORIGIN,
        offer_accept_delay_ms: int = 0,
        ownership_verify_timeout_ms: int = 15_000,
        transport_factory: Callable[[], TonnelHttpTransport] | None = None,
    ) -> None:
        normalized_api_origin = api_origin.strip().rstrip("/")
        if normalized_api_origin not in TONNEL_ALLOWED_API_ORIGINS:
            raise ValueError("Unsupported Tonnel API origin")
        if (
            isinstance(offer_accept_delay_ms, bool)
            or not isinstance(offer_accept_delay_ms, int)
            or not 0 <= offer_accept_delay_ms <= 5_000
        ):
            raise ValueError("Tonnel offer accept delay must be between 0 and 5000 ms")
        if (
            isinstance(ownership_verify_timeout_ms, bool)
            or not isinstance(ownership_verify_timeout_ms, int)
            or not 0 <= ownership_verify_timeout_ms <= 60_000
        ):
            raise ValueError(
                "Tonnel ownership verification timeout must be between 0 and 60000 ms"
            )
        self._miniapps = miniapps
        self._db = db
        self._dry_run = dry_run
        self._transfer_mode = TonnelTransferMode(transfer_mode)
        self._api_origin = normalized_api_origin
        self._offer_accept_delay_ms = offer_accept_delay_ms
        self._ownership_verify_timeout_ms = ownership_verify_timeout_ms
        self._transport_factory = transport_factory or _AiohttpTonnelTransport
        self._batch_auth_context: ContextVar[TonnelBatchAuthContext | None] = (
            ContextVar(f"tonnel_batch_auth_{id(self)}", default=None)
        )
        self._buy_offer_create_guard = asyncio.Lock()
        self._entered_buy_offer_creates: set[str] = set()

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    @property
    def transfer_mode(self) -> TonnelTransferMode:
        return self._transfer_mode

    @property
    def api_origin(self) -> str:
        return self._api_origin

    @property
    def offer_accept_delay_ms(self) -> int:
        return self._offer_accept_delay_ms

    @property
    def ownership_verify_timeout_ms(self) -> int:
        return self._ownership_verify_timeout_ms

    async def authenticate(
        self, account_id: int, *, owner_telegram_id: int
    ) -> _TonnelSession:
        batch_context = self._batch_auth_context.get()
        if batch_context is not None:
            return batch_context.borrow(account_id, owner_telegram_id)
        account = await self._db.get_account(account_id, owner_telegram_id)
        if account is None:
            diagnostic: dict[str, object] = {
                "stage": "account_lookup",
                "account_id": account_id,
                "operation": "account_lookup",
                "api_origin": "local_database",
                "hostname": "local",
                "endpoint": "accounts",
                "method": "READ",
                "http_status": None,
                "exception_type": "TonnelAuthenticationError",
                "failure_kind": "local_state",
                "failure_code": "TELEGRAM_ACCOUNT_NOT_FOUND",
                "message": "Подключённый аккаунт не найден.",
            }
            logger.warning(
                "Tonnel authentication stopped stage=account_lookup account_db_id=%d "
                "failure_code=TELEGRAM_ACCOUNT_NOT_FOUND",
                account_id,
            )
            raise TonnelAuthenticationError(
                "Подключённый аккаунт не найден.", diagnostic=diagnostic
            )
        launch_url = "telegram://mtproto/messages.requestAppWebView"
        launch_started, launch_started_at = self._preflight_started(
            "miniapp_launch", launch_url, method="MTProto"
        )
        try:
            launch = await self._miniapps.open_miniapp(
                account_id,
                TONNEL_BOT_USERNAME,
                owner_telegram_id=owner_telegram_id,
            )
            if not launch.init_data:
                exc = TonnelAuthenticationError(
                    "Tonnel не вернул данные авторизации.",
                    failure_code="TONNEL_INITDATA_MISSING",
                )
                raise exc
        except Exception as exc:
            raise self._preflight_failure(
                "miniapp_launch",
                launch_url,
                launch_started,
                launch_started_at,
                None,
                exc,
                method="MTProto",
            ) from exc
        self._preflight_succeeded(
            "miniapp_launch",
            launch_url,
            launch_started,
            None,
            method="MTProto",
        )
        transport = self._transport_factory()
        auth_url = f"{TONNEL_AUTH_ORIGIN}{TONNEL_AUTH_PATH}"
        auth_started, auth_started_at = self._preflight_started(
            "session_auth", auth_url
        )
        response: TonnelHttpResponse | None = None
        try:
            response = await transport.post_json(
                auth_url,
                {
                    "initData": launch.init_data,
                    "origin": TONNEL_FRONTEND_ORIGIN,
                },
            )
            body = self._mapping(response.body)
            token = body.get("token")
            user = self._mapping(body.get("user"))
            user_id = self._integer(user.get("id"))
            if (
                response.status != 200
                or body.get("status") not in {None, "success"}
                or not isinstance(token, str)
                or not token
                or user_id != account.telegram_account_id
            ):
                raise TonnelAuthenticationError(
                    "Tonnel не подтвердил выбранный аккаунт."
                )
            self._preflight_succeeded("session_auth", auth_url, auth_started, response)
            logger.info(
                "Tonnel auth established account_db_id=%d launch_fingerprint=%s",
                account.id,
                launch.init_data_fingerprint or "none",
            )
            return _TonnelSession(
                account=account,
                transport=transport,
                auth_data=token,
                authenticated_telegram_id=user_id,
                auth_created_at=self._javascript_date_now(),
                launch_fingerprint=launch.init_data_fingerprint,
            )
        except Exception as exc:
            await transport.close()
            failure = self._preflight_failure(
                "session_auth",
                auth_url,
                auth_started,
                auth_started_at,
                response,
                exc,
            )
            if isinstance(exc, TonnelAuthenticationError):
                exc.diagnostic = failure.diagnostic
                raise
            raise failure from exc

    @asynccontextmanager
    async def batch_auth_context(
        self,
        *,
        owner_telegram_id: int,
        n1_account_id: int,
        n2_account_id: int,
    ) -> AsyncIterator[TonnelBatchAuthContext]:
        """Authenticate each account once and lend the contexts to child jobs."""
        if n1_account_id == n2_account_id:
            raise TonnelAuthenticationError(
                "Для передачи нужны разные аккаунты N1 и N2."
            )
        if self._batch_auth_context.get() is not None:
            raise TonnelAuthenticationError(
                "Контекст пакетной авторизации Tonnel уже активен."
            )
        async with AsyncExitStack() as stack:
            # Sequential preparation guarantees that even authentication itself
            # never overlaps access to a saved Telethon session.
            n1_session = await self.authenticate(
                n1_account_id, owner_telegram_id=owner_telegram_id
            )
            stack.push_async_callback(n1_session.transport.close)
            n2_session = await self.authenticate(
                n2_account_id, owner_telegram_id=owner_telegram_id
            )
            stack.push_async_callback(n2_session.transport.close)
            context = TonnelBatchAuthContext(
                owner_telegram_id=owner_telegram_id,
                n1_account_id=n1_account_id,
                n2_account_id=n2_account_id,
                _n1_session=n1_session,
                _n2_session=n2_session,
            )
            token = self._batch_auth_context.set(context)
            try:
                yield context
            finally:
                self._batch_auth_context.reset(token)

    async def get_inventory(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[TonnelGift]:
        session = await self.authenticate(
            account_id, owner_telegram_id=owner_telegram_id
        )
        try:
            return await self._get_inventory(session)
        finally:
            await session.transport.close()

    async def get_market_inventory(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[TonnelMarketGift]:
        session = await self.authenticate(
            account_id, owner_telegram_id=owner_telegram_id
        )
        try:
            return await self._get_market_inventory(session, listed=False)
        finally:
            await session.transport.close()

    async def get_transfer_inventory(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[TonnelGift] | list[TonnelMarketGift]:
        if self._transfer_mode in {
            TonnelTransferMode.MARKET_SALE,
            TonnelTransferMode.BUY_OFFER,
        }:
            return await self.get_market_inventory(
                account_id, owner_telegram_id=owner_telegram_id
            )
        return await self.get_inventory(account_id, owner_telegram_id=owner_telegram_id)

    async def get_balance(
        self, account_id: int, *, owner_telegram_id: int
    ) -> TonnelBalance:
        session = await self.authenticate(
            account_id, owner_telegram_id=owner_telegram_id
        )
        try:
            return await self._get_balance(session)
        finally:
            await session.transport.close()

    async def get_direct_transfer_fee(
        self, account_id: int, *, owner_telegram_id: int
    ) -> Decimal:
        session = await self.authenticate(
            account_id, owner_telegram_id=owner_telegram_id
        )
        try:
            return await self._get_direct_transfer_fee(session)
        finally:
            await session.transport.close()

    async def get_buy_offers_for_gift(
        self,
        account_id: int,
        gift_id: str | int,
        *,
        owner_telegram_id: int,
    ) -> list[TonnelBuyOffer]:
        session = await self.authenticate(
            account_id, owner_telegram_id=owner_telegram_id
        )
        try:
            return await self._get_buy_offers_for_gift(session, gift_id)
        finally:
            await session.transport.close()

    async def get_my_buy_offers(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[TonnelBuyOffer]:
        session = await self.authenticate(
            account_id, owner_telegram_id=owner_telegram_id
        )
        try:
            return await self._get_my_buy_offers(session)
        finally:
            await session.transport.close()

    async def transfer_buy_offer(
        self,
        *,
        owner_telegram_id: int,
        source_account_id: int,
        destination_account_id: int,
        identity: TonnelMarketGiftIdentity,
        offer_amount: str | Decimal,
        dry_run: bool | None = None,
        mutation_guard_key: str | None = None,
        child_job_id: int | None = None,
    ) -> TonnelBuyOfferTransferResult:
        """Create one exact buy offer, accept it once, then verify ownership."""
        effective_dry_run = self._dry_run if dry_run is None else dry_run
        job_started = time.monotonic()
        source, destination = await asyncio.gather(
            self.authenticate(source_account_id, owner_telegram_id=owner_telegram_id),
            self.authenticate(
                destination_account_id, owner_telegram_id=owner_telegram_id
            ),
        )
        async with AsyncExitStack() as stack:
            stack.push_async_callback(source.transport.close)
            stack.push_async_callback(destination.transport.close)
            source_gifts, destination_balance = await asyncio.gather(
                self._get_market_inventory(source, listed=False),
                self._get_balance(destination),
            )
            selected = self._find_exact_market_gift(source_gifts, identity)
            if not selected.eligible:
                raise TonnelGiftUnavailableError(
                    selected.eligibility_reason or "Подарок нельзя передать."
                )
            if (
                selected.seller_telegram_id is not None
                and selected.seller_telegram_id != source.account.telegram_account_id
            ):
                raise TonnelGiftUnavailableError(
                    "Tonnel вернул подарок другого владельца."
                )
            api_gift_id = self._buy_offer_gift_id(selected)
            quote = replace(
                self.buy_offer_quote(offer_amount),
                buyer_balance=destination_balance.ton,
            )
            create_payload = self.build_buy_offer_create_payload(
                destination.auth_data,
                api_gift_id,
                quote.offer_amount,
            )
            # These official read-only calls prove that both sides can observe the
            # offer model before any mutation is permitted.
            pre_for_gift, pre_mine = await asyncio.gather(
                self._get_buy_offers_for_gift(source, selected.sale_id),
                self._get_my_buy_offers(destination),
            )
            pre_create_offer_ids = frozenset(
                self._typed_id_key(offer.offer_id)
                for offer in (*pre_for_gift, *pre_mine)
                if self._typed_equal(offer.gift_id, api_gift_id)
            )
            offer_correlation: dict[str, object] = {
                "pre_create_offer_count": len(pre_create_offer_ids),
                "post_create_offer_count": None,
                "new_offer_count": None,
                "new_offer_id_fingerprint": None,
                "new_offer_id_type": None,
                "unique_new_offer_proven": False,
                "semantic_match_count": 0,
            }
            prepared_at = time.monotonic()
            timings: dict[str, int | None] = {
                "preparation_ms": self._milliseconds(job_started, prepared_at),
                "offer_create_request_ms": None,
                "offer_id_known_ms": None,
                "offer_id_known_to_accept_start_ms": None,
                "n2_offer_confirmed_ms": None,
                "accept_delay_ms": self._offer_accept_delay_ms,
                "n2_offer_confirmed_to_accept_start_ms": None,
                "offer_accept_request_ms": None,
                "accept_success_to_ownership_confirmed_ms": None,
                "total_job_ms": None,
            }
            empty_mutation = self._mutation_result(False, None, None, None)
            if destination_balance.ton < quote.required_buyer_balance:
                return self._buy_offer_result(
                    status=TonnelTransferStatus.FAILED,
                    selected=selected,
                    offer_id=None,
                    quote=quote,
                    create_result=empty_mutation,
                    accept_result=empty_mutation,
                    source_owns=True,
                    destination_owns=False,
                    offer_active=False,
                    error_code="INSUFFICIENT_BALANCE",
                    timings=self._finish_timings(timings, job_started),
                    message="На балансе покупателя недостаточно TON для оффера и комиссии.",
                    offer_correlation=offer_correlation,
                )
            if effective_dry_run:
                # The accept body cannot exist until the server assigns offer_id;
                # its exact official shape is nevertheless fixed and tested.
                return self._buy_offer_result(
                    status=TonnelTransferStatus.DRY_RUN,
                    selected=selected,
                    offer_id=None,
                    quote=quote,
                    create_result=empty_mutation,
                    accept_result=empty_mutation,
                    source_owns=True,
                    destination_owns=False,
                    offer_active=False,
                    error_code=None,
                    timings=self._finish_timings(timings, job_started),
                    message=(
                        "Тестовый запуск завершён. Создание и принятие оффера "
                        "не выполнялись."
                    ),
                    offer_correlation=offer_correlation,
                )

            create_started = time.monotonic()
            create_started_at = self._javascript_date_now()
            task = asyncio.current_task()
            create_request_metadata = dict(
                self._create_request_metadata(
                    gift_id=api_gift_id,
                    offer_amount=quote.offer_amount,
                    mutation_guard_key=mutation_guard_key,
                    child_job_id=child_job_id,
                    task_name=task.get_name() if task is not None else None,
                    request_started_at=create_started_at,
                )
            )
            if mutation_guard_key is not None:
                await self._enter_buy_offer_create_once(
                    mutation_guard_key, api_gift_id
                )
            logger.info(
                "Tonnel BUY_OFFER CREATE started child_job_id=%s "
                "gift_id_type=%s gift_id=%s amount=%s caller=%s task=%s "
                "request_started_at=%s",
                child_job_id,
                type(api_gift_id).__name__,
                api_gift_id,
                self.format_market_price(quote.offer_amount),
                create_request_metadata["caller"],
                create_request_metadata["asyncio_task"],
                create_request_metadata["request_started_at"],
            )
            create_response: TonnelHttpResponse | None = None
            try:
                create_response = await destination.transport.post_json(
                    self._api_url(TONNEL_BUY_OFFER_CREATE_PATH),
                    create_payload,
                    mutation=True,
                )
            except TonnelMutationNetworkError:
                pass
            create_finished = time.monotonic()
            create_request_metadata["response_received_at"] = (
                self._javascript_date_now()
            )
            create_request_metadata["http_transmission_count"] = 1
            timings["offer_create_request_ms"] = self._milliseconds(
                create_started, create_finished
            )
            create_result = self._mutation_result(
                True, create_response, create_started, create_finished
            )
            logger.info(
                "Tonnel BUY_OFFER CREATE completed child_job_id=%s "
                "gift_id_type=%s gift_id=%s response_received_at=%s "
                "http_status=%s status=%s message=%s duration_ms=%s",
                child_job_id,
                type(api_gift_id).__name__,
                api_gift_id,
                create_request_metadata["response_received_at"],
                create_result.http_status,
                create_result.status,
                create_result.message,
                create_result.duration_ms,
            )
            create_body = self._mapping(
                create_response.body if create_response is not None else None
            )
            if create_response is not None and not (
                create_response.status == 200 and create_body.get("status") == "success"
            ):
                return self._buy_offer_result(
                    status=TonnelTransferStatus.FAILED,
                    selected=selected,
                    offer_id=None,
                    quote=quote,
                    create_result=create_result,
                    accept_result=empty_mutation,
                    source_owns=True,
                    destination_owns=False,
                    offer_active=False,
                    error_code="OFFER_CREATE_REJECTED",
                    timings=self._finish_timings(timings, job_started),
                    message=create_result.message or "Tonnel отклонил создание оффера.",
                    offer_correlation=offer_correlation,
                    create_request_metadata=create_request_metadata,
                )
            returned_offer_id = self._extract_offer_id(create_body)
            offer, offer_correlation = await self._reconcile_exact_buy_offer(
                source,
                destination,
                selected,
                quote,
                expected_offer_id=returned_offer_id,
                pre_create_offer_ids=pre_create_offer_ids,
            )
            if offer is None:
                return self._buy_offer_result(
                    status=TonnelTransferStatus.AMBIGUOUS,
                    selected=selected,
                    offer_id=returned_offer_id,
                    quote=quote,
                    create_result=create_result,
                    accept_result=empty_mutation,
                    source_owns=True,
                    destination_owns=False,
                    offer_active=None,
                    error_code="OFFER_CREATE_UNCONFIRMED",
                    timings=self._finish_timings(timings, job_started),
                    message=(
                        "Созданный оффер не удалось точно определить. "
                        "Повторный запрос не отправлялся."
                    ),
                    offer_correlation=offer_correlation,
                    create_request_metadata=create_request_metadata,
                )

            n2_offer_confirmed_at = time.monotonic()
            timings["offer_id_known_ms"] = self._milliseconds(
                job_started, n2_offer_confirmed_at
            )
            timings["n2_offer_confirmed_ms"] = self._milliseconds(
                job_started, n2_offer_confirmed_at
            )
            self._assert_accept_context(source)
            accept_payload = self.build_buy_offer_accept_payload(
                source.auth_data, offer.offer_id
            )
            accept_request_metadata = self._accept_request_metadata(
                source,
                offer,
                n1_session=destination,
                offer_id_source=(
                    "create_response_and_correlated_reads"
                    if returned_offer_id is not None
                    else "n1_getMyOffers_and_n2_getOffers"
                ),
            )
            if self._offer_accept_delay_ms:
                await asyncio.sleep(self._offer_accept_delay_ms / 1000)
            accept_started = time.monotonic()
            timings["offer_id_known_to_accept_start_ms"] = self._milliseconds(
                n2_offer_confirmed_at, accept_started
            )
            timings["n2_offer_confirmed_to_accept_start_ms"] = self._milliseconds(
                n2_offer_confirmed_at, accept_started
            )
            accept_response: TonnelHttpResponse | None = None
            try:
                accept_response = await source.transport.post_json(
                    self._api_url(TONNEL_BUY_OFFER_ACCEPT_PATH),
                    accept_payload,
                    mutation=True,
                )
            except TonnelMutationNetworkError:
                pass
            accept_finished = time.monotonic()
            timings["offer_accept_request_ms"] = self._milliseconds(
                accept_started, accept_finished
            )
            accept_result = self._mutation_result(
                True, accept_response, accept_started, accept_finished
            )
            return await self._reconcile_buy_offer_transfer(
                source,
                destination,
                selected,
                offer,
                quote,
                create_result=create_result,
                accept_result=accept_result,
                accept_reported_success=(
                    accept_response is not None
                    and accept_response.status == 200
                    and self._mapping(accept_response.body).get("status") == "success"
                ),
                accept_finished=accept_finished,
                timings=timings,
                job_started=job_started,
                accept_request_metadata=accept_request_metadata,
                offer_correlation=offer_correlation,
                create_request_metadata=create_request_metadata,
            )

    async def accept_existing_buy_offer_diagnostic(
        self,
        *,
        owner_telegram_id: int,
        source_account_id: int,
        destination_account_id: int,
        gift_id: str | int,
        offer_amount: str | Decimal,
        expected_offer_fingerprint: str,
        confirm: bool = False,
    ) -> TonnelExistingOfferDiagnosticResult:
        """Accept one mature existing offer without ever creating an offer."""
        amount = self.normalize_market_price(offer_amount)
        if not re.fullmatch(r"[0-9a-f]{12}", expected_offer_fingerprint):
            raise ValueError("Invalid expected offer fingerprint")
        source, destination = await asyncio.gather(
            self.authenticate(source_account_id, owner_telegram_id=owner_telegram_id),
            self.authenticate(
                destination_account_id, owner_telegram_id=owner_telegram_id
            ),
        )
        async with AsyncExitStack() as stack:
            stack.push_async_callback(source.transport.close)
            stack.push_async_callback(destination.transport.close)
            source_gifts = await self._get_market_inventory(source, listed=False)
            selected_matches = [
                gift
                for gift in source_gifts
                if self._typed_equal(gift.gift_id, gift_id)
            ]
            if len(selected_matches) != 1:
                raise TonnelGiftUnavailableError(
                    "Точный подарок не найден у исходного аккаунта."
                )
            selected = selected_matches[0]
            api_gift_id = self._buy_offer_gift_id(selected)
            mine, for_gift = await asyncio.gather(
                self._get_my_buy_offers(destination),
                self._get_buy_offers_for_gift(source, selected.sale_id),
            )
            candidate, validation = self._correlate_existing_buy_offer(
                mine,
                for_gift,
                expected_gift_id=api_gift_id,
                expected_amount=amount,
                expected_buyer_id=destination.account.telegram_account_id,
                expected_seller_id=source.account.telegram_account_id,
                selected_seller_id=selected.seller_telegram_id,
                expected_fingerprint=expected_offer_fingerprint,
            )
            raw_candidate_count = validation["candidate_count"]
            candidate_count = (
                raw_candidate_count
                if isinstance(raw_candidate_count, int)
                and not isinstance(raw_candidate_count, bool)
                else 0
            )
            offer_age_ms = (
                self._offer_age_ms(candidate.created_at)
                if candidate is not None
                else None
            )
            empty = self._mutation_result(False, None, None, None)
            if candidate is None:
                return TonnelExistingOfferDiagnosticResult(
                    status=TonnelTransferStatus.FAILED,
                    display_name=selected.display_name,
                    gift_id=selected.gift_id,
                    offer_id=None,
                    offer_age_ms=None,
                    candidate_count=candidate_count,
                    validation=validation,
                    accept_result=empty,
                    accept_request_metadata={},
                    source_owns_after=True,
                    destination_owns_after=False,
                    offer_active_after=None,
                    error_code="EXISTING_OFFER_NOT_EXACT",
                    message=(
                        "Существующий оффер не прошёл точную проверку. "
                        "Принятие не выполнялось."
                    ),
                )

            self._assert_accept_context(source)
            accept_request_metadata = {
                **self._accept_request_metadata(
                    source,
                    candidate,
                    n1_session=destination,
                    offer_id_source="n1_getMyOffers_and_n2_getOffers",
                ),
                "auth_refreshed_immediately_before_accept": True,
                "diagnostic_strategy": "ACCEPT_EXISTING_OFFER_DIAGNOSTIC",
            }
            if not confirm:
                return TonnelExistingOfferDiagnosticResult(
                    status=TonnelTransferStatus.DRY_RUN,
                    display_name=selected.display_name,
                    gift_id=selected.gift_id,
                    offer_id=candidate.offer_id,
                    offer_age_ms=offer_age_ms,
                    candidate_count=candidate_count,
                    validation=validation,
                    accept_result=empty,
                    accept_request_metadata=accept_request_metadata,
                    source_owns_after=True,
                    destination_owns_after=False,
                    offer_active_after=True,
                    error_code=None,
                    message=(
                        "Точная проверка существующего оффера завершена. "
                        "Запрос принятия не отправлялся."
                    ),
                )

            accept_payload = self.build_buy_offer_accept_payload(
                source.auth_data, candidate.offer_id
            )
            accept_started = time.monotonic()
            accept_response: TonnelHttpResponse | None = None
            try:
                accept_response = await source.transport.post_json(
                    self._api_url(TONNEL_BUY_OFFER_ACCEPT_PATH),
                    accept_payload,
                    mutation=True,
                )
            except TonnelMutationNetworkError:
                pass
            accept_finished = time.monotonic()
            accept_result = self._mutation_result(
                True, accept_response, accept_started, accept_finished
            )
            source_owns, destination_owns, offer_active, complete_reads = (
                await self._verify_existing_offer_acceptance(
                    source, destination, selected, candidate
                )
            )
            if (
                complete_reads
                and destination_owns is True
                and source_owns is False
                and offer_active is False
            ):
                return TonnelExistingOfferDiagnosticResult(
                    status=TonnelTransferStatus.SUCCESS,
                    display_name=selected.display_name,
                    gift_id=selected.gift_id,
                    offer_id=candidate.offer_id,
                    offer_age_ms=offer_age_ms,
                    candidate_count=candidate_count,
                    validation=validation,
                    accept_result=accept_result,
                    accept_request_metadata=accept_request_metadata,
                    source_owns_after=False,
                    destination_owns_after=True,
                    offer_active_after=False,
                    error_code=None,
                    message="Существующий оффер принят, владение подтверждено.",
                )
            if (
                complete_reads
                and source_owns is True
                and destination_owns is False
                and offer_active is True
            ):
                return TonnelExistingOfferDiagnosticResult(
                    status=TonnelTransferStatus.FAILED,
                    display_name=selected.display_name,
                    gift_id=selected.gift_id,
                    offer_id=candidate.offer_id,
                    offer_age_ms=offer_age_ms,
                    candidate_count=candidate_count,
                    validation=validation,
                    accept_result=accept_result,
                    accept_request_metadata=accept_request_metadata,
                    source_owns_after=True,
                    destination_owns_after=False,
                    offer_active_after=True,
                    error_code="OFFER_NOT_ACCEPTED",
                    message=(
                        accept_result.message
                        or "Существующий оффер остался активным и не был принят."
                    ),
                )
            return TonnelExistingOfferDiagnosticResult(
                status=TonnelTransferStatus.AMBIGUOUS,
                display_name=selected.display_name,
                gift_id=selected.gift_id,
                offer_id=candidate.offer_id,
                offer_age_ms=offer_age_ms,
                candidate_count=candidate_count,
                validation=validation,
                accept_result=accept_result,
                accept_request_metadata=accept_request_metadata,
                source_owns_after=source_owns,
                destination_owns_after=destination_owns,
                offer_active_after=offer_active,
                error_code="OFFER_ACCEPT_AMBIGUOUS",
                message=(
                    "Результат принятия существующего оффера не определён. "
                    "Повторный запрос не отправлялся."
                ),
            )

    async def transfer_market_sale(
        self,
        *,
        owner_telegram_id: int,
        source_account_id: int,
        destination_account_id: int,
        identity: TonnelMarketGiftIdentity,
        seller_price: str | Decimal,
        dry_run: bool | None = None,
    ) -> TonnelMarketTransferResult:
        """List once, buy the exact stable sale ID once, then verify ownership."""
        effective_dry_run = self._dry_run if dry_run is None else dry_run
        job_started = time.monotonic()
        source, destination = await asyncio.gather(
            self.authenticate(source_account_id, owner_telegram_id=owner_telegram_id),
            self.authenticate(
                destination_account_id, owner_telegram_id=owner_telegram_id
            ),
        )
        async with AsyncExitStack() as stack:
            stack.push_async_callback(source.transport.close)
            stack.push_async_callback(destination.transport.close)
            source_gifts, source_balance, destination_balance = await asyncio.gather(
                self._get_market_inventory(source, listed=False),
                self._get_balance(source),
                self._get_balance(destination),
            )
            selected = self._find_exact_market_gift(source_gifts, identity)
            if not selected.eligible:
                raise TonnelGiftUnavailableError(
                    selected.eligibility_reason or "Подарок нельзя выставить."
                )
            if (
                selected.seller_telegram_id is not None
                and selected.seller_telegram_id != source.account.telegram_account_id
            ):
                raise TonnelGiftUnavailableError(
                    "Tonnel вернул подарок другого продавца."
                )
            quote = replace(
                self.market_quote(seller_price, destination_balance.tonnel),
                seller_balance=source_balance.ton,
                buyer_balance=destination_balance.ton,
            )
            # Build both official payloads while both authenticated contexts are
            # warm. These values remain memory-only and are never logged.
            listing_payload = self.build_list_for_sale_payload(
                source.auth_data, selected, quote.seller_price
            )
            buy_payload = self.build_buy_payload(
                destination.auth_data, selected, quote.seller_price
            )
            preparation_done = time.monotonic()
            base_timings: dict[str, int | None] = {
                "preparation_ms": self._milliseconds(job_started, preparation_done),
                "listing_request_ms": None,
                "sale_id_known_ms": None,
                "sale_id_known_to_buy_start_ms": None,
                "buy_request_ms": None,
                "total_job_ms": None,
            }
            if destination_balance.ton < quote.buyer_price:
                return self._market_result(
                    status=TonnelTransferStatus.FAILED,
                    selected=selected,
                    quote=quote,
                    listing_sent=False,
                    buy_sent=False,
                    listing_status=None,
                    buy_status=None,
                    listing_fields=(),
                    buy_fields=(),
                    source_owns=True,
                    destination_owns=False,
                    listing_still_active=False,
                    error_code="INSUFFICIENT_BALANCE",
                    timings=self._finish_timings(base_timings, job_started),
                    message="На балансе покупателя Tonnel недостаточно TON.",
                )
            if effective_dry_run:
                base_timings["total_job_ms"] = self._milliseconds(
                    job_started, time.monotonic()
                )
                return self._market_result(
                    status=TonnelTransferStatus.DRY_RUN,
                    selected=selected,
                    quote=quote,
                    listing_sent=False,
                    buy_sent=False,
                    listing_status=None,
                    buy_status=None,
                    listing_fields=(),
                    buy_fields=(),
                    source_owns=True,
                    destination_owns=False,
                    listing_still_active=False,
                    error_code=None,
                    timings=base_timings,
                    message=(
                        "Тестовый запуск завершён. Размещение и покупка не выполнялись."
                    ),
                )

            listing_started = time.monotonic()
            listing_response: TonnelHttpResponse | None = None
            listing_fields: tuple[str, ...] = ()
            try:
                listing_response = await source.transport.post_json(
                    self._api_url(TONNEL_LIST_FOR_SALE_PATH),
                    listing_payload,
                    mutation=True,
                )
            except TonnelMutationNetworkError:
                pass
            listing_finished = time.monotonic()
            base_timings["listing_request_ms"] = self._milliseconds(
                listing_started, listing_finished
            )
            sale_id_known_at: float | None = None
            listing_status = listing_response.status if listing_response else None
            if listing_response is not None:
                listing_body = self._mapping(listing_response.body)
                listing_fields = tuple(sorted(listing_body))
                returned_sale_id = self._extract_sale_id(listing_body)
                if (
                    returned_sale_id is not None
                    and returned_sale_id != selected.sale_id
                ):
                    return self._market_result(
                        status=TonnelTransferStatus.AMBIGUOUS,
                        selected=selected,
                        quote=quote,
                        listing_sent=True,
                        buy_sent=False,
                        listing_status=listing_status,
                        buy_status=None,
                        listing_fields=listing_fields,
                        buy_fields=(),
                        source_owns=None,
                        destination_owns=None,
                        listing_still_active=None,
                        error_code="LISTING_ID_MISMATCH",
                        timings=self._finish_timings(base_timings, job_started),
                        message=(
                            "Tonnel вернул другой идентификатор размещения. "
                            "Покупка не выполнялась."
                        ),
                    )
                if (
                    listing_response.status == 200
                    and listing_body.get("status") == "success"
                ):
                    # The stable sale ID is already supplied by /pageGifts before
                    # listing; the frontend itself converts gift_id to this string.
                    sale_id_known_at = listing_finished
                elif listing_response.status < 500:
                    return self._market_result(
                        status=TonnelTransferStatus.FAILED,
                        selected=selected,
                        quote=quote,
                        listing_sent=True,
                        buy_sent=False,
                        listing_status=listing_status,
                        buy_status=None,
                        listing_fields=listing_fields,
                        buy_fields=(),
                        source_owns=True,
                        destination_owns=False,
                        listing_still_active=False,
                        error_code="LISTING_REJECTED",
                        timings=self._finish_timings(base_timings, job_started),
                        message="Tonnel отклонил размещение подарка.",
                    )

            if sale_id_known_at is None:
                listed = await self._find_reconciled_listing(source, selected, quote)
                if listed is None:
                    return self._market_result(
                        status=TonnelTransferStatus.AMBIGUOUS,
                        selected=selected,
                        quote=quote,
                        listing_sent=True,
                        buy_sent=False,
                        listing_status=listing_status,
                        buy_status=None,
                        listing_fields=listing_fields,
                        buy_fields=(),
                        source_owns=None,
                        destination_owns=None,
                        listing_still_active=None,
                        error_code="LISTING_AMBIGUOUS",
                        timings=self._finish_timings(base_timings, job_started),
                        message=(
                            "Результат размещения не определён. "
                            "Повторный запрос не отправлялся."
                        ),
                    )
                sale_id_known_at = time.monotonic()

            base_timings["sale_id_known_ms"] = self._milliseconds(
                job_started, sale_id_known_at
            )
            # Critical path: no database write, progress callback, sleep, or lookup
            # is allowed between the known exact ID and this one BUY request.
            buy_started = time.monotonic()
            base_timings["sale_id_known_to_buy_start_ms"] = self._milliseconds(
                sale_id_known_at, buy_started
            )
            buy_response: TonnelHttpResponse | None = None
            try:
                buy_response = await destination.transport.post_json(
                    self._api_url(f"{TONNEL_BUY_GIFT_PATH}/{selected.sale_id}"),
                    buy_payload,
                    mutation=True,
                )
            except TonnelMutationNetworkError:
                pass
            buy_finished = time.monotonic()
            base_timings["buy_request_ms"] = self._milliseconds(
                buy_started, buy_finished
            )
            buy_status = buy_response.status if buy_response else None
            buy_body = self._mapping(buy_response.body if buy_response else None)
            buy_fields = tuple(sorted(buy_body))
            return await self._reconcile_market_transfer(
                source,
                destination,
                selected,
                quote,
                listing_status=listing_status,
                buy_status=buy_status,
                listing_fields=listing_fields,
                buy_fields=buy_fields,
                buy_reported_success=(
                    buy_response is not None
                    and buy_response.status == 200
                    and buy_body.get("status") == "success"
                ),
                timings=base_timings,
                job_started=job_started,
            )

    async def transfer_exact_gift(
        self,
        *,
        owner_telegram_id: int,
        source_account_id: int,
        destination_account_id: int,
        identity: TonnelGiftIdentity,
        dry_run: bool | None = None,
    ) -> TonnelTransferResult:
        effective_dry_run = self._dry_run if dry_run is None else dry_run
        source = await self.authenticate(
            source_account_id, owner_telegram_id=owner_telegram_id
        )
        destination = await self.authenticate(
            destination_account_id, owner_telegram_id=owner_telegram_id
        )
        async with AsyncExitStack() as stack:
            stack.push_async_callback(source.transport.close)
            stack.push_async_callback(destination.transport.close)
            source_gifts = await self._get_inventory(source)
            selected = self._find_exact_source_gift(source_gifts, identity)
            if not selected.eligible:
                raise TonnelGiftUnavailableError(
                    selected.eligibility_reason or "Подарок пока нельзя передать."
                )
            recipient = await self._resolve_recipient(
                source, destination.account.telegram_account_id
            )
            if recipient.telegram_user_id != destination.account.telegram_account_id:
                raise TonnelRecipientMismatchError(
                    "Tonnel разрешил другого получателя."
                )
            balance, transfer_fee = await asyncio.gather(
                self._get_balance(source), self._get_direct_transfer_fee(source)
            )
            if balance.ton < transfer_fee:
                raise TonnelInsufficientBalanceError(
                    "На балансе Tonnel недостаточно TON для комиссии передачи."
                )
            if effective_dry_run:
                return TonnelTransferResult(
                    status=TonnelTransferStatus.DRY_RUN,
                    display_name=selected.display_name,
                    strategy=TonnelTransferStrategy.DIRECT_RECIPIENT,
                    mutation_sent=False,
                    response_status=None,
                    response_fields=(),
                    source_owns_after=True,
                    destination_owns_after=None,
                    message="Тестовый запуск завершён. Подарок не передавался.",
                )

            mutation_response: TonnelHttpResponse | None = None
            try:
                mutation_response = await source.transport.post_json(
                    self._api_url(TONNEL_DIRECT_TRANSFER_PATH),
                    {
                        "authData": source.auth_data,
                        "gift_id": selected.gift_id,
                        "receiver": recipient.telegram_user_id,
                        "anonymous": False,
                    },
                    mutation=True,
                )
            except TonnelMutationNetworkError:
                return await self._reconcile_transfer(
                    source,
                    destination,
                    selected,
                    response_status=None,
                    response_fields=(),
                    mutation_uncertain=True,
                )

            body = self._mapping(mutation_response.body)
            fields = tuple(sorted(body))
            if mutation_response.status >= 500:
                return await self._reconcile_transfer(
                    source,
                    destination,
                    selected,
                    response_status=mutation_response.status,
                    response_fields=fields,
                    mutation_uncertain=True,
                )
            if mutation_response.status != 200 or body.get("status") != "success":
                return TonnelTransferResult(
                    status=TonnelTransferStatus.FAILED,
                    display_name=selected.display_name,
                    strategy=TonnelTransferStrategy.DIRECT_RECIPIENT,
                    mutation_sent=True,
                    response_status=mutation_response.status,
                    response_fields=fields,
                    source_owns_after=True,
                    destination_owns_after=None,
                    message="Tonnel отклонил прямую передачу подарка.",
                )
            return await self._reconcile_transfer(
                source,
                destination,
                selected,
                response_status=mutation_response.status,
                response_fields=fields,
                mutation_uncertain=False,
            )

    async def _reconcile_transfer(
        self,
        source: _TonnelSession,
        destination: _TonnelSession,
        selected: TonnelGift,
        *,
        response_status: int | None,
        response_fields: tuple[str, ...],
        mutation_uncertain: bool,
    ) -> TonnelTransferResult:
        source_owns: bool | None = None
        destination_owns: bool | None = None
        for delay in (0.0, 0.5, 1.5):
            if delay:
                await asyncio.sleep(delay)
            try:
                source_gifts, destination_gifts = await asyncio.gather(
                    self._get_inventory(source), self._get_inventory(destination)
                )
            except TonnelServiceError:
                continue
            source_owns = self._has_slug(
                source_gifts, selected.identity.collectible_slug
            )
            destination_owns = self._has_slug(
                destination_gifts, selected.identity.collectible_slug
            )
            if destination_owns and not source_owns:
                return TonnelTransferResult(
                    status=TonnelTransferStatus.SUCCESS,
                    display_name=selected.display_name,
                    strategy=TonnelTransferStrategy.DIRECT_RECIPIENT,
                    mutation_sent=True,
                    response_status=response_status,
                    response_fields=response_fields,
                    source_owns_after=False,
                    destination_owns_after=True,
                    message="Подарок передан на выбранный аккаунт.",
                )
            if source_owns:
                break
        if source_owns is True:
            return TonnelTransferResult(
                status=TonnelTransferStatus.FAILED,
                display_name=selected.display_name,
                strategy=TonnelTransferStrategy.DIRECT_RECIPIENT,
                mutation_sent=True,
                response_status=response_status,
                response_fields=response_fields,
                source_owns_after=True,
                destination_owns_after=destination_owns,
                message="Подарок остался на рабочем аккаунте.",
            )
        return TonnelTransferResult(
            status=TonnelTransferStatus.AMBIGUOUS,
            display_name=selected.display_name,
            strategy=TonnelTransferStrategy.DIRECT_RECIPIENT,
            mutation_sent=True,
            response_status=response_status,
            response_fields=response_fields,
            source_owns_after=source_owns,
            destination_owns_after=destination_owns,
            message=(
                "Результат передачи не определён. Повторный запрос не отправлялся."
                if mutation_uncertain
                else "Не удалось подтвердить нового владельца подарка."
            ),
        )

    async def _reconcile_market_transfer(
        self,
        source: _TonnelSession,
        destination: _TonnelSession,
        selected: TonnelMarketGift,
        quote: TonnelMarketQuote,
        *,
        listing_status: int | None,
        buy_status: int | None,
        listing_fields: tuple[str, ...],
        buy_fields: tuple[str, ...],
        buy_reported_success: bool,
        timings: dict[str, int | None],
        job_started: float,
    ) -> TonnelMarketTransferResult:
        source_owns: bool | None = None
        destination_owns: bool | None = None
        listing_active: bool | None = None
        sale_buyer: int | None = None
        complete_reads = False
        for delay in (0.0, 0.5, 1.5):
            if delay:
                await asyncio.sleep(delay)
            try:
                (
                    source_unlisted,
                    source_listed,
                    destination_unlisted,
                    sale,
                ) = await asyncio.gather(
                    self._get_market_inventory(source, listed=False),
                    self._get_market_inventory(source, listed=True),
                    self._get_market_inventory(destination, listed=False),
                    self._get_sale(destination, selected.sale_id),
                )
            except TonnelServiceError:
                continue
            complete_reads = True
            slug = selected.identity.collectible_slug
            source_owns = self._has_market_slug(source_unlisted, slug) or (
                self._has_market_slug(source_listed, slug)
            )
            destination_owns = self._has_market_slug(destination_unlisted, slug)
            sale_buyer = sale.buyer_telegram_id if sale is not None else None
            listing_active = any(
                gift.sale_id == selected.sale_id
                and gift.identity.collectible_slug == slug
                for gift in source_listed
            )
            sale_confirms_destination = (
                sale is not None
                and sale.sale_id == selected.sale_id
                and sale.identity.collectible_slug == slug
                and sale_buyer == destination.account.telegram_account_id
            )
            if (destination_owns or sale_confirms_destination) and not source_owns:
                return self._market_result(
                    status=TonnelTransferStatus.SUCCESS,
                    selected=selected,
                    quote=quote,
                    listing_sent=True,
                    buy_sent=True,
                    listing_status=listing_status,
                    buy_status=buy_status,
                    listing_fields=listing_fields,
                    buy_fields=buy_fields,
                    source_owns=False,
                    destination_owns=True,
                    listing_still_active=False,
                    error_code=None,
                    timings=self._finish_timings(timings, job_started),
                    message="Подарок куплен выбранным моим аккаунтом.",
                )
        if complete_reads and listing_active:
            return self._market_result(
                status=TonnelTransferStatus.FAILED,
                selected=selected,
                quote=quote,
                listing_sent=True,
                buy_sent=True,
                listing_status=listing_status,
                buy_status=buy_status,
                listing_fields=listing_fields,
                buy_fields=buy_fields,
                source_owns=source_owns,
                destination_owns=destination_owns,
                listing_still_active=True,
                error_code="BUY_FAILED",
                timings=self._finish_timings(timings, job_started),
                message="Размещение осталось активным; покупка не подтверждена.",
            )
        if (
            complete_reads
            and source_owns is False
            and destination_owns is False
            and sale_buyer != destination.account.telegram_account_id
        ):
            return self._market_result(
                status=TonnelTransferStatus.FAILED,
                selected=selected,
                quote=quote,
                listing_sent=True,
                buy_sent=True,
                listing_status=listing_status,
                buy_status=buy_status,
                listing_fields=listing_fields,
                buy_fields=buy_fields,
                source_owns=False,
                destination_owns=False,
                listing_still_active=False,
                error_code="EXTERNAL_SALE",
                timings=self._finish_timings(timings, job_started),
                message="Подарок был куплен другим участником рынка.",
            )
        return self._market_result(
            status=TonnelTransferStatus.AMBIGUOUS,
            selected=selected,
            quote=quote,
            listing_sent=True,
            buy_sent=True,
            listing_status=listing_status,
            buy_status=buy_status,
            listing_fields=listing_fields,
            buy_fields=buy_fields,
            source_owns=source_owns,
            destination_owns=destination_owns,
            listing_still_active=listing_active,
            error_code="BUY_AMBIGUOUS",
            timings=self._finish_timings(timings, job_started),
            message=(
                "Ответ покупки получен, но владелец ещё не подтверждён."
                if buy_reported_success
                else "Результат покупки не определён. Повторный запрос не отправлялся."
            ),
        )

    async def _get_market_inventory(
        self, session: _TonnelSession, *, listed: bool
    ) -> list[TonnelMarketGift]:
        gifts: list[TonnelMarketGift] = []
        for page in range(1, TONNEL_MAX_INVENTORY_PAGES + 1):
            filters: dict[str, Any] = {
                "seller": session.account.telegram_account_id,
                "buyer": {"$exists": False},
                "refunded": {"$ne": True},
            }
            if listed:
                filters["price"] = {"$exists": True}
            else:
                filters["price"] = {"$exists": False}
                filters["auction_id"] = {"$exists": False}
            url = self._data_url(
                session.account.telegram_account_id,
                TONNEL_MARKET_INVENTORY_PATH,
            )
            started, started_at = self._preflight_started("page_gifts", url)
            response: TonnelHttpResponse | None = None
            try:
                response = await session.transport.post_json(
                    url,
                    {
                        "page": page,
                        "limit": TONNEL_MARKET_PAGE_SIZE,
                        "sort": json.dumps(
                            {"gift_num": 1, "gift_id": -1}, separators=(",", ":")
                        ),
                        "filter": json.dumps(filters, separators=(",", ":")),
                        "ref": "",
                        "user_auth": session.auth_data,
                    },
                )
                if response.status != 200 or not isinstance(response.body, list):
                    raise TonnelInventoryUnavailableError(
                        "Не удалось загрузить обычный инвентарь Tonnel.",
                        reason=(
                            "http_error"
                            if response.status != 200
                            else "invalid_response"
                        ),
                        http_status=response.status,
                    )
                page_body = response.body
                self._preflight_succeeded("page_gifts", url, started, response)
            except Exception as exc:
                raise self._preflight_failure(
                    "page_gifts", url, started, started_at, response, exc
                ) from exc
            page_gifts = [self._parse_market_gift(item) for item in page_body]
            gifts.extend(gift for gift in page_gifts if gift is not None)
            if len(page_body) < TONNEL_MARKET_PAGE_SIZE:
                break
        return gifts

    async def _get_buy_offers_for_gift(
        self, session: _TonnelSession, gift_id: str | int
    ) -> list[TonnelBuyOffer]:
        url = self._buy_offer_read_url(
            session.account.telegram_account_id, TONNEL_BUY_OFFER_GET_PATH
        )
        started, started_at = self._preflight_started("buy_offer_get_offers", url)
        response: TonnelHttpResponse | None = None
        try:
            response = await session.transport.post_json(
                url,
                {"authData": session.auth_data, "gift_id": gift_id},
            )
            body = self._require_success(response)
            raw_offers = body.get("offers")
            if not isinstance(raw_offers, list):
                raise TonnelApiError(response.status)
            self._preflight_succeeded("buy_offer_get_offers", url, started, response)
        except Exception as exc:
            raise self._preflight_failure(
                "buy_offer_get_offers", url, started, started_at, response, exc
            ) from exc
        return [
            offer
            for offer in (self._parse_buy_offer(raw) for raw in raw_offers)
            if offer is not None
        ]

    async def _get_my_buy_offers(self, session: _TonnelSession) -> list[TonnelBuyOffer]:
        url = self._buy_offer_read_url(
            session.account.telegram_account_id, TONNEL_BUY_OFFER_GET_MINE_PATH
        )
        started, started_at = self._preflight_started("buy_offer_get_my_offers", url)
        response: TonnelHttpResponse | None = None
        try:
            response = await session.transport.post_json(
                url,
                {
                    "authData": session.auth_data,
                    "pageSize": 50,
                    "filter": {},
                    "tillTime": self._javascript_date_now(),
                },
            )
            body = self._require_success(response)
            raw_offers = body.get("offers")
            if not isinstance(raw_offers, list):
                raise TonnelApiError(response.status)
            self._preflight_succeeded("buy_offer_get_my_offers", url, started, response)
        except Exception as exc:
            raise self._preflight_failure(
                "buy_offer_get_my_offers",
                url,
                started,
                started_at,
                response,
                exc,
            ) from exc
        return [
            offer
            for offer in (self._parse_buy_offer(raw) for raw in raw_offers)
            if offer is not None
        ]

    @classmethod
    def _correlate_existing_buy_offer(
        cls,
        mine: Sequence[TonnelBuyOffer],
        for_gift: Sequence[TonnelBuyOffer],
        *,
        expected_gift_id: int,
        expected_amount: Decimal,
        expected_buyer_id: int,
        expected_seller_id: int,
        selected_seller_id: int | None,
        expected_fingerprint: str,
    ) -> tuple[TonnelBuyOffer | None, dict[str, object]]:
        mine_by_id = {
            cls._typed_id_key(offer.offer_id): offer for offer in mine
        }
        gift_by_id = {
            cls._typed_id_key(offer.offer_id): offer for offer in for_gift
        }
        common_ids = set(mine_by_id).intersection(gift_by_id)
        exact: list[tuple[TonnelBuyOffer, dict[str, str], str]] = []
        evaluations: list[tuple[TonnelBuyOffer, dict[str, str], str]] = []
        validation: dict[str, object] = {
            "candidate_count": 0,
            "semantic_candidate_count": 0,
            "common_offer_count": len(common_ids),
            "expected_fingerprint_offer_found": False,
            "offer_id_visible_in_n1": False,
            "offer_id_visible_in_n2": False,
            "offer_id_fingerprint": None,
            "offer_id_type": None,
            "fingerprint_match": False,
            "gift_match": False,
            "amount_match": False,
            "asset_match": False,
            "buyer_match": False,
            "seller_match": False,
            "status": None,
        }
        for key in common_ids:
            mine_offer = mine_by_id[key]
            gift_offer = gift_by_id[key]
            pair = (mine_offer, gift_offer)
            gift_state = cls._semantic_state(
                [item.gift_id for item in pair],
                expected_gift_id,
                typed=True,
            )
            amount_state = cls._semantic_state(
                [item.amount for item in pair], expected_amount
            )
            asset_state = cls._semantic_state(
                [item.asset for item in pair], "TON"
            )
            buyer_state = cls._semantic_state(
                [item.buyer_telegram_id for item in pair], expected_buyer_id
            )
            seller_state = cls._semantic_state(
                [
                    item.seller_telegram_id
                    for item in pair
                ]
                + [selected_seller_id],
                expected_seller_id,
            )
            status_state = cls._semantic_state(
                [item.status for item in pair], "pending"
            )
            evaluation: dict[str, str] = {
                "gift": gift_state,
                "amount": amount_state,
                "asset": asset_state,
                "buyer": buyer_state,
                "seller": seller_state,
                "status": status_state,
            }
            fingerprint = cls.fingerprint(
                f"{type(gift_offer.offer_id).__name__}:{gift_offer.offer_id}"
            )
            evaluations.append((gift_offer, evaluation, fingerprint))
            if all(state == "matched" for state in evaluation.values()):
                exact.append((gift_offer, evaluation, fingerprint))
        validation["semantic_candidate_count"] = len(exact)
        validation["candidate_count"] = len(exact)
        expected = next(
            (
                item
                for item in evaluations
                if item[2] == expected_fingerprint
            ),
            None,
        )
        validation["expected_fingerprint_offer_found"] = expected is not None
        observed = expected or (
            exact[0]
            if len(exact) == 1
            else evaluations[0]
            if len(evaluations) == 1
            else None
        )
        if observed is not None:
            offer, evaluation, fingerprint = observed
            validation.update(
                {
                    "offer_id_visible_in_n1": True,
                    "offer_id_visible_in_n2": True,
                    "offer_id_fingerprint": fingerprint,
                    "offer_id_type": type(offer.offer_id).__name__,
                    "fingerprint_match": fingerprint == expected_fingerprint,
                    "gift_match": evaluation["gift"] == "matched",
                    "amount_match": evaluation["amount"] == "matched",
                    "asset_match": evaluation["asset"] == "matched",
                    "buyer_match": evaluation["buyer"] == "matched",
                    "seller_match": evaluation["seller"] == "matched",
                    "status": offer.status,
                    "match_diagnostics": evaluation,
                }
            )
        else:
            validation["match_diagnostics"] = {
                "gift": "candidate_not_found",
                "amount": "candidate_not_found",
                "asset": "candidate_not_found",
                "buyer": "candidate_not_found",
                "seller": "candidate_not_found",
                "status": "candidate_not_found",
            }
        if len(exact) != 1:
            return None, validation
        exact_offer, _, exact_fingerprint = exact[0]
        if exact_fingerprint != expected_fingerprint:
            return None, validation
        return exact_offer, validation

    @classmethod
    def _semantic_state(
        cls,
        values: Sequence[object | None],
        expected: object,
        *,
        typed: bool = False,
    ) -> str:
        present = [value for value in values if value is not None]
        if not present:
            return "field_missing"
        matches = [
            cls._typed_equal(value, expected) if typed else value == expected
            for value in present
        ]
        return "matched" if all(matches) else "field_present_but_mismatch"

    async def _verify_existing_offer_acceptance(
        self,
        source: _TonnelSession,
        destination: _TonnelSession,
        selected: TonnelMarketGift,
        offer: TonnelBuyOffer,
    ) -> tuple[bool | None, bool | None, bool | None, bool]:
        source_owns: bool | None = None
        destination_owns: bool | None = None
        offer_active: bool | None = None
        complete_reads = False
        for delay in (0.0, 0.5, 1.5):
            if delay:
                await asyncio.sleep(delay)
            try:
                source_gifts, destination_gifts, offers = await asyncio.gather(
                    self._get_market_inventory(source, listed=False),
                    self._get_market_inventory(destination, listed=False),
                    self._get_buy_offers_for_gift(source, selected.sale_id),
                )
            except TonnelServiceError:
                continue
            complete_reads = True
            source_owns = self._has_exact_market_identity(
                source_gifts, selected.identity
            )
            destination_owns = self._has_exact_market_identity(
                destination_gifts, selected.identity
            )
            offer_active = any(
                self._typed_equal(candidate.offer_id, offer.offer_id)
                and candidate.status in TONNEL_BUY_OFFER_STATUS_ACTIVE
                for candidate in offers
            )
            if destination_owns and not source_owns and not offer_active:
                break
        return source_owns, destination_owns, offer_active, complete_reads

    @staticmethod
    def _offer_age_ms(created_at: str | None) -> int | None:
        if created_at is None:
            return None
        try:
            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError:
            return None
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        age_seconds = (
            datetime.now(timezone.utc) - created.astimezone(timezone.utc)
        ).total_seconds()
        return max(0, round(age_seconds * 1000))

    async def _reconcile_exact_buy_offer(
        self,
        source: _TonnelSession,
        destination: _TonnelSession,
        selected: TonnelMarketGift,
        quote: TonnelBuyOfferQuote,
        *,
        expected_offer_id: str | int | None,
        pre_create_offer_ids: frozenset[tuple[str, str]],
    ) -> tuple[TonnelBuyOffer | None, dict[str, object]]:
        api_gift_id = self._buy_offer_gift_id(selected)
        correlation: dict[str, object] = {
            "pre_create_offer_count": len(pre_create_offer_ids),
            "post_create_offer_count": None,
            "new_offer_count": None,
            "new_offer_id_fingerprint": None,
            "new_offer_id_type": None,
            "unique_new_offer_proven": False,
            "semantic_match_count": 0,
        }
        for delay in (0.0, 0.3, 0.7):
            if delay:
                await asyncio.sleep(delay)
            try:
                source_gifts, mine, for_gift = await asyncio.gather(
                    self._get_market_inventory(source, listed=False),
                    self._get_my_buy_offers(destination),
                    self._get_buy_offers_for_gift(source, selected.sale_id),
                )
                current = self._find_exact_market_gift(source_gifts, selected.identity)
            except TonnelServiceError:
                continue
            if (
                current.seller_telegram_id is not None
                and current.seller_telegram_id != source.account.telegram_account_id
            ):
                continue
            mine_matches = [
                offer
                for offer in mine
                if self._buy_offer_matches(
                    offer,
                    expected_offer_id=expected_offer_id,
                    gift_id=api_gift_id,
                    amount=quote.offer_amount,
                    buyer_id=destination.account.telegram_account_id,
                    seller_id=source.account.telegram_account_id,
                )
            ]
            gift_matches = [
                offer
                for offer in for_gift
                if self._buy_offer_matches(
                    offer,
                    expected_offer_id=expected_offer_id,
                    gift_id=api_gift_id,
                    amount=quote.offer_amount,
                    buyer_id=destination.account.telegram_account_id,
                    seller_id=source.account.telegram_account_id,
                )
            ]
            mine_by_id = {
                self._typed_id_key(offer.offer_id): offer for offer in mine_matches
            }
            gift_by_id = {
                self._typed_id_key(offer.offer_id): offer for offer in gift_matches
            }
            post_scope_ids = {
                self._typed_id_key(offer.offer_id)
                for offer in (*mine, *for_gift)
                if self._typed_equal(offer.gift_id, api_gift_id)
            }
            new_ids = post_scope_ids - pre_create_offer_ids
            common_semantic_ids = set(mine_by_id).intersection(gift_by_id)
            if expected_offer_id is None:
                candidate_ids = common_semantic_ids.intersection(new_ids)
            else:
                expected_key = self._typed_id_key(expected_offer_id)
                candidate_ids = common_semantic_ids.intersection({expected_key})
            correlation.update(
                {
                    "post_create_offer_count": len(post_scope_ids),
                    "new_offer_count": len(new_ids),
                    "semantic_match_count": len(candidate_ids),
                    "unique_new_offer_proven": (
                        len(candidate_ids) == 1 and next(iter(candidate_ids)) in new_ids
                    ),
                }
            )
            if len(candidate_ids) == 1:
                candidate = gift_by_id[next(iter(candidate_ids))]
                correlation.update(
                    {
                        "new_offer_id_fingerprint": self.fingerprint(
                            f"{type(candidate.offer_id).__name__}:{candidate.offer_id}"
                        ),
                        "new_offer_id_type": type(candidate.offer_id).__name__,
                    }
                )
                return candidate, correlation
            if len(candidate_ids) > 1:
                return None, correlation
        return None, correlation

    async def _reconcile_buy_offer_transfer(
        self,
        source: _TonnelSession,
        destination: _TonnelSession,
        selected: TonnelMarketGift,
        offer: TonnelBuyOffer,
        quote: TonnelBuyOfferQuote,
        *,
        create_result: TonnelMutationResult,
        accept_result: TonnelMutationResult,
        accept_reported_success: bool,
        accept_finished: float,
        timings: dict[str, int | None],
        job_started: float,
        accept_request_metadata: Mapping[str, object],
        offer_correlation: Mapping[str, object],
        create_request_metadata: Mapping[str, object],
    ) -> TonnelBuyOfferTransferResult:
        source_owns: bool | None = None
        destination_owns: bool | None = None
        offer_active: bool | None = None
        complete_reads = False
        verification: dict[str, object] = {
            "timeout_ms": self._ownership_verify_timeout_ms,
            "probe_count": 0,
            "first_probe_ms": None,
            "confirmed_ms": None,
            "source_owns_after": None,
            "destination_owns_after": None,
        }
        probe_count = 0
        if accept_reported_success:
            scheduled_delays_ms: list[int] = [0]
            remaining_ms = self._ownership_verify_timeout_ms
            while remaining_ms > 0:
                delay_ms = min(
                    TONNEL_OWNERSHIP_VERIFY_POLL_INTERVAL_MS, remaining_ms
                )
                scheduled_delays_ms.append(delay_ms)
                remaining_ms -= delay_ms
        else:
            scheduled_delays_ms = [0, 500, 1_500]
        for delay_ms in scheduled_delays_ms:
            if delay_ms:
                await asyncio.sleep(delay_ms / 1_000)
            probe_started = time.monotonic()
            probe_count += 1
            verification["probe_count"] = probe_count
            if verification["first_probe_ms"] is None:
                verification["first_probe_ms"] = self._milliseconds(
                    accept_finished, probe_started
                )
            try:
                source_gifts, destination_gifts, offers = await asyncio.gather(
                    self._get_market_inventory(source, listed=False),
                    self._get_market_inventory(destination, listed=False),
                    self._get_buy_offers_for_gift(source, selected.sale_id),
                )
            except TonnelServiceError:
                continue
            complete_reads = True
            source_owns = self._has_exact_market_identity(
                source_gifts, selected.identity
            )
            destination_owns = self._has_exact_market_identity(
                destination_gifts, selected.identity
            )
            offer_active = any(
                self._typed_equal(candidate.offer_id, offer.offer_id)
                and candidate.status in TONNEL_BUY_OFFER_STATUS_ACTIVE
                for candidate in offers
            )
            verification["source_owns_after"] = source_owns
            verification["destination_owns_after"] = destination_owns
            if destination_owns and not source_owns:
                ownership_confirmed_at = time.monotonic()
                confirmed_ms = self._milliseconds(
                    accept_finished, ownership_confirmed_at
                )
                verification["confirmed_ms"] = confirmed_ms
                timings["accept_success_to_ownership_confirmed_ms"] = (
                    confirmed_ms if accept_reported_success else None
                )
                return self._buy_offer_result(
                    status=TonnelTransferStatus.SUCCESS,
                    selected=selected,
                    offer_id=offer.offer_id,
                    quote=quote,
                    create_result=create_result,
                    accept_result=accept_result,
                    source_owns=False,
                    destination_owns=True,
                    offer_active=False,
                    error_code=None,
                    timings=self._finish_timings(timings, job_started),
                    message="Точный оффер принят, подарок получен выбранным аккаунтом.",
                    accept_request_metadata=accept_request_metadata,
                    offer_correlation=offer_correlation,
                    ownership_verification=verification,
                    create_request_metadata=create_request_metadata,
                )
        if accept_reported_success:
            return self._buy_offer_result(
                status=TonnelTransferStatus.AMBIGUOUS,
                selected=selected,
                offer_id=offer.offer_id,
                quote=quote,
                create_result=create_result,
                accept_result=accept_result,
                source_owns=source_owns,
                destination_owns=destination_owns,
                offer_active=offer_active,
                error_code="OWNERSHIP_VERIFICATION_TIMEOUT",
                timings=self._finish_timings(timings, job_started),
                message=(
                    "Принятие подтверждено Tonnel, владение ещё не успело обновиться."
                ),
                accept_request_metadata=accept_request_metadata,
                offer_correlation=offer_correlation,
                ownership_verification=verification,
                create_request_metadata=create_request_metadata,
            )
        if complete_reads and source_owns:
            return self._buy_offer_result(
                status=TonnelTransferStatus.FAILED,
                selected=selected,
                offer_id=offer.offer_id,
                quote=quote,
                create_result=create_result,
                accept_result=accept_result,
                source_owns=True,
                destination_owns=False,
                offer_active=offer_active,
                error_code="OFFER_NOT_ACCEPTED",
                timings=self._finish_timings(timings, job_started),
                message=(
                    accept_result.message
                    or "Подарок остался у исходного аккаунта; оффер не принят."
                ),
                accept_request_metadata=accept_request_metadata,
                offer_correlation=offer_correlation,
                ownership_verification=verification,
                create_request_metadata=create_request_metadata,
            )
        return self._buy_offer_result(
            status=TonnelTransferStatus.AMBIGUOUS,
            selected=selected,
            offer_id=offer.offer_id,
            quote=quote,
            create_result=create_result,
            accept_result=accept_result,
            source_owns=source_owns,
            destination_owns=destination_owns,
            offer_active=offer_active,
            error_code="OFFER_ACCEPT_AMBIGUOUS",
            timings=self._finish_timings(timings, job_started),
            message=(
                "Tonnel сообщил об успехе, но владелец ещё не подтверждён."
                if accept_reported_success
                else "Результат принятия оффера не определён. Повтора не было."
            ),
            accept_request_metadata=accept_request_metadata,
            offer_correlation=offer_correlation,
            ownership_verification=verification,
            create_request_metadata=create_request_metadata,
        )

    async def _get_sale(
        self, session: _TonnelSession, sale_id: str
    ) -> TonnelMarketGift | None:
        response = await session.transport.post_json(
            self._data_url(
                session.account.telegram_account_id,
                f"{TONNEL_GIFT_DATA_PATH}/{sale_id}",
            ),
            {"ref": "", "authData": session.auth_data},
        )
        if response.status != 200:
            raise TonnelApiError(response.status)
        body = self._mapping(response.body)
        if body.get("status") == "error" or not body:
            return None
        return self._parse_market_gift(body, requested_sale_id=sale_id)

    async def _find_reconciled_listing(
        self,
        source: _TonnelSession,
        selected: TonnelMarketGift,
        quote: TonnelMarketQuote,
    ) -> TonnelMarketGift | None:
        for delay in (0.0, 0.3, 0.7):
            if delay:
                await asyncio.sleep(delay)
            try:
                candidate = await self._get_sale(source, selected.sale_id)
            except TonnelServiceError:
                continue
            if candidate is None:
                continue
            if (
                candidate.sale_id == selected.sale_id
                and candidate.identity.collectible_slug
                == selected.identity.collectible_slug
                and candidate.listing_price == quote.seller_price
                and candidate.status in {"forsale", "active", "pending"}
                and (
                    candidate.seller_telegram_id is None
                    or candidate.seller_telegram_id
                    == source.account.telegram_account_id
                )
            ):
                return candidate
        return None

    @classmethod
    def build_list_for_sale_payload(
        cls,
        auth_data: str,
        gift: TonnelMarketGift,
        seller_price: str | Decimal,
        *,
        timestamp: int | None = None,
    ) -> Mapping[str, Any]:
        normalized = cls.format_market_price(seller_price)
        request_timestamp = int(time.time()) if timestamp is None else timestamp
        return {
            "authData": auth_data,
            "gift_id": gift.sale_id,
            "price": normalized,
            "asset": "TON",
            "timestamp": request_timestamp,
            "wtf": cls._encrypt_timestamp(str(request_timestamp)),
        }

    @classmethod
    def build_buy_payload(
        cls,
        auth_data: str,
        gift: TonnelMarketGift,
        seller_price: str | Decimal,
        *,
        timestamp: int | None = None,
    ) -> Mapping[str, Any]:
        request_timestamp = int(time.time()) if timestamp is None else timestamp
        return {
            "authData": auth_data,
            "asset": gift.asset,
            # The official frontend sends the listing's seller price here;
            # buyer commission is calculated separately for the balance charge.
            "price": cls.normalize_market_price(seller_price),
            "timestamp": request_timestamp,
            "wtf": cls._encrypt_timestamp(str(request_timestamp)),
        }

    @classmethod
    def build_buy_offer_create_payload(
        cls,
        auth_data: str,
        gift_id: str | int,
        offer_amount: str | Decimal,
    ) -> Mapping[str, Any]:
        if not isinstance(gift_id, int) or isinstance(gift_id, bool):
            raise TypeError("Tonnel buy-offer gift_id must be an integer")
        return {
            "authData": auth_data,
            "gift_id": gift_id,
            "amount": cls.normalize_market_price(offer_amount),
            "asset": "TON",
        }

    @classmethod
    def build_buy_offer_accept_payload(
        cls, auth_data: str, offer_id: str | int
    ) -> Mapping[str, Any]:
        if not cls._valid_opaque_id(offer_id):
            raise ValueError("Invalid Tonnel offer_id")
        return {"authData": auth_data, "offer_id": offer_id}

    @classmethod
    def build_buy_offer_cancel_payload(
        cls, auth_data: str, offer_id: str | int
    ) -> Mapping[str, Any]:
        return cls.build_buy_offer_accept_payload(auth_data, offer_id)

    @classmethod
    def _assert_accept_context(cls, session: _TonnelSession) -> None:
        expected = session.account.telegram_account_id
        if session.authenticated_telegram_id != expected:
            raise TonnelAuthenticationError(
                "Контекст принятия оффера принадлежит другому аккаунту."
            )

    def _accept_request_metadata(
        self,
        session: _TonnelSession,
        offer: TonnelBuyOffer,
        *,
        n1_session: _TonnelSession,
        offer_id_source: str,
    ) -> Mapping[str, object]:
        expected_fingerprint = self.fingerprint(
            f"telegram:{session.account.telegram_account_id}"
        )
        authenticated_fingerprint = self.fingerprint(
            f"telegram:{session.authenticated_telegram_id}"
        )
        return {
            "endpoint": TONNEL_BUY_OFFER_ACCEPT_PATH,
            "method": "POST",
            "body_fields": ["authData", "offer_id"],
            "offer_id_source": offer_id_source,
            "offer_id_type": type(offer.offer_id).__name__,
            "offer_id_python_type": type(offer.offer_id).__name__,
            "offer_id_frontend_expected_type": type(offer.offer_id).__name__,
            "offer_id_fingerprint": self.fingerprint(
                f"{type(offer.offer_id).__name__}:{offer.offer_id}"
            ),
            "authenticated_account_fingerprint": authenticated_fingerprint,
            "expected_n2_fingerprint": expected_fingerprint,
            "identity_match": authenticated_fingerprint == expected_fingerprint,
            "context_shared_with_n1": session.auth_data == n1_session.auth_data,
            "auth_context_created_at": session.auth_created_at,
            "context_prepared_for_n2": True,
            "auth_refreshed_immediately_before_accept": False,
            "content_type_set": True,
            "origin_set": True,
            "referer_set": True,
            "credentials_mode": "same-origin",
            "cookies_sent": False,
            "api_origin": self._api_origin,
        }

    def _create_request_metadata(
        self,
        *,
        gift_id: str | int | None = None,
        offer_amount: Decimal | None = None,
        mutation_guard_key: str | None = None,
        child_job_id: int | None = None,
        task_name: str | None = None,
        request_started_at: str | None = None,
    ) -> Mapping[str, object]:
        metadata: dict[str, object] = {
            "endpoint": TONNEL_BUY_OFFER_CREATE_PATH,
            "method": "POST",
            "body_fields": ["authData", "gift_id", "amount", "asset"],
            "api_origin": self._api_origin,
        }
        if gift_id is not None:
            metadata.update(
                {
                    "gift_id": gift_id,
                    "gift_id_type": type(gift_id).__name__,
                    "amount": self.format_market_price(offer_amount)
                    if offer_amount is not None
                    else None,
                    "caller": (
                        "TonnelTransferJobService._run_buy_offer"
                        if child_job_id is not None
                        else "TonnelService.transfer_buy_offer"
                    ),
                    "child_job_id": child_job_id,
                    "asyncio_task": task_name,
                    "request_started_at": request_started_at,
                    "response_received_at": None,
                    "http_transmission_count": 0,
                    "mutation_guard_fingerprint": (
                        self.fingerprint(mutation_guard_key)
                        if mutation_guard_key is not None
                        else None
                    ),
                }
            )
        return metadata

    async def _enter_buy_offer_create_once(
        self, mutation_guard_key: str, gift_id: str | int
    ) -> None:
        guard = hashlib.sha256(
            f"{mutation_guard_key}:{type(gift_id).__name__}:{gift_id}".encode()
        ).hexdigest()
        async with self._buy_offer_create_guard:
            if guard in self._entered_buy_offer_creates:
                raise TonnelDuplicateMutationError(
                    "Повторная отправка CREATE для этого задания заблокирована."
                )
            self._entered_buy_offer_creates.add(guard)

    @classmethod
    def _preflight_started(
        cls, operation: str, url: str, *, method: str = "POST"
    ) -> tuple[float, str]:
        parsed = urlsplit(url)
        logger.info(
            "Tonnel preflight started operation=%s hostname=%s endpoint=%s method=%s",
            operation,
            parsed.hostname or "unknown",
            parsed.path,
            method,
        )
        return time.monotonic(), cls._javascript_date_now()

    @classmethod
    def _preflight_succeeded(
        cls,
        operation: str,
        url: str,
        started: float,
        response: TonnelHttpResponse | None,
        *,
        method: str = "POST",
    ) -> None:
        parsed = urlsplit(url)
        logger.info(
            "Tonnel preflight succeeded operation=%s hostname=%s endpoint=%s "
            "method=%s duration_ms=%d http_status=%s",
            operation,
            parsed.hostname or "unknown",
            parsed.path,
            method,
            cls._milliseconds(started, time.monotonic()),
            response.status if response is not None else None,
        )

    @classmethod
    def _preflight_failure(
        cls,
        operation: str,
        url: str,
        started: float,
        started_at: str,
        response: TonnelHttpResponse | None,
        exc: Exception,
        *,
        method: str = "POST",
    ) -> TonnelPreflightFailure:
        parsed = urlsplit(url)
        http_status = response.status if response is not None else None
        root_exc = _root_exception(exc)
        failure_code: str | None = getattr(exc, "failure_code", None)
        stage = operation
        if isinstance(exc, MiniAppSessionMissingError):
            failure_code = "TELEGRAM_SESSION_MISSING"
            stage = "telegram_session"
        elif isinstance(exc, MiniAppSessionUnauthorizedError):
            failure_code = "TELEGRAM_SESSION_UNAUTHORIZED"
            stage = "telegram_authorization"
        elif isinstance(exc, MiniAppSessionConnectionError):
            failure_code = "TELEGRAM_CONNECT_FAILED"
            stage = "telegram_connection"
        elif isinstance(exc, MiniAppSessionIdentityMismatchError):
            failure_code = "TELEGRAM_IDENTITY_MISMATCH"
            stage = "telegram_identity"
        elif isinstance(exc, MiniAppAccountNotFoundError):
            failure_code = "TELEGRAM_ACCOUNT_NOT_FOUND"
            stage = "account_lookup"
        elif isinstance(exc, (MiniAppBotResolutionError, MiniAppWebViewError)):
            failure_code = "TONNEL_WEBVIEW_FAILED"
            stage = "tonnel_webview"
        elif operation == "session_auth" and http_status not in {None, 200}:
            failure_code = "TONNEL_AUTH_HTTP_ERROR"
            stage = "tonnel_session_exchange"
        elif operation == "session_auth" and isinstance(
            exc, (TonnelAuthenticationError, TonnelApiError)
        ):
            failure_code = "TONNEL_AUTH_APPLICATION_ERROR"
            stage = "tonnel_session_exchange"
        if failure_code == "TONNEL_INITDATA_MISSING":
            stage = "tonnel_init_data"

        if isinstance(exc, TonnelNetworkError):
            failure_kind = exc.failure_kind
            exception_type = exc.exception_type or type(exc).__name__
            message = {
                "timeout": "Время ожидания ответа Tonnel истекло.",
                "dns": "Не удалось определить адрес сервера Tonnel.",
                "connect": "Не удалось подключиться к серверу Tonnel.",
                "tls": "Не удалось установить защищённое соединение.",
            }.get(failure_kind, "Ошибка сети при обращении к Tonnel.")
        elif http_status is not None and http_status != 200:
            failure_kind = "http"
            exception_type = type(root_exc).__name__
            message = str(exc) if isinstance(exc, TonnelServiceError) else "HTTP error"
        elif isinstance(
            exc,
            (
                TonnelApiError,
                TonnelAuthenticationError,
                TonnelInventoryUnavailableError,
            ),
        ):
            failure_kind = "application"
            exception_type = type(root_exc).__name__
            message = str(exc)
        elif isinstance(root_exc, sqlite3.OperationalError):
            failure_kind = "sqlite_session"
            exception_type = type(root_exc).__name__
            message = "Ошибка локального хранилища Telegram-сессии."
        else:
            failure_kind = _network_failure_kind(exc)
            exception_type = type(root_exc).__name__
            message = {
                "timeout": "Время ожидания ответа истекло.",
                "dns": "Ошибка определения адреса сервера.",
                "connect": "Ошибка подключения к серверу.",
                "tls": "Ошибка защищённого соединения.",
            }.get(failure_kind, "Не удалось выполнить проверку Tonnel.")
        diagnostic: dict[str, object] = {
            "stage": stage,
            "operation": operation,
            "api_origin": (
                f"{parsed.scheme}://{parsed.netloc}"
                if parsed.netloc
                else "telegram_mtproto"
            ),
            "hostname": parsed.hostname or "telegram",
            "endpoint": parsed.path or url,
            "method": method,
            "started_at": started_at,
            "duration_ms": cls._milliseconds(started, time.monotonic()),
            "http_status": http_status,
            "exception_type": exception_type,
            "message": message[:160],
            "failure_kind": failure_kind,
        }
        if failure_code is not None:
            diagnostic["failure_code"] = failure_code
        if isinstance(root_exc, sqlite3.OperationalError):
            diagnostic["underlying_exception_category"] = "sqlite_operational"
            diagnostic["underlying_exception_message"] = (
                _safe_sqlite_operational_message(root_exc)
            )
        logger.warning(
            "Tonnel preflight failed operation=%s hostname=%s endpoint=%s "
            "method=%s duration_ms=%s http_status=%s exception=%s kind=%s "
            "underlying_category=%s underlying_message=%s",
            diagnostic["operation"],
            diagnostic["hostname"],
            diagnostic["endpoint"],
            diagnostic["method"],
            diagnostic["duration_ms"],
            diagnostic["http_status"],
            diagnostic["exception_type"],
            diagnostic["failure_kind"],
            diagnostic.get("underlying_exception_category"),
            diagnostic.get("underlying_exception_message"),
        )
        error_code = (
            "INVENTORY_UNAVAILABLE"
            if isinstance(exc, TonnelInventoryUnavailableError)
            else "NETWORK_ERROR"
        )
        return TonnelPreflightFailure(
            message,
            error_code=error_code,
            diagnostic=diagnostic,
        )

    @classmethod
    def buy_offer_quote(cls, offer_amount: str | Decimal) -> TonnelBuyOfferQuote:
        amount = cls.normalize_market_price(offer_amount)
        proceeds = (amount * (Decimal(1) - TONNEL_BUY_OFFER_SELLER_FEE_RATE)).quantize(
            Decimal("0.001"), rounding=ROUND_HALF_UP
        )
        seller_fee = amount - proceeds
        required = amount + TONNEL_BUY_OFFER_CREATE_FEE
        return TonnelBuyOfferQuote(
            offer_amount=amount,
            seller_proceeds=proceeds,
            seller_fee=seller_fee,
            create_fee=TONNEL_BUY_OFFER_CREATE_FEE,
            required_buyer_balance=required,
            offer_amount_nanotons=cls.ton_to_nanotons(amount),
            seller_proceeds_nanotons=cls.ton_to_nanotons(proceeds),
            seller_fee_nanotons=cls.ton_to_nanotons(seller_fee),
            create_fee_nanotons=cls.ton_to_nanotons(TONNEL_BUY_OFFER_CREATE_FEE),
            required_buyer_nanotons=cls.ton_to_nanotons(required),
        )

    @classmethod
    def market_quote(
        cls, seller_price: str | Decimal, tonnel_balance: str | Decimal = "0"
    ) -> TonnelMarketQuote:
        seller = cls.normalize_market_price(seller_price)
        holdings = cls._decimal(tonnel_balance)
        cashback = cls._cashback_fraction(holdings)
        fee_rate = Decimal("0.005") * (Decimal(1) - cashback)
        buyer = seller * (Decimal(1) + fee_rate)
        return TonnelMarketQuote(
            seller_price=seller,
            buyer_price=buyer,
            seller_proceeds=seller,
            fee_rate=fee_rate,
            cashback_fraction=cashback,
            seller_price_nanotons=cls.ton_to_nanotons(seller),
            buyer_price_nanotons=cls.ton_to_nanotons(buyer),
        )

    @classmethod
    def normalize_market_price(cls, value: str | Decimal) -> Decimal:
        amount = cls.normalize_ton(value)
        exponent = amount.as_tuple().exponent
        if not isinstance(exponent, int):
            raise TypeError("Invalid Decimal exponent")
        decimal_places = max(0, -exponent)
        if decimal_places > 3 or not (
            TONNEL_MIN_MARKET_PRICE <= amount <= TONNEL_MAX_MARKET_PRICE
        ):
            raise ValueError("Invalid Tonnel market price")
        return amount

    @classmethod
    def format_market_price(cls, value: str | Decimal) -> str:
        normalized = cls.normalize_market_price(value)
        text = format(normalized, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text

    @classmethod
    def ton_to_nanotons(cls, value: str | Decimal) -> int:
        amount = cls.normalize_ton(value)
        atomic = amount * TONNEL_ATOMIC_FACTOR
        if atomic != atomic.to_integral_value():
            raise ValueError("TON amount exceeds nanoton precision")
        return int(atomic)

    @staticmethod
    def _cashback_fraction(tonnel_balance: Decimal) -> Decimal:
        for minimum, fraction in (
            (Decimal(6666), Decimal(1)),
            (Decimal(3369), Decimal("0.5")),
            (Decimal(1123), Decimal("0.3")),
            (Decimal(669), Decimal("0.1")),
        ):
            if tonnel_balance >= minimum:
                return fraction
        return Decimal(0)

    async def _get_inventory(self, session: _TonnelSession) -> list[TonnelGift]:
        gifts: list[TonnelGift] = []
        for page in range(1, TONNEL_MAX_INVENTORY_PAGES + 1):
            response = await session.transport.post_json(
                self._data_url(
                    session.account.telegram_account_id, TONNEL_INVENTORY_PATH
                ),
                {
                    "page": page,
                    "authData": session.auth_data,
                    "sort": '{"gift_num":1}',
                    "filter": "{}",
                },
            )
            body = self._require_success(response, inventory=True)
            raw_items = body.get("data")
            if not isinstance(raw_items, list):
                raise TonnelInventoryUnavailableError(
                    "Tonnel вернул неизвестный формат списка подарков."
                )
            page_gifts = [self._parse_gift(item) for item in raw_items]
            gifts.extend(gift for gift in page_gifts if gift is not None)
            if len(raw_items) < TONNEL_PAGE_SIZE:
                break
        return gifts

    async def _get_balance(self, session: _TonnelSession) -> TonnelBalance:
        url = self._data_url(session.account.telegram_account_id, TONNEL_BALANCE_PATH)
        started, started_at = self._preflight_started("balance_info", url)
        response: TonnelHttpResponse | None = None
        try:
            response = await session.transport.post_json(
                url,
                {"authData": session.auth_data, "ref": ""},
            )
            body = self._require_success(response)
            self._preflight_succeeded("balance_info", url, started, response)
        except Exception as exc:
            raise self._preflight_failure(
                "balance_info", url, started, started_at, response, exc
            ) from exc
        return TonnelBalance(
            ton=self._decimal(body.get("balance")),
            tonnel=self._decimal(body.get("tonnelBalance")),
            usdt=self._decimal(body.get("usdtBalance")),
            direct_transfer_enabled=(
                body.get("transferGift")
                if isinstance(body.get("transferGift"), bool)
                else None
            ),
        )

    async def _get_direct_transfer_fee(self, session: _TonnelSession) -> Decimal:
        response = await session.transport.post_json(
            self._data_url(
                session.account.telegram_account_id, TONNEL_RETURN_STATS_PATH
            ),
            {"authData": session.auth_data},
        )
        body = self._require_success(response)
        data = self._mapping(body.get("data"))
        total_returns = self._integer(data.get("totalReturns")) or 0
        return (
            TONNEL_DIRECT_TRANSFER_FEE_HIGH
            if total_returns >= TONNEL_DIRECT_TRANSFER_HIGH_FEE_THRESHOLD
            else TONNEL_DIRECT_TRANSFER_FEE_BASE
        )

    async def _resolve_recipient(
        self, session: _TonnelSession, telegram_user_id: int
    ) -> TonnelRecipient:
        response = await session.transport.post_json(
            f"{TONNEL_GIFTS2_ORIGIN}{TONNEL_USER_INFO_PATH}",
            {"authData": session.auth_data, "user": str(telegram_user_id)},
        )
        body = self._require_success(response)
        data = self._mapping(body.get("data"))
        resolved_id = self._integer(data.get("userId"))
        if resolved_id is None:
            raise TonnelRecipientMismatchError("Получатель не найден в Tonnel.")
        return TonnelRecipient(
            telegram_user_id=resolved_id,
            display_name=str(data.get("name") or "Получатель")[:80],
            username=(
                str(data["username"])[:64]
                if isinstance(data.get("username"), str) and data["username"]
                else None
            ),
        )

    @classmethod
    def _parse_gift(cls, raw: Any) -> TonnelGift | None:
        item = cls._mapping(raw)
        nested = cls._mapping(item.get("gift"))
        owned_id = item.get("owned_gift_id")
        if not cls._valid_opaque_id(owned_id):
            return None
        assert isinstance(owned_id, (str, int)) and not isinstance(owned_id, bool)
        number = nested.get("number")
        name = nested.get("base_name") or nested.get("name")
        if (
            not isinstance(name, str)
            or not name.strip()
            or not cls._valid_opaque_id(number)
        ):
            return None
        slug_name = name.replace("'", "").replace("-", "").replace(" ", "")
        slug = f"{slug_name}-{number}"
        transfer_at = cls._integer(item.get("next_transfer_date"))
        eligible = transfer_at is None or transfer_at <= int(time.time())
        display_name = f"{name.strip()} #{number}"
        return TonnelGift(
            display_name=display_name[:80],
            identity=TonnelGiftIdentity(
                owned_gift_id=owned_id,
                collectible_slug=slug,
            ),
            eligible=eligible,
            eligibility_reason=(
                None if eligible else "Подарок временно заблокирован для передачи."
            ),
            serial_number=number,
            next_transfer_at=transfer_at,
        )

    @classmethod
    def _parse_market_gift(
        cls, raw: Any, *, requested_sale_id: str | None = None
    ) -> TonnelMarketGift | None:
        item = cls._mapping(raw)
        source_id = item.get("gift_id", requested_sale_id)
        if not cls._valid_opaque_id(source_id):
            return None
        assert isinstance(source_id, (str, int)) and not isinstance(source_id, bool)
        sale_id = requested_sale_id or str(source_id)
        number = item.get("gift_num", item.get("num"))
        name = item.get("name") or item.get("gift_name")
        if (
            not isinstance(name, str)
            or not name.strip()
            or not cls._valid_opaque_id(number)
            or not sale_id
        ):
            return None
        slug_name = name.replace("'", "").replace("-", "").replace(" ", "")
        slug = f"{slug_name}-{number}"
        status = item.get("status") if isinstance(item.get("status"), str) else None
        raw_price = item.get("price")
        listing_price = (
            cls._decimal_or_none(raw_price) if raw_price is not None else None
        )
        limited = bool(item.get("limited")) or (
            isinstance(item.get("cancelCounter"), int) and item["cancelCounter"] >= 10
        )
        bundled = sale_id.startswith("-")
        eligible = not limited and not bundled and not bool(item.get("underLoan"))
        reason = None
        if limited:
            reason = "Tonnel ограничил повторное размещение этого подарка."
        elif bundled:
            reason = "Пакетный объект нельзя передать как один подарок."
        elif bool(item.get("underLoan")):
            reason = "Подарок находится в залоге."
        return TonnelMarketGift(
            display_name=f"{name.strip()} #{number}"[:80],
            identity=TonnelMarketGiftIdentity(
                source_gift_id=source_id,
                sale_id=sale_id,
                collectible_slug=slug,
            ),
            eligible=eligible,
            eligibility_reason=reason,
            seller_telegram_id=cls._integer(item.get("seller")),
            buyer_telegram_id=cls._integer(item.get("buyer")),
            asset=(
                item["asset"]
                if isinstance(item.get("asset"), str) and item["asset"]
                else "TON"
            ),
            listing_price=listing_price,
            status=status,
        )

    @classmethod
    def _parse_buy_offer(cls, raw: Any) -> TonnelBuyOffer | None:
        item = cls._mapping(raw)
        offer_id = item.get("offer_id")
        gift_id = item.get("gift_id")
        amount = cls._decimal_or_none(item.get("price", item.get("amount")))
        asset = item.get("asset")
        status = item.get("status")
        if (
            not cls._valid_opaque_id(offer_id)
            or not cls._valid_opaque_id(gift_id)
            or amount is None
            or amount <= 0
            or not isinstance(asset, str)
            or not asset
            or not isinstance(status, str)
            or not status
        ):
            return None
        assert isinstance(offer_id, (str, int)) and not isinstance(offer_id, bool)
        assert isinstance(gift_id, (str, int)) and not isinstance(gift_id, bool)
        created = item.get("createdAt", item.get("created_at"))
        return TonnelBuyOffer(
            offer_id=offer_id,
            gift_id=gift_id,
            amount=amount,
            asset=asset,
            status=status.lower(),
            buyer_telegram_id=cls._integer(item.get("buyer")),
            seller_telegram_id=cls._integer(item.get("seller")),
            created_at=created if isinstance(created, str) else None,
        )

    @classmethod
    def _buy_offer_gift_id(cls, gift: TonnelMarketGift) -> int:
        try:
            gift_id = int(gift.sale_id)
        except ValueError as exc:
            raise TonnelGiftUnavailableError(
                "Tonnel не вернул numeric gift_id для оффера."
            ) from exc
        if gift_id <= 0 or not cls._typed_equal(gift.gift_id, gift_id):
            raise TonnelGiftUnavailableError(
                "Тип идентификатора подарка не соответствует offer API Tonnel."
            )
        return gift_id

    @classmethod
    def _buy_offer_matches(
        cls,
        offer: TonnelBuyOffer,
        *,
        expected_offer_id: str | int | None,
        gift_id: int,
        amount: Decimal,
        buyer_id: int,
        seller_id: int,
    ) -> bool:
        return (
            (
                expected_offer_id is None
                or cls._typed_equal(offer.offer_id, expected_offer_id)
            )
            and cls._typed_equal(offer.gift_id, gift_id)
            and offer.amount == amount
            and offer.asset == "TON"
            and offer.status in TONNEL_BUY_OFFER_STATUS_ACTIVE
            and (offer.buyer_telegram_id is None or offer.buyer_telegram_id == buyer_id)
            and (
                offer.seller_telegram_id is None
                or offer.seller_telegram_id == seller_id
            )
        )

    @classmethod
    def _extract_offer_id(cls, body: Mapping[str, Any]) -> str | int | None:
        data = cls._mapping(body.get("data"))
        for candidate in (body.get("offer_id"), data.get("offer_id")):
            if cls._valid_opaque_id(candidate):
                assert isinstance(candidate, (str, int)) and not isinstance(
                    candidate, bool
                )
                return candidate
        return None

    @staticmethod
    def _typed_id_key(value: str | int) -> tuple[str, str]:
        return type(value).__name__, str(value)

    @classmethod
    def _find_exact_market_gift(
        cls,
        gifts: Sequence[TonnelMarketGift],
        identity: TonnelMarketGiftIdentity,
    ) -> TonnelMarketGift:
        matches = [
            gift
            for gift in gifts
            if cls._typed_equal(gift.gift_id, identity.source_gift_id)
            and gift.sale_id == identity.sale_id
            and gift.identity.collectible_slug == identity.collectible_slug
        ]
        if len(matches) != 1:
            raise TonnelGiftUnavailableError(
                "Выбранный подарок не найден во внутреннем инвентаре Tonnel."
            )
        return matches[0]

    @staticmethod
    def _has_market_slug(gifts: Sequence[TonnelMarketGift], slug: str) -> bool:
        return sum(gift.identity.collectible_slug == slug for gift in gifts) == 1

    @classmethod
    def _has_exact_market_identity(
        cls,
        gifts: Sequence[TonnelMarketGift],
        identity: TonnelMarketGiftIdentity,
    ) -> bool:
        return (
            sum(
                cls._typed_equal(gift.gift_id, identity.source_gift_id)
                and gift.sale_id == identity.sale_id
                and gift.identity.collectible_slug == identity.collectible_slug
                for gift in gifts
            )
            == 1
        )

    @classmethod
    def _extract_sale_id(cls, body: Mapping[str, Any]) -> str | None:
        candidates = [body.get("sale_id")]
        data = cls._mapping(body.get("data"))
        candidates.append(data.get("sale_id"))
        for candidate in candidates:
            if cls._valid_opaque_id(candidate):
                return str(candidate)
        return None

    @staticmethod
    def _javascript_date_now() -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    @classmethod
    def _mutation_result(
        cls,
        request_sent: bool,
        response: TonnelHttpResponse | None,
        started: float | None,
        finished: float | None,
    ) -> TonnelMutationResult:
        body = response.body if response is not None else None
        mapping = cls._mapping(body)
        return TonnelMutationResult(
            request_sent=request_sent,
            http_status=response.status if response is not None else None,
            body_type=cls._safe_body_type(body),
            status=cls._safe_response_status(mapping.get("status")),
            message=cls._safe_response_message(mapping.get("message")),
            safe_field_names=tuple(sorted(str(key)[:64] for key in mapping)),
            duration_ms=(
                cls._milliseconds(started, finished)
                if started is not None and finished is not None
                else None
            ),
        )

    @staticmethod
    def _safe_body_type(value: Any) -> str:
        if value is None:
            return "none"
        if isinstance(value, Mapping):
            return "mapping"
        if isinstance(value, list):
            return "list"
        if isinstance(value, str):
            return "string"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, (int, Decimal)):
            return "number"
        return "other"

    @staticmethod
    def _safe_response_status(value: Any) -> str | None:
        if isinstance(value, str) and _SAFE_STATUS_RE.fullmatch(value):
            return value
        if isinstance(value, (int, bool)):
            return str(value)
        return None

    @staticmethod
    def _safe_response_message(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        message = " ".join(value.replace("\x00", " ").split())
        message = re.sub(
            r"(?i)\b(authData|initData|authorization|cookie|token|wtf)\b\s*[:=]\s*\S+",
            r"\1=<redacted>",
            message,
        )
        message = re.sub(r"\b[A-Za-z0-9_-]{32,}\b", "<redacted>", message)
        return message[:240] or None

    @staticmethod
    def _encrypt_timestamp(timestamp: str) -> str:
        """Match CryptoJS.AES.encrypt(text, passphrase) OpenSSL framing."""
        salt = os.urandom(8)
        material = b""
        block = b""
        while len(material) < 48:
            digest = hashes.Hash(hashes.MD5())
            digest.update(block + TONNEL_REQUEST_SECRET + salt)
            block = digest.finalize()
            material += block
        key, iv = material[:32], material[32:48]
        padder = padding.PKCS7(128).padder()
        padded = padder.update(timestamp.encode()) + padder.finalize()
        encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()
        return base64.b64encode(b"Salted__" + salt + ciphertext).decode()

    @classmethod
    def _market_result(
        cls,
        *,
        status: TonnelTransferStatus,
        selected: TonnelMarketGift,
        quote: TonnelMarketQuote,
        listing_sent: bool,
        buy_sent: bool,
        listing_status: int | None,
        buy_status: int | None,
        listing_fields: tuple[str, ...],
        buy_fields: tuple[str, ...],
        source_owns: bool | None,
        destination_owns: bool | None,
        listing_still_active: bool | None,
        error_code: str | None,
        timings: Mapping[str, int | None],
        message: str,
    ) -> TonnelMarketTransferResult:
        return TonnelMarketTransferResult(
            status=status,
            display_name=selected.display_name,
            sale_id=selected.sale_id,
            seller_price=quote.seller_price,
            buyer_price=quote.buyer_price,
            seller_price_nanotons=quote.seller_price_nanotons,
            buyer_price_nanotons=quote.buyer_price_nanotons,
            commission_percent=quote.fee_rate * Decimal(100),
            listing_sent=listing_sent,
            buy_sent=buy_sent,
            listing_status=listing_status,
            buy_status=buy_status,
            listing_response_fields=listing_fields,
            buy_response_fields=buy_fields,
            source_owns_after=source_owns,
            destination_owns_after=destination_owns,
            listing_still_active=listing_still_active,
            error_code=error_code,
            timings_ms=dict(timings),
            message=message,
            seller_balance=quote.seller_balance,
            buyer_balance=quote.buyer_balance,
            balance_sufficient=(
                quote.buyer_balance >= quote.buyer_price
                if quote.buyer_balance is not None
                else None
            ),
        )

    def _buy_offer_result(
        self,
        *,
        status: TonnelTransferStatus,
        selected: TonnelMarketGift,
        offer_id: str | int | None,
        quote: TonnelBuyOfferQuote,
        create_result: TonnelMutationResult,
        accept_result: TonnelMutationResult,
        source_owns: bool | None,
        destination_owns: bool | None,
        offer_active: bool | None,
        error_code: str | None,
        timings: Mapping[str, int | None],
        message: str,
        accept_request_metadata: Mapping[str, object] | None = None,
        offer_correlation: Mapping[str, object] | None = None,
        ownership_verification: Mapping[str, object] | None = None,
        create_request_metadata: Mapping[str, object] | None = None,
    ) -> TonnelBuyOfferTransferResult:
        return TonnelBuyOfferTransferResult(
            status=status,
            display_name=selected.display_name,
            offer_id=offer_id,
            gift_id=selected.gift_id,
            quote=quote,
            create_result=create_result,
            accept_result=accept_result,
            source_owns_after=source_owns,
            destination_owns_after=destination_owns,
            offer_active_after=offer_active,
            error_code=error_code,
            timings_ms=dict(timings),
            message=message,
            offer_correlation=dict(offer_correlation or {}),
            ownership_verification=dict(ownership_verification or {}),
            create_request_metadata=dict(
                create_request_metadata or self._create_request_metadata()
            ),
            accept_request_metadata=dict(accept_request_metadata or {}),
        )

    @staticmethod
    def _milliseconds(start: float, end: float) -> int:
        return max(0, round((end - start) * 1000))

    @classmethod
    def _finish_timings(
        cls, timings: dict[str, int | None], started: float
    ) -> dict[str, int | None]:
        result = dict(timings)
        result["total_job_ms"] = cls._milliseconds(started, time.monotonic())
        return result

    @staticmethod
    def select_transfer_strategy(
        *,
        direct_recipient: bool,
        private_reserved: bool,
        listing_bound: bool,
        offer: bool,
        public_listing: bool,
    ) -> TonnelTransferStrategy:
        if direct_recipient:
            return TonnelTransferStrategy.DIRECT_RECIPIENT
        if private_reserved:
            return TonnelTransferStrategy.PRIVATE_RESERVED
        if listing_bound:
            return TonnelTransferStrategy.LISTING_BOUND
        if offer:
            return TonnelTransferStrategy.OFFER
        if public_listing:
            return TonnelTransferStrategy.PUBLIC_LISTING
        return TonnelTransferStrategy.UNKNOWN

    @staticmethod
    def normalize_ton(value: str | Decimal) -> Decimal:
        try:
            amount = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("Invalid TON amount") from exc
        if not amount.is_finite() or amount <= 0:
            raise ValueError("Invalid TON amount")
        return amount

    @classmethod
    def purchase_price(cls, seller_price: str | Decimal) -> Decimal:
        return cls.normalize_ton(seller_price) * TONNEL_BUYER_FEE_FACTOR

    @classmethod
    def seller_proceeds(cls, seller_price: str | Decimal) -> Decimal:
        return cls.normalize_ton(seller_price)

    @classmethod
    def _find_exact_source_gift(
        cls, gifts: Sequence[TonnelGift], identity: TonnelGiftIdentity
    ) -> TonnelGift:
        matches = [
            gift
            for gift in gifts
            if cls._typed_equal(gift.gift_id, identity.owned_gift_id)
            and gift.identity.collectible_slug == identity.collectible_slug
        ]
        if len(matches) != 1:
            raise TonnelGiftUnavailableError(
                "Выбранный подарок не найден или его идентификатор изменился."
            )
        return matches[0]

    @staticmethod
    def _has_slug(gifts: Sequence[TonnelGift], slug: str) -> bool:
        return sum(gift.identity.collectible_slug == slug for gift in gifts) == 1

    @staticmethod
    def _typed_equal(left: Any, right: Any) -> bool:
        return type(left) is type(right) and left == right

    @staticmethod
    def _valid_opaque_id(value: Any) -> bool:
        return (isinstance(value, str) and bool(value) and len(value) <= 256) or (
            isinstance(value, int) and not isinstance(value, bool)
        )

    def _api_url(self, path: str) -> str:
        return f"{self._api_origin}{path}"

    def _data_url(self, telegram_user_id: int, path: str) -> str:
        origin = (
            TONNEL_GIFTS3_ORIGIN if telegram_user_id % 3 == 0 else TONNEL_GIFTS2_ORIGIN
        )
        return f"{origin}{path}"

    def _buy_offer_read_url(self, telegram_user_id: int, path: str) -> str:
        if self._api_origin == TONNEL_REGIONAL_API_ORIGIN:
            return f"{TONNEL_REGIONAL_DATA_ORIGIN}{path}"
        return self._data_url(telegram_user_id, path)

    @classmethod
    def _require_success(
        cls, response: TonnelHttpResponse, *, inventory: bool = False
    ) -> Mapping[str, Any]:
        body = cls._mapping(response.body)
        if response.status != 200:
            raise TonnelApiError(response.status, cls._safe_code(body.get("code")))
        if body.get("status") != "success":
            if inventory:
                raise TonnelInventoryUnavailableError(
                    "Список подарков Tonnel недоступен. Проверьте подключение "
                    "@Tonnel_Network_bot к рабочему аккаунту."
                )
            raise TonnelApiError(response.status, cls._safe_code(body.get("code")))
        return body

    @staticmethod
    def _mapping(value: Any) -> Mapping[str, Any]:
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    def _integer(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    @staticmethod
    def _decimal(value: Any) -> Decimal:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return Decimal(0)
        return result if result.is_finite() else Decimal(0)

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return result if result.is_finite() else None

    @staticmethod
    def _safe_code(value: Any) -> str | None:
        return (
            value
            if isinstance(value, str) and _SAFE_STATUS_RE.fullmatch(value)
            else None
        )

    @staticmethod
    def fingerprint(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()[:12]


__all__ = [
    "TONNEL_BUYER_FEE_FACTOR",
    "TONNEL_DIRECT_TRANSFER_FEE_BASE",
    "TONNEL_DIRECT_TRANSFER_FEE_HIGH",
    "TonnelApiError",
    "TonnelAuthenticationError",
    "TonnelBalance",
    "TonnelBatchAuthContext",
    "TonnelBuyOffer",
    "TonnelBuyOfferQuote",
    "TonnelBuyOfferTransferResult",
    "TonnelExistingOfferDiagnosticResult",
    "TonnelGift",
    "TonnelGiftIdentity",
    "TonnelGiftUnavailableError",
    "TonnelHttpResponse",
    "TonnelHttpTransport",
    "TonnelInsufficientBalanceError",
    "TonnelInventoryUnavailableError",
    "TonnelMarketGift",
    "TonnelMarketGiftIdentity",
    "TonnelMarketQuote",
    "TonnelMarketTransferResult",
    "TonnelMutationNetworkError",
    "TonnelMutationResult",
    "TonnelNetworkError",
    "TonnelPreflightFailure",
    "TonnelRecipientMismatchError",
    "TonnelService",
    "TonnelServiceError",
    "TonnelTransferMode",
    "TonnelTransferResult",
    "TonnelTransferStatus",
    "TonnelTransferStrategy",
]
