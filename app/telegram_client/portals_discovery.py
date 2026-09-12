from __future__ import annotations

import asyncio
import http.cookiejar
import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, Request, build_opener

from .miniapp_service import MiniAppLaunchResult

logger = logging.getLogger(__name__)

PORTALS_FRONTEND_ORIGIN = "https://portal-market.com"
PORTALS_API_ORIGIN = "https://portal-market.com/api"
PORTALS_GAMES_API_ORIGIN = "https://backend.portal-market.com"
PORTALS_AUTH_PATH = "/users/auth"
PORTALS_INVENTORY_PATH = "/nfts/owned"
PORTALS_RECEIVED_OFFERS_PATH = "/offers/received"
PORTALS_NFT_OFFERS_PATH = "/offers/nft/{nft_id}"
PORTALS_ACCEPT_OFFER_PATH = "/offers/{offer_id}/accept"
PORTALS_HTTP_TIMEOUT_SECONDS = 25

_STATIC_READ_ONLY_PATHS = frozenset(
    {PORTALS_AUTH_PATH, PORTALS_INVENTORY_PATH, PORTALS_RECEIVED_OFFERS_PATH}
)
_NFT_OFFERS_PATH_PATTERN = re.compile(r"^/offers/nft/[^/?#]+$")


class PortalsDiscoveryError(RuntimeError):
    """A sanitized Portals discovery failure."""


class PortalsDiscoveryInputError(PortalsDiscoveryError):
    """The supplied launch or frontend data is invalid."""


class PortalsDiscoveryNetworkError(PortalsDiscoveryError):
    """A read-only Portals request could not be completed."""


class PortalsDiscoveryAuthError(PortalsDiscoveryError):
    """Portals rejected fresh Telegram Mini App authorization."""


@dataclass(frozen=True, slots=True)
class PortalsTraceEvent:
    hostname: str
    method: str
    path: str
    status: int
    query_fields: tuple[str, ...]
    response_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PortalsDiscoveryResult:
    frontend_origin: str
    api_origins: tuple[str, ...]
    authorization_scheme: str
    init_data_sent_directly: bool
    auth_response_fields: tuple[str, ...]
    auth_response_token_present: bool
    cookie_names: tuple[str, ...]
    cookie_attribute_names: tuple[str, ...]
    inventory_response_fields: tuple[str, ...]
    inventory_item_fields: tuple[str, ...]
    inventory_item_count: int | None
    inventory_item_id_kind: str | None
    received_response_fields: tuple[str, ...]
    received_item_fields: tuple[str, ...]
    received_offer_fields: tuple[str, ...]
    received_nft_fields: tuple[str, ...]
    received_item_count: int | None
    offer_id_kind: str | None
    nft_id_kind: str | None
    deal_view_requested: bool
    deal_view_response_fields: tuple[str, ...]
    traces: tuple[PortalsTraceEvent, ...]


@dataclass(frozen=True, slots=True)
class PortalsFrontendContract:
    api_origins: tuple[str, ...]
    auth_path_detected: bool
    inventory_path_detected: bool
    received_offers_path_detected: bool
    nft_offers_path_detected: bool
    accept_path_detected: bool
    accept_payload_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PortalsHttpResponse:
    status: int
    body: Any = field(repr=False)
    cookie_names: tuple[str, ...] = ()
    cookie_attribute_names: tuple[str, ...] = ()


class PortalsProbeTransport(Protocol):
    def get_json(
        self,
        path: str,
        *,
        query: Mapping[str, str | int] | None = None,
        authorization: str,
    ) -> PortalsHttpResponse: ...

    def close(self) -> None: ...


TransportFactory = Callable[[], PortalsProbeTransport]


class _UrllibPortalsTransport:
    def __init__(self) -> None:
        self._cookies = http.cookiejar.CookieJar()
        self._opener = build_opener(HTTPCookieProcessor(self._cookies))

    def get_json(
        self,
        path: str,
        *,
        query: Mapping[str, str | int] | None = None,
        authorization: str,
    ) -> PortalsHttpResponse:
        if (
            path not in _STATIC_READ_ONLY_PATHS
            and not _NFT_OFFERS_PATH_PATTERN.fullmatch(path)
        ):
            raise PortalsDiscoveryInputError(
                "Portals discovery permits only known read-only paths"
            )
        query_string = urlencode(query or {})
        url = f"{PORTALS_API_ORIGIN}{path}"
        if query_string:
            url = f"{url}?{query_string}"
        request = Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json",
                "Authorization": authorization,
                "Origin": PORTALS_FRONTEND_ORIGIN,
                "Referer": f"{PORTALS_FRONTEND_ORIGIN}/",
                "User-Agent": "Mozilla/5.0 Portals-flow-discovery/1.0",
            },
        )
        try:
            with self._opener.open(
                request, timeout=PORTALS_HTTP_TIMEOUT_SECONDS
            ) as response:
                status = response.status
                raw_body = response.read()
                response_headers = response.headers
        except HTTPError as exc:
            status = exc.code
            raw_body = exc.read()
            response_headers = exc.headers
        except (URLError, TimeoutError, OSError) as exc:
            raise PortalsDiscoveryNetworkError(
                f"Portals network failure: {type(exc).__name__}"
            ) from None

        try:
            body = json.loads(raw_body) if raw_body else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = None
        cookie_names = {cookie.name for cookie in self._cookies}
        cookie_attributes: set[str] = set()
        for set_cookie in response_headers.get_all("Set-Cookie") or ():
            parts = [part.strip() for part in set_cookie.split(";")]
            if parts and "=" in parts[0]:
                cookie_names.add(parts[0].split("=", 1)[0])
            cookie_attributes.update(
                part.split("=", 1)[0].lower() for part in parts[1:] if part
            )
        return PortalsHttpResponse(
            status=status,
            body=body,
            cookie_names=tuple(sorted(cookie_names)),
            cookie_attribute_names=tuple(sorted(cookie_attributes)),
        )

    def close(self) -> None:
        self._cookies.clear()


class PortalsDiscoveryService:
    """Inspect only Portals authentication and read-only inventory/offer APIs."""

    def __init__(self, *, transport_factory: TransportFactory | None = None) -> None:
        self._transport_factory = transport_factory or _UrllibPortalsTransport

    async def inspect(self, launch: MiniAppLaunchResult) -> PortalsDiscoveryResult:
        return await asyncio.to_thread(self._inspect_sync, launch)

    def _inspect_sync(self, launch: MiniAppLaunchResult) -> PortalsDiscoveryResult:
        self._validate_launch(launch)
        assert launch.init_data is not None
        authorization = self._authorization_header(launch.init_data)
        transport = self._transport_factory()
        traces: list[PortalsTraceEvent] = []
        responses: list[PortalsHttpResponse] = []
        try:
            auth_response = transport.get_json(
                PORTALS_AUTH_PATH, authorization=authorization
            )
            responses.append(auth_response)
            traces.append(self._trace(PORTALS_AUTH_PATH, {}, auth_response))
            self._log_trace(traces[-1])
            if not 200 <= auth_response.status < 300 or not isinstance(
                auth_response.body, dict
            ):
                raise PortalsDiscoveryAuthError(
                    f"Portals authentication returned HTTP {auth_response.status}"
                )
            auth_fields = self._field_names(auth_response.body)
            auth_token_present = "token" in auth_response.body
            auth_response.body.pop("token", None)

            inventory_query = {"offset": 0, "limit": 20}
            inventory_response = transport.get_json(
                PORTALS_INVENTORY_PATH,
                query=inventory_query,
                authorization=authorization,
            )
            responses.append(inventory_response)
            traces.append(
                self._trace(PORTALS_INVENTORY_PATH, inventory_query, inventory_response)
            )
            self._log_trace(traces[-1])

            received_query = {"offset": 0, "limit": 20}
            received_response = transport.get_json(
                PORTALS_RECEIVED_OFFERS_PATH,
                query=received_query,
                authorization=authorization,
            )
            responses.append(received_response)
            traces.append(
                self._trace(
                    PORTALS_RECEIVED_OFFERS_PATH, received_query, received_response
                )
            )
            self._log_trace(traces[-1])

            inventory_body = inventory_response.body
            raw_nfts = (
                inventory_body.get("nfts") if isinstance(inventory_body, dict) else None
            )
            first_inventory = (
                raw_nfts[0] if isinstance(raw_nfts, list) and raw_nfts else None
            )

            received_body = received_response.body
            top_offers = (
                received_body.get("top_offers")
                if isinstance(received_body, dict)
                else None
            )
            first_received = (
                top_offers[0] if isinstance(top_offers, list) and top_offers else None
            )
            offer = (
                first_received.get("offer")
                if isinstance(first_received, dict)
                else None
            )
            nft = (
                first_received.get("nft") if isinstance(first_received, dict) else None
            )
            nft_id = nft.get("id") if isinstance(nft, dict) else None
            deal_view_requested = False
            deal_view_fields: tuple[str, ...] = ()
            if self._is_supported_id(nft_id):
                encoded_nft_id = quote(str(nft_id), safe="")
                deal_path = PORTALS_NFT_OFFERS_PATH.format(nft_id=encoded_nft_id)
                deal_query = {"limit": 10}
                deal_response = transport.get_json(
                    deal_path,
                    query=deal_query,
                    authorization=authorization,
                )
                responses.append(deal_response)
                traces.append(self._trace(deal_path, deal_query, deal_response))
                self._log_trace(traces[-1])
                deal_view_requested = True
                deal_view_fields = self._field_names(deal_response.body)

            cookie_names = tuple(
                sorted(
                    {name for response in responses for name in response.cookie_names}
                )
            )
            cookie_attributes = tuple(
                sorted(
                    {
                        name
                        for response in responses
                        for name in response.cookie_attribute_names
                    }
                )
            )
            return PortalsDiscoveryResult(
                frontend_origin=PORTALS_FRONTEND_ORIGIN,
                api_origins=(PORTALS_API_ORIGIN, PORTALS_GAMES_API_ORIGIN),
                authorization_scheme="tma",
                init_data_sent_directly=True,
                auth_response_fields=auth_fields,
                auth_response_token_present=auth_token_present,
                cookie_names=cookie_names,
                cookie_attribute_names=cookie_attributes,
                inventory_response_fields=self._field_names(inventory_body),
                inventory_item_fields=self._field_names(first_inventory),
                inventory_item_count=(
                    len(raw_nfts) if isinstance(raw_nfts, list) else None
                ),
                inventory_item_id_kind=self._nested_id_kind(first_inventory),
                received_response_fields=self._field_names(received_body),
                received_item_fields=self._field_names(first_received),
                received_offer_fields=self._field_names(offer),
                received_nft_fields=self._field_names(nft),
                received_item_count=(
                    len(top_offers) if isinstance(top_offers, list) else None
                ),
                offer_id_kind=self._nested_id_kind(offer),
                nft_id_kind=self._nested_id_kind(nft),
                deal_view_requested=deal_view_requested,
                deal_view_response_fields=deal_view_fields,
                traces=tuple(traces),
            )
        except PortalsDiscoveryError as exc:
            logger.warning("Portals discovery failed exception=%s", type(exc).__name__)
            raise
        except Exception as exc:  # noqa: BLE001 - never expose request credentials
            logger.warning("Portals discovery failed exception=%s", type(exc).__name__)
            raise PortalsDiscoveryError(
                f"Portals discovery failed: {type(exc).__name__}"
            ) from None
        finally:
            authorization = ""
            for response in responses:
                if isinstance(response.body, dict):
                    response.body.clear()
            transport.close()

    @staticmethod
    def inspect_frontend_source(source: str) -> PortalsFrontendContract:
        if not source.strip():
            raise PortalsDiscoveryInputError("Portals frontend source is empty")
        origins = tuple(
            origin
            for origin in (PORTALS_API_ORIGIN, PORTALS_GAMES_API_ORIGIN)
            if origin in source
        )
        return PortalsFrontendContract(
            api_origins=origins,
            auth_path_detected='url:"/users/auth"' in source,
            inventory_path_detected='url:"/nfts/owned"' in source,
            received_offers_path_detected='url:"/offers/received"' in source,
            nft_offers_path_detected="/offers/nft/${" in source,
            accept_path_detected="/offers/${" in source and "/accept" in source,
            accept_payload_fields=("amount",)
            if "data:{amount:" in source or "data:ee.amount" in source
            else (),
        )

    @staticmethod
    def _validate_launch(launch: MiniAppLaunchResult) -> None:
        if launch.bot_username != "@portals":
            raise PortalsDiscoveryInputError("Discovery supports only @portals")
        if not launch.webview_url:
            raise PortalsDiscoveryInputError("Portals WebView URL is missing")
        if not launch.init_data_obtained or not launch.init_data:
            raise PortalsDiscoveryInputError("Portals initData is missing")
        parsed = urlsplit(launch.webview_url)
        if parsed.scheme != "https" or parsed.hostname != "portal-market.com":
            raise PortalsDiscoveryInputError("Unexpected Portals frontend origin")

    @staticmethod
    def _authorization_header(init_data: str) -> str:
        if not init_data or "hash=" not in init_data:
            raise PortalsDiscoveryInputError("Portals initData is invalid")
        return f"tma {init_data}"

    @classmethod
    def _trace(
        cls,
        path: str,
        query: Mapping[str, str | int],
        response: PortalsHttpResponse,
    ) -> PortalsTraceEvent:
        return PortalsTraceEvent(
            hostname="portal-market.com",
            method="GET",
            path=cls._path_template(path),
            status=response.status,
            query_fields=tuple(sorted(query)),
            response_fields=cls._field_names(response.body),
        )

    @staticmethod
    def _path_template(path: str) -> str:
        if _NFT_OFFERS_PATH_PATTERN.fullmatch(path):
            return PORTALS_NFT_OFFERS_PATH
        return path

    @staticmethod
    def _field_names(value: Any) -> tuple[str, ...]:
        if not isinstance(value, dict):
            return ()
        return tuple(sorted(str(key) for key in value))

    @classmethod
    def _nested_id_kind(cls, value: Any) -> str | None:
        if not isinstance(value, dict) or "id" not in value:
            return None
        return cls._id_kind(value["id"])

    @staticmethod
    def _id_kind(value: Any) -> str:
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, str):
            return "numeric-string" if value.isdecimal() else "opaque-string"
        return type(value).__name__

    @staticmethod
    def _is_supported_id(value: Any) -> bool:
        return not isinstance(value, bool) and isinstance(value, (str, int))

    @staticmethod
    def _log_trace(trace: PortalsTraceEvent) -> None:
        logger.info(
            "Portals trace hostname=%s method=%s path=%s status=%d "
            "query_fields=%s response_fields=%s",
            trace.hostname,
            trace.method,
            trace.path,
            trace.status,
            ",".join(trace.query_fields),
            ",".join(trace.response_fields),
        )
