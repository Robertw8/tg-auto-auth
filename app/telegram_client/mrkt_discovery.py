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
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPCookieProcessor, Request, build_opener

from .miniapp_service import MiniAppLaunchResult

logger = logging.getLogger(__name__)

MRKT_FRONTEND_ORIGIN = "https://cdn.tgmrkt.io"
MRKT_API_ORIGIN = "https://api.tgmrkt.io"
MRKT_AUTH_PATH = "/api/v1/auth"
MRKT_INVENTORY_PATH = "/api/v1/gifts"
_ALLOWED_PROBE_PATHS = frozenset({MRKT_AUTH_PATH, MRKT_INVENTORY_PATH})

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


class MrktDiscoveryError(RuntimeError):
    """A safe MRKT discovery probe failure."""


class MrktDiscoveryInputError(MrktDiscoveryError):
    """The Mini App launch result cannot be used for the MRKT probe."""


class MrktDiscoveryNetworkError(MrktDiscoveryError):
    """The read-only probe could not reach the MRKT API."""


class MrktDiscoveryAuthError(MrktDiscoveryError):
    """MRKT rejected the Mini App authentication request."""


@dataclass(frozen=True, slots=True)
class MrktTraceEvent:
    hostname: str
    method: str
    path: str
    status: int
    request_fields: tuple[str, ...]
    response_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MrktDiscoveryResult:
    frontend_origin: str
    api_origin: str
    auth_token_obtained: bool
    cookie_names: tuple[str, ...]
    cookie_attribute_names: tuple[str, ...]
    inventory_response_fields: tuple[str, ...]
    inventory_item_fields: tuple[str, ...]
    inventory_item_count: int | None
    inventory_item_id_kind: str | None
    traces: tuple[MrktTraceEvent, ...]


@dataclass(frozen=True, slots=True)
class DiscoveryHttpResponse:
    status: int
    body: Any = field(repr=False)
    cookie_names: tuple[str, ...] = ()
    cookie_attribute_names: tuple[str, ...] = ()


class MrktProbeTransport(Protocol):
    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> DiscoveryHttpResponse: ...

    def close(self) -> None: ...


TransportFactory = Callable[[], MrktProbeTransport]


class _UrllibMrktTransport:
    def __init__(self) -> None:
        self._cookies = http.cookiejar.CookieJar()
        self._opener = build_opener(HTTPCookieProcessor(self._cookies))

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> DiscoveryHttpResponse:
        if path not in _ALLOWED_PROBE_PATHS:
            raise MrktDiscoveryInputError("MRKT probe path is not read-only")

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": MRKT_FRONTEND_ORIGIN,
            "Referer": f"{MRKT_FRONTEND_ORIGIN}/",
            "User-Agent": "Mozilla/5.0 MRKT-flow-discovery/1.0",
        }
        if authorization:
            # MRKT's current HTTP client sends the returned token directly.
            headers["Authorization"] = authorization

        request = Request(
            f"{MRKT_API_ORIGIN}{path}",
            data=json.dumps(payload, separators=(",", ":")).encode(),
            method="POST",
            headers=headers,
        )
        try:
            with self._opener.open(request, timeout=25) as response:
                status = response.status
                raw_body = response.read()
                response_headers = response.headers
        except HTTPError as exc:
            status = exc.code
            raw_body = exc.read()
            response_headers = exc.headers
        except (URLError, TimeoutError, OSError) as exc:
            raise MrktDiscoveryNetworkError(
                f"MRKT network failure: {type(exc).__name__}"
            ) from exc

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

        return DiscoveryHttpResponse(
            status=status,
            body=body,
            cookie_names=tuple(sorted(cookie_names)),
            cookie_attribute_names=tuple(sorted(cookie_attributes)),
        )

    def close(self) -> None:
        self._cookies.clear()


class MrktDiscoveryService:
    """Run only MRKT authentication and unlisted-inventory discovery requests."""

    def __init__(self, *, transport_factory: TransportFactory | None = None) -> None:
        self._transport_factory = transport_factory or _UrllibMrktTransport

    async def inspect(
        self,
        launch: MiniAppLaunchResult,
    ) -> MrktDiscoveryResult:
        return await asyncio.to_thread(self._inspect_sync, launch)

    def _inspect_sync(self, launch: MiniAppLaunchResult) -> MrktDiscoveryResult:
        self._validate_launch(launch)
        assert launch.init_data is not None

        transport = self._transport_factory()
        traces: list[MrktTraceEvent] = []
        auth_payload: dict[str, Any] | None = None
        token: str | None = None
        try:
            auth_payload = self._auth_payload(launch.init_data)
            auth_response = transport.post_json(MRKT_AUTH_PATH, auth_payload)
            traces.append(self._trace(MRKT_AUTH_PATH, auth_payload, auth_response))
            self._log_trace(traces[-1])
            auth_payload.clear()

            auth_body = auth_response.body
            if not isinstance(auth_body, dict):
                raise MrktDiscoveryAuthError(
                    f"MRKT auth returned HTTP {auth_response.status} without JSON"
                )
            raw_token = auth_body.pop("token", None)
            if not 200 <= auth_response.status < 300 or not isinstance(raw_token, str):
                raise MrktDiscoveryAuthError(
                    f"MRKT auth returned HTTP {auth_response.status}"
                )
            token = raw_token

            inventory_payload = {
                "isListed": False,
                "count": 20,
                "cursor": "",
                **_DEFAULT_INVENTORY_FILTERS,
            }
            inventory_response = transport.post_json(
                MRKT_INVENTORY_PATH,
                inventory_payload,
                authorization=token,
            )
            token = None
            traces.append(
                self._trace(
                    MRKT_INVENTORY_PATH,
                    inventory_payload,
                    inventory_response,
                )
            )
            self._log_trace(traces[-1])

            inventory_body = inventory_response.body
            response_fields = self._field_names(inventory_body)
            gifts = (
                inventory_body.get("gifts")
                if isinstance(inventory_body, dict)
                else None
            )
            first_gift = gifts[0] if isinstance(gifts, list) and gifts else None
            item_fields = self._field_names(first_gift)
            item_id_kind = (
                self._id_kind(first_gift.get("id"))
                if isinstance(first_gift, dict) and "id" in first_gift
                else None
            )

            return MrktDiscoveryResult(
                frontend_origin=MRKT_FRONTEND_ORIGIN,
                api_origin=MRKT_API_ORIGIN,
                auth_token_obtained=True,
                cookie_names=auth_response.cookie_names,
                cookie_attribute_names=auth_response.cookie_attribute_names,
                inventory_response_fields=response_fields,
                inventory_item_fields=item_fields,
                inventory_item_count=(len(gifts) if isinstance(gifts, list) else None),
                inventory_item_id_kind=item_id_kind,
                traces=tuple(traces),
            )
        except MrktDiscoveryError as exc:
            logger.warning("MRKT discovery failed exception=%s", type(exc).__name__)
            raise
        except Exception as exc:  # noqa: BLE001 - redact all transport failures
            logger.warning("MRKT discovery failed exception=%s", type(exc).__name__)
            raise MrktDiscoveryError(
                f"MRKT discovery failed: {type(exc).__name__}"
            ) from None
        finally:
            if auth_payload is not None:
                auth_payload.clear()
            token = None
            transport.close()

    @staticmethod
    def _validate_launch(launch: MiniAppLaunchResult) -> None:
        if launch.bot_username != "@mrkt":
            raise MrktDiscoveryInputError("Discovery probe supports only @mrkt")
        if not launch.init_data_obtained or not launch.init_data:
            raise MrktDiscoveryInputError("MRKT launch data is missing")
        if not launch.webview_url:
            raise MrktDiscoveryInputError("MRKT WebView URL is missing")
        parsed = urlsplit(launch.webview_url)
        if parsed.scheme != "https" or parsed.hostname != "cdn.tgmrkt.io":
            raise MrktDiscoveryInputError("Unexpected MRKT frontend origin")

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

    @classmethod
    def _trace(
        cls,
        path: str,
        payload: Mapping[str, Any],
        response: DiscoveryHttpResponse,
    ) -> MrktTraceEvent:
        return MrktTraceEvent(
            hostname="api.tgmrkt.io",
            method="POST",
            path=path,
            status=response.status,
            request_fields=tuple(sorted(payload)),
            response_fields=cls._field_names(response.body),
        )

    @staticmethod
    def _field_names(value: Any) -> tuple[str, ...]:
        if not isinstance(value, dict):
            return ()
        return tuple(sorted(str(key) for key in value))

    @staticmethod
    def _id_kind(value: Any) -> str:
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, str):
            if value.isdecimal():
                return "numeric-string"
            if re.fullmatch(
                r"[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}",
                value,
            ):
                return "uuid-like-string"
            return "opaque-string"
        return type(value).__name__

    @staticmethod
    def _log_trace(trace: MrktTraceEvent) -> None:
        logger.info(
            "MRKT trace hostname=%s method=%s path=%s status=%d "
            "request_fields=%s response_fields=%s",
            trace.hostname,
            trace.method,
            trace.path,
            trace.status,
            ",".join(trace.request_fields),
            ",".join(trace.response_fields),
        )
