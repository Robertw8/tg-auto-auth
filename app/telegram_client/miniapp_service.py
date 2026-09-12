from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, parse_qsl, urlsplit

from telethon import TelegramClient, utils
from telethon.tl import functions, types

from app.db import Database

from .session_service import SessionService

logger = logging.getLogger(__name__)
SUPPORTED_MINIAPPS = {
    "mrkt": "@mrkt",
    "portals": "@portals",
    "tonnel": "@Tonnel_Network_bot",
    "tonnel_network_bot": "@Tonnel_Network_bot",
}
TONNEL_APP_SHORT_NAME = "gift"
PLATFORM = "android"


class MiniAppError(RuntimeError):
    """Base error for a safe Mini App launch failure."""


class UnsupportedMiniAppBotError(MiniAppError):
    """The requested bot is outside the explicit allowlist."""


class MiniAppAccountNotFoundError(MiniAppError):
    """The selected database account is missing or belongs to another user."""


class MiniAppSessionMissingError(MiniAppError):
    """The account record has no corresponding local session file."""


class MiniAppSessionUnauthorizedError(MiniAppError):
    """Telegram no longer considers the saved session authorized."""


class MiniAppSessionConnectionError(MiniAppError):
    """The saved Telegram session could not be connected or inspected."""


class MiniAppSessionIdentityMismatchError(MiniAppError):
    """The saved session belongs to a different Telegram account."""


class MiniAppBotResolutionError(MiniAppError):
    """The selected username did not resolve to a Telegram bot."""


class MiniAppWebViewError(MiniAppError):
    """Telegram did not produce the requested Mini App WebView."""


class _MiniAppClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_entity(self, entity: str) -> Any: ...

    async def __call__(self, request: Any) -> Any: ...


ClientFactory = Callable[[Path], _MiniAppClient]


@dataclass(frozen=True, slots=True)
class MiniAppLaunchResult:
    account_id: int
    bot_username: str
    resolved_bot_id: int
    webview_obtained: bool
    init_data_obtained: bool
    init_data_fingerprint: str | None
    init_data_fields: frozenset[str]
    webview_url: str | None = field(repr=False)
    init_data: str | None = field(repr=False)
    query_id: int | None = field(repr=False)


class MiniAppService:
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        db: Database,
        sessions: SessionService,
        *,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._db = db
        self._sessions = sessions
        self._client_factory = client_factory or self._client

    async def open_miniapp(
        self,
        account_id: int,
        bot_username: str,
        *,
        owner_telegram_id: int,
    ) -> MiniAppLaunchResult:
        canonical_bot = self.normalize_bot_username(bot_username)
        account = await self._db.get_account(account_id, owner_telegram_id)
        if account is None:
            raise MiniAppAccountNotFoundError("Connected account not found")
        if not self._sessions.exists(account.session_key):
            raise MiniAppSessionMissingError("Connected account session is missing")

        logger.info(
            "Mini App launch started account_db_id=%d bot=%s",
            account.id,
            canonical_bot,
        )
        client = self._client_factory(self._sessions.path_for(account.session_key))
        try:
            try:
                await client.connect()
                authorized = await client.is_user_authorized()
                me = await client.get_me() if authorized else None
            except Exception as exc:
                raise MiniAppSessionConnectionError(
                    "Could not connect or inspect the saved Telegram session"
                ) from exc
            if not authorized:
                raise MiniAppSessionUnauthorizedError(
                    "Connected account session was revoked"
                )
            telegram_id = getattr(me, "id", None)
            if (
                isinstance(telegram_id, bool)
                or not isinstance(telegram_id, int)
                or telegram_id != account.telegram_account_id
            ):
                raise MiniAppSessionIdentityMismatchError(
                    "Saved Telegram session identity does not match the account record"
                )

            try:
                entity = await client.get_entity(canonical_bot)
            except Exception as exc:
                raise MiniAppBotResolutionError(
                    "Could not resolve the selected Telegram Mini App bot"
                ) from exc
            if not isinstance(entity, types.User) or not entity.bot:
                raise MiniAppBotResolutionError(
                    "Selected username did not resolve to a Telegram bot"
                )
            input_bot = utils.get_input_user(entity)
            logger.info(
                "Mini App bot resolved account_db_id=%d bot=%s resolved_bot_id=%d",
                account.id,
                canonical_bot,
                entity.id,
            )

            try:
                if canonical_bot == SUPPORTED_MINIAPPS["tonnel"]:
                    response = await client(
                        functions.messages.RequestAppWebViewRequest(
                            peer=types.InputPeerSelf(),
                            app=types.InputBotAppShortName(
                                bot_id=input_bot,
                                short_name=TONNEL_APP_SHORT_NAME,
                            ),
                            platform=PLATFORM,
                        )
                    )
                else:
                    response = await client(
                        functions.messages.RequestMainWebViewRequest(
                            peer=types.InputPeerEmpty(),
                            bot=input_bot,
                            platform=PLATFORM,
                        )
                    )
            except Exception as exc:
                raise MiniAppWebViewError(
                    "Telegram Mini App WebView request failed"
                ) from exc
            webview_url = self._response_url(response)
            init_data, init_data_fields = self._extract_valid_init_data(webview_url)
            fingerprint = self._fingerprint(init_data) if init_data else None
            result = MiniAppLaunchResult(
                account_id=account.id,
                bot_username=canonical_bot,
                resolved_bot_id=entity.id,
                webview_obtained=webview_url is not None,
                init_data_obtained=init_data is not None,
                init_data_fingerprint=fingerprint,
                init_data_fields=init_data_fields,
                webview_url=webview_url,
                init_data=init_data,
                query_id=self._response_query_id(response),
            )
            logger.info(
                "Mini App WebView request completed account_db_id=%d bot=%s "
                "resolved_bot_id=%d webview_obtained=%s init_data_obtained=%s "
                "init_data_fingerprint=%s",
                account.id,
                canonical_bot,
                entity.id,
                result.webview_obtained,
                result.init_data_obtained,
                fingerprint or "none",
            )
            return result
        except Exception as exc:
            cause = exc.__cause__ if exc.__cause__ is not None else exc
            logger.warning(
                "Mini App launch failed account_db_id=%d bot=%s exception=%s "
                "cause_exception=%s",
                account.id,
                canonical_bot,
                type(exc).__name__,
                type(cause).__name__,
            )
            raise
        finally:
            try:
                await client.disconnect()
            except Exception as exc:  # noqa: BLE001 - never mask launch result
                logger.warning(
                    "Mini App client disconnect failed account_db_id=%d bot=%s "
                    "exception=%s",
                    account.id,
                    canonical_bot,
                    type(exc).__name__,
                )

    def _client(self, session_path: Path) -> _MiniAppClient:
        return TelegramClient(str(session_path), self._api_id, self._api_hash)

    @staticmethod
    def normalize_bot_username(bot_username: str) -> str:
        key = bot_username.strip().removeprefix("@").lower()
        try:
            return SUPPORTED_MINIAPPS[key]
        except KeyError as exc:
            raise UnsupportedMiniAppBotError(
                "Only @mrkt, @portals, and @Tonnel_Network_bot are supported"
            ) from exc

    @staticmethod
    def _response_url(response: Any) -> str | None:
        if not isinstance(response, types.WebViewResultUrl):
            return None
        url = response.url.strip()
        return url or None

    @staticmethod
    def _response_query_id(response: Any) -> int | None:
        if not isinstance(response, types.WebViewResultUrl):
            return None
        return response.query_id

    @staticmethod
    def _extract_valid_init_data(
        webview_url: str | None,
    ) -> tuple[str | None, frozenset[str]]:
        if webview_url is None:
            return None, frozenset()
        parsed_url = urlsplit(webview_url)
        init_data = None
        for parameter_string in (parsed_url.fragment, parsed_url.query):
            values = parse_qs(
                parameter_string.lstrip("?#"),
                keep_blank_values=True,
            ).get("tgWebAppData")
            if values and values[0]:
                init_data = values[0]
                break
        if init_data is None:
            return None, frozenset()

        try:
            fields = frozenset(
                key for key, _ in parse_qsl(init_data, keep_blank_values=True)
            )
        except ValueError:
            return None, frozenset()
        if not fields or "auth_date" not in fields or "hash" not in fields:
            return None, fields
        return init_data, fields

    @staticmethod
    def _fingerprint(init_data: str) -> str:
        return hashlib.sha256(init_data.encode()).hexdigest()[:8]
