from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import secrets
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import StrEnum

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError
from telethon.errors import PhoneCodeExpiredError, SessionPasswordNeededError

from app.bot.flow_registry import LoginFlowRegistry
from app.bot.keyboards import account_connected_keyboard, mask_phone
from app.db import Database
from app.telegram_client import (
    AuthAttemptMismatchError,
    AuthorizedAccount,
    AuthService,
    SessionService,
)

logger = logging.getLogger(__name__)
ATTEMPT_TIMEOUT_SECONDS = 5 * 60
CODE_REQUEST_LIMIT = 3
CODE_REQUEST_WINDOW_SECONDS = 15 * 60


class AttemptState(StrEnum):
    CREATED = "created"
    READY = "ready"
    REQUESTING_CODE = "requesting_code"
    CODE_SENT = "code_sent"
    PASSWORD_NEEDED = "password_needed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    FAILED = "failed"


class AttemptAccessError(RuntimeError):
    """The supplied token cannot access the requested authorization attempt."""


class AttemptConflictError(RuntimeError):
    """The attempted operation is not valid for the current attempt state."""


class CodeRateLimitError(RuntimeError):
    def __init__(self, retry_after: int) -> None:
        super().__init__("Application code-request rate limit exceeded")
        self.retry_after = retry_after


@dataclass(frozen=True, slots=True)
class LaunchAuthorization:
    attempt_id: str
    launch_token: str


@dataclass(frozen=True, slots=True)
class BrowserAuthorization:
    session_token: str
    csrf_token: str
    expires_in: int


@dataclass(frozen=True, slots=True)
class CodeDelivery:
    delivery_type: str | None


@dataclass(frozen=True, slots=True)
class SignInResult:
    password_required: bool
    account: AuthorizedAccount | None = None


@dataclass(slots=True)
class _WebAttempt:
    attempt_id: str
    owner_telegram_id: int
    chat_id: int
    account_role: str
    launch_token_digest: str | None
    expires_at: float
    state: AttemptState = AttemptState.CREATED
    browser_token_digest: str | None = None
    csrf_token_digest: str | None = None
    session_key: str | None = None
    normalized_phone: str | None = None
    phone_code_hash: str | None = None
    timeout_task: asyncio.Task[None] | None = None
    operation_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class WebAuthCoordinator:
    """Owns short-lived Web App tokens and their in-memory Telethon state."""

    def __init__(
        self,
        *,
        bot: Bot,
        db: Database,
        sessions: SessionService,
        auth_service: AuthService,
        login_flows: LoginFlowRegistry,
    ) -> None:
        self._bot = bot
        self._db = db
        self._sessions = sessions
        self._auth_service = auth_service
        self._login_flows = login_flows
        self._attempts: dict[str, _WebAttempt] = {}
        self._attempt_by_owner: dict[int, str] = {}
        self._launch_tokens: dict[str, str] = {}
        self._browser_tokens: dict[str, str] = {}
        self._code_requests: defaultdict[int, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def create_attempt(
        self, owner_telegram_id: int, chat_id: int, account_role: str = "TARGET"
    ) -> LaunchAuthorization:
        account_role = account_role.strip().upper()
        if account_role not in {"OWNER", "TARGET"}:
            raise ValueError("Unsupported account role")
        if not await self._login_flows.acquire(owner_telegram_id):
            raise AttemptConflictError("A login is already active for this user")

        attempt_id = secrets.token_hex(8)
        launch_token = secrets.token_urlsafe(32)
        launch_digest = self._digest(launch_token)
        attempt = _WebAttempt(
            attempt_id=attempt_id,
            owner_telegram_id=owner_telegram_id,
            chat_id=chat_id,
            account_role=account_role,
            launch_token_digest=launch_digest,
            expires_at=time.monotonic() + ATTEMPT_TIMEOUT_SECONDS,
        )
        try:
            async with self._lock:
                self._attempts[attempt_id] = attempt
                self._attempt_by_owner[owner_telegram_id] = attempt_id
                self._launch_tokens[launch_digest] = attempt_id
                attempt.timeout_task = asyncio.create_task(
                    self._expire_after_timeout(attempt),
                    name=f"web-auth-timeout-{attempt_id}",
                )
        except Exception:
            await self._login_flows.release(owner_telegram_id)
            raise

        logger.info(
            "Web authorization attempt created attempt_id=%s control_user_id=%d",
            attempt_id,
            owner_telegram_id,
        )
        return LaunchAuthorization(attempt_id=attempt_id, launch_token=launch_token)

    async def redeem_launch_token(
        self, launch_token: str, telegram_user_id: int
    ) -> BrowserAuthorization:
        launch_digest = self._digest(launch_token)
        async with self._lock:
            attempt_id = self._launch_tokens.get(launch_digest)
            attempt = self._attempts.get(attempt_id or "")
            if (
                attempt is None
                or attempt.launch_token_digest is None
                or not secrets.compare_digest(
                    launch_digest, attempt.launch_token_digest
                )
                or attempt.owner_telegram_id != telegram_user_id
                or attempt.state is not AttemptState.CREATED
                or self._is_expired(attempt)
            ):
                raise AttemptAccessError("Invalid or expired authorization token")

            browser_token = secrets.token_urlsafe(32)
            csrf_token = secrets.token_urlsafe(32)
            browser_digest = self._digest(browser_token)
            self._launch_tokens.pop(launch_digest, None)
            attempt.launch_token_digest = None
            attempt.browser_token_digest = browser_digest
            attempt.csrf_token_digest = self._digest(csrf_token)
            attempt.state = AttemptState.READY
            self._browser_tokens[browser_digest] = attempt.attempt_id
            expires_in = max(1, int(attempt.expires_at - time.monotonic()))

        logger.info(
            "Web authorization token redeemed attempt_id=%s control_user_id=%d",
            attempt.attempt_id,
            telegram_user_id,
        )
        return BrowserAuthorization(
            session_token=browser_token,
            csrf_token=csrf_token,
            expires_in=expires_in,
        )

    async def request_code(
        self,
        *,
        browser_token: str,
        csrf_token: str,
        telegram_user_id: int,
        phone: str,
    ) -> CodeDelivery:
        attempt = await self._authorize_browser(
            browser_token, csrf_token, telegram_user_id
        )
        async with attempt.operation_lock:
            self._ensure_active_state(attempt, AttemptState.READY)
            try:
                await self._consume_code_request_quota(attempt.owner_telegram_id)
            except CodeRateLimitError:
                await self._close_attempt(
                    attempt, AttemptState.FAILED, delete_session=True
                )
                raise
            attempt.state = AttemptState.REQUESTING_CODE
            attempt.session_key = self._sessions.create_key(attempt.owner_telegram_id)
            try:
                result = await self._auth_service.request_code(
                    attempt.attempt_id,
                    self._sessions.path_for(attempt.session_key),
                    phone,
                    control_user_id=attempt.owner_telegram_id,
                )
            except Exception:
                await self._close_attempt(
                    attempt, AttemptState.FAILED, delete_session=True
                )
                raise

            attempt.normalized_phone = result.normalized_phone
            attempt.phone_code_hash = result.phone_code_hash
            attempt.state = AttemptState.CODE_SENT
            return CodeDelivery(delivery_type=result.delivery_type)

    async def sign_in_with_code(
        self,
        *,
        browser_token: str,
        csrf_token: str,
        telegram_user_id: int,
        code: str,
    ) -> SignInResult:
        attempt = await self._authorize_browser(
            browser_token, csrf_token, telegram_user_id
        )
        async with attempt.operation_lock:
            self._ensure_active_state(attempt, AttemptState.CODE_SENT)
            if (
                attempt.session_key is None
                or attempt.normalized_phone is None
                or attempt.phone_code_hash is None
            ):
                await self._close_attempt(
                    attempt, AttemptState.FAILED, delete_session=True
                )
                raise AttemptConflictError("Authorization state was lost")

            try:
                account = await self._auth_service.sign_in_with_code(
                    attempt.attempt_id,
                    self._sessions.path_for(attempt.session_key),
                    phone=attempt.normalized_phone,
                    code=code,
                    phone_code_hash=attempt.phone_code_hash,
                )
            except Exception as exc:
                if isinstance(exc, SessionPasswordNeededError):
                    attempt.normalized_phone = None
                    attempt.phone_code_hash = None
                    attempt.state = AttemptState.PASSWORD_NEEDED
                    return SignInResult(password_required=True)
                if isinstance(exc, (PhoneCodeExpiredError, AuthAttemptMismatchError)):
                    await self._close_attempt(
                        attempt, AttemptState.FAILED, delete_session=True
                    )
                raise
            finally:
                code = ""

            account = await self._persist_account(attempt, account)
            return SignInResult(password_required=False, account=account)

    async def sign_in_with_password(
        self,
        *,
        browser_token: str,
        csrf_token: str,
        telegram_user_id: int,
        password: str,
    ) -> AuthorizedAccount:
        attempt = await self._authorize_browser(
            browser_token, csrf_token, telegram_user_id
        )
        async with attempt.operation_lock:
            self._ensure_active_state(attempt, AttemptState.PASSWORD_NEEDED)
            if attempt.session_key is None:
                await self._close_attempt(
                    attempt, AttemptState.FAILED, delete_session=True
                )
                raise AttemptConflictError("Authorization state was lost")
            try:
                account = await self._auth_service.sign_in_with_password(
                    attempt.attempt_id,
                    self._sessions.path_for(attempt.session_key),
                    password,
                )
            except AuthAttemptMismatchError:
                await self._close_attempt(
                    attempt, AttemptState.FAILED, delete_session=True
                )
                raise
            finally:
                password = ""

            return await self._persist_account(attempt, account)

    async def cancel_browser_attempt(
        self,
        *,
        browser_token: str,
        csrf_token: str,
        telegram_user_id: int,
    ) -> None:
        attempt = await self._authorize_browser(
            browser_token, csrf_token, telegram_user_id
        )
        async with attempt.operation_lock:
            await self._close_attempt(
                attempt, AttemptState.CANCELLED, delete_session=True
            )

    async def cancel_owner(self, owner_telegram_id: int) -> bool:
        async with self._lock:
            attempt_id = self._attempt_by_owner.get(owner_telegram_id)
            attempt = self._attempts.get(attempt_id or "")
        if attempt is None:
            await self._login_flows.release(owner_telegram_id)
            return False
        async with attempt.operation_lock:
            await self._close_attempt(
                attempt, AttemptState.CANCELLED, delete_session=True
            )
        return True

    async def shutdown(self) -> None:
        async with self._lock:
            attempts = list(self._attempts.values())
        for attempt in attempts:
            async with attempt.operation_lock:
                await self._close_attempt(
                    attempt, AttemptState.CANCELLED, delete_session=True
                )

    async def _persist_account(
        self, attempt: _WebAttempt, account: AuthorizedAccount
    ) -> AuthorizedAccount:
        if attempt.session_key is None:
            raise AttemptConflictError("Authorization session was lost")
        try:
            old_session_key = await self._db.save_account(
                owner_telegram_id=attempt.owner_telegram_id,
                telegram_account_id=account.id,
                session_key=attempt.session_key,
                username=account.username,
                first_name=account.first_name,
                phone=account.phone,
                role=attempt.account_role,
            )
        except (sqlite3.Error, OSError):
            await self._logout_and_delete(attempt.session_key)
            await self._close_attempt(attempt, AttemptState.FAILED, delete_session=True)
            raise

        if old_session_key and old_session_key != attempt.session_key:
            await self._logout_and_delete(old_session_key)
        await self._close_attempt(attempt, AttemptState.COMPLETED, delete_session=False)
        account_name = (
            f"@{account.username}"
            if account.username
            else account.first_name or "Telegram-аккаунт"
        )
        role_name = (
            "Мой аккаунт" if attempt.account_role == "OWNER" else "Рабочий аккаунт"
        )
        try:
            await self._bot.send_message(
                attempt.chat_id,
                "<b>✅ Аккаунт подключён</b>\n\n"
                f"{html.escape(account_name)}\n"
                f"Тип: {role_name}\n"
                f"Телефон: {html.escape(mask_phone(account.phone))}\n\n"
                "Теперь его можно использовать в Portals и Tonnel.",
                reply_markup=account_connected_keyboard(),
            )
        except TelegramAPIError:
            pass
        return account

    async def _logout_and_delete(self, session_key: str) -> None:
        try:
            await self._auth_service.logout(self._sessions.path_for(session_key))
        except Exception as exc:  # noqa: BLE001 - cleanup must still delete locally
            logger.warning(
                "Authorized session cleanup failed exception=%s", type(exc).__name__
            )
        self._sessions.delete(session_key)

    async def _authorize_browser(
        self, browser_token: str, csrf_token: str, telegram_user_id: int
    ) -> _WebAttempt:
        browser_digest = self._digest(browser_token)
        csrf_digest = self._digest(csrf_token)
        async with self._lock:
            attempt_id = self._browser_tokens.get(browser_digest)
            attempt = self._attempts.get(attempt_id or "")
            if (
                attempt is None
                or attempt.browser_token_digest is None
                or attempt.csrf_token_digest is None
                or not secrets.compare_digest(
                    browser_digest, attempt.browser_token_digest
                )
                or not secrets.compare_digest(csrf_digest, attempt.csrf_token_digest)
                or attempt.owner_telegram_id != telegram_user_id
                or self._is_expired(attempt)
            ):
                raise AttemptAccessError("Invalid or expired browser authorization")
            return attempt

    async def _consume_code_request_quota(self, owner_telegram_id: int) -> None:
        now = time.monotonic()
        async with self._lock:
            requests = self._code_requests[owner_telegram_id]
            while requests and now - requests[0] >= CODE_REQUEST_WINDOW_SECONDS:
                requests.popleft()
            if len(requests) >= CODE_REQUEST_LIMIT:
                retry_after = max(
                    1, int(CODE_REQUEST_WINDOW_SECONDS - (now - requests[0]))
                )
                raise CodeRateLimitError(retry_after)
            requests.append(now)

    async def _expire_after_timeout(self, attempt: _WebAttempt) -> None:
        try:
            await asyncio.sleep(ATTEMPT_TIMEOUT_SECONDS)
            async with attempt.operation_lock:
                async with self._lock:
                    if self._attempts.get(attempt.attempt_id) is not attempt:
                        return
                await self._close_attempt(
                    attempt, AttemptState.EXPIRED, delete_session=True
                )
            try:
                await self._bot.send_message(
                    attempt.chat_id,
                    "Время входа истекло. Выберите «Добавить аккаунт», чтобы начать заново.",
                )
            except TelegramAPIError:
                pass
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001 - isolate timeout cleanup
            logger.error(
                "Web authorization timeout cleanup failed attempt_id=%s exception=%s",
                attempt.attempt_id,
                type(exc).__name__,
            )

    async def _close_attempt(
        self,
        attempt: _WebAttempt,
        final_state: AttemptState,
        *,
        delete_session: bool,
    ) -> None:
        try:
            await self._auth_service.abort_attempt(attempt.attempt_id)
        except Exception as exc:  # noqa: BLE001 - local cleanup must continue
            logger.warning(
                "Telethon attempt cleanup failed attempt_id=%s exception=%s",
                attempt.attempt_id,
                type(exc).__name__,
            )
        if delete_session and attempt.session_key is not None:
            self._sessions.delete(attempt.session_key)

        async with self._lock:
            self._attempts.pop(attempt.attempt_id, None)
            if (
                self._attempt_by_owner.get(attempt.owner_telegram_id)
                == attempt.attempt_id
            ):
                self._attempt_by_owner.pop(attempt.owner_telegram_id, None)
            if attempt.launch_token_digest is not None:
                self._launch_tokens.pop(attempt.launch_token_digest, None)
            if attempt.browser_token_digest is not None:
                self._browser_tokens.pop(attempt.browser_token_digest, None)
            attempt.state = final_state
            attempt.launch_token_digest = None
            attempt.browser_token_digest = None
            attempt.csrf_token_digest = None
            attempt.normalized_phone = None
            attempt.phone_code_hash = None

        timeout_task = attempt.timeout_task
        attempt.timeout_task = None
        if timeout_task is not None and timeout_task is not asyncio.current_task():
            timeout_task.cancel()
        await self._login_flows.release(attempt.owner_telegram_id)
        logger.info(
            "Web authorization attempt closed attempt_id=%s state=%s",
            attempt.attempt_id,
            final_state,
        )

    def _ensure_active_state(
        self, attempt: _WebAttempt, expected: AttemptState
    ) -> None:
        if self._is_expired(attempt):
            raise AttemptAccessError("Authorization attempt expired")
        if attempt.state is not expected:
            raise AttemptConflictError(
                f"Authorization step is already {attempt.state.value}"
            )

    @staticmethod
    def _is_expired(attempt: _WebAttempt) -> bool:
        return time.monotonic() >= attempt.expires_at

    @staticmethod
    def _digest(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()
