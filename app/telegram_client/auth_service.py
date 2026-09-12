from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    SessionPasswordNeededError,
)
from telethon.tl.types import User
from telethon.utils import parse_phone

logger = logging.getLogger(__name__)
PHONE_LIKE_PATTERN = re.compile(r"(?<!\w)\+?\d(?:[\s().-]*\d){6,}(?!\w)")
LONG_TOKEN_PATTERN = re.compile(r"\b[A-Za-z0-9_-]{24,}\b")


@dataclass(frozen=True, slots=True)
class AuthorizedAccount:
    id: int
    username: str | None
    first_name: str | None
    phone: str | None


@dataclass(frozen=True, slots=True)
class CodeRequestResult:
    normalized_phone: str
    phone_code_hash: str
    hash_fingerprint: str
    delivery_type: str | None


@dataclass(slots=True)
class _ActiveAuthAttempt:
    attempt_id: str
    client: TelegramClient
    session_path: Path
    normalized_phone: str
    phone_code_hash: str | None = None
    hash_fingerprint: str | None = None
    session_identity: tuple[int, int] | None = None
    operation_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class PhoneCodeHashMissingError(RuntimeError):
    """Telegram accepted a code request without returning usable login state."""


class AuthAttemptMismatchError(RuntimeError):
    """FSM data no longer matches the live Telethon authorization attempt."""


class DuplicateCodeRequestError(RuntimeError):
    """A code request was already made for this authorization attempt."""


class AuthService:
    def __init__(self, api_id: int, api_hash: str) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._api_id_fingerprint = self._identifier_fingerprint(str(api_id))
        self._attempts: dict[str, _ActiveAuthAttempt] = {}
        self._attempts_lock = asyncio.Lock()

    def _client(self, session_path: Path) -> TelegramClient:
        return TelegramClient(str(session_path), self._api_id, self._api_hash)

    async def request_code(
        self,
        attempt_id: str,
        session_path: Path,
        phone: str,
        *,
        control_user_id: int,
    ) -> CodeRequestResult:
        normalized_phone = parse_phone(phone)
        if normalized_phone is None:
            raise ValueError("Phone number could not be normalized")

        resolved_session_path = session_path.resolve()
        request_timestamp = datetime.now(UTC).isoformat(timespec="milliseconds")
        masked_phone = self._mask_phone(normalized_phone)
        session_label = self._session_label(resolved_session_path)
        async with self._attempts_lock:
            if attempt_id in self._attempts:
                logger.warning(
                    "Telegram code request duplicate rejected timestamp=%s "
                    "attempt_id=%s control_user_id=%d phone=%s "
                    "api_id_fingerprint=%s session=%s",
                    request_timestamp,
                    attempt_id,
                    control_user_id,
                    masked_phone,
                    self._api_id_fingerprint,
                    session_label,
                )
                raise DuplicateCodeRequestError(
                    "send_code_request was already called for this attempt"
                )
            active_same_phone_attempts = sum(
                existing.normalized_phone == normalized_phone
                for existing in self._attempts.values()
            )
            attempt = _ActiveAuthAttempt(
                attempt_id=attempt_id,
                client=self._client(resolved_session_path),
                session_path=resolved_session_path,
                normalized_phone=normalized_phone,
            )
            self._attempts[attempt_id] = attempt

        logger.info(
            "Telegram code request started timestamp=%s attempt_id=%s "
            "control_user_id=%d phone=%s api_id_fingerprint=%s session=%s "
            "active_same_phone_attempts=%d",
            request_timestamp,
            attempt_id,
            control_user_id,
            masked_phone,
            self._api_id_fingerprint,
            session_label,
            active_same_phone_attempts,
        )
        try:
            async with attempt.operation_lock:
                await attempt.client.connect()
                logger.info(
                    "Calling Telethon send_code_request timestamp=%s attempt_id=%s "
                    "phone=%s api_id_fingerprint=%s session=%s",
                    request_timestamp,
                    attempt_id,
                    masked_phone,
                    self._api_id_fingerprint,
                    session_label,
                )
                sent_code = await attempt.client.send_code_request(normalized_phone)
                phone_code_hash = sent_code.phone_code_hash
                if not phone_code_hash:
                    raise PhoneCodeHashMissingError(
                        "Telegram returned no phone_code_hash for the code request"
                    )

                attempt.phone_code_hash = phone_code_hash
                attempt.hash_fingerprint = self._hash_fingerprint(phone_code_hash)
                attempt.session_identity = self._session_identity(resolved_session_path)
                delivery_type = self._delivery_type(sent_code.type)
                next_type = self._delivery_type(getattr(sent_code, "next_type", None))
                timeout_seconds = self._timeout_seconds(
                    getattr(sent_code, "timeout", None)
                )
                logger.info(
                    "Telegram SentCode received timestamp=%s attempt_id=%s phone=%s "
                    "api_id_fingerprint=%s session=%s sent_code_returned=%s "
                    "delivery_type=%s next_type=%s timeout_seconds=%s "
                    "phone_code_hash_returned=%s hash_fingerprint=%s",
                    request_timestamp,
                    attempt_id,
                    masked_phone,
                    self._api_id_fingerprint,
                    session_label,
                    True,
                    delivery_type or "unknown",
                    next_type or "none",
                    timeout_seconds if timeout_seconds is not None else "none",
                    True,
                    attempt.hash_fingerprint,
                )
                return CodeRequestResult(
                    normalized_phone=normalized_phone,
                    phone_code_hash=phone_code_hash,
                    hash_fingerprint=attempt.hash_fingerprint,
                    delivery_type=delivery_type,
                )
        except Exception as exc:
            logger.warning(
                "Telegram code request failed timestamp=%s attempt_id=%s phone=%s "
                "api_id_fingerprint=%s session=%s sent_code_returned=%s "
                "phone_code_hash_returned=%s exception=%s flood_wait_seconds=%s "
                "message=%s",
                request_timestamp,
                attempt_id,
                masked_phone,
                self._api_id_fingerprint,
                session_label,
                "sent_code" in locals(),
                bool(attempt.phone_code_hash),
                type(exc).__name__,
                self._flood_wait_seconds(exc),
                self._sanitize_error(
                    exc,
                    phone,
                    str(self._api_id),
                    self._api_hash,
                ),
            )
            await self.abort_attempt(attempt_id)
            raise

    async def sign_in_with_code(
        self,
        attempt_id: str,
        session_path: Path,
        *,
        phone: str,
        code: str,
        phone_code_hash: str,
    ) -> AuthorizedAccount:
        attempt = await self._get_attempt(attempt_id)
        async with attempt.operation_lock:
            self._validate_attempt(
                attempt,
                session_path=session_path,
                phone=phone,
                phone_code_hash=phone_code_hash,
            )
            supplied_fingerprint = self._hash_fingerprint(phone_code_hash)
            logger.info(
                "Telegram code handler using auth state attempt_id=%s session=%s "
                "phone=%s hash_fingerprint=%s same_client=%s",
                attempt_id,
                self._session_label(attempt.session_path),
                self._mask_phone(phone),
                supplied_fingerprint,
                True,
            )
            try:
                await attempt.client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=phone_code_hash,
                )
                account = await self._get_account(attempt.client)
            except SessionPasswordNeededError:
                logger.info(
                    "Telegram code accepted; 2FA required attempt_id=%s session=%s",
                    attempt_id,
                    self._session_label(attempt.session_path),
                )
                raise
            except PhoneCodeExpiredError as exc:
                logger.warning(
                    "Telegram code sign-in failed attempt_id=%s exception=%s "
                    "hash_fingerprint=%s",
                    attempt_id,
                    type(exc).__name__,
                    supplied_fingerprint,
                )
                await self._finish_attempt_locked(attempt)
                raise
            except Exception as exc:
                logger.warning(
                    "Telegram code sign-in failed attempt_id=%s exception=%s message=%s "
                    "hash_fingerprint=%s",
                    attempt_id,
                    type(exc).__name__,
                    self._sanitize_error(exc, phone, code, phone_code_hash),
                    supplied_fingerprint,
                )
                # Invalid codes and transient failures retain the live auth attempt.
                raise

            await self._finish_attempt_locked(attempt)
            return account

    async def sign_in_with_password(
        self,
        attempt_id: str,
        session_path: Path,
        password: str,
    ) -> AuthorizedAccount:
        attempt = await self._get_attempt(attempt_id)
        async with attempt.operation_lock:
            self._validate_session(attempt, session_path)
            try:
                await attempt.client.sign_in(password=password)
                account = await self._get_account(attempt.client)
            except Exception as exc:
                logger.warning(
                    "Telegram 2FA sign-in failed attempt_id=%s exception=%s message=%s",
                    attempt_id,
                    type(exc).__name__,
                    self._sanitize_error(exc, password),
                )
                # Wrong passwords and transient failures retain the live auth attempt.
                raise

            await self._finish_attempt_locked(attempt)
            return account

    async def abort_attempt(self, attempt_id: str) -> None:
        async with self._attempts_lock:
            attempt = self._attempts.get(attempt_id)
        if attempt is None:
            return

        async with attempt.operation_lock:
            async with self._attempts_lock:
                if self._attempts.get(attempt_id) is not attempt:
                    return
                self._attempts.pop(attempt_id, None)
            await attempt.client.disconnect()
            self._protect_session_file(attempt.session_path)
            logger.info(
                "Telethon auth attempt closed attempt_id=%s session=%s",
                attempt_id,
                self._session_label(attempt.session_path),
            )

    async def shutdown(self) -> None:
        async with self._attempts_lock:
            attempt_ids = list(self._attempts)
        await asyncio.gather(
            *(self.abort_attempt(attempt_id) for attempt_id in attempt_ids),
            return_exceptions=True,
        )

    async def logout(self, session_path: Path) -> bool:
        client = self._client(session_path)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                return True
            return bool(await client.log_out())
        finally:
            await client.disconnect()
            self._protect_session_file(session_path)

    async def _get_attempt(self, attempt_id: str) -> _ActiveAuthAttempt:
        async with self._attempts_lock:
            attempt = self._attempts.get(attempt_id)
        if attempt is None:
            raise AuthAttemptMismatchError(
                "No live Telethon client exists for this authorization attempt"
            )
        return attempt

    async def _finish_attempt_locked(self, attempt: _ActiveAuthAttempt) -> None:
        async with self._attempts_lock:
            if self._attempts.get(attempt.attempt_id) is attempt:
                self._attempts.pop(attempt.attempt_id, None)
        await attempt.client.disconnect()
        self._protect_session_file(attempt.session_path)

    def _validate_attempt(
        self,
        attempt: _ActiveAuthAttempt,
        *,
        session_path: Path,
        phone: str,
        phone_code_hash: str,
    ) -> None:
        self._validate_session(attempt, session_path)
        if phone != attempt.normalized_phone:
            raise AuthAttemptMismatchError(
                "Normalized phone does not match the attempt"
            )
        if attempt.phone_code_hash is None or not secrets.compare_digest(
            phone_code_hash, attempt.phone_code_hash
        ):
            raise AuthAttemptMismatchError("phone_code_hash does not match the attempt")

    def _validate_session(
        self,
        attempt: _ActiveAuthAttempt,
        session_path: Path,
    ) -> None:
        resolved_path = session_path.resolve()
        if resolved_path != attempt.session_path:
            raise AuthAttemptMismatchError("Session path does not match the attempt")
        if attempt.session_identity is None:
            raise AuthAttemptMismatchError("Session identity was not recorded")
        if self._session_identity(resolved_path) != attempt.session_identity:
            raise AuthAttemptMismatchError("Temporary session file was replaced")

    @staticmethod
    def _protect_session_file(session_path: Path) -> None:
        try:
            Path(f"{session_path}.session").chmod(0o600)
        except OSError:
            pass

    @staticmethod
    def _session_identity(session_path: Path) -> tuple[int, int]:
        stat = Path(f"{session_path}.session").stat()
        return stat.st_dev, stat.st_ino

    @staticmethod
    def _session_label(session_path: Path) -> str:
        return f"{session_path.parent.name}/{session_path.name}.session"

    @staticmethod
    def _mask_phone(phone: str) -> str:
        return f"***{phone[-4:]}" if len(phone) >= 4 else "***"

    @staticmethod
    def _hash_fingerprint(phone_code_hash: str) -> str:
        return hashlib.sha256(phone_code_hash.encode()).hexdigest()[:6]

    @staticmethod
    def _identifier_fingerprint(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()[:8]

    @staticmethod
    def _delivery_type(sent_code_type: Any) -> str | None:
        type_name = type(sent_code_type).__name__
        if type_name == "SentCodeTypeApp":
            return "Telegram app"
        if type_name in {
            "SentCodeTypeSms",
            "SentCodeTypeFirebaseSms",
            "SentCodeTypeFragmentSms",
        }:
            return "SMS"
        if type_name in {
            "SentCodeTypeCall",
            "SentCodeTypeFlashCall",
            "SentCodeTypeMissedCall",
        }:
            return "call"
        return None

    @staticmethod
    def _timeout_seconds(value: Any) -> int | None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return value

    @staticmethod
    def _flood_wait_seconds(error: Exception) -> int | None:
        if isinstance(error, FloodWaitError):
            return error.seconds
        return None

    @staticmethod
    def _sanitize_error(error: Exception, *sensitive_values: str) -> str:
        message = str(error).replace("\n", " ").replace("\r", " ")
        for value in sensitive_values:
            if value:
                message = message.replace(value, "<redacted>")
        message = PHONE_LIKE_PATTERN.sub("<redacted-phone>", message)
        message = LONG_TOKEN_PATTERN.sub("<redacted-token>", message)
        return message[:300] or "no details"

    @staticmethod
    async def _get_account(client: TelegramClient) -> AuthorizedAccount:
        me = await client.get_me()
        if not isinstance(me, User):
            raise TypeError("Telegram did not return an authorized user")
        return AuthorizedAccount(
            id=me.id,
            username=me.username,
            first_name=me.first_name,
            phone=me.phone,
        )
