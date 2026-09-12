from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

from aiogram.utils.web_app import WebAppInitData, safe_parse_webapp_init_data
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from telethon.errors import (
    ApiIdInvalidError,
    ApiIdPublishedFloodError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberFloodError,
    PhoneNumberInvalidError,
    RPCError,
)

from app.config import Config
from app.telegram_client import (
    AuthAttemptMismatchError,
    AuthorizedAccount,
    DuplicateCodeRequestError,
    PhoneCodeHashMissingError,
)
from app.web.auth_flow import (
    AttemptAccessError,
    AttemptConflictError,
    CodeRateLimitError,
    WebAuthCoordinator,
)

logger = logging.getLogger(__name__)
PHONE_PATTERN = re.compile(r"^\+[1-9]\d{7,14}$")
CODE_PATTERN = re.compile(r"^\d{4,8}$")
INIT_DATA_MAX_AGE = timedelta(minutes=5)
INIT_DATA_FUTURE_TOLERANCE = timedelta(seconds=30)


class BootstrapBody(BaseModel):
    launch_token: str = Field(min_length=32, max_length=128)


class PhoneBody(BaseModel):
    phone: str = Field(min_length=8, max_length=16)


class CodeBody(BaseModel):
    code: str = Field(min_length=4, max_length=16)


class PasswordBody(BaseModel):
    password: str = Field(min_length=1, max_length=256)


def create_web_app(config: Config, coordinator: WebAuthCoordinator) -> FastAPI:
    static_dir = Path(__file__).resolve().parent / "static"
    public_url = urlsplit(config.webapp_public_url)
    expected_origin = f"{public_url.scheme}://{public_url.netloc}"
    app = FastAPI(
        title="Авторизация аккаунта Telegram",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.middleware("http")
    async def security_headers(request: Request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=(), payment=()"
        )
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; script-src 'self' https://telegram.org; "
            "style-src 'self'; connect-src 'self'; img-src 'self' data:; "
            "base-uri 'none'; form-action 'self'; frame-ancestors 'self' "
            "https://web.telegram.org https://*.telegram.org"
        )
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        del request, exc
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": "Некорректный запрос."},
        )

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        del request
        logger.error("Unhandled Web App error (%s)", type(exc).__name__)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Произошла непредвиденная ошибка сервера."},
        )

    @app.get("/auth", include_in_schema=False)
    async def auth_page() -> FileResponse:
        return FileResponse(static_dir / "auth.html")

    @app.get("/healthz", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    def validated_user_id(init_data: str) -> int:
        try:
            parsed: WebAppInitData = safe_parse_webapp_init_data(
                config.bot_token, init_data
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Данные авторизации Telegram некорректны.",
            ) from exc
        now = datetime.now(timezone.utc)
        auth_date = parsed.auth_date
        if auth_date.tzinfo is None:
            auth_date = auth_date.replace(tzinfo=timezone.utc)
        age = now - auth_date
        if (
            parsed.user is None
            or age > INIT_DATA_MAX_AGE
            or age < -INIT_DATA_FUTURE_TOLERANCE
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Данные авторизации Telegram отсутствуют или истекли.",
            )
        return parsed.user.id

    def enforce_origin(request: Request) -> None:
        if request.headers.get("origin") != expected_origin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Источник запроса не разрешён.",
            )

    async def browser_identity(
        request: Request,
        x_telegram_init_data: str,
        x_auth_session: str,
        x_csrf_token: str,
    ) -> tuple[int, str, str]:
        enforce_origin(request)
        return validated_user_id(x_telegram_init_data), x_auth_session, x_csrf_token

    @app.post("/api/auth/bootstrap")
    async def bootstrap(
        body: BootstrapBody,
        request: Request,
        x_telegram_init_data: str = Header(alias="X-Telegram-Init-Data"),
    ) -> dict[str, object]:
        enforce_origin(request)
        user_id = validated_user_id(x_telegram_init_data)
        try:
            authorization = await coordinator.redeem_launch_token(
                body.launch_token, user_id
            )
        except AttemptAccessError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Ссылка авторизации некорректна, истекла или уже использована.",
            ) from exc
        finally:
            body.launch_token = ""
        return {
            "sessionToken": authorization.session_token,
            "csrfToken": authorization.csrf_token,
            "expiresIn": authorization.expires_in,
        }

    @app.post("/api/auth/phone")
    async def send_code(
        body: PhoneBody,
        request: Request,
        x_telegram_init_data: str = Header(alias="X-Telegram-Init-Data"),
        x_auth_session: str = Header(alias="X-Auth-Session"),
        x_csrf_token: str = Header(alias="X-CSRF-Token"),
    ) -> dict[str, object]:
        user_id, browser_token, csrf_token = await browser_identity(
            request, x_telegram_init_data, x_auth_session, x_csrf_token
        )
        phone = body.phone.strip()
        body.phone = ""
        if not PHONE_PATTERN.fullmatch(phone):
            phone = ""
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Введите корректный международный номер телефона со знаком +.",
            )
        try:
            result = await coordinator.request_code(
                browser_token=browser_token,
                csrf_token=csrf_token,
                telegram_user_id=user_id,
                phone=phone,
            )
        except Exception as exc:
            _raise_code_request_http_error(exc)
            raise
        finally:
            phone = ""
        return {"next": "code", "deliveryType": result.delivery_type or "unknown"}

    @app.post("/api/auth/code")
    async def submit_code(
        body: CodeBody,
        request: Request,
        x_telegram_init_data: str = Header(alias="X-Telegram-Init-Data"),
        x_auth_session: str = Header(alias="X-Auth-Session"),
        x_csrf_token: str = Header(alias="X-CSRF-Token"),
    ) -> dict[str, object]:
        user_id, browser_token, csrf_token = await browser_identity(
            request, x_telegram_init_data, x_auth_session, x_csrf_token
        )
        code = body.code.replace(" ", "").replace("-", "")
        body.code = ""
        if not CODE_PATTERN.fullmatch(code):
            code = ""
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Введите код входа Telegram, используя только цифры.",
            )
        try:
            result = await coordinator.sign_in_with_code(
                browser_token=browser_token,
                csrf_token=csrf_token,
                telegram_user_id=user_id,
                code=code,
            )
        except Exception as exc:
            _raise_telethon_http_error(exc)
            raise
        finally:
            code = ""
        if result.password_required:
            return {"next": "password"}
        if result.account is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Telegram не вернул данные подключённого аккаунта.",
            )
        return _success_response(result.account)

    @app.post("/api/auth/password")
    async def submit_password(
        body: PasswordBody,
        request: Request,
        x_telegram_init_data: str = Header(alias="X-Telegram-Init-Data"),
        x_auth_session: str = Header(alias="X-Auth-Session"),
        x_csrf_token: str = Header(alias="X-CSRF-Token"),
    ) -> dict[str, object]:
        user_id, browser_token, csrf_token = await browser_identity(
            request, x_telegram_init_data, x_auth_session, x_csrf_token
        )
        password = body.password
        body.password = ""
        try:
            account = await coordinator.sign_in_with_password(
                browser_token=browser_token,
                csrf_token=csrf_token,
                telegram_user_id=user_id,
                password=password,
            )
        except Exception as exc:
            _raise_telethon_http_error(exc)
            raise
        finally:
            password = ""
        return _success_response(account)

    @app.post("/api/auth/cancel", status_code=status.HTTP_204_NO_CONTENT)
    async def cancel(
        request: Request,
        x_telegram_init_data: str = Header(alias="X-Telegram-Init-Data"),
        x_auth_session: str = Header(alias="X-Auth-Session"),
        x_csrf_token: str = Header(alias="X-CSRF-Token"),
    ) -> None:
        user_id, browser_token, csrf_token = await browser_identity(
            request, x_telegram_init_data, x_auth_session, x_csrf_token
        )
        try:
            await coordinator.cancel_browser_attempt(
                browser_token=browser_token,
                csrf_token=csrf_token,
                telegram_user_id=user_id,
            )
        except AttemptAccessError:
            return

    return app


def _success_response(account: AuthorizedAccount) -> dict[str, object]:
    return {
        "next": "success",
        "account": {
            "id": account.id,
            "username": account.username,
            "firstName": account.first_name,
            "phone": account.phone,
        },
    }


def _raise_telethon_http_error(exc: Exception) -> None:
    if isinstance(exc, AttemptAccessError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Попытка авторизации истекла. Вернитесь в бот и попробуйте снова.",
        ) from exc
    if isinstance(exc, AttemptConflictError):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Этот шаг авторизации уже отправлен.",
        ) from exc
    if isinstance(exc, CodeRateLimitError):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Telegram временно ограничил новые попытки. Попробуйте позже.",
            headers={"Retry-After": str(exc.retry_after)},
        ) from exc
    if isinstance(exc, FloodWaitError):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Telegram временно ограничил новые попытки. Попробуйте позже.",
            headers={"Retry-After": str(exc.seconds)},
        ) from exc
    if isinstance(exc, PhoneNumberInvalidError):
        detail = "Проверьте номер и международный код страны."
    elif isinstance(exc, PhoneNumberBannedError):
        detail = "Telegram не разрешает подключить этот номер."
    elif isinstance(exc, PhoneNumberFloodError):
        detail = "Telegram временно ограничил новые попытки. Попробуйте позже."
    elif isinstance(exc, PhoneCodeInvalidError):
        detail = "Неверный код. Проверьте его и попробуйте ещё раз."
    elif isinstance(exc, PhoneCodeExpiredError):
        detail = "Код больше не действует. Запросите новый."
    elif isinstance(exc, PasswordHashInvalidError):
        detail = "Неверный пароль 2FA."
    elif isinstance(exc, (ApiIdInvalidError, ApiIdPublishedFloodError)):
        detail = "Telegram отклонил данные приложения."
    elif isinstance(
        exc,
        (
            AuthAttemptMismatchError,
            DuplicateCodeRequestError,
            PhoneCodeHashMissingError,
        ),
    ):
        detail = (
            "Состояние авторизации больше нельзя использовать. Начните заново в боте."
        )
    elif isinstance(exc, (RPCError, OSError)):
        detail = (
            "Не удалось связаться с Telegram. Проверьте соединение и попробуйте снова."
        )
    elif isinstance(exc, (sqlite3.Error,)):
        detail = "Не удалось сохранить подключённый аккаунт. Новая сессия удалена."
    else:
        return
    status_code = (
        status.HTTP_400_BAD_REQUEST
        if isinstance(
            exc,
            (
                PhoneNumberInvalidError,
                PhoneNumberBannedError,
                PhoneNumberFloodError,
                PhoneCodeInvalidError,
                PhoneCodeExpiredError,
                PasswordHashInvalidError,
            ),
        )
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    raise HTTPException(status_code=status_code, detail=detail) from exc


def _raise_code_request_http_error(exc: Exception) -> None:
    if isinstance(exc, CodeRateLimitError):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Telegram временно ограничил новые попытки. Попробуйте позже.",
            headers={"Retry-After": str(exc.retry_after)},
        ) from exc
    if isinstance(exc, FloodWaitError):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Telegram временно ограничил новые попытки. Попробуйте позже.",
            headers={"Retry-After": str(exc.seconds)},
        ) from exc
    if isinstance(exc, (PhoneNumberInvalidError, PhoneNumberBannedError)):
        detail = "Проверьте номер и международный код страны."
    elif isinstance(exc, PhoneNumberFloodError):
        detail = "Telegram временно ограничил новые попытки. Попробуйте позже."
    elif isinstance(exc, (RPCError, OSError)):
        detail = (
            "Не удалось связаться с Telegram. Проверьте соединение и попробуйте снова."
        )
    else:
        _raise_telethon_http_error(exc)
        return
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from exc
