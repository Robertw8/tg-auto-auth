from __future__ import annotations

import asyncio
import getpass
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    ApiIdPublishedFloodError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberInvalidError,
    RPCError,
    SessionPasswordNeededError,
)


def load_api_credentials() -> tuple[int, str]:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")

    api_id_value = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    if not api_id_value or not api_hash:
        raise RuntimeError(
            "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in the project .env"
        )

    try:
        api_id = int(api_id_value)
    except ValueError as exc:
        raise RuntimeError("TELEGRAM_API_ID must be an integer") from exc
    if api_id <= 0:
        raise RuntimeError("TELEGRAM_API_ID must be positive")
    return api_id, api_hash


def delivery_type(sent_code_type: Any) -> str:
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
    return "other"


async def run_diagnostic() -> None:
    api_id, api_hash = load_api_credentials()
    phone = input("Phone number in international format: ").strip()
    if not phone:
        raise RuntimeError("Phone number is required")

    with tempfile.TemporaryDirectory(prefix="tg-local-auth-") as temporary_dir:
        session_path = Path(temporary_dir) / "diagnostic"
        client = TelegramClient(str(session_path), api_id, api_hash)
        try:
            await client.connect()
            sent_code = await client.send_code_request(phone)
            phone_code_hash = sent_code.phone_code_hash

            print(f"Delivery type: {delivery_type(sent_code.type)}")
            print(f"phone_code_hash received: {bool(phone_code_hash)}")
            if not phone_code_hash:
                raise RuntimeError("Telegram did not return usable login state")

            code = input("Login code: ").strip().replace(" ", "").replace("-", "")
            try:
                await client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=phone_code_hash,
                )
            except SessionPasswordNeededError:
                code = ""
                password = getpass.getpass("2FA password: ")
                try:
                    await client.sign_in(password=password)
                finally:
                    password = ""
            finally:
                code = ""

            me = await client.get_me()
            if me is None:
                raise RuntimeError("Telegram did not return the authorized account")
            print(f"id: {me.id}")
            print(f"username: {me.username or '—'}")
            print(f"first_name: {me.first_name or '—'}")
        finally:
            await client.disconnect()


async def main() -> None:
    try:
        await run_diagnostic()
    except FloodWaitError as exc:
        print(f"Telegram FloodWait: wait {exc.seconds} seconds before trying again.")
    except PhoneNumberInvalidError:
        print("Telegram rejected the phone number as invalid.")
    except PhoneNumberBannedError:
        print("Telegram reports that this phone number is banned.")
    except ApiIdInvalidError:
        print("Telegram rejected TELEGRAM_API_ID or TELEGRAM_API_HASH.")
    except ApiIdPublishedFloodError:
        print("Telegram reports that this API ID is published or rate-limited.")
    except PhoneCodeInvalidError:
        print("Telegram rejected the login code as invalid.")
    except PhoneCodeExpiredError:
        print("Telegram rejected the login code as expired.")
    except PasswordHashInvalidError:
        print("Telegram rejected the 2FA password.")
    except (RPCError, OSError) as exc:
        print(f"Telegram/network failure: {type(exc).__name__}")
    except (EOFError, KeyboardInterrupt):
        print("Diagnostic cancelled.")


if __name__ == "__main__":
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    asyncio.run(main())
