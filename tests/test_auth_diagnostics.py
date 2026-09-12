from __future__ import annotations

import logging
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from telethon.errors import FloodWaitError

from app.telegram_client.auth_service import (
    AuthService,
    DuplicateCodeRequestError,
)


class SentCodeTypeApp:
    pass


class SentCodeTypeSms:
    pass


class _FakeSentCode:
    phone_code_hash = "private-phone-code-hash"
    type = SentCodeTypeApp()
    next_type = SentCodeTypeSms()
    timeout = 45


class _FakeClient:
    def __init__(self, session_path: Path, response: Any = None) -> None:
        self._session_file = Path(f"{session_path}.session")
        self._response = response if response is not None else _FakeSentCode()
        self.send_code_calls = 0
        self.disconnected = False

    async def connect(self) -> None:
        self._session_file.touch(exist_ok=True)

    async def send_code_request(self, phone: str) -> Any:
        del phone
        self.send_code_calls += 1
        if isinstance(self._response, Exception):
            raise self._response
        return self._response

    async def disconnect(self) -> None:
        self.disconnected = True


class AuthDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_diagnostic_contains_safe_delivery_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_path = Path(directory) / "login"
            client = _FakeClient(session_path)
            service = AuthService(123456, "a" * 32)
            with (
                patch.object(service, "_client", return_value=client),
                self.assertLogs(
                    "app.telegram_client.auth_service", level=logging.INFO
                ) as captured,
            ):
                await service.request_code(
                    "attempt-one",
                    session_path,
                    "+48123456789",
                    control_user_id=7,
                )
                await service.abort_attempt("attempt-one")

        output = "\n".join(captured.output)
        self.assertEqual(client.send_code_calls, 1)
        self.assertIn("timestamp=", output)
        self.assertIn("attempt_id=attempt-one", output)
        self.assertIn("phone=***6789", output)
        self.assertIn("api_id_fingerprint=", output)
        self.assertIn("active_same_phone_attempts=0", output)
        self.assertIn("sent_code_returned=True", output)
        self.assertIn("delivery_type=Telegram app", output)
        self.assertIn("next_type=SMS", output)
        self.assertIn("timeout_seconds=45", output)
        self.assertIn("phone_code_hash_returned=True", output)
        self.assertNotIn("+48123456789", output)
        self.assertNotIn("private-phone-code-hash", output)
        self.assertNotIn("a" * 32, output)

    async def test_duplicate_attempt_is_logged_and_not_sent_twice(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_path = Path(directory) / "login"
            client = _FakeClient(session_path)
            service = AuthService(123456, "a" * 32)
            with (
                patch.object(service, "_client", return_value=client),
                self.assertLogs(
                    "app.telegram_client.auth_service", level=logging.WARNING
                ) as captured,
            ):
                await service.request_code(
                    "attempt-one",
                    session_path,
                    "+48123456789",
                    control_user_id=7,
                )
                with self.assertRaises(DuplicateCodeRequestError):
                    await service.request_code(
                        "attempt-one",
                        session_path,
                        "+48123456789",
                        control_user_id=7,
                    )
                await service.abort_attempt("attempt-one")

        self.assertEqual(client.send_code_calls, 1)
        self.assertIn("duplicate rejected", "\n".join(captured.output))

    async def test_same_phone_active_attempt_is_observable_without_blocking(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first_path = Path(directory) / "first"
            second_path = Path(directory) / "second"
            first_client = _FakeClient(first_path)
            second_client = _FakeClient(second_path)
            service = AuthService(123456, "a" * 32)
            with (
                patch.object(
                    service, "_client", side_effect=[first_client, second_client]
                ),
                self.assertLogs(
                    "app.telegram_client.auth_service", level=logging.INFO
                ) as captured,
            ):
                await service.request_code(
                    "attempt-one",
                    first_path,
                    "+48123456789",
                    control_user_id=7,
                )
                await service.request_code(
                    "attempt-two",
                    second_path,
                    "+48123456789",
                    control_user_id=8,
                )
                await service.shutdown()

        self.assertIn("active_same_phone_attempts=1", "\n".join(captured.output))
        self.assertEqual(first_client.send_code_calls, 1)
        self.assertEqual(second_client.send_code_calls, 1)

    async def test_flood_wait_seconds_and_exception_class_are_logged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_path = Path(directory) / "login"
            client = _FakeClient(session_path, FloodWaitError(None, 17))
            service = AuthService(123456, "a" * 32)
            with (
                patch.object(service, "_client", return_value=client),
                self.assertLogs(
                    "app.telegram_client.auth_service", level=logging.WARNING
                ) as captured,
                self.assertRaises(FloodWaitError),
            ):
                await service.request_code(
                    "attempt-flood",
                    session_path,
                    "+48123456789",
                    control_user_id=7,
                )

        output = "\n".join(captured.output)
        self.assertIn("exception=FloodWaitError", output)
        self.assertIn("flood_wait_seconds=17", output)
        self.assertIn("sent_code_returned=False", output)
        self.assertIn("phone_code_hash_returned=False", output)
        self.assertNotIn("+48123456789", output)

    async def test_request_exception_redacts_config_and_phone(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_path = Path(directory) / "login"
            api_hash = "b" * 32
            client = _FakeClient(
                session_path,
                RuntimeError(f"phone=+48123456789 api_id=123456 api_hash={api_hash}"),
            )
            service = AuthService(123456, api_hash)
            with (
                patch.object(service, "_client", return_value=client),
                self.assertLogs(
                    "app.telegram_client.auth_service", level=logging.WARNING
                ) as captured,
                self.assertRaises(RuntimeError),
            ):
                await service.request_code(
                    "attempt-redaction",
                    session_path,
                    "+48123456789",
                    control_user_id=7,
                )

        output = "\n".join(captured.output)
        self.assertNotIn("+48123456789", output)
        self.assertNotIn("api_id=123456", output)
        self.assertNotIn(api_hash, output)
        self.assertIn("api_id_fingerprint=", output)


if __name__ == "__main__":
    unittest.main()
