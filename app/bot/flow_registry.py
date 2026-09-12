from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)
TimeoutCallback = Callable[[], Awaitable[None]]


class LoginFlowRegistry:
    """Process-local guard against concurrent logins for one bot user."""

    def __init__(self) -> None:
        self._active_users: dict[int, asyncio.Task[None] | None] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, user_id: int) -> bool:
        async with self._lock:
            if user_id in self._active_users:
                return False
            self._active_users[user_id] = None
            return True

    async def pause_timeout(self, user_id: int) -> bool:
        """Pause expiry while an update for this login is being processed."""
        async with self._lock:
            if user_id not in self._active_users:
                return False
            task = self._active_users[user_id]
            self._active_users[user_id] = None
        if task is not None:
            task.cancel()
        return True

    async def arm_timeout(
        self,
        user_id: int,
        timeout_seconds: float,
        callback: TimeoutCallback,
    ) -> bool:
        async with self._lock:
            if user_id not in self._active_users:
                return False
            previous_task = self._active_users[user_id]
            task = asyncio.create_task(
                self._expire(user_id, timeout_seconds, callback),
                name=f"login-timeout-{user_id}",
            )
            self._active_users[user_id] = task
        if previous_task is not None:
            previous_task.cancel()
        return True

    async def release(self, user_id: int) -> None:
        async with self._lock:
            task = self._active_users.pop(user_id, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def shutdown(self) -> None:
        async with self._lock:
            tasks = [task for task in self._active_users.values() if task is not None]
            self._active_users.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _expire(
        self,
        user_id: int,
        timeout_seconds: float,
        callback: TimeoutCallback,
    ) -> None:
        try:
            await asyncio.sleep(timeout_seconds)
            current_task = asyncio.current_task()
            async with self._lock:
                if self._active_users.get(user_id) is not current_task:
                    return
                self._active_users.pop(user_id, None)
            await callback()
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001 - isolate background cleanup failures
            logger.error("Login timeout cleanup failed (%s)", type(exc).__name__)
