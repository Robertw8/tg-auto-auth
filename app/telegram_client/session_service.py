from __future__ import annotations

import re
import secrets
from pathlib import Path


class SessionService:
    _KEY_PATTERN = re.compile(r"^[0-9]+_[0-9a-f]{32}$")

    def __init__(self, sessions_dir: Path) -> None:
        self._sessions_dir = sessions_dir.resolve()
        self._sessions_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._sessions_dir.chmod(0o700)

    def create_key(self, owner_telegram_id: int) -> str:
        return f"{owner_telegram_id}_{secrets.token_hex(16)}"

    def path_for(self, session_key: str) -> Path:
        if not self._KEY_PATTERN.fullmatch(session_key):
            raise ValueError("Invalid internal session key")
        path = (self._sessions_dir / session_key).resolve()
        if path.parent != self._sessions_dir:
            raise ValueError("Session path escaped the configured directory")
        return path

    def exists(self, session_key: str) -> bool:
        return Path(f"{self.path_for(session_key)}.session").is_file()

    def delete(self, session_key: str) -> bool:
        base = self.path_for(session_key)
        session_file = Path(f"{base}.session")
        candidates = (
            session_file,
            Path(f"{session_file}-journal"),
            Path(f"{session_file}-wal"),
            Path(f"{session_file}-shm"),
        )
        for candidate in candidates:
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                # A later cleanup/retry can remove a locked artifact.
                pass
        return not any(candidate.exists() for candidate in candidates)

    def cleanup_unregistered(self, registered_keys: set[str]) -> int:
        """Remove partial sessions left by interrupted authorization flows."""
        discovered_keys = {
            path.name.partition(".session")[0]
            for path in self._sessions_dir.glob("*.session*")
        }
        orphaned_keys = {
            key
            for key in discovered_keys - registered_keys
            if self._KEY_PATTERN.fullmatch(key)
        }
        return sum(self.delete(key) for key in orphaned_keys)
