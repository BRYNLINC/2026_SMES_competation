from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import BinaryIO


class ConcurrentPytestRunError(RuntimeError):
    pass


class PytestRunLock:
    def __init__(self, artifacts_root: Path, run_id: str) -> None:
        artifacts_root = Path(artifacts_root)
        self.lock_path = artifacts_root / ".pytest_session.lock"
        self.metadata_path = artifacts_root / ".pytest_session.json"
        self.run_id = str(run_id)
        self.owner_token = f"{os.getpid()}:{self.run_id}:{uuid.uuid4().hex}"
        self._lock_file: BinaryIO | None = None

    def acquire(self) -> None:
        if self._lock_file is not None:
            return

        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = self.lock_path.open("a+b")
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)

        try:
            self._acquire_os_lock(lock_file)
        except OSError as exc:
            lock_file.close()
            owner_payload = self._read_metadata()
            owner_text = (
                json.dumps(owner_payload, ensure_ascii=False, sort_keys=True)
                if owner_payload
                else "unavailable"
            )
            raise ConcurrentPytestRunError(
                f"another pytest session is active; lock_path={self.lock_path}; owner={owner_text}"
            ) from exc

        self._lock_file = lock_file
        try:
            self._write_metadata()
        except BaseException:
            self.release()
            raise

    def release(self) -> None:
        lock_file = self._lock_file
        if lock_file is None:
            return

        try:
            owner_payload = self._read_metadata()
            if owner_payload.get("owner_token") == self.owner_token:
                self.metadata_path.unlink(missing_ok=True)
        except OSError:
            pass
        finally:
            try:
                self._release_os_lock(lock_file)
            except OSError:
                pass
            finally:
                lock_file.close()
                self._lock_file = None

    def _write_metadata(self) -> None:
        payload = {
            "owner_token": self.owner_token,
            "pid": os.getpid(),
            "run_id": self.run_id,
            "started_at": datetime.now().astimezone().isoformat(),
            "cwd": str(Path.cwd()),
            "command_line": subprocess.list2cmdline([str(part) for part in sys.argv]),
        }
        temp_path = self.metadata_path.with_name(
            f".{self.metadata_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            temp_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            os.replace(temp_path, self.metadata_path)
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _read_metadata(self) -> dict:
        try:
            payload = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _acquire_os_lock(lock_file: BinaryIO) -> None:
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            return

        import fcntl

        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _release_os_lock(lock_file: BinaryIO) -> None:
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            return

        import fcntl

        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
