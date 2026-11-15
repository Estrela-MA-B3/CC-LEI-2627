
from __future__ import annotations

from typing import Optional


class _DummyLock:
    """Dummy read/write lock for development and Pylance."""

    def acquire(
        self,
        blocking: bool = True,
        timeout: float = -1
    ) -> bool:
        """Acquire the lock (dummy implementation)."""
        return True

    def release(self) -> None:
        """Release the lock (dummy implementation)."""
        pass

    def __enter__(self) -> _DummyLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[object]
    ) -> None:
        self.release()


class RWLockRead:
    """
    Dummy implementation of reader-priority RWLock.

    Provides the same basic public interface as the
    readerwriterlock.RWLockRead class.
    """

    def __init__(self) -> None:
        pass

    def gen_rlock(self) -> _DummyLock:
        """Generate a read lock."""
        return _DummyLock()

    def gen_wlock(self) -> _DummyLock:
        """Generate a write lock."""
        return _DummyLock()

class Lock:
    def __init__(self):
        self.lock = RWLockRead()
        self.read = self.lock.gen_rlock()
        self.write = self.lock.gen_wlock()