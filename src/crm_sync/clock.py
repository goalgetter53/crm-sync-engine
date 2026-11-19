"""Injectable clocks. Every time-dependent component takes a Clock so tests are deterministic."""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...

    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class ManualClock:
    """Test clock: ``sleep`` advances virtual time instantly and is recorded."""

    def __init__(self, start_ms: int = 1_767_225_600_000) -> None:  # 2026-01-01T00:00Z
        self._t = start_ms
        self._lock = threading.Lock()
        self.slept: list[float] = []

    def now_ms(self) -> int:
        with self._lock:
            return self._t

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.advance(int(seconds * 1000))

    def advance(self, ms: int) -> None:
        with self._lock:
            self._t += max(0, ms)
