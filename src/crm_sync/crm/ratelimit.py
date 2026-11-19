"""Client-side rate-limit scheduler.

* Token bucket for the 10-second burst limit (HubSpot private apps: 100 or 190 / 10 s), plus an
  exact rolling-window guard because HubSpot's windows roll.
* A separate token bucket for the CRM search API (5 req/s per account; we default to 4).
* A daily budget that fails fast instead of burning the account's shared daily quota.
* A global cool-down when the server says 429, honouring ``Retry-After`` when present.
* ``X-HubSpot-RateLimit-Remaining`` headers drain the local bucket so several processes sharing
  one app converge on the server's view.
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from crm_sync.clock import Clock

Kind = Literal["general", "search"]
DAY_MS = 86_400_000


class DailyLimitExceeded(RuntimeError):
    pass


class TokenBucket:
    def __init__(self, capacity: float, refill_per_sec: float, clock: Clock) -> None:
        self.capacity = float(capacity)
        self.rate = float(refill_per_sec)
        self.clock = clock
        self.tokens = float(capacity)
        self._last = clock.now_ms()

    def _refill(self) -> None:
        now = self.clock.now_ms()
        elapsed = max(0, now - self._last) / 1000.0
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self._last = now

    def wait_time(self, n: float = 1.0) -> float:
        self._refill()
        if self.tokens >= n:
            return 0.0
        return (n - self.tokens) / self.rate

    def take(self, n: float = 1.0) -> None:
        self._refill()
        self.tokens -= n

    def drain_to(self, remaining: float) -> None:
        self._refill()
        self.tokens = min(self.tokens, max(0.0, remaining))


class RollingWindow:
    """Exact "at most N requests in any W ms" guard (HubSpot's limits are rolling windows).

    A token bucket alone admits ``capacity + rate * W`` requests in a window of length W, which
    overshoots a rolling limit right after a burst; this log of send times closes that gap.
    """

    def __init__(self, limit: int, window_ms: int, clock: Clock, margin_ms: int = 25) -> None:
        self.limit = limit
        self.window_ms = window_ms
        # The server timestamps arrival, we timestamp departure: pad for one-way latency.
        self.margin_ms = margin_ms
        self.clock = clock
        self._sent: deque[int] = deque()

    def wait_time(self) -> float:
        now = self.clock.now_ms()
        while self._sent and self._sent[0] <= now - self.window_ms:
            self._sent.popleft()
        if len(self._sent) < self.limit:
            return 0.0
        return (self._sent[0] + self.window_ms + self.margin_ms - now) / 1000.0

    def record(self) -> None:
        self._sent.append(self.clock.now_ms())


@dataclass
class LimiterStats:
    acquired: int = 0
    throttled_waits: int = 0
    throttled_seconds: float = 0.0
    cooldowns: int = 0
    cooldown_seconds: float = 0.0


class RateLimiter:
    def __init__(
        self,
        clock: Clock,
        *,
        requests_per_10s: int = 100,
        search_per_second: float = 4.0,
        daily_limit: int = 250_000,
    ) -> None:
        self.clock = clock
        self.general = TokenBucket(requests_per_10s, requests_per_10s / 10.0, clock)
        self.search = TokenBucket(max(1.0, search_per_second), search_per_second, clock)
        self.general_window = RollingWindow(requests_per_10s, 10_000, clock)
        self.search_window = RollingWindow(max(1, int(search_per_second)), 1_000, clock)
        self.daily_limit = daily_limit
        self.daily_used = 0
        self._day = clock.now_ms() // DAY_MS
        self._cooldown_until = 0
        self._lock = threading.Lock()
        self.stats = LimiterStats()

    def acquire(self, kind: Kind = "general") -> None:
        with self._lock:
            day = self.clock.now_ms() // DAY_MS
            if day != self._day:
                self._day, self.daily_used = day, 0
            if self.daily_used >= self.daily_limit:
                raise DailyLimitExceeded(
                    f"daily request budget of {self.daily_limit} exhausted; resume tomorrow"
                )
            while True:
                now = self.clock.now_ms()
                wait = max(0.0, (self._cooldown_until - now) / 1000.0)
                bucket = self.search if kind == "search" else self.general
                window = self.search_window if kind == "search" else self.general_window
                wait = max(wait, bucket.wait_time(), window.wait_time())
                if wait <= 0:
                    break
                self.stats.throttled_waits += 1
                self.stats.throttled_seconds += wait
                self.clock.sleep(wait)
            (self.search if kind == "search" else self.general).take()
            (self.search_window if kind == "search" else self.general_window).record()
            self.daily_used += 1
            self.stats.acquired += 1

    def on_rate_limited(self, retry_after_s: float | None, attempt: int) -> float:
        """Server returned 429: pause *all* traffic. Returns the delay applied (seconds)."""
        delay = retry_after_s if retry_after_s is not None else min(30.0, 0.5 * 2**attempt)
        with self._lock:
            until = self.clock.now_ms() + int(delay * 1000)
            self._cooldown_until = max(self._cooldown_until, until)
            self.general.drain_to(0)
            self.stats.cooldowns += 1
            self.stats.cooldown_seconds += delay
        return delay

    def observe_headers(self, headers: Mapping[str, str]) -> None:
        with self._lock:
            remaining = headers.get("x-hubspot-ratelimit-remaining")
            if remaining is not None and remaining.isdigit():
                self.general.drain_to(float(remaining))
            daily_left = headers.get("x-hubspot-ratelimit-daily-remaining")
            if daily_left is not None and daily_left.isdigit():
                self.daily_used = max(self.daily_used, self.daily_limit - int(daily_left))
