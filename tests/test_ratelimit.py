from __future__ import annotations

import pytest

from crm_sync.clock import ManualClock
from crm_sync.crm.ratelimit import DAY_MS, DailyLimitExceeded, RateLimiter, TokenBucket


def test_bucket_allows_burst_then_throttles() -> None:
    clock = ManualClock()
    rl = RateLimiter(clock, requests_per_10s=10)
    for _ in range(10):
        rl.acquire()
    assert clock.slept == []  # burst capacity
    rl.acquire()
    # Rolling window: the 11th request waits until the first one ages out of the 10 s window,
    # a plain token bucket would have let it through after 1 s and earned a 429.
    assert clock.slept[0] == pytest.approx(10.0, abs=0.05)
    assert rl.stats.throttled_waits == 1


def test_sustained_rate_matches_limit() -> None:
    clock = ManualClock()
    rl = RateLimiter(clock, requests_per_10s=100)
    start = clock.now_ms()
    for _ in range(300):
        rl.acquire()
    elapsed = (clock.now_ms() - start) / 1000
    assert elapsed == pytest.approx(20.0, abs=0.2)  # 100 burst + 200 at 10/s


def test_search_bucket_is_separate() -> None:
    clock = ManualClock()
    rl = RateLimiter(clock, requests_per_10s=100, search_per_second=4)
    for _ in range(4):
        rl.acquire("search")
    rl.acquire("general")
    assert clock.slept == []
    rl.acquire("search")
    assert clock.slept[-1] == pytest.approx(1.0, abs=0.05)


def test_daily_limit_fails_fast_and_resets_next_day() -> None:
    clock = ManualClock()
    rl = RateLimiter(clock, daily_limit=3)
    for _ in range(3):
        rl.acquire()
    with pytest.raises(DailyLimitExceeded):
        rl.acquire()
    clock.advance(DAY_MS)
    rl.acquire()


def test_429_pauses_all_traffic_for_retry_after() -> None:
    clock = ManualClock()
    rl = RateLimiter(clock, requests_per_10s=100)
    rl.on_rate_limited(2.5, attempt=0)
    t0 = clock.now_ms()
    rl.acquire()
    assert clock.now_ms() - t0 >= 2500
    assert rl.stats.cooldowns == 1


def test_server_headers_drain_local_bucket() -> None:
    clock = ManualClock()
    rl = RateLimiter(clock, requests_per_10s=100, daily_limit=1000)
    rl.observe_headers(
        {"x-hubspot-ratelimit-remaining": "0", "x-hubspot-ratelimit-daily-remaining": "10"}
    )
    assert rl.daily_used == 990
    rl.acquire()
    assert clock.slept  # had to wait because the server said the window was exhausted


def test_token_bucket_refills_but_caps_at_capacity() -> None:
    clock = ManualClock()
    b = TokenBucket(5, 1, clock)
    b.take(5)
    clock.advance(100_000)
    assert b.wait_time() == 0
    b._refill()
    assert b.tokens == 5
