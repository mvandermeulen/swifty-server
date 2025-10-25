"""Utilities for HTTP and WebSocket rate limiting and throttling."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from redis import Redis
from redis.exceptions import RedisError

from redis_store import REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT


@dataclass
class RateLimitStatus:
    """Represents the result of a rate-limit evaluation."""

    allowed: bool
    remaining: int
    reset_after: float


class RateLimiter:
    """Simple fixed-window rate limiter with Redis-backed persistence."""

    def __init__(
        self,
        limit: int,
        window_seconds: int,
        *,
        prefix: str,
        redis_client: Redis | None = None,
    ) -> None:
        self._limit = max(0, limit)
        self._window = max(1, window_seconds)
        self._prefix = prefix
        self._redis = redis_client or self._create_redis_client()
        self._lock = asyncio.Lock()
        self._counters: dict[str, list[float]] = {}

    @staticmethod
    def _create_redis_client() -> Redis | None:
        try:
            return Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                socket_timeout=0.5,
                socket_connect_timeout=0.5,
            )
        except RedisError:
            return None

    @property
    def enabled(self) -> bool:
        return self._limit > 0

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def window(self) -> int:
        return self._window

    async def check(self, identifier: str) -> RateLimitStatus:
        """Evaluate whether the caller identified by ``identifier`` is allowed."""

        if not self.enabled:
            return RateLimitStatus(True, -1, 0)

        if self._redis is not None:
            status = await asyncio.get_running_loop().run_in_executor(
                None, self._check_redis, identifier
            )
            if status is not None:
                return status

        return await self._check_memory(identifier)

    # Redis-backed implementation -----------------------------------------
    def _check_redis(self, identifier: str) -> RateLimitStatus | None:
        key = f"{self._prefix}:{identifier}"
        if self._redis is None:
            return None
        try:
            pipeline = self._redis.pipeline()
            pipeline.incr(key, 1)
            pipeline.ttl(key)
            current, ttl = pipeline.execute()
            if ttl == -1:
                self._redis.expire(key, self._window)
                ttl = self._window
            if int(current) <= self._limit:
                remaining = max(self._limit - int(current), 0)
                return RateLimitStatus(True, remaining, float(ttl))
            return RateLimitStatus(False, 0, float(max(ttl, 0)))
        except RedisError:
            return None

    # In-memory fallback implementation -----------------------------------
    async def _check_memory(self, identifier: str) -> RateLimitStatus:
        now = time.monotonic()
        async with self._lock:
            bucket = self._counters.setdefault(identifier, [])
            window_start = now - self._window
            while bucket and bucket[0] < window_start:
                bucket.pop(0)

            if len(bucket) >= self._limit:
                reset_after = self._window - (now - bucket[0])
                return RateLimitStatus(False, 0, max(reset_after, 0))

            bucket.append(now)
            remaining = max(self._limit - len(bucket), 0)
            reset_after = self._window - (now - bucket[0]) if bucket else self._window
            return RateLimitStatus(True, remaining, max(reset_after, 0))


class ConcurrencyLimiter:
    """Restrict the number of concurrent requests handled by the service."""

    def __init__(self, limit: int, timeout_seconds: float) -> None:
        self._limit = max(0, limit)
        self._timeout = max(0.0, timeout_seconds)
        self._semaphore: Optional[asyncio.Semaphore]
        if self._limit > 0:
            self._semaphore = asyncio.Semaphore(self._limit)
        else:
            self._semaphore = None

    @property
    def enabled(self) -> bool:
        return self._semaphore is not None

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def timeout(self) -> float:
        return self._timeout

    async def acquire(self) -> bool:
        if self._semaphore is None:
            return True
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=self._timeout)
        except asyncio.TimeoutError:
            return False
        return True

    def release(self) -> None:
        if self._semaphore is not None:
            self._semaphore.release()
