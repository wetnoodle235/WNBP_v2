# ──────────────────────────────────────────────────────────
# V5.0 Backend — In-Memory Rate Limiter Middleware
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


def _rate_limit_enabled() -> bool:
    """Return False when RATE_LIMIT_ENABLED=false/0/no (used in tests)."""
    return os.getenv("RATE_LIMIT_ENABLED", "true").lower() not in ("false", "0", "no")


@dataclass
class _Bucket:
    """Sliding-window token bucket for a single client."""
    tokens: float
    last_refill: float


@dataclass
class RateLimitConfig:
    """Rate limit configuration per endpoint group."""
    requests_per_second: float = 10.0
    burst: int = 20
    # Route prefix → custom limits (e.g. "/v1/sse" can be lower)
    group_limits: dict[str, tuple[float, int]] = field(default_factory=dict)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Token-bucket rate limiter keyed by client IP.

    Each client gets ``burst`` tokens; tokens refill at
    ``requests_per_second``.  When tokens are exhausted a 429 is
    returned with a ``Retry-After`` header.
    """

    def __init__(self, app, config: RateLimitConfig | None = None) -> None:  # type: ignore[override]
        super().__init__(app)
        self.config = config or RateLimitConfig()
        self._buckets: dict[str, _Bucket] = defaultdict(
            lambda: _Bucket(
                tokens=float(self.config.burst),
                last_refill=time.monotonic(),
            )
        )

    async def dispatch(
        self, request: Request, call_next: Callable
    ) -> Response:
        # Skip rate limiting when disabled (e.g. during tests)
        if not _rate_limit_enabled():
            return await call_next(request)

        # Skip rate limiting for health checks
        if request.url.path in ("/health", "/docs", "/openapi.json"):
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        rps, burst = self._get_limits(request.url.path)

        bucket = self._buckets[client_ip]
        now = time.monotonic()
        elapsed = now - bucket.last_refill
        bucket.tokens = min(float(burst), bucket.tokens + elapsed * rps)
        bucket.last_refill = now

        if bucket.tokens < 1.0:
            retry_after = (1.0 - bucket.tokens) / rps
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(int(retry_after) + 1)},
            )

        bucket.tokens -= 1.0
        response = await call_next(request)
        response.headers["X-RateLimit-Remaining"] = str(int(bucket.tokens))
        return response

    def _get_limits(self, path: str) -> tuple[float, int]:
        for prefix, limits in self.config.group_limits.items():
            if path.startswith(prefix):
                return limits
        return self.config.requests_per_second, self.config.burst

    @staticmethod
    def _get_client_ip(request: Request) -> str:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def cleanup_stale(self, max_age: float = 3600.0) -> int:
        """Remove buckets not seen for *max_age* seconds. Call periodically."""
        now = time.monotonic()
        stale = [
            ip
            for ip, b in self._buckets.items()
            if now - b.last_refill > max_age
        ]
        for ip in stale:
            del self._buckets[ip]
        return len(stale)
