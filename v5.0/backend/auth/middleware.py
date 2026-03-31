# ──────────────────────────────────────────────────────────
# V5.0 Backend — API Key Auth Middleware & Dependencies
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, Request, status

from .models import APIKeyInfo, User, get_api_key_by_hash, get_user_by_api_key, hash_api_key
from .tiers import TIERS, tier_allows_endpoint, tier_allows_features, tier_allows_sport

# Load .env so os.getenv picks up PLATFORM_API_KEY etc.
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    load_dotenv(_env_file, override=False)


# ── Tier result limits ───────────────────────────────────

TIER_RESULT_LIMITS = {
    "free": 10,
    "starter": 200,
    "pro": 1000,
    "enterprise": 999999,  # unlimited
}

# Free tier only gets these endpoint types
FREE_ENDPOINTS = {"games", "standings", "news"}


def _auth_enabled() -> bool:
    """Check if auth is required. Respects both AUTH_ENABLED and REQUIRE_AUTH env vars."""
    for var in ("AUTH_ENABLED", "REQUIRE_AUTH"):
        val = os.getenv(var, "").lower()
        if val in ("true", "1", "yes"):
            return True
    return False


def _get_platform_api_key() -> str:
    return os.getenv("PLATFORM_API_KEY", "")


# ── In-memory sliding-window rate limiter ────────────────

class _RateCounter:
    """Per-key daily request counter with sliding window."""

    def __init__(self) -> None:
        self._windows: dict[str, list[float]] = defaultdict(list)

    def check(self, key_id: str, limit: int) -> tuple[bool, int]:
        """Return (allowed, remaining). Window = 86400 s (1 day)."""
        now = time.time()
        window = 86400.0
        timestamps = self._windows[key_id]
        cutoff = now - window
        timestamps[:] = [t for t in timestamps if t > cutoff]
        if len(timestamps) >= limit:
            return False, 0
        timestamps.append(now)
        return True, limit - len(timestamps)


_rate_counter = _RateCounter()


def _is_platform_key(raw_key: str) -> bool:
    """Check if the provided key matches the internal platform key."""
    platform_key = _get_platform_api_key()
    return bool(platform_key and raw_key == platform_key)


def _make_platform_key_info() -> APIKeyInfo:
    """Return a synthetic unlimited key for platform/internal requests."""
    return APIKeyInfo(
        key_id="__platform__",
        user_id="__platform__",
        tier="enterprise",
        sport_access=["all"],
        rate_limit=999999,
        name="platform",
    )


def _make_anonymous_key_info() -> APIKeyInfo:
    """Return a synthetic unlimited key when auth is disabled."""
    return APIKeyInfo(
        key_id="__anonymous__",
        user_id="__anonymous__",
        tier="enterprise",
        sport_access=["all"],
        rate_limit=999999,
        name="anonymous",
    )


# ── Dependencies ─────────────────────────────────────────

async def resolve_api_key_user(request: Request) -> Optional[User]:
    """Try to resolve a User from the API key in request (X-API-Key header or api_key param).
    Returns None if no key provided. Raises 401 for invalid keys."""
    raw_key: Optional[str] = request.headers.get("X-API-Key")
    if raw_key is None:
        raw_key = request.query_params.get("api_key")
    if raw_key is None:
        return None

    # Platform key bypass
    if _is_platform_key(raw_key):
        return None  # handled separately

    # Look up user by api_key column in users table
    user = await get_user_by_api_key(raw_key)
    if user:
        return user

    # Fall back to legacy hashed api_keys table
    key_hash = hash_api_key(raw_key)
    info = await get_api_key_by_hash(key_hash)
    if info is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key",
        )
    return None


async def get_api_key(request: Request) -> Optional[APIKeyInfo]:
    """Extract and validate API key from X-API-Key header or ?api_key= param.
    Returns None when auth is disabled or no key is provided."""
    raw_key: Optional[str] = request.headers.get("X-API-Key")
    if raw_key is None:
        raw_key = request.query_params.get("api_key")
    if raw_key is None:
        return None

    # Platform key bypass
    if _is_platform_key(raw_key):
        return _make_platform_key_info()

    # Check users table first (new system: api_key stored directly)
    user = await get_user_by_api_key(raw_key)
    if user:
        return APIKeyInfo(
            key_id=user.api_key,
            user_id=user.id,
            tier=user.tier,
            sport_access=["all"] if user.tier in ("pro", "enterprise") else TIERS.get(user.tier, TIERS["free"]).get("sports", ["nba"]),
            rate_limit=TIERS.get(user.tier, TIERS["free"])["rate_limit"],
            name=user.display_name or user.email,
        )

    # Fall back to legacy hashed api_keys table
    key_hash = hash_api_key(raw_key)
    info = await get_api_key_by_hash(key_hash)
    if info is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key",
        )
    return info


async def require_api_key(
    api_key: Optional[APIKeyInfo] = Depends(get_api_key),
) -> APIKeyInfo:
    """Dependency that requires a valid API key when auth is enabled."""
    if not _auth_enabled():
        return api_key or _make_anonymous_key_info()
    if api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required. Pass via X-API-Key header or ?api_key= parameter.",
        )
    return api_key


async def check_tier_access(
    api_key: APIKeyInfo,
    sport: str,
    endpoint: str,
) -> None:
    """Raise 403 if the key's tier doesn't allow this sport/endpoint."""
    if not _auth_enabled():
        return
    if api_key.key_id in ("__platform__", "__anonymous__"):
        return

    # Free tier endpoint restriction
    if api_key.tier == "free" and endpoint not in FREE_ENDPOINTS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Your 'free' tier only includes: {', '.join(sorted(FREE_ENDPOINTS))}. Upgrade at /auth/tiers",
        )

    if not tier_allows_sport(api_key.tier, sport):
        if api_key.sport_access != ["all"] and sport not in api_key.sport_access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Your '{api_key.tier}' tier does not include access to '{sport}'. Upgrade at /auth/tiers",
            )

    if not tier_allows_endpoint(api_key.tier, endpoint):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Your '{api_key.tier}' tier does not include the '{endpoint}' endpoint. Upgrade at /auth/tiers",
        )


async def check_features_access(api_key: APIKeyInfo) -> None:
    """Raise 403 if the key's tier doesn't allow feature data."""
    if not _auth_enabled():
        return
    if api_key.key_id in ("__platform__", "__anonymous__"):
        return
    if not tier_allows_features(api_key.tier):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Feature data requires 'pro' tier or higher. Upgrade at /auth/tiers",
        )


async def rate_limit_check(api_key: APIKeyInfo) -> None:
    """Check rate limits using in-memory counter with sliding window."""
    if not _auth_enabled():
        return
    if api_key.key_id in ("__platform__", "__anonymous__"):
        return

    allowed, remaining = _rate_counter.check(api_key.key_id, api_key.rate_limit)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Daily rate limit of {api_key.rate_limit} requests exceeded. Upgrade at /auth/tiers",
            headers={"Retry-After": "3600"},
        )


def get_tier_result_limit(tier: str) -> int:
    """Return the max results a tier is allowed."""
    return TIER_RESULT_LIMITS.get(tier, TIER_RESULT_LIMITS["free"])


def clamp_limit(requested: int, tier: str) -> int:
    """Clamp the user-requested limit to their tier max."""
    tier_max = get_tier_result_limit(tier)
    return min(requested, tier_max)
