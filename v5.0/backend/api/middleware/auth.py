# ──────────────────────────────────────────────────────────
# V5.0 Backend — JWT Authentication Middleware
# ──────────────────────────────────────────────────────────

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from config import get_settings

_bearer = HTTPBearer(auto_error=False)


def create_access_token(
    data: dict[str, Any],
    expires_delta: timedelta | None = None,
) -> str:
    """Create a signed JWT access token."""
    settings = get_settings()
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta
        or timedelta(minutes=settings.access_token_expire_minutes)
    )
    to_encode.update({"exp": expire, "iat": datetime.now(timezone.utc)})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def verify_token(token: str) -> dict[str, Any] | None:
    """Decode and verify a JWT. Returns payload or None on failure."""
    settings = get_settings()
    try:
        payload = jwt.decode(
            token, settings.secret_key, algorithms=[settings.algorithm]
        )
        return payload
    except JWTError:
        return None


async def get_current_user(
    credentials: Annotated[
        Optional[HTTPAuthorizationCredentials], Depends(_bearer)
    ] = None,
) -> dict[str, Any]:
    """FastAPI dependency — extracts and validates the current user from JWT.

    Returns a dict with at least ``sub`` and ``tier`` keys.
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = verify_token(credentials.credentials)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return {
        "sub": payload.get("sub", "anonymous"),
        "tier": payload.get("tier", "free"),
        **{k: v for k, v in payload.items() if k not in ("exp", "iat")},
    }


def require_tier(minimum: str = "free"):
    """Return a dependency that enforces a minimum access tier.

    Tier hierarchy: free < starter < pro < enterprise
    """
    tier_levels = {"free": 0, "starter": 1, "pro": 2, "enterprise": 3}
    min_level = tier_levels.get(minimum, 0)

    async def _check(
        user: Annotated[dict[str, Any], Depends(get_current_user)],
    ) -> dict[str, Any]:
        user_level = tier_levels.get(user.get("tier", "free"), 0)
        if user_level < min_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This endpoint requires '{minimum}' tier or above",
            )
        return user

    return _check
