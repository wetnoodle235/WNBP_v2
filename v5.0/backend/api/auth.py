# ──────────────────────────────────────────────────────────
# V5.0 Backend — Auth API Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Optional

import bcrypt
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status

from api.middleware.auth import create_access_token, get_current_user, verify_token
from auth.models import (
    User,
    create_referral_reward,
    create_session,
    create_user,
    delete_session,
    get_active_referral_days,
    get_active_subscription,
    get_referral_rewards,
    get_user_by_api_key,
    get_user_by_email,
    get_user_by_id,
    get_user_by_referral_code,
    list_user_api_keys,
    regenerate_user_api_key,
    update_referral_reward_tier,
    update_user_display_name,
)
from auth.tiers import BUNDLES, TIERS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["Authentication"])


# ── Helpers ──────────────────────────────────────────────

def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


def _user_response(user: User, token: str, include_api_key: bool = False) -> dict:
    data: dict[str, Any] = {
        "user_id": user.id,
        "email": user.email,
        "display_name": user.display_name,
        "tier": user.tier,
        "referral_code": user.referral_code,
        "token": token,
    }
    if include_api_key:
        data["api_key"] = user.api_key
    return data


# ── Registration & Login ─────────────────────────────────

@router.post(
    "/register",
    summary="Register a new user",
    description="Create a new account with email and password. Returns a JWT token and API key.",
    responses={
        201: {"description": "User created with free API key"},
        409: {"description": "Email already registered"},
    },
)
async def register(
    email: str = Body(..., embed=True),
    password: str = Body(..., embed=True, min_length=8),
    display_name: str = Body("", embed=True),
    referral_code: Optional[str] = Body(None, embed=True),
):
    existing = await get_user_by_email(email)
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    # Validate referral code if provided
    referred_by = None
    if referral_code:
        referrer = await get_user_by_referral_code(referral_code)
        if not referrer:
            raise HTTPException(status_code=400, detail="Invalid referral code")
        referred_by = referrer.id

    pw_hash = _hash_password(password)
    user = await create_user(
        email=email,
        password_hash=pw_hash,
        display_name=display_name or email.split("@")[0],
        referred_by=referred_by,
    )

    token = create_access_token({"sub": user.id, "email": user.email, "tier": user.tier})

    # Store session
    expires = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
    await create_session(user.id, token, expires)

    return {
        "success": True,
        "data": _user_response(user, token, include_api_key=True),
        "meta": {"note": "Save your API key — use it for all data API requests."},
    }


@router.post(
    "/login",
    summary="Login and get JWT token",
    description="Authenticate with email and password to receive a JWT token.",
)
async def login(
    email: str = Body(..., embed=True),
    password: str = Body(..., embed=True),
):
    user = await get_user_by_email(email)
    if not user or not _verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = create_access_token({"sub": user.id, "email": user.email, "tier": user.tier})

    expires = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
    await create_session(user.id, token, expires)

    return {
        "success": True,
        "data": _user_response(user, token, include_api_key=True),
    }


@router.post(
    "/logout",
    summary="Logout and invalidate session",
    description="Invalidate the current JWT session.",
)
async def logout(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
    request: Request,
):
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else ""
    if token:
        await delete_session(token)
    return {"success": True, "data": {"message": "Logged out successfully"}}


@router.post(
    "/refresh",
    summary="Refresh JWT token",
    description="Exchange a valid JWT for a new one with extended expiry.",
)
async def refresh_token(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
    request: Request,
):
    # Invalidate old session
    auth_header = request.headers.get("Authorization", "")
    old_token = auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else ""
    if old_token:
        await delete_session(old_token)

    db_user = await get_user_by_id(user["sub"])
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    new_token = create_access_token({"sub": db_user.id, "email": db_user.email, "tier": db_user.tier})
    expires = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
    await create_session(db_user.id, new_token, expires)

    return {"success": True, "data": {"token": new_token}}


# ── User Profile ─────────────────────────────────────────

@router.get(
    "/me",
    summary="Get user profile and subscription",
    description="Returns the authenticated user's profile, subscription, and referral info.",
)
async def get_me(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
):
    db_user = await get_user_by_id(user["sub"])
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    sub = await get_active_subscription(user["sub"])
    referral_days = await get_active_referral_days(user["sub"])

    return {
        "success": True,
        "data": {
            "user_id": db_user.id,
            "email": db_user.email,
            "display_name": db_user.display_name,
            "tier": db_user.tier,
            "api_key": db_user.api_key,
            "referral_code": db_user.referral_code,
            "referred_by": db_user.referred_by,
            "subscription": sub,
            "referral_free_days": referral_days,
            "referral_reward_tier": db_user.referral_reward_tier if hasattr(db_user, 'referral_reward_tier') else "starter",
            "created_at": db_user.created_at,
            "updated_at": db_user.updated_at,
        },
    }


@router.put(
    "/profile",
    summary="Update user profile",
    description="Update display name.",
)
async def update_profile(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
    display_name: str = Body(..., embed=True),
):
    await update_user_display_name(user["sub"], display_name)
    return {"success": True, "data": {"display_name": display_name}}


@router.post(
    "/api-key/regenerate",
    summary="Regenerate API key",
    description="Generate a new API key, invalidating the old one.",
)
async def regenerate_api_key(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
):
    new_key = await regenerate_user_api_key(user["sub"])
    return {
        "success": True,
        "data": {"api_key": new_key},
        "meta": {"note": "Your old API key is now invalid. Save this new one."},
    }


# ── Legacy API Key Management (kept for backward compat) ─

@router.get(
    "/api-keys",
    summary="List your API keys",
    description="List all API keys for the authenticated user (key values are not shown).",
)
async def list_keys(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
):
    keys = await list_user_api_keys(user["sub"])
    # Also include the user's primary api_key info
    db_user = await get_user_by_id(user["sub"])
    primary = {
        "id": "primary",
        "name": "Primary API Key",
        "tier": db_user.tier if db_user else "free",
        "is_active": True,
        "note": "Use GET /auth/me to see your primary API key",
    }
    return {"success": True, "data": [primary] + keys, "meta": {"count": len(keys) + 1}}


# ── Referral Info ────────────────────────────────────────

@router.get(
    "/referrals",
    summary="Get referral rewards",
    description="Returns all referral rewards earned by the authenticated user.",
)
async def get_referrals(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
):
    rewards = await get_referral_rewards(user["sub"])
    active_days = await get_active_referral_days(user["sub"])
    return {
        "success": True,
        "data": {
            "rewards": rewards,
            "active_free_days": active_days,
        },
    }


@router.put(
    "/referrals/reward-tier",
    summary="Set preferred referral reward tier",
    description="Choose which tier (starter, pro, enterprise) you want to receive when your referral converts.",
)
async def set_referral_reward_tier(
    user: Annotated[dict[str, Any], Depends(get_current_user)],
    tier: str = Body(..., embed=True),
):
    if tier not in ("starter", "pro", "enterprise"):
        raise HTTPException(status_code=400, detail="tier must be 'starter', 'pro', or 'enterprise'")
    await update_referral_reward_tier(user["sub"], tier)
    return {"success": True, "data": {"referral_reward_tier": tier}}


# ── Tier & Bundle Info (public) ──────────────────────────

@router.get(
    "/tiers",
    summary="List available tiers and pricing",
    description="Returns all subscription tiers with their rate limits, sport access, endpoint access, and pricing.",
)
async def list_tiers():
    tiers_list = []
    for name, info in TIERS.items():
        tiers_list.append({
            "tier": name,
            "rate_limit": info["rate_limit"],
            "sports": info["sports"],
            "endpoints": info["endpoints"],
            "features": info["features"],
            "historical": info["historical"],
            "result_limit": info["result_limit"],
            "price_monthly_cents": info["price_monthly"],
            "price_monthly_display": f"${info['price_monthly'] / 100:.2f}" if info["price_monthly"] else "Free",
        })
    return {"success": True, "data": tiers_list}


@router.get(
    "/bundles",
    summary="List available sport bundles",
    description="Returns sport bundles with discount information.",
)
async def list_bundles():
    bundles_list = []
    for name, info in BUNDLES.items():
        bundles_list.append({
            "bundle": name,
            "sports": info["sports"],
            "discount_percent": int(info["discount"] * 100),
        })
    return {"success": True, "data": bundles_list}
