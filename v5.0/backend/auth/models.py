# ──────────────────────────────────────────────────────────
# V5.0 Backend — Auth Database Models (aiosqlite)
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import hashlib
import json
import secrets
import string
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

import aiosqlite

from .database import get_db


# ── Dataclasses ──────────────────────────────────────────

@dataclass
class User:
    id: str
    email: str
    password_hash: str
    display_name: str = ""
    tier: str = "free"
    api_key: str = ""
    referral_code: str = ""
    referred_by: Optional[str] = None
    stripe_customer_id: Optional[str] = None
    referral_reward_tier: str = "starter"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class APIKey:
    id: str
    key_hash: str
    user_id: str
    name: str
    tier: str = "free"
    sport_access: list[str] = field(default_factory=lambda: ["nba"])
    rate_limit: int = 100
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    expires_at: Optional[str] = None
    is_active: bool = True


@dataclass
class Subscription:
    id: str
    user_id: str
    stripe_subscription_id: str
    tier: str
    sports: list[str] = field(default_factory=list)
    status: str = "active"
    current_period_start: Optional[str] = None
    current_period_end: Optional[str] = None


@dataclass
class APIKeyInfo:
    """Lightweight key info passed through middleware."""
    key_id: str
    user_id: str
    tier: str
    sport_access: list[str]
    rate_limit: int
    name: str


# ── Helpers ──────────────────────────────────────────────

def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def generate_api_key() -> tuple[str, str]:
    """Return (raw_key, key_hash). Raw key shown once to user."""
    raw = f"ss_{secrets.token_urlsafe(32)}"
    return raw, hash_api_key(raw)


def _generate_referral_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _generate_user_api_key() -> str:
    return f"sk_{uuid4().hex}"


# ── User CRUD ────────────────────────────────────────────

async def create_user(
    email: str,
    password_hash: str,
    display_name: str = "",
    referred_by: Optional[str] = None,
) -> User:
    user_id = str(uuid4())
    api_key = _generate_user_api_key()
    referral_code = _generate_referral_code()
    user = User(
        id=user_id,
        email=email,
        password_hash=password_hash,
        display_name=display_name or email.split("@")[0],
        tier="free",
        api_key=api_key,
        referral_code=referral_code,
        referred_by=referred_by,
    )
    async with get_db() as db:
        await db.execute(
            """INSERT INTO users
               (id, email, password_hash, display_name, tier, api_key, referral_code, referred_by, referral_reward_tier)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user.id, user.email, user.password_hash, user.display_name,
             user.tier, user.api_key, user.referral_code, user.referred_by, "starter"),
        )
        await db.commit()
    return user


async def get_user_by_email(email: str) -> Optional[User]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, email, password_hash, display_name, tier, api_key, referral_code, referred_by, stripe_customer_id, created_at, updated_at FROM users WHERE email = ?",
            (email,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return _row_to_user(row)


async def get_user_by_id(user_id: str) -> Optional[User]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, email, password_hash, display_name, tier, api_key, referral_code, referred_by, stripe_customer_id, created_at, updated_at FROM users WHERE id = ?",
            (user_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return _row_to_user(row)


async def get_user_by_api_key(api_key: str) -> Optional[User]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, email, password_hash, display_name, tier, api_key, referral_code, referred_by, stripe_customer_id, created_at, updated_at FROM users WHERE api_key = ?",
            (api_key,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return _row_to_user(row)


async def get_user_by_referral_code(code: str) -> Optional[User]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, email, password_hash, display_name, tier, api_key, referral_code, referred_by, stripe_customer_id, created_at, updated_at FROM users WHERE referral_code = ?",
            (code,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return _row_to_user(row)


async def update_user_tier(user_id: str, tier: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE users SET tier = ?, updated_at = datetime('now') WHERE id = ?",
            (tier, user_id),
        )
        await db.commit()


async def update_user_display_name(user_id: str, display_name: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE users SET display_name = ?, updated_at = datetime('now') WHERE id = ?",
            (display_name, user_id),
        )
        await db.commit()


async def update_user_stripe_id(user_id: str, stripe_customer_id: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE users SET stripe_customer_id = ?, updated_at = datetime('now') WHERE id = ?",
            (stripe_customer_id, user_id),
        )
        await db.commit()


async def regenerate_user_api_key(user_id: str) -> str:
    new_key = _generate_user_api_key()
    async with get_db() as db:
        await db.execute(
            "UPDATE users SET api_key = ?, updated_at = datetime('now') WHERE id = ?",
            (new_key, user_id),
        )
        await db.commit()
    return new_key


def _row_to_user(row) -> User:
    return User(
        id=row["id"],
        email=row["email"],
        password_hash=row["password_hash"],
        display_name=row["display_name"],
        tier=row["tier"],
        api_key=row["api_key"],
        referral_code=row["referral_code"],
        referred_by=row["referred_by"],
        stripe_customer_id=row["stripe_customer_id"],
        referral_reward_tier=row["referral_reward_tier"] if "referral_reward_tier" in row.keys() else "starter",
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Session CRUD ─────────────────────────────────────────

async def create_session(user_id: str, token: str, expires_at: str) -> str:
    session_id = str(uuid4())
    async with get_db() as db:
        await db.execute(
            "INSERT INTO sessions (id, user_id, token, expires_at) VALUES (?, ?, ?, ?)",
            (session_id, user_id, token, expires_at),
        )
        await db.commit()
    return session_id


async def get_session_by_token(token: str) -> Optional[dict]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, user_id, token, expires_at, created_at FROM sessions WHERE token = ?",
            (token,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {"id": row["id"], "user_id": row["user_id"], "token": row["token"],
                "expires_at": row["expires_at"], "created_at": row["created_at"]}


async def delete_session(token: str) -> None:
    async with get_db() as db:
        await db.execute("DELETE FROM sessions WHERE token = ?", (token,))
        await db.commit()


async def delete_user_sessions(user_id: str) -> None:
    async with get_db() as db:
        await db.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        await db.commit()


# ── Referral Rewards CRUD ────────────────────────────────

REFERRAL_DAYS = {
    "starter": 7,
    "pro": 5,
    "enterprise": 3,
}


async def create_referral_reward(
    referrer_id: str,
    referred_user_id: str,
    tier_purchased: str,
    preferred_tier: Optional[str] = None,
) -> Optional[dict]:
    """Grant referral reward to referrer using their preferred reward tier."""
    reward_tier = preferred_tier or tier_purchased
    if reward_tier not in REFERRAL_DAYS:
        reward_tier = "starter"  # fallback
    days = REFERRAL_DAYS[reward_tier]

    reward_id = str(uuid4())
    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(days=days)).isoformat()

    async with get_db() as db:
        await db.execute(
            """INSERT INTO referral_rewards
               (id, user_id, referred_user_id, tier_rewarded, days_granted, activated_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (reward_id, referrer_id, referred_user_id, reward_tier, days, now.isoformat(), expires_at),
        )
        await db.commit()

    return {
        "id": reward_id,
        "referrer_id": referrer_id,
        "referred_user_id": referred_user_id,
        "tier_rewarded": reward_tier,
        "days_granted": days,
        "expires_at": expires_at,
    }


async def update_referral_reward_tier(user_id: str, tier: str) -> None:
    """Update the user's preferred referral reward tier (starter/pro/enterprise)."""
    if tier not in ("starter", "pro", "enterprise"):
        tier = "starter"
    async with get_db() as db:
        await db.execute(
            "UPDATE users SET referral_reward_tier = ?, updated_at = datetime('now') WHERE id = ?",
            (tier, user_id),
        )
        await db.commit()


async def get_referral_rewards(user_id: str) -> list[dict]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, user_id, referred_user_id, tier_rewarded, days_granted, activated_at, expires_at FROM referral_rewards WHERE user_id = ? ORDER BY activated_at DESC",
            (user_id,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "id": r["id"],
                "referred_user_id": r["referred_user_id"],
                "tier_rewarded": r["tier_rewarded"],
                "days_granted": r["days_granted"],
                "activated_at": r["activated_at"],
                "expires_at": r["expires_at"],
            }
            for r in rows
        ]


async def get_active_referral_days(user_id: str) -> dict[str, int]:
    """Return total remaining free days per tier from active referral rewards."""
    now = datetime.now(timezone.utc).isoformat()
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT tier_rewarded, SUM(days_granted) as total_days FROM referral_rewards "
            "WHERE user_id = ? AND expires_at > ? GROUP BY tier_rewarded",
            (user_id, now),
        )
        rows = await cursor.fetchall()
        return {r["tier_rewarded"]: r["total_days"] for r in rows}


# ── API Key CRUD (legacy — kept for backward compatibility) ──

async def create_api_key(
    user_id: str,
    name: str,
    tier: str = "free",
    sport_access: Optional[list[str]] = None,
    rate_limit: int = 100,
    expires_at: Optional[str] = None,
) -> tuple[str, APIKey]:
    """Create an API key. Returns (raw_key, api_key_record)."""
    raw_key, key_hash = generate_api_key()
    key_id = str(uuid4())
    sports = sport_access or ["nba"]
    api_key = APIKey(
        id=key_id,
        key_hash=key_hash,
        user_id=user_id,
        name=name,
        tier=tier,
        sport_access=sports,
        rate_limit=rate_limit,
        expires_at=expires_at,
    )
    async with get_db() as db:
        await db.execute(
            """INSERT INTO api_keys
               (id, key_hash, user_id, name, tier, sport_access, rate_limit, created_at, expires_at, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                api_key.id, api_key.key_hash, api_key.user_id, api_key.name,
                api_key.tier, json.dumps(api_key.sport_access), api_key.rate_limit,
                api_key.created_at, api_key.expires_at, 1,
            ),
        )
        await db.commit()
    return raw_key, api_key


async def get_api_key_by_hash(key_hash: str) -> Optional[APIKeyInfo]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, key_hash, user_id, name, tier, sport_access, rate_limit, created_at, expires_at, is_active FROM api_keys WHERE key_hash = ? AND is_active = 1",
            (key_hash,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        if row["expires_at"]:
            exp = datetime.fromisoformat(row["expires_at"])
            if exp < datetime.now(timezone.utc):
                return None
        sport_access = json.loads(row["sport_access"]) if row["sport_access"] else ["nba"]
        return APIKeyInfo(
            key_id=row["id"],
            user_id=row["user_id"],
            tier=row["tier"],
            sport_access=sport_access,
            rate_limit=row["rate_limit"],
            name=row["name"],
        )


async def list_user_api_keys(user_id: str) -> list[dict[str, Any]]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, name, tier, sport_access, rate_limit, created_at, expires_at, is_active "
            "FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "tier": r["tier"],
                "sport_access": json.loads(r["sport_access"]) if r["sport_access"] else [],
                "rate_limit": r["rate_limit"],
                "created_at": r["created_at"],
                "expires_at": r["expires_at"],
                "is_active": bool(r["is_active"]),
            }
            for r in rows
        ]


async def revoke_api_key(key_id: str, user_id: str) -> bool:
    async with get_db() as db:
        cursor = await db.execute(
            "UPDATE api_keys SET is_active = 0 WHERE id = ? AND user_id = ?",
            (key_id, user_id),
        )
        await db.commit()
        return cursor.rowcount > 0


async def update_user_keys_tier(user_id: str, tier: str, sport_access: list[str], rate_limit: int) -> None:
    """Upgrade/downgrade all active keys for a user to a new tier."""
    async with get_db() as db:
        await db.execute(
            "UPDATE api_keys SET tier = ?, sport_access = ?, rate_limit = ? WHERE user_id = ? AND is_active = 1",
            (tier, json.dumps(sport_access), rate_limit, user_id),
        )
        await db.commit()


# ── Subscription CRUD ────────────────────────────────────

async def create_subscription(
    user_id: str,
    stripe_subscription_id: str,
    tier: str,
    sports: list[str],
    current_period_start: Optional[str] = None,
    current_period_end: Optional[str] = None,
) -> Subscription:
    sub_id = str(uuid4())
    sub = Subscription(
        id=sub_id,
        user_id=user_id,
        stripe_subscription_id=stripe_subscription_id,
        tier=tier,
        sports=sports,
        current_period_start=current_period_start,
        current_period_end=current_period_end,
    )
    async with get_db() as db:
        await db.execute(
            """INSERT INTO subscriptions
               (id, user_id, stripe_subscription_id, tier, sports, status,
                current_period_start, current_period_end)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                sub.id, sub.user_id, sub.stripe_subscription_id, sub.tier,
                json.dumps(sub.sports), sub.status,
                sub.current_period_start, sub.current_period_end,
            ),
        )
        await db.commit()
    return sub


async def get_active_subscription(user_id: str) -> Optional[dict[str, Any]]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, user_id, stripe_subscription_id, tier, sports, status, current_period_start, current_period_end, created_at FROM subscriptions WHERE user_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "stripe_subscription_id": row["stripe_subscription_id"],
            "tier": row["tier"],
            "sports": json.loads(row["sports"]) if row["sports"] else [],
            "status": row["status"],
            "current_period_start": row["current_period_start"],
            "current_period_end": row["current_period_end"],
        }


async def update_subscription_status(stripe_subscription_id: str, status: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE subscriptions SET status = ? WHERE stripe_subscription_id = ?",
            (status, stripe_subscription_id),
        )
        await db.commit()


async def update_subscription_period(
    stripe_subscription_id: str,
    current_period_start: str,
    current_period_end: str,
) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE subscriptions SET current_period_start = ?, current_period_end = ? WHERE stripe_subscription_id = ?",
            (current_period_start, current_period_end, stripe_subscription_id),
        )
        await db.commit()


async def get_subscription_by_user_and_tier(user_id: str, tier: str) -> Optional[dict]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT id, user_id, stripe_subscription_id, tier, sports, status, current_period_start, current_period_end, created_at FROM subscriptions WHERE user_id = ? AND tier = ? AND status = 'active' LIMIT 1",
            (user_id, tier),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "stripe_subscription_id": row["stripe_subscription_id"],
            "tier": row["tier"],
            "status": row["status"],
        }
