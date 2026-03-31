# ──────────────────────────────────────────────────────────
# V5.0 Backend — Auth SQLite Database
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
import secrets
import string
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import aiosqlite
import bcrypt

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "users.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id                  TEXT PRIMARY KEY,
    email               TEXT UNIQUE NOT NULL,
    password_hash       TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    tier                TEXT NOT NULL DEFAULT 'free',
    api_key             TEXT UNIQUE NOT NULL,
    referral_code       TEXT UNIQUE NOT NULL,
    referred_by         TEXT,
    stripe_customer_id  TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS referral_rewards (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(id),
    referred_user_id TEXT NOT NULL REFERENCES users(id),
    tier_rewarded   TEXT NOT NULL,
    days_granted    INTEGER NOT NULL,
    activated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id),
    token       TEXT UNIQUE NOT NULL,
    expires_at  TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS api_keys (
    id              TEXT PRIMARY KEY,
    key_hash        TEXT UNIQUE NOT NULL,
    user_id         TEXT NOT NULL REFERENCES users(id),
    name            TEXT NOT NULL,
    tier            TEXT NOT NULL DEFAULT 'free',
    sport_access    TEXT,
    rate_limit      INTEGER NOT NULL DEFAULT 100,
    created_at      TEXT NOT NULL,
    expires_at      TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id                      TEXT PRIMARY KEY,
    user_id                 TEXT NOT NULL REFERENCES users(id),
    stripe_subscription_id  TEXT UNIQUE,
    tier                    TEXT NOT NULL,
    sports                  TEXT,
    status                  TEXT NOT NULL DEFAULT 'active',
    current_period_start    TEXT,
    current_period_end      TEXT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key);
CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_referral_rewards_user ON referral_rewards(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_stripe ON subscriptions(stripe_subscription_id);
"""

# Dev account seeded on startup
_DEV_EMAIL = os.getenv("DEV_SEED_EMAIL", "dev@wnbp.com")
_DEV_PASSWORD = os.getenv("DEV_SEED_PASSWORD", "wnbp_dev_2026")
_DEV_DISPLAY = os.getenv("DEV_SEED_DISPLAY", "WNBP Developer")
_DEV_TIER = os.getenv("DEV_SEED_TIER", "enterprise")


def _generate_referral_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _generate_api_key() -> str:
    return f"sk_{uuid4().hex}"


async def init_db() -> None:
    """Create tables and seed dev account if missing."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.executescript(_SCHEMA)
        await db.commit()

        # Seed dev account
        cursor = await db.execute("SELECT id FROM users WHERE email = ?", (_DEV_EMAIL,))
        row = await cursor.fetchone()
        if not row:
            pw_hash = bcrypt.hashpw(_DEV_PASSWORD.encode(), bcrypt.gensalt()).decode()
            user_id = str(uuid4())
            api_key = _generate_api_key()
            referral_code = _generate_referral_code()
            await db.execute(
                """INSERT INTO users
                   (id, email, password_hash, display_name, tier, api_key, referral_code)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user_id, _DEV_EMAIL, pw_hash, _DEV_DISPLAY, _DEV_TIER, api_key, referral_code),
            )
            await db.commit()
            logger.info("Seeded dev account: %s (tier=%s, api_key=%s)", _DEV_EMAIL, _DEV_TIER, api_key)
        else:
            logger.info("Dev account already exists, skipping seed.")

    logger.info("Auth database initialised at %s", DB_PATH)


@asynccontextmanager
async def get_db():
    """Yield an aiosqlite connection with row_factory enabled."""
    db = await aiosqlite.connect(str(DB_PATH))
    db.row_factory = aiosqlite.Row
    try:
        yield db
    finally:
        await db.close()
