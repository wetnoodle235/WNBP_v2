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

import asyncio

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
    referral_reward_tier TEXT NOT NULL DEFAULT 'starter',
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
    source                  TEXT NOT NULL DEFAULT 'stripe',
    billing_interval        TEXT,
    current_period_start    TEXT,
    current_period_end      TEXT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS subscription_notifications (
    id                  TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL REFERENCES users(id),
    subscription_id     TEXT NOT NULL REFERENCES subscriptions(id),
    event_type          TEXT NOT NULL,
    period_end          TEXT,
    sent_at             TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(subscription_id, event_type, period_end)
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
CREATE INDEX IF NOT EXISTS idx_subscription_notifications_sub ON subscription_notifications(subscription_id);
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

        # Forward-only migration for pre-existing DBs that predate referral tiers.
        cols_cur = await db.execute("PRAGMA table_info(users)")
        cols = await cols_cur.fetchall()
        col_names = {str(c[1]) for c in cols}
        if "referral_reward_tier" not in col_names:
            await db.execute("ALTER TABLE users ADD COLUMN referral_reward_tier TEXT NOT NULL DEFAULT 'starter'")
            await db.execute("UPDATE users SET referral_reward_tier = 'starter' WHERE referral_reward_tier IS NULL OR referral_reward_tier = ''")

        sub_cols_cur = await db.execute("PRAGMA table_info(subscriptions)")
        sub_cols = await sub_cols_cur.fetchall()
        sub_col_names = {str(c[1]) for c in sub_cols}
        if "source" not in sub_col_names:
            await db.execute("ALTER TABLE subscriptions ADD COLUMN source TEXT NOT NULL DEFAULT 'stripe'")
        if "billing_interval" not in sub_col_names:
            await db.execute("ALTER TABLE subscriptions ADD COLUMN billing_interval TEXT")

        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscription_notifications (
                id                  TEXT PRIMARY KEY,
                user_id             TEXT NOT NULL REFERENCES users(id),
                subscription_id     TEXT NOT NULL REFERENCES subscriptions(id),
                event_type          TEXT NOT NULL,
                period_end          TEXT,
                sent_at             TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(subscription_id, event_type, period_end)
            );
            CREATE INDEX IF NOT EXISTS idx_subscription_notifications_sub ON subscription_notifications(subscription_id);
            """
        )

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
    # Warm the connection pool immediately so the first request isn't slowed
    # by pool initialisation.
    await _get_pool()


# ── Connection Pool ───────────────────────────────────────

_DB_POOL_SIZE = 5
_pool: asyncio.Queue | None = None
_pool_event: asyncio.Event | None = None


async def _make_connection() -> aiosqlite.Connection:
    """Open and configure a single SQLite connection."""
    conn = await aiosqlite.connect(str(DB_PATH))
    conn.row_factory = aiosqlite.Row
    # WAL mode allows concurrent reads while a write is in progress.
    await conn.execute("PRAGMA journal_mode=WAL")
    # NORMAL sync is safe with WAL and much faster than FULL.
    await conn.execute("PRAGMA synchronous=NORMAL")
    # 8 MB in-process page cache per connection.
    await conn.execute("PRAGMA cache_size=-8000")
    return conn


async def _get_pool() -> asyncio.Queue:
    """Lazily initialise the connection pool (thread-safe for asyncio)."""
    global _pool, _pool_event

    if _pool is not None:
        return _pool

    # Exactly one coroutine creates the pool; others wait on the event.
    if _pool_event is None:
        _pool_event = asyncio.Event()
        q: asyncio.Queue = asyncio.Queue(maxsize=_DB_POOL_SIZE)
        for _ in range(_DB_POOL_SIZE):
            conn = await _make_connection()
            await q.put(conn)
        _pool = q
        _pool_event.set()
    else:
        await _pool_event.wait()

    return _pool  # type: ignore[return-value]


async def close_pool() -> None:
    """Drain and close all pooled connections (called at shutdown)."""
    global _pool, _pool_event
    if _pool is None:
        return
    while not _pool.empty():
        try:
            conn = _pool.get_nowait()
            await conn.close()
        except Exception:
            pass
    _pool = None
    _pool_event = None


@asynccontextmanager
async def get_db():
    """Yield a pooled aiosqlite connection.

    Connections are reused across requests.  On unhandled exceptions the
    connection is discarded and a fresh one is placed back in the pool.
    """
    pool = await _get_pool()
    db = await pool.get()
    try:
        yield db
    except Exception:
        # Replace the potentially-dirty connection with a fresh one.
        try:
            await db.close()
        except Exception:
            pass
        try:
            db = await _make_connection()
        except Exception:
            db = None  # type: ignore[assignment]
        if db is not None:
            await pool.put(db)
        raise
    else:
        await pool.put(db)
