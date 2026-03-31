# ──────────────────────────────────────────────────────────
# V5.0 Backend — Tier & Bundle Definitions
# ──────────────────────────────────────────────────────────

from __future__ import annotations

TIERS: dict[str, dict] = {
    "free": {
        "rate_limit": 100,          # requests per day
        "sports": ["nba"],          # only NBA
        "endpoints": ["games", "standings", "news"],
        "features": False,          # no feature data
        "historical": False,        # current season only
        "price_monthly": 0,
        "result_limit": 10,
    },
    "starter": {
        "rate_limit": 1000,
        "sports": ["nba", "nfl", "mlb", "nhl"],
        "endpoints": ["games", "standings", "teams", "players", "injuries", "news"],
        "features": False,
        "historical": True,         # all seasons
        "price_monthly": 1999,      # $19.99
        "result_limit": 200,
    },
    "pro": {
        "rate_limit": 10000,
        "sports": "all",            # all sports
        "endpoints": "all",         # all endpoints
        "features": True,           # feature data access
        "historical": True,
        "price_monthly": 4999,      # $49.99
        "result_limit": 1000,
    },
    "enterprise": {
        "rate_limit": 100000,
        "sports": "all",
        "endpoints": "all",
        "features": True,
        "historical": True,
        "price_monthly": 14999,     # $149.99
        "result_limit": 999999,     # unlimited
    },
}

BUNDLES: dict[str, dict] = {
    "us_major": {
        "sports": ["nba", "nfl", "mlb", "nhl"],
        "discount": 0.20,
    },
    "soccer": {
        "sports": ["epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl"],
        "discount": 0.25,
    },
    "all_sports": {
        "sports": "all",
        "discount": 0.30,
    },
}

TIER_ORDER = ["free", "starter", "pro", "enterprise"]


def tier_level(tier: str) -> int:
    """Return numeric level for a tier (higher = more access)."""
    try:
        return TIER_ORDER.index(tier)
    except ValueError:
        return 0


def tier_allows_sport(tier: str, sport: str) -> bool:
    sports = TIERS.get(tier, TIERS["free"])["sports"]
    if sports == "all":
        return True
    return sport in sports


def tier_allows_endpoint(tier: str, endpoint: str) -> bool:
    endpoints = TIERS.get(tier, TIERS["free"])["endpoints"]
    if endpoints == "all":
        return True
    return endpoint in endpoints


def tier_allows_features(tier: str) -> bool:
    return TIERS.get(tier, TIERS["free"]).get("features", False)


def tier_allows_historical(tier: str) -> bool:
    return TIERS.get(tier, TIERS["free"]).get("historical", False)


def tier_result_limit(tier: str) -> int:
    return TIERS.get(tier, TIERS["free"]).get("result_limit", 10)
