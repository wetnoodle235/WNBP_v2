# ──────────────────────────────────────────────────────────
# V5.0 Backend — Auth Module
# ──────────────────────────────────────────────────────────

from .database import get_db, init_db
from .middleware import (
    check_features_access,
    check_tier_access,
    clamp_limit,
    get_api_key,
    get_tier_result_limit,
    rate_limit_check,
    require_api_key,
)
from .tiers import BUNDLES, TIERS

__all__ = [
    "init_db",
    "get_db",
    "get_api_key",
    "require_api_key",
    "check_tier_access",
    "check_features_access",
    "rate_limit_check",
    "clamp_limit",
    "get_tier_result_limit",
    "TIERS",
    "BUNDLES",
]
