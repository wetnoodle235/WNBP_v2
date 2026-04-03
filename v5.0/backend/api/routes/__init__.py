# ──────────────────────────────────────────────────────────
# V5.0 Backend — Route Registry
# ──────────────────────────────────────────────────────────

from .sports import router as sports_router
from .meta import router as meta_router
from .predictions import router as predictions_router
from .live import router as live_router
from .features import router as features_router
from .stripe import router as stripe_router
from .paper import router as paper_router
from .autobet import router as autobet_router
from .charts import router as charts_router

__all__ = [
    "sports_router",
    "meta_router",
    "predictions_router",
    "live_router",
    "features_router",
    "stripe_router",
    "paper_router",
    "autobet_router",
    "charts_router",
]
