# ──────────────────────────────────────────────────────────
# V5.0 Backend — Features Endpoint (Pro+ Tier)
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from auth.middleware import check_features_access, require_api_key
from auth.models import APIKeyInfo
from config import SPORT_DEFINITIONS, get_current_season, get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/{sport}", tags=["Features"])


def _validate_sport(sport: str) -> str:
    if sport not in SPORT_DEFINITIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown sport '{sport}'. Valid: {', '.join(sorted(SPORT_DEFINITIONS))}",
        )
    return sport


ValidSport = Annotated[str, Depends(_validate_sport)]


@router.get(
    "/features",
    summary="ML feature data for a sport",
    description=(
        "Returns the extracted ML features DataFrame for a sport/season. "
        "Useful for data scientists building their own models. **Requires pro tier or higher.**"
    ),
    responses={
        200: {
            "description": "Feature data for the requested sport and season",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "game_id": "401710910",
                                "home_team": "BOS",
                                "away_team": "LAL",
                                "home_win_pct": 0.72,
                                "away_win_pct": 0.55,
                                "home_avg_pts": 115.3,
                                "away_avg_pts": 108.7,
                            }
                        ],
                        "meta": {
                            "sport": "nba",
                            "season": "2024",
                            "count": 1,
                            "total_features": 42,
                        },
                    }
                }
            },
        },
        403: {"description": "Tier does not allow feature data access"},
    },
)
async def get_features(
    sport: ValidSport,
    api_key: APIKeyInfo = Depends(require_api_key),
    season: str = Query(None, description="Season year. Defaults to current."),
    limit: int = Query(100, ge=1, le=5000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Row offset for pagination"),
):
    # Enforce pro tier
    await check_features_access(api_key)

    settings = get_settings()
    effective_season = season or get_current_season(sport)

    # Look for feature files
    features_dir = settings.data_dir / "features" / sport
    feature_file = features_dir / f"features_{effective_season}.parquet"

    if not feature_file.exists():
        # Fall back to checking normalized dir for any feature-like file
        feature_file = settings.normalized_dir / sport / f"features_{effective_season}.parquet"

    if not feature_file.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No feature data found for {sport} season {effective_season}",
        )

    try:
        import pandas as pd

        df = pd.read_parquet(feature_file)
        total = len(df)
        page = df.iloc[offset : offset + limit]
        records = page.to_dict(orient="records")

        # Clean NaN values for JSON serialization
        for record in records:
            for k, v in record.items():
                if isinstance(v, float) and (v != v):  # NaN check
                    record[k] = None

        return {
            "success": True,
            "data": records,
            "meta": {
                "sport": sport,
                "season": effective_season,
                "count": len(records),
                "total": total,
                "total_features": len(df.columns),
                "feature_names": list(df.columns),
                "limit": limit,
                "offset": offset,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    except Exception as e:
        logger.error("Error reading features for %s/%s: %s", sport, effective_season, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error reading feature data",
        )
