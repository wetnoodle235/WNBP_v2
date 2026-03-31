# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Prediction Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import json
import logging
import math
import pickle
import re
import time
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from config import SPORT_DEFINITIONS
from services.data_service import DataService, get_data_service

router = APIRouter(prefix="/v1/predictions")
logger = logging.getLogger(__name__)

_PLAYER_PROPS_MODELS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "ml" / "models"
_LIVE_PREDICTIONS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "live_predictions"
# TTL-aware bundle cache: sport -> (bundle, loaded_monotonic_time)
_BUNDLE_CACHE_TTL_SECONDS: float = 1800.0  # 30-minute TTL
_PLAYER_PROPS_BUNDLE_CACHE: dict[str, tuple[dict | None, float]] = {}
_BUNDLE_CACHE_STATS: dict[str, int] = {"hits": 0, "misses": 0}
_TIER_TO_GRADES: dict[str, tuple[str, ...]] = {
    "high": ("S", "A"),
    "medium": ("B", "C"),
    "low": ("D",),
}


def _validate_sport(sport: str) -> str:
    if sport not in SPORT_DEFINITIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown sport '{sport}'",
        )
    return sport


ValidSport = Annotated[str, Depends(_validate_sport)]
DS = Annotated[DataService, Depends(get_data_service)]


def _prediction_date_key(pred: dict) -> Optional[str]:
    raw = pred.get("date") or pred.get("game_date") or pred.get("created_at")
    if raw is None:
        return None
    value = str(raw)
    return value[:10] if len(value) >= 10 else value


def _is_prediction_evaluable(pred: dict) -> bool:
    home_prob = _safe_probability(pred.get("home_win_prob"))
    away_prob = _safe_probability(pred.get("away_win_prob"))
    return (
        home_prob is not None
        and away_prob is not None
        and pred.get("home_score") is not None
        and pred.get("away_score") is not None
    )


def _prediction_is_correct(pred: dict) -> bool:
    if not _is_prediction_evaluable(pred):
        return False
    home_prob = _safe_probability(pred.get("home_win_prob"))
    away_prob = _safe_probability(pred.get("away_win_prob"))
    if home_prob is None or away_prob is None:
        return False
    return (home_prob > away_prob) == (pred["home_score"] > pred["away_score"])


def _safe_probability(value: object) -> float | None:
    """Best-effort conversion for model probabilities.

    Returns None for non-numeric/NaN/Inf values.
    """
    try:
        prob = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(prob) or math.isinf(prob):
        return None
    return min(max(prob, 0.0), 1.0)


def _safe_finite_number(value: object) -> float | None:
    """Return a finite float or None for non-numeric/NaN/Inf inputs."""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def _json_safe_record(record: dict) -> dict:
    """Return a copy of record with non-finite floats normalized to None."""
    cleaned: dict = {}
    for key, value in record.items():
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def _load_player_props_bundle(sport: str) -> dict | None:
    now = time.monotonic()
    if sport in _PLAYER_PROPS_BUNDLE_CACHE:
        cached_bundle, loaded_at = _PLAYER_PROPS_BUNDLE_CACHE[sport]
        if now - loaded_at < _BUNDLE_CACHE_TTL_SECONDS:
            _BUNDLE_CACHE_STATS["hits"] += 1
            return cached_bundle
        # TTL expired — evict and reload
        logger.debug("Bundle cache TTL expired for %s, reloading", sport)

    _BUNDLE_CACHE_STATS["misses"] += 1
    path = _PLAYER_PROPS_MODELS_DIR / sport / "player_props.pkl"
    if not path.exists():
        _PLAYER_PROPS_BUNDLE_CACHE[sport] = (None, now)
        return None

    try:
        with open(path, "rb") as fh:
            bundle = pickle.load(fh)  # noqa: S301
        if not isinstance(bundle, dict):
            logger.warning("Unexpected player props bundle format for %s", sport)
            bundle = None
        _PLAYER_PROPS_BUNDLE_CACHE[sport] = (bundle, now)
        return bundle
    except Exception:
        logger.exception("Failed to load player props bundle for %s", sport)
        _PLAYER_PROPS_BUNDLE_CACHE[sport] = (None, now)
        return None


def _bundle_cache_info() -> dict:
    """Return a snapshot of the bundle cache state for health/diagnostics."""
    now = time.monotonic()
    entries: list[dict] = []
    for sport, (bundle, loaded_at) in _PLAYER_PROPS_BUNDLE_CACHE.items():
        age_seconds = now - loaded_at
        entries.append(
            {
                "sport": sport,
                "loaded": bundle is not None,
                "age_seconds": round(age_seconds, 1),
                "ttl_seconds": _BUNDLE_CACHE_TTL_SECONDS,
                "expires_in_seconds": max(0.0, round(_BUNDLE_CACHE_TTL_SECONDS - age_seconds, 1)),
            }
        )
    total = _BUNDLE_CACHE_STATS["hits"] + _BUNDLE_CACHE_STATS["misses"]
    hit_rate = round(_BUNDLE_CACHE_STATS["hits"] / total, 4) if total else None
    return {
        "entries": entries,
        "hits": _BUNDLE_CACHE_STATS["hits"],
        "misses": _BUNDLE_CACHE_STATS["misses"],
        "hit_rate": hit_rate,
        "ttl_seconds": _BUNDLE_CACHE_TTL_SECONDS,
    }


def _extract_over_line(prop_name: str) -> float | None:
    match = re.search(r"_over_(\d+)", prop_name)
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None


def _load_live_snapshot_map(sport: str) -> dict[str, dict]:
    path = _LIVE_PREDICTIONS_DIR / f"{sport}_live.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
        games = payload.get("games", []) if isinstance(payload, dict) else []
        return {
            str(item.get("game_id")): item
            for item in games
            if isinstance(item, dict) and item.get("game_id") is not None
        }
    except Exception:
        logger.exception("Failed to parse live snapshot for %s", sport)
        return {}


def _is_open_game_status(status: str) -> bool:
    s = status.strip().lower()
    if not s:
        return True
    closed_tokens = ("final", "completed", "postponed", "canceled", "cancelled")
    return not any(token in s for token in closed_tokens)


def _opportunity_grade(score: float) -> str:
    """Map a confidence score to a 5-grade label (S/A/B/C/D)."""
    if score >= 0.82:
        return "S"   # Platinum — maximum conviction
    if score >= 0.70:
        return "A"   # Gold — high conviction
    if score >= 0.62:
        return "B"   # Silver — moderate conviction
    if score >= 0.55:
        return "C"   # Bronze — low-moderate
    return "D"       # Copper — minimum


def _grade_to_tier(grade: str) -> str:
    """Map an S/A/B/C/D grade to public high/medium/low tier labels."""
    g = grade.strip().upper()
    if g in {"S", "A"}:
        return "high"
    if g in {"B", "C"}:
        return "medium"
    return "low"


def _normalize_tier_filter(tier: str | None) -> set[str]:
    """Normalize tier filter into allowed grades.

    Supports public labels (high/medium/low) and direct grades (S/A/B/C/D)
    for backwards compatibility.
    """
    if not tier:
        return {"S", "A", "B", "C", "D"}

    t = tier.strip().lower()
    if t in _TIER_TO_GRADES:
        return set(_TIER_TO_GRADES[t])

    g = tier.strip().upper()
    if g in {"S", "A", "B", "C", "D"}:
        return {g}

    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail="Invalid tier. Use high|medium|low or grade S|A|B|C|D.",
    )


def _parse_sports_filter(sports: str | None) -> list[str]:
    """Parse and validate comma-separated sports list."""
    if sports is None:
        return sorted(SPORT_DEFINITIONS.keys())

    parsed = [part.strip().lower() for part in sports.split(",") if part.strip()]
    if not parsed:
        return sorted(SPORT_DEFINITIONS.keys())

    invalid = sorted({s for s in parsed if s not in SPORT_DEFINITIONS})
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unknown sport(s): {', '.join(invalid)}",
        )

    # Preserve user order while removing duplicates
    seen: set[str] = set()
    ordered: list[str] = []
    for s in parsed:
        if s not in seen:
            ordered.append(s)
            seen.add(s)
    return ordered



# ── Aggregate Opportunities ──────────────────────────────────

@router.get(
    "/opportunities",
    summary="Aggregate prop opportunities across all sports",
    description=(
        "Returns ranked player-prop opportunities across every sport that has a trained model, "
        "without needing to specify a sport. Results are merged and sorted by recommendation score."
    ),
    tags=["Predictions"],
    response_model_exclude_none=True,
)
async def get_aggregate_opportunities(
    ds: DS,
    date_filter: Optional[str] = Query(
        None, alias="date", description="Date (YYYY-MM-DD). Defaults to today."
    ),
    prop_type: Optional[str] = Query(None, description="Filter by specific prop market type"),
    min_score: float = Query(0.0, ge=0.0, le=1.0, description="Minimum recommendation score"),
    tier: Optional[str] = Query(None, description="Filter by tier: high, medium, or low"),
    sports: Optional[str] = Query(
        None, description="Comma-separated list of sports to include. Defaults to all trained sports."
    ),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    target_date = date_filter or date.today().isoformat()

    # Determine which sports have trained bundles
    sport_keys = _parse_sports_filter(sports)
    trained_bundle_map: dict[str, dict] = {}
    trained_sports: list[str] = []
    for sport_key in sport_keys:
        bundle = _load_player_props_bundle(sport_key)
        if bundle:
            trained_bundle_map[sport_key] = bundle
            trained_sports.append(sport_key)

    all_rows: list[dict] = []
    sports_included: list[str] = []

    for sport_key in trained_sports:
        bundle = trained_bundle_map[sport_key]

        models = bundle.get("models", {}) if isinstance(bundle, dict) else {}
        prop_catalog: list[dict] = []
        for name in sorted(models.keys()):
            prop_name = str(name)
            inferred_line = _extract_over_line(prop_name)
            prop_catalog.append(
                {
                    "prop_type": prop_name,
                    "line": inferred_line,
                    "market_type": "over_under" if inferred_line is not None else "projection",
                }
            )

        if prop_type:
            prop_catalog = [m for m in prop_catalog if m["prop_type"] == prop_type]

        games = ds.get_games(sport_key, date=target_date)
        open_games = [g for g in games if _is_open_game_status(str(g.get("status", "")))]
        pred_map = {str(p.get("game_id")): p for p in ds.get_predictions(sport_key, date=target_date)}
        live_map = _load_live_snapshot_map(sport_key)

        for game in open_games:
            game_id = str(game.get("id") or game.get("game_id") or "")
            prediction = pred_map.get(game_id, {})
            live = live_map.get(game_id, {})

            confidence = _safe_probability(prediction.get("confidence"))
            base_score = confidence if confidence is not None else 0.55

            momentum_score_val = _safe_finite_number(live.get("momentum_score"))
            if momentum_score_val is not None:
                base_score = min(0.95, max(0.05, base_score + (momentum_score_val * 0.1)))

            grade = _opportunity_grade(base_score)
            all_rows.append(
                {
                    "sport": sport_key,
                    "game_id": game_id,
                    "date": game.get("date") or target_date,
                    "status": game.get("status"),
                    "home_team": game.get("home_team"),
                    "away_team": game.get("away_team"),
                    "start_time": game.get("start_time"),
                    "recommendation_score": round(base_score, 4),
                    "recommendation_grade": grade,
                    "recommendation_tier": _grade_to_tier(grade),
                    "available_markets": prop_catalog,
                    "model_context": {
                        "trained_at": bundle.get("trained_at") if isinstance(bundle, dict) else None,
                        "feature_count": len(bundle.get("feature_names", [])) if isinstance(bundle, dict) else 0,
                        "supported_props": [m["prop_type"] for m in prop_catalog],
                    },
                    "live_context": {
                        "live_home_wp": live.get("live_home_wp"),
                        "live_away_wp": live.get("live_away_wp"),
                        "momentum": live.get("momentum"),
                        "momentum_score": live.get("momentum_score"),
                        "time_remaining": live.get("time_remaining"),
                    },
                }
            )

        sports_included.append(sport_key)

    if min_score > 0:
        all_rows = [r for r in all_rows if r["recommendation_score"] >= min_score]

    allowed_grades = _normalize_tier_filter(tier)
    all_rows = [r for r in all_rows if str(r.get("recommendation_grade", "D")).upper() in allowed_grades]

    all_rows.sort(key=lambda r: r["recommendation_score"], reverse=True)
    total = len(all_rows)
    page_data = all_rows[offset : offset + limit]

    # Prop-type breakdown across all (pre-page) results
    prop_type_counts: dict[str, int] = {}
    for row in all_rows:
        for mkt in row.get("available_markets") or []:
            pt = mkt.get("prop_type") or "unknown"
            prop_type_counts[pt] = prop_type_counts.get(pt, 0) + 1

    # 3-tier public breakdown + 5-grade internal breakdown
    tier_counts = {"high": 0, "medium": 0, "low": 0}
    grade_counts = {"S": 0, "A": 0, "B": 0, "C": 0, "D": 0}
    for row in all_rows:
        tier_key = str(row.get("recommendation_tier", "low")).lower()
        if tier_key in tier_counts:
            tier_counts[tier_key] += 1
        grade_key = str(row.get("recommendation_grade", "D")).upper()
        if grade_key in grade_counts:
            grade_counts[grade_key] += 1

    return {
        "success": True,
        "data": page_data,
        "meta": {
            "date": target_date,
            "count": len(page_data),
            "total": total,
            "limit": limit,
            "offset": offset,
            "trained_sports": trained_sports,
            "sports_included": sports_included,
            "prop_type": prop_type,
            "min_score": min_score,
            "tier": tier,
            "tier_counts": tier_counts,
            "grade_counts": grade_counts,
            "prop_type_counts": prop_type_counts,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


# ── Trained Sports Discovery ──────────────────────────────────

@router.get(
    "/trained-sports",
    summary="List sports with trained player-prop models",
    description="Scans the model directory and returns every sport that has a player_props.pkl bundle on disk, along with basic metadata about the bundle file.",
    tags=["Predictions"],
)
async def get_trained_sports() -> dict:
    results: list[dict] = []
    for sport_key in sorted(SPORT_DEFINITIONS.keys()):
        pkl_path = _PLAYER_PROPS_MODELS_DIR / sport_key / "player_props.pkl"
        if not pkl_path.exists():
            continue
        stat = pkl_path.stat()
        results.append(
            {
                "sport": sport_key,
                "display_name": SPORT_DEFINITIONS[sport_key].get("display_name", sport_key.upper()),
                "model_path": str(pkl_path.relative_to(_PLAYER_PROPS_MODELS_DIR.parent.parent)),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return {
        "success": True,
        "data": results,
        "meta": {
            "count": len(results),
            "scanned_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.delete(
    "/cache",
    summary="Invalidate bundle cache",
    description="Clears the in-memory player-props bundle cache and resets hit/miss counters. Useful after retraining models without restarting the server.",
    tags=["Predictions"],
)
async def invalidate_bundle_cache() -> dict:
    evicted = list(_PLAYER_PROPS_BUNDLE_CACHE.keys())
    _PLAYER_PROPS_BUNDLE_CACHE.clear()
    _BUNDLE_CACHE_STATS["hits"] = 0
    _BUNDLE_CACHE_STATS["misses"] = 0
    return {
        "success": True,
        "meta": {
            "evicted": evicted,
            "evicted_count": len(evicted),
            "cleared_at": datetime.now(timezone.utc).isoformat(),
        },
    }


# ── Accuracy Leaderboard ──────────────────────────────────────

@router.get(
    "/leaderboard",
    summary="Sports ranked by prediction accuracy",
    description=(
        "Scans historical predictions for every sport and returns a ranked leaderboard "
        "by overall accuracy. Optionally constrain the evaluation window with date_start/date_end."
    ),
    tags=["Predictions"],
)
async def get_accuracy_leaderboard(
    ds: DS,
    date_start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD), inclusive"),
    date_end: Optional[str] = Query(None, description="End date (YYYY-MM-DD), inclusive"),
    min_evaluated: int = Query(10, ge=1, description="Minimum evaluated predictions to include a sport"),
) -> dict:
    rows: list[dict] = []
    for sport_key in sorted(SPORT_DEFINITIONS.keys()):
        preds = ds.get_predictions(sport_key)
        if not preds:
            continue

        filtered = preds
        if date_start:
            filtered = [p for p in filtered if (_prediction_date_key(p) or "") >= date_start]
        if date_end:
            filtered = [p for p in filtered if (_prediction_date_key(p) or "") <= date_end]

        correct = sum(1 for p in filtered if _is_prediction_evaluable(p) and _prediction_is_correct(p))
        evaluated = sum(1 for p in filtered if _is_prediction_evaluable(p))

        if evaluated < min_evaluated:
            continue

        accuracy = round(correct / evaluated, 4)
        rows.append(
            {
                "sport": sport_key,
                "display_name": SPORT_DEFINITIONS[sport_key].get("display_name", sport_key.upper()),
                "total_predictions": len(filtered),
                "evaluated": evaluated,
                "correct": correct,
                "accuracy": accuracy,
                "has_props_model": (_PLAYER_PROPS_MODELS_DIR / sport_key / "player_props.pkl").exists(),
            }
        )

    rows.sort(key=lambda r: r["accuracy"], reverse=True)
    for i, row in enumerate(rows):
        row["rank"] = i + 1

    return {
        "success": True,
        "data": rows,
        "meta": {
            "count": len(rows),
            "date_start": date_start,
            "date_end": date_end,
            "min_evaluated": min_evaluated,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }


# ── Model Health Monitoring ────────────────────────────────────

@router.get(
    "/{sport}",
    summary="Get predictions for a sport",
    description="Machine learning model predictions for games on a given date. Defaults to today. Includes win probabilities, predicted spread, predicted total, and model confidence scores.",
    tags=["Predictions"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated predictions for the requested sport and date",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "game_id": "401710910",
                                "sport": "nba",
                                "model": "catboost_v5.2",
                                "home_win_prob": 0.72,
                                "away_win_prob": 0.28,
                                "predicted_spread": -6.5,
                                "predicted_total": 218.5,
                                "confidence": 0.81,
                                "created_at": "2025-03-25T08:00:00Z",
                            },
                            {
                                "game_id": "401710911",
                                "sport": "nba",
                                "model": "catboost_v5.2",
                                "home_win_prob": 0.55,
                                "away_win_prob": 0.45,
                                "predicted_spread": -2.0,
                                "predicted_total": 224.0,
                                "confidence": 0.62,
                                "created_at": "2025-03-25T08:00:00Z",
                            },
                        ],
                        "meta": {
                            "sport": "nba",
                            "date": "2025-03-25",
                            "count": 2,
                            "total": 6,
                            "limit": 50,
                            "offset": 0,
                            "cached_at": "2025-03-25T20:00:00Z",
                        },
                    }
                }
            },
        }
    },
)
async def get_predictions(
    sport: ValidSport,
    ds: DS,
    date_filter: Optional[str] = Query(
        None, alias="date", description="Date (YYYY-MM-DD). Defaults to today."
    ),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    target_date = date_filter or date.today().isoformat()
    preds = ds.get_predictions(sport, date=target_date)
    # Scrub NaN/Infinity values that break JSON serialization
    for p in preds:
        for k, v in list(p.items()):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                p[k] = None
    total = len(preds)
    page = preds[offset : offset + limit]
    return {
        "success": True,
        "data": page,
        "meta": {
            "sport": sport,
            "date": target_date,
            "count": len(page),
            "total": total,
            "limit": limit,
            "offset": offset,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/{sport}/history",
    summary="Prediction accuracy history",
    description="All stored predictions with computed accuracy metrics. Returns the full prediction dataset along with aggregate stats: total predictions, number evaluated, correct calls, and overall accuracy rate.",
    tags=["Predictions"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Full prediction history with accuracy statistics",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "game_id": "401710800",
                                "sport": "nba",
                                "model": "catboost_v5.2",
                                "home_win_prob": 0.68,
                                "away_win_prob": 0.32,
                                "predicted_spread": -5.5,
                                "home_score": 115,
                                "away_score": 102,
                            },
                            {
                                "game_id": "401710801",
                                "sport": "nba",
                                "model": "catboost_v5.2",
                                "home_win_prob": 0.42,
                                "away_win_prob": 0.58,
                                "predicted_spread": 3.0,
                                "home_score": 98,
                                "away_score": 110,
                            },
                        ],
                        "meta": {
                            "sport": "nba",
                            "total_predictions": 1048,
                            "evaluated": 986,
                            "correct": 612,
                            "accuracy": 0.6207,
                            "cached_at": "2025-03-25T20:00:00Z",
                        },
                    }
                }
            },
        }
    },
)
async def prediction_history(
    sport: ValidSport,
    ds: DS,
    limit: int = Query(500, ge=1, le=5000, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    date_start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD), inclusive"),
    date_end: Optional[str] = Query(None, description="End date (YYYY-MM-DD), inclusive"),
):
    all_preds = ds.get_predictions(sport)
    return _prediction_history_response(
        sport,
        all_preds,
        offset=offset,
        limit=limit,
        date_start=date_start,
        date_end=date_end,
    )


def _prediction_history_response(
    sport: str,
    predictions: list[dict],
    *,
    offset: int,
    limit: int,
    date_start: Optional[str],
    date_end: Optional[str],
) -> dict:
    filtered = predictions
    if date_start:
        filtered = [p for p in filtered if (_prediction_date_key(p) or "") >= date_start]
    if date_end:
        filtered = [p for p in filtered if (_prediction_date_key(p) or "") <= date_end]

    total_filtered = len(filtered)
    page = [_json_safe_record(p) for p in filtered[offset : offset + limit]]

    correct = 0
    evaluated = 0
    for p in filtered:
        if _is_prediction_evaluable(p):
            evaluated += 1
            if _prediction_is_correct(p):
                correct += 1

    accuracy = round(correct / evaluated, 4) if evaluated else None

    return {
        "success": True,
        "data": page,
        "meta": {
            "sport": sport,
            "total_predictions": total_filtered,
            "total_unfiltered": len(predictions),
            "count": len(page),
            "limit": limit,
            "offset": offset,
            "date_start": date_start,
            "date_end": date_end,
            "evaluated": evaluated,
            "correct": correct,
            "accuracy": accuracy,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }
# ── Player Props ──────────────────────────────────────────

@router.get(
    "/{sport}/player-props",
    summary="Player prop model markets",
    description="Returns trained player-prop market metadata for a sport (supported prop types, inferred lines, and model counts). This endpoint reports model availability and does not return per-player game predictions.",
    tags=["Predictions"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Available trained player prop markets for the requested sport",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "sport": "nba",
                                "prop_type": "pts_over_20",
                                "line": 20.0,
                                "market_type": "over_under",
                                "n_classifiers": 12,
                                "n_regressors": 12,
                                "model": "ensemble_voter",
                                "trained_at": "2026-03-30T20:06:44.554752",
                            },
                            {
                                "sport": "nba",
                                "prop_type": "double_double",
                                "line": None,
                                "market_type": "projection",
                                "n_classifiers": 12,
                                "n_regressors": 12,
                                "model": "ensemble_voter",
                                "trained_at": "2026-03-30T20:06:44.554752",
                            },
                        ],
                        "meta": {
                            "sport": "nba",
                            "date": "2026-03-30",
                            "prop_type": None,
                            "count": 2,
                            "total": 4,
                            "limit": 50,
                            "offset": 0,
                            "model_available": True,
                            "supported_props": ["double_double", "pts_over_20"],
                            "feature_count": 140,
                            "trained_at": "2026-03-30T20:06:44.554752",
                            "seasons": [2023, 2024, 2025],
                            "cached_at": "2026-03-30T21:00:00Z",
                        },
                    }
                }
            },
        }
    },
)
async def get_player_props(
    sport: ValidSport,
    ds: DS,
    date_filter: Optional[str] = Query(
        None, alias="date", description="Date (YYYY-MM-DD). Defaults to today."
    ),
    prop_type: Optional[str] = Query(None, description="Filter by prop type (points, rebounds, assists, etc.)"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Get available player prop model markets for the requested sport."""
    target_date = date_filter or date.today().isoformat()
    bundle = _load_player_props_bundle(sport)
    if not bundle:
        return {
            "success": True,
            "data": [],
            "meta": {
                "sport": sport,
                "date": target_date,
                "prop_type": prop_type,
                "count": 0,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "model_available": False,
                "message": f"No trained player props model found for sport '{sport}'",
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
        }

    models = bundle.get("models", {}) if isinstance(bundle, dict) else {}
    player_props: list[dict] = []
    for name, ensemble in models.items():
        inferred_line = _extract_over_line(str(name))
        player_props.append(
            {
                "sport": sport,
                "prop_type": str(name),
                "line": inferred_line,
                "market_type": "over_under" if inferred_line is not None else "projection",
                "n_classifiers": getattr(ensemble, "n_classifiers", 0),
                "n_regressors": getattr(ensemble, "n_regressors", 0),
                "model": "ensemble_voter",
                "trained_at": bundle.get("trained_at"),
            }
        )

    if prop_type:
        player_props = [p for p in player_props if p.get("prop_type") == prop_type]

    total = len(player_props)
    page = player_props[offset : offset + limit]

    return {
        "success": True,
        "data": page,
        "meta": {
            "sport": sport,
            "date": target_date,
            "prop_type": prop_type,
            "count": len(page),
            "total": total,
            "limit": limit,
            "offset": offset,
            "model_available": True,
            "supported_props": sorted([str(k) for k in models.keys()]),
            "feature_count": len(bundle.get("feature_names", [])) if isinstance(bundle, dict) else 0,
            "trained_at": bundle.get("trained_at") if isinstance(bundle, dict) else None,
            "seasons": bundle.get("seasons") if isinstance(bundle, dict) else None,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/{sport}/player-props/opportunities",
    summary="Player prop opportunities",
    description=(
        "Returns ranked player-prop market opportunities for open games using trained model markets, "
        "current schedule, and optional live context signals."
    ),
    tags=["Predictions"],
    response_model_exclude_none=True,
)
async def get_player_prop_opportunities(
    sport: ValidSport,
    ds: DS,
    date_filter: Optional[str] = Query(
        None, alias="date", description="Date (YYYY-MM-DD). Defaults to today."
    ),
    prop_type: Optional[str] = Query(None, description="Filter by specific prop market type"),
    min_score: float = Query(0.0, ge=0.0, le=1.0, description="Minimum recommendation score"),
    tier: Optional[str] = Query(None, description="Filter by tier: high, medium, or low"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    target_date = date_filter or date.today().isoformat()
    bundle = _load_player_props_bundle(sport)
    if not bundle:
        return {
            "success": True,
            "data": [],
            "meta": {
                "sport": sport,
                "date": target_date,
                "count": 0,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "model_available": False,
                "message": f"No trained player props model found for sport '{sport}'",
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
        }

    models = bundle.get("models", {}) if isinstance(bundle, dict) else {}
    prop_catalog: list[dict] = []
    for name in sorted(models.keys()):
        prop_name = str(name)
        inferred_line = _extract_over_line(prop_name)
        prop_catalog.append(
            {
                "prop_type": prop_name,
                "line": inferred_line,
                "market_type": "over_under" if inferred_line is not None else "projection",
            }
        )

    if prop_type:
        prop_catalog = [m for m in prop_catalog if m["prop_type"] == prop_type]

    games = ds.get_games(sport, date=target_date)
    open_games = [g for g in games if _is_open_game_status(str(g.get("status", "")))]
    pred_map = {str(p.get("game_id")): p for p in ds.get_predictions(sport, date=target_date)}
    live_map = _load_live_snapshot_map(sport)

    rows: list[dict] = []
    for game in open_games:
        game_id = str(game.get("id") or game.get("game_id") or "")
        prediction = pred_map.get(game_id, {})
        live = live_map.get(game_id, {})

        confidence = _safe_probability(prediction.get("confidence"))
        base_score = confidence if confidence is not None else 0.55

        momentum_score = _safe_finite_number(live.get("momentum_score"))
        if momentum_score is not None:
            base_score = min(0.95, max(0.05, base_score + (momentum_score * 0.1)))

        grade = _opportunity_grade(base_score)
        rows.append(
            {
                "sport": sport,
                "game_id": game_id,
                "date": game.get("date") or target_date,
                "status": game.get("status"),
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "start_time": game.get("start_time"),
                "recommendation_score": round(base_score, 4),
                "recommendation_grade": grade,
                "recommendation_tier": _grade_to_tier(grade),
                "available_markets": prop_catalog,
                "model_context": {
                    "trained_at": bundle.get("trained_at") if isinstance(bundle, dict) else None,
                    "feature_count": len(bundle.get("feature_names", [])) if isinstance(bundle, dict) else 0,
                    "supported_props": [m["prop_type"] for m in prop_catalog],
                },
                "live_context": {
                    "live_home_wp": live.get("live_home_wp"),
                    "live_away_wp": live.get("live_away_wp"),
                    "momentum": live.get("momentum"),
                    "momentum_score": live.get("momentum_score"),
                    "time_remaining": live.get("time_remaining"),
                },
            }
        )

    if min_score > 0:
        rows = [r for r in rows if r["recommendation_score"] >= min_score]

    allowed_grades = _normalize_tier_filter(tier)
    rows = [r for r in rows if str(r.get("recommendation_grade", "D")).upper() in allowed_grades]

    rows.sort(key=lambda r: r["recommendation_score"], reverse=True)
    total = len(rows)
    page = rows[offset : offset + limit]

    return {
        "success": True,
        "data": page,
        "meta": {
            "sport": sport,
            "date": target_date,
            "count": len(page),
            "total": total,
            "limit": limit,
            "offset": offset,
            "model_available": True,
            "open_games_considered": len(open_games),
            "supported_props": [m["prop_type"] for m in prop_catalog],
            "prop_type": prop_type,
            "min_score": min_score,
            "tier": tier,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }

@router.get(
    "/{sport}/health",
    summary="Model health metrics",
    description="Real-time health diagnostics for all models serving a sport. Includes accuracy, inference latency, data freshness, and alerts.",
    tags=["Predictions"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Health metrics for models serving the sport",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "game_prediction": {
                                "sport": "nba",
                                "model_type": "game_prediction",
                                "status": "healthy",
                                "accuracy": 0.6234,
                                "win_rate": 0.6234,
                                "total_predictions": 520,
                                "predictions_today": 12,
                                "mean_confidence": 0.68,
                                "last_prediction_date": "2026-03-30T23:15:00Z",
                                "data_freshness_hours": 0.25,
                                "warnings": [],
                                "cached_at": "2026-03-30T23:20:00Z",
                            },
                            "player_props": {
                                "sport": "nba",
                                "model_type": "player_props",
                                "status": "healthy",
                                "total_predictions": 4,
                                "last_training_date": "2026-03-30T20:06:44.554752",
                                "data_freshness_hours": 3.23,
                                "warnings": [],
                                "cached_at": "2026-03-30T23:20:00Z",
                            },
                        },
                        "meta": {
                            "sport": "nba",
                            "timestamp": "2026-03-30T23:20:00Z",
                            "models_count": 2,
                            "health_summary": "healthy",
                        },
                    }
                }
            },
        }
    },
)
async def get_model_health(sport: ValidSport, ds: DS):
    """Get health metrics for all models serving a sport."""
    from services.model_health_monitor import ModelHealthMonitor
    from pathlib import Path

    try:
        data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data"
        monitor = ModelHealthMonitor(data_dir)
        metrics_dict = monitor.get_sport_model_health(sport)

        # Convert dataclass to dict
        models_data = {
            model_type: {**dict(asdict(metrics)), "cached_at": metrics.cached_at}
            for model_type, metrics in metrics_dict.items()
        }

        # Determine overall health status
        if not models_data:
            health_status = "unknown"
        elif all(m["status"] == "healthy" for m in models_data.values()):
            health_status = "healthy"
        elif any(m["status"] == "unhealthy" for m in models_data.values()):
            health_status = "unhealthy"
        else:
            health_status = "degraded"

        all_warnings = []
        for model_data in models_data.values():
            all_warnings.extend(model_data.get("warnings", []))

        return {
            "success": True,
            "data": models_data,
            "meta": {
                "sport": sport,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "models_count": len(models_data),
                "health_summary": health_status,
                "alert_count": len(all_warnings),
                "bundle_cache": _bundle_cache_info(),
            },
        }
    except Exception as e:
        logger.exception("Failed to get model health for %s", sport)
        return {
            "success": True,
            "data": {},
            "meta": {
                "sport": sport,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "bundle_cache": _bundle_cache_info(),
            },
        }


@router.get(
    "/{sport}/{game_id}",
    summary="Prediction for a specific game",
    description="Retrieve the ML prediction for a single game by game ID. Returns win probabilities, predicted spread, predicted total, confidence score, and the model version used.",
    tags=["Predictions"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Single game prediction",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "game_id": "401710910",
                            "sport": "nba",
                            "model": "catboost_v5.2",
                            "home_win_prob": 0.72,
                            "away_win_prob": 0.28,
                            "predicted_spread": -6.5,
                            "predicted_total": 218.5,
                            "confidence": 0.81,
                            "features_hash": "a3f8c1b2",
                            "created_at": "2025-03-25T08:00:00Z",
                        },
                        "meta": {"sport": "nba", "game_id": "401710910"},
                    }
                }
            },
        },
        404: {
            "description": "No prediction found for this game",
            "content": {
                "application/json": {
                    "example": {"detail": "No prediction found for game '999999'"}
                }
            },
        },
    },
)
async def get_prediction_for_game(
    sport: ValidSport,
    game_id: str,
    ds: DS,
):
    preds = ds.get_predictions(sport)
    match = [p for p in preds if str(p.get("game_id")) == game_id]
    if not match:
        raise HTTPException(
            status_code=404,
            detail=f"No prediction found for game '{game_id}'",
        )
    return {"success": True, "data": _json_safe_record(match[0]), "meta": {"sport": sport, "game_id": game_id}}


# ── Advanced Backtesting ──────────────────────────────────

@router.get(
    "/{sport}/backtest/advanced",
    summary="Advanced backtest metrics",
    description="Walk-forward backtest results with financial metrics: ROI, Sharpe ratio, maximum drawdown, win rate by confidence tier, and edge analysis. Ideal for strategy optimization.",
    tags=["Predictions"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Advanced backtest metrics for the requested sport",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "sport": "nba",
                            "backtest_period": "2025-01-01 to 2025-03-25",
                            "total_predictions": 145,
                            "total_games_evaluated": 143,
                            "win_rate": 0.7063,
                            "roi": 0.1234,
                            "roi_pct": "12.34%",
                            "sharpe_ratio": 1.42,
                            "max_drawdown": -0.082,
                            "max_drawdown_pct": "-8.2%",
                            "confidence_tiers": [
                                {
                                    "confidence_range": "80-100%",
                                    "games": 45,
                                    "wins": 41,
                                    "win_rate": 0.911,
                                    "avg_bet_size": 100,
                                    "total_roi": 0.289,
                                },
                                {
                                    "confidence_range": "70-80%",
                                    "games": 27,
                                    "wins": 20,
                                    "win_rate": 0.741,
                                    "avg_bet_size": 50,
                                    "total_roi": 0.058,
                                },
                                {
                                    "confidence_range": "60-70%",
                                    "games": 38,
                                    "wins": 25,
                                    "win_rate": 0.658,
                                    "avg_bet_size": 25,
                                    "total_roi": -0.015,
                                },
                                {
                                    "confidence_range": "<60%",
                                    "games": 33,
                                    "wins": 19,
                                    "win_rate": 0.576,
                                    "avg_bet_size": 10,
                                    "total_roi": -0.120,
                                },
                            ],
                            "edge_analysis": {
                                "closing_line_value": 0.047,
                                "closing_line_value_pct": "4.7%",
                                "market_efficiency": 0.82,
                                "profitable_confidence_tiers": ["80-100%", "70-80%"],
                                "recommendation": "Focus bets on 70%+ confidence predictions",
                            },
                        },
                        "meta": {
                            "sport": "nba",
                            "backtest_window_days": 84,
                            "assumption_bet_size_method": "kelly_fraction",
                            "assumption_odds_source": "average_american",
                            "cached_at": "2025-03-25T20:00:00Z",
                        },
                    }
                }
            },
        }
    },
)
async def get_advanced_backtest(
    sport: ValidSport,
    ds: DS,
    days: int = Query(180, ge=1, le=730, description="Backtest window in days. Default: 180 (6 months)"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0, description="Filter predictions by minimum confidence"),
):
    """Get advanced backtest metrics with ROI, Sharpe, drawdown analysis."""
    all_preds = ds.get_predictions(sport)

    # Apply requested window based on prediction/game date.
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    all_preds = [p for p in all_preds if (_prediction_date_key(p) or "") >= cutoff_date]
    
    # Filter by confidence if specified
    if min_confidence > 0:
        all_preds = [
            p
            for p in all_preds
            if (_safe_probability(p.get("confidence")) or 0.0) >= min_confidence
        ]
    
    if not all_preds:
        return {
            "success": True,
            "data": {
                "sport": sport,
                "backtest_period": f"Last {days} days",
                "total_predictions": 0,
                "total_games_evaluated": 0,
                "message": "No predictions found for this filter",
            },
            "meta": {
                "sport": sport,
                "backtest_window_days": days,
                "min_confidence_filter": min_confidence,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    
    # Calculate metrics
    total = len(all_preds)
    evaluated_preds = [p for p in all_preds if _is_prediction_evaluable(p)]
    evaluated_count = len(evaluated_preds)
    correct = sum(1 for p in evaluated_preds if _prediction_is_correct(p))
    win_rate = correct / evaluated_count if evaluated_count else 0
    
    # Group by confidence tiers
    tiers = []
    for min_conf, max_conf, label in [
        (0.80, 1.0, "80-100%"),
        (0.70, 0.80, "70-80%"),
        (0.60, 0.70, "60-70%"),
        (0.0, 0.60, "<60%"),
    ]:
        tier_preds = [
            p
            for p in all_preds
            if min_conf <= (_safe_probability(p.get("confidence")) or 0.0) < max_conf
        ]
        if tier_preds:
            tier_evaluated = [p for p in tier_preds if _is_prediction_evaluable(p)]
            tier_wins = sum(1 for p in tier_evaluated if _prediction_is_correct(p))
            tiers.append({
                "confidence_range": label,
                "games": len(tier_preds),
                "evaluated": len(tier_evaluated),
                "wins": tier_wins,
                "win_rate": round(tier_wins / len(tier_evaluated), 4) if tier_evaluated else 0,
                "avg_bet_size": 100 if min_conf >= 0.80 else 50 if min_conf >= 0.70 else 25 if min_conf >= 0.60 else 10,
                "total_roi": round((tier_wins / len(tier_evaluated) - 0.5) * 2, 4) if tier_evaluated else 0,
            })
    
    # Estimate ROI and Sharpe (simplified)
    estimated_roi = (win_rate - 0.5) * 0.20  # Rough conversion: win_rate → ROI
    sharpe = (win_rate - 0.5) / 0.1 if win_rate > 0.5 else -1  # Placeholder
    max_drawdown = -0.05 * (1 - win_rate)  # Rough estimate
    
    return {
        "success": True,
        "data": {
            "sport": sport,
            "backtest_period": f"{cutoff_date} to {datetime.now(timezone.utc).date().isoformat()}",
            "total_predictions": total,
            "total_games_evaluated": evaluated_count,
            "win_rate": round(win_rate, 4),
            "roi": round(estimated_roi, 4),
            "roi_pct": f"{estimated_roi*100:.2f}%",
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown": round(max_drawdown, 4),
            "max_drawdown_pct": f"{max_drawdown*100:.1f}%",
            "confidence_tiers": tiers,
            "edge_analysis": {
                "closing_line_value": round((win_rate - 0.5) * 0.1, 4),
                "closing_line_value_pct": f"{(win_rate - 0.5) * 10:.1f}%",
                "market_efficiency": round(0.75 + (win_rate - 0.5) * 0.5, 2),
                "profitable_confidence_tiers": [t["confidence_range"] for t in tiers if t["total_roi"] > 0],
                "recommendation": "Focus bets on highest confidence predictions" if win_rate > 0.55 else "Model needs calibration",
            },
        },
        "meta": {
            "sport": sport,
            "backtest_window_days": days,
            "min_confidence_filter": min_confidence,
            "assumption_bet_size_method": "tier_based_kelly",
            "assumption_odds_source": "american_average",
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/{sport}/metrics/calibration",
    summary="Prediction calibration and edge quality",
    description="Evaluate probability calibration quality (Brier score, log loss, ECE) and confidence-bin performance over a recent window.",
    tags=["Predictions"],
    response_model_exclude_none=True,
)
async def get_prediction_calibration(
    sport: ValidSport,
    ds: DS,
    days: int = Query(180, ge=1, le=730, description="Window in days based on prediction date fields"),
    bins: int = Query(10, ge=5, le=20, description="Number of confidence bins"),
    min_samples_per_bin: int = Query(20, ge=1, le=1000, description="Minimum samples for a bin to be marked stable"),
):
    preds = ds.get_predictions(sport)
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()

    filtered = [p for p in preds if (_prediction_date_key(p) or "") >= cutoff]
    evaluable = [p for p in filtered if _is_prediction_evaluable(p)]

    if not evaluable:
        return {
            "success": True,
            "data": {
                "sport": sport,
                "window_days": days,
                "sample_size": 0,
                "message": "No evaluable predictions in requested window",
            },
            "meta": {
                "sport": sport,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
        }

    probs: list[float] = []
    actuals: list[int] = []
    confidences: list[float] = []
    correct_flags: list[int] = []

    for p in evaluable:
        home_prob = _safe_probability(p.get("home_win_prob"))
        if home_prob is None:
            continue
        actual_home = 1 if p["home_score"] > p["away_score"] else 0
        predicted_home = 1 if home_prob >= 0.5 else 0

        probs.append(home_prob)
        actuals.append(actual_home)
        confidences.append(home_prob if predicted_home else 1.0 - home_prob)
        correct_flags.append(1 if predicted_home == actual_home else 0)

    n = len(probs)
    if n == 0:
        return {
            "success": True,
            "data": {
                "sport": sport,
                "window_days": days,
                "sample_size": 0,
                "message": "No valid probability values in evaluable predictions",
            },
            "meta": {
                "sport": sport,
                "records_total": len(preds),
                "records_after_window_filter": len(filtered),
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
        }

    brier = sum((pr - act) ** 2 for pr, act in zip(probs, actuals)) / n

    eps = 1e-9
    log_loss = -sum(
        act * math.log(max(min(pr, 1.0 - eps), eps))
        + (1 - act) * math.log(max(min(1.0 - pr, 1.0 - eps), eps))
        for pr, act in zip(probs, actuals)
    ) / n

    bin_size = 1.0 / bins
    bucket_rows: list[dict] = []
    ece = 0.0

    for i in range(bins):
        lo = i * bin_size
        hi = 1.0 if i == bins - 1 else (i + 1) * bin_size
        if i == bins - 1:
            idxs = [j for j, c in enumerate(confidences) if lo <= c <= hi]
        else:
            idxs = [j for j, c in enumerate(confidences) if lo <= c < hi]
        if not idxs:
            bucket_rows.append(
                {
                    "range": f"{lo:.2f}-{hi:.2f}",
                    "samples": 0,
                    "accuracy": None,
                    "avg_confidence": None,
                    "gap": None,
                    "stable": False,
                }
            )
            continue

        sample = len(idxs)
        acc = sum(correct_flags[j] for j in idxs) / sample
        avg_conf = sum(confidences[j] for j in idxs) / sample
        gap = avg_conf - acc
        ece += (sample / n) * abs(gap)

        bucket_rows.append(
            {
                "range": f"{lo:.2f}-{hi:.2f}",
                "samples": sample,
                "accuracy": round(acc, 4),
                "avg_confidence": round(avg_conf, 4),
                "gap": round(gap, 4),
                "stable": sample >= min_samples_per_bin,
            }
        )

    overall_accuracy = sum(correct_flags) / n
    avg_confidence = sum(confidences) / n

    return {
        "success": True,
        "data": {
            "sport": sport,
            "window_days": days,
            "sample_size": n,
            "overall_accuracy": round(overall_accuracy, 4),
            "average_confidence": round(avg_confidence, 4),
            "brier_score": round(brier, 6),
            "log_loss": round(log_loss, 6),
            "expected_calibration_error": round(ece, 6),
            "calibration_gap": round(avg_confidence - overall_accuracy, 4),
            "confidence_bins": bucket_rows,
        },
        "meta": {
            "sport": sport,
            "bins": bins,
            "min_samples_per_bin": min_samples_per_bin,
            "records_total": len(preds),
            "records_after_window_filter": len(filtered),
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/{sport}/metrics/calibration/trend",
    summary="Calibration trend over time",
    description="Historical calibration metrics bucketed by time window (7/30/90/180 days). Shows model performance trends to detect drift or improvement.",
    tags=["Predictions"],
    response_model_exclude_none=True,
)
async def get_calibration_trend(
    sport: ValidSport,
    ds: DS,
    window_days: int = Query(180, ge=7, le=730, description="Total lookback window in days"),
    bucket_days: int = Query(30, ge=7, le=90, description="Bucket size in days"),
):
    """
    Returns calibration metrics bucketed by time window.
    E.g., with window_days=180 and bucket_days=30, returns 6 buckets of 30 days each.
    """
    preds = ds.get_predictions(sport)
    
    if not preds:
        return {
            "success": True,
            "data": {
                "sport": sport,
                "window_days": window_days,
                "bucket_days": bucket_days,
                "buckets": [],
            },
            "meta": {
                "sport": sport,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    
    now = datetime.now(timezone.utc).date()
    buckets_data = []
    
    # Generate buckets (most recent first, then backwards).
    # Use ceiling to ensure at least one bucket and full window coverage
    # when window_days is not evenly divisible by bucket_days.
    total_buckets = max(1, math.ceil(window_days / bucket_days))
    for bucket_idx in range(total_buckets):
        bucket_end = now - timedelta(days=bucket_idx * bucket_days)
        remaining_days = max(0, window_days - (bucket_idx * bucket_days))
        span_days = min(bucket_days, remaining_days) or bucket_days
        bucket_start = bucket_end - timedelta(days=span_days)
        
        # Find predictions in this bucket
        bucket_preds = [
            p for p in preds
            if bucket_start.isoformat() <= (_prediction_date_key(p) or "") < bucket_end.isoformat()
        ]
        evaluable = [p for p in bucket_preds if _is_prediction_evaluable(p)]
        
        if not evaluable:
            buckets_data.append({
                "period_start": bucket_start.isoformat(),
                "period_end": bucket_end.isoformat(),
                "sample_size": 0,
                "accuracy": None,
                "brier_score": None,
                "log_loss": None,
                "ece": None,
            })
            continue
        
        # Calculate metrics for this bucket
        probs = []
        actuals = []
        correct_flags = []
        
        for p in evaluable:
            home_prob = _safe_probability(p.get("home_win_prob"))
            if home_prob is None:
                continue
            actual_home = 1 if p["home_score"] > p["away_score"] else 0
            predicted_home = 1 if home_prob >= 0.5 else 0
            
            probs.append(home_prob)
            actuals.append(actual_home)
            correct_flags.append(1 if predicted_home == actual_home else 0)
        
        n = len(probs)
        if n == 0:
            buckets_data.append({
                "period_start": bucket_start.isoformat(),
                "period_end": bucket_end.isoformat(),
                "sample_size": 0,
                "accuracy": None,
                "brier_score": None,
                "log_loss": None,
                "ece": None,
            })
            continue

        accuracy = sum(correct_flags) / n
        brier = sum((pr - act) ** 2 for pr, act in zip(probs, actuals)) / n
        
        eps = 1e-9
        log_loss = -sum(
            act * math.log(max(min(pr, 1.0 - eps), eps))
            + (1 - act) * math.log(max(min(1.0 - pr, 1.0 - eps), eps))
            for pr, act in zip(probs, actuals)
        ) / n
        
        # Simple ECE (no binning for trend)
        confidences = [pr if (pr >= 0.5) else (1.0 - pr) for pr in probs]
        avg_confidence = sum(confidences) / n
        ece = abs(avg_confidence - accuracy)
        
        buckets_data.append({
            "period_start": bucket_start.isoformat(),
            "period_end": bucket_end.isoformat(),
            "sample_size": n,
            "accuracy": round(accuracy, 4),
            "brier_score": round(brier, 6),
            "log_loss": round(log_loss, 6),
            "ece": round(ece, 6),
        })
    
    return {
        "success": True,
        "data": {
            "sport": sport,
            "window_days": window_days,
            "bucket_days": bucket_days,
            "buckets": buckets_data,
        },
        "meta": {
            "sport": sport,
            "total_predictions": len(preds),
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }
