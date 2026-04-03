# ──────────────────────────────────────────────────────────
# V5.0 Backend — Charts & Visualization Routes
# ──────────────────────────────────────────────────────────
# GET /v1/{sport}/charts/stat-trend        — Recharts LineChart data
# GET /v1/{sport}/charts/distribution      — Histogram bins
# GET /v1/{sport}/charts/correlation.png   — Seaborn heatmap PNG
# GET /v1/{sport}/charts/leaders-bar       — Plotly/recharts bar data
# GET /v1/{sport}/charts/win-probability   — Play-by-play wp stream
# POST /v1/{sport}/charts/scatter          — Altair Vega-Lite scatter spec
# POST /v1/{sport}/charts/heatmap          — Altair correlation heatmap spec
# GET /v1/{sport}/charts/forecast          — Prophet time-series forecast
# POST /v1/{sport}/charts/wordcloud.png    — WordCloud from text list
# GET /v1/{sport}/charts/shap              — SHAP feature importance (ML model)

from __future__ import annotations

import logging
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel

from auth.middleware import require_api_key
from auth.models import APIKeyInfo
from config import SPORT_DEFINITIONS
from services.data_service import DataService, get_data_service
from services.chart_service import ChartService, get_chart_service

router = APIRouter(prefix="/v1/{sport}/charts", tags=["charts"])
logger = logging.getLogger(__name__)

DS = Annotated[DataService, Depends(get_data_service)]
CS = Annotated[ChartService, Depends(get_chart_service)]
ApiKey = Annotated[APIKeyInfo, Depends(require_api_key)]


def _sport(sport: str) -> str:
    if sport not in SPORT_DEFINITIONS:
        raise HTTPException(status_code=404, detail=f"Unknown sport '{sport}'")
    return sport


ValidSport = Annotated[str, Depends(_sport)]


# ── Pydantic models ────────────────────────────────────────

class ScatterRequest(BaseModel):
    records: list[dict[str, Any]]
    x: str
    y: str
    color: Optional[str] = None
    tooltip: Optional[list[str]] = None
    title: str = ""

class HeatmapRequest(BaseModel):
    records: list[dict[str, Any]]
    fields: list[str]
    title: str = "Correlation Heatmap"

class WordCloudRequest(BaseModel):
    texts: list[str]
    max_words: int = 150
    colormap: str = "Set2"


# ── Endpoints ─────────────────────────────────────────────

@router.get("/stat-trend")
async def stat_trend(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    season: Optional[str] = Query(None),
    metrics: str = Query("points,assists,rebounds", description="Comma-separated stat fields"),
    limit: int = Query(50, le=200),
) -> dict:
    """
    Returns recharts LineChart-compatible data for player/team stat trends over time.
    Useful for the stats pages — plug directly into <LineChart data={...}>.
    """
    fields = [m.strip() for m in metrics.split(",")]
    players = ds.get_player_stats(sport, season=season, columns=["game_date"] + fields)
    if not players:
        return {"data": [], "fields": fields}
    data = cs.stat_trend_data(players[:limit], x_field="game_date", y_fields=fields)
    return {"data": data, "fields": fields, "sport": sport, "season": season}


@router.get("/distribution")
async def distribution(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    field: str = Query("points", description="Stat field to build histogram for"),
    season: Optional[str] = Query(None),
    bins: int = Query(20, le=50),
) -> dict:
    """Histogram distribution for any numeric stat field — recharts BarChart data."""
    players = ds.get_player_stats(sport, season=season, columns=[field])
    data = cs.distribution_data(players, field=field, bins=bins)
    return {"data": data, "field": field, "sport": sport, "bin_count": bins}


@router.get("/correlation.png", response_class=Response)
async def correlation_png(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    fields: str = Query(
        "points,assists,rebounds,steals,blocks",
        description="Comma-separated fields for correlation matrix",
    ),
    season: Optional[str] = Query(None),
    title: str = Query(""),
) -> Response:
    """Seaborn correlation heatmap as PNG. Embed directly in <img src=...>."""
    field_list = [f.strip() for f in fields.split(",")]
    players = ds.get_player_stats(sport, season=season, columns=field_list)
    png = cs.seaborn_correlation_png(
        players,
        fields=field_list,
        title=title or f"{sport.upper()} Stat Correlations",
    )
    if not png:
        raise HTTPException(status_code=422, detail="Insufficient data for correlation matrix")
    return Response(content=png, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"})


@router.get("/distribution.png", response_class=Response)
async def distribution_png(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    field: str = Query("points"),
    season: Optional[str] = Query(None),
    hue: Optional[str] = Query(None, description="Optional grouping field"),
) -> Response:
    """KDE distribution plot as PNG."""
    players = ds.get_player_stats(sport, season=season, columns=[field] + ([hue] if hue else []))
    png = cs.seaborn_distribution_png(
        players, field=field,
        title=f"{sport.upper()} {field.replace('_', ' ').title()} Distribution",
        hue_field=hue,
    )
    if not png:
        raise HTTPException(status_code=422, detail="No data for distribution")
    return Response(content=png, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"})


@router.get("/leaders-bar")
async def leaders_bar(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    metric: str = Query("points"),
    season: Optional[str] = Query(None),
    limit: int = Query(15, le=30),
    format: str = Query("recharts", description="recharts | plotly"),
) -> dict:
    """Top N players by stat — bar chart data."""
    players = ds.get_player_stats(sport, season=season, columns=["player_name", metric])
    players_sorted = sorted(
        [p for p in players if p.get(metric) is not None],
        key=lambda p: float(p.get(metric, 0)),
        reverse=True,
    )[:limit]

    if format == "plotly":
        fig = cs.plotly_bar(
            players_sorted, x="player_name", y=metric,
            title=f"Top {limit} by {metric.replace('_', ' ').title()} — {sport.upper()}",
            orientation="h",
        )
        return {"figure": fig, "format": "plotly"}

    # recharts BarChart format
    data = [{"name": p.get("player_name", "?"),
             "value": round(float(p.get(metric, 0)), 2)} for p in players_sorted]
    return {"data": data, "metric": metric, "format": "recharts"}


@router.get("/win-probability")
async def win_probability(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    game_id: str = Query(...),
    season: Optional[str] = Query(None),
) -> dict:
    """
    Play-by-play win probability stream for a specific game.
    Returns recharts AreaChart-compatible data.
    """
    pbp_data = []
    try:
        game = ds.get_game(sport, game_id, season=season)
        pbp_data = game.get("play_by_play", []) if game else []
    except Exception:
        pass
    data = cs.win_probability_stream(pbp_data)
    return {"data": data, "game_id": game_id, "sport": sport}


@router.post("/scatter")
async def scatter_spec(
    sport: ValidSport,
    cs: CS,
    _key: ApiKey,
    body: ScatterRequest,
) -> dict:
    """
    Returns an Altair/Vega-Lite JSON spec for an interactive scatter plot.
    Render on frontend with: <VegaEmbed spec={spec} />
    """
    spec = cs.altair_scatter(
        body.records, x=body.x, y=body.y,
        color=body.color, tooltip=body.tooltip, title=body.title,
    )
    return {"spec": spec, "schema": "vega-lite"}


@router.post("/heatmap")
async def heatmap_spec(
    sport: ValidSport,
    cs: CS,
    _key: ApiKey,
    body: HeatmapRequest,
) -> dict:
    """
    Returns an Altair/Vega-Lite correlation heatmap JSON spec.
    Render on frontend with: <VegaEmbed spec={spec} />
    """
    import pandas as pd
    df = pd.DataFrame(body.records)[body.fields].select_dtypes("number")
    corr = df.corr()
    spec = cs.altair_heatmap(corr, title=body.title)
    return {"spec": spec, "schema": "vega-lite"}


@router.get("/forecast")
async def forecast(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    field: str = Query("points", description="Stat field to forecast"),
    season: Optional[str] = Query(None, description="Historical season(s) to train on"),
    periods: int = Query(16, le=52, description="Future periods to forecast"),
    freq: str = Query("W", description="Frequency: D=daily, W=weekly, M=monthly"),
) -> dict:
    """
    Prophet time-series forecast for a stat field.
    Returns recharts AreaChart-compatible data with forecast + confidence bands.
    """
    records = ds.get_player_stats(sport, season=season, columns=["game_date", field])
    # Aggregate by date for a sport-wide trend
    import pandas as pd
    df = pd.DataFrame(records)
    if df.empty or field not in df.columns:
        return {"error": f"No data for {field}", "data": []}

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df[field] = pd.to_numeric(df[field], errors="coerce")
    agg = df.dropna(subset=["game_date", field]).groupby("game_date")[field].mean().reset_index()
    agg.columns = ["game_date", field]

    result = cs.prophet_forecast(
        agg.to_dict("records"),
        date_field="game_date",
        value_field=field,
        periods=periods,
        freq=freq,
    )
    return {**result, "sport": sport, "season": season}


@router.post("/wordcloud.png", response_class=Response)
async def wordcloud(
    sport: ValidSport,
    cs: CS,
    _key: ApiKey,
    body: WordCloudRequest,
) -> Response:
    """
    Generate a WordCloud PNG from a list of text strings.
    Useful for news headlines, Google Trends keywords, player mentions.
    """
    png = cs.wordcloud_png(
        body.texts,
        max_words=body.max_words,
        colormap=body.colormap,
    )
    if not png:
        raise HTTPException(status_code=422, detail="No text provided")
    return Response(content=png, media_type="image/png",
                    headers={"Cache-Control": "no-cache"})


@router.get("/shap")
async def shap_importance(
    sport: ValidSport,
    ds: DS,
    cs: CS,
    _key: ApiKey,
    season: Optional[str] = Query(None),
    max_features: int = Query(15, le=30),
) -> dict:
    """
    SHAP feature importance for the trained ML model for this sport.
    Returns recharts BarChart data showing what drives predictions.
    """
    import pickle
    import pathlib

    model_dir = pathlib.Path(__file__).resolve().parents[3] / "ml" / "models"
    bundle_path = model_dir / f"{sport}_bundle.pkl"
    if not bundle_path.exists():
        raise HTTPException(status_code=404, detail=f"No trained model for {sport}")

    try:
        with open(bundle_path, "rb") as f:
            bundle = pickle.load(f)
        model = bundle.get("model") or bundle.get("ensemble")
        feature_df = bundle.get("feature_df")
        if model is None or feature_df is None:
            raise ValueError("Bundle missing model or feature_df")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Model load error: {exc}")

    import pandas as pd
    if not isinstance(feature_df, pd.DataFrame):
        feature_df = pd.DataFrame(feature_df)

    importance = cs.shap_feature_importance(model, feature_df, max_features=max_features)
    if not importance:
        raise HTTPException(status_code=422, detail="SHAP computation failed")

    return {
        "data": importance,
        "sport": sport,
        "model": str(bundle_path.name),
        "feature_count": len(importance),
    }
