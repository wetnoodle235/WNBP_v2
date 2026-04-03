# ──────────────────────────────────────────────────────────
# V5.0 Backend — Chart & Visualization Service
# ──────────────────────────────────────────────────────────
# Generates chart specs and static exports using:
#   • Plotly — interactive JSON specs + PNG exports (via kaleido)
#   • Altair  — Vega-Lite JSON specs for the frontend
#   • Seaborn/Matplotlib — heatmaps, distributions → PNG
#   • WordCloud — topic clouds from news/trends data
#   • SHAP — model explainability plots
#   • Prophet — time-series forecasting
#
# All methods return either:
#   - dict: JSON spec ready to serialize
#   - bytes: PNG/SVG image data
#   - list[dict]: recharts-compatible data array

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── Lazy imports (heavy packages) ────────────────────────────

def _plotly():
    import plotly.graph_objects as go
    import plotly.express as px
    return go, px

def _altair():
    import altair as alt
    return alt

def _seaborn():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns
    return plt, sns

def _wordcloud():
    from wordcloud import WordCloud
    return WordCloud

def _prophet():
    from prophet import Prophet
    return Prophet

def _shap():
    import shap
    return shap


# ── Chart Service ─────────────────────────────────────────────

class ChartService:

    # ── Recharts-compatible data helpers ──────────────────────

    def stat_trend_data(
        self,
        records: list[dict],
        x_field: str,
        y_fields: list[str],
        label_field: Optional[str] = None,
    ) -> list[dict]:
        """Convert raw records to recharts LineChart data format."""
        out = []
        for r in records:
            row: dict[str, Any] = {x_field: r.get(x_field)}
            for f in y_fields:
                if f in r and r[f] is not None:
                    try:
                        row[f] = round(float(r[f]), 3)
                    except (TypeError, ValueError):
                        row[f] = r[f]
            if label_field:
                row["label"] = r.get(label_field)
            out.append(row)
        return out

    def distribution_data(
        self,
        records: list[dict],
        field: str,
        bins: int = 20,
    ) -> list[dict]:
        """Build histogram bins for recharts BarChart."""
        values = [float(r[field]) for r in records if r.get(field) is not None]
        if not values:
            return []
        hist, edges = np.histogram(values, bins=bins)
        return [
            {"range": f"{edges[i]:.1f}–{edges[i+1]:.1f}", "count": int(hist[i])}
            for i in range(len(hist))
        ]

    def win_probability_stream(
        self,
        play_by_play: list[dict],
    ) -> list[dict]:
        """Format play-by-play win probability for recharts AreaChart."""
        return [
            {
                "event": i,
                "desc": r.get("description", "")[:40],
                "home_wp": round(float(r.get("home_win_probability", 0.5)), 3),
                "away_wp": round(1 - float(r.get("home_win_probability", 0.5)), 3),
            }
            for i, r in enumerate(play_by_play)
            if r.get("home_win_probability") is not None
        ]

    def radar_data(
        self,
        entity: dict,
        metrics: list[str],
        label: str = "value",
    ) -> list[dict]:
        """Build radar chart data for recharts RadarChart."""
        return [
            {
                "metric": m.replace("_", " ").title(),
                label: round(float(entity.get(m, 0)), 2),
            }
            for m in metrics
            if entity.get(m) is not None
        ]

    # ── Altair JSON specs (Vega-Lite) ─────────────────────────

    def altair_scatter(
        self,
        records: list[dict],
        x: str,
        y: str,
        color: Optional[str] = None,
        tooltip: Optional[list[str]] = None,
        title: str = "",
    ) -> dict:
        """Return Vega-Lite JSON spec for an interactive scatter plot."""
        alt = _altair()
        df = pd.DataFrame(records)
        if df.empty:
            return {}
        enc: dict[str, Any] = {
            "x": alt.X(x, type="quantitative"),
            "y": alt.Y(y, type="quantitative"),
        }
        if color and color in df.columns:
            enc["color"] = alt.Color(color, type="nominal")
        if tooltip:
            enc["tooltip"] = [alt.Tooltip(t) for t in tooltip if t in df.columns]

        chart = (
            alt.Chart(df)
            .mark_circle(size=60, opacity=0.7)
            .encode(**enc)
            .properties(title=title, width="container", height=300)
            .interactive()
        )
        return json.loads(chart.to_json())

    def altair_heatmap(
        self,
        matrix_df: pd.DataFrame,
        title: str = "Correlation Heatmap",
    ) -> dict:
        """Vega-Lite heatmap spec from a correlation matrix DataFrame."""
        alt = _altair()
        melted = matrix_df.reset_index().melt(id_vars="index")
        melted.columns = ["x", "y", "correlation"]
        chart = (
            alt.Chart(melted)
            .mark_rect()
            .encode(
                x=alt.X("x:N", title=None),
                y=alt.Y("y:N", title=None),
                color=alt.Color(
                    "correlation:Q",
                    scale=alt.Scale(scheme="redblue", domain=[-1, 1]),
                ),
                tooltip=["x", "y", alt.Tooltip("correlation:Q", format=".2f")],
            )
            .properties(title=title, width="container", height=320)
        )
        return json.loads(chart.to_json())

    # ── Plotly JSON specs ─────────────────────────────────────

    def plotly_line(
        self,
        records: list[dict],
        x: str,
        y_series: dict[str, str],  # {series_name: field_name}
        title: str = "",
        y_label: str = "",
    ) -> dict:
        """Multi-line Plotly chart. Returns full figure dict for plotly.js."""
        go, px = _plotly()
        df = pd.DataFrame(records)
        if df.empty:
            return {}
        traces = []
        for name, field in y_series.items():
            if field in df.columns:
                traces.append(go.Scatter(
                    x=df[x].tolist(), y=df[field].tolist(),
                    mode="lines+markers", name=name,
                ))
        fig = go.Figure(data=traces)
        fig.update_layout(
            title=title, yaxis_title=y_label,
            template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40),
            height=320,
        )
        return fig.to_dict()

    def plotly_bar(
        self,
        records: list[dict],
        x: str,
        y: str,
        color: Optional[str] = None,
        title: str = "",
        orientation: str = "v",
    ) -> dict:
        go, px = _plotly()
        df = pd.DataFrame(records)
        if df.empty:
            return {}
        kwargs: dict[str, Any] = dict(x=x, y=y, title=title, template="plotly_dark", height=320)
        if color and color in df.columns:
            kwargs["color"] = color
        if orientation == "h":
            kwargs["x"], kwargs["y"] = y, x
            kwargs["orientation"] = "h"
        fig = px.bar(df, **kwargs)
        fig.update_layout(margin=dict(l=40, r=20, t=40, b=40))
        return fig.to_dict()

    def plotly_png(self, figure_dict: dict) -> bytes:
        """Export a plotly figure dict to PNG bytes via kaleido."""
        go, _ = _plotly()
        fig = go.Figure(figure_dict)
        return fig.to_image(format="png", width=800, height=400, scale=2)

    # ── Seaborn PNG exports ────────────────────────────────────

    def seaborn_correlation_png(
        self,
        records: list[dict],
        fields: list[str],
        title: str = "Feature Correlation",
    ) -> bytes:
        plt, sns = _seaborn()
        df = pd.DataFrame(records)[fields].dropna()
        if df.empty or len(df.columns) < 2:
            return b""
        corr = df.corr()
        fig, ax = plt.subplots(figsize=(max(6, len(fields)), max(5, len(fields) - 1)))
        sns.heatmap(
            corr, annot=True, fmt=".2f", cmap="coolwarm",
            center=0, square=True, linewidths=0.5, ax=ax,
        )
        ax.set_title(title, pad=12)
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        return buf.getvalue()

    def seaborn_distribution_png(
        self,
        records: list[dict],
        field: str,
        title: str = "",
        hue_field: Optional[str] = None,
    ) -> bytes:
        plt, sns = _seaborn()
        df = pd.DataFrame(records).dropna(subset=[field])
        if df.empty:
            return b""
        fig, ax = plt.subplots(figsize=(8, 4))
        kwargs: dict[str, Any] = {"ax": ax, "fill": True, "alpha": 0.7}
        if hue_field and hue_field in df.columns:
            kwargs["hue"] = hue_field
        sns.kdeplot(data=df, x=field, **kwargs)
        ax.set_title(title or f"Distribution of {field}", pad=10)
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        return buf.getvalue()

    # ── WordCloud PNG ──────────────────────────────────────────

    def wordcloud_png(
        self,
        texts: list[str],
        width: int = 800,
        height: int = 400,
        background_color: str = "black",
        colormap: str = "Set2",
        max_words: int = 150,
    ) -> bytes:
        WordCloud = _wordcloud()
        combined = " ".join(texts)
        if not combined.strip():
            return b""
        wc = WordCloud(
            width=width, height=height,
            background_color=background_color,
            colormap=colormap,
            max_words=max_words,
            collocations=False,
        ).generate(combined)
        fig, ax = _seaborn()[0].subplots(figsize=(width / 100, height / 100))
        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100, bbox_inches="tight", pad_inches=0)
        _seaborn()[0].close(fig)
        return buf.getvalue()

    # ── Prophet Forecasting ────────────────────────────────────

    def prophet_forecast(
        self,
        records: list[dict],
        date_field: str,
        value_field: str,
        periods: int = 30,
        freq: str = "W",
    ) -> dict:
        """
        Run Prophet forecast on a time-series.
        Returns dict with historical + forecast + confidence interval,
        recharts-compatible.
        """
        Prophet = _prophet()
        df = pd.DataFrame(records)[[date_field, value_field]].dropna()
        df.columns = ["ds", "y"]
        df["ds"] = pd.to_datetime(df["ds"])
        df["y"] = pd.to_numeric(df["y"], errors="coerce")
        df = df.dropna()

        if len(df) < 4:
            return {"error": "Insufficient data for forecast (need ≥ 4 points)"}

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=(freq in ("D", "W")),
            daily_seasonality=False,
            interval_width=0.80,
        )
        model.fit(df)

        future = model.make_future_dataframe(periods=periods, freq=freq)
        forecast = model.predict(future)

        result_rows = []
        hist_set = set(df["ds"].astype(str))
        for _, row in forecast.iterrows():
            ds = row["ds"].strftime("%Y-%m-%d")
            result_rows.append({
                "date": ds,
                "actual": round(float(df.loc[df["ds"] == row["ds"], "y"].values[0]), 3)
                          if ds in hist_set else None,
                "forecast": round(float(row["yhat"]), 3),
                "lower": round(float(row["yhat_lower"]), 3),
                "upper": round(float(row["yhat_upper"]), 3),
                "is_forecast": ds not in hist_set,
            })
        return {"field": value_field, "freq": freq, "data": result_rows}

    # ── SHAP Explainability ────────────────────────────────────

    def shap_feature_importance(
        self,
        model: Any,
        df: pd.DataFrame,
        max_features: int = 15,
    ) -> list[dict]:
        """
        Compute SHAP values and return feature importance list for recharts.
        Returns [{"feature": str, "importance": float, "direction": "positive"|"negative"}]
        """
        shap = _shap()
        try:
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(df)
            if isinstance(shap_values, list):
                shap_values = shap_values[1]  # binary: take positive class
            mean_abs = np.abs(shap_values).mean(axis=0)
            mean_dir = shap_values.mean(axis=0)
            feat_imp = sorted(
                [
                    {
                        "feature": col.replace("_", " ").title(),
                        "importance": round(float(mean_abs[i]), 4),
                        "direction": "positive" if mean_dir[i] > 0 else "negative",
                    }
                    for i, col in enumerate(df.columns)
                ],
                key=lambda x: x["importance"],
                reverse=True,
            )
            return feat_imp[:max_features]
        except Exception as exc:
            logger.warning("SHAP failed: %s", exc)
            return []


# ── Singleton ─────────────────────────────────────────────────

_chart_service: ChartService | None = None

def get_chart_service() -> ChartService:
    global _chart_service
    if _chart_service is None:
        _chart_service = ChartService()
    return _chart_service
