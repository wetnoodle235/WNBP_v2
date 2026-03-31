#!/usr/bin/env python3
"""Analyze weak-performing sports for feature quality issues.

Reads the latest model performance diagnostic report, identifies weak sports,
and inspects corresponding feature parquet files for common ML quality issues:
- High missingness
- Near-constant features
- Potential leakage candidates in feature space
- Class balance and sample-size risk
"""

from __future__ import annotations

import argparse
import json
import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class SportFeatureFinding:
    sport: str
    rows: int
    cols: int
    high_missing_columns: list[dict[str, Any]]
    near_constant_columns: list[str]
    leakage_like_columns: list[str]
    target_balance: dict[str, Any]
    severity: str
    recommendations: list[str]
    remediation_hints: list[str]
    top_fix_candidates: list[dict[str, Any]]


# Sport-family thresholds to reduce one-size-fits-all noise.
_LARGE_LEAGUES = {"nba", "nfl", "mlb", "nhl", "ncaab", "ncaaf", "wnba"}
_LOW_VOLUME = {"ucl", "nwsl", "ufc", "f1", "golf"}

_SPORT_HINTS: dict[str, list[str]] = {
    "csgo": [
        "Prioritize round-economy and objective features (plants, defuses, save rounds).",
        "Audit LoL-style objective features and remove non-CSGO signals.",
    ],
    "dota2": [
        "Improve game/player ID reconciliation before feature extraction.",
        "Validate team-pair/date joins for player_stats to reduce null propagation.",
    ],
    "lol": [
        "Backfill objective timelines (towers, dragons, herald/baron) for recent matches.",
        "Verify side-based splits (blue/red) are populated in recent seasons.",
    ],
    "mlb": [
        "Ensure rolling player form windows are refreshed daily after consolidation.",
        "Check pitcher/bullpen availability coverage before inference.",
    ],
    "nwsl": [
        "NWSL xG/xGA data is sparse — use simple shot/possession proxies until xG is backfilled.",
        "Verify source coverage: statsbomb NWSL export often lags 2–4 weeks.",
        "Drop leakage-like columns (result/outcome suffix) before any training run.",
    ],
    "ucl": [
        "UCL xG features are sparsely covered outside knockout stages — consider season-level averages.",
        "Backfill group-stage xG from fbref or statsbomb before the next training cycle.",
        "Drop leakage-like columns (result/outcome suffix) before any training run.",
    ],
    "atp": [
        "ATP set-count features have 75%+ missingness — impute with tour-wide median or drop.",
        "Verify tennisabstract import cadence for recent seasons.",
    ],
}


def _feature_family(column: str) -> str:
    c = column.lower()
    if any(tok in c for tok in ("odds", "moneyline", "spread", "total", "implied")):
        return "odds_market"
    if any(tok in c for tok in ("injury", "lineup", "depth", "availability")):
        return "roster_availability"
    if any(tok in c for tok in ("xg", "xga", "expected_goal", "shot_quality")):
        return "advanced_attack"
    if any(tok in c for tok in ("form", "momentum", "streak", "rolling", "recent", "overperformance")):
        return "player_team_form"
    if any(tok in c for tok in ("h2h", "surface", "rest", "fatigue", "travel", "opponent")):
        return "matchup_context"
    if any(tok in c for tok in ("q1", "q2", "q3", "q4", "period", "half", "inning", "set", "map")):
        return "event_split"
    if any(tok in c for tok in ("elo", "rate", "pct", "eff", "per_")):
        return "efficiency_rate"
    return "general"


def _build_top_fix_candidates(
    sport: str,
    high_missing_columns: list[dict[str, Any]],
    near_constant_columns: list[str],
) -> list[dict[str, Any]]:
    """Generate top fix candidates scored by impact likelihood."""
    candidates: dict[str, dict[str, Any]] = {}

    for item in high_missing_columns:
        col = str(item.get("column", ""))
        if not col:
            continue
        ratio = float(item.get("missing_ratio", 0.0))
        fam = _feature_family(col)
        score = round(100.0 * ratio, 2)
        candidates[col] = {
            "column": col,
            "family": fam,
            "issue": "high_missingness",
            "score": score,
            "recommended_action": "Backfill source data or apply explicit imputation before training.",
        }

    for col in near_constant_columns:
        fam = _feature_family(col)
        existing = candidates.get(col)
        nc_score = 55.0
        if existing:
            # Column is both sparse and near-constant: bump impact.
            existing["issue"] = "high_missingness_and_near_constant"
            existing["score"] = round(float(existing["score"]) + nc_score, 2)
            existing["recommended_action"] = "Drop or redesign this feature; current signal is low-information."
        else:
            candidates[col] = {
                "column": col,
                "family": fam,
                "issue": "near_constant",
                "score": nc_score,
                "recommended_action": "Drop or transform to a more discriminative feature.",
            }

    ranked = sorted(candidates.values(), key=lambda x: float(x["score"]), reverse=True)
    return ranked[:5]


def _build_remediation_hints(sport: str, top_fix_candidates: list[dict[str, Any]]) -> list[str]:
    hints: list[str] = []
    hints.extend(_SPORT_HINTS.get(sport.lower(), []))

    families = [str(c.get("family", "general")) for c in top_fix_candidates]
    if "odds_market" in families:
        hints.append("Stabilize odds ingestion cadence and align odds timestamps to game snapshots.")
    if "advanced_attack" in families:
        hints.append("Backfill xG/xGA from fbref or StatsBomb; replace nulls with league-average before retraining.")
    if "event_split" in families:
        hints.append("Exclude empty period/quarter split columns or rebuild split extraction logic.")
    if "player_team_form" in families:
        hints.append("Increase rolling window robustness for early-season games with sparse history.")
    if "matchup_context" in families:
        hints.append("Recompute matchup/context features after every normalize+consolidate cycle.")
    if "efficiency_rate" in families:
        hints.append("Verify pace/efficiency normalizers run post-consolidation; missing values cascade from upstream gaps.")
    if "roster_availability" in families:
        hints.append("Check injury-report importer cadence; roster availability columns must be non-null by game time.")

    if not hints:
        hints.append("Prioritize top fix candidates and retrain with feature-importance review.")
    return hints[:5]


def _thresholds_for_sport(sport: str) -> dict[str, float]:
    s = sport.lower()
    if s in _LARGE_LEAGUES:
        return {
            "missing_ratio_warn": 0.50,
            "near_constant_mode": 0.98,
            "near_constant_count_warn": 12,
            "min_rows_warn": 1000,
            "imbalance_lo": 0.40,
            "imbalance_hi": 0.60,
        }
    if s in _LOW_VOLUME:
        return {
            "missing_ratio_warn": 0.60,
            "near_constant_mode": 0.99,
            "near_constant_count_warn": 20,
            "min_rows_warn": 400,
            "imbalance_lo": 0.35,
            "imbalance_hi": 0.65,
        }
    return {
        "missing_ratio_warn": 0.55,
        "near_constant_mode": 0.985,
        "near_constant_count_warn": 15,
        "min_rows_warn": 600,
        "imbalance_lo": 0.37,
        "imbalance_hi": 0.63,
    }


def _latest_perf_report(reports_dir: Path) -> Path | None:
    candidates = sorted(reports_dir.glob("model_perf_diagnostic_*.json"))
    return candidates[-1] if candidates else None


def _load_weak_sports(report_path: Path, max_sports: int) -> list[str]:
    data = json.loads(report_path.read_text())
    metrics = data.get("metrics", [])
    weak = [m for m in metrics if bool(m.get("is_weak"))]
    weak.sort(key=lambda m: int(m.get("priority", 0)), reverse=True)
    return [str(m.get("sport", "")).lower() for m in weak[:max_sports] if m.get("sport")]


def _target_balance(df: pd.DataFrame, sport: str) -> dict[str, Any]:
    if "home_score" not in df.columns or "away_score" not in df.columns:
        return {"available": False}

    hs = pd.to_numeric(df["home_score"], errors="coerce")
    aw = pd.to_numeric(df["away_score"], errors="coerce")
    valid = hs.notna() & aw.notna()
    if int(valid.sum()) == 0:
        return {"available": False}

    home_win = (hs[valid] > aw[valid]).astype(int)
    pct = float(home_win.mean())
    t = _thresholds_for_sport(sport)
    return {
        "available": True,
        "samples": int(valid.sum()),
        "home_win_rate": round(pct, 4),
        "is_imbalanced": bool(pct < t["imbalance_lo"] or pct > t["imbalance_hi"]),
    }


def _analyze_sport(sport: str, features_dir: Path) -> SportFeatureFinding | None:
    path = features_dir / f"{sport}_all.parquet"
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    rows, cols = len(df), len(df.columns)
    t = _thresholds_for_sport(sport)

    excluded_pattern_tokens = (
        "game_id",
        "date",
        "team_id",
        "season",
        "home_score",
        "away_score",
    )
    quality_cols = [
        c for c in df.columns
        if not any(tok in c.lower() for tok in excluded_pattern_tokens)
    ]
    if rows == 0:
        return SportFeatureFinding(
            sport=sport,
            rows=0,
            cols=cols,
            high_missing_columns=[],
            near_constant_columns=[],
            leakage_like_columns=[],
            target_balance={"available": False},
            severity="critical",
            recommendations=["Feature file is empty; verify extraction and consolidation steps."],
            remediation_hints=["Re-run extraction for this sport and verify source parquet availability."],
            top_fix_candidates=[],
        )

    missing_ratio = (df[quality_cols].isna().sum() / rows).sort_values(ascending=False)
    high_missing = [
        {"column": c, "missing_ratio": round(float(v), 4)}
        for c, v in missing_ratio.items()
        if float(v) >= t["missing_ratio_warn"]
    ][:20]

    numeric_cols = df[quality_cols].select_dtypes(include=["number", "bool"]).columns
    near_constant = []
    for c in numeric_cols:
        series = pd.to_numeric(df[c], errors="coerce")
        non_na = series.dropna()
        if len(non_na) < 10:
            continue
        # Treat columns with <= 2 unique values and dominant mode as near-constant.
        nunique = int(non_na.nunique())
        if nunique <= 2:
            top_freq = float(non_na.value_counts(normalize=True, dropna=True).iloc[0])
            if top_freq >= t["near_constant_mode"]:
                near_constant.append(c)

    suspicious_tokens = ("result", "outcome", "label", "target", "winner")
    leakage_like = [c for c in df.columns if any(tok in c.lower() for tok in suspicious_tokens)]
    leakage_like = [
        c for c in leakage_like
        if c not in {"home_score", "away_score"}
        and not c.lower().endswith("_win_pct")
        and not c.lower().endswith("_won_pct")
    ]

    balance = _target_balance(df, sport)

    recs: list[str] = []
    severity = "info"
    if high_missing:
        severity = "warning"
        recs.append(
            f"Drop or impute columns with >={int(t['missing_ratio_warn'] * 100)}% missingness before training."
        )
    if len(near_constant) >= int(t["near_constant_count_warn"]):
        severity = "warning"
        recs.append("Prune near-constant features to reduce noise and overfitting risk.")
    if leakage_like:
        severity = "critical"
        recs.append("Review leakage-like columns and exclude any post-outcome features.")
    if balance.get("is_imbalanced"):
        severity = "warning" if severity != "critical" else severity
        recs.append("Apply class weighting or stratified validation due to target imbalance.")
    if rows < int(t["min_rows_warn"]):
        severity = "warning" if severity != "critical" else severity
        recs.append("Sample size is small; increase historical window or reduce model complexity.")

    if not recs:
        recs.append("No major feature-quality issues detected.")

    top_fix_candidates = _build_top_fix_candidates(sport, high_missing, near_constant[:60])
    remediation_hints = _build_remediation_hints(sport, top_fix_candidates)

    return SportFeatureFinding(
        sport=sport,
        rows=rows,
        cols=cols,
        high_missing_columns=high_missing,
        near_constant_columns=near_constant[:30],
        leakage_like_columns=leakage_like[:30],
        target_balance=balance,
        severity=severity,
        recommendations=recs,
        remediation_hints=remediation_hints,
        top_fix_candidates=top_fix_candidates,
    )


def _write_compact_csv(findings: list[SportFeatureFinding], out_path: Path) -> None:
    """Write a compact row-per-sport summary for easy trend tracking in CI artifacts."""
    with open(out_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "date",
            "sport",
            "severity",
            "rows",
            "cols",
            "high_missing_count",
            "near_constant_count",
            "leakage_like_count",
            "target_balance_available",
            "target_home_win_rate",
            "target_imbalanced",
            "top_missing_column",
            "top_missing_ratio",
            "recommendation_1",
            "top_fix_1",
        ])
        today = date.today().isoformat()
        for f in findings:
            top_missing_col = ""
            top_missing_ratio = ""
            if f.high_missing_columns:
                top_missing_col = str(f.high_missing_columns[0].get("column", ""))
                top_missing_ratio = str(f.high_missing_columns[0].get("missing_ratio", ""))
            writer.writerow([
                today,
                f.sport,
                f.severity,
                f.rows,
                f.cols,
                len(f.high_missing_columns),
                len(f.near_constant_columns),
                len(f.leakage_like_columns),
                bool(f.target_balance.get("available", False)),
                f.target_balance.get("home_win_rate", ""),
                bool(f.target_balance.get("is_imbalanced", False)),
                top_missing_col,
                top_missing_ratio,
                f.recommendations[0] if f.recommendations else "",
                (f.top_fix_candidates[0]["column"] if f.top_fix_candidates else ""),
            ])


def _write_history_snapshots(payload: dict[str, Any], findings: list[SportFeatureFinding], reports_dir: Path) -> tuple[Path, Path]:
    history_dir = reports_dir / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().astimezone().strftime("%Y-%m-%dT%H%M%S%f%z")

    json_path = history_dir / f"weak_sports_feature_analysis_{stamp}.json"
    csv_path = history_dir / f"weak_sports_feature_analysis_{stamp}.csv"
    json_path.write_text(json.dumps(payload, indent=2))
    _write_compact_csv(findings, csv_path)
    return json_path, csv_path


def _prune_history(reports_dir: Path, keep: int) -> int:
    """Delete oldest history snapshots beyond *keep* most recent pairs.

    JSON and CSV files share the same timestamp stem; both are removed together.
    Returns the number of pairs removed.
    """
    history_dir = reports_dir / "history"
    if not history_dir.exists():
        return 0

    json_files = sorted(history_dir.glob("weak_sports_feature_analysis_*.json"))
    excess = len(json_files) - keep
    removed = 0
    for json_path in json_files[:excess]:
        stem = json_path.stem  # e.g. weak_sports_feature_analysis_2026-03-30T...
        json_path.unlink(missing_ok=True)
        csv_path = json_path.with_suffix(".csv")
        csv_path.unlink(missing_ok=True)
        removed += 1
    return removed


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze weak-performing sports feature quality")
    parser.add_argument("--data-dir", default="data", help="Path to data directory")
    parser.add_argument("--max-sports", type=int, default=8, help="Max weak sports to analyze")
    parser.add_argument("--sports", default="", help="Comma-separated sports override")
    parser.add_argument(
        "--max-critical",
        type=int,
        default=-1,
        help="Fail if critical findings exceed this count (disabled when < 0)",
    )
    parser.add_argument(
        "--keep-history",
        type=int,
        default=50,
        metavar="N",
        help="Keep the last N history snapshots; prune the rest (default: 50)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    reports_dir = data_dir / "reports"
    features_dir = data_dir / "features"
    reports_dir.mkdir(parents=True, exist_ok=True)

    if args.sports.strip():
        sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()]
    else:
        report_path = _latest_perf_report(reports_dir)
        if report_path is None:
            print("No model_perf_diagnostic report found; run model_performance_diagnostic.py first.")
            return 1
        sports = _load_weak_sports(report_path, args.max_sports)

    if not sports:
        print("No weak sports identified; nothing to analyze.")
        return 0

    findings: list[SportFeatureFinding] = []
    for sport in sports:
        finding = _analyze_sport(sport, features_dir)
        if finding is None:
            findings.append(
                SportFeatureFinding(
                    sport=sport,
                    rows=0,
                    cols=0,
                    high_missing_columns=[],
                    near_constant_columns=[],
                    leakage_like_columns=[],
                    target_balance={"available": False},
                    severity="critical",
                    recommendations=[f"Missing {sport}_all.parquet; run feature extraction and consolidation."],
                    remediation_hints=["Generate the missing _all.parquet file before retraining."],
                    top_fix_candidates=[],
                )
            )
        else:
            findings.append(finding)

    critical_count = sum(1 for f in findings if f.severity == "critical")
    generated_at = datetime.now().astimezone().isoformat(timespec="microseconds")
    out = {
        "date": date.today().isoformat(),
        "generated_at": generated_at,
        "sports_analyzed": [f.sport for f in findings],
        "critical_count": critical_count,
        "warning_count": sum(1 for f in findings if f.severity == "warning"),
        "findings": [
            {
                "sport": f.sport,
                "rows": f.rows,
                "cols": f.cols,
                "severity": f.severity,
                "high_missing_columns": f.high_missing_columns,
                "near_constant_columns": f.near_constant_columns,
                "leakage_like_columns": f.leakage_like_columns,
                "target_balance": f.target_balance,
                "recommendations": f.recommendations,
                "remediation_hints": f.remediation_hints,
                "top_fix_candidates": f.top_fix_candidates,
            }
            for f in findings
        ],
    }

    out_path = reports_dir / f"weak_sports_feature_analysis_{date.today().isoformat()}.json"
    out_path.write_text(json.dumps(out, indent=2))

    csv_path = reports_dir / f"weak_sports_feature_analysis_{date.today().isoformat()}.csv"
    _write_compact_csv(findings, csv_path)
    history_json_path, history_csv_path = _write_history_snapshots(out, findings, reports_dir)
    pruned = _prune_history(reports_dir, keep=args.keep_history)

    print("Weak Sports Feature Analysis")
    print("=" * 40)
    for f in findings:
        print(f"- {f.sport.upper():10} severity={f.severity:8} rows={f.rows:6} cols={f.cols:4}")
    print(f"\nReport saved: {out_path}")
    print(f"CSV saved: {csv_path}")
    print(f"History snapshot saved: {history_json_path}")
    print(f"History CSV saved: {history_csv_path}")
    if pruned:
        print(f"Pruned {pruned} old history snapshot(s) (keep={args.keep_history})")

    if args.max_critical >= 0 and critical_count > args.max_critical:
        print(
            f"FAIL: critical findings {critical_count} exceed threshold {args.max_critical}",
            flush=True,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())