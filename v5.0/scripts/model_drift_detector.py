#!/usr/bin/env python3
"""
Model Drift Detector — scripts/model_drift_detector.py

Compares rolling 7-day prediction accuracy vs the seasonal baseline for each sport.
Flags any sport where accuracy has dropped more than DRIFT_THRESHOLD percentage points.

Designed to run daily via cron. Output: JSON report + exit code 1 if drift detected.

Usage:
    python3 scripts/model_drift_detector.py [--days 7] [--threshold 0.02] [--report-path ./drift_report.json]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ─── Path setup ─────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
PREDICTIONS_DIR = DATA_DIR / "predictions"
NORMALIZED_DB = DATA_DIR / "normalized.duckdb"

sys.path.insert(0, str(REPO_ROOT / "backend"))

# ─── Constants ───────────────────────────────────────────────────────────────
DRIFT_THRESHOLD = 0.02          # 2% point drop triggers alert
MIN_SAMPLE_SIZE = 20            # Ignore windows with fewer predictions
BASELINE_MIN_DAYS = 30          # Days of history required for baseline

# Sports to check — must have enough prediction history
TARGET_SPORTS = [
    "nfl", "nba", "mlb", "nhl", "ncaab", "ncaaf", "wnba",
    "epl", "laliga", "bundesliga", "ligue1", "seriea", "mls",
    "ufc", "atp", "wta",
]


def get_conn():
    import duckdb
    return duckdb.connect(str(NORMALIZED_DB), read_only=True)


def load_prediction_accuracy(sport: str, days_back: int, conn) -> dict | None:
    """
    Load recent prediction outcomes from normalized DB.
    Returns {accuracy, n_correct, n_total, period_start, period_end} or None.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Try prediction_outcomes table first, fall back to predictions with result
    queries = [
        f"""
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN predicted_correct = true THEN 1 ELSE 0 END) AS n_correct
        FROM prediction_outcomes
        WHERE sport = '{sport}'
          AND game_date >= '{cutoff}'
          AND game_date <= '{today}'
        """,
        f"""
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN outcome_correct = 1 THEN 1 ELSE 0 END) AS n_correct
        FROM predictions
        WHERE sport = '{sport}'
          AND date >= '{cutoff}'
          AND date <= '{today}'
          AND outcome_correct IS NOT NULL
        """,
    ]

    for query in queries:
        try:
            result = conn.execute(query).fetchone()
            if result and result[0] and result[0] >= MIN_SAMPLE_SIZE:
                n_total, n_correct = int(result[0]), int(result[1] or 0)
                return {
                    "accuracy": n_correct / n_total,
                    "n_correct": n_correct,
                    "n_total": n_total,
                    "period_start": cutoff,
                    "period_end": today,
                }
        except Exception:
            continue

    return None


def load_baseline_accuracy(sport: str, days_back: int, conn) -> dict | None:
    """
    Load season-long baseline accuracy (90-day window, shifted before the recent window).
    """
    recent_cutoff = datetime.utcnow() - timedelta(days=days_back)
    baseline_cutoff = (recent_cutoff - timedelta(days=BASELINE_MIN_DAYS)).strftime("%Y-%m-%d")
    baseline_end = recent_cutoff.strftime("%Y-%m-%d")

    queries = [
        f"""
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN predicted_correct = true THEN 1 ELSE 0 END) AS n_correct
        FROM prediction_outcomes
        WHERE sport = '{sport}'
          AND game_date >= '{baseline_cutoff}'
          AND game_date < '{baseline_end}'
        """,
        f"""
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN outcome_correct = 1 THEN 1 ELSE 0 END) AS n_correct
        FROM predictions
        WHERE sport = '{sport}'
          AND date >= '{baseline_cutoff}'
          AND date < '{baseline_end}'
          AND outcome_correct IS NOT NULL
        """,
    ]

    for query in queries:
        try:
            result = conn.execute(query).fetchone()
            if result and result[0] and result[0] >= MIN_SAMPLE_SIZE:
                n_total, n_correct = int(result[0]), int(result[1] or 0)
                return {
                    "accuracy": n_correct / n_total,
                    "n_correct": n_correct,
                    "n_total": n_total,
                    "period_start": baseline_cutoff,
                    "period_end": baseline_end,
                }
        except Exception:
            continue

    # Fall back to parquet files if DB not accessible
    return load_baseline_from_parquet(sport)


def load_baseline_from_parquet(sport: str) -> dict | None:
    """Fall back to reading parquet prediction history."""
    try:
        import pandas as pd

        files = list(PREDICTIONS_DIR.glob(f"{sport}_*.parquet"))
        if not files:
            return None

        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception:
                pass

        if not dfs:
            return None

        df = pd.concat(dfs, ignore_index=True)

        # Find outcome column
        outcome_col = None
        for c in ["outcome_correct", "predicted_correct", "correct"]:
            if c in df.columns:
                outcome_col = c
                break

        if not outcome_col:
            return None

        df = df.dropna(subset=[outcome_col])
        if len(df) < MIN_SAMPLE_SIZE:
            return None

        n_total = len(df)
        n_correct = int(df[outcome_col].astype(float).sum())
        return {
            "accuracy": n_correct / n_total,
            "n_correct": n_correct,
            "n_total": n_total,
            "period_start": "historical",
            "period_end": "baseline",
        }
    except Exception:
        return None


def check_sport_drift(sport: str, days: int, threshold: float, conn) -> dict:
    """Check if a sport's recent accuracy has drifted from baseline."""
    recent = load_prediction_accuracy(sport, days, conn)
    if not recent:
        return {"sport": sport, "status": "skip", "reason": f"insufficient_recent_data"}

    baseline = load_baseline_accuracy(sport, days, conn)
    if not baseline:
        return {"sport": sport, "status": "skip", "reason": "insufficient_baseline_data"}

    drift = baseline["accuracy"] - recent["accuracy"]
    drifted = drift > threshold

    return {
        "sport": sport,
        "status": "drift" if drifted else "ok",
        "recent_accuracy": round(recent["accuracy"], 4),
        "baseline_accuracy": round(baseline["accuracy"], 4),
        "drift": round(drift, 4),
        "threshold": threshold,
        "recent_n": recent["n_total"],
        "baseline_n": baseline["n_total"],
        "recent_period": f"{recent['period_start']} → {recent['period_end']}",
        "baseline_period": f"{baseline['period_start']} → {baseline['period_end']}",
    }


def main():
    parser = argparse.ArgumentParser(description="Model drift detector")
    parser.add_argument("--days", type=int, default=7, help="Recent window in days")
    parser.add_argument("--threshold", type=float, default=DRIFT_THRESHOLD, help="Drift alert threshold (0.02 = 2%)")
    parser.add_argument("--report-path", type=str, default=None, help="Save JSON report to file")
    parser.add_argument("--sports", nargs="*", default=None, help="Specific sports to check")
    args = parser.parse_args()

    sports = args.sports or TARGET_SPORTS
    results = []
    drifted_sports = []

    try:
        conn = get_conn()
    except Exception as e:
        print(f"[drift] WARNING: Cannot connect to DuckDB: {e}. Using parquet fallback.")
        conn = None

    for sport in sports:
        try:
            if conn:
                result = check_sport_drift(sport, args.days, args.threshold, conn)
            else:
                baseline = load_baseline_from_parquet(sport)
                if baseline:
                    result = {
                        "sport": sport, "status": "skip",
                        "reason": "db_unavailable_parquet_only",
                        "baseline_accuracy": round(baseline["accuracy"], 4),
                    }
                else:
                    result = {"sport": sport, "status": "skip", "reason": "no_data"}
        except Exception as e:
            result = {"sport": sport, "status": "error", "error": str(e)}

        results.append(result)

        if result.get("status") == "drift":
            drifted_sports.append(sport)
            print(
                f"[DRIFT] {sport.upper()}: "
                f"recent={result['recent_accuracy']:.1%} vs "
                f"baseline={result['baseline_accuracy']:.1%} "
                f"(Δ={result['drift']:+.1%})"
            )
        elif result.get("status") == "ok":
            print(
                f"[OK]    {sport.upper()}: "
                f"recent={result['recent_accuracy']:.1%} vs "
                f"baseline={result['baseline_accuracy']:.1%}"
            )
        elif result.get("status") == "skip":
            print(f"[SKIP]  {sport.upper()}: {result.get('reason')}")

    # Build report
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "window_days": args.days,
        "threshold": args.threshold,
        "drifted_sports": drifted_sports,
        "total_checked": len([r for r in results if r["status"] != "skip"]),
        "total_drifted": len(drifted_sports),
        "results": results,
    }

    # Save report if requested
    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\n[drift] Report saved to {report_path}")
    else:
        default_path = REPORTS_DIR / "drift_report.json"
        default_path.parent.mkdir(parents=True, exist_ok=True)
        with open(default_path, "w") as f:
            json.dump(report, f, indent=2)

    # Summary
    print(f"\n{'='*50}")
    print(f"Drift check complete: {len(drifted_sports)}/{len(sports)} sports flagged")
    if drifted_sports:
        print(f"DRIFTED: {', '.join(drifted_sports)}")
        print("ACTION: Consider retraining these models.")

    if conn:
        conn.close()

    # Exit 1 if any drift detected (useful for cron alerting)
    sys.exit(1 if drifted_sports else 0)


if __name__ == "__main__":
    main()
