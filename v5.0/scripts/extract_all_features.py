#!/usr/bin/env python3
"""
Multi-season feature extraction for all sports.

Builds comprehensive feature parquets combining ALL available seasons
into {sport}_all.parquet for maximum model training data.

Usage:
    python3 scripts/extract_all_features.py                  # All sports
    python3 scripts/extract_all_features.py --sport nba      # Single sport
    python3 scripts/extract_all_features.py --parallel 4     # 4 workers
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
DATA_DIR = PROJECT_ROOT / "data"

sys.path.insert(0, str(BACKEND_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("extract_all")


def _discover_seasons(sport: str) -> list[int]:
    sport_dir = DATA_DIR / "normalized" / sport
    seasons = set()
    for p in sport_dir.glob("games_*.parquet"):
        try:
            seasons.add(int(p.stem.split("_")[-1]))
        except ValueError:
            pass
    return sorted(seasons)


def _extract_sport(sport: str, timeout: int = 900) -> dict:
    """Extract ALL available seasons for a sport, save combined parquet."""
    t0 = time.monotonic()
    seasons = _discover_seasons(sport)
    if not seasons:
        return {"sport": sport, "status": "skip", "reason": "no game data"}

    seasons_str = ",".join(str(s) for s in seasons)
    output_path = DATA_DIR / "features" / f"{sport}_all.parquet"

    proc = subprocess.run(
        [
            sys.executable, "-m", "ml.feature_extraction",
            "--sport", sport,
            "--seasons", seasons_str,
            "--output", str(output_path),
        ],
        cwd=str(BACKEND_DIR),
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    elapsed = round(time.monotonic() - t0, 1)
    if proc.returncode != 0:
        err = proc.stderr.strip()[-500:] if proc.stderr else "unknown"
        return {"sport": sport, "status": "error", "error": err, "time": elapsed}

    # Parse output for row count
    import pandas as pd
    try:
        df = pd.read_parquet(output_path)
        rows, cols = len(df), len(df.columns)
    except Exception:
        rows, cols = 0, 0

    return {
        "sport": sport,
        "status": "ok",
        "seasons": seasons,
        "rows": rows,
        "features": cols,
        "time": elapsed,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", help="Single sport to extract")
    parser.add_argument("--parallel", type=int, default=1, help="Worker processes")
    parser.add_argument("--timeout", type=int, default=900, help="Per-sport timeout (s)")
    args = parser.parse_args()

    from features.registry import EXTRACTORS

    sports = [args.sport] if args.sport else sorted(EXTRACTORS.keys())
    (DATA_DIR / "features").mkdir(parents=True, exist_ok=True)

    logger.info("Extracting features for %d sports (parallel=%d)", len(sports), args.parallel)

    results = []
    total_t0 = time.monotonic()

    if args.parallel <= 1:
        for sport in sports:
            logger.info("─── %s ───", sport.upper())
            r = _extract_sport(sport, args.timeout)
            results.append(r)
            if r["status"] == "ok":
                logger.info("  ✓ %s: %d rows × %d cols (%d seasons, %.0fs)",
                            sport, r["rows"], r["features"], len(r["seasons"]), r["time"])
            else:
                logger.warning("  ✗ %s: %s", sport, r.get("error", r.get("reason", "?")))
    else:
        with ProcessPoolExecutor(max_workers=args.parallel) as pool:
            futures = {pool.submit(_extract_sport, s, args.timeout): s for s in sports}
            for fut in as_completed(futures):
                r = fut.result()
                results.append(r)
                sport = r["sport"]
                if r["status"] == "ok":
                    logger.info("  ✓ %s: %d rows × %d cols (%.0fs)",
                                sport, r["rows"], r["features"], r["time"])
                else:
                    logger.warning("  ✗ %s: %s", sport, r.get("error", r.get("reason", "?")))

    total = round(time.monotonic() - total_t0, 1)
    ok = sum(1 for r in results if r["status"] == "ok")
    total_rows = sum(r.get("rows", 0) for r in results)

    print(f"\n{'═'*60}")
    print(f"  Multi-Season Feature Extraction Complete")
    print(f"{'═'*60}")
    print(f"  Sports: {ok}/{len(sports)} succeeded")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Time: {total:.0f}s")
    print(f"{'═'*60}")

    for r in sorted(results, key=lambda x: x.get("rows", 0), reverse=True):
        if r["status"] == "ok":
            print(f"  {r['sport']:12s}  {r['rows']:>7,} rows  {r['features']:>3} cols  "
                  f"{len(r['seasons'])} seasons  {r['time']:.0f}s")
        else:
            print(f"  {r['sport']:12s}  FAILED: {r.get('error', r.get('reason', '?'))[:60]}")


if __name__ == "__main__":
    main()
