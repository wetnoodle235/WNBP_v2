# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Feature Extraction CLI
# ──────────────────────────────────────────────────────────
"""
CLI entry-point for running enhanced feature extraction across
all available normalized data types (games, standings, injuries,
odds, player_stats, etc.).

Usage
-----
::

    # Extract features for a single sport (all available seasons)
    python -m ml.feature_extraction --sport nba

    # Extract for specific seasons
    python -m ml.feature_extraction --sport nba --seasons 2024,2025

    # Verbose mode — print feature importance rankings
    python -m ml.feature_extraction --sport nba --verbose

    # Save output to a file
    python -m ml.feature_extraction --sport nba --output features.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from features.registry import EXTRACTORS, get_extractor

logger = logging.getLogger(__name__)


# ── Metadata columns (not used as features) ─────────────

_META_COLS = {
    "game_id",
    "date",
    "home_team_id",
    "away_team_id",
    "home_score",
    "away_score",
}


def _discover_seasons(data_dir: Path, sport: str) -> list[int]:
    """Auto-discover available seasons from curated games partitions.

    Falls back to legacy normalized flat files when curated partitions are not
    present yet.
    """
    curated_games_dir = data_dir / "normalized_curated" / sport / "games"
    seasons: set[int] = set()
    if curated_games_dir.exists():
        for season_dir in curated_games_dir.glob("season=*"):
            token = season_dir.name.replace("season=", "")
            if token.isdigit():
                seasons.add(int(token))
    if seasons:
        return sorted(seasons)

    sport_dir = data_dir / "normalized" / sport
    if not sport_dir.exists():
        return []

    for path in sport_dir.glob("games_*.parquet"):
        stem = path.stem  # e.g. "games_2024"
        parts = stem.split("_")
        if len(parts) >= 2:
            try:
                seasons.add(int(parts[-1]))
            except ValueError:
                pass
    return sorted(seasons)


def _data_inventory(data_dir: Path, sport: str) -> dict[str, list[int]]:
    """Report which data types and seasons are available.

    Primary inventory source is curated layout. Legacy normalized is used only
    as a fallback for any data type not found in curated.
    """
    curated_sport_dir = data_dir / "normalized_curated" / sport
    sport_dir = data_dir / "normalized" / sport
    if not curated_sport_dir.exists() and not sport_dir.exists():
        return {}

    data_types = [
        "games", "standings", "injuries", "odds",
        "player_stats", "players", "teams", "news", "predictions",
    ]
    inventory: dict[str, list[int]] = {}
    for dtype in data_types:
        seasons = set()
        curated_dtype_dir = curated_sport_dir / dtype
        if curated_dtype_dir.exists():
            for season_dir in curated_dtype_dir.glob("season=*"):
                token = season_dir.name.replace("season=", "")
                if token.isdigit():
                    seasons.add(int(token))
                elif token == "all":
                    seasons.add(0)

        # Legacy fallback for data types not found in curated yet.
        if not seasons and sport_dir.exists():
            for path in sport_dir.glob(f"{dtype}_*.parquet"):
                parts = path.stem.split("_")
                try:
                    seasons.add(int(parts[-1]))
                except ValueError:
                    pass
            if (sport_dir / f"{dtype}.parquet").exists():
                seasons.add(0)  # 0 signals "unseasoned"

        if seasons:
            inventory[dtype] = sorted(s for s in seasons if s > 0)
    return inventory


def _feature_importance_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Rank features by variance and correlation with outcome.

    Returns a DataFrame with feature_name, variance, and
    correlation with home_win (if scores are available).
    """
    feature_cols = [c for c in df.columns if c not in _META_COLS and c != "season"]
    numeric = df[feature_cols].select_dtypes(include=[np.number])

    if numeric.empty:
        return pd.DataFrame(columns=["feature", "variance", "nonzero_pct", "correlation"])

    variance = numeric.var().fillna(0)
    nonzero_pct = (numeric != 0).mean()

    # Correlation with home_win if we have scores
    correlations = pd.Series(0.0, index=numeric.columns)
    if "home_score" in df.columns and "away_score" in df.columns:
        hs = pd.to_numeric(df["home_score"], errors="coerce")
        aws = pd.to_numeric(df["away_score"], errors="coerce")
        valid = hs.notna() & aws.notna()
        if valid.sum() > 10:
            home_win = (hs[valid] > aws[valid]).astype(float)
            for col in numeric.columns:
                vals = numeric.loc[valid, col]
                if vals.std() > 0:
                    correlations[col] = abs(float(vals.corr(home_win)))

    ranking = pd.DataFrame({
        "feature": numeric.columns,
        "variance": variance.values,
        "nonzero_pct": nonzero_pct.values,
        "correlation": correlations.values,
    })
    ranking = ranking.sort_values("correlation", ascending=False).reset_index(drop=True)
    ranking.index = ranking.index + 1
    ranking.index.name = "rank"
    return ranking


def extract(
    sport: str,
    data_dir: Path,
    seasons: list[int] | None = None,
    verbose: bool = False,
    incremental: bool = False,
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Run full feature extraction for a sport.

    Parameters
    ----------
    sport : str
        Lower-case sport key (e.g. ``"nba"``).
    data_dir : Path
        Root data directory containing ``normalized/`` parquet files.
    seasons : list[int] | None
        Seasons to extract. If None, auto-discovers all available.
    verbose : bool
        If True, print detailed feature diagnostics.
    incremental : bool
        If True and *output_path* exists, only extract NEW games not already
        in the output file.  Existing rows are preserved and merged.
    output_path : Path | None
        Path to existing features parquet (for incremental mode).

    Returns
    -------
    DataFrame of feature vectors (one row per game).
    """
    if seasons is None:
        seasons = _discover_seasons(data_dir, sport)
        if not seasons:
            logger.error("No game data found for sport %r", sport)
            return pd.DataFrame()

    # Incremental: load existing features to find already-extracted game_ids
    existing_df: pd.DataFrame | None = None
    existing_game_ids: set[str] | None = None
    if incremental and output_path and output_path.exists():
        try:
            existing_df = pd.read_parquet(output_path)
            if "game_id" in existing_df.columns:
                existing_game_ids = set(existing_df["game_id"].astype(str))
                logger.info(
                    "Incremental mode: %d existing features for %s",
                    len(existing_game_ids), sport,
                )
        except Exception as exc:
            logger.warning("Could not load existing features for incremental: %s", exc)
            existing_df = None
            existing_game_ids = None

    # Data inventory
    inventory = _data_inventory(data_dir, sport)
    if verbose:
        print(f"\n{'='*60}")
        print(f"  Data Inventory for {sport.upper()}")
        print(f"{'='*60}")
        for dtype, szns in sorted(inventory.items()):
            print(f"  {dtype:20s} → seasons: {szns if szns else '[aggregate only]'}")
        print()

    extractor = get_extractor(sport, data_dir)

    frames: list[pd.DataFrame] = []
    total_start = time.time()

    for season in seasons:
        start = time.time()
        logger.info("Extracting features for %s season %d …", sport, season)
        try:
            df = extractor.extract_all(season, existing_game_ids=existing_game_ids)
            if df is not None and len(df) > 0:
                df["season"] = season
                frames.append(df)
                elapsed = time.time() - start
                logger.info(
                    "  → %d games, %d features (%.1fs)",
                    len(df),
                    len([c for c in df.columns if c not in _META_COLS and c != "season"]),
                    elapsed,
                )
            else:
                logger.warning("  → no data for season %d", season)
        except Exception:
            logger.error("  → error extracting season %d", season, exc_info=True)

    # Merge new features with existing (incremental)
    if existing_df is not None and not existing_df.empty:
        if frames:
            new_df = pd.concat(frames, ignore_index=True)
            logger.info("Incremental: %d new rows + %d existing = merging", len(new_df), len(existing_df))
            full = pd.concat([existing_df, new_df], ignore_index=True)
            # Deduplicate by game_id (keep last = new extraction)
            if "game_id" in full.columns:
                full = full.drop_duplicates(subset=["game_id"], keep="last").reset_index(drop=True)
        else:
            logger.info("Incremental: no new games — returning cached features")
            return existing_df
    elif not frames:
        logger.error("No feature data extracted for %s", sport)
        return pd.DataFrame()
    else:
        full = pd.concat(frames, ignore_index=True)

    # Ensure all feature columns are numeric
    feature_cols = [c for c in full.columns if c not in _META_COLS and c != "season"]
    for col in feature_cols:
        if full[col].dtype == object:
            full[col] = pd.to_numeric(full[col], errors="coerce").fillna(0)

    total_elapsed = time.time() - total_start

    # Summary
    n_features = len([c for c in full.columns if c not in _META_COLS and c != "season"])
    n_games = len(full)
    completed = full["home_score"].notna().sum() if "home_score" in full.columns else 0
    nulls = full[feature_cols].isnull().sum().sum()
    total_cells = n_games * n_features

    print(f"\n{'─'*60}")
    print(f"  Feature Extraction Summary — {sport.upper()}")
    print(f"{'─'*60}")
    print(f"  Seasons:       {seasons}")
    print(f"  Games:         {n_games} ({completed} completed)")
    print(f"  Features:      {n_features}")
    print(f"  Data sources:  {', '.join(sorted(inventory.keys()))}")
    print(f"  Null cells:    {nulls}/{total_cells} ({100*nulls/total_cells:.1f}%)" if total_cells > 0 else "")
    print(f"  Time:          {total_elapsed:.1f}s")
    print(f"{'─'*60}")

    # Feature groups breakdown
    groups = {
        "Form":       [c for c in feature_cols if "form_" in c],
        "Advanced":   [c for c in feature_cols if any(k in c for k in ["pythag", "score_std", "margin_std", "scoring_trend", "win_streak", "avg_total", "close_game"])],
        "H2H":        [c for c in feature_cols if c.startswith("h2h_")],
        "Momentum":   [c for c in feature_cols if "momentum" in c],
        "Splits":     [c for c in feature_cols if "home_win_pct" in c or "away_win_pct" in c and "stnd" not in c and "form" not in c],
        "Season":     [c for c in feature_cols if "season_" in c],
        "Conference": [c for c in feature_cols if "conference" in c or "standings_win" in c],
        "Schedule":   [c for c in feature_cols if any(k in c for k in ["rest_", "b2b", "back_to_back", "games_in_last"])],
        "Special":    [c for c in feature_cols if c in ("ist_game", "altitude")],
        "Odds (legacy)":    [c for c in feature_cols if c in ("home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob")],
        "Injuries":   [c for c in feature_cols if "injury" in c],
        "Odds (enh)": [c for c in feature_cols if c.startswith("odds_")],
        "Standings":  [c for c in feature_cols if "stnd_" in c],
        "Player Stats": [c for c in feature_cols if "pstats_" in c],
        "SOS":        [c for c in feature_cols if c.endswith("_sos") or c == "sos_diff"],
        "Last 5":     [c for c in feature_cols if "last5_" in c],
    }

    print(f"\n  Feature Groups:")
    for group, cols in groups.items():
        if cols:
            print(f"    {group:20s} {len(cols):3d} features")

    if verbose:
        print(f"\n{'='*60}")
        print(f"  Feature Importance Ranking (by |correlation| with home_win)")
        print(f"{'='*60}")
        ranking = _feature_importance_ranking(full)
        if not ranking.empty:
            # Print top 30 and bottom 10
            top_n = min(30, len(ranking))
            for i, row in ranking.head(top_n).iterrows():
                bar = "█" * int(row["correlation"] * 40)
                print(
                    f"  {i:3d}. {row['feature']:40s}  "
                    f"corr={row['correlation']:.3f}  "
                    f"var={row['variance']:>10.2f}  "
                    f"nz={row['nonzero_pct']:.0%}  {bar}"
                )
            if len(ranking) > top_n:
                print(f"  ... ({len(ranking) - top_n} more features)")
                print(f"\n  Bottom 10 (least correlated):")
                for i, row in ranking.tail(10).iterrows():
                    print(
                        f"  {i:3d}. {row['feature']:40s}  "
                        f"corr={row['correlation']:.3f}  "
                        f"nz={row['nonzero_pct']:.0%}"
                    )

        # Dead features warning
        dead = ranking.loc[ranking["nonzero_pct"] <= 0.05]
        if not dead.empty:
            print(f"\n  ⚠ {len(dead)} dead features (≤5% non-zero):")
            for _, row in dead.iterrows():
                print(f"    - {row['feature']} (nz={row['nonzero_pct']:.0%})")

    print()
    return full


# ── CLI ──────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ml.feature_extraction",
        description="Enhanced ML feature extraction using all normalized data types.",
    )
    parser.add_argument(
        "--sport",
        required=True,
        help=f"Sport key. Available: {', '.join(sorted(EXTRACTORS.keys()))}",
    )
    parser.add_argument(
        "--seasons",
        default=None,
        help="Comma-separated season years (e.g. 2023,2024,2025). Auto-discovers if omitted.",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Save extracted features to parquet file.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print feature importance rankings and diagnostics.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only extract NEW games not in existing output file (fast daily mode).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    # Resolve data directory
    backend_dir = Path(__file__).resolve().parent.parent
    data_dir = backend_dir.parent / "data"
    if not data_dir.exists():
        data_dir = backend_dir / "data"
    if not data_dir.exists():
        print(f"ERROR: Data directory not found at {data_dir}", file=sys.stderr)
        sys.exit(1)

    # Parse seasons
    seasons = None
    if args.seasons:
        try:
            seasons = [int(s.strip()) for s in args.seasons.split(",")]
        except ValueError:
            print(f"ERROR: Invalid seasons format: {args.seasons!r}", file=sys.stderr)
            sys.exit(1)

    # Extract
    out_path = Path(args.output) if args.output else None
    try:
        df = extract(
            sport=args.sport.lower(),
            data_dir=data_dir,
            seasons=seasons,
            verbose=args.verbose,
            incremental=args.incremental,
            output_path=out_path,
        )
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if df.empty:
        print("No features extracted.", file=sys.stderr)
        sys.exit(1)

    # Save if requested
    if args.output:
        out_path = Path(args.output)
        # If output is a directory, save as {sport}_all.parquet inside it
        if out_path.is_dir() or (not out_path.suffix):
            out_path.mkdir(parents=True, exist_ok=True)
            out_path = out_path / f"{args.sport.lower()}_all.parquet"
        if out_path.suffix == ".parquet":
            df.to_parquet(out_path, index=False)
        elif out_path.suffix == ".csv":
            df.to_csv(out_path, index=False)
        else:
            df.to_parquet(out_path, index=False)
        print(f"Saved {len(df)} rows × {len(df.columns)} cols → {out_path}")


if __name__ == "__main__":
    main()
