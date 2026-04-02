#!/usr/bin/env python3
"""
Feature Store Consolidation Utility

Rebuilds combined feature parquets ({sport}_all.parquet) from seasonal files.
Called after daily feature extraction to ensure trainer gets fresh data.

This fixes the issue where daily pipeline generates season_{YYYY}.parquet files
but trainer expects combined {sport}_all.parquet with all seasons.
"""

import sys
from pathlib import Path
from typing import Any

import pandas as pd


def consolidate_sport(sport: str, features_dir: Path, verbose: bool = False) -> dict[str, Any]:
    """
    Consolidate seasonal feature parquets for a sport into {sport}_all.parquet.

    Merges new seasonal data INTO the existing _all.parquet rather than
    rebuilding from scratch.  This prevents the daily pipeline (which only
    extracts the current season) from wiping out historical seasons stored in
    _all.parquet.

    Args:
        sport: Sport name (e.g., 'mlb', 'nba')
        features_dir: Path to features directory
        verbose: Enable debug logging

    Returns:
        Dict with consolidation status and metrics
    """
    # Find all seasonal parquets for this sport (exclude _all itself)
    seasonal_files = sorted(features_dir.glob(f"{sport}_*.parquet"))
    seasonal_files = [f for f in seasonal_files if not f.stem.endswith("_all")]
    if not seasonal_files:
        return {"sport": sport, "status": "no_seasonal_files", "files": 0}

    combined_path = features_dir / f"{sport}_all.parquet"

    # mtime-based skip: if _all.parquet is newer than ALL seasonal files, nothing new to add
    if combined_path.exists():
        all_mtime = combined_path.stat().st_mtime
        newest_seasonal = max(f.stat().st_mtime for f in seasonal_files)
        if all_mtime > newest_seasonal:
            if verbose:
                print(f"  {sport}: _all.parquet is up-to-date, skipping", flush=True)
            return {"sport": sport, "status": "up_to_date", "files": len(seasonal_files)}

    # Load seasonal files
    new_dfs = []
    for fpath in seasonal_files:
        try:
            df = pd.read_parquet(fpath)
            if not df.empty:
                new_dfs.append(df)
                if verbose:
                    print(f"  Loaded {fpath.stem}: {len(df)} rows × {len(df.columns)} cols", flush=True)
        except Exception as e:
            if verbose:
                print(f"  Warning: Failed to load {fpath.stem}: {e}", flush=True)

    if not new_dfs:
        return {"sport": sport, "status": "no_valid_files", "files": len(seasonal_files)}

    new_data = pd.concat(new_dfs, ignore_index=True)

    # CRITICAL: merge into existing _all.parquet to preserve historical seasons.
    # Without this, running the daily pipeline (which only extracts the current
    # season) would overwrite years of training data with a single season.
    id_cols = ["game_id", "player_id"] if "player_id" in new_data.columns else ["game_id"]
    if combined_path.exists():
        try:
            existing = pd.read_parquet(combined_path)
            existing_seasons = set(existing["season"].unique()) if "season" in existing.columns else set()
            new_seasons = set(new_data["season"].unique()) if "season" in new_data.columns else set()
            if existing_seasons - new_seasons:
                # Existing _all.parquet has seasons not covered by seasonal files →
                # merge to preserve historical data
                combined = pd.concat([existing, new_data], ignore_index=True)
                if verbose:
                    print(
                        f"  {sport}: merging {len(existing)} existing + {len(new_data)} new rows "
                        f"(seasons {sorted(existing_seasons)} + {sorted(new_seasons)})",
                        flush=True,
                    )
            else:
                # Seasonal files cover all (or more) seasons → rebuild cleanly
                combined = new_data
        except Exception as e:
            if verbose:
                print(f"  Warning: Could not read existing _all.parquet ({e}), rebuilding", flush=True)
            combined = new_data
    else:
        combined = new_data

    # Deduplicate — keep last (newest extraction wins)
    combined = combined.drop_duplicates(subset=id_cols, keep="last").reset_index(drop=True)

    combined.to_parquet(combined_path, compression="snappy", index=False)

    if verbose:
        print(
            f"  Saved to {combined_path.name}: {len(combined)} rows (deduped) × {len(combined.columns)} cols",
            flush=True,
        )

    return {
        "sport": sport,
        "status": "consolidated",
        "files_combined": len(new_dfs),
        "rows_output": len(combined),
        "columns": len(combined.columns),
        "output_path": str(combined_path),
    }


def main(argv: list[str] | None = None) -> int:
    """
    Consolidate all sports' feature stores.
    
    Usage:
        python consolidate_features.py [data_dir]
    """
    import argparse
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    parser = argparse.ArgumentParser(
        description="Consolidate seasonal feature parquets into combined _all.parquet files"
    )
    parser.add_argument(
        "data_dir",
        nargs="?",
        default="data",
        help="Path to data directory (default: data)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--sport",
        help="Consolidate only a specific sport (e.g., 'mlb')"
    )
    
    args = parser.parse_args(argv)
    
    data_dir = Path(args.data_dir)
    features_dir = data_dir / "features"
    
    if not features_dir.is_dir():
        print(f"ERROR: Features directory not found: {features_dir}", file=sys.stderr)
        return 1
    
    if args.verbose:
        print(f"Consolidating features from: {features_dir}")
    
    # Determine which sports to consolidate
    sports_to_process = []
    if args.sport:
        sports_to_process = [args.sport.lower()]
    else:
        # Find all sports with seasonal files
        seasonal_files = features_dir.glob("*_*.parquet")
        sports = set()
        for fpath in seasonal_files:
            # Parse sport name from filename (e.g., mlb_2026.parquet → mlb)
            parts = fpath.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                sports.add("_".join(parts[:-1]))
        sports_to_process = sorted(sports)
    
    if not sports_to_process:
        print("No seasonal features found to consolidate", file=sys.stderr)
        return 0
    
    if args.verbose:
        print(f"Processing {len(sports_to_process)} sports: {', '.join(sports_to_process)}")
    
    # Parallelize across sports — each writes to a separate _all.parquet file
    results = []
    workers = min(len(sports_to_process), 6)
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {
                pool.submit(consolidate_sport, sport, features_dir, args.verbose): sport
                for sport in sports_to_process
            }
            for fut in as_completed(futs):
                results.append(fut.result())
    else:
        for sport in sports_to_process:
            results.append(consolidate_sport(sport, features_dir, verbose=args.verbose))

    for result in sorted(results, key=lambda r: r["sport"]):
        if result["status"] == "consolidated":
            print(f"✓ {result['sport']}: {result['files_combined']} files → {result['rows_output']} rows")
        else:
            print(f"⚠ {result['sport']}: {result['status']}")
    
    # Summary
    successful = sum(1 for r in results if r["status"] == "consolidated")
    print(f"\n{successful}/{len(results)} sports consolidated successfully")
    
    return 0 if successful == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
