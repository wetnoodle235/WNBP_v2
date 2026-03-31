#!/usr/bin/env python3
"""Merge per-season parquet files into combined files.

For each sport directory under ``data/normalized/``, finds season-specific
files like ``games_2023.parquet``, ``games_2024.parquet`` etc. and merges
them into a single ``games.parquet``.  Deduplicates by the appropriate
primary key for each data type.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NORMALIZED_DIR = PROJECT_ROOT / "data" / "normalized"

# Regex: match  <type>_<4-digit-year>.parquet
_SEASON_RE = re.compile(r"^(.+)_(\d{4})\.parquet$")

# Primary-key columns used for deduplication per data type.
# Compound keys use a tuple; single keys use a string.
_DEDUP_KEYS: dict[str, str | tuple[str, ...]] = {
    "games": "id",
    "teams": "id",
    "players": "id",
    "standings": ("team_id", "season"),
    "player_stats": ("game_id", "player_id"),
    "odds": ("game_id", "bookmaker"),
    "injuries": ("player_id", "sport"),
    "news": "id",
}


def _dedup_key(data_type: str) -> str | list[str] | None:
    """Return the dedup column(s) for *data_type*, or ``None``."""
    key = _DEDUP_KEYS.get(data_type)
    if key is None:
        return None
    return list(key) if isinstance(key, tuple) else key


def merge_sport(sport_dir: Path, *, dry_run: bool = False) -> dict[str, int]:
    """Merge all season files in *sport_dir*. Returns {type: row_count}."""
    # Discover data types from filenames
    types_to_files: dict[str, list[Path]] = {}
    for f in sorted(sport_dir.iterdir()):
        m = _SEASON_RE.match(f.name)
        if m:
            data_type = m.group(1)
            types_to_files.setdefault(data_type, []).append(f)

    results: dict[str, int] = {}
    for data_type, files in sorted(types_to_files.items()):
        if len(files) == 0:
            continue

        frames = []
        for f in files:
            try:
                df = pd.read_parquet(f)
                if not df.empty:
                    frames.append(df)
            except Exception as exc:
                print(f"  ⚠ Failed to read {f.name}: {exc}", file=sys.stderr)

        if not frames:
            continue

        combined = pd.concat(frames, ignore_index=True)

        # Deduplicate
        key = _dedup_key(data_type)
        before = len(combined)
        if key is not None:
            cols = [key] if isinstance(key, str) else key
            valid_cols = [c for c in cols if c in combined.columns]
            if valid_cols:
                combined = combined.drop_duplicates(subset=valid_cols, keep="last")
        else:
            combined = combined.drop_duplicates(keep="last")
        after = len(combined)

        out_path = sport_dir / f"{data_type}.parquet"

        if dry_run:
            print(f"  {data_type}: {len(files)} files → {after} rows "
                  f"(deduped {before - after})")
        else:
            combined.to_parquet(out_path, index=False, engine="pyarrow")
            print(f"  {data_type}: {len(files)} files → {after} rows "
                  f"(deduped {before - after}) → {out_path.name}")

        results[data_type] = after

    return results


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sport", help="Process only this sport")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be merged without writing files")
    args = parser.parse_args()

    if not NORMALIZED_DIR.is_dir():
        print(f"Normalized directory not found: {NORMALIZED_DIR}", file=sys.stderr)
        sys.exit(1)

    sport_dirs = sorted(NORMALIZED_DIR.iterdir())
    if args.sport:
        sport_dirs = [d for d in sport_dirs if d.name == args.sport]

    total_types = 0
    total_rows = 0

    for sport_dir in sport_dirs:
        if not sport_dir.is_dir():
            continue
        print(f"\n{sport_dir.name}/")
        results = merge_sport(sport_dir, dry_run=args.dry_run)
        total_types += len(results)
        total_rows += sum(results.values())

    action = "Would merge" if args.dry_run else "Merged"
    print(f"\n✓ {action} {total_types} data types, {total_rows:,} total rows")


if __name__ == "__main__":
    main()
