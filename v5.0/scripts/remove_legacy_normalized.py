#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_CORE_CATEGORIES = ["games", "teams", "players", "stats"]


def curated_has_core(curated_root: Path, sport: str, season: str, core_categories: list[str]) -> bool:
    for cat in core_categories:
        season_dir = curated_root / sport / cat / f"season={season}"
        if not season_dir.exists():
            return False
        if not any(season_dir.rglob("*.parquet")):
            return False
    return True


def remove_legacy_for_season(normalized_root: Path, sport: str, season: str, apply: bool) -> tuple[int, int]:
    removed = 0
    skipped = 0
    sport_dir = normalized_root / sport
    if not sport_dir.exists():
        return removed, skipped

    pattern = f"*_{season}.parquet"
    for file in sport_dir.glob(pattern):
        if apply:
            try:
                file.unlink(missing_ok=True)
                removed += 1
            except Exception:
                skipped += 1
        else:
            removed += 1

    return removed, skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove legacy normalized v1 files when curated core parity exists")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parent.parent), help="v5.0 root")
    parser.add_argument("--sports", default="", help="Comma-separated sports; default autodetect from normalized")
    parser.add_argument("--seasons", default="", help="Comma-separated seasons; default autodetect per sport")
    parser.add_argument("--core-categories", default=",".join(DEFAULT_CORE_CATEGORIES), help="Core curated categories required before removal")
    parser.add_argument("--apply", action="store_true", help="Perform deletion (otherwise dry-run)")
    parser.add_argument("--remove-empty-sport-dirs", action="store_true", help="Remove empty sport directories after deletion")
    return parser.parse_args()


def discover_sports(normalized_root: Path) -> list[str]:
    if not normalized_root.exists():
        return []
    return sorted([d.name for d in normalized_root.iterdir() if d.is_dir()])


def discover_seasons(normalized_root: Path, sport: str) -> list[str]:
    sport_dir = normalized_root / sport
    if not sport_dir.exists():
        return []
    seasons: set[str] = set()
    for file in sport_dir.glob("games_*.parquet"):
        val = file.stem.replace("games_", "")
        if val.isdigit():
            seasons.add(val)
    return sorted(seasons)


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    normalized_root = root / "data" / "normalized"
    curated_root = root / "data" / "normalized_curated"

    core_categories = [c.strip() for c in args.core_categories.split(",") if c.strip()]

    sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()] if args.sports.strip() else discover_sports(normalized_root)
    requested_seasons = [s.strip() for s in args.seasons.split(",") if s.strip()]

    total_removable = 0
    total_removed = 0
    total_skipped = 0
    blocked: list[str] = []

    for sport in sports:
        seasons = requested_seasons or discover_seasons(normalized_root, sport)
        for season in seasons:
            if not curated_has_core(curated_root, sport, season, core_categories):
                blocked.append(f"{sport}:{season}")
                continue

            removable, skipped = remove_legacy_for_season(normalized_root, sport, season, apply=args.apply)
            total_removable += removable
            total_skipped += skipped
            if args.apply:
                total_removed += removable - skipped

        if args.apply and args.remove_empty_sport_dirs:
            sport_dir = normalized_root / sport
            if sport_dir.exists() and not any(sport_dir.iterdir()):
                try:
                    sport_dir.rmdir()
                except Exception:
                    pass

    mode = "apply" if args.apply else "dry-run"
    print(f"mode={mode}")
    print(f"core_categories={','.join(core_categories)}")
    print(f"removable_files={total_removable}")
    print(f"removed_files={total_removed}")
    print(f"skipped_files={total_skipped}")
    print(f"blocked_sport_seasons={len(blocked)}")
    if blocked:
        print("blocked_list=" + ",".join(blocked))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
