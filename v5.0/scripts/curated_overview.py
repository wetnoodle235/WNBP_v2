#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SeasonCoverage:
    sport: str
    season: str
    legacy_kinds: list[str]
    curated_categories: list[str]


def discover_legacy_kinds(normalized_dir: Path, sport: str, season: str) -> list[str]:
    sport_dir = normalized_dir / sport
    if not sport_dir.exists():
        return []
    suffix = f"_{season}.parquet"
    kinds: set[str] = set()
    for file in sport_dir.glob(f"*{suffix}"):
        name = file.name
        if name.endswith(suffix):
            kinds.add(name[: -len(suffix)])
    return sorted(kinds)


def discover_curated_categories(curated_dir: Path, sport: str, season: str) -> list[str]:
    sport_dir = curated_dir / sport
    if not sport_dir.exists():
        return []
    categories: list[str] = []
    for category_dir in sorted([d for d in sport_dir.iterdir() if d.is_dir()]):
        season_dir = category_dir / f"season={season}"
        if season_dir.exists() and any(season_dir.rglob("*.parquet")):
            categories.append(category_dir.name)
    return categories


def discover_seasons(normalized_dir: Path, sport: str) -> list[str]:
    sport_dir = normalized_dir / sport
    if not sport_dir.exists():
        return []
    seasons: set[str] = set()
    for file in sport_dir.glob("games_*.parquet"):
        stem = file.stem.replace("games_", "")
        if stem.isdigit():
            seasons.add(stem)
    return sorted(seasons)


def build_coverage(normalized_dir: Path, curated_dir: Path, sports: list[str]) -> list[SeasonCoverage]:
    rows: list[SeasonCoverage] = []
    for sport in sports:
        seasons = discover_seasons(normalized_dir, sport)
        for season in seasons:
            rows.append(
                SeasonCoverage(
                    sport=sport,
                    season=season,
                    legacy_kinds=discover_legacy_kinds(normalized_dir, sport, season),
                    curated_categories=discover_curated_categories(curated_dir, sport, season),
                )
            )
    return rows


def write_markdown(rows: list[SeasonCoverage], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Curated Coverage Overview")
    lines.append("")
    lines.append("| Sport | Season | Legacy kinds | Curated categories | Curated/Legacy |")
    lines.append("|---|---:|---:|---:|---:|")

    for row in rows:
        legacy_count = len(row.legacy_kinds)
        curated_count = len(row.curated_categories)
        ratio = f"{curated_count}/{legacy_count}" if legacy_count else f"{curated_count}/0"
        lines.append(
            f"| {row.sport} | {row.season} | {legacy_count} | {curated_count} | {ratio} |"
        )

    lines.append("")
    lines.append("## Category Detail")
    lines.append("")
    for row in rows:
        lines.append(f"### {row.sport} {row.season}")
        lines.append("")
        lines.append(f"- legacy kinds: {', '.join(row.legacy_kinds) if row.legacy_kinds else '(none)'}")
        lines.append(f"- curated categories: {', '.join(row.curated_categories) if row.curated_categories else '(none)'}")
        lines.append("")

    output_file.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate cross-sport curated coverage overview")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parent.parent), help="v5.0 root")
    parser.add_argument("--sports", default="", help="Comma-separated sports; default autodetect from normalized")
    parser.add_argument("--output", default="data/reports/curated_coverage_overview.md", help="Output markdown path relative to root")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    normalized_dir = root / "data" / "normalized"
    curated_dir = root / "data" / "normalized_curated"

    if args.sports.strip():
        sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()]
    else:
        sports = sorted([d.name for d in normalized_dir.iterdir() if d.is_dir()]) if normalized_dir.exists() else []

    rows = build_coverage(normalized_dir, curated_dir, sports)
    output_file = root / args.output
    write_markdown(rows, output_file)
    print(f"wrote {output_file} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
