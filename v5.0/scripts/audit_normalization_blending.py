#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


VENDOR_HINTS = ("vendor", "provider", "bookmaker", "sportsbook", "source")
ID_HINTS = ("game_id", "team_id", "player_id", "home_team_id", "away_team_id", "date", "season")


@dataclass
class AuditRow:
    sport: str
    season: str
    category: str
    rows: int
    columns: int
    vendor_columns: list[str]
    vendor_uniques: dict[str, int]
    id_columns: list[str]


def discover_sports(curated_root: Path) -> list[str]:
    if not curated_root.exists():
        return []
    return sorted([d.name for d in curated_root.iterdir() if d.is_dir()])


def discover_seasons(curated_root: Path, sport: str) -> list[str]:
    sport_dir = curated_root / sport
    if not sport_dir.exists():
        return []

    seasons: set[str] = set()
    for category_dir in [d for d in sport_dir.iterdir() if d.is_dir()]:
        for season_dir in category_dir.glob("season=*"):
            season = season_dir.name.replace("season=", "")
            if season:
                seasons.add(season)
    return sorted(seasons)


def load_category_df(curated_root: Path, sport: str, category: str, season: str) -> pd.DataFrame:
    season_dir = curated_root / sport / category / f"season={season}"
    if not season_dir.exists():
        return pd.DataFrame()

    files = sorted(season_dir.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()

    frames = [pd.read_parquet(f, engine="pyarrow") for f in files]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def audit_category(curated_root: Path, sport: str, season: str, category: str) -> AuditRow | None:
    df = load_category_df(curated_root, sport, category, season)
    if df.empty:
        return None

    vendor_cols = [c for c in df.columns if any(h in c.lower() for h in VENDOR_HINTS)]
    vendor_uniques = {c: int(df[c].nunique(dropna=True)) for c in vendor_cols}
    id_cols = [c for c in df.columns if c in ID_HINTS]

    return AuditRow(
        sport=sport,
        season=season,
        category=category,
        rows=int(len(df)),
        columns=int(len(df.columns)),
        vendor_columns=vendor_cols,
        vendor_uniques=vendor_uniques,
        id_columns=id_cols,
    )


def write_markdown(rows: list[AuditRow], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Normalization and Vendor Blending Audit")
    lines.append("")
    lines.append("This report inspects curated parquet outputs and surfaces schema harmonization keys and vendor/provider diversity.")
    lines.append("")
    lines.append("| Sport | Season | Category | Rows | Cols | Vendor cols | Max vendor cardinality | ID cols |")
    lines.append("|---|---:|---|---:|---:|---:|---:|---:|")

    for r in rows:
        max_vendor_card = max(r.vendor_uniques.values()) if r.vendor_uniques else 0
        lines.append(
            f"| {r.sport} | {r.season} | {r.category} | {r.rows} | {r.columns} | {len(r.vendor_columns)} | {max_vendor_card} | {len(r.id_columns)} |"
        )

    lines.append("")
    lines.append("## Details")
    lines.append("")
    for r in rows:
        lines.append(f"### {r.sport} {r.season} {r.category}")
        lines.append("")
        lines.append(f"- rows: {r.rows}")
        lines.append(f"- columns: {r.columns}")
        lines.append(f"- id columns detected: {', '.join(r.id_columns) if r.id_columns else '(none)'}")
        if r.vendor_columns:
            lines.append("- vendor columns and unique values:")
            for col in r.vendor_columns:
                lines.append(f"  - {col}: {r.vendor_uniques.get(col, 0)}")
        else:
            lines.append("- vendor columns and unique values: (none)")
        lines.append("")

    output.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit curated normalization and vendor blending")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parent.parent), help="v5.0 root")
    parser.add_argument("--sports", default="", help="Comma-separated sports; default all discovered")
    parser.add_argument("--seasons", default="", help="Comma-separated seasons; default discovered per sport")
    parser.add_argument("--categories", default="odds,market_signals,predictions,player_props,games,stats", help="Comma-separated categories")
    parser.add_argument("--output", default="data/reports/normalization_blending_audit.md", help="Output markdown path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    curated_root = root / "data" / "normalized_curated"

    sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()] if args.sports.strip() else discover_sports(curated_root)
    requested_seasons = [s.strip() for s in args.seasons.split(",") if s.strip()]
    categories = [c.strip() for c in args.categories.split(",") if c.strip()]

    rows: list[AuditRow] = []
    for sport in sports:
        seasons = requested_seasons or discover_seasons(curated_root, sport)
        for season in seasons:
            for category in categories:
                row = audit_category(curated_root, sport, season, category)
                if row is not None:
                    rows.append(row)

    output = root / args.output
    write_markdown(rows, output)
    print(f"wrote {output} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
