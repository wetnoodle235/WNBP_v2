#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import date
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq


PROVIDER_COL_HINTS = ("provider", "vendor", "source")
YEAR_RE = re.compile(r"^(?:19|20)\d{2}$")
MIN_SEASON_YEAR = 1990
MAX_SEASON_YEAR = date.today().year + 2
SEASON_TYPE_VALUES = {
    "regular",
    "preseason",
    "postseason",
    "playoffs",
    "final",
    "finals",
    "group",
    "group_stage",
    "knockout",
    "qualifier",
    "qualifying",
}
SPORT_ALIASES = {
    "college-football": "ncaaf",
    "college_football": "ncaaf",
    "cfb": "ncaaf",
    "college-basketball": "ncaab",
    "college_basketball": "ncaab",
    "cbb": "ncaab",
    "women-college-basketball": "ncaaw",
    "women_college_basketball": "ncaaw",
    "womens-college-basketball": "ncaaw",
    "women's-college-basketball": "ncaaw",
    "premier-league": "epl",
    "premier_league": "epl",
    "uefa-champions-league": "ucl",
    "uefa_champions_league": "ucl",
    "champions-league": "ucl",
    "champions_league": "ucl",
    "uefa-europa-league": "europa",
    "uefa_europa_league": "europa",
    "formula1": "f1",
    "formula-1": "f1",
    "dota": "dota2",
    "league-of-legends": "lol",
    "league_of_legends": "lol",
    "counter-strike": "csgo",
    "counter_strike": "csgo",
    "women-tennis": "wta",
    "mens-tennis": "atp",
}


def _norm_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _build_vendor_aliases(vendors: Iterable[str]) -> dict[str, set[str]]:
    aliases: dict[str, set[str]] = {}
    for vendor in vendors:
        base = _norm_text(vendor)
        opts = {base}
        if base.endswith("api"):
            opts.add(base.replace("api", ""))
        if base.endswith("stats"):
            opts.add(base.replace("stats", ""))
        if base == "footballdata":
            opts.add("footballdata")
            opts.add("football-data")
        if base == "oddsapi":
            opts.update({"theoddsapi", "theodds"})
        if base == "openf1":
            opts.add("openf1")
        aliases[vendor] = {_norm_text(x) for x in opts}
    return aliases


def discover_provider_priority_sports(root: Path) -> set[str]:
    provider_map = root / "backend" / "normalization" / "provider_map.py"
    if not provider_map.exists():
        return set()

    text = provider_map.read_text(encoding="utf-8", errors="ignore")
    sports = set(re.findall(r'^[ \t]*"([a-z0-9_]+)"\s*:\s*\{', text, flags=re.MULTILINE))
    return {s.lower() for s in sports}


def infer_sport_from_parts(parts: tuple[str, ...], sports: set[str]) -> str | None:
    lower_parts = [p.lower() for p in parts]

    for part in lower_parts:
        if part in sports:
            return part
        if part in SPORT_ALIASES:
            return SPORT_ALIASES[part]

    joined = "/".join(lower_parts)
    for alias, sport in SPORT_ALIASES.items():
        if alias in joined:
            return sport

    for sport in sorted(sports, key=len, reverse=True):
        if re.search(rf"(^|[^a-z0-9]){re.escape(sport)}([^a-z0-9]|$)", joined):
            return sport
    return None


def discover_curated_categories_for_sport(curated_root: Path, sport: str) -> set[str]:
    sport_dir = curated_root / sport
    if not sport_dir.exists():
        return set()

    categories: set[str] = set()
    for file in sport_dir.rglob("*.parquet"):
        rel = file.relative_to(sport_dir)
        cat_parts: list[str] = []
        for part in rel.parts:
            if part.startswith("season="):
                break
            cat_parts.append(part)
        if cat_parts:
            categories.add("/".join(cat_parts))
    return categories


def provider_columns_for_file(file_path: Path) -> list[str]:
    try:
        parquet = pq.ParquetFile(file_path)
    except Exception:
        return []

    try:
        names = parquet.schema_arrow.names
    except Exception:
        return []

    return [c for c in names if any(h in c.lower() for h in PROVIDER_COL_HINTS)]


def _extract_years_from_text(text: str) -> set[int]:
    years: set[int] = set()
    # Only accept stand-alone 4-digit year tokens from path-like text.
    tokens = re.split(r"[^0-9]+", text)
    for token in tokens:
        if not token:
            continue
        if not YEAR_RE.match(token):
            continue
        year = int(token)
        if year < MIN_SEASON_YEAR or year > MAX_SEASON_YEAR:
            continue
        years.add(year)
    return years


def _extract_season_types_from_text(text: str) -> set[str]:
    norm = _norm_text(text)
    hits: set[str] = set()
    for value in SEASON_TYPE_VALUES:
        alias = _norm_text(value)
        if alias and alias in norm:
            hits.add(value)
    return hits


def discover_file_years_and_types(file_path: Path) -> tuple[set[int], set[str]]:
    text = "/".join(file_path.parts)
    years = _extract_years_from_text(text)
    season_types = _extract_season_types_from_text(text)
    return years, season_types


def discover_parquet_season_types(file_path: Path) -> set[str]:
    try:
        parquet = pq.ParquetFile(file_path)
    except Exception:
        return set()

    try:
        cols = {c.lower() for c in parquet.schema_arrow.names}
    except Exception:
        return set()

    if "season_type" not in cols:
        return set()

    hits: set[str] = set()
    try:
        for batch in parquet.iter_batches(columns=["season_type"], batch_size=2048):
            for value in batch["season_type"].to_pylist():
                if value is None:
                    continue
                text = str(value).strip().lower()
                if text:
                    hits.add(text)
            if len(hits) >= 12:
                break
    except Exception:
        return set()
    return hits


def discover_dataset_years_and_types(
    files: list[Path],
    season_type_scan_limit: int,
) -> tuple[set[int], set[str]]:
    years: set[int] = set()
    season_types: set[str] = set()

    for file_path in files:
        file_years, file_types = discover_file_years_and_types(file_path)
        years.update(file_years)
        season_types.update(file_types)

    scanned = 0
    for file_path in files:
        if scanned >= season_type_scan_limit:
            break
        if file_path.suffix.lower() != ".parquet":
            continue
        season_types.update(discover_parquet_season_types(file_path))
        scanned += 1

    return years, season_types


def scan_vendor_hits_in_files(
    files: list[Path],
    vendors: Iterable[str],
    vendor_aliases: dict[str, set[str]],
) -> dict[str, int]:
    hits = {vendor: 0 for vendor in vendors}

    for file_path in files:
        cols = provider_columns_for_file(file_path)
        if not cols:
            continue
        try:
            parquet = pq.ParquetFile(file_path)
            seen_in_file: set[str] = set()
            for batch in parquet.iter_batches(columns=cols, batch_size=4096):
                for column in cols:
                    values = batch[column].to_pylist()
                    for value in values:
                        if value is None:
                            continue
                        norm = _norm_text(str(value))
                        if not norm:
                            continue
                        for vendor in vendors:
                            if vendor in seen_in_file:
                                continue
                            aliases = vendor_aliases.get(vendor, {vendor})
                            if any(a and a in norm for a in aliases):
                                seen_in_file.add(vendor)
                if len(seen_in_file) == len(hits):
                    break

            for vendor in seen_in_file:
                hits[vendor] += 1
        except Exception:
            continue

    return hits


@dataclass
class VendorSportRow:
    sport: str
    vendor: str
    raw_files: int
    raw_years: set[int]
    raw_season_types: set[str]
    normalized_files: int
    curated_files: int
    curated_categories: int
    normalized_vendor_hits: int
    curated_vendor_hits: int
    status: str
    notes: str


def status_for_row(row: VendorSportRow) -> tuple[str, str]:
    if row.raw_files == 0:
        return "-", "No raw files for this vendor+sport path inference"
    if row.curated_vendor_hits > 0:
        return "[x] covered", "Vendor signal appears in curated provider/source columns"
    if row.normalized_vendor_hits > 0 and row.curated_files > 0:
        return "[ ] partial", "Vendor signal in normalized but not found in curated provider/source columns"
    if row.normalized_files > 0 and row.curated_files > 0:
        return "[ ] review", "Sport is curated, but this vendor has no explicit provider/source signal"
    if row.normalized_files > 0 and row.curated_files == 0:
        return "[ ] ignored", "Sport has normalized files but no curated output"
    return "[ ] ignored", "Raw exists but no normalized/curated path detected"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sport/vendor raw vs curated checklist")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parent.parent), help="v5.0 root")
    parser.add_argument("--sports", default="", help="Comma-separated sports to include")
    parser.add_argument(
        "--output",
        default="data/reports/sport_vendor_raw_vs_curated_checklist.md",
        help="Markdown output path relative to --root",
    )
    parser.add_argument(
        "--csv-output",
        default="data/reports/sport_vendor_raw_vs_curated_checklist.csv",
        help="CSV output path relative to --root",
    )
    parser.add_argument(
        "--season-type-scan-limit",
        type=int,
        default=200,
        help="Max parquet files per sport to scan for season_type values",
    )
    return parser.parse_args()


def format_years(years: set[int]) -> str:
    if not years:
        return "-"
    ordered = sorted(years)
    if len(ordered) <= 8:
        return ", ".join(str(y) for y in ordered)
    return f"{ordered[0]}..{ordered[-1]} ({len(ordered)} yrs)"


def format_types(values: set[str]) -> str:
    if not values:
        return "-"
    return ", ".join(sorted(values))


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    raw_root = root / "data" / "raw"
    normalized_root = root / "data" / "normalized"
    curated_root = root / "data" / "normalized_curated"

    raw_vendors = sorted([d.name for d in raw_root.iterdir() if d.is_dir()]) if raw_root.exists() else []
    vendor_aliases = _build_vendor_aliases(raw_vendors)

    normalized_sports = {d.name.lower() for d in normalized_root.iterdir() if d.is_dir()} if normalized_root.exists() else set()
    curated_sports = {d.name.lower() for d in curated_root.iterdir() if d.is_dir()} if curated_root.exists() else set()
    mapped_sports = discover_provider_priority_sports(root)

    base_sports = normalized_sports | curated_sports | mapped_sports
    if args.sports.strip():
        selected_sports = {s.strip().lower() for s in args.sports.split(",") if s.strip()}
    else:
        selected_sports = set(base_sports)

    raw_files_by_vendor_sport: dict[tuple[str, str], int] = defaultdict(int)
    unknown_raw_files_by_vendor: dict[str, int] = defaultdict(int)
    raw_years_by_vendor_sport: dict[tuple[str, str], set[int]] = defaultdict(set)
    raw_types_by_vendor_sport: dict[tuple[str, str], set[str]] = defaultdict(set)

    for vendor in raw_vendors:
        vendor_dir = raw_root / vendor
        for file_path in vendor_dir.rglob("*"):
            if not file_path.is_file():
                continue
            sport = infer_sport_from_parts(file_path.relative_to(vendor_dir).parts, selected_sports)
            if sport is None:
                unknown_raw_files_by_vendor[vendor] += 1
                continue
            raw_files_by_vendor_sport[(vendor, sport)] += 1
            file_years, file_types = discover_file_years_and_types(file_path.relative_to(raw_root))
            raw_years_by_vendor_sport[(vendor, sport)].update(file_years)
            raw_types_by_vendor_sport[(vendor, sport)].update(file_types)

    all_sports_from_raw = {sport for (_, sport) in raw_files_by_vendor_sport.keys()}
    selected_sports = sorted(selected_sports | all_sports_from_raw)

    normalized_files_by_sport: dict[str, list[Path]] = {}
    curated_files_by_sport: dict[str, list[Path]] = {}
    curated_categories_by_sport: dict[str, set[str]] = {}
    normalized_years_by_sport: dict[str, set[int]] = {}
    curated_years_by_sport: dict[str, set[int]] = {}
    normalized_types_by_sport: dict[str, set[str]] = {}
    curated_types_by_sport: dict[str, set[str]] = {}

    for sport in selected_sports:
        normalized_files_by_sport[sport] = sorted((normalized_root / sport).glob("*.parquet")) if (normalized_root / sport).exists() else []
        curated_files_by_sport[sport] = sorted((curated_root / sport).rglob("*.parquet")) if (curated_root / sport).exists() else []
        curated_categories_by_sport[sport] = discover_curated_categories_for_sport(curated_root, sport)
        normalized_years_by_sport[sport], normalized_types_by_sport[sport] = discover_dataset_years_and_types(
            normalized_files_by_sport[sport],
            args.season_type_scan_limit,
        )
        curated_years_by_sport[sport], curated_types_by_sport[sport] = discover_dataset_years_and_types(
            curated_files_by_sport[sport],
            args.season_type_scan_limit,
        )

    normalized_vendor_hits_by_sport: dict[str, dict[str, int]] = {}
    curated_vendor_hits_by_sport: dict[str, dict[str, int]] = {}
    for sport in selected_sports:
        normalized_vendor_hits_by_sport[sport] = scan_vendor_hits_in_files(
            normalized_files_by_sport[sport],
            raw_vendors,
            vendor_aliases,
        )
        curated_vendor_hits_by_sport[sport] = scan_vendor_hits_in_files(
            curated_files_by_sport[sport],
            raw_vendors,
            vendor_aliases,
        )

    rows: list[VendorSportRow] = []
    for sport in selected_sports:
        for vendor in raw_vendors:
            row = VendorSportRow(
                sport=sport,
                vendor=vendor,
                raw_files=raw_files_by_vendor_sport.get((vendor, sport), 0),
                raw_years=raw_years_by_vendor_sport.get((vendor, sport), set()),
                raw_season_types=raw_types_by_vendor_sport.get((vendor, sport), set()),
                normalized_files=len(normalized_files_by_sport.get(sport, [])),
                curated_files=len(curated_files_by_sport.get(sport, [])),
                curated_categories=len(curated_categories_by_sport.get(sport, set())),
                normalized_vendor_hits=normalized_vendor_hits_by_sport.get(sport, {}).get(vendor, 0),
                curated_vendor_hits=curated_vendor_hits_by_sport.get(sport, {}).get(vendor, 0),
                status="",
                notes="",
            )
            row.status, row.notes = status_for_row(row)
            if row.raw_files > 0 or row.status in {"[ ] ignored", "[ ] partial", "[ ] review"}:
                rows.append(row)

    out_path = root / args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# Sport-by-Sport Vendor Checklist: Raw vs Normalized Curated")
    lines.append("")
    lines.append("Status legend: `[x] covered`, `[ ] partial`, `[ ] review`, `[ ] ignored`.")
    lines.append("")
    lines.append("## Sport Summary")
    lines.append("")
    lines.append("| Sport | Raw vendor-sport pairs | Normalized files | Curated files | Curated categories | Covered pairs | Ignored pairs |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")

    by_sport: dict[str, list[VendorSportRow]] = defaultdict(list)
    for row in rows:
        by_sport[row.sport].append(row)

    for sport in sorted(by_sport.keys()):
        sport_rows = by_sport[sport]
        raw_pairs = sum(1 for r in sport_rows if r.raw_files > 0)
        norm_files = sport_rows[0].normalized_files if sport_rows else 0
        cur_files = sport_rows[0].curated_files if sport_rows else 0
        cur_cats = sport_rows[0].curated_categories if sport_rows else 0
        covered_pairs = sum(1 for r in sport_rows if r.status == "[x] covered")
        ignored_pairs = sum(1 for r in sport_rows if r.raw_files > 0 and r.status == "[ ] ignored")
        lines.append(f"| {sport} | {raw_pairs} | {norm_files} | {cur_files} | {cur_cats} | {covered_pairs} | {ignored_pairs} |")

    lines.append("")
    lines.append("## Season Year And Type Coverage")
    lines.append("")
    lines.append("| Sport | Raw years | Normalized years | Curated years | Raw-only years (not curated) | Raw season types | Normalized season types | Curated season types | Type gaps (raw not curated) |")
    lines.append("|---|---|---|---|---|---|---|---|---|")

    for sport in sorted(by_sport.keys()):
        sport_rows = by_sport[sport]
        raw_years: set[int] = set()
        raw_types: set[str] = set()
        for row in sport_rows:
            raw_years.update(row.raw_years)
            raw_types.update(row.raw_season_types)

        norm_years = normalized_years_by_sport.get(sport, set())
        cur_years = curated_years_by_sport.get(sport, set())
        norm_types = normalized_types_by_sport.get(sport, set())
        cur_types = curated_types_by_sport.get(sport, set())

        raw_only_years = raw_years - cur_years
        type_gaps = raw_types - cur_types

        lines.append(
            "| "
            f"{sport} | {format_years(raw_years)} | {format_years(norm_years)} | {format_years(cur_years)} | "
            f"{format_years(raw_only_years)} | {format_types(raw_types)} | {format_types(norm_types)} | "
            f"{format_types(cur_types)} | {format_types(type_gaps)} |"
        )

    lines.append("")
    lines.append("## Ignored Raw Volume")
    lines.append("")
    lines.append("| Sport | Vendor | Raw files | Reason |")
    lines.append("|---|---|---:|---|")
    for sport in sorted(by_sport.keys()):
        ignored_rows = [r for r in by_sport[sport] if r.status == "[ ] ignored" and r.raw_files > 0]
        for row in sorted(ignored_rows, key=lambda r: r.raw_files, reverse=True):
            lines.append(f"| {sport} | {row.vendor} | {row.raw_files} | {row.notes} |")
    lines.append("")

    lines.append("## Vendor Checklist")
    lines.append("")

    for sport in sorted(by_sport.keys()):
        lines.append(f"### {sport}")
        lines.append("")
        lines.append("| Status | Vendor | Raw files | Raw years | Raw season types | Normalized vendor hits | Curated vendor hits | Notes |")
        lines.append("|---|---|---:|---|---|---:|---:|---|")

        sport_rows = sorted(
            [r for r in by_sport[sport] if r.raw_files > 0],
            key=lambda r: (0 if r.status == "[ ] ignored" else 1, r.vendor),
        )
        if not sport_rows:
            lines.append("| - | (none) | 0 | - | - | 0 | 0 | No raw vendor files inferred for this sport |")
        else:
            for row in sport_rows:
                lines.append(
                    f"| {row.status} | {row.vendor} | {row.raw_files} | {format_years(row.raw_years)} | {format_types(row.raw_season_types)} | "
                    f"{row.normalized_vendor_hits} | {row.curated_vendor_hits} | {row.notes} |"
                )
        lines.append("")

    lines.append("## Raw Files Not Mapped To A Sport")
    lines.append("")
    lines.append("| Vendor | Unknown raw files |")
    lines.append("|---|---:|")
    for vendor in raw_vendors:
        lines.append(f"| {vendor} | {unknown_raw_files_by_vendor.get(vendor, 0)} |")

    out_path.write_text("\n".join(lines), encoding="utf-8")

    csv_path = root / args.csv_output
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "sport",
                "vendor",
                "status",
                "raw_files",
                "raw_years",
                "raw_season_types",
                "normalized_files",
                "curated_files",
                "curated_categories",
                "normalized_vendor_hits",
                "curated_vendor_hits",
                "notes",
            ]
        )
        for row in sorted(rows, key=lambda r: (r.sport, r.vendor)):
            writer.writerow(
                [
                    row.sport,
                    row.vendor,
                    row.status,
                    row.raw_files,
                    format_years(row.raw_years),
                    format_types(row.raw_season_types),
                    row.normalized_files,
                    row.curated_files,
                    row.curated_categories,
                    row.normalized_vendor_hits,
                    row.curated_vendor_hits,
                    row.notes,
                ]
            )
    print(f"wrote {out_path} and {csv_path} sports={len(by_sport)} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())