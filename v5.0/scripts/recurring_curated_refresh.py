#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from config import ALL_SPORTS, get_current_season, get_settings

curated_mod = importlib.import_module("normalization.curated_parquet_builder")
CuratedParquetBuilder = curated_mod.CuratedParquetBuilder

duckdb_catalog_mod = importlib.import_module("services.duckdb_catalog")
DuckDBCatalog = duckdb_catalog_mod.DuckDBCatalog
create_duckdb_connection = duckdb_catalog_mod.create_duckdb_connection


VOLATILE_CATEGORIES = {
    "news",
    "injuries",
    "odds",
    "live_scores",
    "scoreboard",
    "scores",
    "market_signals",
    "schedule_fatigue",
}


@dataclass
class RefreshSummary:
    sport: str
    seasons: list[str]
    categories: list[str]
    outputs: int
    rows: int
    partitions: int


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def discover_changed_files(normalized_root: Path, since: datetime | None) -> list[Path]:
    files = sorted(normalized_root.rglob("*.parquet"))
    if since is None:
        return files
    changed: list[Path] = []
    for f in files:
        try:
            modified = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
        except Exception:
            continue
        if modified >= since:
            changed.append(f)
    return changed


def parse_changed_scope(files: Iterable[Path], normalized_root: Path) -> dict[str, dict[str, set[str]]]:
    scope: dict[str, dict[str, set[str]]] = {}
    for file in files:
        try:
            rel = file.relative_to(normalized_root)
        except ValueError:
            continue
        if len(rel.parts) < 2:
            continue
        sport = rel.parts[0]
        stem = file.stem

        season = "all"
        kind = stem
        if "_" in stem:
            maybe_kind, maybe_season = stem.rsplit("_", 1)
            if maybe_season.isdigit():
                kind = maybe_kind
                season = maybe_season

        sport_scope = scope.setdefault(sport, {})
        season_scope = sport_scope.setdefault(season, set())
        season_scope.add(kind)
    return scope


def categories_for_mode(mode: str, kinds: set[str] | None) -> list[str] | None:
    if mode == "full":
        return None
    if mode == "volatile":
        return sorted(VOLATILE_CATEGORIES)

    # auto mode
    if not kinds:
        return sorted(VOLATILE_CATEGORIES)

    selected = set(VOLATILE_CATEGORIES)
    selected.update(kinds)
    return sorted(selected)


def run_refresh(
    mode: str,
    sports: list[str],
    lookback_hours: int,
    refresh_duckdb: bool,
    state_file: Path,
) -> list[RefreshSummary]:
    settings = get_settings()
    normalized_root = settings.normalized_dir

    state = load_state(state_file)
    last_success = _parse_iso(state.get("last_success_utc"))
    if mode == "auto" and last_success is None:
        last_success = _now_utc() - timedelta(hours=lookback_hours)

    changed_files = discover_changed_files(normalized_root, last_success if mode == "auto" else None)
    changed_scope = parse_changed_scope(changed_files, normalized_root)

    builder = CuratedParquetBuilder()
    summaries: list[RefreshSummary] = []

    target_sports = sports
    if mode == "auto":
        target_sports = sorted(set(target_sports) | set(changed_scope.keys()))

    for sport in target_sports:
        if mode == "full":
            seasons = builder.discover_seasons(sport)
            categories = None
            sport_results = builder.build_sport(sport=sport, seasons=seasons or None, categories=categories)
            summaries.append(
                RefreshSummary(
                    sport=sport,
                    seasons=seasons,
                    categories=["*"],
                    outputs=len(sport_results),
                    rows=sum(r.rows for r in sport_results),
                    partitions=sum(r.partitions for r in sport_results),
                )
            )
            continue

        sport_scope = changed_scope.get(sport, {})
        season_keys = sorted(k for k in sport_scope.keys() if k != "all")
        if not season_keys:
            season_keys = [get_current_season(sport)]

        total_outputs = 0
        total_rows = 0
        total_parts = 0
        categories_used: set[str] = set()

        for season in season_keys:
            kinds = sport_scope.get(season, set())
            categories = categories_for_mode(mode, kinds if mode == "auto" else None)
            categories_used.update(categories or ["*"])

            results = builder.build_sport(
                sport=sport,
                seasons=[season],
                categories=categories,
            )
            total_outputs += len(results)
            total_rows += sum(r.rows for r in results)
            total_parts += sum(r.partitions for r in results)

        # handle global kinds (no season suffix)
        global_kinds = sport_scope.get("all", set())
        if global_kinds:
            categories = categories_for_mode(mode, global_kinds if mode == "auto" else None)
            categories_used.update(categories or ["*"])
            results = builder.build_sport(
                sport=sport,
                seasons=None,
                categories=categories,
            )
            total_outputs += len(results)
            total_rows += sum(r.rows for r in results)
            total_parts += sum(r.partitions for r in results)

        summaries.append(
            RefreshSummary(
                sport=sport,
                seasons=season_keys,
                categories=sorted(categories_used),
                outputs=total_outputs,
                rows=total_rows,
                partitions=total_parts,
            )
        )

    if refresh_duckdb:
        conn = create_duckdb_connection(settings.duckdb_path)
        try:
            # Bulk refresh once per run.
            catalog = DuckDBCatalog(conn)
            catalog.refresh_all(target_sports)
        finally:
            conn.close()

    payload = {
        "last_success_utc": _now_utc().isoformat(),
        "mode": mode,
        "sports": target_sports,
        "changed_files": len(changed_files),
        "summaries": [asdict(s) for s in summaries],
    }
    save_state(state_file, payload)
    return summaries


def write_report(report_path: Path, mode: str, summaries: list[RefreshSummary]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Recurring Curated Refresh Report")
    lines.append("")
    lines.append(f"- mode: {mode}")
    lines.append(f"- generated_utc: {_now_utc().isoformat()}")
    lines.append("")
    lines.append("| Sport | Seasons | Categories | Outputs | Rows | Partitions |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for row in summaries:
        season_text = ",".join(row.seasons) if row.seasons else "(auto)"
        lines.append(
            f"| {row.sport} | {season_text} | {len(row.categories)} | {row.outputs} | {row.rows} | {row.partitions} |"
        )
    report_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recurring curated parquet + DuckDB bulk refresh")
    parser.add_argument("--mode", choices=["auto", "volatile", "full"], default="auto")
    parser.add_argument("--sports", default=",".join(ALL_SPORTS), help="Comma-separated sports")
    parser.add_argument("--lookback-hours", type=int, default=24, help="Initial lookback when no prior state exists")
    parser.add_argument("--skip-duckdb", action="store_true", help="Skip DuckDB bulk catalog refresh")
    parser.add_argument("--state-file", default="data/reports/curated_refresh_state.json")
    parser.add_argument("--report-file", default="data/reports/recurring_curated_refresh_report.md")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()]

    summaries = run_refresh(
        mode=args.mode,
        sports=sports,
        lookback_hours=args.lookback_hours,
        refresh_duckdb=not args.skip_duckdb,
        state_file=PROJECT_ROOT / args.state_file,
    )
    write_report(PROJECT_ROOT / args.report_file, args.mode, summaries)

    total_outputs = sum(s.outputs for s in summaries)
    total_rows = sum(s.rows for s in summaries)
    total_partitions = sum(s.partitions for s in summaries)
    logging.info(
        "recurring curated refresh complete mode=%s sports=%d outputs=%d rows=%d partitions=%d",
        args.mode,
        len(summaries),
        total_outputs,
        total_rows,
        total_partitions,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
