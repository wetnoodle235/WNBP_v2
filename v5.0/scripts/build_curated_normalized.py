#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from config import ALL_SPORTS, get_settings

curated_mod = importlib.import_module("normalization.curated_parquet_builder")
BASE_CATEGORIES = curated_mod.BASE_CATEGORIES
CuratedParquetBuilder = curated_mod.CuratedParquetBuilder

duckdb_catalog_mod = importlib.import_module("services.duckdb_catalog")
DuckDBCatalog = duckdb_catalog_mod.DuckDBCatalog
create_duckdb_connection = duckdb_catalog_mod.create_duckdb_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build curated normalized parquets and optional DuckDB joined views.",
    )
    parser.add_argument("--sports", default=",".join(ALL_SPORTS), help="Comma-separated sports list")
    parser.add_argument("--seasons", default="", help="Comma-separated seasons; empty = auto-discover per sport")
    parser.add_argument(
        "--categories",
        default="",
        help="Comma-separated categories. Empty uses sport defaults (base categories + sport-specific overrides).",
    )
    parser.add_argument("--build-duckdb", action="store_true", help="Refresh DuckDB catalog/views after parquet build")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()]
    seasons = [s.strip() for s in args.seasons.split(",") if s.strip()]
    categories = [c.strip() for c in args.categories.split(",") if c.strip()]

    builder = CuratedParquetBuilder()
    total_rows = 0
    total_partitions = 0
    total_outputs = 0

    for sport in sports:
        results = builder.build_sport(
            sport=sport,
            seasons=seasons or None,
            categories=categories or None,
        )
        for res in results:
            logging.info(
                "curated build sport=%s season=%s category=%s rows=%d partitions=%d root=%s",
                res.sport,
                res.season,
                res.category,
                res.rows,
                res.partitions,
                res.output_root,
            )
            total_rows += res.rows
            total_partitions += res.partitions
            total_outputs += 1

    logging.info(
        "curated build complete outputs=%d rows=%d partitions=%d",
        total_outputs,
        total_rows,
        total_partitions,
    )

    if args.build_duckdb:
        settings = get_settings()
        try:
            conn = create_duckdb_connection(settings.duckdb_path)
        except ModuleNotFoundError:
            logging.warning(
                "duckdb package is not installed; curated parquet build completed but catalog refresh was skipped"
            )
            return 0
        try:
            catalog = DuckDBCatalog(conn)
            catalog.refresh_all(sports)
            logging.info("duckdb catalog refresh complete sports=%s", ",".join(sports))
        finally:
            conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
