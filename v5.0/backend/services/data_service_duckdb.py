# ──────────────────────────────────────────────────────────
# V5.0 Backend — DuckDB Data Service Adapter
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import importlib
from pathlib import Path

import pandas as pd

from config import get_current_season
from services.data_service import DataService

logger = logging.getLogger(__name__)


class DataServiceDuckDB(DataService):
    """DuckDB-backed reader with transparent parquet fallback.

    This adapter mirrors the DataService API and overrides only storage reads.
    Non-enabled sports and any DuckDB read failures automatically fall back to
    the existing pandas/pyarrow parquet path.
    """

    def __init__(self) -> None:
        super().__init__()
        self._conn = None
        self._duckdb_ready = False

        enabled = (self._settings.duckdb_enabled_sports or "").strip()
        self._enabled_sports = {
            s.strip().lower() for s in enabled.split(",") if s.strip()
        }

        try:
            duckdb = importlib.import_module("duckdb")
            db_path = str(self._settings.duckdb_path) if self._settings.duckdb_path else ":memory:"
            self._conn = duckdb.connect(database=db_path)
            self._duckdb_ready = True
            logger.info("DuckDB reader enabled (db=%s)", db_path)
        except Exception as exc:
            logger.warning("DuckDB unavailable, falling back to parquet reader: %s", exc)
            self._duckdb_ready = False

    def clear_cache(self) -> None:
        super().clear_cache()
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            self._duckdb_ready = False

    def _duckdb_enabled_for_sport(self, sport: str) -> bool:
        if not self._duckdb_ready or self._conn is None:
            return False
        if not self._enabled_sports:
            return True
        return sport.lower() in self._enabled_sports

    def _load_kind(
        self,
        sport: str,
        kind: str,
        season: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        if not self._duckdb_enabled_for_sport(sport):
            return super()._load_kind(sport, kind, season=season, columns=columns)

        sport_dir = self._settings.normalized_dir / sport
        if not sport_dir.is_dir():
            return pd.DataFrame()

        try:
            if season == "all":
                pattern = sport_dir / f"{kind}_*.parquet"
                return self._query_parquet(pattern, columns=columns)

            target_season = season if season else get_current_season(sport)
            target = sport_dir / f"{kind}_{target_season}.parquet"
            if target.exists():
                return self._query_parquet(target, columns=columns)

            if not season:
                pattern = sport_dir / f"{kind}_*.parquet"
                return self._query_parquet(pattern, columns=columns)

            return pd.DataFrame()
        except Exception:
            logger.exception(
                "DuckDB read failed for sport=%s kind=%s season=%s; using parquet fallback",
                sport,
                kind,
                season,
            )
            return super()._load_kind(sport, kind, season=season, columns=columns)

    def _query_parquet(self, path_or_glob: Path, columns: list[str] | None = None) -> pd.DataFrame:
        if not self._duckdb_ready or self._conn is None:
            return pd.DataFrame()

        column_sql = "*"
        if columns:
            safe_cols = [c for c in columns if c and c.replace("_", "").isalnum()]
            if safe_cols:
                column_sql = ", ".join(f'"{c}"' for c in safe_cols)

        sql = f"SELECT {column_sql} FROM read_parquet(?, union_by_name=true)"
        return self._conn.execute(sql, [str(path_or_glob)]).df()
