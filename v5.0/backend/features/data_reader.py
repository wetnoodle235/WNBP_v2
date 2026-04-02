# ──────────────────────────────────────────────────────────
# V5.0 Backend — Curated Data Reader (DuckDB)
# ──────────────────────────────────────────────────────────
#
# Provides a fast DuckDB-backed reader over normalized_curated
# hive-partitioned parquets:
#
#   data/normalized_curated/{sport}/{category}/
#       season={YYYY}/[date={YYYY-MM-DD}/]part.parquet
#
# Falls back to legacy flat parquets in data/normalized/ when
# a curated category is not present.
#
# Usage:
#   reader = get_reader(data_dir)
#   df = reader.load("nba", "games", season=2026)
#   df = reader.load("nba", "games")           # all seasons
#
# The module holds ONE in-memory DuckDB connection per process
# (thread-local, read-only).  Multiple BaseFeatureExtractor
# instances share the same reader.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ── Thread-local singleton ────────────────────────────────
_local = threading.local()


def get_reader(data_dir: Path) -> "CuratedDataReader":
    """Return (or lazily create) a per-thread CuratedDataReader."""
    key = str(data_dir)
    readers: dict[str, "CuratedDataReader"] = getattr(_local, "readers", None) or {}
    if key not in readers:
        readers[key] = CuratedDataReader(data_dir)
        _local.readers = readers
    return readers[key]


# ── Helpers ───────────────────────────────────────────────

def _safe_view(sport: str, category: str) -> str:
    """Sanitize to a valid DuckDB view/alias name."""
    return f"{sport}_{category}".replace("/", "_").replace("-", "_")


class CuratedDataReader:
    """DuckDB-backed reader over normalized_curated hive-partitioned parquets.

    All reads are via in-memory DuckDB (no shared file lock).
    Falls back to legacy normalized/ flat parquets when curated
    data is absent for a given sport+category.
    """

    # Categories whose legacy path uses a different filename stem
    _LEGACY_ALIAS: dict[str, str] = {
        "player_stats": "player_stats",
        "team_stats": "team_stats",
        "schedule_fatigue": "schedule_fatigue",
        "market_signals": "market_signals",
        "injuries": "injuries",
        "standings": "standings",
        "odds": "odds",
        "games": "games",
        "players": "players",
        "teams": "teams",
    }

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.curated_dir = self.data_dir / "normalized_curated"
        self.legacy_dir = self.data_dir / "normalized"
        self._conn: Any = None
        self._init()

    def _init(self) -> None:
        try:
            import duckdb  # noqa: PLC0415
            self._conn = duckdb.connect(database=":memory:")
            logger.info("CuratedDataReader: in-memory DuckDB initialized")
        except ImportError:
            logger.warning("duckdb not installed — falling back to pandas parquet reader")
            self._conn = None

    # ── Public API ────────────────────────────────────────

    def load(
        self,
        sport: str,
        category: str,
        season: int | str | None = None,
        extra_where: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load sport+category data from curated parquets (DuckDB) or legacy fallback.

        Args:
            sport:       Sport key (e.g. "nba", "epl", "nhl").
            category:    Data category (e.g. "games", "player_stats", "standings").
            season:      Season year as int/str.  None = all available seasons.
            extra_where: Additional SQL WHERE clause fragment (DuckDB only).
            columns:     Optional column projection.

        Returns:
            pandas DataFrame (empty if data unavailable).
        """
        curated = self.curated_dir / sport / category
        if curated.exists():
            df = self._load_curated(sport, category, season=season,
                                    extra_where=extra_where, columns=columns)
            if df is not None and not df.empty:
                return df
            # Fall through to legacy if curated returned nothing
        return self._load_legacy(sport, category, season=season, columns=columns)

    def load_all_seasons(
        self,
        sport: str,
        category: str,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load all available seasons for sport+category (curated-first)."""
        return self.load(sport, category, season=None, columns=columns)

    def available_seasons(self, sport: str, category: str) -> list[int]:
        """Return sorted list of available seasons for a curated sport+category."""
        curated = self.curated_dir / sport / category
        if not curated.exists():
            # Fall back to legacy filename discovery
            return self._legacy_seasons(sport, category)
        seasons: list[int] = []
        for p in curated.iterdir():
            if p.is_dir() and p.name.startswith("season="):
                try:
                    seasons.append(int(p.name.split("=")[1]))
                except ValueError:
                    pass
        return sorted(seasons)

    def has_category(self, sport: str, category: str) -> bool:
        """True if curated OR legacy data exists for sport+category."""
        if (self.curated_dir / sport / category).exists():
            return True
        legacy = self.legacy_dir / sport
        if not legacy.exists():
            return False
        stem = self._LEGACY_ALIAS.get(category, category)
        return bool(list(legacy.glob(f"{stem}_*.parquet")) or (legacy / f"{stem}.parquet").exists())

    # ── Internal: curated DuckDB reads ───────────────────

    def _load_curated(
        self,
        sport: str,
        category: str,
        season: int | str | None,
        extra_where: str | None,
        columns: list[str] | None,
    ) -> pd.DataFrame | None:
        """Query curated hive-partitioned parquets via DuckDB."""
        curated_path = self.curated_dir / sport / category
        glob_pattern = str(curated_path / "**" / "*.parquet")

        if self._conn is not None:
            return self._duckdb_query(glob_pattern, season=season,
                                      extra_where=extra_where, columns=columns)
        # DuckDB unavailable — use pandas glob read
        return self._pandas_curated_read(curated_path, season=season)

    def _duckdb_query(
        self,
        glob_pattern: str,
        season: int | str | None,
        extra_where: str | None,
        columns: list[str] | None,
    ) -> pd.DataFrame:
        """Execute a DuckDB SELECT over a glob of hive-partitioned parquets."""
        try:
            col_sql = "*"
            if columns:
                safe = [c for c in columns if c.replace("_", "").isalnum()]
                if safe:
                    col_sql = ", ".join(f'"{c}"' for c in safe)

            where_parts: list[str] = []
            params: list[Any] = []

            if season is not None and str(season) != "all":
                where_parts.append("CAST(season AS VARCHAR) = ?")
                params.append(str(season))
            if extra_where:
                where_parts.append(extra_where)

            from_clause = (
                f"read_parquet(?, union_by_name=true, hive_partitioning=true)"
            )
            sql = f"SELECT {col_sql} FROM {from_clause}"
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)

            return self._conn.execute(sql, [glob_pattern] + params).df()
        except Exception as exc:
            logger.debug("DuckDB query failed (%s): %s", glob_pattern, exc)
            return pd.DataFrame()

    def _pandas_curated_read(
        self,
        curated_path: Path,
        season: int | str | None,
    ) -> pd.DataFrame:
        """Pandas fallback for curated parquets (no DuckDB)."""
        import glob as glob_mod  # noqa: PLC0415

        if season is not None and str(season) != "all":
            pattern = str(curated_path / f"season={season}" / "**" / "*.parquet")
        else:
            pattern = str(curated_path / "**" / "*.parquet")

        files = glob_mod.glob(pattern, recursive=True)
        if not files:
            return pd.DataFrame()
        frames = []
        for f in files:
            try:
                frames.append(pd.read_parquet(f))
            except Exception:
                pass
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        # Inject season from directory name if column missing
        if "season" not in df.columns and season is not None:
            df["season"] = int(season)
        return df

    # ── Internal: legacy flat-parquet reads ──────────────

    def _load_legacy(
        self,
        sport: str,
        category: str,
        season: int | str | None,
        columns: list[str] | None,
    ) -> pd.DataFrame:
        """Read from legacy data/normalized/{sport}/{category}_{season}.parquet."""
        sport_dir = self.legacy_dir / sport
        if not sport_dir.exists():
            return pd.DataFrame()

        stem = self._LEGACY_ALIAS.get(category, category)

        if season is not None and str(season) != "all":
            path = sport_dir / f"{stem}_{season}.parquet"
            if not path.exists():
                bare = sport_dir / f"{stem}.parquet"
                path = bare if bare.exists() else None
            if path is None:
                logger.debug("Legacy parquet not found: %s/%s season %s", sport, category, season)
                return pd.DataFrame()
            try:
                df = pd.read_parquet(path, columns=columns)
                return df
            except Exception as exc:
                logger.warning("Legacy read failed %s: %s", path, exc)
                return pd.DataFrame()

        # All seasons
        import glob as glob_mod  # noqa: PLC0415
        files = sorted(glob_mod.glob(str(sport_dir / f"{stem}_*.parquet")))
        bare = sport_dir / f"{stem}.parquet"
        if bare.exists() and str(bare) not in files:
            files.append(str(bare))
        if not files:
            return pd.DataFrame()
        frames = []
        for f in files:
            try:
                frames.append(pd.read_parquet(f, columns=columns))
            except Exception:
                pass
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _legacy_seasons(self, sport: str, category: str) -> list[int]:
        """Discover seasons from legacy parquet filenames."""
        import re  # noqa: PLC0415
        sport_dir = self.legacy_dir / sport
        if not sport_dir.exists():
            return []
        stem = self._LEGACY_ALIAS.get(category, category)
        seasons: list[int] = []
        for p in sport_dir.glob(f"{stem}_*.parquet"):
            m = re.search(r"_(\d{4})\.parquet$", p.name)
            if m:
                seasons.append(int(m.group(1)))
        return sorted(seasons)

    def close(self) -> None:
        """Release DuckDB connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
