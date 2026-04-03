# ──────────────────────────────────────────────────────────
# V5.0 Backend — DuckDB Data Service Adapter
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import importlib
from pathlib import Path

import pandas as pd

from config import SPORT_DEFINITIONS
from config import get_current_season
from services.data_service import DataService, _ALL_KINDS

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

            # Bootstrap: create/refresh views in write mode, then switch to
            # read-only so the sync pipeline can acquire the write lock freely.
            if self._settings.duckdb_use_curated and db_path != ":memory:":
                try:
                    write_conn = duckdb.connect(database=db_path)
                    duckdb_catalog_mod = importlib.import_module("services.duckdb_catalog")
                    catalog = duckdb_catalog_mod.DuckDBCatalog(write_conn)
                    if self._enabled_sports:
                        catalog.refresh_all(sorted(self._enabled_sports))
                    else:
                        sport_dirs = [d.name for d in self._settings.normalized_curated_dir.glob("*") if d.is_dir()]
                        catalog.refresh_all(sorted(sport_dirs))
                    write_conn.close()
                    logger.info("DuckDB curated catalog bootstrapped (write mode)")
                except Exception as bootstrap_exc:
                    logger.warning("DuckDB catalog bootstrap failed: %s", bootstrap_exc)

            # Open in read-only mode — no write lock held, allows concurrent
            # reads from API + ML while the sync pipeline writes freely.
            self._conn = duckdb.connect(database=db_path, read_only=(db_path != ":memory:"))
            self._duckdb_ready = True
            logger.info("DuckDB reader enabled (db=%s, read_only=%s)", db_path, db_path != ":memory:")
        except Exception as exc:
            logger.warning("DuckDB unavailable, falling back to parquet reader: %s", exc)
            self._duckdb_ready = False

    def reconnect(self) -> None:
        """Reconnect in read-only mode to pick up new views from the sync pipeline."""
        if self._conn is None:
            return
        try:
            duckdb = importlib.import_module("duckdb")
            db_path = str(self._settings.duckdb_path) if self._settings.duckdb_path else ":memory:"
            old = self._conn
            self._conn = duckdb.connect(database=db_path, read_only=(db_path != ":memory:"))
            old.close()
            logger.info("DuckDB reconnected (read_only) — new views visible")
        except Exception as exc:
            logger.warning("DuckDB reconnect failed: %s", exc)

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
        if self._settings.duckdb_use_curated:
            try:
                return self._load_kind_from_curated_views(sport, kind, season=season, columns=columns)
            except Exception:
                logger.exception(
                    "Curated DuckDB read failed for sport=%s kind=%s season=%s; using parquet fallback",
                    sport,
                    kind,
                    season,
                )
                return super()._load_kind(sport, kind, season=season, columns=columns)

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

    def _load_kind_from_curated_views(
        self,
        sport: str,
        kind: str,
        season: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        if not self._duckdb_enabled_for_sport(sport):
            return super()._load_kind(sport, kind, season=season, columns=columns)

        base_view = self._view_for_kind(sport, kind)
        if base_view is None:
            return super()._load_kind(sport, kind, season=season, columns=columns)

        # Special-case player_stats from the umbrella stats view when direct
        # player_stats category is not available.
        if kind == "player_stats" and not self._view_exists(base_view):
            stats_view = f"{sport}_stats"
            if self._view_exists(stats_view):
                return self._query_view(
                    stats_view,
                    season=season,
                    columns=columns,
                    extra_where="stats_kind = 'player_stats'",
                )
            return super()._load_kind(sport, kind, season=season, columns=columns)

        # Prefer latest-odds aggregate snapshot when available.
        if kind == "odds":
            latest_odds_view = f"{sport}_market_odds_latest"
            if self._view_exists(latest_odds_view):
                return self._query_view(latest_odds_view, season=season, columns=columns)

        # Prefer team-season rollup aggregate for team stats when available.
        if kind == "team_stats":
            team_rollup_view = f"{sport}_team_season_rollup"
            if self._view_exists(team_rollup_view):
                return self._query_view(team_rollup_view, season=season, columns=columns)

        if not self._view_exists(base_view):
            return super()._load_kind(sport, kind, season=season, columns=columns)
        
        return self._query_view(base_view, season=season, columns=columns)

    def get_player_stats(
        self,
        sport: str,
        season: str | None = None,
        player_id: str | None = None,
        aggregate: bool = False,
    ) -> list[dict]:
        # Non-aggregate path stays exactly as before.
        if not aggregate:
            return super().get_player_stats(sport, season=season, player_id=player_id, aggregate=aggregate)

        key = f"player_stats_agg_duckdb:{sport}:{season}:{player_id}"
        ttl = self._settings.cache_ttl_stats

        def loader() -> list[dict]:
            if not self._duckdb_enabled_for_sport(sport):
                return super(DataServiceDuckDB, self).get_player_stats(sport, season=season, player_id=player_id, aggregate=aggregate)

            # Prefer already-aggregated curated views to avoid pandas groupby overhead.
            candidate_views: list[tuple[str, str | None]] = [
                (f"{sport}_player_season_rollup", None),
                (f"{sport}_player_season_averages", None),
                (f"{sport}_season_averages_player", None),
                (f"{sport}_all_season_averages", "season_avg_scope = 'player'"),
                (f"{sport}_season_averages", "season_avg_scope = 'player'"),
            ]

            for view_name, extra_where in candidate_views:
                if not self._view_exists(view_name):
                    continue
                try:
                    df = self._query_view(view_name, season=season, columns=None, extra_where=extra_where)
                except Exception:
                    logger.debug("Skipping invalid aggregate candidate view: %s", view_name)
                    continue
                if df.empty:
                    continue
                if player_id and "player_id" in df.columns:
                    df = df[df["player_id"].astype(str) == player_id]
                return self._df_to_records(df)

            return super(DataServiceDuckDB, self).get_player_stats(sport, season=season, player_id=player_id, aggregate=aggregate)

        return self._get_cached(key, ttl, loader)

    # ── Cross-sport aggregate query ───────────────────────

    def query_cross_sport(
        self,
        kind: str,
        sports: list[str],
        *,
        date_filter: str | None = None,
        limit_per_sport: int | None = None,
    ) -> list[dict]:
        """Single DuckDB UNION ALL query against a pre-built cross-sport view.

        Falls back to the base Python-loop implementation when:
        - DuckDB is not ready or ``duckdb_use_curated`` is disabled
        - The ``_all_{kind}`` view was not built by the catalog at startup
        - The DuckDB query itself fails for any reason
        """
        if not self._duckdb_ready or not self._settings.duckdb_use_curated or self._conn is None:
            return super().query_cross_sport(kind, sports, date_filter=date_filter, limit_per_sport=limit_per_sport)

        all_view = f"_all_{kind}"
        if not self._view_exists(all_view):
            return super().query_cross_sport(kind, sports, date_filter=date_filter, limit_per_sport=limit_per_sport)

        try:
            return self._exec_cross_sport_query(
                all_view, sports, date_filter=date_filter, limit_per_sport=limit_per_sport
            )
        except Exception:
            logger.exception("query_cross_sport DuckDB failed for kind=%s; falling back to Python loop", kind)
            return super().query_cross_sport(kind, sports, date_filter=date_filter, limit_per_sport=limit_per_sport)

    def _exec_cross_sport_query(
        self,
        all_view: str,
        sports: list[str],
        *,
        date_filter: str | None,
        limit_per_sport: int | None,
    ) -> list[dict]:
        """Build and execute a parameterised DuckDB query against a cross-sport view."""
        # Introspect the merged view schema once so we can build safe WHERE clauses.
        try:
            info_rows = self._conn.execute(f"PRAGMA table_info('{all_view}')").fetchall()
            available_cols: set[str] = {str(r[1]) for r in info_rows}
        except Exception:
            available_cols = set()

        where_parts: list[str] = []
        params: list[object] = []

        # Sport filter
        if sports:
            placeholders = ", ".join("?" * len(sports))
            where_parts.append(f"sport IN ({placeholders})")
            params.extend(sports)

        # Date filter — UNION ALL BY NAME ensures all potential date columns exist
        # (as NULL where a sport doesn't use that column name).
        if date_filter:
            date_cols = [
                c for c in ("date", "start_date", "game_date", "start_time")
                if c in available_cols
            ]
            if date_cols:
                date_conds = " OR ".join(f'CAST("{c}" AS VARCHAR) LIKE ?' for c in date_cols)
                where_parts.append(f"({date_conds})")
                params.extend(f"{date_filter}%" for _ in date_cols)

        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        if limit_per_sport and "sport" in available_cols:
            # Use a window ROW_NUMBER to apply a per-sport row cap in a single pass.
            sql = (
                f"SELECT * EXCLUDE (_rn) FROM ("
                f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY sport) AS _rn"
                f"  FROM {all_view}{where_sql}"
                f") WHERE _rn <= ?"
            )
            params.append(limit_per_sport)
        else:
            sql = f"SELECT * FROM {all_view}{where_sql}"

        df = self._conn.execute(sql, list(params)).df()
        return self._df_to_records(df)

    @staticmethod
    def _view_for_kind(sport: str, kind: str) -> str | None:
        kind_to_view = {
            "games": f"{sport}_games",
            "game_stats": f"{sport}_game_stats",
            "player_game_stats": f"{sport}_player_game_stats",
            "teams": f"{sport}_teams",
            "players": f"{sport}_players",
            "lineups": f"{sport}_lineups",
            "racers": f"{sport}_players",
            "drivers": f"{sport}_players",
            "fighters": f"{sport}_players",
            "standings": f"{sport}_standings",
            "player_stats": f"{sport}_player_stats",
            "racer_stats": f"{sport}_player_stats",
            "driver_stats": f"{sport}_player_stats",
            "fighter_stats": f"{sport}_player_stats",
            "goalie_stats": f"{sport}_goalie_stats",
            "skater_stats": f"{sport}_skater_stats",
            "plays": f"{sport}_plays",
            "play_stats": f"{sport}_play_stats",
            "drives": f"{sport}_drives",
            "circuits": f"{sport}_circuits",
            "constructor_stats": f"{sport}_constructor_stats",
            "pitcher_vs_batter": f"{sport}_pitcher_vs_batter",
            "batter_vs_pitcher": f"{sport}_batter_vs_pitcher",
            "team_stats": f"{sport}_team_stats",
            "team_game_stats": f"{sport}_team_game_stats",
            "batter_game_stats": f"{sport}_batter_game_stats",
            "pitcher_game_stats": f"{sport}_pitcher_game_stats",
            "advanced_stats": f"{sport}_advanced_stats",
            "advanced_batting": f"{sport}_advanced_batting",
            "match_events": f"{sport}_match_events",
            "play_by_play": f"{sport}_play_by_play",
            "ratings": f"{sport}_ratings",
            "coaches": f"{sport}_staff_coaches",
            "draft": f"{sport}_draft_all",
            "draft_picks": f"{sport}_draft_picks",
            "draft_positions": f"{sport}_draft_positions",
            "draft_teams": f"{sport}_draft_teams",
            "player_portal": f"{sport}_players_categories_portal",
            "player_returning": f"{sport}_players_categories_returning",
            "player_usage": f"{sport}_players_categories_usage",
            "all_stats": f"{sport}_all_stats",
            "season_averages_all": f"{sport}_all_season_averages",
            "all_season_averages": f"{sport}_all_season_averages",
            "odds": f"{sport}_odds",
            "odds_history": f"{sport}_odds_history",
            "odds_all": f"{sport}_odds_all",
            "injuries": f"{sport}_injuries",
            "news": f"{sport}_news",
            "transactions": f"{sport}_transactions",
            "weather": f"{sport}_weather",
            "market_signals": f"{sport}_market_signals",
            "market_history": f"{sport}_market_history",
            "schedule_fatigue": f"{sport}_schedule_fatigue",
            "player_props": f"{sport}_player_props",
            "player_props_history": f"{sport}_player_props_history",
            "player_props_all": f"{sport}_player_props_all",
            "predictions": f"{sport}_predictions",
        }
        return kind_to_view.get(kind)

    def list_available_sports(self) -> list[dict]:
        if not self._duckdb_ready or self._conn is None:
            return super().list_available_sports()

        if not self._settings.duckdb_use_curated:
            return super().list_available_sports()

        tracked_kinds = set(_ALL_KINDS) | {
            "advanced_stats",
            "advanced_batting",
            "match_events",
            "ratings",
            "transactions",
            "weather",
        }

        results: list[dict] = []
        for sport, defn in sorted(SPORT_DEFINITIONS.items()):
            kinds: dict[str, int] = {}
            for kind in tracked_kinds:
                view_name = self._view_for_kind(sport, kind)
                if view_name and self._view_exists(view_name):
                    kinds[kind] = 1
            if kinds:
                results.append(
                    {
                        "key": sport,
                        **defn,
                        "data_types": kinds,
                        "file_count": sum(kinds.values()),
                    }
                )
        return results

    def get_seasons(self, sport: str, kind: str = "games") -> list[str]:
        if not self._duckdb_ready or self._conn is None or not self._settings.duckdb_use_curated:
            return super().get_seasons(sport, kind=kind)

        view_name = self._view_for_kind(sport, kind)
        if not view_name or not self._view_exists(view_name):
            return super().get_seasons(sport, kind=kind)

        try:
            info_rows = self._conn.execute(f"PRAGMA table_info('{view_name}')").fetchall()
            cols = {str(r[1]) for r in info_rows}
            if "season" not in cols:
                return []

            rows = self._conn.execute(
                f"SELECT DISTINCT CAST(season AS VARCHAR) AS season FROM {view_name} WHERE season IS NOT NULL"
            ).fetchall()
            seasons = sorted({str(r[0]) for r in rows if r and r[0] is not None and str(r[0]).strip()})
            return seasons
        except Exception:
            logger.exception("Failed to derive seasons from view %s", view_name)
            return super().get_seasons(sport, kind=kind)

    def _view_exists(self, name: str) -> bool:
        """Return True if *name* is a SQL view or base table in the DuckDB catalog."""
        if not self._duckdb_ready or self._conn is None:
            return False
        try:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
                [name],
            ).fetchone()
            return bool(row and row[0] > 0)
        except Exception:
            return False

    def _query_view(
        self,
        view_name: str,
        *,
        season: str | None,
        columns: list[str] | None,
        extra_where: str | None = None,
    ) -> pd.DataFrame:
        if not self._duckdb_ready or self._conn is None:
            return pd.DataFrame()

        column_sql = "*"
        available_cols: set[str] = set()
        try:
            info_rows = self._conn.execute(f"PRAGMA table_info('{view_name}')").fetchall()
            available_cols = {str(r[1]) for r in info_rows}
        except Exception:
            available_cols = set()

        if columns:
            safe_cols = [c for c in columns if c and c.replace("_", "").isalnum()]
            safe_cols = [c for c in safe_cols if not available_cols or c in available_cols]
            if safe_cols:
                column_sql = ", ".join(f'"{c}"' for c in safe_cols)

        where_parts: list[str] = []
        params: list[object] = []

        if season not in (None, "all") and "season" in available_cols:
            where_parts.append("CAST(season AS VARCHAR) = ?")
            params.append(str(season))
        if extra_where:
            where_parts.append(extra_where)

        sql = f"SELECT {column_sql} FROM {view_name}"
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)

        return self._conn.execute(sql, params).df()

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
