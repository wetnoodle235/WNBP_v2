# ──────────────────────────────────────────────────────────
# V5.0 Backend — DuckDB Catalog over Curated Parquets
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import importlib
import logging
import re
from pathlib import Path

from config import get_settings

logger = logging.getLogger(__name__)


class DuckDBCatalog:
    """Registers curated parquet categories and joined views in DuckDB.

    Views created per sport:
    - <sport>_teams
    - <sport>_conferences
    - <sport>_games
    - <sport>_stats
    - <sport>_season_averages
    - <sport>_coaches
    - <sport>_referees
    - <sport>_game_core (joined)
    - <sport>_ml_dataset (games + stats join)
    """

    def __init__(self, conn) -> None:
        settings = get_settings()
        self._conn = conn
        self._curated_dir = settings.normalized_curated_dir

    def refresh_sport(self, sport: str) -> None:
        sport_dir = self._curated_dir / sport
        if not sport_dir.exists():
            return

        categories = self._discover_categories(sport_dir)
        if not categories:
            return

        for category in categories:
            self._create_category_view(sport, category)

        self._create_consolidated_views(sport, categories)
        self._create_compatibility_alias_views(sport)
        self._create_joined_views(sport)

    def refresh_all(self, sports: list[str]) -> None:
        for sport in sports:
            try:
                self.refresh_sport(sport)
            except Exception:
                logger.exception("Failed refreshing DuckDB catalog for %s", sport)

    def _create_category_view(self, sport: str, category: str) -> None:
        path_glob = self._curated_dir / sport / Path(*category.split("/")) / "**" / "*.parquet"
        view_name = self._view_name_for_category(sport, category)
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * FROM read_parquet('{path_glob.as_posix()}', union_by_name=true)"
        )
        try:
            self._conn.execute(sql)
        except Exception:
            # Missing category parquets is expected for some sports.
            self._conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")

    def _discover_categories(self, sport_dir: Path) -> list[str]:
        categories: set[str] = set()
        for season_dir in sport_dir.rglob("season=*"):
            if not season_dir.is_dir():
                continue
            try:
                rel = season_dir.parent.relative_to(sport_dir)
            except ValueError:
                continue
            if rel.parts:
                categories.add("/".join(rel.parts))
        return sorted(categories)

    @staticmethod
    def _view_name_for_category(sport: str, category: str) -> str:
        normalized = category.replace("/", "_")
        normalized = re.sub(r"[^A-Za-z0-9_]", "_", normalized)
        return f"{sport}_{normalized}"

    def _has_view(self, view_name: str) -> bool:
        try:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM information_schema.views WHERE table_name = ?",
                [view_name],
            ).fetchone()
            return bool(row and row[0] > 0)
        except Exception:
            return False

    def _has_columns(self, view_name: str, columns: set[str]) -> bool:
        try:
            rows = self._conn.execute(f"PRAGMA table_info('{view_name}')").fetchall()
            names = {str(r[1]) for r in rows}
            return columns.issubset(names)
        except Exception:
            return False

    def _create_joined_views(self, sport: str) -> None:
        games_view = f"{sport}_games"
        teams_view = f"{sport}_teams"
        stats_view = f"{sport}_stats"
        refs_view = f"{sport}_referees"
        coaches_view = f"{sport}_coaches"

        if not self._has_view(games_view):
            return

        has_team_ids = self._has_columns(games_view, {"home_team_id", "away_team_id"})
        has_team_key = self._has_columns(teams_view, {"team_id"}) or self._has_columns(teams_view, {"id"})
        team_id_col = "team_id" if self._has_columns(teams_view, {"team_id"}) else "id"
        team_name_col = "name" if self._has_columns(teams_view, {"name"}) else "team_name"

        if has_team_ids and has_team_key:
            game_core_sql = f"""
            CREATE OR REPLACE VIEW {sport}_game_core AS
            SELECT
                g.*,
                ht.{team_name_col} AS home_team_name,
                aw.{team_name_col} AS away_team_name
            FROM {games_view} g
            LEFT JOIN {teams_view} ht ON CAST(g.home_team_id AS VARCHAR) = CAST(ht.{team_id_col} AS VARCHAR)
            LEFT JOIN {teams_view} aw ON CAST(g.away_team_id AS VARCHAR) = CAST(aw.{team_id_col} AS VARCHAR)
            """
        else:
            game_core_sql = f"CREATE OR REPLACE VIEW {sport}_game_core AS SELECT * FROM {games_view}"

        self._conn.execute(game_core_sql)

        game_id_col = "game_id" if self._has_columns(games_view, {"game_id"}) else "id"
        stats_game_col = "game_id" if self._has_columns(stats_view, {"game_id"}) else "id"

        if self._has_view(stats_view) and self._has_columns(stats_view, {stats_game_col}) and self._has_columns(games_view, {game_id_col}):
            ml_sql = f"""
            CREATE OR REPLACE VIEW {sport}_ml_dataset AS
            SELECT
                g.*,
                s.* EXCLUDE ({stats_game_col})
            FROM {sport}_game_core g
            LEFT JOIN {stats_view} s
              ON CAST(g.{game_id_col} AS VARCHAR) = CAST(s.{stats_game_col} AS VARCHAR)
            """
        else:
            ml_sql = f"CREATE OR REPLACE VIEW {sport}_ml_dataset AS SELECT * FROM {sport}_game_core"

        self._conn.execute(ml_sql)

        # Accessor views that simplify API and training reads.
        self._conn.execute(f"CREATE OR REPLACE VIEW {sport}_rest_dataset AS SELECT * FROM {sport}_game_core")

        if self._has_view(refs_view):
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {sport}_game_officials AS "
                f"SELECT g.*, r.* EXCLUDE (game_id) FROM {sport}_game_core g "
                f"LEFT JOIN {refs_view} r ON CAST(g.{game_id_col} AS VARCHAR)=CAST(r.game_id AS VARCHAR)"
            )

        if self._has_view(coaches_view):
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {sport}_game_coaches AS "
                f"SELECT g.*, c.* EXCLUDE (game_id) FROM {sport}_game_core g "
                f"LEFT JOIN {coaches_view} c ON CAST(g.{game_id_col} AS VARCHAR)=CAST(c.game_id AS VARCHAR)"
            )

    def _create_compatibility_alias_views(self, sport: str) -> None:
        """Create legacy-named views that point at grouped storage paths."""
        alias_map = {
            f"{sport}_player_season_averages": f"{sport}_season_averages_player",
            f"{sport}_team_season_averages": f"{sport}_season_averages_team",
            f"{sport}_standings_season_averages": f"{sport}_season_averages_standings",
            f"{sport}_team_game_stats": f"{sport}_game_stats_team_game_stats",
            f"{sport}_player_stats": f"{sport}_game_stats_player_stats",
            f"{sport}_batter_game_stats": f"{sport}_game_stats_batter_game_stats",
            f"{sport}_pitcher_game_stats": f"{sport}_game_stats_pitcher_game_stats",
            f"{sport}_goalie_stats": f"{sport}_game_stats_goalie_stats",
            f"{sport}_skater_stats": f"{sport}_game_stats_skater_stats",
        }
        for alias_name, source_name in alias_map.items():
            if not self._has_view(source_name):
                continue
            self._conn.execute(f"CREATE OR REPLACE VIEW {alias_name} AS SELECT * FROM {source_name}")

        # Fallback aliases from consolidated view for sports that don't emit
        # dedicated per-variant categories.
        consolidated = f"{sport}_game_stats"
        if self._has_view(consolidated) and self._has_columns(consolidated, {"stats_scope"}):
            if not self._has_view(f"{sport}_team_game_stats"):
                self._conn.execute(
                    f"CREATE OR REPLACE VIEW {sport}_team_game_stats AS "
                    f"SELECT * FROM {consolidated} WHERE stats_scope = 'team'"
                )
            if not self._has_view(f"{sport}_player_stats"):
                self._conn.execute(
                    f"CREATE OR REPLACE VIEW {sport}_player_stats AS "
                    f"SELECT * FROM {consolidated} WHERE stats_scope = 'player'"
                )

    def _create_consolidated_views(self, sport: str, categories: list[str]) -> None:
        """Create optional high-level views that combine closely-related categories.

        These views reduce query clutter while preserving original category views.
        """
        category_set = set(categories)

        game_stats_categories = [
            c
            for c in [
                "team_game_stats",
                "game_stats/team_game_stats",
                "player_stats",
                "game_stats/player_stats",
                "batter_game_stats",
                "game_stats/batter_game_stats",
                "pitcher_game_stats",
                "game_stats/pitcher_game_stats",
                "goalie_stats",
                "game_stats/goalie_stats",
                "skater_stats",
                "game_stats/skater_stats",
            ]
            if c in category_set
        ]
        self._create_union_view(
            view_name=f"{sport}_game_stats",
            sport=sport,
            categories=game_stats_categories,
            extra_select=(
                "CASE "
                "WHEN raw_category = 'team_game_stats' THEN 'team' "
                "WHEN raw_category = 'game_stats/team_game_stats' THEN 'team' "
                "WHEN raw_category IN ('pitcher_game_stats') THEN 'pitcher' "
                "WHEN raw_category IN ('game_stats/pitcher_game_stats') THEN 'pitcher' "
                "WHEN raw_category IN ('batter_game_stats') THEN 'batter' "
                "WHEN raw_category IN ('game_stats/batter_game_stats') THEN 'batter' "
                "ELSE 'player' END AS stats_scope"
            ),
        )

        player_game_categories = [
            c
            for c in [
                "player_stats",
                "game_stats/player_stats",
                "batter_game_stats",
                "game_stats/batter_game_stats",
                "pitcher_game_stats",
                "game_stats/pitcher_game_stats",
                "goalie_stats",
                "game_stats/goalie_stats",
                "skater_stats",
                "game_stats/skater_stats",
            ]
            if c in category_set
        ]
        self._create_union_view(
            view_name=f"{sport}_player_game_stats",
            sport=sport,
            categories=player_game_categories,
            extra_select=(
                "CASE "
                "WHEN raw_category IN ('pitcher_game_stats') THEN 'pitcher' "
                "WHEN raw_category IN ('game_stats/pitcher_game_stats') THEN 'pitcher' "
                "WHEN raw_category IN ('batter_game_stats') THEN 'batter' "
                "WHEN raw_category IN ('game_stats/batter_game_stats') THEN 'batter' "
                "WHEN raw_category IN ('goalie_stats') THEN 'goalie' "
                "WHEN raw_category IN ('game_stats/goalie_stats') THEN 'goalie' "
                "WHEN raw_category IN ('skater_stats') THEN 'skater' "
                "WHEN raw_category IN ('game_stats/skater_stats') THEN 'skater' "
                "ELSE 'player' END AS player_role"
            ),
        )

        season_avg_categories = [
            c
            for c in categories
            if c == "season_averages"
            or c.startswith("season_averages/")
            or c in {"player_season_averages", "team_season_averages", "standings_season_averages"}
        ]
        self._create_union_view(
            view_name=f"{sport}_all_season_averages",
            sport=sport,
            categories=season_avg_categories,
            extra_select=(
                "CASE "
                "WHEN entity_type IS NOT NULL THEN CAST(entity_type AS VARCHAR) "
                "WHEN raw_category = 'player_season_averages' THEN 'player' "
                "WHEN raw_category = 'season_averages/player' THEN 'player' "
                "WHEN raw_category = 'team_season_averages' THEN 'team' "
                "WHEN raw_category = 'season_averages/team' THEN 'team' "
                "WHEN raw_category = 'standings_season_averages' THEN 'team_standings' "
                "WHEN raw_category = 'season_averages/standings' THEN 'team_standings' "
                "ELSE 'mixed' END AS season_avg_scope"
            ),
        )

    def _create_union_view(
        self,
        *,
        view_name: str,
        sport: str,
        categories: list[str],
        extra_select: str,
    ) -> None:
        paths = [self._category_glob(sport, c) for c in categories]
        paths = [p for p in paths if p]
        if not paths:
            self._conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")
            return

        path_list_sql = ", ".join(f"'{self._escape_sql(p)}'" for p in paths)
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH source_rows AS (
            SELECT
                *,
                regexp_extract(filename, '.*/normalized_curated/[^/]+/(.*?)/season=.*', 1) AS raw_category
            FROM read_parquet([{path_list_sql}], union_by_name=true, filename=true)
        )
        SELECT
            source_rows.*,
            {extra_select}
        FROM source_rows
        """
        self._conn.execute(sql)

    def _category_glob(self, sport: str, category: str) -> str:
        path_glob = self._curated_dir / sport / Path(*category.split("/")) / "**" / "*.parquet"
        return path_glob.as_posix()

    @staticmethod
    def _escape_sql(value: str) -> str:
        return value.replace("'", "''")


def create_duckdb_connection(db_path: Path, *, read_only: bool = False):
    """Create a DuckDB connection.

    Args:
        db_path: Path to the DuckDB file.
        read_only: If True, open in read-only mode (no write lock held).
                   Multiple read-only connections can coexist with one writer.
    """
    duckdb = importlib.import_module("duckdb")
    return duckdb.connect(database=str(db_path), read_only=read_only)
