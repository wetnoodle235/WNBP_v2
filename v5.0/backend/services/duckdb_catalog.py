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
        self._create_targeted_aggregate_views(sport)

    def refresh_all(self, sports: list[str]) -> None:
        for sport in sports:
            try:
                self.refresh_sport(sport)
            except Exception:
                logger.exception("Failed refreshing DuckDB catalog for %s", sport)
        if sports:
            try:
                self._create_cross_sport_views(sports)
            except Exception:
                logger.exception("Failed creating cross-sport aggregate views")

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

    def _drop_relation(self, name: str) -> None:
        """Drop relation if it exists as a view or table."""
        try:
            self._conn.execute(f"DROP VIEW IF EXISTS {name}")
        except Exception:
            pass
        try:
            self._conn.execute(f"DROP TABLE IF EXISTS {name}")
        except Exception:
            pass

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

        if self._has_view(coaches_view) and self._has_columns(coaches_view, {"game_id"}):
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {sport}_game_coaches AS "
                f"SELECT g.*, c.* EXCLUDE (game_id) FROM {sport}_game_core g "
                f"LEFT JOIN {coaches_view} c ON CAST(g.{game_id_col} AS VARCHAR)=CAST(c.game_id AS VARCHAR)"
            )

    def _create_compatibility_alias_views(self, sport: str) -> None:
        """Create legacy-named views that point at grouped storage paths."""
        alias_candidates: dict[str, list[str]] = {
            f"{sport}_games": [f"{sport}_game_schedule_base", f"{sport}_games"],
            f"{sport}_teams": [f"{sport}_team_identity_base", f"{sport}_teams"],
            f"{sport}_players": [f"{sport}_player_identity_base", f"{sport}_players"],
            f"{sport}_conferences": [f"{sport}_reference_conferences_base", f"{sport}_conferences"],
            f"{sport}_standings": [f"{sport}_team_record_standings", f"{sport}_standings"],
            f"{sport}_stats": [f"{sport}_season_team_stats_box", f"{sport}_stats_all"],
            f"{sport}_team_stats": [f"{sport}_season_team_stats_base", f"{sport}_stats_team"],
            f"{sport}_advanced_stats": [f"{sport}_stats_advanced"],
            f"{sport}_advanced_batting": [f"{sport}_stats_advanced_batting"],
            f"{sport}_ratings": [f"{sport}_stats_ratings", f"{sport}_team_ratings_sp"],
            f"{sport}_coaches": [f"{sport}_team_context_staff", f"{sport}_staff_coaches"],
            f"{sport}_injuries": [f"{sport}_player_identity_injury", f"{sport}_injuries"],
            f"{sport}_news": [f"{sport}_player_identity_news", f"{sport}_news"],
            f"{sport}_odds": [f"{sport}_market_odds_live", f"{sport}_odds_current"],
            f"{sport}_odds_history": [f"{sport}_market_odds_history_history", f"{sport}_odds_history"],
            f"{sport}_player_props": [f"{sport}_market_props_live", f"{sport}_player_props_current"],
            f"{sport}_market_signals": [f"{sport}_market_signals_derived", f"{sport}_market_signals"],
            f"{sport}_player_categories_portal": [f"{sport}_player_portal_base", f"{sport}_players_categories_portal"],
            f"{sport}_player_categories_returning": [f"{sport}_player_returning_base", f"{sport}_players_categories_returning"],
            f"{sport}_player_categories_usage": [f"{sport}_player_usage_base", f"{sport}_players_categories_usage"],
            f"{sport}_player_season_averages": [f"{sport}_season_averages_player"],
            f"{sport}_team_season_averages": [f"{sport}_season_averages_team"],
            f"{sport}_standings_season_averages": [f"{sport}_season_averages_standings"],
            f"{sport}_team_game_stats": [f"{sport}_game_stats_team_game_stats"],
            f"{sport}_player_stats": [f"{sport}_game_stats_player_stats"],
            f"{sport}_batter_game_stats": [f"{sport}_game_stats_batter_game_stats"],
            f"{sport}_pitcher_game_stats": [f"{sport}_game_stats_pitcher_game_stats"],
            f"{sport}_goalie_stats": [f"{sport}_game_stats_goalie_stats"],
            f"{sport}_skater_stats": [f"{sport}_game_stats_skater_game_stats", f"{sport}_game_stats_skater_stats"],
        }
        for alias_name, source_candidates in alias_candidates.items():
            for source_name in source_candidates:
                if source_name == alias_name:
                    continue
                if not self._has_view(source_name):
                    continue
                self._conn.execute(f"CREATE OR REPLACE VIEW {alias_name} AS SELECT * FROM {source_name}")
                break

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

        stats_rollup_categories = [
            c
            for c in [
                "stats",
                "stats/all",
                "season/team_stats/base",
                "season/team_stats/box",
                "team_stats",
                "stats/team",
                "advanced_stats",
                "stats/advanced",
                "game/advanced/epa",
                "game/advanced/havoc",
                "game/advanced/ppa",
                "game/advanced/win_prob",
                "advanced_batting",
                "stats/advanced_batting",
                "ratings",
                "stats/ratings",
                "team/ratings/sp",
                "team/ratings/sp_conference",
                "team/ratings/srs",
                "team/ratings/elo",
                "team/ratings/fpi",
                "team/ratings/talent",
            ]
            if c in category_set
        ]
        self._create_union_view(
            view_name=f"{sport}_all_stats",
            sport=sport,
            categories=stats_rollup_categories,
            extra_select=(
                "CASE "
                "WHEN raw_category IN ('stats', 'stats/all') THEN 'general_stats' "
                "WHEN raw_category IN ('team_stats', 'stats/team', 'season/team_stats/base', 'season/team_stats/box') THEN 'team_stats' "
                "WHEN raw_category IN ('advanced_stats', 'stats/advanced', 'game/advanced/epa', 'game/advanced/havoc', 'game/advanced/ppa', 'game/advanced/win_prob') THEN 'advanced_stats' "
                "WHEN raw_category IN ('advanced_batting', 'stats/advanced_batting') THEN 'advanced_batting' "
                "WHEN raw_category IN ('ratings', 'stats/ratings', 'team/ratings/sp', 'team/ratings/sp_conference', 'team/ratings/srs', 'team/ratings/elo', 'team/ratings/fpi', 'team/ratings/talent') THEN 'ratings' "
                "ELSE 'other' END AS stats_category"
            ),
        )

        odds_rollup_categories = [
            c
            for c in [
                "odds",
                "odds/current",
                "market/odds/live",
                "odds_history",
                "odds/history",
                "market/odds_history/history",
            ]
            if c in category_set
        ]
        self._create_union_view(
            view_name=f"{sport}_odds_all",
            sport=sport,
            categories=odds_rollup_categories,
            extra_select=(
                "CASE "
                "WHEN raw_category IN ('odds_history', 'odds/history', 'market/odds_history/history') THEN 'history' "
                "ELSE 'current' END AS odds_scope"
            ),
        )

        player_props_rollup_categories = [
            c
            for c in [
                "player_props",
                "player_props/current",
                "market/props/live",
                "player_props_history",
                "player_props/history",
            ]
            if c in category_set
        ]
        self._create_union_view(
            view_name=f"{sport}_player_props_all",
            sport=sport,
            categories=player_props_rollup_categories,
            extra_select=(
                "CASE "
                "WHEN raw_category IN ('player_props_history', 'player_props/history') THEN 'history' "
                "ELSE 'current' END AS props_scope"
            ),
        )

        market_rollup_categories = [
            c
            for c in [
                "odds",
                "odds/current",
                "market/odds/live",
                "odds_history",
                "odds/history",
                "market/odds_history/history",
                "player_props",
                "player_props/current",
                "market/props/live",
                "player_props_history",
                "player_props/history",
                "market_signals",
                "market/signals/derived",
            ]
            if c in category_set
        ]
        self._create_union_view(
            view_name=f"{sport}_market_history",
            sport=sport,
            categories=market_rollup_categories,
            extra_select=(
                "CASE "
                "WHEN raw_category IN ('odds', 'odds/current', 'market/odds/live') THEN 'odds_current' "
                "WHEN raw_category IN ('odds_history', 'odds/history', 'market/odds_history/history') THEN 'odds_history' "
                "WHEN raw_category IN ('player_props', 'player_props/current', 'market/props/live') THEN 'player_props_current' "
                "WHEN raw_category IN ('player_props_history', 'player_props/history') THEN 'player_props_history' "
                "WHEN raw_category IN ('market_signals', 'market/signals/derived') THEN 'market_signals' "
                "ELSE 'other' END AS market_scope"
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

    def _create_targeted_aggregate_views(self, sport: str) -> None:
        """Create a small set of performance-focused aggregate views.

        These are intentionally limited to avoid storage/query clutter while
        accelerating frequent API access patterns.
        """
        if sport.lower() not in {"ncaaf", "nba"}:
            return
        self._create_market_odds_latest_view(sport)
        self._create_game_daily_snapshot_view(sport)
        self._create_player_season_rollup_view(sport)
        self._create_team_season_rollup_view(sport)
        self._create_team_recent_form_view(sport)

    def _create_market_odds_latest_view(self, sport: str) -> None:
        source_view = f"{sport}_odds_all" if self._has_view(f"{sport}_odds_all") else f"{sport}_odds"
        target_view = f"{sport}_market_odds_latest"
        self._drop_relation(target_view)

        required = {"game_id", "bookmaker"}
        if not self._has_view(source_view) or not self._has_columns(source_view, required):
            self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")
            return

        ts_col = "timestamp" if self._has_columns(source_view, {"timestamp"}) else "date"
        sql = f"""
        CREATE OR REPLACE TABLE {target_view} AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(game_id AS VARCHAR), CAST(bookmaker AS VARCHAR)
                    ORDER BY TRY_CAST({ts_col} AS TIMESTAMP) DESC, TRY_CAST(date AS TIMESTAMP) DESC
                ) AS _rn
            FROM {source_view}
        )
        SELECT * EXCLUDE (_rn)
        FROM ranked
        WHERE _rn = 1
        """
        self._conn.execute(sql)

    def _create_game_daily_snapshot_view(self, sport: str) -> None:
        games_view = f"{sport}_game_core" if self._has_view(f"{sport}_game_core") else f"{sport}_games"
        odds_latest_view = f"{sport}_market_odds_latest"
        target_view = f"{sport}_game_daily_snapshot"
        self._drop_relation(target_view)

        if not self._has_view(games_view):
            self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")
            return

        date_expr = "TRY_CAST(g.date AS DATE)"
        if self._has_columns(games_view, {"start_time"}):
            date_expr = "COALESCE(TRY_CAST(g.start_time AS DATE), TRY_CAST(g.date AS DATE))"

        if self._has_view(odds_latest_view) and self._has_columns(games_view, {"id"}) and self._has_columns(odds_latest_view, {"game_id"}):
            odds_ts_col = "o.timestamp AS odds_timestamp" if self._has_columns(odds_latest_view, {"timestamp"}) else "TRY_CAST(o.date AS TIMESTAMP) AS odds_timestamp"
            bookmaker_col = "o.bookmaker AS market_bookmaker" if self._has_columns(odds_latest_view, {"bookmaker"}) else "NULL AS market_bookmaker"
            h2h_home_col = "o.h2h_home" if self._has_columns(odds_latest_view, {"h2h_home"}) else "NULL AS h2h_home"
            h2h_away_col = "o.h2h_away" if self._has_columns(odds_latest_view, {"h2h_away"}) else "NULL AS h2h_away"
            spread_home_col = "o.spread_home" if self._has_columns(odds_latest_view, {"spread_home"}) else "NULL AS spread_home"
            spread_away_col = "o.spread_away" if self._has_columns(odds_latest_view, {"spread_away"}) else "NULL AS spread_away"
            total_line_col = "o.total_line" if self._has_columns(odds_latest_view, {"total_line"}) else "NULL AS total_line"
            sql = f"""
            CREATE OR REPLACE TABLE {target_view} AS
            SELECT
                g.*,
                {date_expr} AS snapshot_date,
                {bookmaker_col},
                {h2h_home_col},
                {h2h_away_col},
                {spread_home_col},
                {spread_away_col},
                {total_line_col},
                {odds_ts_col}
            FROM {games_view} g
            LEFT JOIN {odds_latest_view} o
              ON CAST(g.id AS VARCHAR) = CAST(o.game_id AS VARCHAR)
            """
            self._conn.execute(sql)
            return

        sql = f"""
        CREATE OR REPLACE TABLE {target_view} AS
        SELECT
            g.*,
            {date_expr} AS snapshot_date
        FROM {games_view} g
        """
        self._conn.execute(sql)

    def _create_player_season_rollup_view(self, sport: str) -> None:
        target_view = f"{sport}_player_season_rollup"
        self._drop_relation(target_view)

        # Prefer existing player season average categories when available.
        for base_view in [f"{sport}_player_season_averages", f"{sport}_season_averages_player"]:
            if self._has_view(base_view):
                self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM {base_view}")
                return

        # Fallback to PPA game-level data for NCAAF-like datasets where the
        # generic player_game_stats view may be sparse or empty.
        ppa_view = f"{sport}_player_game_stats_ppa"
        if self._has_view(ppa_view) and self._has_columns(ppa_view, {"season", "player_id", "player_name"}):
            self._conn.execute(
                f"""
                CREATE OR REPLACE TABLE {target_view} AS
                SELECT
                    CAST(season AS VARCHAR) AS season,
                    CAST(player_id AS VARCHAR) AS player_id,
                    ANY_VALUE(player_name) AS player_name,
                    ANY_VALUE(position) AS position,
                    ANY_VALUE(team_name) AS team_name,
                    COUNT(*) AS games_played,
                    AVG(TRY_CAST(average_ppa_all AS DOUBLE)) AS average_ppa_all,
                    AVG(TRY_CAST(average_ppa_pass AS DOUBLE)) AS average_ppa_pass,
                    AVG(TRY_CAST(average_ppa_rush AS DOUBLE)) AS average_ppa_rush
                FROM {ppa_view}
                GROUP BY 1, 2
                """
            )
            return

        # Last-resort fallback: expose an empty schema-safe view.
        self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")

    def _create_team_season_rollup_view(self, sport: str) -> None:
        target_view = f"{sport}_team_season_rollup"
        self._drop_relation(target_view)

        source_candidates = [
            f"{sport}_team_stats",
            f"{sport}_season_team_stats_base",
            f"{sport}_season_team_stats_box",
        ]
        source_view = next((name for name in source_candidates if self._has_view(name)), None)
        if source_view is None:
            games_view = f"{sport}_game_core" if self._has_view(f"{sport}_game_core") else f"{sport}_games"
            required = {"season", "date", "home_team_id", "away_team_id", "home_team", "away_team", "home_score", "away_score", "status"}
            if not self._has_view(games_view) or not self._has_columns(games_view, required):
                self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")
                return

            self._conn.execute(
                f"""
                CREATE OR REPLACE TABLE {target_view} AS
                WITH team_games AS (
                    SELECT
                        CAST(season AS VARCHAR) AS season,
                        TRY_CAST(date AS DATE) AS game_date,
                        CAST(home_team_id AS VARCHAR) AS team_id,
                        CAST(home_team AS VARCHAR) AS team_name,
                        TRY_CAST(home_score AS DOUBLE) AS points_for,
                        TRY_CAST(away_score AS DOUBLE) AS points_against,
                        CAST(status AS VARCHAR) AS game_status
                    FROM {games_view}
                    UNION ALL
                    SELECT
                        CAST(season AS VARCHAR) AS season,
                        TRY_CAST(date AS DATE) AS game_date,
                        CAST(away_team_id AS VARCHAR) AS team_id,
                        CAST(away_team AS VARCHAR) AS team_name,
                        TRY_CAST(away_score AS DOUBLE) AS points_for,
                        TRY_CAST(home_score AS DOUBLE) AS points_against,
                        CAST(status AS VARCHAR) AS game_status
                    FROM {games_view}
                ),
                finals AS (
                    SELECT
                        *,
                        CASE WHEN points_for > points_against THEN 1 ELSE 0 END AS is_win,
                        CASE WHEN points_for < points_against THEN 1 ELSE 0 END AS is_loss,
                        (points_for - points_against) AS margin
                    FROM team_games
                    WHERE team_id IS NOT NULL
                      AND game_date IS NOT NULL
                      AND points_for IS NOT NULL
                      AND points_against IS NOT NULL
                      AND lower(game_status) LIKE '%final%'
                )
                SELECT
                    season,
                    team_id,
                    ANY_VALUE(team_name) AS team_name,
                    COUNT(*) AS games_played,
                    SUM(is_win) AS wins,
                    SUM(is_loss) AS losses,
                    AVG(points_for) AS avg_points_for,
                    AVG(points_against) AS avg_points_against,
                    AVG(margin) AS avg_margin,
                    MAX(game_date) AS as_of_date
                FROM finals
                GROUP BY season, team_id
                """
            )
            return

        # When source_view exists, select only columns that actually exist
        try:
            rows = self._conn.execute(f"PRAGMA table_info('{source_view}')").fetchall()
            available_cols = {str(r[1]) for r in rows}
        except Exception:
            logger.warning(f"Could not introspect columns from {source_view}")
            self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")
            return
        
        # Build SELECT list with only available columns
        col_list = ["season", "team_id", "team_name", "games_played", "wins", "losses", 
                    "avg_points_for", "avg_points_against", "avg_margin", "as_of_date"]
        safe_cols = [c for c in col_list if c in available_cols]
        
        # Only try to compute per-game stats if we have the required columns
        has_yards = "totalYards" in available_cols and "games_played" in available_cols
        has_opponent_yards = "totalYardsOpponent" in available_cols and "games_played" in available_cols
        has_turnovers = "turnoversOpponent" in available_cols and "turnovers" in available_cols and "games_played" in available_cols
        has_points_opp = "off_pointsPerOpportunity" in available_cols and "def_pointsPerOpportunity" in available_cols
        
        sql = f"""
            CREATE OR REPLACE TABLE {target_view} AS
            SELECT
                {', '.join(safe_cols)}
        """
        
        if has_yards:
            sql += f"""
                , CASE WHEN games_played > 0
                    THEN TRY_CAST(totalYards AS DOUBLE) / CAST(games_played AS DOUBLE)
                    ELSE NULL END AS total_yards_per_game
            """
        if has_opponent_yards:
            sql += f"""
                , CASE WHEN games_played > 0
                    THEN TRY_CAST(totalYardsOpponent AS DOUBLE) / CAST(games_played AS DOUBLE)
                    ELSE NULL END AS total_yards_allowed_per_game
            """
        if has_turnovers:
            sql += f"""
                , CASE WHEN games_played > 0
                    THEN (TRY_CAST(turnoversOpponent AS DOUBLE) - TRY_CAST(turnovers AS DOUBLE)) / CAST(games_played AS DOUBLE)
                    ELSE NULL END AS turnover_margin_per_game
            """
        if has_points_opp:
            sql += f"""
                , (TRY_CAST(off_pointsPerOpportunity AS DOUBLE) - TRY_CAST(def_pointsPerOpportunity AS DOUBLE)) AS points_per_opportunity_diff
            """
        
        sql += f" FROM {source_view}"
        
        try:
            self._conn.execute(sql)
        except Exception as e:
            logger.warning(f"Failed to create {target_view} with computed columns; creating empty view: {e}")
            self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")

    def _create_team_recent_form_view(self, sport: str) -> None:
        target_view = f"{sport}_team_recent_form"
        self._drop_relation(target_view)

        games_view = f"{sport}_game_core" if self._has_view(f"{sport}_game_core") else f"{sport}_games"
        required = {"season", "date", "home_team_id", "away_team_id", "home_team", "away_team", "home_score", "away_score", "status"}
        if not self._has_view(games_view) or not self._has_columns(games_view, required):
            self._conn.execute(f"CREATE OR REPLACE TABLE {target_view} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")
            return

        self._conn.execute(
            f"""
            CREATE OR REPLACE TABLE {target_view} AS
            WITH team_games AS (
                SELECT
                    CAST(season AS VARCHAR) AS season,
                    TRY_CAST(date AS DATE) AS game_date,
                    CAST(home_team_id AS VARCHAR) AS team_id,
                    CAST(home_team AS VARCHAR) AS team_name,
                    CAST(away_team AS VARCHAR) AS opponent_team,
                    TRY_CAST(home_score AS DOUBLE) AS points_for,
                    TRY_CAST(away_score AS DOUBLE) AS points_against,
                    CAST(status AS VARCHAR) AS game_status,
                    CAST(id AS VARCHAR) AS game_id
                FROM {games_view}
                UNION ALL
                SELECT
                    CAST(season AS VARCHAR) AS season,
                    TRY_CAST(date AS DATE) AS game_date,
                    CAST(away_team_id AS VARCHAR) AS team_id,
                    CAST(away_team AS VARCHAR) AS team_name,
                    CAST(home_team AS VARCHAR) AS opponent_team,
                    TRY_CAST(away_score AS DOUBLE) AS points_for,
                    TRY_CAST(home_score AS DOUBLE) AS points_against,
                    CAST(status AS VARCHAR) AS game_status,
                    CAST(id AS VARCHAR) AS game_id
                FROM {games_view}
            ),
            finals AS (
                SELECT
                    *,
                    CASE WHEN points_for > points_against THEN 1 ELSE 0 END AS is_win,
                    (points_for - points_against) AS margin,
                    ROW_NUMBER() OVER (
                        PARTITION BY season, team_id
                        ORDER BY game_date DESC, game_id DESC
                    ) AS rn
                FROM team_games
                WHERE team_id IS NOT NULL
                  AND game_date IS NOT NULL
                  AND points_for IS NOT NULL
                  AND points_against IS NOT NULL
                  AND lower(game_status) LIKE '%final%'
            )
            SELECT
                season,
                team_id,
                ANY_VALUE(team_name) AS team_name,
                MAX(game_date) AS as_of_date,
                SUM(CASE WHEN rn <= 3 THEN is_win ELSE 0 END) AS wins_last_3,
                SUM(CASE WHEN rn <= 3 THEN 1 ELSE 0 END) AS games_last_3,
                AVG(CASE WHEN rn <= 3 THEN margin END) AS avg_margin_last_3,
                AVG(CASE WHEN rn <= 3 THEN points_for END) AS avg_points_for_last_3,
                AVG(CASE WHEN rn <= 3 THEN points_against END) AS avg_points_against_last_3,
                SUM(CASE WHEN rn <= 5 THEN is_win ELSE 0 END) AS wins_last_5,
                SUM(CASE WHEN rn <= 5 THEN 1 ELSE 0 END) AS games_last_5,
                AVG(CASE WHEN rn <= 5 THEN margin END) AS avg_margin_last_5,
                AVG(CASE WHEN rn <= 5 THEN points_for END) AS avg_points_for_last_5,
                AVG(CASE WHEN rn <= 5 THEN points_against END) AS avg_points_against_last_5,
                SUM(CASE WHEN rn <= 10 THEN is_win ELSE 0 END) AS wins_last_10,
                SUM(CASE WHEN rn <= 10 THEN 1 ELSE 0 END) AS games_last_10,
                AVG(CASE WHEN rn <= 10 THEN margin END) AS avg_margin_last_10,
                AVG(CASE WHEN rn <= 10 THEN points_for END) AS avg_points_for_last_10,
                AVG(CASE WHEN rn <= 10 THEN points_against END) AS avg_points_against_last_10
            FROM finals
            GROUP BY season, team_id
            """
        )

    def _create_cross_sport_views(self, sports: list[str]) -> None:
        """Build UNION ALL BY NAME views spanning all per-sport views for common data kinds.

        Creates:
          _all_games, _all_teams, _all_players, _all_news, _all_odds,
          _all_injuries, _all_standings, _all_predictions,
          _all_player_stats, _all_team_stats

        Each view injects a ``sport`` column when the source view lacks one so that
        callers can filter by sport with a single SQL predicate.
        """
        # Each entry: (kind_suffix, priority_templates_per_sport)
        # For each sport the first matching template is used.
        cross_kinds: list[tuple[str, list[str]]] = [
            ("games",        ["{s}_game_core", "{s}_games"]),
            ("teams",        ["{s}_teams"]),
            ("players",      ["{s}_players"]),
            ("news",         ["{s}_news"]),
            ("odds",         ["{s}_odds_all", "{s}_odds"]),
            ("injuries",     ["{s}_injuries"]),
            ("standings",    ["{s}_standings"]),
            ("predictions",  ["{s}_predictions"]),
            # For player_stats, prefer pre-aggregated season-average views.
            ("player_stats", [
                "{s}_player_season_rollup",
                "{s}_player_season_averages",
                "{s}_season_averages_player",
                "{s}_player_stats",
            ]),
            ("team_stats",   ["{s}_team_stats"]),
        ]

        _empty_sql = "SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0"

        for view_suffix, templates in cross_kinds:
            target = f"_all_{view_suffix}"
            branches: list[str] = []

            for sport in sports:
                for tmpl in templates:
                    src = tmpl.format(s=sport)
                    if not self._has_view(src):
                        continue
                    already_has_sport = self._has_columns(src, {"sport"})
                    if already_has_sport:
                        branches.append(f"(SELECT * FROM {src})")
                    else:
                        branches.append(f"(SELECT *, '{sport}' AS sport FROM {src})")
                    break  # use only the first matching template per sport

            if not branches:
                try:
                    self._conn.execute(f"CREATE OR REPLACE VIEW {target} AS {_empty_sql}")
                except Exception:
                    pass
                continue

            union_sql = " UNION ALL BY NAME ".join(branches)
            try:
                self._conn.execute(f"CREATE OR REPLACE VIEW {target} AS {union_sql}")
                logger.debug("Created cross-sport view %s (%d sport(s))", target, len(branches))
            except Exception:
                logger.exception("Failed creating cross-sport view %s; creating empty stub", target)
                try:
                    self._conn.execute(f"CREATE OR REPLACE VIEW {target} AS {_empty_sql}")
                except Exception:
                    pass

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
