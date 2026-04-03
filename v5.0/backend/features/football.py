# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Football
# ──────────────────────────────────────────────────────────
#
# Covers NFL and NCAAF.  Produces ~45 features per game
# including EPA-based metrics, rushing/passing balance,
# turnovers, red zone, third down, special teams, and
# weather impact.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)

# Standard NFL team full-name → abbreviation lookup (used for 2023+ games that lack box stats)
_NFL_NAME_TO_ABBREV: dict[str, str] = {
    "Arizona Cardinals": "ARI",
    "Atlanta Falcons": "ATL",
    "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "Houston Texans": "HOU",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV",
    "Oakland Raiders": "LV",
    "Los Angeles Chargers": "LAC",
    "San Diego Chargers": "LAC",
    "Los Angeles Rams": "LAR",
    "St. Louis Rams": "LAR",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New England Patriots": "NE",
    "New Orleans Saints": "NO",
    "New York Giants": "NYG",
    "New York Jets": "NYJ",
    "Philadelphia Eagles": "PHI",
    "Pittsburgh Steelers": "PIT",
    "Seattle Seahawks": "SEA",
    "San Francisco 49ers": "SF",
    "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN",
    "Washington Commanders": "WSH",
    "Washington Football Team": "WSH",
    "Washington Redskins": "WSH",
}

_nfl_abbrev_cache: dict[str, str] = {}


def _build_nfl_abbrev_map(mapping: dict[str, str], sport_dir: "Path", reader=None) -> None:
    """Supplement NFL name→abbrev mapping by cross-referencing older game files with player_stats."""
    global _nfl_abbrev_cache
    if _nfl_abbrev_cache:
        mapping.update(_nfl_abbrev_cache)
        return
    built: dict[str, str] = {}
    for yr in range(2020, 2026):
        try:
            if reader is not None:
                g = reader.load("nfl", "games", season=yr)
                ps = reader.load("nfl", "player_stats", season=yr)
            else:
                g_path = sport_dir / f"games_{yr}.parquet"
                ps_path = sport_dir / f"player_stats_{yr}.parquet"
                if not g_path.exists() or not ps_path.exists():
                    continue
                g = pd.read_parquet(g_path)
                ps = pd.read_parquet(ps_path)
        except Exception:
            continue
        if "home_team" not in g.columns or "game_id" not in ps.columns:
            continue
        # Group player_stats by game_id to get team abbreviations per game
        for gid, grp in ps.groupby("game_id"):
            gid_str = str(gid)
            game_row = g.loc[g["id"].astype(str) == gid_str]
            if game_row.empty:
                continue
            row = game_row.iloc[0]
            abbrevs = [t for t in grp["team_id"].unique() if t not in ("AFC", "NFC")]
            home_name = str(row.get("home_team", ""))
            away_name = str(row.get("away_team", ""))
            # Use existing mapping to identify which abbrev is home vs away
            home_ab = mapping.get(home_name, "")
            away_ab = mapping.get(away_name, "")
            if not home_ab and not away_ab and len(abbrevs) == 2:
                # Can't determine; skip
                continue
            for ab in abbrevs:
                if ab == home_ab:
                    built[home_name] = ab
                elif ab == away_ab:
                    built[away_name] = ab
    _nfl_abbrev_cache = built
    mapping.update(built)


class FootballExtractor(BaseFeatureExtractor):
    """Feature extractor for American football (NFL, NCAAF)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_games_cache: pd.DataFrame | None = None
        self._team_id_map: dict[str, str] = {}  # numeric_id_str → abbreviation
        self._team_id_map_built: bool = False

    @staticmethod
    def _n(val, fallback: float = 0.0) -> float:
        """Convert value to float, returning *fallback* on failure."""
        try:
            return float(val)
        except (TypeError, ValueError):
            return fallback

    def _build_team_id_map(self, season: int | None = None) -> None:
        """Build numeric ESPN team ID → abbreviation mapping from teams data.

        NFL/NCAAF games use numeric IDs (e.g. '1', '59', '124179') while
        player_stats use abbreviations ('KC', 'APP', 'VILL').  This map
        lets us look up the right player_stats slice for any team_id.
        """
        if self._team_id_map_built:
            return
        try:
            seasons = self._reader.available_seasons(self.sport, "teams")
        except Exception:
            seasons = []
        for s in seasons:
            try:
                t = self._reader.load(self.sport, "teams", season=s)
                if "id" in t.columns and "abbreviation" in t.columns:
                    for _, row in t.iterrows():
                        numeric = str(row["id"]).strip()
                        abbrev = str(row["abbreviation"]).strip()
                        if numeric and abbrev and abbrev not in ("nan", "None"):
                            self._team_id_map[numeric] = abbrev
            except Exception:
                pass
        self._team_id_map_built = True

    def _resolve_team_abbrev(self, team_id: str) -> str:
        """Return the abbreviation for a team_id, resolving numeric IDs via map."""
        self._build_team_id_map()
        tid = str(team_id).strip()
        return self._team_id_map.get(tid, tid)

    def _load_all_games(self) -> pd.DataFrame:
        """Load and cache all seasons' game data for cross-season form calculations.

        For NFL: augments missing box-score columns (2023-2025) by aggregating
        per-player stats up to team-game totals from player_stats parquets.
        """
        if self._all_games_cache is not None:
            return self._all_games_cache
        combined = self._reader.load_all_seasons(self.sport, "games")
        if combined.empty:
            self._all_games_cache = combined
            return combined
        if "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
            combined.sort_values("date", inplace=True, ignore_index=True)
        # Augment NFL games missing box-score stats from player_stats
        if self.sport == "nfl" and not combined.empty:
            combined = self._augment_nfl_games_from_player_stats(combined)
        self._all_games_cache = combined
        return combined

    def _augment_nfl_games_from_player_stats(self, games: pd.DataFrame) -> pd.DataFrame:
        """For games missing box-score stats, derive team totals from player_stats.

        This fills: passing_yards, rushing_yards, passing_touchdowns,
        rushing_touchdowns, turnovers, sacks, completion_pct, receiving_yards,
        interceptions_thrown, fumbles_lost, tackles for 2023-2025 seasons.
        """
        if "home_passing_yards" not in games.columns:
            games = games.copy()
            games["home_passing_yards"] = np.nan

        # Find seasons where most games are missing passing_yards
        games["_season_int"] = pd.to_numeric(games.get("season", pd.Series(dtype=float)), errors="coerce")
        seasons_to_fix: list[int] = []
        for season, grp in games.groupby("_season_int"):
            if pd.isna(season):
                continue
            season = int(season)
            null_frac = grp["home_passing_yards"].isna().mean()
            if null_frac > 0.5:
                seasons_to_fix.append(season)

        if not seasons_to_fix:
            return games

        logger.info("NFL: deriving box stats from player_stats for seasons %s", seasons_to_fix)

        for season in seasons_to_fix:
            try:
                ps = self._reader.load(self.sport, "player_stats", season=season)
            except Exception:
                continue
            if "game_id" not in ps.columns or "team_id" not in ps.columns:
                continue

            def _col(c: str) -> pd.Series:
                if c in ps.columns:
                    return pd.to_numeric(ps[c], errors="coerce").fillna(0)
                return pd.Series(0.0, index=ps.index)

            # Build game-team aggregates
            agg = ps.groupby(["game_id", "team_id"]).apply(lambda g: pd.Series({
                "pass_yds_tot": _col("pass_yds").loc[g.index].sum(),
                "rush_yds_tot": _col("rush_yds").loc[g.index].sum(),
                "pass_td_tot": _col("pass_td").loc[g.index].sum(),
                "rush_td_tot": _col("rush_td").loc[g.index].sum(),
                "pass_int_tot": _col("pass_int").loc[g.index].sum(),
                "fumbles_lost_tot": _col("fumbles_lost").loc[g.index].sum(),
                "sacks_tot": _col("sacks").loc[g.index].sum(),
                "pass_att_tot": _col("pass_att").loc[g.index].sum(),
                "pass_cmp_tot": _col("pass_cmp").loc[g.index].sum(),
                "rec_yds_tot": _col("rec_yds").loc[g.index].sum(),
                "tackles_tot": _col("tackles").loc[g.index].sum(),
            }), include_groups=False).reset_index()

            agg["comp_pct"] = np.where(
                agg["pass_att_tot"] > 0,
                agg["pass_cmp_tot"] / agg["pass_att_tot"] * 100.0,
                0.0
            )
            agg["turnovers_tot"] = agg["pass_int_tot"] + agg["fumbles_lost_tot"]
            agg["game_id"] = agg["game_id"].astype(str)
            agg["team_id"] = agg["team_id"].astype(str)

            # Match game rows to fill in missing stats
            season_mask = games["_season_int"] == season
            games_s = games.loc[season_mask].copy()
            if "id" not in games_s.columns:
                continue

            games_s["_game_id_str"] = games_s["id"].astype(str)
            agg_dict = {gid: grp for gid, grp in agg.groupby("game_id")}

            # Column mapping: player_stats_col → games_col_suffix
            stat_map = {
                "pass_yds_tot": "passing_yards",
                "rush_yds_tot": "rushing_yards",
                "pass_td_tot": "passing_touchdowns",
                "rush_td_tot": "rushing_touchdowns",
                "turnovers_tot": "turnovers",
                "sacks_tot": "sacks",
                "comp_pct": "completion_pct",
                "rec_yds_tot": "receiving_yards",
                "pass_int_tot": "interceptions_thrown",
                "fumbles_lost_tot": "fumbles_lost",
                "tackles_tot": "tackles",
            }

            for stat_key, games_col in stat_map.items():
                for prefix in ("home_", "away_"):
                    full_col = f"{prefix}{games_col}"
                    if full_col not in games.columns:
                        games[full_col] = np.nan

            # Build team name → abbreviation mapping using this season's player_stats + games
            # NFL games use numeric ESPN IDs but player_stats use abbreviations (e.g. 'KC', 'BAL')
            # We match by finding which abbreviation belongs to each team using team names
            name_to_abbrev: dict[str, str] = _NFL_NAME_TO_ABBREV.copy()
            # Supplement with dynamic cross-reference from any year with both data
            _build_nfl_abbrev_map(name_to_abbrev, self.data_dir, reader=self._reader)

            for idx, row in games_s.iterrows():
                gid = str(row["id"])
                if gid not in agg_dict:
                    continue
                game_agg = agg_dict[gid]
                home_name = str(row.get("home_team", ""))
                away_name = str(row.get("away_team", ""))

                home_abbrev = name_to_abbrev.get(home_name, "")
                away_abbrev = name_to_abbrev.get(away_name, "")

                # Fallback: if only 2 teams in game_agg, assign by exclusion
                if not home_abbrev or not away_abbrev:
                    teams_in_game = list(game_agg["team_id"].unique())
                    valid = [t for t in teams_in_game if t not in ("AFC", "NFC")]
                    if len(valid) == 2:
                        if home_abbrev and valid[0] == home_abbrev:
                            away_abbrev = valid[1]
                        elif home_abbrev and valid[1] == home_abbrev:
                            away_abbrev = valid[0]
                        elif away_abbrev and valid[0] == away_abbrev:
                            home_abbrev = valid[1]
                        elif away_abbrev and valid[1] == away_abbrev:
                            home_abbrev = valid[0]

                for prefix, abbrev in (("home_", home_abbrev), ("away_", away_abbrev)):
                    if not abbrev:
                        continue
                    team_row = game_agg.loc[game_agg["team_id"] == abbrev]
                    if team_row.empty:
                        continue
                    tr = team_row.iloc[0]
                    for stat_key, games_col in stat_map.items():
                        full_col = f"{prefix}{games_col}"
                        if pd.isna(games.at[idx, full_col]):
                            games.at[idx, full_col] = float(tr[stat_key])

        games = games.drop(columns=["_season_int"], errors="ignore")
        return games

    # ── Helpers ────────────────────────────────────────────

    def _stat(self, game: dict, prefix: str, key: str, default: float = 0.0) -> float:
        return float(pd.to_numeric(game.get(f"{prefix}{key}", default), errors="coerce") or default)

    def _team_stat_averages(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling averages of key box-score stats."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "pass_yds_pg": 0.0, "rush_yds_pg": 0.0, "total_yds_pg": 0.0,
                "pass_yds_allowed_pg": 0.0, "rush_yds_allowed_pg": 0.0,
                "turnovers_pg": 0.0, "takeaways_pg": 0.0,
                "sacks_pg": 0.0, "penalties_pg": 0.0,
                "sacks_allowed_pg": 0.0, "completion_pct": 0.0,
                "first_downs_pg": 0.0, "penalty_yards_pg": 0.0,
                "fumbles_lost_pg": 0.0, "scoring_efficiency": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        def _avg(stat: str, opp: bool = False) -> float:
            vals = []
            for row, h in zip(records, is_home):
                if opp:
                    p = "away_" if h else "home_"
                else:
                    p = "home_" if h else "away_"
                vals.append(pd.to_numeric(row.get(f"{p}{stat}", 0), errors="coerce") or 0.0)
            return float(np.mean(vals)) if vals else 0.0

        return {
            "pass_yds_pg": _avg("passing_yards"),
            "rush_yds_pg": _avg("rushing_yards"),
            "total_yds_pg": _avg("passing_yards") + _avg("rushing_yards"),
            "pass_yds_allowed_pg": _avg("passing_yards", opp=True),
            "rush_yds_allowed_pg": _avg("rushing_yards", opp=True),
            "turnovers_pg": _avg("turnovers"),
            "takeaways_pg": _avg("turnovers", opp=True),
            "sacks_pg": _avg("sacks"),
            "penalties_pg": _avg("penalties"),
            "sacks_allowed_pg": _avg("sacks_allowed"),
            "completion_pct": _avg("completion_pct"),
            "first_downs_pg": _avg("first_downs"),
            "penalty_yards_pg": _avg("penalty_yards"),
            "passing_tds_pg": _avg("passing_touchdowns"),
            "rushing_tds_pg": _avg("rushing_touchdowns"),
            "total_tds_pg": _avg("passing_touchdowns") + _avg("rushing_touchdowns"),
            "tds_allowed_pg": _avg("passing_touchdowns", opp=True) + _avg("rushing_touchdowns", opp=True),
            "yards_per_play": _avg("yards_per_play"),
            "yards_per_play_allowed": _avg("yards_per_play", opp=True),
            "receiving_yds_pg": _avg("receiving_yards"),
            "int_thrown_pg": _avg("interceptions_thrown"),
            "int_thrown_allowed_pg": _avg("interceptions_thrown", opp=True),
            "tackles_pg": _avg("tackles"),
            "fumbles_lost_pg": _avg("fumbles_lost"),
            "scoring_efficiency": _avg("scoring_efficiency"),
        }

    def _epa_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """EPA (Expected Points Added) features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "epa_pass_off": 0.0, "epa_rush_off": 0.0, "epa_total_off": 0.0,
                "epa_pass_def": 0.0, "epa_rush_def": 0.0, "epa_total_def": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        def _epa_avg(stat: str, opp: bool = False) -> float:
            vals = []
            for row, h in zip(records, is_home):
                p = ("away_" if h else "home_") if opp else ("home_" if h else "away_")
                vals.append(pd.to_numeric(row.get(f"{p}{stat}", 0), errors="coerce") or 0.0)
            return float(np.mean(vals)) if vals else 0.0

        epa_pass_off = _epa_avg("passing_epa")
        epa_rush_off = _epa_avg("rushing_epa")
        epa_pass_def = _epa_avg("passing_epa", opp=True)
        epa_rush_def = _epa_avg("rushing_epa", opp=True)

        air_off = _epa_avg("air_yards")
        yac_off = _epa_avg("yac")
        air_def = _epa_avg("air_yards", opp=True)
        yac_def = _epa_avg("yac", opp=True)

        return {
            "epa_pass_off": epa_pass_off,
            "epa_rush_off": epa_rush_off,
            "epa_total_off": epa_pass_off + epa_rush_off,
            "epa_pass_def": epa_pass_def,
            "epa_rush_def": epa_rush_def,
            "epa_total_def": epa_pass_def + epa_rush_def,
            "air_yards_pg": air_off,
            "yac_pg": yac_off,
            "air_yards_allowed_pg": air_def,
            "yac_allowed_pg": yac_def,
        }

    def _efficiency_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Third-down, fourth-down, red-zone, and time-of-possession features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "third_down_pct": 0.0, "red_zone_pct": 0.0, "fourth_down_pct": 0.0,
                "time_of_possession": 0.0, "turnover_margin": 0.0, "defensive_td_pg": 0.0,
            }

        is_home = (recent["home_team_id"] == team_id).values
        n = len(recent)

        def _col_team(home_col: str, away_col: str, default: float = 0.0) -> pd.Series:
            zero = pd.Series(np.full(n, default))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(default) if home_col in recent.columns else zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(default) if away_col in recent.columns else zero
            return pd.Series(np.where(is_home, h.values, a.values))

        third_conv = _col_team("home_third_down_conv", "away_third_down_conv").sum()
        third_att = _col_team("home_third_down_att", "away_third_down_att").sum()
        fourth_conv = _col_team("home_fourth_down_conv", "away_fourth_down_conv").sum()
        fourth_att = _col_team("home_fourth_down_att", "away_fourth_down_att").sum()
        red_zone_pct = _col_team("home_red_zone_pct", "away_red_zone_pct", default=0.0)
        top_secs = _col_team("home_possession_seconds", "away_possession_seconds", default=1800.0)
        top_mins = top_secs / 60.0  # convert to minutes
        team_to = _col_team("home_turnovers", "away_turnovers")
        opp_to = _col_team("away_turnovers", "home_turnovers")
        def_td = _col_team("home_defensive_tds", "away_defensive_tds", default=0.0)

        rz_valid = red_zone_pct[red_zone_pct > 0]
        rz_mean = float(rz_valid.mean()) if len(rz_valid) > 0 else 0.0

        return {
            "third_down_pct": float(third_conv / third_att) if third_att > 0 else 0.0,
            "fourth_down_pct": float(fourth_conv / fourth_att) if fourth_att > 0 else 0.0,
            "red_zone_pct": rz_mean,
            "time_of_possession": float(top_mins.mean()),
            "turnover_margin": float((opp_to - team_to).mean()),
            "defensive_td_pg": float(def_td.mean()),
        }

    def _rushing_passing_balance(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Ratio of rushing to passing plays and yards."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"rush_pct": 0.5, "pass_pct": 0.5, "yards_per_play": 0.0}

        is_home = recent["home_team_id"] == team_id
        is_home = (recent["home_team_id"] == team_id).values
        n = len(recent)

        def _col_team(home_col: str, away_col: str, default: float = 0.0) -> pd.Series:
            zero = pd.Series(np.full(n, default))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(default) if home_col in recent.columns else zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(default) if away_col in recent.columns else zero
            return pd.Series(np.where(is_home, h.values, a.values))

        ra = _col_team("home_rushing_attempts", "away_rushing_attempts")
        pa = _col_team("home_pass_attempts", "away_pass_attempts")
        ry = _col_team("home_rushing_yards", "away_rushing_yards")
        py = _col_team("home_passing_yards", "away_passing_yards")
        rush_att = ra.sum()
        pass_att = pa.sum()
        total_yds = (ry + py).sum()
        total_plays = (ra + pa).sum()

        total_att = rush_att + pass_att
        return {
            "rush_pct": float(rush_att / total_att) if total_att > 0 else 0.5,
            "pass_pct": float(pass_att / total_att) if total_att > 0 else 0.5,
            "yards_per_play": float(total_yds / total_plays) if total_plays > 0 else 0.0,
        }

    def _special_teams(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Punt return, kick return, and field goal features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"fg_pct": 0.0, "punt_avg": 0.0, "kick_return_avg": 0.0}

        is_home = (recent["home_team_id"] == team_id).values
        n = len(recent)

        def _col_team(home_col: str, away_col: str) -> pd.Series:
            zero = pd.Series(np.zeros(n))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(0) if home_col in recent.columns else zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(0) if away_col in recent.columns else zero
            return pd.Series(np.where(is_home, h.values, a.values))

        fg_made = _col_team("home_field_goals_made", "away_field_goals_made").sum()
        fg_att = _col_team("home_field_goals_attempted", "away_field_goals_attempted").sum()
        punt_yds = _col_team("home_punt_yards", "away_punt_yards").sum()
        # No punt count col; approximate from punt_yards (avg ~40 yards/punt)
        punt_count = n  # normalized value: punt_yards / n ≈ avg punt yardage

        return {
            "fg_pct": float(fg_made / fg_att) if fg_att > 0 else 0.0,
            "punt_avg": float(punt_yds / (n * 1.0)) if n > 0 else 0.0,
            "kick_return_avg": 0.0,
        }

    def _weather_impact(self, game: dict[str, Any]) -> dict[str, float]:
        """Weather features that affect gameplay (wind, temp, dome)."""
        return {
            "temperature": float(pd.to_numeric(game.get("temperature", 70), errors="coerce") or 70.0),
            "wind_speed": float(pd.to_numeric(game.get("wind_speed", 0), errors="coerce") or 0.0),
            "is_dome": 1.0 if str(game.get("dome", game.get("is_dome", ""))).lower() in ("true", "1", "yes") else 0.0,
            "is_rain": 1.0 if "rain" in str(game.get("weather", "")).lower() else 0.0,
            "is_snow": 1.0 if "snow" in str(game.get("weather", "")).lower() else 0.0,
        }

    def _get_ps_team_index(self, season: int) -> dict[str, pd.DataFrame]:
        """Build and cache per-team player stats index for O(1) team lookups."""
        if not hasattr(self, "_ps_team_index_cache"):
            self._ps_team_index_cache: dict[str, dict[str, pd.DataFrame]] = {}
        key = str(season)
        if key not in self._ps_team_index_cache:
            ps = self.load_player_stats(season)
            if ps.empty or "team_id" not in ps.columns:
                self._ps_team_index_cache[key] = {}
            else:
                ps = ps.copy()
                ps["_team_id_str"] = ps["team_id"].astype(str)
                if "date" in ps.columns:
                    ps["_date_dt"] = pd.to_datetime(ps["date"], errors="coerce")
                    ps.sort_values("_date_dt", inplace=True, ignore_index=True)
                self._ps_team_index_cache[key] = {
                    tid: grp for tid, grp in ps.groupby("_team_id_str")
                }
        return self._ps_team_index_cache[key]

    def _nfl_player_features(
        self,
        team_id: str,
        season: int,
        date: str,
    ) -> dict[str, float]:
        """NFL-specific individual player stats: QB rating, rushing, defensive production."""
        defaults = {
            "nfl_ps_qb_rating": 0.0,
            "nfl_ps_pass_yds_pg": 0.0,
            "nfl_ps_rush_yds_pg": 0.0,
            "nfl_ps_sacks_pg": 0.0,
            "nfl_ps_def_int_pg": 0.0,
            "nfl_ps_rush_td_pg": 0.0,
            "nfl_ps_yds_per_attempt": 0.0,
            "nfl_ps_completion_pct": 0.0,
            "nfl_ps_yds_per_carry": 0.0,
            "nfl_ps_pass_td_pg": 0.0,
            "nfl_ps_fumbles_lost_pg": 0.0,
            "nfl_ps_kick_return_avg": 0.0,
            "nfl_ps_punt_return_avg": 0.0,
        }
        team_index = self._get_ps_team_index(season)
        if not team_index:
            return defaults

        tid = str(team_id)
        abbrev = self._resolve_team_abbrev(tid)
        team_ps = team_index.get(abbrev)
        if team_ps is None:
            team_ps = team_index.get(tid)
        if team_ps is None or team_ps.empty:
            # fallback: try direct numeric id mapping
            mapped_id = self._team_id_map.get(tid)
            if mapped_id:
                team_ps = team_index.get(str(mapped_id))

        if team_ps is None or team_ps.empty or "date" not in team_ps.columns:
            return defaults

        # Date filter: only use games before this game (O(k) on small per-team subset)
        game_date = pd.to_datetime(date, errors="coerce")
        if not pd.isna(game_date):
            date_col = team_ps.get("_date_dt", pd.to_datetime(team_ps["date"], errors="coerce"))
            team_ps = team_ps.loc[date_col < game_date]

        if team_ps.empty:
            return defaults

        def _mean(col: str) -> float:
            if col in team_ps.columns:
                return float(pd.to_numeric(team_ps[col], errors="coerce").fillna(0).mean())
            return 0.0

        # Passing: QB passer rating and yards per game
        qb_mask = pd.to_numeric(team_ps.get("pass_att", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0
        qb_rows = team_ps.loc[qb_mask]
        qb_rating = float(pd.to_numeric(qb_rows.get("pass_rating", pd.Series(dtype=float)), errors="coerce").fillna(0).mean()) if not qb_rows.empty else 0.0
        qb_yds = float(pd.to_numeric(qb_rows.get("pass_yds", pd.Series(dtype=float)), errors="coerce").fillna(0).mean()) if not qb_rows.empty else 0.0

        # Passing efficiency: yards per attempt, completion %
        if not qb_rows.empty:
            total_pass_yds = pd.to_numeric(qb_rows.get("pass_yds", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
            total_pass_att = pd.to_numeric(qb_rows.get("pass_att", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
            total_pass_cmp = pd.to_numeric(qb_rows.get("pass_cmp", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
            yds_per_att = float(total_pass_yds / total_pass_att) if total_pass_att > 0 else 0.0
            comp_pct = float(total_pass_cmp / total_pass_att) * 100.0 if total_pass_att > 0 else 0.0
            pass_td_pg = float(pd.to_numeric(qb_rows.get("pass_td", pd.Series(dtype=float)), errors="coerce").fillna(0).mean())
        else:
            yds_per_att = 0.0
            comp_pct = 0.0
            pass_td_pg = 0.0

        # Rushing: average rush yards per game and efficiency
        rush_yds = _mean("rush_yds")
        rush_td = _mean("rush_td")
        rush_rows = team_ps.loc[pd.to_numeric(team_ps.get("rush_att", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0] if "rush_att" in team_ps.columns else team_ps.iloc[0:0]
        if not rush_rows.empty:
            total_rush_yds = pd.to_numeric(rush_rows["rush_yds"], errors="coerce").fillna(0).sum()
            total_rush_att = pd.to_numeric(rush_rows["rush_att"], errors="coerce").fillna(0).sum()
            yds_per_carry = float(total_rush_yds / total_rush_att) if total_rush_att > 0 else 0.0
        else:
            yds_per_carry = 0.0

        # Turnovers: sacks, interceptions thrown, fumbles lost
        sacks = _mean("sacks")
        def_int = _mean("pass_int")  # QB interceptions thrown (offensive turnover metric)
        fumbles_lost = _mean("fumbles_lost")

        # Special teams: kick/punt return efficiency
        kr_rows = team_ps.loc[pd.to_numeric(team_ps.get("kr_no", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0] if "kr_no" in team_ps.columns else team_ps.iloc[0:0]
        kick_return_avg = float(pd.to_numeric(kr_rows["kr_avg"], errors="coerce").fillna(0).mean()) if not kr_rows.empty and "kr_avg" in kr_rows.columns else 0.0
        pr_rows = team_ps.loc[pd.to_numeric(team_ps.get("pr_no", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0] if "pr_no" in team_ps.columns else team_ps.iloc[0:0]
        punt_return_avg = float(pd.to_numeric(pr_rows["pr_avg"], errors="coerce").fillna(0).mean()) if not pr_rows.empty and "pr_avg" in pr_rows.columns else 0.0

        return {
            "nfl_ps_qb_rating": qb_rating,
            "nfl_ps_pass_yds_pg": qb_yds,
            "nfl_ps_rush_yds_pg": rush_yds,
            "nfl_ps_sacks_pg": sacks,
            "nfl_ps_def_int_pg": def_int,
            "nfl_ps_rush_td_pg": rush_td,
            "nfl_ps_yds_per_attempt": yds_per_att,
            "nfl_ps_completion_pct": comp_pct,
            "nfl_ps_yds_per_carry": yds_per_carry,
            "nfl_ps_pass_td_pg": pass_td_pg,
            "nfl_ps_fumbles_lost_pg": fumbles_lost,
            "nfl_ps_kick_return_avg": kick_return_avg,
            "nfl_ps_punt_return_avg": punt_return_avg,
        }

    def _nfl_standings_features(self, team_id: str, season: int) -> dict[str, float]:
        """NFL standings: win%, points margin, home/away record splits, rank, streak."""
        defaults = {
            "nfl_stnd_win_pct": 0.5,
            "nfl_stnd_pts_margin": 0.0,
            "nfl_stnd_home_win_pct": 0.5,
            "nfl_stnd_away_win_pct": 0.5,
            "nfl_stnd_overall_rank": 16.0,
            "nfl_stnd_streak": 0.0,
            "nfl_stnd_gp": 0.0,
        }
        standings = self.load_standings(season)
        if standings.empty or "team_id" not in standings.columns:
            return defaults

        row_df = standings.loc[standings["team_id"].astype(str) == str(team_id)]
        if row_df.empty:
            return defaults
        row = row_df.iloc[0]

        wins = self._n(row.get("wins"))
        losses = self._n(row.get("losses"))
        ties = self._n(row.get("ties"))
        gp = wins + losses + ties
        win_pct = (wins + 0.5 * ties) / gp if gp > 0 else 0.5

        pf = self._n(row.get("points_for"))
        pa = self._n(row.get("points_against"))
        pts_margin = (pf - pa) / gp if gp > 0 else 0.0

        overall_rank = self._n(row.get("overall_rank", 16.0), 16.0)

        # Streak: "W4" → +4, "L1" → -1
        streak_val = 0.0
        streak_str = str(row.get("streak", ""))
        if streak_str and streak_str[0] in ("W", "w"):
            streak_val = self._n(streak_str[1:])
        elif streak_str and streak_str[0] in ("L", "l"):
            streak_val = -self._n(streak_str[1:])

        # Home/Away record: "8-0" → 1.0, "5-4" → 0.556
        def _parse_record(rec_str: str) -> float:
            try:
                parts = str(rec_str).split("-")
                w, l = float(parts[0]), float(parts[1])
                t = float(parts[2]) if len(parts) > 2 else 0.0
                total = w + l + t
                return (w + 0.5 * t) / total if total > 0 else 0.5
            except (ValueError, IndexError):
                return 0.5

        home_pct = _parse_record(row.get("home_record", ""))
        away_pct = _parse_record(row.get("away_record", ""))

        return {
            "nfl_stnd_win_pct": win_pct,
            "nfl_stnd_pts_margin": pts_margin,
            "nfl_stnd_home_win_pct": home_pct,
            "nfl_stnd_away_win_pct": away_pct,
            "nfl_stnd_overall_rank": overall_rank,
            "nfl_stnd_streak": streak_val,
            "nfl_stnd_gp": gp,
        }

    def _qb_impact_features(
        self,
        team_id: str,
        season: int,
        date: str,
    ) -> dict[str, float]:
        """Identify QB-level performance signals for the most recent game.

        Looks up player_stats for the team and extracts starter QB metrics
        (pass_yds, pass_td, pass_int, pass_rating) over the last 5 games.
        These proxy current QB form, which is the single strongest predictor
        in NFL outcomes.
        """
        defaults = {
            "qb_pass_yds_avg": 220.0,
            "qb_pass_td_avg": 1.5,
            "qb_pass_int_avg": 0.8,
            "qb_passer_rating_avg": 85.0,
            "qb_form_score": 0.0,
        }
        try:
            ps = self.load_player_stats(season)
            if ps.empty:
                return defaults
            if "team_id" not in ps.columns:
                return defaults

            # Resolve numeric team_id → abbreviation (player_stats uses abbrevs)
            abbrev = self._resolve_team_abbrev(str(team_id))
            team_ps = ps.loc[ps["team_id"].astype(str).isin([str(team_id), abbrev])].copy()
            if team_ps.empty:
                return defaults

            # Identify QB rows: either position=='QB' or has significant passing attempts
            pos_col = ps.get("position", pd.Series(dtype=str)) if "position" in ps.columns else pd.Series(dtype=str)
            is_qb_by_pos = team_ps.get("position", pd.Series(dtype=str)).astype(str).str.upper() == "QB"
            pass_att = pd.to_numeric(team_ps.get("pass_att", pd.Series(0)), errors="coerce").fillna(0)
            is_qb_by_pass = pass_att >= 5  # at least 5 attempts = QB game
            team_ps = team_ps.loc[is_qb_by_pos | is_qb_by_pass].copy()
            if team_ps.empty:
                return defaults

            if "date" in team_ps.columns:
                team_ps["_dt"] = pd.to_datetime(team_ps["date"], errors="coerce")
                game_dt = pd.to_datetime(date, errors="coerce")
                if pd.notna(game_dt):
                    team_ps = team_ps.loc[team_ps["_dt"] < game_dt]

            if team_ps.empty:
                return defaults

            if "date" in team_ps.columns:
                team_ps = team_ps.sort_values("_dt").tail(5)

            def _avg(col: str, fallback: float) -> float:
                if col not in team_ps.columns:
                    return fallback
                vals = pd.to_numeric(team_ps[col], errors="coerce").dropna()
                return float(vals.mean()) if not vals.empty else fallback

            yds = _avg("pass_yds", 220.0)
            tds = _avg("pass_td", 1.5)
            ints = _avg("pass_int", 0.8)
            rtg = _avg("pass_rating", 85.0)

            # Composite form score: high yards/TD good, ints bad
            form_score = (yds / 300.0 + tds / 2.0 - ints / 1.0 + (rtg - 85.0) / 50.0) / 4.0

            return {
                "qb_pass_yds_avg": yds,
                "qb_pass_td_avg": tds,
                "qb_pass_int_avg": ints,
                "qb_passer_rating_avg": rtg,
                "qb_form_score": form_score,
            }
        except Exception:
            return defaults

    def _divisional_context_features(
        self,
        home_team_id: str,
        away_team_id: str,
        season: int,
        game: dict[str, Any],
    ) -> dict[str, float]:
        """NFL divisional/conference matchup context and season stage features.

        Divisional games are historically tighter (teams scout each other every year).
        Conference games matter for playoff seeding. Season week indicates fatigue /
        lineup rotation risk (late-season, week >= 15).
        """
        defaults: dict[str, float] = {
            "is_divisional_game": 0.0,
            "is_conference_game": 0.0,
            "is_postseason": 0.0,
            "nfl_week": 9.0,
            "season_progress": 0.5,
            "is_late_season": 0.0,
            "home_clinch_status": 0.0,
            "away_clinch_status": 0.0,
        }
        try:
            standings = self.load_standings(season)
            if standings.empty or "team_id" not in standings.columns:
                return defaults

            def _row(tid: str) -> pd.Series:
                r = standings.loc[standings["team_id"].astype(str) == str(tid)]
                return r.iloc[0] if not r.empty else pd.Series(dtype=object)

            h_row = _row(home_team_id)
            a_row = _row(away_team_id)

            h_conf = str(h_row.get("conference", "")) if not h_row.empty else ""
            a_conf = str(a_row.get("conference", "")) if not a_row.empty else ""
            h_div = str(h_row.get("division", "")) if not h_row.empty else ""
            a_div = str(a_row.get("division", "")) if not a_row.empty else ""

            same_conf = 1.0 if h_conf and a_conf and h_conf == a_conf else 0.0
            same_div = 1.0 if h_div and a_div and h_div == a_div else 0.0

            # Clinch status: 0 = none, 1 = something clinched (playoff spot / division / bye)
            h_clinch = float(bool(self._n(h_row.get("clinch_status", 0)))) if not h_row.empty else 0.0
            a_clinch = float(bool(self._n(a_row.get("clinch_status", 0)))) if not a_row.empty else 0.0

            # Season week from game record (1-18 regular, 1-4 post)
            raw_week = game.get("week")
            nfl_week = float(pd.to_numeric(raw_week, errors="coerce")) if raw_week is not None else 9.0
            if pd.isna(nfl_week):
                nfl_week = 9.0

            season_type = str(game.get("season_type", "regular")).lower()
            is_post = 1.0 if "post" in season_type else 0.0
            season_progress = min(nfl_week / 18.0, 1.0) if not is_post else 1.0
            is_late = 1.0 if nfl_week >= 15 else 0.0

            return {
                "is_divisional_game": same_div,
                "is_conference_game": same_conf,
                "is_postseason": is_post,
                "nfl_week": nfl_week,
                "season_progress": season_progress,
                "is_late_season": is_late,
                "home_clinch_status": h_clinch,
                "away_clinch_status": a_clinch,
            }
        except Exception:
            return defaults

    def _quality_weighted_form_nfl(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 8,
    ) -> dict[str, float]:
        """Win form weighted by opponent quality (NFL 16-18 game season — shorter window).

        Uses single lookup per unique opponent — O(n × log n).
        """
        defaults = {"nfl_quality_form": 0.0, "nfl_quality_win_rate": 0.5}
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return defaults

        is_home = recent["home_team_id"].astype(str) == str(team_id)
        h_sc = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        a_sc = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)
        tm_sc = np.where(is_home, h_sc, a_sc)
        op_sc = np.where(is_home, a_sc, h_sc)
        wins = (tm_sc > op_sc).astype(float)
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))

        opp_qual: dict[str, float] = {}
        for opp_id in set(opp_ids):
            opp_hist = self._team_games_before(games, str(opp_id), date, limit=16)
            if opp_hist.empty:
                opp_qual[str(opp_id)] = 0.5
            else:
                oh = opp_hist["home_team_id"].astype(str) == str(opp_id)
                ohs = pd.to_numeric(opp_hist["home_score"], errors="coerce").fillna(0)
                oas = pd.to_numeric(opp_hist["away_score"], errors="coerce").fillna(0)
                ow = np.where(oh, ohs > oas, oas > ohs).astype(float)
                opp_qual[str(opp_id)] = float(ow.mean()) if len(ow) else 0.5

        opp_arr = np.array([opp_qual.get(str(o), 0.5) for o in opp_ids])
        n = max(len(recent), 1)
        quality_form = float(np.dot(wins * 2.0 - 1.0, opp_arr) / n)
        quality_win_rate = float(np.dot(wins, opp_arr) / n)
        return {"nfl_quality_form": quality_form, "nfl_quality_win_rate": quality_win_rate}

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self._load_all_games()
        current_season_games = self.load_games(season)
        odds_df = self.load_odds(season)

        h_id, a_id = self._resolve_game_team_ids(game, current_season_games)
        date = str(game.get("date", ""))
        game_id = str(game.get("id", ""))
        home_team = str(game.get("home_team", ""))
        away_team = str(game.get("away_team", ""))

        features: dict[str, Any] = {
            "game_id": game_id,
            "date": date,
            "home_team_id": h_id,
            "away_team_id": a_id,
            "home_score": pd.to_numeric(game.get("home_score"), errors="coerce"),
            "away_score": pd.to_numeric(game.get("away_score"), errors="coerce"),
            # Period/quarter scores — passed through as targets for extra-market models
            # NFL also has home_h1_score/home_h2_score which map to Q1+Q2 halftime
            "home_q1": pd.to_numeric(game.get("home_q1"), errors="coerce"),
            "home_q2": pd.to_numeric(game.get("home_q2"), errors="coerce"),
            "home_q3": pd.to_numeric(game.get("home_q3"), errors="coerce"),
            "home_q4": pd.to_numeric(game.get("home_q4"), errors="coerce"),
            "home_ot": pd.to_numeric(game.get("home_ot"), errors="coerce"),
            # Precomputed half scores for halftime market training
            "home_h1": pd.to_numeric(game.get("home_h1_score"), errors="coerce"),
            "home_h2": pd.to_numeric(game.get("home_h2_score"), errors="coerce"),
            "away_q1": pd.to_numeric(game.get("away_q1"), errors="coerce"),
            "away_q2": pd.to_numeric(game.get("away_q2"), errors="coerce"),
            "away_q3": pd.to_numeric(game.get("away_q3"), errors="coerce"),
            "away_q4": pd.to_numeric(game.get("away_q4"), errors="coerce"),
            "away_ot": pd.to_numeric(game.get("away_ot"), errors="coerce"),
            "away_h1": pd.to_numeric(game.get("away_h1_score"), errors="coerce"),
            "away_h2": pd.to_numeric(game.get("away_h2_score"), errors="coerce"),
            # Turnovers (INT + fumbles lost) — target for total turnovers O/U market
            "home_turnovers_game": pd.to_numeric(game.get("home_turnovers"), errors="coerce"),
            "away_turnovers_game": pd.to_numeric(game.get("away_turnovers"), errors="coerce"),
        }

        # Common features
        h_form = self.team_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        # Home/Away split rolling form
        h_home_form = self.home_away_form(h_id, date, games_df, is_home=True)
        features.update({f"home_home_{k}": v for k, v in h_home_form.items()})
        a_away_form = self.home_away_form(a_id, date, games_df, is_home=False)
        features.update({f"away_away_{k}": v for k, v in a_away_form.items()})
        features["ha_win_pct_diff"] = h_home_form["ha_win_pct"] - a_away_form["ha_win_pct"]
        features["ha_ppg_diff"] = h_home_form["ha_ppg"] - a_away_form["ha_ppg"]

        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)

        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]

        h_splits = self.home_away_splits(h_id, games_df, season)
        features["home_home_win_pct"] = h_splits["home_win_pct"]
        a_splits = self.home_away_splits(a_id, games_df, season)
        features["away_away_win_pct"] = a_splits["away_win_pct"]

        # Rest
        features["home_rest_days"] = float(self.rest_days(h_id, date, games_df))
        features["away_rest_days"] = float(self.rest_days(a_id, date, games_df))
        features["rest_advantage"] = features["home_rest_days"] - features["away_rest_days"]
        features["short_week_home"] = 1.0 if features["home_rest_days"] < 7 else 0.0
        features["short_week_away"] = 1.0 if features["away_rest_days"] < 7 else 0.0

        # EPA
        h_epa = self._epa_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_epa.items()})
        a_epa = self._epa_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_epa.items()})

        # Yardage & stat averages
        h_stats = self._team_stat_averages(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_stats.items()})
        a_stats = self._team_stat_averages(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_stats.items()})

        # Efficiency
        h_eff = self._efficiency_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_eff.items()})
        a_eff = self._efficiency_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_eff.items()})

        # Stat differentials (key predictive signals)
        features["total_yds_diff"] = features.get("home_total_yds_pg", 0.0) - features.get("away_total_yds_pg", 0.0)
        features["turnovers_diff"] = features.get("away_turnovers_pg", 0.0) - features.get("home_turnovers_pg", 0.0)
        features["sacks_diff"] = features.get("home_sacks_pg", 0.0) - features.get("away_sacks_pg", 0.0)
        features["tds_diff"] = features.get("home_total_tds_pg", 0.0) - features.get("away_total_tds_pg", 0.0)
        features["yards_per_play_diff"] = features.get("home_yards_per_play", 0.0) - features.get("away_yards_per_play", 0.0)
        features["first_downs_diff"] = features.get("home_first_downs_pg", 0.0) - features.get("away_first_downs_pg", 0.0)
        # EPA differentials — net expected points advantage
        features["epa_pass_off_diff"] = features.get("home_epa_pass_off", 0.0) - features.get("away_epa_pass_off", 0.0)
        features["epa_rush_off_diff"] = features.get("home_epa_rush_off", 0.0) - features.get("away_epa_rush_off", 0.0)
        features["epa_total_off_diff"] = features.get("home_epa_total_off", 0.0) - features.get("away_epa_total_off", 0.0)
        features["epa_net_diff"] = features.get("home_epa_total_off", 0.0) - features.get("away_epa_total_off", 0.0) \
            - features.get("home_epa_total_def", 0.0) + features.get("away_epa_total_def", 0.0)
        # Air yards & YAC differential (passing depth and after-catch efficiency)
        features["air_yards_diff"] = features.get("home_air_yards_pg", 0.0) - features.get("away_air_yards_pg", 0.0)
        features["yac_diff"] = features.get("home_yac_pg", 0.0) - features.get("away_yac_pg", 0.0)
        # Efficiency differentials
        features["completion_pct_diff"] = features.get("home_completion_pct", 0.0) - features.get("away_completion_pct", 0.0)
        features["third_down_pct_diff"] = features.get("home_third_down_pct", 0.0) - features.get("away_third_down_pct", 0.0)
        features["red_zone_pct_diff"] = features.get("home_red_zone_pct", 0.0) - features.get("away_red_zone_pct", 0.0)
        features["turnover_margin_diff"] = features.get("home_turnover_margin", 0.0) - features.get("away_turnover_margin", 0.0)
        features["fumbles_lost_diff"] = features.get("away_fumbles_lost_pg", 0.0) - features.get("home_fumbles_lost_pg", 0.0)  # positive = home advantage
        features["scoring_efficiency_diff"] = features.get("home_scoring_efficiency", 0.0) - features.get("away_scoring_efficiency", 0.0)

        # Rush/pass balance
        h_bal = self._rushing_passing_balance(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_bal.items()})
        a_bal = self._rushing_passing_balance(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_bal.items()})

        # Special teams
        h_st = self._special_teams(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_st.items()})
        a_st = self._special_teams(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_st.items()})

        # Weather
        weather = self._weather_impact(game)
        features.update(weather)

        # ELO ratings
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Odds — with team-name fallback
        odds = self._odds_features(game_id, odds_df, home_team=home_team, away_team=away_team, date=date)
        features.update(odds)

        # Market movement enrichment
        market = self._market_signal_features(game_id, season, home_team=home_team, away_team=away_team, date=date)
        features.update(market)

        # Schedule fatigue enrichment
        h_fat = self._schedule_fatigue_features(game_id, h_id, season)
        features.update({f"home_{k}": v for k, v in h_fat.items()})
        a_fat = self._schedule_fatigue_features(game_id, a_id, season)
        features.update({f"away_{k}": v for k, v in a_fat.items()})
        features["fatigue_score_diff"] = h_fat["fatigue_score"] - a_fat["fatigue_score"]

        # NFL Player stats (QB rating, rushing, defensive production)
        h_nfl_ps = self._nfl_player_features(h_id, season, date)
        features.update({f"home_{k}": v for k, v in h_nfl_ps.items()})
        a_nfl_ps = self._nfl_player_features(a_id, season, date)
        features.update({f"away_{k}": v for k, v in a_nfl_ps.items()})
        # NFL player stats differentials
        for key in h_nfl_ps:
            features[f"{key}_diff"] = h_nfl_ps[key] - a_nfl_ps[key]

        # Season kicker FG% from team_stats (100% filled for NFL)
        team_stats = self.load_team_stats(season)
        if not team_stats.empty and "team_id" in team_stats.columns and "field_goal_pct" in team_stats.columns:
            ts = team_stats.copy()
            ts["_tid"] = ts["team_id"].astype(str)
            h_row = ts.loc[ts["_tid"] == str(h_id)]
            a_row = ts.loc[ts["_tid"] == str(a_id)]
            h_kfg = float(pd.to_numeric(h_row["field_goal_pct"].iloc[0], errors="coerce")) / 100.0 if not h_row.empty else 0.0
            a_kfg = float(pd.to_numeric(a_row["field_goal_pct"].iloc[0], errors="coerce")) / 100.0 if not a_row.empty else 0.0
        else:
            h_kfg, a_kfg = 0.0, 0.0
        features["home_kicker_fg_pct"] = h_kfg
        features["away_kicker_fg_pct"] = a_kfg
        features["kicker_fg_pct_diff"] = h_kfg - a_kfg

        # ── Quarter Rolling Stats (NFL: Q1/Q2/Q3/Q4) ─────
        h_per = self._period_rolling_stats(h_id, date, games_df, n=10, period_scheme="quarters")
        features.update({f"home_{k}": v for k, v in h_per.items()})
        a_per = self._period_rolling_stats(a_id, date, games_df, n=10, period_scheme="quarters")
        features.update({f"away_{k}": v for k, v in a_per.items()})
        features["period_first_ppg_diff"] = h_per["period_first_ppg"] - a_per["period_first_ppg"]
        features["period_first_win_pct_diff"] = h_per["period_first_win_pct"] - a_per["period_first_win_pct"]
        features["period_first_half_win_pct_diff"] = h_per["period_first_half_win_pct"] - a_per["period_first_half_win_pct"]
        features["period_comeback_diff"] = h_per["period_comeback_rate"] - a_per["period_comeback_rate"]
        features["period_first_opp_ppg_diff"] = h_per["period_first_opp_ppg"] - a_per["period_first_opp_ppg"]
        features["period_second_half_ppg_diff"] = h_per["period_second_half_ppg"] - a_per["period_second_half_ppg"]
        features["period_ot_rate_diff"] = h_per["period_ot_rate"] - a_per["period_ot_rate"]

        # Scoring trends (last 5 games)
        h_last5 = self._scoring_last_n(h_id, date, games_df, n=5)
        features.update({f"home_{k}": v for k, v in h_last5.items()})
        a_last5 = self._scoring_last_n(a_id, date, games_df, n=5)
        features.update({f"away_{k}": v for k, v in a_last5.items()})
        features["last5_ppg_diff"] = h_last5["last_n_ppg"] - a_last5["last_n_ppg"]
        features["last5_margin_diff"] = h_last5["last_n_margin"] - a_last5["last_n_margin"]

        # Strength of schedule (returns float = avg opponent win%)
        h_sos = self._strength_of_schedule(h_id, date, games_df, season)
        a_sos = self._strength_of_schedule(a_id, date, games_df, season)
        features["home_sos_rating"] = h_sos
        features["away_sos_rating"] = a_sos
        features["sos_diff"] = h_sos - a_sos

        # Injury burden (uses shared base._injury_features)
        h_inj = self._injury_features(h_id, season)
        features.update({f"home_{k}": v for k, v in h_inj.items()})
        a_inj = self._injury_features(a_id, season)
        features.update({f"away_{k}": v for k, v in a_inj.items()})
        features["injury_advantage"] = a_inj["injury_severity_score"] - h_inj["injury_severity_score"]

        # QB performance features (rolling 5-game average)
        h_qb = self._qb_impact_features(h_id, season, date)
        features.update({f"home_{k}": v for k, v in h_qb.items()})
        a_qb = self._qb_impact_features(a_id, season, date)
        features.update({f"away_{k}": v for k, v in a_qb.items()})
        features["qb_form_score_diff"] = h_qb["qb_form_score"] - a_qb["qb_form_score"]
        features["qb_rating_diff"] = h_qb["qb_passer_rating_avg"] - a_qb["qb_passer_rating_avg"]

        # NFL standings (win%, pts margin, home/away splits, rank, streak)
        h_stnd = self._nfl_standings_features(h_id, season)
        features.update({f"home_{k}": v for k, v in h_stnd.items()})
        a_stnd = self._nfl_standings_features(a_id, season)
        features.update({f"away_{k}": v for k, v in a_stnd.items()})
        features["nfl_stnd_rank_diff"] = a_stnd["nfl_stnd_overall_rank"] - h_stnd["nfl_stnd_overall_rank"]
        features["nfl_stnd_win_pct_diff"] = h_stnd["nfl_stnd_win_pct"] - a_stnd["nfl_stnd_win_pct"]

        # Divisional / conference / season-stage context
        div_ctx = self._divisional_context_features(h_id, a_id, season, game)
        features.update(div_ctx)

        # Quality-weighted form (win rate weighted by opponent quality)
        h_qf = self._quality_weighted_form_nfl(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_qf.items()})
        a_qf = self._quality_weighted_form_nfl(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_qf.items()})
        features["nfl_quality_form_diff"] = h_qf["nfl_quality_form"] - a_qf["nfl_quality_form"]
        features["nfl_quality_win_rate_diff"] = h_qf["nfl_quality_win_rate"] - a_qf["nfl_quality_win_rate"]

        return features

    def get_feature_names(self) -> list[str]:
        return [
            # Home/away form
            "home_form_win_pct", "home_form_ppg", "home_form_opp_ppg",
            "home_form_avg_margin", "home_form_games_played",
            "away_form_win_pct", "away_form_ppg", "away_form_opp_ppg",
            "away_form_avg_margin", "away_form_games_played",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            # Momentum & splits
            "home_momentum", "away_momentum", "momentum_diff",
            "home_home_win_pct", "away_away_win_pct",
            # Home/Away rolling form
            "home_home_ha_win_pct", "home_home_ha_ppg", "home_home_ha_opp_ppg",
            "home_home_ha_avg_margin", "home_home_ha_games_played",
            "away_away_ha_win_pct", "away_away_ha_ppg", "away_away_ha_opp_ppg",
            "away_away_ha_avg_margin", "away_away_ha_games_played",
            "ha_win_pct_diff", "ha_ppg_diff",
            # Rest
            "home_rest_days", "away_rest_days", "rest_advantage",
            "short_week_home", "short_week_away",
            # EPA
            "home_epa_pass_off", "home_epa_rush_off", "home_epa_total_off",
            "home_epa_pass_def", "home_epa_rush_def", "home_epa_total_def",
            "away_epa_pass_off", "away_epa_rush_off", "away_epa_total_off",
            "away_epa_pass_def", "away_epa_rush_def", "away_epa_total_def",
            # Stat averages
            "home_pass_yds_pg", "home_rush_yds_pg", "home_total_yds_pg",
            "home_pass_yds_allowed_pg", "home_rush_yds_allowed_pg",
            "home_turnovers_pg", "home_takeaways_pg", "home_sacks_pg", "home_penalties_pg",
            "home_sacks_allowed_pg", "home_completion_pct", "home_first_downs_pg", "home_penalty_yards_pg",
            "away_pass_yds_pg", "away_rush_yds_pg", "away_total_yds_pg",
            "away_pass_yds_allowed_pg", "away_rush_yds_allowed_pg",
            "away_turnovers_pg", "away_takeaways_pg", "away_sacks_pg", "away_penalties_pg",
            "away_sacks_allowed_pg", "away_completion_pct", "away_first_downs_pg", "away_penalty_yards_pg",
            # Efficiency
            "home_third_down_pct", "home_red_zone_pct", "home_fourth_down_pct",
            "home_time_of_possession", "home_turnover_margin", "home_defensive_td_pg",
            "away_third_down_pct", "away_red_zone_pct", "away_fourth_down_pct",
            "away_time_of_possession", "away_turnover_margin", "away_defensive_td_pg",
            # Rush/pass balance
            "home_rush_pct", "home_pass_pct", "home_yards_per_play",
            "away_rush_pct", "away_pass_pct", "away_yards_per_play",
            # Special teams
            "home_fg_pct", "home_punt_avg", "home_kick_return_avg",
            "away_fg_pct", "away_punt_avg", "away_kick_return_avg",
            # Weather
            "temperature", "wind_speed", "is_dome", "is_rain", "is_snow",
            # ELO ratings
            "home_elo", "home_elo_diff", "home_elo_expected_win",
            "away_elo", "away_elo_diff", "away_elo_expected_win",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
            # Market signals
            "market_aggregate_abs_move", "market_h2h_home_move", "market_h2h_away_move",
            "market_spread_home_move", "market_total_line_move",
            "market_observation_count", "market_source_count",
            "market_regime_stable", "market_regime_moving", "market_regime_volatile",
            # Schedule fatigue
            "home_fatigue_rest_days", "home_fatigue_is_back_to_back", "home_fatigue_games_last_7d",
            "home_fatigue_games_last_14d", "home_fatigue_score",
            "away_fatigue_rest_days", "away_fatigue_is_back_to_back", "away_fatigue_games_last_7d",
            "away_fatigue_games_last_14d", "away_fatigue_score", "fatigue_score_diff",
            # NFL Player stats
            "home_nfl_ps_qb_rating", "home_nfl_ps_pass_yds_pg", "home_nfl_ps_rush_yds_pg",
            "home_nfl_ps_sacks_pg", "home_nfl_ps_def_int_pg", "home_nfl_ps_rush_td_pg",
            "home_nfl_ps_yds_per_attempt", "home_nfl_ps_completion_pct", "home_nfl_ps_yds_per_carry",
            "home_nfl_ps_pass_td_pg", "home_nfl_ps_fumbles_lost_pg",
            "home_nfl_ps_kick_return_avg", "home_nfl_ps_punt_return_avg",
            "away_nfl_ps_qb_rating", "away_nfl_ps_pass_yds_pg", "away_nfl_ps_rush_yds_pg",
            "away_nfl_ps_sacks_pg", "away_nfl_ps_def_int_pg", "away_nfl_ps_rush_td_pg",
            "away_nfl_ps_yds_per_attempt", "away_nfl_ps_completion_pct", "away_nfl_ps_yds_per_carry",
            "away_nfl_ps_pass_td_pg", "away_nfl_ps_fumbles_lost_pg",
            "away_nfl_ps_kick_return_avg", "away_nfl_ps_punt_return_avg",
            # NFL player stats differentials
            "nfl_ps_qb_rating_diff", "nfl_ps_pass_yds_pg_diff", "nfl_ps_rush_yds_pg_diff",
            "nfl_ps_sacks_pg_diff", "nfl_ps_def_int_pg_diff", "nfl_ps_rush_td_pg_diff",
            "nfl_ps_yds_per_attempt_diff", "nfl_ps_completion_pct_diff", "nfl_ps_yds_per_carry_diff",
            "nfl_ps_pass_td_pg_diff", "nfl_ps_fumbles_lost_pg_diff",
            "nfl_ps_kick_return_avg_diff", "nfl_ps_punt_return_avg_diff",
            # Kicker FG% from season team_stats
            "home_kicker_fg_pct", "away_kicker_fg_pct", "kicker_fg_pct_diff",
            # Injury burden
            "home_injury_count", "home_injury_severity_score", "home_injury_out_count",
            "home_injury_dtd_count", "home_injury_questionable_count",
            "away_injury_count", "away_injury_severity_score", "away_injury_out_count",
            "away_injury_dtd_count", "away_injury_questionable_count",
            "injury_advantage",
            # Stat differentials
            "total_yds_diff", "turnovers_diff", "sacks_diff", "tds_diff",
            "yards_per_play_diff", "first_downs_diff",
            # EPA differentials
            "epa_pass_off_diff", "epa_rush_off_diff", "epa_total_off_diff", "epa_net_diff",
            # Air yards & YAC differentials
            "air_yards_diff", "yac_diff",
            # Efficiency differentials
            "completion_pct_diff", "third_down_pct_diff", "red_zone_pct_diff", "turnover_margin_diff",
            "fumbles_lost_diff", "scoring_efficiency_diff",
            # Air yards per play
            "home_air_yards_pg", "home_yac_pg", "home_air_yards_allowed_pg", "home_yac_allowed_pg",
            "away_air_yards_pg", "away_yac_pg", "away_air_yards_allowed_pg", "away_yac_allowed_pg",
            # New stat averages
            "home_passing_tds_pg", "home_rushing_tds_pg", "home_total_tds_pg",
            "home_tds_allowed_pg", "home_receiving_yds_pg",
            "away_passing_tds_pg", "away_rushing_tds_pg", "away_total_tds_pg",
            "away_tds_allowed_pg", "away_receiving_yds_pg",
            # Quarter rolling stats
            "home_period_first_ppg", "away_period_first_ppg",
            "home_period_first_opp_ppg", "away_period_first_opp_ppg",
            "home_period_first_win_pct", "away_period_first_win_pct",
            "home_period_first_half_ppg", "away_period_first_half_ppg",
            "home_period_first_half_opp_ppg", "away_period_first_half_opp_ppg",
            "home_period_first_half_win_pct", "away_period_first_half_win_pct",
            "home_period_second_half_ppg", "away_period_second_half_ppg",
            "home_period_comeback_rate", "away_period_comeback_rate",
            "home_period_ot_rate", "away_period_ot_rate",
            "period_first_ppg_diff", "period_first_opp_ppg_diff",
            "period_first_win_pct_diff", "period_first_half_win_pct_diff",
            "period_second_half_ppg_diff", "period_comeback_diff", "period_ot_rate_diff",
            # New stat averages: interceptions, tackles, fumbles, scoring efficiency
            "home_int_thrown_pg", "home_int_thrown_allowed_pg", "home_tackles_pg",
            "home_fumbles_lost_pg", "home_scoring_efficiency",
            "away_int_thrown_pg", "away_int_thrown_allowed_pg", "away_tackles_pg",
            "away_fumbles_lost_pg", "away_scoring_efficiency",
            # Scoring trends (last 5)
            "home_last_n_ppg", "home_last_n_opp_ppg", "home_last_n_margin",
            "away_last_n_ppg", "away_last_n_opp_ppg", "away_last_n_margin",
            "last5_ppg_diff", "last5_margin_diff",
            # Strength of schedule
            "home_sos_rating", "away_sos_rating", "sos_diff",
            # NFL standings (season win%, point margin, home/away splits, rank, streak)
            "home_nfl_stnd_win_pct", "home_nfl_stnd_pts_margin",
            "home_nfl_stnd_home_win_pct", "home_nfl_stnd_away_win_pct",
            "home_nfl_stnd_overall_rank", "home_nfl_stnd_streak", "home_nfl_stnd_gp",
            "away_nfl_stnd_win_pct", "away_nfl_stnd_pts_margin",
            "away_nfl_stnd_home_win_pct", "away_nfl_stnd_away_win_pct",
            "away_nfl_stnd_overall_rank", "away_nfl_stnd_streak", "away_nfl_stnd_gp",
            "nfl_stnd_rank_diff", "nfl_stnd_win_pct_diff",
            # Divisional / conference / season-stage context
            "is_divisional_game", "is_conference_game", "is_postseason",
            "nfl_week", "season_progress", "is_late_season",
            "home_clinch_status", "away_clinch_status",
            # QB performance features
            "home_qb_pass_yds_avg", "home_qb_pass_td_avg", "home_qb_pass_int_avg",
            "home_qb_passer_rating_avg", "home_qb_form_score",
            "away_qb_pass_yds_avg", "away_qb_pass_td_avg", "away_qb_pass_int_avg",
            "away_qb_passer_rating_avg", "away_qb_form_score",
            "qb_form_score_diff", "qb_rating_diff",
        ]
