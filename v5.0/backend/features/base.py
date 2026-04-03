# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Base Class
# ──────────────────────────────────────────────────────────
#
# Sport-agnostic feature extraction from normalized parquet
# data.  Every sport-specific extractor inherits from this
# class and implements ``extract_game_features`` plus
# ``get_feature_names``.
#
# Data is read via CuratedDataReader (DuckDB over
# normalized_curated hive-partitioned parquets) with automatic
# fallback to legacy data/normalized/ flat parquets.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import bisect
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.data_reader import get_reader, CuratedDataReader

logger = logging.getLogger(__name__)

# Status values that indicate a game has been fully completed.
# Soccer leagues use "full_time", "final_aet", "final_pen" while
# other sports use "final", "closed", "complete", "finished".
_COMPLETED_STATUSES: frozenset[str] = frozenset(
    [
        "final",
        "closed",
        "complete",
        "finished",
        # Soccer / football
        "full_time",
        "final_aet",
        "final_pen",
        "ft",
    ]
)


class BaseFeatureExtractor(ABC):
    """Base class for sport-specific feature extraction.

    Reads normalized parquet data produced by the normalization
    pipeline and builds ML-ready feature vectors.
    """

    sport: str = ""

    # ── Construction ──────────────────────────────────────

    def __init__(self, data_dir: Path | str) -> None:
        self.data_dir = Path(data_dir) if not isinstance(data_dir, Path) else data_dir
        self.normalized_dir = self.data_dir / "normalized"
        # DuckDB-backed curated reader (shared per-thread singleton)
        self._reader: CuratedDataReader = get_reader(self.data_dir)
        self._games_cache: dict[str, pd.DataFrame] = {}
        self._team_stats_cache: dict[str, pd.DataFrame] = {}
        self._player_stats_cache: dict[str, pd.DataFrame] = {}
        self._odds_cache: dict[str, pd.DataFrame] = {}
        self._market_signals_cache: dict[str, pd.DataFrame] = {}
        self._schedule_fatigue_cache: dict[str, pd.DataFrame] = {}
        self._injuries_cache: dict[str, pd.DataFrame] = {}
        self._standings_cache: dict[str, pd.DataFrame] = {}
        self._team_id_map: dict[str, str] = {}  # primary_id → player_stats_id
        # Pre-built index: team_id → list of (date, row_idx) sorted newest first
        self._team_history_idx: dict[str, dict[str, list[int]]] = {}
        # Home/away split index: maps games cache_id → {team_id: {"home": [pos], "away": [pos]}}
        self._team_splits_idx: dict[str, dict[str, dict[str, list[int]]]] = {}
        # Tennis-specific: all player stats cache + player index (lazy-loaded once)
        self._all_pstats_cache: pd.DataFrame | None = None
        self._player_stats_index: dict[str, pd.DataFrame] = {}

    # ── Data Loaders (with per-season caching) ────────────

    def _parquet_path(self, data_type: str, season: int) -> Path:
        """Legacy path helper — kept for any direct usages in subclasses."""
        return self.normalized_dir / self.sport / f"{data_type}_{season}.parquet"

    def _load_all_games(self) -> pd.DataFrame:
        """Load all seasons of game data via DuckDB/curated with legacy fallback.

        Results are cached on the instance.  Provides cross-season form/H2H
        lookback.  Curated parquets have multi-season coverage (e.g. NBA
        2020-2026) vs old normalized which only has current season.
        """
        if getattr(self, "_all_games_cache", None) is not None:
            return self._all_games_cache  # type: ignore[return-value]
        combined = self._reader.load_all_seasons(self.sport, "games")
        if combined.empty:
            # Hard legacy fallback
            sport_dir = self.normalized_dir / self.sport
            frames: list[pd.DataFrame] = []
            for p in sorted(sport_dir.glob("games_*.parquet")):
                try:
                    frames.append(pd.read_parquet(p))
                except Exception:
                    pass
            combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not combined.empty and "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
            combined.sort_values("date", inplace=True, ignore_index=True)
        self._all_games_cache: pd.DataFrame | None = combined  # type: ignore[assignment]
        return combined

    def _parquet_path_fallback(self, data_type: str, season: int) -> Path | None:
        """Legacy path helper — used only when curated is unavailable."""
        path = self._parquet_path(data_type, season)
        if path.exists():
            return path
        bare = self.normalized_dir / self.sport / f"{data_type}.parquet"
        if bare.exists():
            return bare
        return None

    def _build_team_id_map(self, season: int) -> None:
        """Build a mapping from primary team IDs (ESPN) to player_stats IDs (NBA stats).

        Uses player names as a bridge: injuries have ESPN team_ids and
        player_stats/players have NBA-stats team_ids for the same players.
        """
        if self._team_id_map:
            return

        injuries = self.load_injuries(season)
        player_stats = self.load_player_stats(season)

        if injuries.empty or player_stats.empty:
            return
        if "team_id" not in injuries.columns or "team_id" not in player_stats.columns:
            return
        if "player_name" not in injuries.columns or "player_name" not in player_stats.columns:
            return

        # Build name → NBA-stats team_id from player_stats
        ps_unique = player_stats[["player_name", "team_id"]].drop_duplicates("player_name").copy()
        ps_unique["name_lower"] = ps_unique["player_name"].str.lower().str.strip()

        # Also try the players roster from curated (broader coverage)
        players_df = self._reader.load(self.sport, "players", season=season)
        if players_df.empty:
            players_path = self._parquet_path_fallback("players", season)
            if players_path:
                try:
                    players_df = pd.read_parquet(players_path)
                except Exception:
                    players_df = pd.DataFrame()
        if not players_df.empty and "name" in players_df.columns and "team_id" in players_df.columns:
            p_unique = players_df[["name", "team_id"]].drop_duplicates("name").copy()
            p_unique.columns = ["player_name", "team_id"]
            p_unique["name_lower"] = p_unique["player_name"].str.lower().str.strip()
            ps_unique = pd.concat([ps_unique, p_unique], ignore_index=True).drop_duplicates("name_lower")

        inj_df = injuries[["player_name", "team_id"]].copy()
        inj_df["name_lower"] = inj_df["player_name"].str.lower().str.strip()

        merged = inj_df.merge(ps_unique, on="name_lower", suffixes=("_espn", "_nba"))

        if merged.empty:
            return

        # Take most common NBA-stats ID per ESPN ID (majority vote)
        mapping: dict[str, str] = {}
        for espn_id, group in merged.groupby("team_id_espn"):
            espn_str = str(espn_id)
            nba_counts = group["team_id_nba"].astype(str).value_counts()
            best_nba = nba_counts.index[0]
            if espn_str != best_nba:
                mapping[espn_str] = best_nba

        self._team_id_map = mapping
        logger.debug("Built team ID map with %d entries", len(self._team_id_map))

    def _load_team_name_map(self, season: int | str) -> dict[str, str]:
        """Build team name → team_id mapping from teams data."""
        season_int = int(season) if not isinstance(season, int) else season
        teams_df = self._reader.load(self.sport, "teams", season=season_int)
        if teams_df.empty:
            # Fallback: try adjacent seasons
            for s in [season_int - 1, season_int + 1, season_int - 2]:
                path = self.normalized_dir / self.sport / f"teams_{s}.parquet"
                if path.exists():
                    try:
                        teams_df = pd.read_parquet(path)
                        break
                    except Exception:
                        pass
        if not teams_df.empty:
            name_col = "name" if "name" in teams_df.columns else ("team_name" if "team_name" in teams_df.columns else None)
            id_col = "id" if "id" in teams_df.columns else ("team_id" if "team_id" in teams_df.columns else None)
            if name_col and id_col:
                return dict(zip(teams_df[name_col], teams_df[id_col].astype(str)))
        return {}

    def _resolve_team_ids(self, df: pd.DataFrame, season: int) -> pd.DataFrame:
        """Fill missing team_ids from team name lookups."""
        if df.empty:
            return df

        for name_col, id_col in [("home_team", "home_team_id"), ("away_team", "away_team_id")]:
            if id_col not in df.columns or name_col not in df.columns:
                continue
            missing = df[id_col].isna() | (df[id_col].astype(str).isin(["", "None", "nan"]))
            if not missing.any():
                continue

            team_map = self._load_team_name_map(season)
            if not team_map:
                logger.warning("No team name map available for season %s", season)
                break

            df.loc[missing, id_col] = df.loc[missing, name_col].map(team_map)

            still_missing = df[id_col].isna() | (df[id_col].astype(str).isin(["", "None", "nan"]))
            if still_missing.any():
                unmapped = df.loc[still_missing, name_col].unique()
                logger.debug(
                    "Could not resolve team_id for: %s — assigning synthetic IDs",
                    unmapped,
                )
                for name in unmapped:
                    synth_id = f"syn_{abs(hash(name)) % 100000}"
                    df.loc[(df[name_col] == name) & still_missing, id_col] = synth_id

        return df

    def load_games(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._games_cache:
            df = self._reader.load(self.sport, "games", season=season)
            if df.empty:
                logger.warning("Games not found for %s season %s", self.sport, season)
            else:
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    df.sort_values("date", inplace=True)
                df = self._resolve_team_ids(df, season)
            self._games_cache[cache_key] = df
        return self._games_cache[cache_key]

    def load_team_stats(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._team_stats_cache:
            df = self._reader.load(self.sport, "team_stats", season=season)
            if df.empty:
                # team_season_averages is richer; try it as fallback
                df = self._reader.load(self.sport, "team_season_averages", season=season)
            if df.empty:
                df = self._reader.load(self.sport, "standings", season=season)
            if df.empty:
                logger.warning("Team stats not found for %s season %s", self.sport, season)
            self._team_stats_cache[cache_key] = df
        return self._team_stats_cache[cache_key]

    def load_team_season_averages(self, season: int | str) -> pd.DataFrame:
        """Load pre-computed team season averages from curated data.

        Available for NBA, NFL, and other sports with detailed box scores.
        Columns: avg_points, avg_rebounds, avg_assists, field_goal_pct, etc.
        """
        return self._reader.load(self.sport, "team_season_averages", season=season)

    def load_player_stats(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._player_stats_cache:
            df = self._reader.load(self.sport, "player_stats", season=season)
            if df.empty:
                logger.warning("Player stats not found for %s season %s", self.sport, season)
            self._player_stats_cache[cache_key] = df
        return self._player_stats_cache[cache_key]

    def load_odds(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._odds_cache:
            df = self._reader.load(self.sport, "odds", season=season)
            if df.empty:
                logger.debug("Odds not found for %s season %s", self.sport, season)
            self._odds_cache[cache_key] = df
        return self._odds_cache[cache_key]

    def load_injuries(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._injuries_cache:
            df = self._reader.load(self.sport, "injuries", season=season)
            if df.empty:
                logger.debug("Injuries not found for %s season %s", self.sport, season)
            elif "team_id" in df.columns:
                df["team_id"] = df["team_id"].astype(str)
            self._injuries_cache[cache_key] = df
        return self._injuries_cache[cache_key]

    def load_market_signals(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._market_signals_cache:
            df = self._reader.load(self.sport, "market_signals", season=season)
            if df.empty:
                logger.debug("Market signals not found for %s season %s", self.sport, season)
            self._market_signals_cache[cache_key] = df
        return self._market_signals_cache[cache_key]

    def load_schedule_fatigue(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._schedule_fatigue_cache:
            df = self._reader.load(self.sport, "schedule_fatigue", season=season)
            if df.empty:
                logger.debug("Schedule fatigue not found for %s season %s", self.sport, season)
            self._schedule_fatigue_cache[cache_key] = df
        return self._schedule_fatigue_cache[cache_key]

    def load_standings(self, season: int | str) -> pd.DataFrame:
        cache_key = str(season)
        if cache_key not in self._standings_cache:
            df = self._reader.load(self.sport, "standings", season=season)
            if df.empty:
                logger.debug("Standings not found for %s season %s", self.sport, season)
            elif "team_id" in df.columns:
                df["team_id"] = df["team_id"].astype(str)
            self._standings_cache[cache_key] = df
        return self._standings_cache[cache_key]

    def load_match_events(self, season: int | str) -> pd.DataFrame:
        """Load match events (goals, cards, etc.) — available for soccer leagues.

        Columns: match_id, event_type, minute, second, player_id, team_id.
        Supported: bundesliga, laliga, ligue1, mls.
        """
        return self._reader.load(self.sport, "match_events", season=season)

    def load_weather(self, season: int | str) -> pd.DataFrame:
        """Load weather data — available for mlb, nwsl.

        Columns: game_id, temp_f, wind_mph, wind_direction, humidity_pct,
                 precipitation, condition, dome.
        """
        return self._reader.load(self.sport, "weather", season=season)

    def load_batter_game_stats(self, season: int | str) -> pd.DataFrame:
        """Load per-game batter stats — MLB only.

        Columns: game_id, player_id, team_id, ab, hits, hr, rbi, runs, bb, etc.
        """
        return self._reader.load(self.sport, "batter_game_stats", season=season)

    def load_pitcher_game_stats(self, season: int | str) -> pd.DataFrame:
        """Load per-game pitcher stats — MLB only.

        Columns: game_id, player_id, team_id, innings, earned_runs,
                 strikeouts, walks, home_runs_allowed, batters_faced, etc.
        """
        return self._reader.load(self.sport, "pitcher_game_stats", season=season)

    def load_advanced_batting(self, season: int | str) -> pd.DataFrame:
        """Load season-level advanced batting stats — MLB only.

        Columns: player_id, season, team_id, iso, babip, bb_pct, k_pct, woba.
        """
        return self._reader.load(self.sport, "advanced_batting", season=season)

    def load_ratings(self, season: int | str) -> pd.DataFrame:
        """Load power ratings — NBA only currently.

        Columns: sport, source, rating_type, team_or_player, value, date, season.
        """
        return self._reader.load(self.sport, "ratings", season=season)

    def available_seasons(self) -> list[int]:
        """Return all available seasons from curated data."""
        return self._reader.available_seasons(self.sport, "games")

    # ── Vectorized Helpers ────────────────────────────────

    def _resolve_game_team_ids(
        self,
        game: dict,
        games_df: pd.DataFrame,
    ) -> tuple[str, str]:
        """Return (home_team_id, away_team_id) for *game*, falling back to
        looking them up in *games_df* when the game dict contains null values.

        This is needed because raw parquet rows often have null team_ids
        (resolved only inside ``load_games``), so passing a raw parquet row
        dict directly to ``extract_game_features`` would produce empty form
        lookups and zeroed features.
        """
        h_id = str(game.get("home_team_id", "") or "")
        a_id = str(game.get("away_team_id", "") or "")
        _missing = {"", "nan", "none", "null"}

        if h_id.lower() in _missing or a_id.lower() in _missing:
            # Try to find the row in games_df by game id
            gid = str(game.get("id", game.get("game_id", "")) or "")
            if gid and not games_df.empty:
                id_col = next(
                    (c for c in ("id", "game_id") if c in games_df.columns), None
                )
                if id_col:
                    match = games_df[games_df[id_col].astype(str) == gid]
                    if not match.empty:
                        h_id = str(match.iloc[0].get("home_team_id", "") or "")
                        a_id = str(match.iloc[0].get("away_team_id", "") or "")

        # If still missing, fall back to matching by team name
        if (h_id.lower() in _missing or a_id.lower() in _missing) and not games_df.empty:
            h_name = str(game.get("home_team", "") or "")
            a_name = str(game.get("away_team", "") or "")
            if h_name and "home_team" in games_df.columns:
                rows = games_df[games_df["home_team"] == h_name]
                if not rows.empty and "home_team_id" in rows.columns:
                    cand = str(rows["home_team_id"].dropna().iloc[0]) if rows["home_team_id"].notna().any() else ""
                    if cand.lower() not in _missing:
                        h_id = cand
            if a_name and "away_team" in games_df.columns:
                rows = games_df[games_df["away_team"] == a_name]
                if not rows.empty and "away_team_id" in rows.columns:
                    cand = str(rows["away_team_id"].dropna().iloc[0]) if rows["away_team_id"].notna().any() else ""
                    if cand.lower() not in _missing:
                        a_id = cand

        return h_id, a_id

    def _build_team_history_index(self, games: pd.DataFrame) -> dict[str, Any]:
        """Pre-build per-team index of completed game row positions, newest first.

        Returns {team_id: {"dates": np.ndarray(descending), "positions": np.ndarray}}
        Uses numpy arrays + bisect for O(log k) date lookups.
        """
        cache_id = id(games)
        if cache_id in self._team_history_idx:
            return self._team_history_idx[cache_id]

        if games.empty:
            self._team_history_idx[cache_id] = {}
            return {}

        status_col = games.get("status", pd.Series("final", index=games.index))
        status_mask = status_col.str.lower().isin(_COMPLETED_STATUSES)
        has_scores = (
            games["home_score"].notna() & games["away_score"].notna()
            if "home_score" in games.columns and "away_score" in games.columns
            else pd.Series(False, index=games.index)
        )
        completed = (status_mask | has_scores).values

        home_ids = games["home_team_id"].values if "home_team_id" in games.columns else []
        away_ids = games["away_team_id"].values if "away_team_id" in games.columns else []

        # Pre-convert dates to int64 nanoseconds for fast comparison.
        # Cast to datetime64[ns] first so that datetime64[us/ms/s] sources all
        # produce the same unit — pd.Timestamp.value is always nanoseconds.
        if "date" in games.columns:
            dates_ns = (
                pd.to_datetime(games["date"], errors="coerce")
                .astype("datetime64[ns]")
                .values.astype("int64")
            )
        else:
            dates_ns = np.zeros(len(games), dtype="int64")

        # Sort by date descending
        sorted_positions = np.argsort(dates_ns)[::-1]

        raw: dict[str, tuple[list[int], list[int]]] = {}  # team -> (dates_desc, positions)
        for pos in sorted_positions:
            if not completed[pos]:
                continue
            dt_ns = int(dates_ns[pos])
            h = str(home_ids[pos]) if pos < len(home_ids) else ""
            a = str(away_ids[pos]) if pos < len(away_ids) else ""
            if h:
                e = raw.setdefault(h, ([], []))
                e[0].append(dt_ns)
                e[1].append(int(pos))
            if a:
                e = raw.setdefault(a, ([], []))
                e[0].append(dt_ns)
                e[1].append(int(pos))

        # Convert to numpy arrays for fast bisect
        idx = {
            team: {
                "dates": np.array(dates_list, dtype="int64"),  # descending
                "positions": np.array(pos_list, dtype="int64"),
            }
            for team, (dates_list, pos_list) in raw.items()
        }
        self._team_history_idx[cache_id] = idx
        return idx

    def _split_home_away(self, recent: pd.DataFrame, team_id: str):
        """Return (home_rows, away_rows) sub-DataFrames for team_id."""
        is_home = recent["home_team_id"].astype(str) == str(team_id)
        return recent[is_home], recent[~is_home]

    def _team_games_before(
        self,
        games: pd.DataFrame,
        team_id: str,
        date: str | pd.Timestamp,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Return completed games for *team_id* before *date*, newest first.

        Uses a pre-built index with binary search for O(log k + limit) lookup.
        Pass `limit` to avoid building unnecessarily large DataFrames.
        """
        if games.empty:
            return games

        idx = self._build_team_history_index(games)
        entry = idx.get(team_id)
        if entry is None:
            return games.iloc[0:0]

        dates_desc: np.ndarray = entry["dates"]    # sorted descending
        positions: np.ndarray = entry["positions"]

        ts_ns = pd.Timestamp(date).value  # int64 nanoseconds
        # Binary search on negated (ascending) dates array
        neg_dates = -dates_desc  # now ascending
        neg_ts = -ts_ns
        # bisect_right gives first position in neg_dates that is > neg_ts
        # i.e., first position where -dt > -ts → dt < ts (valid entries start here)
        cutoff = int(np.searchsorted(neg_dates, neg_ts, side="right"))

        if cutoff >= len(positions):
            return games.iloc[0:0]

        valid_positions = positions[cutoff:]
        if limit is not None:
            valid_positions = valid_positions[:limit]

        return games.iloc[valid_positions]

    @staticmethod
    def _win_flag(row: pd.Series, team_id: str) -> bool:
        """Return True if *team_id* won *row*."""
        is_home = row.get("home_team_id") == team_id
        h_score = pd.to_numeric(row.get("home_score", 0), errors="coerce") or 0
        a_score = pd.to_numeric(row.get("away_score", 0), errors="coerce") or 0
        return (is_home and h_score > a_score) or (not is_home and a_score > h_score)

    @staticmethod
    def _team_score(row: pd.Series, team_id: str) -> tuple[float, float]:
        """Return (team_points, opponent_points) for *team_id*."""
        h = pd.to_numeric(row.get("home_score", 0), errors="coerce") or 0.0
        a = pd.to_numeric(row.get("away_score", 0), errors="coerce") or 0.0
        if row.get("home_team_id") == team_id:
            return h, a
        return a, h

    @staticmethod
    def _vec_win_flags(df: pd.DataFrame, team_id: str) -> pd.Series:
        """Vectorized: return boolean Series of wins for *team_id*."""
        h = pd.to_numeric(df.get("home_score", 0), errors="coerce").fillna(0)
        a = pd.to_numeric(df.get("away_score", 0), errors="coerce").fillna(0)
        is_home = df.get("home_team_id") == team_id
        return (is_home & (h > a)) | (~is_home & (a > h))

    @staticmethod
    def _vec_team_scores(df: pd.DataFrame, team_id: str) -> tuple[pd.Series, pd.Series]:
        """Vectorized: return (team_pts, opp_pts) Series for *team_id*."""
        h = pd.to_numeric(df.get("home_score", 0), errors="coerce").fillna(0)
        a = pd.to_numeric(df.get("away_score", 0), errors="coerce").fillna(0)
        is_home = df.get("home_team_id") == team_id
        team_pts = h.where(is_home, a)
        opp_pts = a.where(is_home, h)
        return team_pts, opp_pts

    # ── Common Feature Methods (shared across all sports) ─

    def team_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling win rate and scoring averages over the last *window* games."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "form_win_pct": 0.0,
                "form_ppg": 0.0,
                "form_opp_ppg": 0.0,
                "form_avg_margin": 0.0,
                "form_games_played": 0,
            }

        wins = self._vec_win_flags(recent, team_id)
        team_pts, opp_pts = self._vec_team_scores(recent, team_id)

        return {
            "form_win_pct": float(wins.mean()),
            "form_ppg": float(team_pts.mean()),
            "form_opp_ppg": float(opp_pts.mean()),
            "form_avg_margin": float((team_pts - opp_pts).mean()),
            "form_games_played": len(recent),
        }

    def home_away_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        is_home: bool,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling win rate and scoring averages for home-only or away-only games.

        Home/away performance can diverge significantly — tracking them separately
        provides better signal than combining all games.
        """
        recent_all = self._team_games_before(games, team_id, date, limit=window * 3)
        if recent_all.empty:
            return {
                "ha_win_pct": 0.0,
                "ha_ppg": 0.0,
                "ha_opp_ppg": 0.0,
                "ha_avg_margin": 0.0,
                "ha_games_played": 0,
            }

        # Filter to home or away games only
        if is_home:
            filtered = recent_all[recent_all["home_team_id"] == str(team_id)]
        else:
            filtered = recent_all[recent_all["away_team_id"] == str(team_id)]

        # Fall back to all games if insufficient home/away split data
        if len(filtered) < 3:
            filtered = recent_all

        filtered = filtered.tail(window)
        if filtered.empty:
            return {
                "ha_win_pct": 0.0,
                "ha_ppg": 0.0,
                "ha_opp_ppg": 0.0,
                "ha_avg_margin": 0.0,
                "ha_games_played": 0,
            }

        wins = self._vec_win_flags(filtered, team_id)
        team_pts, opp_pts = self._vec_team_scores(filtered, team_id)

        return {
            "ha_win_pct": float(wins.mean()),
            "ha_ppg": float(team_pts.mean()),
            "ha_opp_ppg": float(opp_pts.mean()),
            "ha_avg_margin": float((team_pts - opp_pts).mean()),
            "ha_games_played": len(filtered),
        }

    def head_to_head(
        self,
        team_a: str,
        team_b: str,
        games: pd.DataFrame,
        date: str | None = None,
        n: int = 10,
    ) -> dict[str, float]:
        """Head-to-head record between two teams.

        Uses team_a's history index (O(log n)) then filters for team_b
        opponent — avoids full-scan string comparison on all games.
        """
        if games.empty:
            return {"h2h_games": 0, "h2h_win_pct": 0.0, "h2h_avg_margin": 0.0}

        # Get team_a's recent games via index (fast O(log n) + O(limit))
        effective_date = date or "2099-01-01"
        team_a_games = self._team_games_before(games, team_a, effective_date, limit=500)
        if team_a_games.empty:
            return {"h2h_games": 0, "h2h_win_pct": 0.0, "h2h_avg_margin": 0.0}

        # Filter for games where team_b is the opponent (on small subset)
        is_home_a = team_a_games["home_team_id"] == team_a
        opp = team_a_games["away_team_id"].where(is_home_a, team_a_games["home_team_id"])
        h2h = team_a_games[opp == team_b].head(n)  # already newest-first
        if h2h.empty:
            return {"h2h_games": 0, "h2h_win_pct": 0.0, "h2h_avg_margin": 0.0}

        wins_a = self._vec_win_flags(h2h, team_a)
        team_pts, opp_pts = self._vec_team_scores(h2h, team_a)

        return {
            "h2h_games": len(h2h),
            "h2h_win_pct": float(wins_a.mean()),
            "h2h_avg_margin": float((team_pts - opp_pts).mean()),
        }

    def head_to_head_at_home(
        self,
        home_team: str,
        away_team: str,
        games: pd.DataFrame,
        date: str | None = None,
        n: int = 8,
    ) -> dict[str, float]:
        """H2H record specifically when home_team hosts away_team (venue-specific)."""
        defaults = {"h2h_home_games": 0, "h2h_home_win_pct": 0.0, "h2h_home_avg_margin": 0.0}
        if games.empty:
            return defaults
        effective_date = date or "2099-01-01"
        home_games = self._team_games_before(games, home_team, effective_date, limit=500)
        if home_games.empty:
            return defaults
        venue_h2h = home_games[
            (home_games["home_team_id"] == home_team) &
            (home_games["away_team_id"] == away_team)
        ].head(n)
        if venue_h2h.empty:
            return defaults
        wins = self._vec_win_flags(venue_h2h, home_team)
        tp, op = self._vec_team_scores(venue_h2h, home_team)
        return {
            "h2h_home_games": len(venue_h2h),
            "h2h_home_win_pct": float(wins.mean()),
            "h2h_home_avg_margin": float((tp - op).mean()),
        }

    def rest_days(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> int:
        """Days since the team's last game."""
        recent = self._team_games_before(games, team_id, date)
        if recent.empty:
            return 7  # default to well-rested
        last_date = recent.iloc[0]["date"]
        return max(0, (pd.Timestamp(date) - pd.Timestamp(last_date)).days)

    def _build_team_splits_index(self, games: pd.DataFrame) -> dict[str, dict[str, list[int]]]:
        """Build and cache {team_id: {"home": [pos...], "away": [pos...]}} for O(1) splits lookup."""
        cache_id = id(games)
        if cache_id in self._team_splits_idx:
            return self._team_splits_idx[cache_id]
        idx: dict[str, dict[str, list[int]]] = {}
        if games.empty:
            self._team_splits_idx[cache_id] = idx
            return idx
        status_col = games.get("status", pd.Series("final", index=games.index))
        status_mask = status_col.str.lower().isin(_COMPLETED_STATUSES)
        has_scores = (
            games["home_score"].notna() & games["away_score"].notna()
            if "home_score" in games.columns and "away_score" in games.columns
            else pd.Series(False, index=games.index)
        )
        completed = (status_mask | has_scores).values
        home_ids = games["home_team_id"].values if "home_team_id" in games.columns else []
        away_ids = games["away_team_id"].values if "away_team_id" in games.columns else []
        for i in range(len(games)):
            if not completed[i]:
                continue
            h = str(home_ids[i]) if i < len(home_ids) else ""
            a = str(away_ids[i]) if i < len(away_ids) else ""
            if h:
                idx.setdefault(h, {"home": [], "away": []})["home"].append(i)
            if a:
                idx.setdefault(a, {"home": [], "away": []})["away"].append(i)
        self._team_splits_idx[cache_id] = idx
        return idx

    def home_away_splits(
        self,
        team_id: str,
        games: pd.DataFrame,
        season: int,
    ) -> dict[str, float]:
        """Win percentages at home vs away."""
        if games.empty:
            return {"home_win_pct": 0.0, "away_win_pct": 0.0}

        splits_idx = self._build_team_splits_index(games)
        team_entry = splits_idx.get(team_id, {"home": [], "away": []})
        home_positions = team_entry.get("home", [])
        away_positions = team_entry.get("away", [])

        if home_positions:
            home_games = games.iloc[home_positions]
            home_wins = self._vec_win_flags(home_games, team_id)
            home_pct = float(home_wins.mean()) if not home_wins.empty else 0.0
        else:
            home_pct = 0.0

        if away_positions:
            away_games = games.iloc[away_positions]
            away_wins = self._vec_win_flags(away_games, team_id)
            away_pct = float(away_wins.mean()) if not away_wins.empty else 0.0
        else:
            away_pct = 0.0

        return {"home_win_pct": home_pct, "away_win_pct": away_pct}

    def momentum(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 5,
    ) -> float:
        """Weighted momentum score over the last *window* games.

        Recent wins count more than older ones.  Returns a float in
        ``[-1.0, 1.0]`` where positive means winning momentum.
        """
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return 0.0

        weights = np.linspace(1.0, 0.5, num=len(recent))
        results = self._vec_win_flags(recent, team_id).astype(float)
        # Map 0/1 to -1/+1 for momentum direction
        results = results * 2 - 1
        return float(np.average(results.values, weights=weights))

    def season_stats(
        self,
        team_id: str,
        games: pd.DataFrame,
        season: int,
    ) -> dict[str, float]:
        """Aggregate season statistics for a team."""
        completed = games.loc[
            games.get("status", pd.Series("final", index=games.index)).str.lower().isin(
                _COMPLETED_STATUSES
            )
        ]
        team_mask = (completed.get("home_team_id") == team_id) | (
            completed.get("away_team_id") == team_id
        )
        team_games = completed.loc[team_mask]

        if team_games.empty:
            return {"season_games": 0, "season_win_pct": 0.0, "season_ppg": 0.0, "season_opp_ppg": 0.0}

        wins = self._vec_win_flags(team_games, team_id)
        team_pts, opp_pts = self._vec_team_scores(team_games, team_id)

        return {
            "season_games": len(team_games),
            "season_win_pct": float(wins.mean()),
            "season_ppg": float(team_pts.mean()),
            "season_opp_ppg": float(opp_pts.mean()),
        }

    # ── ELO Rating ────────────────────────────────────────

    def _precompute_elo(
        self,
        games: pd.DataFrame,
        k: float = 32.0,
    ) -> dict:
        """Precompute per-player/team ELO timelines.

        Stores per-entity sorted numpy arrays of (date_ns, pre_game_elo)
        instead of full dict snapshots per game.  This reduces memory from
        O(players × games) dicts to O(players × games) numpy scalars —
        roughly 12x cheaper and avoids OOM on large-roster sports (ATP/WTA).
        """
        cache_key = id(games)
        if hasattr(self, "_elo_cache") and self._elo_cache_key == cache_key:
            return self._elo_cache

        elo_current: dict[str, float] = {}
        # player/team → list of (date_ns: int64, pre_game_elo: float)
        player_hist: dict[str, list] = {}

        if games.empty or "date" not in games.columns:
            self._elo_cache = {}
            self._elo_cache_key = cache_key
            return {}
        dated = games.sort_values("date").reset_index(drop=True)

        # Vectorised prep — convert once rather than per-row in Python loop
        h_ids = dated["home_team_id"].astype(str).values if "home_team_id" in dated.columns else np.full(len(dated), "")
        a_ids = dated["away_team_id"].astype(str).values if "away_team_id" in dated.columns else np.full(len(dated), "")
        h_scores = pd.to_numeric(dated["home_score"], errors="coerce").values if "home_score" in dated.columns else np.full(len(dated), np.nan)
        a_scores = pd.to_numeric(dated["away_score"], errors="coerce").values if "away_score" in dated.columns else np.full(len(dated), np.nan)
        dates_ns_arr = (
            pd.to_datetime(dated["date"], errors="coerce")
            .astype("datetime64[ns]")
            .astype("int64")
            .values
            if "date" in dated.columns
            else np.zeros(len(dated), dtype="int64")
        )

        for i in range(len(dated)):
            h = h_ids[i]
            a = a_ids[i]
            hs = h_scores[i]
            as_ = a_scores[i]
            date_ns = int(dates_ns_arr[i])

            rh = elo_current.get(h, 1500.0)
            ra = elo_current.get(a, 1500.0)

            # Record pre-game ELO for both participants
            if h and a and date_ns != 0:
                if h not in player_hist:
                    player_hist[h] = []
                if a not in player_hist:
                    player_hist[a] = []
                player_hist[h].append((date_ns, rh))
                player_hist[a].append((date_ns, ra))

            if np.isnan(hs) or np.isnan(as_) or not h or not a:
                continue
            eh = 1.0 / (1.0 + 10.0 ** ((ra - rh) / 400.0))
            sh = 1.0 if hs > as_ else (0.0 if hs < as_ else 0.5)
            # Margin-of-victory K multiplier: log-scale reward for dominant wins
            # Prevents autocorrection bias (per 538) and rewards predictive quality
            margin = abs(float(hs) - float(as_))
            elo_diff_dir = (rh - ra) * (1.0 if hs >= as_ else -1.0)
            # autocorrelation correction factor (avoids over-rewarding expected blowouts)
            autocorr = 1.0 / (1.0 + 0.001 * max(0.0, elo_diff_dir))
            mov_mult = np.log(margin + 1.0) * autocorr
            elo_current[h] = rh + k * mov_mult * (sh - eh)
            elo_current[a] = ra + k * mov_mult * ((1.0 - sh) - (1.0 - eh))

        # Convert to numpy for fast searchsorted lookups
        elo_np: dict[str, tuple] = {}
        for pid, hist in player_hist.items():
            if hist:
                dates_ns_out = np.array([d for d, _ in hist], dtype="int64")
                elos = np.array([e for _, e in hist], dtype="float32")
                elo_np[pid] = (dates_ns_out, elos)

        # Keep final ELO for any player not yet in history (edge case)
        self._elo_current = elo_current
        self._elo_player_hist: dict[str, tuple] = elo_np
        self._elo_cache: dict = {}  # kept for interface compatibility
        self._elo_cache_key = cache_key
        return self._elo_cache

    def elo_features(
        self,
        team_id: str,
        opp_id: str,
        date: str,
        games: pd.DataFrame,
        k: float = 32.0,
        _game_idx: Any = None,
    ) -> dict[str, float]:
        """ELO rating for *team_id* vs *opp_id* as of *date*.

        Uses per-entity numpy timelines with binary search — O(log n) per
        lookup, O(players × games × 12 bytes) total memory.
        """
        self._precompute_elo(games, k=k)

        def _get_elo(pid: str) -> float:
            hist = self._elo_player_hist.get(str(pid))
            if hist is None:
                return 1500.0
            dates_ns, elos = hist
            try:
                cutoff_ns = int(pd.Timestamp(date).value)
            except Exception:
                return 1500.0
            pos = int(np.searchsorted(dates_ns, cutoff_ns, side="left")) - 1
            return float(elos[pos]) if pos >= 0 else 1500.0

        p_elo = _get_elo(team_id)
        o_elo = _get_elo(opp_id)

        return {
            "elo": p_elo,
            "elo_diff": p_elo - o_elo,
            "elo_expected_win": 1.0 / (1.0 + 10 ** ((o_elo - p_elo) / 400.0)),
        }

    def _odds_features(
        self,
        game_id: str,
        odds_df: pd.DataFrame,
        home_team: str = "",
        away_team: str = "",
        date: str = "",
    ) -> dict[str, float]:
        """Extract consensus odds features for a game.

        Falls back to team-name + date matching when game_id doesn't resolve
        (e.g. NHL where OddsAPI uses hex IDs and ESPN uses numeric IDs).
        """
        _default = {
            "home_moneyline": 0.0,
            "away_moneyline": 0.0,
            "spread": 0.0,
            "total": 0.0,
            "home_implied_prob": 0.5,
        }
        if odds_df.empty or "game_id" not in odds_df.columns:
            return _default

        # Primary match: by game_id
        match = odds_df.loc[odds_df["game_id"] == game_id]

        # Fallback: match by team names + date (handles hex-vs-numeric ID mismatch)
        if match.empty and home_team and away_team and date:
            def _norm(s: str) -> str:
                return (
                    str(s).lower().strip()
                    .replace("é", "e").replace("è", "e").replace("ê", "e")
                    .replace("ó", "o").replace("á", "a").replace("ú", "u")
                )
            date_str = str(date)[:10]
            hn, an = _norm(home_team), _norm(away_team)
            if "home_team" in odds_df.columns and "away_team" in odds_df.columns and "date" in odds_df.columns:
                mask = (
                    odds_df["date"].astype(str).str[:10] == date_str
                ) & (
                    odds_df["home_team"].apply(_norm) == hn
                ) & (
                    odds_df["away_team"].apply(_norm) == an
                )
                match = odds_df.loc[mask]

        if match.empty:
            return _default

        row = match.iloc[0]

        # Handle both h2h_home (OddsAPI format) and home_moneyline
        h_ml_raw = row.get("h2h_home", None) or row.get("home_moneyline", 0)
        a_ml_raw = row.get("h2h_away", None) or row.get("away_moneyline", 0)
        h_ml = pd.to_numeric(h_ml_raw, errors="coerce") or 0.0
        a_ml = pd.to_numeric(a_ml_raw, errors="coerce") or 0.0

        # American odds → implied probability
        if h_ml < 0:
            h_prob = abs(h_ml) / (abs(h_ml) + 100)
        elif h_ml > 0:
            h_prob = 100 / (h_ml + 100)
        else:
            h_prob = 0.5

        return {
            "home_moneyline": float(h_ml),
            "away_moneyline": float(a_ml),
            "spread": float(pd.to_numeric(row.get("spread_home_line", row.get("spread", 0)), errors="coerce") or 0.0),
            "total": float(pd.to_numeric(row.get("total_line", row.get("total", 0)), errors="coerce") or 0.0),
            "home_implied_prob": float(h_prob),
        }

    # ── Enhanced Feature Methods (new data sources) ───────

    @staticmethod
    def _american_to_implied(ml: float) -> float:
        """Convert American odds to implied probability."""
        if ml < 0:
            return abs(ml) / (abs(ml) + 100)
        elif ml > 0:
            return 100.0 / (ml + 100)
        return 0.5

    def _injury_features(
        self,
        team_id: str,
        season: int,
    ) -> dict[str, float]:
        """Injury burden features for a team.

        Returns counts and severity scores derived from the injury report.
        """
        defaults = {
            "injury_count": 0.0,
            "injury_severity_score": 0.0,
            "injury_out_count": 0.0,
            "injury_dtd_count": 0.0,
            "injury_questionable_count": 0.0,
        }
        injuries = self.load_injuries(season)
        if injuries.empty or "team_id" not in injuries.columns:
            return defaults

        team_inj = injuries.loc[injuries["team_id"] == str(team_id)]
        if team_inj.empty:
            return defaults

        status_col = team_inj["status"].fillna("").str.lower() if "status" in team_inj.columns else pd.Series("", index=team_inj.index)

        out_mask = status_col.str.contains("out", na=False)
        dtd_mask = status_col.str.contains("day-to-day|day to day|dtd", na=False, regex=True)
        quest_mask = status_col.str.contains("questionable|doubtful|probable", na=False, regex=True)

        out_count = float(out_mask.sum())
        dtd_count = float(dtd_mask.sum())
        quest_count = float(quest_mask.sum())

        severity = out_count * 3.0 + dtd_count * 1.0 + quest_count * 2.0

        return {
            "injury_count": float(len(team_inj)),
            "injury_severity_score": severity,
            "injury_out_count": out_count,
            "injury_dtd_count": dtd_count,
            "injury_questionable_count": quest_count,
        }

    def _enhanced_odds_features(
        self,
        game_id: str,
        odds_df: pd.DataFrame,
        home_team: str = "",
        away_team: str = "",
        date: str = "",
    ) -> dict[str, float]:
        """Enhanced odds features including spread/total movement and source consensus.

        Looks across multiple bookmaker rows for the same game_id.
        Falls back to team-name + date matching when game_id doesn't resolve.
        """
        defaults = {
            "odds_spread": 0.0,
            "odds_total": 0.0,
            "odds_home_ml": 0.0,
            "odds_away_ml": 0.0,
            "odds_home_implied_prob": 0.5,
            "odds_away_implied_prob": 0.5,
            "odds_spread_home_line": 0.0,
            "odds_spread_away_line": 0.0,
            "odds_source_count": 0.0,
            "odds_favorite_agreement": 0.0,
        }
        if odds_df.empty or "game_id" not in odds_df.columns:
            return defaults

        match = odds_df.loc[odds_df["game_id"] == game_id]

        # Fallback: match by team names + date
        if match.empty and home_team and away_team and date:
            def _norm(s: str) -> str:
                return (
                    str(s).lower().strip()
                    .replace("é", "e").replace("è", "e").replace("ê", "e")
                )
            date_str = str(date)[:10]
            hn, an = _norm(home_team), _norm(away_team)
            if "home_team" in odds_df.columns and "away_team" in odds_df.columns and "date" in odds_df.columns:
                mask = (
                    odds_df["date"].astype(str).str[:10] == date_str
                ) & (
                    odds_df["home_team"].apply(_norm) == hn
                ) & (
                    odds_df["away_team"].apply(_norm) == an
                )
                match = odds_df.loc[mask]

        if match.empty:
            return defaults

        def _num(val: Any, fallback: float = 0.0) -> float:
            v = pd.to_numeric(val, errors="coerce")
            return float(v) if pd.notna(v) else fallback

        h_mls, a_mls, spreads, totals = [], [], [], []
        for _, row in match.iterrows():
            hml = _num(row.get("h2h_home", row.get("home_moneyline")))
            aml = _num(row.get("h2h_away", row.get("away_moneyline")))
            spr = _num(row.get("spread_home", row.get("spread_home_line", row.get("spread"))))
            tot = _num(row.get("total_line", row.get("total")))
            if hml != 0:
                h_mls.append(hml)
            if aml != 0:
                a_mls.append(aml)
            if spr != 0:
                spreads.append(spr)
            if tot != 0:
                totals.append(tot)

        avg_hml = float(np.mean(h_mls)) if h_mls else 0.0
        avg_aml = float(np.mean(a_mls)) if a_mls else 0.0
        avg_spread = float(np.mean(spreads)) if spreads else 0.0
        avg_total = float(np.mean(totals)) if totals else 0.0

        h_prob = self._american_to_implied(avg_hml)
        a_prob = self._american_to_implied(avg_aml)

        # Favorite agreement: fraction of sources that agree on who the favorite is
        fav_count = sum(1 for m in h_mls if m < 0) if h_mls else 0
        fav_agreement = fav_count / len(h_mls) if h_mls else 0.0

        # Spread line prices
        row0 = match.iloc[0]
        shl = _num(row0.get("spread_home_line", 0))
        sal = _num(row0.get("spread_away_line", 0))

        return {
            "odds_spread": avg_spread,
            "odds_total": avg_total,
            "odds_home_ml": avg_hml,
            "odds_away_ml": avg_aml,
            "odds_home_implied_prob": h_prob,
            "odds_away_implied_prob": a_prob,
            "odds_spread_home_line": shl,
            "odds_spread_away_line": sal,
            "odds_source_count": float(len(match)),
            "odds_favorite_agreement": fav_agreement,
        }

    def _market_signal_features(
        self,
        game_id: str,
        season: int,
        home_team: str = "",
        away_team: str = "",
        date: str = "",
    ) -> dict[str, float]:
        """Aggregate market movement enrichments for one game.

        Uses normalized ``market_signals`` rows and falls back to team/date matching
        if game IDs differ across providers.
        """
        defaults = {
            "market_aggregate_abs_move": 0.0,
            "market_h2h_home_move": 0.0,
            "market_h2h_away_move": 0.0,
            "market_spread_home_move": 0.0,
            "market_total_line_move": 0.0,
            "market_observation_count": 0.0,
            "market_source_count": 0.0,
            "market_regime_stable": 1.0,
            "market_regime_moving": 0.0,
            "market_regime_volatile": 0.0,
        }
        market_df = self.load_market_signals(season)
        if market_df.empty:
            return defaults

        match = market_df.loc[market_df.get("game_id", pd.Series(dtype=str)).astype(str) == str(game_id)]

        if match.empty and home_team and away_team and date:
            if all(c in market_df.columns for c in ("home_team", "away_team", "date")):
                date_s = str(date)[:10]
                def _norm(s: str) -> str:
                    return str(s).lower().strip()
                hn, an = _norm(home_team), _norm(away_team)
                mask = (
                    market_df["date"].astype(str).str[:10] == date_s
                ) & (
                    market_df["home_team"].astype(str).str.lower().str.strip() == hn
                ) & (
                    market_df["away_team"].astype(str).str.lower().str.strip() == an
                )
                match = market_df.loc[mask]

        if match.empty:
            return defaults

        def _mean_col(col: str) -> float:
            if col not in match.columns:
                return 0.0
            vals = pd.to_numeric(match[col], errors="coerce").dropna()
            return float(vals.mean()) if not vals.empty else 0.0

        regime = (
            match["market_regime"].astype(str).str.lower().value_counts().idxmax()
            if "market_regime" in match.columns and not match.empty
            else "stable"
        )

        return {
            "market_aggregate_abs_move": _mean_col("aggregate_abs_move"),
            "market_h2h_home_move": _mean_col("h2h_home_move"),
            "market_h2h_away_move": _mean_col("h2h_away_move"),
            "market_spread_home_move": _mean_col("spread_home_move"),
            "market_total_line_move": _mean_col("total_line_move"),
            "market_observation_count": _mean_col("observation_count"),
            "market_source_count": _mean_col("source_count"),
            "market_regime_stable": 1.0 if regime == "stable" else 0.0,
            "market_regime_moving": 1.0 if regime == "moving" else 0.0,
            "market_regime_volatile": 1.0 if regime == "volatile" else 0.0,
        }

    def _schedule_fatigue_features(
        self,
        game_id: str,
        team_id: str,
        season: int,
    ) -> dict[str, float]:
        """Return team-level fatigue features from normalized schedule enrichment."""
        defaults = {
            "fatigue_rest_days": 7.0,
            "fatigue_is_back_to_back": 0.0,
            "fatigue_games_last_7d": 0.0,
            "fatigue_games_last_14d": 0.0,
            "fatigue_home_away_switch": 0.0,
            "fatigue_away_streak_before": 0.0,
            "fatigue_home_streak_before": 0.0,
            "fatigue_score": 0.0,
            "fatigue_level_low": 1.0,
            "fatigue_level_medium": 0.0,
            "fatigue_level_high": 0.0,
        }
        fatigue_df = self.load_schedule_fatigue(season)
        if fatigue_df.empty or "game_id" not in fatigue_df.columns:
            return defaults

        mask = fatigue_df["game_id"].astype(str) == str(game_id)
        if "team_id" in fatigue_df.columns and str(team_id):
            mask = mask & (fatigue_df["team_id"].astype(str) == str(team_id))
        match = fatigue_df.loc[mask]
        if match.empty:
            return defaults

        row = match.iloc[0]
        level = str(row.get("fatigue_level", "low")).lower()
        rest_days = pd.to_numeric(row.get("rest_days"), errors="coerce")

        return {
            "fatigue_rest_days": float(rest_days) if pd.notna(rest_days) else 7.0,
            "fatigue_is_back_to_back": float(pd.to_numeric(row.get("is_back_to_back"), errors="coerce") or 0.0),
            "fatigue_games_last_7d": float(pd.to_numeric(row.get("games_last_7d"), errors="coerce") or 0.0),
            "fatigue_games_last_14d": float(pd.to_numeric(row.get("games_last_14d"), errors="coerce") or 0.0),
            "fatigue_home_away_switch": float(pd.to_numeric(row.get("home_away_switch"), errors="coerce") or 0.0),
            "fatigue_away_streak_before": float(pd.to_numeric(row.get("away_streak_before"), errors="coerce") or 0.0),
            "fatigue_home_streak_before": float(pd.to_numeric(row.get("home_streak_before"), errors="coerce") or 0.0),
            "fatigue_score": float(pd.to_numeric(row.get("fatigue_score"), errors="coerce") or 0.0),
            "fatigue_level_low": 1.0 if level == "low" else 0.0,
            "fatigue_level_medium": 1.0 if level == "medium" else 0.0,
            "fatigue_level_high": 1.0 if level == "high" else 0.0,
        }

    def _standings_features(
        self,
        team_id: str,
        season: int,
    ) -> dict[str, float]:
        """Enhanced standings features: record, L10, SOS, rankings.

        Parses home/away record strings (e.g. '34-7') and last-ten records.
        """
        defaults = {
            "stnd_win_pct": 0.0,
            "stnd_home_win_pct": 0.0,
            "stnd_away_win_pct": 0.0,
            "stnd_l10_win_pct": 0.0,
            "stnd_games_behind": 0.0,
            "stnd_conf_rank": 0.0,
            "stnd_div_rank": 0.0,
            "stnd_overall_rank": 0.0,
            "stnd_streak": 0.0,
            "stnd_pts_diff": 0.0,
            "stnd_sos": 0.0,
        }
        standings = self.load_standings(season)
        if standings.empty or "team_id" not in standings.columns:
            return defaults

        row_df = standings.loc[standings["team_id"] == str(team_id)]
        if row_df.empty:
            return defaults
        row = row_df.iloc[0]

        def _num(val: Any, fallback: float = 0.0) -> float:
            v = pd.to_numeric(val, errors="coerce")
            return float(v) if pd.notna(v) else fallback

        def _parse_record(rec: Any) -> tuple[float, float]:
            """Parse 'W-L' string to (wins, losses)."""
            try:
                parts = str(rec).split("-")
                if len(parts) >= 2:
                    return float(parts[0]), float(parts[1])
            except (ValueError, TypeError):
                pass
            return 0.0, 0.0

        win_pct = _num(row.get("pct"))
        pf = _num(row.get("points_for"))
        pa = _num(row.get("points_against"))
        pts_diff = pf - pa

        # Home/away records
        hw, hl = _parse_record(row.get("home_record"))
        home_wpct = hw / (hw + hl) if (hw + hl) > 0 else 0.0
        aw, al = _parse_record(row.get("away_record"))
        away_wpct = aw / (aw + al) if (aw + al) > 0 else 0.0

        # Last 10
        l10w, l10l = _parse_record(row.get("last_ten"))
        l10_pct = l10w / (l10w + l10l) if (l10w + l10l) > 0 else 0.0

        # Games behind leader (estimate from rank and win_pct gap)
        conf_rank = _num(row.get("conference_rank"))
        div_rank = _num(row.get("division_rank"))
        overall_rank = _num(row.get("overall_rank"))

        # GB: find top team in same conference
        gb = 0.0
        conf = str(row.get("conference", ""))
        if conf and conf.lower() not in ("", "none", "nan"):
            conf_teams = standings.loc[
                standings["conference"].astype(str) == conf
            ]
            if not conf_teams.empty:
                top_pct = pd.to_numeric(conf_teams["pct"], errors="coerce").max()
                gp = _num(row.get("games_played"))
                if gp > 0 and pd.notna(top_pct):
                    gb = (float(top_pct) - win_pct) * gp / 2.0

        # Streak: parse e.g. "W5" → +5, "L3" → -3
        streak_val = 0.0
        streak_str = str(row.get("streak", ""))
        if streak_str and streak_str[0] in ("W", "w"):
            streak_val = _num(streak_str[1:])
        elif streak_str and streak_str[0] in ("L", "l"):
            streak_val = -_num(streak_str[1:])

        # SOS: average opponent win% (approximated from pts ratio)
        sos = 0.0
        gp = _num(row.get("games_played"))
        if gp > 0 and (pf + pa) > 0:
            sos = pa / (pf + pa)  # higher = tougher opponents

        return {
            "stnd_win_pct": win_pct,
            "stnd_home_win_pct": home_wpct,
            "stnd_away_win_pct": away_wpct,
            "stnd_l10_win_pct": l10_pct,
            "stnd_games_behind": gb,
            "stnd_conf_rank": conf_rank,
            "stnd_div_rank": div_rank,
            "stnd_overall_rank": overall_rank,
            "stnd_streak": streak_val,
            "stnd_pts_diff": pts_diff,
            "stnd_sos": sos,
        }

    def _player_stats_features(
        self,
        team_id: str,
        season: int,
        date: str | None = None,
    ) -> dict[str, float]:
        """Aggregated team-level stats from individual player stats.

        Sport-aware: NBA gets pts/reb/ast + advanced stats, NFL gets passing/rushing stats.
        Filters by date to prevent data leakage when date is provided.
        Falls back gracefully if columns are missing.
        """
        defaults = {
            "pstats_team_ppg": 0.0,
            "pstats_team_rpg": 0.0,
            "pstats_team_apg": 0.0,
            "pstats_top_scorer_ppg": 0.0,
            "pstats_top_scorer_share": 0.0,
            "pstats_player_count": 0.0,
            "pstats_team_spg": 0.0,
            "pstats_team_bpg": 0.0,
            "pstats_team_topg": 0.0,
            "pstats_avg_plus_minus": 0.0,
            "pstats_efg_pct": 0.0,
            "pstats_ts_pct": 0.0,
            "pstats_ast_to_ratio": 0.0,
        }
        player_stats = self.load_player_stats(season)
        if player_stats.empty or "team_id" not in player_stats.columns:
            return defaults

        # Try direct match first, then mapped ID
        tid = str(team_id)
        team_ps = player_stats.loc[player_stats["team_id"].astype(str) == tid]

        if team_ps.empty:
            self._build_team_id_map(season)
            mapped_id = self._team_id_map.get(tid)
            if mapped_id:
                team_ps = player_stats.loc[player_stats["team_id"].astype(str) == mapped_id]

        if team_ps.empty:
            return defaults

        # Date filtering: only use stats from games before this game (prevent leakage)
        if date and "date" in team_ps.columns:
            game_date = pd.to_datetime(date, errors="coerce")
            if not pd.isna(game_date):
                team_ps = team_ps.copy()
                team_ps["_date"] = pd.to_datetime(team_ps["date"], errors="coerce")
                team_ps = team_ps.loc[team_ps["_date"] < game_date]

        if team_ps.empty:
            return defaults

        feats: dict[str, float] = {}

        def _col_mean(col: str) -> float:
            if col in team_ps.columns:
                return float(pd.to_numeric(team_ps[col], errors="coerce").fillna(0).mean())
            return 0.0

        # Basketball stats: pts, reb, ast
        if "pts" in team_ps.columns:
            pts = pd.to_numeric(team_ps["pts"], errors="coerce").fillna(0)
            feats["pstats_team_ppg"] = float(pts.mean())

            # Top scorer
            if "player_id" in team_ps.columns:
                player_totals = (
                    team_ps.assign(_pts=pd.to_numeric(team_ps["pts"], errors="coerce").fillna(0))
                    .groupby("player_id")["_pts"]
                    .mean()
                )
                if not player_totals.empty:
                    feats["pstats_top_scorer_ppg"] = float(player_totals.max())
                    team_total = player_totals.sum()
                    feats["pstats_top_scorer_share"] = (
                        float(player_totals.max() / team_total) if team_total > 0 else 0.0
                    )
                else:
                    feats["pstats_top_scorer_ppg"] = 0.0
                    feats["pstats_top_scorer_share"] = 0.0
            else:
                feats["pstats_top_scorer_ppg"] = 0.0
                feats["pstats_top_scorer_share"] = 0.0
        else:
            feats["pstats_team_ppg"] = 0.0
            feats["pstats_top_scorer_ppg"] = 0.0
            feats["pstats_top_scorer_share"] = 0.0

        feats["pstats_team_rpg"] = _col_mean("reb")
        feats["pstats_team_apg"] = _col_mean("ast")

        # Advanced defensive/efficiency stats
        feats["pstats_team_spg"] = _col_mean("stl")
        feats["pstats_team_bpg"] = _col_mean("blk")
        feats["pstats_team_topg"] = _col_mean("to")
        feats["pstats_avg_plus_minus"] = _col_mean("plus_minus")

        # Shooting efficiency: eFG% = (FGM + 0.5 * 3PM) / FGA
        fgm = pd.to_numeric(team_ps.get("fgm", pd.Series(dtype=float)), errors="coerce").fillna(0)
        fga = pd.to_numeric(team_ps.get("fga", pd.Series(dtype=float)), errors="coerce").fillna(0)
        three_m = pd.to_numeric(team_ps.get("three_m", pd.Series(dtype=float)), errors="coerce").fillna(0)
        ftm = pd.to_numeric(team_ps.get("ftm", pd.Series(dtype=float)), errors="coerce").fillna(0)
        fta = pd.to_numeric(team_ps.get("fta", pd.Series(dtype=float)), errors="coerce").fillna(0)
        pts_arr = pd.to_numeric(team_ps.get("pts", pd.Series(dtype=float)), errors="coerce").fillna(0)

        total_fga = float(fga.sum())
        total_fgm = float(fgm.sum())
        total_3m = float(three_m.sum())
        total_ftm = float(ftm.sum())
        total_fta = float(fta.sum())
        total_pts = float(pts_arr.sum())

        feats["pstats_efg_pct"] = (
            float((total_fgm + 0.5 * total_3m) / total_fga) if total_fga > 0 else 0.0
        )
        ts_denom = 2 * (total_fga + 0.44 * total_fta)
        feats["pstats_ts_pct"] = float(total_pts / ts_denom) if ts_denom > 0 else 0.0

        # Assist to turnover ratio
        total_ast = float(pd.to_numeric(team_ps.get("ast", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        total_to = float(pd.to_numeric(team_ps.get("to", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        feats["pstats_ast_to_ratio"] = float(total_ast / total_to) if total_to > 0 else 0.0

        # Player count on roster
        if "player_id" in team_ps.columns:
            feats["pstats_player_count"] = float(team_ps["player_id"].nunique())
        else:
            feats["pstats_player_count"] = float(len(team_ps))

        return feats

    def _strength_of_schedule(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        season: int,
        window: int = 20,
    ) -> float:
        """Strength of schedule: average opponent win% for recent games."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return 0.5

        standings = self.load_standings(season)
        if standings.empty or "team_id" not in standings.columns or "pct" not in standings.columns:
            return 0.5

        opp_pcts = []
        for _, game in recent.iterrows():
            opp_id = (
                str(game.get("away_team_id"))
                if str(game.get("home_team_id")) == str(team_id)
                else str(game.get("home_team_id"))
            )
            opp_row = standings.loc[standings["team_id"].astype(str) == opp_id]
            if not opp_row.empty:
                pct = pd.to_numeric(opp_row.iloc[0].get("pct", 0.5), errors="coerce")
                if pd.notna(pct):
                    opp_pcts.append(float(pct))

        return float(np.mean(opp_pcts)) if opp_pcts else 0.5

    def _scoring_last_n(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        n: int = 5,
    ) -> dict[str, float]:
        """Scoring averages over the last *n* games."""
        recent = self._team_games_before(games, team_id, date, limit=n)
        if recent.empty:
            return {"last_n_ppg": 0.0, "last_n_opp_ppg": 0.0, "last_n_margin": 0.0}

        team_pts, opp_pts = self._vec_team_scores(recent, team_id)
        return {
            "last_n_ppg": float(team_pts.mean()),
            "last_n_opp_ppg": float(opp_pts.mean()),
            "last_n_margin": float((team_pts - opp_pts).mean()),
        }

    def _period_rolling_stats(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        n: int = 10,
        period_scheme: str = "quarters",  # "quarters" | "periods" | "innings" | "halves"
    ) -> dict[str, float]:
        """Rolling per-period scoring stats: Q1/P1/inning averages + halftime/comeback metrics.

        period_scheme:
          "quarters"  — NBA/NFL/NCAAB/NCAAF: home/away_q1..q4
          "periods"   — NHL: home/away_p1..p3
          "innings"   — MLB: home/away_i1..i9
          "halves"    — Soccer: home/away_h1..h2
        """
        defaults: dict[str, float] = {
            "period_first_ppg": 0.0,
            "period_first_opp_ppg": 0.0,
            "period_first_win_pct": 0.0,
            "period_first_half_ppg": 0.0,
            "period_first_half_opp_ppg": 0.0,
            "period_first_half_win_pct": 0.0,
            "period_second_half_ppg": 0.0,
            "period_comeback_rate": 0.0,
            "period_ot_rate": 0.0,
        }
        recent = self._team_games_before(games, team_id, date, limit=n)
        if recent.empty:
            return defaults

        tid = str(team_id)

        def _is_home(row: pd.Series) -> bool:
            return str(row.get("home_team_id", "")) == tid

        if period_scheme == "quarters":
            p_names = ["q1", "q2", "q3", "q4"]
            ot_col = "ot"
        elif period_scheme == "periods":
            p_names = ["p1", "p2", "p3"]
            ot_col = "ot"
        elif period_scheme == "halves":
            p_names = ["h1", "h2"]
            ot_col = "ot"
        else:  # innings
            p_names = [f"i{i}" for i in range(1, 10)]
            ot_col = None

        first_scores, first_opp, first_half, first_half_opp, second_half, comebacks, ot_games = (
            [], [], [], [], [], [], []
        )

        for _, row in recent.iterrows():
            home = _is_home(row)
            prefix = "home_" if home else "away_"
            opp_prefix = "away_" if home else "home_"

            ps = [pd.to_numeric(row.get(f"{prefix}{p}"), errors="coerce") for p in p_names]
            os_ = [pd.to_numeric(row.get(f"{opp_prefix}{p}"), errors="coerce") for p in p_names]

            if pd.notna(ps[0]) and pd.notna(os_[0]):
                first_scores.append(float(ps[0]))
                first_opp.append(float(os_[0]))

            if period_scheme in ("quarters", "periods", "halves"):
                mid = 2 if period_scheme == "quarters" else (1 if period_scheme == "periods" else 1)
                h1_team = sum(float(v) for v in ps[:mid] if pd.notna(v))
                h1_opp = sum(float(v) for v in os_[:mid] if pd.notna(v))
                h2_team = sum(float(v) for v in ps[mid:] if pd.notna(v))
                if any(pd.notna(v) for v in ps[:mid]):
                    first_half.append(h1_team)
                    first_half_opp.append(h1_opp)
                    second_half.append(h2_team)
                    comebacks.append(1.0 if h1_team < h1_opp and h2_team > h1_opp else 0.0)

            if ot_col:
                ot_val = pd.to_numeric(row.get(f"home_{ot_col}"), errors="coerce")
                ot_games.append(1.0 if pd.notna(ot_val) and ot_val > 0 else 0.0)

        result: dict[str, float] = dict(defaults)
        if first_scores:
            result["period_first_ppg"] = float(np.mean(first_scores))
            result["period_first_opp_ppg"] = float(np.mean(first_opp))
            result["period_first_win_pct"] = float(
                np.mean([1.0 if s > o else 0.0 for s, o in zip(first_scores, first_opp)])
            )
        if first_half:
            result["period_first_half_ppg"] = float(np.mean(first_half))
            result["period_first_half_opp_ppg"] = float(np.mean(first_half_opp))
            result["period_first_half_win_pct"] = float(
                np.mean([1.0 if s > o else 0.0 for s, o in zip(first_half, first_half_opp)])
            )
            result["period_second_half_ppg"] = float(np.mean(second_half))
            result["period_comeback_rate"] = float(np.mean(comebacks))
        if ot_games:
            result["period_ot_rate"] = float(np.mean(ot_games))
        return result

    # ── Abstract Interface ────────────────────────────────

    @abstractmethod
    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        """Extract a feature vector for a single game/match/event.

        Parameters
        ----------
        game:
            A dict from a games DataFrame row (via ``row.to_dict()``).

        Returns
        -------
        dict mapping feature names to numeric values.
        """

    @abstractmethod
    def get_feature_names(self) -> list[str]:
        """Return the ordered list of feature names this extractor produces."""

    # ── Batch Extraction ──────────────────────────────────

    def extract_all(
        self, season: int, *, existing_game_ids: set[str] | None = None,
    ) -> pd.DataFrame:
        """Extract features for every game in *season*.

        Parameters
        ----------
        existing_game_ids : set[str] | None
            If provided, skip games whose id is already in this set
            (incremental extraction — only process NEW completed games).

        Returns a DataFrame where each row is a feature vector
        and columns match ``get_feature_names()`` (plus metadata
        columns like ``game_id``, ``date``, ``home_team_id``,
        ``away_team_id``, ``home_score``, ``away_score``).
        """
        games = self.load_games(season)
        if games.empty:
            logger.warning("No games found for %s season %s", self.sport, season)
            return pd.DataFrame()

        # Only process completed games — skip scheduled/upcoming rows that have no scores.
        # This prevents unplayed games (home_score=NaN→0) from polluting training data.
        completed_mask = pd.Series(True, index=games.index)
        if "status" in games.columns:
            completed_mask = games["status"].str.lower().isin(_COMPLETED_STATUSES)
        if "home_score" in games.columns and "away_score" in games.columns:
            # Scores must be present AND at least one team must have scored,
            # otherwise scheduled games with 0-0 placeholder scores pass through.
            has_real_scores = (
                games["home_score"].notna()
                & games["away_score"].notna()
                & ((games["home_score"] > 0) | (games["away_score"] > 0))
            )
            completed_mask = completed_mask | has_real_scores
        games = games[completed_mask].reset_index(drop=True)
        if games.empty:
            logger.warning("No completed games for %s season %s", self.sport, season)
            return pd.DataFrame()

        # Incremental: skip already-extracted games
        if existing_game_ids:
            id_col = "id" if "id" in games.columns else "game_id"
            before = len(games)
            games = games[~games[id_col].astype(str).isin(existing_game_ids)].reset_index(drop=True)
            skipped = before - len(games)
            if games.empty:
                logger.info(
                    "%s season %s: all %d games already extracted — skipping",
                    self.sport, season, skipped,
                )
                return pd.DataFrame()
            logger.info(
                "%s season %s: %d new games to extract (%d cached)",
                self.sport, season, len(games), skipped,
            )

        features: list[dict[str, Any]] = []
        success, failed = 0, 0

        for _, game in games.iterrows():
            try:
                f = self.extract_game_features(game.to_dict())
                features.append(f)
                success += 1
            except Exception as e:
                failed += 1
                logger.warning(
                    "Failed to extract features for game %s: %s",
                    game.get("id", "unknown"),
                    e,
                )

        logger.info(
            "%s season %s: extracted %d/%d games (%d failed)",
            self.sport,
            season,
            success,
            success + failed,
            failed,
        )
        return pd.DataFrame(features)

    def clear_cache(self) -> None:
        """Drop all cached DataFrames to free memory."""
        self._games_cache.clear()
        self._team_stats_cache.clear()
        self._player_stats_cache.clear()
        self._odds_cache.clear()
        self._market_signals_cache.clear()
        self._schedule_fatigue_cache.clear()
        self._injuries_cache.clear()
        self._standings_cache.clear()
        self._team_id_map.clear()
        self._team_history_idx.clear()
        self._team_splits_idx.clear()
