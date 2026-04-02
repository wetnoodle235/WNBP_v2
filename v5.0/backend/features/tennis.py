# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Tennis
# ──────────────────────────────────────────────────────────
#
# Covers ATP and WTA.  Produces ~30 features per match
# including surface-specific win rates, serve/return stats,
# break point conversion, ranking, H2H, and fatigue.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor, _COMPLETED_STATUSES

logger = logging.getLogger(__name__)

_SURFACES = ("hard", "clay", "grass", "carpet")

# Venue → surface mapping for tournaments without explicit surface data
# Based on well-known tournament venues (city, country)
# Tournament prestige levels (4=Grand Slam, 3=Masters 1000, 2=ATP 500, 1=ATP 250/other)
_TOURNAMENT_PRESTIGE: dict[str, int] = {
    "australian open": 4, "roland garros": 4, "french open": 4,
    "wimbledon": 4, "us open": 4,
    "indian wells masters": 3, "miami masters": 3, "monte carlo masters": 3,
    "madrid masters": 3, "rome masters": 3, "canada masters": 3,
    "cincinnati masters": 3, "shanghai masters": 3, "paris masters": 3,
    "paris bercy": 3, "rolex paris masters": 3,
    "halle": 2, "queen's club": 2, "queens club": 2, "dubai": 2,
    "doha": 2, "rotterdam": 2, "barcelona": 2, "washington": 2,
    "tokyo": 2, "vienna": 2, "basel": 2, "beijing": 2,
    "astana": 2, "stockholm": 2, "atp finals": 4, "nitto atp finals": 4,
    "united cup": 2, "davis cup": 3, "laver cup": 3,
}

_VENUE_SURFACE: dict[str, str] = {
    # Grand Slams
    "melbourne": "hard",         # Australian Open
    "roland garros": "clay",     # French Open
    "paris, france": "clay",     # Roland Garros city match
    "london": "grass",           # Wimbledon
    "new york": "hard",          # US Open
    "flushing": "hard",          # US Open (Flushing Meadows)
    # Masters 1000 — Hard
    "indian wells": "hard",
    "miami": "hard",
    "montreal": "hard",
    "toronto": "hard",
    "cincinnati": "hard",
    "shanghai": "hard",
    "beijing": "hard",
    "vienna": "hard",
    # Masters 1000 — Clay
    "monte-carlo": "clay",
    "monte carlo": "clay",
    "madrid": "clay",
    "rome": "clay",
    # Masters 1000 — Grass / Indoor
    "halle": "grass",
    "queen's club": "grass",
    "queens club": "grass",
    "eastbourne": "grass",
    # ATP 500 / 250 common venues
    "dubai": "hard",
    "doha": "hard",
    "abu dhabi": "hard",
    "acapulco": "hard",
    "rotterdam": "hard",
    "barcelona": "clay",
    "bucharest": "clay",
    "estoril": "clay",
    "geneva": "clay",
    "lyon": "clay",
    "hamburg": "clay",
    "gstaad": "clay",
    "kitzbuhel": "clay",
    "bastad": "clay",
    "umag": "clay",
    "metz": "hard",
    "st. petersburg": "hard",
    "astana": "hard",
    "stockholm": "hard",
    "sofia": "hard",
    "pune": "hard",
    "hong kong": "hard",
    "moscow": "hard",
    "antwerp": "hard",
    "tokyo": "hard",
    "osaka": "hard",
    "zhuhai": "hard",
}


def _infer_surface(game: dict) -> str:
    """Infer court surface from game data — check explicit field first, then venue mapping."""
    import math as _math
    surface = game.get("surface") or game.get("court_type") or ""
    # Treat float NaN as missing
    try:
        if _math.isnan(surface):
            surface = ""
    except (TypeError, ValueError):
        pass
    if surface and str(surface).lower() not in ("", "unknown", "none", "nan"):
        return str(surface).lower()
    venue = str(game.get("venue", "") or "").lower()
    for key, surf in _VENUE_SURFACE.items():
        if key in venue:
            return surf
    return "hard"  # default: most common surface


def _infer_tournament_prestige(game: dict) -> int:
    """Return tournament prestige level (4=Grand Slam, 3=Masters, 2=500, 1=250/other)."""
    venue = str(game.get("venue", "") or "").lower()
    for key, level in _TOURNAMENT_PRESTIGE.items():
        if key in venue:
            return level
    return 1  # default: ATP 250 / small event


class TennisExtractor(BaseFeatureExtractor):
    """Feature extractor for tennis (ATP, WTA)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_games_cache: pd.DataFrame | None = None
        # Bridge: player_name_lower → numeric ESPN player_id
        # Populated in _load_all_player_stats() from games with numeric IDs
        self._name_to_player_id: dict[str, str] = {}

    def _load_all_games(self) -> pd.DataFrame:
        """Load and cache all seasons' game data for cross-season form calculations."""
        if self._all_games_cache is not None:
            return self._all_games_cache
        try:
            combined = self._reader.load_all_seasons(self.sport, "games")
        except Exception:
            combined = pd.DataFrame()
        if not combined.empty and "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
            combined.sort_values("date", inplace=True, ignore_index=True)
        # Infer surface from venue for rows missing surface data
        if not combined.empty and "venue" in combined.columns:
            missing_surf = combined.get("surface", pd.Series(dtype=str)).isna() | \
                           combined.get("surface", pd.Series(dtype=str)).eq("")
            if "surface" not in combined.columns:
                combined["surface"] = ""
                missing_surf = pd.Series(True, index=combined.index)
            combined.loc[missing_surf, "surface"] = combined.loc[missing_surf].apply(
                lambda row: _infer_surface(row.to_dict()), axis=1
            )
        self._all_games_cache = combined
        return combined

    def extract_all(
        self, season: int, *, existing_game_ids: set[str] | None = None,
    ) -> pd.DataFrame:
        """Override extract_all to pre-load games/odds once and reuse across all games.
        
        This prevents 9,605× redundant file loads that were causing hangs.
        """
        games = self.load_games(season)
        all_games = self._load_all_games()
        odds = self.load_odds(season)
        standings = self.load_standings(season)
        if games.empty:
            logger.warning("No games found for %s season %s", self.sport, season)
            return pd.DataFrame()

        # Filter to completed games only
        completed_mask = pd.Series(True, index=games.index)
        if "status" in games.columns:
            completed_mask = games["status"].str.lower().isin(_COMPLETED_STATUSES)
        if "home_score" in games.columns and "away_score" in games.columns:
            has_scores = games["home_score"].notna() & games["away_score"].notna()
            completed_mask = completed_mask | has_scores
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
                f = self.extract_game_features_cached(game.to_dict(), all_games, odds, standings)
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

    def extract_game_features_cached(
        self,
        game: dict[str, Any],
        games_df: pd.DataFrame,
        odds_df: pd.DataFrame,
        standings_df: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """Extract features using pre-loaded games and odds DataFrames.
        
        This is the cached version called from extract_all to avoid redundant loads.
        """
        h_id_raw = str(game.get("home_team_id", game.get("player_a_id", "")))
        a_id_raw = str(game.get("away_team_id", game.get("player_b_id", "")))
        h_name = str(game.get("home_team", ""))
        a_name = str(game.get("away_team", ""))
        date = str(game.get("date", ""))
        game_id = str(game.get("id", ""))
        surface = _infer_surface(game)

        # Ensure player_stats index is built (populates _name_to_player_id bridge)
        self._load_all_player_stats()

        # Resolve UUID-based IDs (2025+) to numeric IDs for player_stats lookups
        h_id_resolved = self._resolve_player_id(h_id_raw, h_name)
        a_id_resolved = self._resolve_player_id(a_id_raw, a_name)
        # Use raw IDs for game-history lookups (game records use raw IDs)
        h_id = h_id_raw
        a_id = a_id_raw

        features: dict[str, Any] = {
            "game_id": game_id,
            "date": date,
            "home_team_id": h_id,
            "away_team_id": a_id,
            "home_score": pd.to_numeric(game.get("home_score"), errors="coerce"),
            "away_score": pd.to_numeric(game.get("away_score"), errors="coerce"),
        }

        # Surface encoding
        for s in _SURFACES:
            features[f"surface_{s}"] = 1.0 if surface == s else 0.0

        # Match format: Grand Slams (best-of-5) vs regular (best-of-3)
        best_of = int(game.get("best_of", 3) or 3)
        features["is_best_of_5"] = 1.0 if best_of >= 5 else 0.0

        # Tournament prestige level (1-4)
        prestige = _infer_tournament_prestige(game)
        features["tournament_prestige"] = float(prestige)
        features["is_grand_slam"] = 1.0 if prestige == 4 else 0.0
        features["is_masters"] = 1.0 if prestige == 3 else 0.0

        # Overall form (use raw ID for game-history lookups)
        h_form = self.team_form(h_id, date, games_df, window=10)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, games_df, window=10)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        # Surface form
        h_sf = self._surface_form(h_id, date, surface, games_df)
        features["home_surface_win_pct"] = h_sf["surface_win_pct"]
        features["home_surface_matches"] = float(h_sf["surface_matches"])
        a_sf = self._surface_form(a_id, date, surface, games_df)
        features["away_surface_win_pct"] = a_sf["surface_win_pct"]
        features["away_surface_matches"] = float(a_sf["surface_matches"])

        # H2H
        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)

        # Serve — use resolved numeric ID for player_stats lookups; raw ID for game history
        h_serve = self._player_serve_stats(h_id_resolved, date, games_df=games_df)
        features.update({f"home_{k}": v for k, v in h_serve.items()})
        a_serve = self._player_serve_stats(a_id_resolved, date, games_df=games_df)
        features.update({f"away_{k}": v for k, v in a_serve.items()})

        # Return
        h_ret = {"break_point_conversion": h_serve.get("break_point_conversion", 0.0),
                 "break_points_saved_pct": h_serve.get("break_points_saved_pct", 0.0),
                 "return_points_won_pct": h_serve.get("return_points_won_pct", 0.0)}
        features.update({f"home_{k}": v for k, v in h_ret.items()})
        a_ret = {"break_point_conversion": a_serve.get("break_point_conversion", 0.0),
                 "break_points_saved_pct": a_serve.get("break_points_saved_pct", 0.0),
                 "return_points_won_pct": a_serve.get("return_points_won_pct", 0.0)}
        features.update({f"away_{k}": v for k, v in a_ret.items()})

        # Ranking
        h_rank = self._ranking_features(game, "home_", standings_df=standings_df)
        features.update({f"home_{k}": v for k, v in h_rank.items()})
        a_rank = self._ranking_features(game, "away_", standings_df=standings_df)
        features.update({f"away_{k}": v for k, v in a_rank.items()})
        features["ranking_diff"] = features["home_ranking"] - features["away_ranking"]

        # Fatigue
        h_fat = self._fatigue_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_fat.items()})
        a_fat = self._fatigue_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_fat.items()})

        # Win streak and days since last match
        features["home_win_streak"] = self._win_streak(h_id, date, games_df)
        features["away_win_streak"] = self._win_streak(a_id, date, games_df)
        features["win_streak_diff"] = features["home_win_streak"] - features["away_win_streak"]
        features["home_days_since_last"] = self._days_since_last_match(h_id, date, games_df)
        features["away_days_since_last"] = self._days_since_last_match(a_id, date, games_df)
        features["rest_advantage"] = features["home_days_since_last"] - features["away_days_since_last"]

        # Set win rate + tiebreak win pct (pressure/clutch metric)
        features["home_set_win_rate"] = self._set_win_rate(h_id, date, games_df)
        features["away_set_win_rate"] = self._set_win_rate(a_id, date, games_df)
        features["home_tiebreak_win_pct"] = self._tiebreak_win_pct(h_id, date, games_df)
        features["away_tiebreak_win_pct"] = self._tiebreak_win_pct(a_id, date, games_df)
        features["tiebreak_win_pct_diff"] = (
            features["home_tiebreak_win_pct"] - features["away_tiebreak_win_pct"]
        )

        # ELO ratings (computed from match history)
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Key differentials
        features["surface_win_pct_diff"] = (
            features.get("home_surface_win_pct", 0.0) - features.get("away_surface_win_pct", 0.0)
        )
        features["first_serve_pct_diff"] = (
            features.get("home_first_serve_pct", 0.0) - features.get("away_first_serve_pct", 0.0)
        )
        features["first_serve_won_pct_diff"] = (
            features.get("home_first_serve_won_pct", 0.0) - features.get("away_first_serve_won_pct", 0.0)
        )
        features["second_serve_won_pct_diff"] = (
            features.get("home_second_serve_won_pct", 0.0) - features.get("away_second_serve_won_pct", 0.0)
        )
        features["break_point_conversion_diff"] = (
            features.get("home_break_point_conversion", 0.0) - features.get("away_break_point_conversion", 0.0)
        )
        features["break_points_saved_pct_diff"] = (
            features.get("home_break_points_saved_pct", 0.0) - features.get("away_break_points_saved_pct", 0.0)
        )
        features["aces_per_match_diff"] = (
            features.get("home_aces_per_match", 0.0) - features.get("away_aces_per_match", 0.0)
        )
        features["double_faults_per_match_diff"] = (
            features.get("away_double_faults_per_match", 0.0) - features.get("home_double_faults_per_match", 0.0)
        )
        features["set_win_rate_diff"] = (
            features.get("home_set_win_rate", 0.0) - features.get("away_set_win_rate", 0.0)
        )
        features["elo_diff"] = (
            features.get("home_elo", 1500.0) - features.get("away_elo", 1500.0)
        )
        features["fatigue_diff"] = (
            features.get("away_matches_7d", 0.0) - features.get("home_matches_7d", 0.0)
        )

        # Form differentials (overall recent form)
        features["form_win_pct_diff"] = (
            features.get("home_form_win_pct", 0.0) - features.get("away_form_win_pct", 0.0)
        )
        features["form_ppg_diff"] = (
            features.get("home_form_ppg", 0.0) - features.get("away_form_ppg", 0.0)
        )
        features["form_margin_diff"] = (
            features.get("home_form_avg_margin", 0.0) - features.get("away_form_avg_margin", 0.0)
        )
        # Ranking points differential (raw points, not just rank number)
        features["ranking_points_diff"] = (
            features.get("home_ranking_points", 0.0) - features.get("away_ranking_points", 0.0)
        )
        # Surface-specific H2H win rate on current surface
        h2h_surface = self._h2h_surface_win_pct(h_id, a_id, surface, games_df, date=date)
        features["h2h_surface_win_pct"] = h2h_surface
        features["h2h_surface_win_pct_diff"] = h2h_surface - 0.5  # centred signal

        # Tournament prestige-level form (e.g. Grand Slam performance history)
        prestige = int(game.get("tournament_prestige", 2) or 2)
        h_pf = self._prestige_form(h_id, date, games_df, prestige)
        a_pf = self._prestige_form(a_id, date, games_df, prestige)
        features["home_prestige_win_pct"] = h_pf["prestige_win_pct"]
        features["away_prestige_win_pct"] = a_pf["prestige_win_pct"]
        features["prestige_win_pct_diff"] = h_pf["prestige_win_pct"] - a_pf["prestige_win_pct"]

        # Strength of schedule proxy: average ranking of recent opponents
        features["home_avg_opp_ranking"] = self._avg_opponent_ranking(h_id, date, games_df)
        features["away_avg_opp_ranking"] = self._avg_opponent_ranking(a_id, date, games_df)
        # Lower avg_opp_ranking = harder schedule (lower rank = better player)
        features["schedule_difficulty_diff"] = features["away_avg_opp_ranking"] - features["home_avg_opp_ranking"]

        # Momentum: rolling net-set differential over last 5 matches
        features["home_momentum"] = self.momentum(h_id, date, games_df, window=5)
        features["away_momentum"] = self.momentum(a_id, date, games_df, window=5)
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]

        # Quality-weighted form (win rate weighted by opponent ranking quality)
        h_qf = self._ranking_quality_form(h_id, date, games_df, standings_df)
        features["home_ranking_quality_form"] = h_qf
        a_qf = self._ranking_quality_form(a_id, date, games_df, standings_df)
        features["away_ranking_quality_form"] = a_qf
        features["ranking_quality_form_diff"] = h_qf - a_qf

        # Quality metric differentials
        for _qk in ("tiebreaks_won_pm", "winners_per_match", "unforced_errors_per_match",
                    "winner_ue_ratio", "net_points_won_pm"):
            _sign = -1.0 if _qk == "unforced_errors_per_match" else 1.0
            features[f"{_qk}_diff"] = _sign * (
                features.get(f"home_{_qk}", 0.0) - features.get(f"away_{_qk}", 0.0)
            )

        # Odds
        odds = self._odds_features(game_id, odds_df)
        features.update(odds)

        return features

    # ── Helpers ────────────────────────────────────────────

    def _avg_opponent_ranking(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> float:
        """Average ranking of recent opponents (proxy for schedule difficulty).

        Lower return value = harder schedule (lower rank number = better player).
        Returns 200.0 (mid-tier) if no ranking data is available.
        """
        recent = self._team_games_before(games, player_id, date, limit=window)
        if recent.empty:
            return 200.0
        is_home = recent["home_team_id"] == player_id
        opp_rank = is_home.map(lambda h: "away_ranking" if h else "home_ranking")
        # Vectorized opponent ranking lookup
        rankings = []
        for idx, row in recent.iterrows():
            col = "away_ranking" if row.get("home_team_id") == player_id else "home_ranking"
            rank = pd.to_numeric(row.get(col), errors="coerce")
            if pd.notna(rank) and rank > 0:
                rankings.append(float(rank))
        return float(sum(rankings) / len(rankings)) if rankings else 200.0

    def _surface_form(
        self,
        player_id: str,
        date: str,
        surface: str,
        games: pd.DataFrame,
        window: int = 20,
    ) -> dict[str, float]:
        """Win rate on a specific surface."""
        recent = self._team_games_before(games, player_id, date)
        surface_games = recent.loc[
            recent.get("surface", pd.Series("", index=recent.index)).str.lower() == surface.lower()
        ].head(window)

        if surface_games.empty:
            return {"surface_win_pct": 0.0, "surface_matches": 0}

        wins = self._vec_win_flags(surface_games, player_id)
        return {
            "surface_win_pct": float(wins.mean()),
            "surface_matches": len(surface_games),
        }

    def _load_all_player_stats(self) -> pd.DataFrame:
        """Load and concatenate player stats from all available seasons, enriched with game dates.
        
        Cached on first load to avoid expensive groupby on ~30k rows.
        """
        if self._all_pstats_cache is not None:
            return self._all_pstats_cache
        if isinstance(self._all_pstats_cache, pd.DataFrame) and len(self._all_pstats_cache) == 0:
            # Already tried, empty result
            return self._all_pstats_cache
            
        sport_dir = self.data_dir / "normalized" / self.sport
        try:
            combined_raw = self._reader.load_all_seasons(self.sport, "player_stats")
        except Exception:
            combined_raw = pd.DataFrame()
        if combined_raw.empty:
            self._all_pstats_cache = pd.DataFrame()
            self._build_name_id_bridge(sport_dir)
            return self._all_pstats_cache
        frames = [combined_raw]
        combined = pd.concat(frames, ignore_index=True)

        # Merge extra quality columns from player_stats.parquet (non-year-specific)
        # Note: these columns (tiebreaks_won, winners, etc.) are already present via
        # year-specific parquets when available. The full parquet currently has them all null,
        # so we skip the merge to avoid overhead.
        _extra_cols = [
            "tiebreaks_won", "winners", "unforced_errors", "net_points_won",
            "second_serve_pct", "break_points_won", "break_points_faced",
        ]
        # Ensure extra columns exist in combined (filled with NaN if not already present)
        for col in _extra_cols:
            if col not in combined.columns:
                combined[col] = np.nan

        # date is typically null — enrich from games files via game_id → id join
        if "game_id" in combined.columns:
            try:
                games_lookup = self._load_all_games()[["id", "date"]].drop_duplicates("id") if "id" in self._load_all_games().columns else pd.DataFrame()
            except Exception:
                games_lookup = pd.DataFrame()
            if not games_lookup.empty:
                games_lookup["date"] = pd.to_datetime(games_lookup["date"], errors="coerce")
                combined = combined.drop(columns=["date"], errors="ignore").merge(
                    games_lookup.rename(columns={"id": "game_id"}),
                    on="game_id", how="left"
                )

        if "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
            combined.sort_values("date", inplace=True, ignore_index=True)
        # Pre-convert player_id to str and cache it for fast per-game lookups
        if "player_id" in combined.columns:
            combined["_player_id_str"] = combined["player_id"].astype(str)
            # Build per-player index: dict[player_id_str -> sorted sub-DataFrame]
            # This is expensive (O(n log n)) so we only do it once and cache it
            self._player_stats_index = {
                pid: grp for pid, grp in combined.groupby("_player_id_str")
            }
        else:
            self._player_stats_index = {}

        # Build name → numeric ID bridge from all games
        self._build_name_id_bridge(sport_dir)

        self._all_pstats_cache = combined
        return combined

    def _build_name_id_bridge(self, sport_dir: Path | None = None) -> None:
        """Build player_name → numeric_player_id bridge from games with numeric IDs."""
        if self._name_to_player_id:
            return  # Already built
        try:
            gdf = self._load_all_games()
            if gdf.empty:
                return
            for name_col, id_col in [("home_team", "home_team_id"), ("away_team", "away_team_id")]:
                if name_col in gdf.columns and id_col in gdf.columns:
                    pairs = gdf[[name_col, id_col]].dropna()
                    numeric_mask = pairs[id_col].astype(str).str.match(r"^\d+$")
                    for _, row in pairs[numeric_mask].iterrows():
                        name_key = str(row[name_col]).strip().lower()
                        if name_key and name_key not in self._name_to_player_id:
                            self._name_to_player_id[name_key] = str(row[id_col])
        except Exception:
            pass

    def _resolve_player_id(self, player_id: str, player_name: str = "") -> str:
        """Resolve UUID-based player IDs to numeric IDs using name bridge.
        
        2025+ games use UUID team_ids while player_stats use numeric ESPN IDs.
        If player_id looks like a UUID and we have a name, look up by name.
        """
        if not player_id or player_id.isdigit():
            return player_id
        # Looks like UUID (contains dashes, not purely numeric)
        if "-" in player_id and player_name:
            name_key = player_name.strip().lower()
            resolved = self._name_to_player_id.get(name_key)
            if resolved:
                return resolved
        return player_id

    def _player_serve_stats(
        self,
        player_id: str,
        date: str,
        window: int = 10,
        games_df: "pd.DataFrame | None" = None,
    ) -> dict[str, float]:
        """Rolling serve/return stats from player_stats files and game history."""
        defaults = {
            "first_serve_pct": 0.0, "first_serve_won_pct": 0.0,
            "second_serve_won_pct": 0.0, "aces_per_match": 0.0,
            "double_faults_per_match": 0.0, "service_games_won_pct": 0.0,
            "break_point_conversion": 0.0, "break_points_saved_pct": 0.0,
            "return_points_won_pct": 0.0, "ace_df_ratio": 0.0,
            "tiebreaks_won_pm": 0.0, "winners_per_match": 0.0,
            "unforced_errors_per_match": 0.0, "winner_ue_ratio": 0.0,
            "net_points_won_pm": 0.0,
        }
        pstats = self._load_all_player_stats()
        result = dict(defaults)

        if not pstats.empty and "player_id" in pstats.columns:
            ts = pd.Timestamp(date)
            player_df = self._player_stats_index.get(str(player_id))
            if player_df is not None and not player_df.empty:
                recent = player_df.loc[player_df["date"] < ts].tail(window)
                if not recent.empty:
                    def _avg(col: str) -> float:
                        vals = pd.to_numeric(recent.get(col, pd.Series(dtype=float)), errors="coerce").dropna()
                        return float(vals.mean()) if len(vals) > 0 else 0.0

                    aces = _avg("aces")
                    dfs = _avg("double_faults")
                    bp_won = recent.get("break_points_won", pd.Series(dtype=float))
                    bp_faced = recent.get("break_points_faced", pd.Series(dtype=float))
                    bp_conv = 0.0
                    if hasattr(bp_faced, "sum") and pd.to_numeric(bp_faced, errors="coerce").sum() > 0:
                        bp_conv = float(
                            pd.to_numeric(bp_won, errors="coerce").sum() /
                            pd.to_numeric(bp_faced, errors="coerce").sum() * 100
                        )
                    result.update({
                        "first_serve_pct": _avg("first_serve_pct"),
                        "second_serve_won_pct": _avg("second_serve_pct"),
                        "aces_per_match": aces,
                        "double_faults_per_match": dfs,
                        "break_point_conversion": bp_conv,
                        "ace_df_ratio": float(aces / dfs) if dfs > 0 else float(aces * 2),
                    })
                    # Quality metrics from enriched player_stats.parquet
                    tb = _avg("tiebreaks_won")
                    winners = _avg("winners")
                    ue = _avg("unforced_errors")
                    np_won = _avg("net_points_won")
                    result.update({
                        "tiebreaks_won_pm": tb,
                        "winners_per_match": winners,
                        "unforced_errors_per_match": ue,
                        "winner_ue_ratio": float(winners / ue) if ue > 0 else float(winners * 0.1),
                        "net_points_won_pm": np_won,
                    })

        # Supplement with rolling stats from game history (has first_serve_won_pct etc.)
        if games_df is not None and not games_df.empty:
            recent_games = self._team_games_before(games_df, player_id, date, limit=window)
            if not recent_games.empty:
                is_home = (recent_games["home_team_id"] == player_id).values
                records = recent_games.to_dict("records")

                def _gavg(home_col: str, away_col: str) -> float:
                    vals = []
                    for row, h in zip(records, is_home):
                        v = pd.to_numeric(row.get(home_col if h else away_col), errors="coerce")
                        if pd.notna(v) and v > 0:
                            vals.append(float(v))
                    return float(np.mean(vals)) if vals else 0.0

                fsw = _gavg("home_first_serve_won_pct", "away_first_serve_won_pct")
                ssw = _gavg("home_second_serve_won_pct", "away_second_serve_won_pct")
                bpc = _gavg("home_break_point_conversion_pct", "away_break_point_conversion_pct")
                bps = _gavg("home_break_point_save_pct", "away_break_point_save_pct")
                adf = _gavg("home_ace_df_ratio", "away_ace_df_ratio")
                # Duration (common col, not side-specific)
                dur_vals = [float(v) for row in records
                            for v in [pd.to_numeric(row.get("duration_minutes"), errors="coerce")]
                            if pd.notna(v) and v > 0]
                if dur_vals:
                    result["avg_match_duration"] = float(np.mean(dur_vals))
                if fsw > 0:
                    result["first_serve_won_pct"] = fsw
                if ssw > 0:
                    result["second_serve_won_pct"] = ssw
                if bpc > 0:
                    result["break_point_conversion"] = bpc
                if bps > 0:
                    result["break_points_saved_pct"] = bps
                if adf > 0:
                    result["ace_df_ratio"] = adf

        return result

    def _serve_stats(
        self,
        game: dict[str, Any],
        prefix: str,
    ) -> dict[str, float]:
        """Serve-related career/recent averages."""
        first_serve_pct = pd.to_numeric(game.get(f"{prefix}first_serve_pct", 0), errors="coerce") or 0.0
        first_serve_won = pd.to_numeric(game.get(f"{prefix}first_serve_won_pct", 0), errors="coerce") or 0.0
        second_serve_won = pd.to_numeric(game.get(f"{prefix}second_serve_won_pct", 0), errors="coerce") or 0.0
        aces_pm = pd.to_numeric(game.get(f"{prefix}aces_per_match", 0), errors="coerce") or 0.0
        df_pm = pd.to_numeric(game.get(f"{prefix}double_faults_per_match", 0), errors="coerce") or 0.0
        service_games_won = pd.to_numeric(game.get(f"{prefix}service_games_won_pct", 0), errors="coerce") or 0.0

        return {
            "first_serve_pct": first_serve_pct,
            "first_serve_won_pct": first_serve_won,
            "second_serve_won_pct": second_serve_won,
            "aces_per_match": aces_pm,
            "double_faults_per_match": df_pm,
            "service_games_won_pct": service_games_won,
        }

    def _return_stats(
        self,
        game: dict[str, Any],
        prefix: str,
    ) -> dict[str, float]:
        """Return game performance."""
        bp_conv = pd.to_numeric(game.get(f"{prefix}break_point_conversion", 0), errors="coerce") or 0.0
        bp_saved = pd.to_numeric(game.get(f"{prefix}break_points_saved_pct", 0), errors="coerce") or 0.0
        return_won = pd.to_numeric(game.get(f"{prefix}return_points_won_pct", 0), errors="coerce") or 0.0

        return {
            "break_point_conversion": bp_conv,
            "break_points_saved_pct": bp_saved,
            "return_points_won_pct": return_won,
        }

    def _ranking_features(
        self,
        game: dict[str, Any],
        prefix: str,
        standings_df: pd.DataFrame | None = None,
    ) -> dict[str, float]:
        """Current ranking and ranking points.

        Falls back to the standings parquet if the game row itself lacks
        ranking information (which is common for historical data sources
        that store rankings separately).
        """
        ranking = pd.to_numeric(game.get(f"{prefix}ranking", 0), errors="coerce") or 0.0
        points = pd.to_numeric(game.get(f"{prefix}ranking_points", 0), errors="coerce") or 0.0

        # If game row has no ranking, look it up in the standings table
        if ranking == 0.0 and standings_df is not None and not standings_df.empty:
            player_id = str(game.get(f"{prefix}team_id", ""))
            if player_id and "team_id" in standings_df.columns:
                row = standings_df[standings_df["team_id"] == player_id]
                if not row.empty:
                    r = row.iloc[0]
                    ranking = float(pd.to_numeric(r.get("rank", 0), errors="coerce") or 0.0)
                    points = float(pd.to_numeric(r.get("points", 0), errors="coerce") or 0.0)

        return {"ranking": ranking, "ranking_points": points}

    def _fatigue_features(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> dict[str, float]:
        """Matches played recently as fatigue proxy.

        Uses the team history index (O(log n)) rather than full-scan
        boolean masking to avoid slow object-array string comparisons.
        """
        ts = pd.Timestamp(date)

        # Get last 50 games via index — covers any 30-day window
        recent = self._team_games_before(games, player_id, date, limit=50)
        if recent.empty:
            return {"matches_7d": 0.0, "matches_14d": 0.0, "matches_30d": 0.0, "avg_sets_recent": 0.0}

        dates = pd.to_datetime(recent["date"], errors="coerce")
        matches_7d = int((dates >= ts - pd.Timedelta(days=7)).sum())
        matches_14d = int((dates >= ts - pd.Timedelta(days=14)).sum())
        matches_30d = int((dates >= ts - pd.Timedelta(days=30)).sum())

        avg_sets = 0.0
        if "total_sets" in recent.columns:
            avg_sets = float(pd.to_numeric(recent["total_sets"].head(5), errors="coerce").mean()) or 0.0

        return {
            "matches_7d": float(matches_7d),
            "matches_14d": float(matches_14d),
            "matches_30d": float(matches_30d),
            "avg_sets_recent": avg_sets,
        }

    def _win_streak(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
        max_streak: int = 20,
    ) -> float:
        """Current consecutive win streak (capped at max_streak)."""
        recent = self._team_games_before(games, player_id, date, limit=max_streak)
        if recent.empty:
            return 0.0
        is_home = (recent["home_team_id"] == player_id).values
        streak = 0
        for i in range(len(recent)):
            result = str(recent.iloc[i].get("result", ""))
            won = (is_home[i] and result == "home_win") or (not is_home[i] and result == "away_win")
            if won:
                streak += 1
            else:
                break
        return float(streak)

    def _days_since_last_match(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> float:
        """Days since the player's last match (rest days proxy). Returns 14 if no history."""
        recent = self._team_games_before(games, player_id, date, limit=1)
        if recent.empty:
            return 14.0
        try:
            last_date = pd.Timestamp(recent.iloc[0]["date"])
            today = pd.Timestamp(date)
            diff = (today - last_date).days
            return float(max(0, min(diff, 30)))
        except Exception:
            return 14.0

    def _set_win_rate(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> float:
        """Rolling set win rate (vectorized)."""
        recent = self._team_games_before(games, player_id, date, limit=window)
        if recent.empty:
            return 0.0

        is_home = recent["home_team_id"] == player_id
        h_sets = pd.to_numeric(recent.get("home_sets_won", 0), errors="coerce").fillna(0)
        a_sets = pd.to_numeric(recent.get("away_sets_won", 0), errors="coerce").fillna(0)
        sets_won = h_sets.where(is_home, a_sets)
        sets_lost = a_sets.where(is_home, h_sets)
        total = sets_won.sum() + sets_lost.sum()
        return float(sets_won.sum() / total) if total > 0 else 0.0


    def _h2h_surface_win_pct(
        self,
        player_a: str,
        player_b: str,
        surface: str,
        games: pd.DataFrame,
        date: str | None = None,
        n: int = 15,
    ) -> float:
        """H2H win rate for player_a vs player_b on the specific surface."""
        if games.empty:
            return 0.5  # neutral default

        effective_date = date or "2099-01-01"
        a_games = self._team_games_before(games, player_a, effective_date, limit=500)
        if a_games.empty:
            return 0.5

        is_home_a = a_games["home_team_id"] == player_a
        opp = a_games["away_team_id"].where(is_home_a, a_games["home_team_id"])
        h2h = a_games[opp == player_b]

        if "surface" in h2h.columns:
            h2h = h2h[h2h["surface"].str.lower().eq(surface)]

        h2h = h2h.head(n)
        if h2h.empty:
            return 0.5

        wins_a = self._vec_win_flags(h2h, player_a)
        return float(wins_a.mean())

    def _tiebreak_win_pct(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 20,
    ) -> float:
        """Fraction of tiebreak sets (score=7) won in last `window` matches."""
        recent = self._team_games_before(games, player_id, date, limit=window)
        if recent.empty:
            return 0.0

        is_home = recent["home_team_id"] == player_id
        tiebreak_won, tiebreak_total = 0, 0

        for prefix_p, prefix_o in (("home_", "away_"), ("away_", "home_")):
            mask = is_home if prefix_p == "home_" else ~is_home
            subset = recent[mask]
            for q in ("q1", "q2", "q3", "q4", "q5"):
                p_col = f"{prefix_p}{q}"
                o_col = f"{prefix_o}{q}"
                if p_col not in subset.columns or o_col not in subset.columns:
                    continue
                p_scores = pd.to_numeric(subset[p_col], errors="coerce").fillna(-1)
                o_scores = pd.to_numeric(subset[o_col], errors="coerce").fillna(-1)
                # A tiebreak set has score 7 on one side
                tb_mask = (p_scores == 7) | (o_scores == 7)
                tiebreak_total += int(tb_mask.sum())
                tiebreak_won += int(((p_scores == 7) & tb_mask).sum())

        return float(tiebreak_won / tiebreak_total) if tiebreak_total > 0 else 0.0

    def _ranking_quality_form(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
        standings_df: pd.DataFrame | None = None,
        window: int = 15,
    ) -> float:
        """Win rate weighted by opponent ranking quality (rank 1 > rank 100).

        Returns quality-weighted form score in [-1, 1] range.
        """
        recent = self._team_games_before(games, player_id, date, limit=window)
        if recent.empty:
            return 0.0
        wins = self._vec_win_flags(recent, player_id)
        is_home = recent["home_team_id"].astype(str) == str(player_id)
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))

        opp_weights: list[float] = []
        for opp_id in opp_ids:
            # Try to get opponent ranking from standings; fallback to game columns
            opp_rank = 200.0  # default = mediocre
            if standings_df is not None and not standings_df.empty:
                omatch = standings_df[standings_df["team_id"].astype(str) == str(opp_id)]
                if not omatch.empty and "ranking" in omatch.columns:
                    r = pd.to_numeric(omatch["ranking"].iloc[0], errors="coerce")
                    if pd.notna(r) and r > 0:
                        opp_rank = float(r)
            # Convert rank to quality weight: rank 1 = weight 1.0, rank 200 = weight ~0.0
            quality_weight = max(0.0, 1.0 - (opp_rank - 1.0) / 200.0)
            opp_weights.append(quality_weight)

        weights = np.array(opp_weights, dtype=float)
        n = max(len(wins), 1)
        return float(np.dot(wins * 2.0 - 1.0, weights) / n)

    def _prestige_form(
        self,
        player_id: str,
        date: str,
        games: pd.DataFrame,
        prestige: int,
        window: int = 20,
    ) -> dict[str, float]:
        """Win rate at tournaments of similar prestige level (GS=4, Masters=3, etc.)."""
        defaults = {"prestige_win_pct": 0.0, "prestige_matches": 0}
        recent = self._team_games_before(games, player_id, date, limit=100)
        if recent.empty or "tournament_prestige" not in recent.columns:
            return defaults
        prestige_games = recent.loc[
            pd.to_numeric(recent["tournament_prestige"], errors="coerce") == prestige
        ].head(window)
        if prestige_games.empty:
            return defaults
        wins = self._vec_win_flags(prestige_games, player_id)
        return {
            "prestige_win_pct": float(wins.mean()),
            "prestige_matches": len(prestige_games),
        }

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        """Backwards-compatible extract_game_features that loads games/odds locally.
        
        Called by external code. For extract_all, use extract_game_features_cached instead.
        """
        season = game.get("season", 0)
        games_df = self._load_all_games()
        odds_df = self.load_odds(season)
        standings_df = self.load_standings(season)
        return self.extract_game_features_cached(game, games_df, odds_df, standings_df)

    def get_feature_names(self) -> list[str]:
        return [
            # Surface
            "surface_hard", "surface_clay", "surface_grass", "surface_carpet",
            # Match format and tournament prestige
            "is_best_of_5", "tournament_prestige", "is_grand_slam", "is_masters",
            # Form
            "home_form_win_pct", "home_form_ppg", "home_form_opp_ppg",
            "home_form_avg_margin", "home_form_games_played",
            "away_form_win_pct", "away_form_ppg", "away_form_opp_ppg",
            "away_form_avg_margin", "away_form_games_played",
            # Surface form
            "home_surface_win_pct", "home_surface_matches",
            "away_surface_win_pct", "away_surface_matches",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            # Serve
            "home_first_serve_pct", "home_first_serve_won_pct", "home_second_serve_won_pct",
            "home_aces_per_match", "home_double_faults_per_match", "home_service_games_won_pct",
            "away_first_serve_pct", "away_first_serve_won_pct", "away_second_serve_won_pct",
            "away_aces_per_match", "away_double_faults_per_match", "away_service_games_won_pct",
            # Return
            "home_break_point_conversion", "home_break_points_saved_pct", "home_return_points_won_pct",
            "away_break_point_conversion", "away_break_points_saved_pct", "away_return_points_won_pct",
            # Match quality metrics (winners, UE, tiebreaks, net play)
            "home_tiebreaks_won_pm", "home_winners_per_match", "home_unforced_errors_per_match",
            "home_winner_ue_ratio", "home_net_points_won_pm",
            "away_tiebreaks_won_pm", "away_winners_per_match", "away_unforced_errors_per_match",
            "away_winner_ue_ratio", "away_net_points_won_pm",
            # Ranking
            "home_ranking", "home_ranking_points", "away_ranking", "away_ranking_points", "ranking_diff",
            # Fatigue
            "home_matches_7d", "home_matches_14d", "home_matches_30d", "home_avg_sets_recent",
            "away_matches_7d", "away_matches_14d", "away_matches_30d", "away_avg_sets_recent",
            # Set win rate + tiebreak clutch
            "home_set_win_rate", "away_set_win_rate",
            "home_tiebreak_win_pct", "away_tiebreak_win_pct", "tiebreak_win_pct_diff",
            # ELO
            "home_elo", "home_elo_diff", "home_elo_expected_win",
            "away_elo", "away_elo_diff", "away_elo_expected_win",
            # Differentials
            "surface_win_pct_diff", "first_serve_pct_diff", "first_serve_won_pct_diff",
            "second_serve_won_pct_diff", "break_point_conversion_diff", "break_points_saved_pct_diff",
            "aces_per_match_diff", "double_faults_per_match_diff", "set_win_rate_diff",
            "elo_diff", "fatigue_diff",
            "form_win_pct_diff", "form_ppg_diff", "form_margin_diff",
            "ranking_points_diff",
            "h2h_surface_win_pct", "h2h_surface_win_pct_diff",
            "home_prestige_win_pct", "away_prestige_win_pct", "prestige_win_pct_diff",
            # Quality metric differentials
            "tiebreaks_won_pm_diff", "winners_per_match_diff", "unforced_errors_per_match_diff",
            "winner_ue_ratio_diff", "net_points_won_pm_diff",
            # Schedule difficulty and momentum
            "home_avg_opp_ranking", "away_avg_opp_ranking", "schedule_difficulty_diff",
            "home_momentum", "away_momentum", "momentum_diff",
            # Win streak and rest
            "home_win_streak", "away_win_streak", "win_streak_diff",
            "home_days_since_last", "away_days_since_last", "rest_advantage",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
        ]
