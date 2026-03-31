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


class TennisExtractor(BaseFeatureExtractor):
    """Feature extractor for tennis (ATP, WTA)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport

    def extract_all(self, season: int) -> pd.DataFrame:
        """Override extract_all to pre-load games/odds once and reuse across all games.
        
        This prevents 9,605× redundant file loads that were causing hangs.
        """
        games = self.load_games(season)
        odds = self.load_odds(season)
        
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

        features: list[dict[str, Any]] = []
        success, failed = 0, 0

        for _, game in games.iterrows():
            try:
                f = self.extract_game_features_cached(game.to_dict(), games, odds)
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
    ) -> dict[str, Any]:
        """Extract features using pre-loaded games and odds DataFrames.
        
        This is the cached version called from extract_all to avoid redundant loads.
        """
        h_id = str(game.get("home_team_id", game.get("player_a_id", "")))
        a_id = str(game.get("away_team_id", game.get("player_b_id", "")))
        date = str(game.get("date", ""))
        game_id = str(game.get("id", ""))
        surface = str(game.get("surface", "hard")).lower()

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

        # Overall form
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

        # Serve — use rolling player stats (player_stats files) + game history
        h_serve = self._player_serve_stats(h_id, date, games_df=games_df)
        features.update({f"home_{k}": v for k, v in h_serve.items()})
        a_serve = self._player_serve_stats(a_id, date, games_df=games_df)
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
        h_rank = self._ranking_features(game, "home_")
        features.update({f"home_{k}": v for k, v in h_rank.items()})
        a_rank = self._ranking_features(game, "away_")
        features.update({f"away_{k}": v for k, v in a_rank.items()})
        features["ranking_diff"] = features["home_ranking"] - features["away_ranking"]

        # Fatigue
        h_fat = self._fatigue_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_fat.items()})
        a_fat = self._fatigue_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_fat.items()})

        # Set win rate
        features["home_set_win_rate"] = self._set_win_rate(h_id, date, games_df)
        features["away_set_win_rate"] = self._set_win_rate(a_id, date, games_df)

        # ELO ratings (computed from match history)
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Odds
        odds = self._odds_features(game_id, odds_df)
        features.update(odds)

        return features

    # ── Helpers ────────────────────────────────────────────

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
        frames = []
        for p in sorted(sport_dir.glob("player_stats_*.parquet")):
            try:
                frames.append(pd.read_parquet(p))
            except Exception:
                pass
        if not frames:
            self._all_pstats_cache = pd.DataFrame()
            return self._all_pstats_cache
        combined = pd.concat(frames, ignore_index=True)

        # date is typically null — enrich from games files via game_id → id join
        if "game_id" in combined.columns:
            game_frames = []
            for p in sorted(sport_dir.glob("games_*.parquet")):
                try:
                    gdf = pd.read_parquet(p, columns=["id", "date"])
                    game_frames.append(gdf)
                except Exception:
                    pass
            if game_frames:
                games_lookup = pd.concat(game_frames, ignore_index=True).drop_duplicates("id")
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
        self._all_pstats_cache = combined
        return combined

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
    ) -> dict[str, float]:
        """Current ranking and ranking points."""
        ranking = pd.to_numeric(game.get(f"{prefix}ranking", 0), errors="coerce") or 0.0
        points = pd.to_numeric(game.get(f"{prefix}ranking_points", 0), errors="coerce") or 0.0
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

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self.load_games(season)
        odds_df = self.load_odds(season)

        h_id = str(game.get("home_team_id", game.get("player_a_id", "")))
        a_id = str(game.get("away_team_id", game.get("player_b_id", "")))
        date = str(game.get("date", ""))
        game_id = str(game.get("id", ""))
        surface = str(game.get("surface", "hard")).lower()

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

        # Overall form
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

        # Serve — use rolling player stats (player_stats files) + game history
        h_serve = self._player_serve_stats(h_id, date, games_df=games_df)
        features.update({f"home_{k}": v for k, v in h_serve.items()})
        a_serve = self._player_serve_stats(a_id, date, games_df=games_df)
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
        h_rank = self._ranking_features(game, "home_")
        features.update({f"home_{k}": v for k, v in h_rank.items()})
        a_rank = self._ranking_features(game, "away_")
        features.update({f"away_{k}": v for k, v in a_rank.items()})
        features["ranking_diff"] = features["home_ranking"] - features["away_ranking"]

        # Fatigue
        h_fat = self._fatigue_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_fat.items()})
        a_fat = self._fatigue_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_fat.items()})

        # Set win rate
        features["home_set_win_rate"] = self._set_win_rate(h_id, date, games_df)
        features["away_set_win_rate"] = self._set_win_rate(a_id, date, games_df)

        # ELO ratings (computed from match history)
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Odds
        odds = self._odds_features(game_id, odds_df)
        features.update(odds)

        return features

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        """Backwards-compatible extract_game_features that loads games/odds locally.
        
        Called by external code. For extract_all, use extract_game_features_cached instead.
        """
        season = game.get("season", 0)
        games_df = self.load_games(season)
        odds_df = self.load_odds(season)
        return self.extract_game_features_cached(game, games_df, odds_df)

    def get_feature_names(self) -> list[str]:
        return [
            # Surface
            "surface_hard", "surface_clay", "surface_grass", "surface_carpet",
            # Match format
            "is_best_of_5",
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
            # Ranking
            "home_ranking", "home_ranking_points", "away_ranking", "away_ranking_points", "ranking_diff",
            # Fatigue
            "home_matches_7d", "home_matches_14d", "home_matches_30d", "home_avg_sets_recent",
            "away_matches_7d", "away_matches_14d", "away_matches_30d", "away_avg_sets_recent",
            # Set win rate
            "home_set_win_rate", "away_set_win_rate",
            # ELO
            "home_elo", "home_elo_diff", "home_elo_expected_win",
            "away_elo", "away_elo_diff", "away_elo_expected_win",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
        ]
