# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Golf
# ──────────────────────────────────────────────────────────
#
# Covers PGA/LPGA/etc.  Produces ~60 features per player per
# tournament including recent form, scoring consistency,
# field strength, prestige, rank percentile, relative scoring,
# season efficiency, consistency, and multi-window momentum.
#
# Uses pre-indexed lookups for O(1) player history access
# to handle 5000+ player-tournament combinations efficiently.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)


class GolfExtractor(BaseFeatureExtractor):
    """Feature extractor for golf (PGA, LPGA, etc.)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_games_cache: pd.DataFrame | None = None

    # ── Fast vectorized helpers (pre-indexed) ─────────────

    @staticmethod
    def _fast_history(hist: pd.DataFrame, window: int = 10) -> dict[str, float]:
        """Form features from pre-filtered, pre-sorted player history."""
        if hist.empty:
            return {
                "form_avg_finish": 0.0, "form_finish_std": 0.0,
                "form_top_10_rate": 0.0, "form_top_25_rate": 0.0,
                "form_win_rate": 0.0, "form_cut_made_rate": 0.0,
                "form_avg_score_to_par": 0.0, "form_avg_rounds": 0.0,
                "form_tournaments_played": 0.0,
            }
        recent = hist.head(window)
        n = len(recent)
        pos = recent["_position"].dropna()
        stp = recent["_score_to_par"].dropna()
        rds = recent["_rounds"].dropna()

        top_10 = (pos <= 10).sum() if len(pos) > 0 else 0
        top_25 = (pos <= 25).sum() if len(pos) > 0 else 0
        wins = (pos == 1).sum() if len(pos) > 0 else 0
        cuts = (pos > 0).sum() if len(pos) > 0 else 0

        return {
            "form_avg_finish": float(pos.mean()) if len(pos) > 0 else 0.0,
            "form_finish_std": float(pos.std()) if len(pos) > 1 else 0.0,
            "form_top_10_rate": float(top_10 / n),
            "form_top_25_rate": float(top_25 / n),
            "form_win_rate": float(wins / n),
            "form_cut_made_rate": float(cuts / n),
            "form_avg_score_to_par": float(stp.mean()) if len(stp) > 0 else 0.0,
            "form_avg_rounds": float(rds.mean()) if len(rds) > 0 else 0.0,
            "form_tournaments_played": float(n),
        }

    @staticmethod
    def _fast_history_long(hist: pd.DataFrame, window: int = 20) -> dict[str, float]:
        """Long-window (20 event) form features for trend comparison."""
        if hist.empty:
            return {
                "long_avg_finish": 0.0, "long_top_10_rate": 0.0,
                "long_top_25_rate": 0.0, "long_avg_score_to_par": 0.0,
            }
        recent = hist.head(window)
        n = len(recent)
        pos = recent["_position"].dropna()
        stp = recent["_score_to_par"].dropna()
        return {
            "long_avg_finish": float(pos.mean()) if len(pos) > 0 else 0.0,
            "long_top_10_rate": float((pos <= 10).sum() / n) if len(pos) > 0 else 0.0,
            "long_top_25_rate": float((pos <= 25).sum() / n) if len(pos) > 0 else 0.0,
            "long_avg_score_to_par": float(stp.mean()) if len(stp) > 0 else 0.0,
        }

    @staticmethod
    def _fast_scoring(hist: pd.DataFrame, window: int = 10) -> dict[str, float]:
        """Scoring consistency from pre-filtered history."""
        empty = {"scoring_avg": 0.0, "scoring_consistency": 0.0,
                 "best_score_to_par": 0.0, "worst_score_to_par": 0.0}
        if hist.empty:
            return empty
        scores = hist.head(window)["_score_to_par"].dropna()
        if len(scores) == 0:
            return empty
        return {
            "scoring_avg": float(scores.mean()),
            "scoring_consistency": float(scores.std()) if len(scores) > 1 else 0.0,
            "best_score_to_par": float(scores.min()),
            "worst_score_to_par": float(scores.max()),
        }

    @staticmethod
    def _fast_rest(hist: pd.DataFrame, game_date: str) -> dict[str, float]:
        """Rest days and recent activity from pre-filtered history."""
        if hist.empty:
            return {"rest_days": 14.0, "tournaments_last_30d": 0.0}
        last_date = pd.to_datetime(hist.iloc[0]["_date"], errors="coerce")
        current = pd.to_datetime(game_date, errors="coerce")
        if pd.isna(last_date) or pd.isna(current):
            return {"rest_days": 14.0, "tournaments_last_30d": 0.0}
        rest = (current - last_date).days
        cutoff = current - pd.Timedelta(days=30)
        dates = pd.to_datetime(hist["_date"], errors="coerce")
        t30 = (dates >= cutoff).sum()
        return {"rest_days": float(max(rest, 0)), "tournaments_last_30d": float(t30)}

    @staticmethod
    def _fast_momentum(hist: pd.DataFrame, window: int = 5) -> dict[str, float]:
        """Trend in recent finish positions and score-to-par trajectory."""
        if hist.empty or len(hist) < 2:
            return {"momentum_trend": 0.0, "improving": 0.0, "score_to_par_trend": 0.0,
                    "consecutive_cuts": 0.0, "top5_last3": 0.0}
        recent = hist.head(window)
        pos = recent["_position"].dropna()
        if len(pos) < 2:
            return {"momentum_trend": 0.0, "improving": 0.0, "score_to_par_trend": 0.0,
                    "consecutive_cuts": 0.0, "top5_last3": 0.0}
        # Oldest→newest for trend calculation
        vals = pos.values[::-1]
        x = np.arange(len(vals), dtype=float)
        slope = np.polyfit(x, vals, 1)[0]

        # Score-to-par trend: negative slope = improving (scoring lower)
        stp = recent["_score_to_par"].dropna()
        stp_trend = 0.0
        if len(stp) >= 2:
            stp_vals = stp.values[::-1]
            sx = np.arange(len(stp_vals), dtype=float)
            stp_trend = float(-np.polyfit(sx, stp_vals, 1)[0])  # positive = improving

        # Consecutive cuts made (cuts made = position > 0 and rounds >= 4)
        all_pos = hist["_position"].values
        consec = 0
        for p in all_pos:
            if pd.notna(p) and p > 0:
                consec += 1
            else:
                break

        # Top-5 finishes in last 3 events
        last3 = hist.head(3)["_position"].dropna()
        top5_last3 = float((last3 <= 5).sum()) if len(last3) > 0 else 0.0

        return {
            "momentum_trend": float(-slope), "improving": 1.0 if slope < 0 else 0.0,
            "score_to_par_trend": stp_trend,
            "consecutive_cuts": float(consec),
            "top5_last3": top5_last3,
        }

    # ── extract_all override ──────────────────────────────

    @staticmethod
    def _parse_standings_row(row: dict) -> dict[str, float]:
        """Parse streak/home_record/away_record from computed standings."""
        import re
        result: dict[str, float] = {
            "season_avg_finish": 0.0, "top5_count_season": 0.0, "top25_count_season": 0.0,
            "fedex_cup_points": 0.0, "points_per_event": 0.0,
        }
        streak = str(row.get("streak", "") or "")
        m = re.search(r"Avg\s+([\d.]+)", streak)
        if m:
            result["season_avg_finish"] = float(m.group(1))

        home_rec = str(row.get("home_record", "") or "")
        m = re.search(r"(\d+)\s+top-?5", home_rec)
        if m:
            result["top5_count_season"] = float(m.group(1))

        away_rec = str(row.get("away_record", "") or "")
        m = re.search(r"(\d+)\s+top-?25", away_rec)
        if m:
            result["top25_count_season"] = float(m.group(1))

        pts = pd.to_numeric(row.get("points", 0), errors="coerce") or 0
        gp = max(float(pd.to_numeric(row.get("games_played", 1), errors="coerce") or 1), 1)
        result["fedex_cup_points"] = float(pts)
        result["points_per_event"] = float(pts) / gp
        return result

    @staticmethod
    def _tournament_prestige(broadcast: str) -> dict[str, float]:
        """Score tournament prestige from broadcast network (0–3)."""
        bc = str(broadcast or "").upper()
        # Majors / WGC / playoff: NBC, CBS, ESPN are premium
        if "NBC" in bc or "CBS" in bc:
            prestige = 3.0
        elif "ESPN2" in bc or "ABC" in bc:
            prestige = 2.0
        elif "ESPN+" in bc:
            prestige = 1.5
        else:
            prestige = 1.0  # Golf Channel / other
        return {"tournament_prestige": prestige}

    def extract_all(
        self, season: int, *, existing_game_ids: set[str] | None = None,
    ) -> pd.DataFrame:
        """Expand each tournament into per-player rows using player_stats."""
        games = self.load_games(season)
        if games.empty:
            logger.warning("No games for %s season %d", self.sport, season)
            return pd.DataFrame()

        # Load player stats for this + prior seasons (history window)
        all_stats = []
        for s in range(max(2020, season - 3), season + 1):
            try:
                ps = self.load_player_stats(s)
                if not ps.empty:
                    all_stats.append(ps)
            except Exception:
                pass

        stats = pd.concat(all_stats, ignore_index=True) if all_stats else pd.DataFrame()
        if stats.empty:
            logger.warning("No player_stats for %s — using tournament-level features only", self.sport)
            status_col = "status" if "status" in games.columns else None
            if status_col:
                games = games[games[status_col].str.lower().isin(
                    {"final", "closed", "complete", "finished"}
                )]
            id_col = next((c for c in ("id", "game_id") if c in games.columns), None)
            if id_col is None or games.empty:
                return pd.DataFrame()
            features: list[dict[str, Any]] = []
            date_col = next((c for c in ("date", "start_date") if c in games.columns), None)
            sorted_games = games.sort_values(date_col) if date_col else games
            for idx, (_, game) in enumerate(sorted_games.iterrows()):
                gd = game.to_dict()
                gid = str(gd.get(id_col, ""))
                if existing_game_ids and gid in existing_game_ids:
                    continue
                f: dict[str, Any] = {
                    "game_id": gid,
                    "season": season,
                    "date": str(gd.get(date_col, "")) if date_col else "",
                    "sport": self.sport,
                    "field_size": float(gd.get("field_size", 0) or 0),
                    "purse": float(gd.get("purse", 0) or 0),
                    "round_number": float(idx + 1),
                    "total_rounds": float(gd.get("total_rounds", gd.get("rounds", 4)) or 4),
                    "home_score": float(gd.get("home_score", gd.get("winning_score", 0)) or 0),
                    "away_score": 0.0,
                }
                features.append(f)
            logger.info("%s fallback: %d tournament-level rows", self.sport, len(features))
            return pd.DataFrame(features) if features else pd.DataFrame()

        # Pre-compute indexed columns once
        stats["_pid"] = stats["player_id"].astype(str)
        stats["_gid"] = stats["game_id"].astype(str)
        stats["_date"] = stats["date"].astype(str)
        stats["_position"] = pd.to_numeric(stats.get("position", pd.Series(dtype=float)), errors="coerce")
        stats["_score_to_par"] = pd.to_numeric(stats.get("score_to_par", pd.Series(dtype=float)), errors="coerce")
        stats["_rounds"] = pd.to_numeric(stats.get("rounds", pd.Series(dtype=float)), errors="coerce")

        # Build per-player index (sorted desc by date)
        player_index: dict[str, pd.DataFrame] = {}
        for pid, grp in stats.groupby("_pid"):
            player_index[str(pid)] = grp.sort_values("_date", ascending=False).reset_index(drop=True)

        # Current season filter
        if "season" in stats.columns:
            current_stats = stats[stats["season"].astype(str) == str(season)]
        else:
            current_stats = stats

        # Pre-compute field strength per tournament (with rank stats)
        field_cache: dict[str, dict[str, float]] = {}
        for gid, grp in current_stats.groupby("_gid"):
            pos = grp["_position"].dropna()
            stp = grp["_score_to_par"].dropna()
            field_cache[str(gid)] = {
                "field_size": float(len(grp)),
                "field_avg_finish": float(pos.mean()) if len(pos) > 0 else 0.0,
                "field_avg_stp": float(stp.mean()) if len(stp) > 0 else 0.0,
                "field_stp_std": float(stp.std()) if len(stp) > 1 else 0.0,
            }

        # Load world rankings/standings for the season
        standings_lookup: dict[str, dict[str, float]] = {}
        try:
            stnd = self.load_standings(season)
            if not stnd.empty and "team_id" in stnd.columns:
                stnd["_pid"] = stnd["team_id"].astype(str)
                for _, row in stnd.iterrows():
                    pid = str(row.get("_pid", ""))
                    base = {
                        "world_rank": float(pd.to_numeric(row.get("rank", 500), errors="coerce") or 500),
                        "season_wins": float(pd.to_numeric(row.get("wins", 0), errors="coerce") or 0),
                        "season_points": float(pd.to_numeric(row.get("points", 0), errors="coerce") or 0),
                        "season_games_played": float(pd.to_numeric(row.get("games_played", 0), errors="coerce") or 0),
                    }
                    base.update(self._parse_standings_row(row.to_dict()))
                    standings_lookup[pid] = base
        except Exception:
            pass

        # Build per-tournament world rank list for field percentile calculation
        # Uses standings_lookup for current season only
        tournament_ranks: dict[str, list[float]] = {}
        for gid, grp in current_stats.groupby("_gid"):
            ranks = []
            for pid in grp["_pid"].values:
                r = standings_lookup.get(str(pid), {}).get("world_rank", 500.0)
                ranks.append(r)
            tournament_ranks[str(gid)] = ranks

        # Build broadcast lookup from games DataFrame
        broadcast_lookup: dict[str, str] = {}
        if "broadcast" in games.columns:
            for _, g in games.iterrows():
                broadcast_lookup[str(g.get("id", ""))] = str(g.get("broadcast", "") or "")

        features: list[dict[str, Any]] = []
        success, failed = 0, 0

        for _, game in games.iterrows():
            game_id = str(game.get("id", ""))
            game_date = str(game.get("date", ""))
            game_status = str(game.get("status", "")).lower()

            if game_status not in ("final", "complete", "closed", "finished"):
                continue

            # Incremental: skip already-extracted tournaments
            if existing_game_ids and game_id in existing_game_ids:
                continue

            tournament = current_stats[current_stats["_gid"] == game_id]
            if tournament.empty:
                continue

            field_feat = field_cache.get(game_id, {
                "field_size": 0.0, "field_avg_finish": 0.0,
                "field_avg_stp": 0.0, "field_stp_std": 0.0,
            })
            broadcast = broadcast_lookup.get(game_id, "")
            prestige_feat = self._tournament_prestige(broadcast)

            # Field rank stats for this tournament
            field_ranks = tournament_ranks.get(game_id, [])
            field_rank_median = float(np.median(field_ranks)) if field_ranks else 500.0
            field_rank_mean = float(np.mean(field_ranks)) if field_ranks else 500.0

            for _, player in tournament.iterrows():
                try:
                    pid = str(player["_pid"])
                    position = player["_position"]
                    stp = player["_score_to_par"]

                    if pd.isna(position) or position <= 0:
                        continue

                    # O(1) player history lookup + date filter
                    phist = player_index.get(pid, pd.DataFrame())
                    hist_before = phist[phist["_date"] < game_date] if not phist.empty else pd.DataFrame()

                    feat: dict[str, Any] = {
                        "game_id": game_id,
                        "date": game_date,
                        "player_id": pid,
                        "player_name": str(player.get("player_name", "")),
                        "position": float(position),
                        "score_to_par": float(stp) if not pd.isna(stp) else 0.0,
                        "won": 1.0 if position == 1 else 0.0,
                        "top_10": 1.0 if position <= 10 else 0.0,
                        "top_5": 1.0 if position <= 5 else 0.0,
                        "top_25": 1.0 if position <= 25 else 0.0,
                    }

                    feat.update(self._fast_history(hist_before, 10))
                    feat.update(self._fast_history_long(hist_before, 20))
                    feat.update(self._fast_scoring(hist_before, 10))
                    feat.update(field_feat)
                    feat.update(self._fast_rest(hist_before, game_date))
                    feat.update(self._fast_momentum(hist_before, 5))
                    feat.update(prestige_feat)

                    # World ranking and season standing features
                    stnd_row = standings_lookup.get(pid, {})
                    world_rank = stnd_row.get("world_rank", 500.0)
                    feat["world_rank"] = world_rank
                    feat["world_rank_inv"] = 1.0 / max(world_rank, 1.0)
                    feat["season_wins"] = stnd_row.get("season_wins", 0.0)
                    feat["season_points"] = stnd_row.get("season_points", 0.0)
                    feat["season_games_played"] = stnd_row.get("season_games_played", 0.0)

                    # Enhanced standings parse features
                    feat["season_avg_finish"] = stnd_row.get("season_avg_finish", 0.0)
                    feat["top5_count_season"] = stnd_row.get("top5_count_season", 0.0)
                    feat["top25_count_season"] = stnd_row.get("top25_count_season", 0.0)
                    feat["fedex_cup_points"] = stnd_row.get("fedex_cup_points", 0.0)
                    feat["points_per_event"] = stnd_row.get("points_per_event", 0.0)

                    # Field rank percentile (lower rank = stronger field)
                    field_sz = max(field_feat.get("field_size", 1.0), 1.0)
                    # Player's relative standing in this field
                    rank_vs_field = (world_rank - field_rank_mean) / max(field_rank_mean, 1.0)
                    feat["rank_vs_field_mean"] = float(rank_vs_field)
                    feat["field_rank_median"] = float(field_rank_median)
                    feat["field_rank_mean"] = float(field_rank_mean)
                    feat["field_strength_rank"] = float(
                        sum(1 for r in field_ranks if r < world_rank) / max(len(field_ranks), 1)
                    )  # fraction of field ranked better → 0 = world #1 in field

                    # Relative scoring vs field average
                    form_stp = feat.get("form_avg_score_to_par", 0.0)
                    field_avg_stp = field_feat.get("field_avg_stp", 0.0)
                    feat["rel_scoring_vs_field"] = form_stp - field_avg_stp  # negative = better than field

                    # Form short-vs-long comparison (trajectory quality)
                    long_avg = feat.get("long_avg_finish", feat.get("form_avg_finish", 50.0))
                    short_avg = feat.get("form_avg_finish", 50.0)
                    feat["form_short_vs_long"] = long_avg - short_avg  # positive = improving recently

                    # Scoring consistency coefficient of variation
                    s_avg = abs(feat.get("scoring_avg", 0.0)) + 1e-9
                    s_std = feat.get("scoring_consistency", 0.0)
                    feat["scoring_cv"] = float(s_std / s_avg)

                    # Finish position normalized by field size (percentile 0=worst, 1=best)
                    feat["finish_percentile"] = float(1.0 - (position - 1) / max(field_sz - 1, 1))

                    # Top-5 rate season efficiency
                    gp = max(stnd_row.get("season_games_played", 1.0), 1.0)
                    feat["top5_rate_season"] = stnd_row.get("top5_count_season", 0.0) / gp
                    feat["top25_rate_season"] = stnd_row.get("top25_count_season", 0.0) / gp

                    features.append(feat)
                    success += 1
                except Exception as exc:
                    failed += 1
                    if failed <= 3:
                        logger.warning("Golf feature error: %s", exc)

        logger.info(
            "%s season %d: %d player-tournament features (%d failed)",
            self.sport, season, success, failed,
        )

        if not features:
            return pd.DataFrame()

        return pd.DataFrame(features)

    # ── Required abstract methods ─────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        """Not used — extract_all() is overridden."""
        return {}

    def get_feature_names(self) -> list[str]:
        return [
            # Labels and metadata
            "position", "score_to_par", "won", "top_10", "top_5", "top_25",
            # Short-window form (10 events)
            "form_avg_finish", "form_finish_std", "form_top_10_rate",
            "form_top_25_rate", "form_win_rate", "form_cut_made_rate",
            "form_avg_score_to_par", "form_avg_rounds", "form_tournaments_played",
            # Long-window form (20 events)
            "long_avg_finish", "long_top_10_rate", "long_top_25_rate", "long_avg_score_to_par",
            # Scoring
            "scoring_avg", "scoring_consistency", "best_score_to_par", "worst_score_to_par",
            "scoring_cv",
            # Field strength
            "field_size", "field_avg_finish", "field_avg_stp", "field_stp_std",
            "field_rank_median", "field_rank_mean", "field_strength_rank",
            # Rest / fatigue
            "rest_days", "tournaments_last_30d",
            # Momentum
            "momentum_trend", "improving", "score_to_par_trend",
            "consecutive_cuts", "top5_last3",
            # Tournament prestige
            "tournament_prestige",
            # World ranking / season standing
            "world_rank", "world_rank_inv", "season_wins",
            "season_points", "season_games_played",
            # Enhanced standings
            "season_avg_finish", "top5_count_season", "top25_count_season",
            "fedex_cup_points", "points_per_event",
            "top5_rate_season", "top25_rate_season",
            # Relative features
            "rank_vs_field_mean", "rel_scoring_vs_field",
            "form_short_vs_long", "finish_percentile",
        ]
