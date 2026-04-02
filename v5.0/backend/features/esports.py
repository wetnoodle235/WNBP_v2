# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Esports
# ──────────────────────────────────────────────────────────
#
# Covers LoL, CS2, Dota 2, Valorant.  Produces ~30 features
# per match including win rates, game duration trends,
# K/D/A averages, objective control, economy, vision, and
# draft / side analysis.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)


class EsportsExtractor(BaseFeatureExtractor):
    """Feature extractor for esports (LoL, CS2, Dota 2, Valorant)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_games_cache: pd.DataFrame | None = None

    # ── Helpers ────────────────────────────────────────────

    def _match_stats(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """K/D/A averages and game duration trends."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "kills_pg": 0.0, "deaths_pg": 0.0, "assists_pg": 0.0,
                "kda": 0.0, "avg_duration_min": 0.0, "duration_std": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        kills, deaths, assists, durations = [], [], [], []
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            kills.append(pd.to_numeric(row.get(f"{p}kills", 0), errors="coerce") or 0.0)
            deaths.append(pd.to_numeric(row.get(f"{p}deaths", 0), errors="coerce") or 0.0)
            assists.append(pd.to_numeric(row.get(f"{p}assists", 0), errors="coerce") or 0.0)
            dur = pd.to_numeric(row.get("duration_minutes", row.get("duration", 0)), errors="coerce") or 0.0
            durations.append(dur)

        avg_k = float(np.mean(kills))
        avg_d = float(np.mean(deaths))
        avg_a = float(np.mean(assists))

        return {
            "kills_pg": avg_k,
            "deaths_pg": avg_d,
            "assists_pg": avg_a,
            "kda": float((avg_k + avg_a) / avg_d) if avg_d > 0 else 0.0,
            "avg_duration_min": float(np.mean(durations)),
            "duration_std": float(np.std(durations)) if len(durations) > 1 else 0.0,
        }

    def _objective_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Objective control rate (towers, dragons, barons, etc.)."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "objectives_pg": 0.0, "first_objective_rate": 0.0,
                "towers_pg": 0.0, "dragons_pg": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        objectives, first_obj, towers, dragons = 0.0, 0.0, 0.0, 0.0
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            objectives += pd.to_numeric(row.get(f"{p}objectives", 0), errors="coerce") or 0.0
            first_obj += 1.0 if str(row.get(f"{p}first_objective", "")).lower() in ("true", "1", "yes") else 0.0
            towers += pd.to_numeric(row.get(f"{p}towers", 0), errors="coerce") or 0.0
            dragons += pd.to_numeric(row.get(f"{p}dragons", row.get(f"{p}roshan", 0)), errors="coerce") or 0.0

        n = len(recent)
        return {
            "objectives_pg": float(objectives / n),
            "first_objective_rate": float(first_obj / n),
            "towers_pg": float(towers / n),
            "dragons_pg": float(dragons / n),
        }

    def _economy_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Gold/CS per minute, economy rating."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"gold_per_min": 0.0, "cs_per_min": 0.0, "gold_diff_at_15": 0.0}

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        gpm, cspm, gd15 = [], [], []
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            gpm.append(pd.to_numeric(row.get(f"{p}gold_per_min", 0), errors="coerce") or 0.0)
            cspm.append(pd.to_numeric(row.get(f"{p}cs_per_min", 0), errors="coerce") or 0.0)
            gd15.append(pd.to_numeric(row.get(f"{p}gold_diff_at_15", 0), errors="coerce") or 0.0)

        return {
            "gold_per_min": float(np.mean(gpm)),
            "cs_per_min": float(np.mean(cspm)),
            "gold_diff_at_15": float(np.mean(gd15)),
        }

    def _vision_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Vision/ward control metrics."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"wards_placed_pg": 0.0, "wards_destroyed_pg": 0.0, "vision_score_pg": 0.0}

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        placed, destroyed, vision = 0.0, 0.0, 0.0
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            placed += pd.to_numeric(row.get(f"{p}wards_placed", 0), errors="coerce") or 0.0
            destroyed += pd.to_numeric(row.get(f"{p}wards_destroyed", 0), errors="coerce") or 0.0
            vision += pd.to_numeric(row.get(f"{p}vision_score", 0), errors="coerce") or 0.0

        n = len(recent)
        return {
            "wards_placed_pg": float(placed / n),
            "wards_destroyed_pg": float(destroyed / n),
            "vision_score_pg": float(vision / n),
        }

    def _side_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        is_blue_side: bool,
        window: int = 20,
    ) -> dict[str, float]:
        """Win rate on blue/red (or CT/T) side."""
        if games.empty:
            return {"side_win_pct": 0.0, "side_matches": 0}

        ts = pd.Timestamp(date)
        status_mask = games.get("status", pd.Series("final", index=games.index)).str.lower().isin(
            ["final", "closed", "complete", "finished"]
        )
        if is_blue_side:
            side_mask = games.get("home_team_id") == team_id
        else:
            side_mask = games.get("away_team_id") == team_id

        side_games = games.loc[side_mask & status_mask & (games["date"] < ts)].sort_values(
            "date", ascending=False
        ).head(window)

        if side_games.empty:
            return {"side_win_pct": 0.0, "side_matches": 0}

        wins = self._vec_win_flags(side_games, team_id)
        return {
            "side_win_pct": float(wins.mean()),
            "side_matches": len(side_games),
        }

    def _get_ps_team_index(self, season: int | str) -> dict[str, pd.DataFrame]:
        """Build and cache per-team player stats index for O(1) team lookups.

        When all dates are null (e.g. Dota2 2026), falls back to game_id-based
        temporal ordering — Dota2 match IDs are sequential so a higher game_id
        means a later match.  The index stores rows sorted by _game_id_int so
        the caller can filter by ``game_id < current_game_id``.
        """
        if not hasattr(self, "_ps_team_index_cache"):
            self._ps_team_index_cache: dict[str, dict[str, pd.DataFrame]] = {}
        if not hasattr(self, "_ps_gameid_based"):
            self._ps_gameid_based: set[str] = set()
        key = str(season)
        if key not in self._ps_team_index_cache:
            ps = self.load_player_stats(season)
            if ps.empty or "team_id" not in ps.columns:
                self._ps_team_index_cache[key] = {}
            else:
                ps = ps.copy()
                ps["_team_id_str"] = ps["team_id"].astype(str)
                # Try date-based sort first
                dates_valid = False
                if "date" in ps.columns:
                    ps["_date_dt"] = pd.to_datetime(ps["date"], errors="coerce")
                    dates_valid = ps["_date_dt"].notna().any()
                if dates_valid:
                    ps.sort_values("_date_dt", inplace=True, ignore_index=True)
                elif "game_id" in ps.columns:
                    # Fall back to game_id sequential ordering (Dota2 long IDs)
                    ps["_game_id_int"] = pd.to_numeric(ps["game_id"], errors="coerce").fillna(0).astype("int64")
                    ps.sort_values("_game_id_int", inplace=True, ignore_index=True)
                    self._ps_gameid_based.add(key)
                self._ps_team_index_cache[key] = {
                    tid: grp for tid, grp in ps.groupby("_team_id_str")
                }
        return self._ps_team_index_cache[key]

    def _player_aggregate_features(
        self,
        team_id: str,
        date: str,
        player_stats: pd.DataFrame,
        window: int = 10,
        season: int | str | None = None,
        current_game_id: str | None = None,
    ) -> dict[str, float]:
        """Rolling aggregate stats from individual player performances for a team.

        Computes per-player recent form (kda, gpm, damage/rating) then
        aggregates across the team roster — captures individual skill form
        that team-level stats miss (e.g. a star player on hot streak).

        When all player-stats dates are null (e.g. Dota2 2025/2026), falls back
        to game_id-based temporal ordering: Dota2 match IDs are sequential
        integers so filtering ``game_id < current_game_id`` gives prior matches.
        """
        defaults = {
            "player_avg_kda": 0.0, "player_avg_gpm": 0.0,
            "player_avg_rating": 0.0, "player_form_std": 0.0,
            "player_avg_damage": 0.0, "player_star_kda": 0.0,
        }
        if player_stats is None or player_stats.empty:
            return defaults
        if "team_id" not in player_stats.columns:
            return defaults

        ts = pd.Timestamp(date)
        try:
            if season is not None:
                # Fast O(1) lookup via per-team index
                team_index = self._get_ps_team_index(season)
                team_df = team_index.get(str(team_id))
                if team_df is None or team_df.empty:
                    return defaults
                key = str(season)
                if hasattr(self, "_ps_gameid_based") and key in self._ps_gameid_based:
                    # Date-null fallback: use sequential game_id ordering
                    if current_game_id is None:
                        return defaults
                    cur_gid = pd.to_numeric(current_game_id, errors="coerce")
                    if pd.isna(cur_gid):
                        return defaults
                    gid_col = team_df.get("_game_id_int", pd.to_numeric(team_df.get("game_id", pd.Series()), errors="coerce"))
                    team_ps = team_df.loc[gid_col < int(cur_gid)].copy()
                else:
                    date_col = team_df.get("_date_dt", pd.to_datetime(team_df["date"], errors="coerce"))
                    team_ps = team_df.loc[date_col < ts].copy()
            else:
                team_ps = player_stats[
                    (player_stats["team_id"].astype(str) == str(team_id)) &
                    (pd.to_datetime(player_stats["date"], errors="coerce") < ts)
                ].copy()
        except Exception:
            return defaults

        if team_ps.empty:
            return defaults

        sort_col = "_game_id_int" if "_game_id_int" in team_ps.columns else (
            "_date_dt" if "_date_dt" in team_ps.columns else "date"
        )
        team_ps = team_ps.sort_values(sort_col, ascending=False)

        player_kdas, player_gpms, player_ratings, player_damages = [], [], [], []
        for pid, grp in team_ps.groupby("player_id"):
            recent = grp.head(window)
            kda = pd.to_numeric(recent.get("kda", pd.Series()), errors="coerce").mean()
            gpm = pd.to_numeric(recent.get("gold_per_min", pd.Series()), errors="coerce").mean()
            rating = pd.to_numeric(recent.get("rating", pd.Series()), errors="coerce").mean()
            damage = pd.to_numeric(recent.get("damage", pd.Series()), errors="coerce").mean()
            if not np.isnan(kda):
                player_kdas.append(kda)
            if not np.isnan(gpm):
                player_gpms.append(gpm)
            if not np.isnan(rating):
                player_ratings.append(rating)
            if not np.isnan(damage):
                player_damages.append(damage)

        if not player_kdas:
            return defaults

        return {
            "player_avg_kda": float(np.mean(player_kdas)),
            "player_avg_gpm": float(np.mean(player_gpms)) if player_gpms else 0.0,
            "player_avg_rating": float(np.mean(player_ratings)) if player_ratings else 0.0,
            "player_form_std": float(np.std(player_kdas)) if len(player_kdas) > 1 else 0.0,
            "player_avg_damage": float(np.mean(player_damages)) if player_damages else 0.0,
            "player_star_kda": float(np.max(player_kdas)) if player_kdas else 0.0,
        }

    # ── Hero / Draft Features (Dota2 only) ──────────────

    def _build_hero_winrate_index(self, games: pd.DataFrame) -> None:
        """Pre-build per-hero win/total counts sorted by date for O(1) lookup.
        
        Stores ``_hero_index``: dict mapping hero_id → list of (date, won: bool).
        """
        if hasattr(self, "_hero_index"):
            return  # already built
        self._hero_index: dict[str, list[tuple[str, bool]]] = {}
        if "home_heroes" not in games.columns:
            return
        df = games.dropna(subset=["home_heroes"]).sort_values("date")
        for _, row in df.iterrows():
            date = str(row["date"])
            h_heroes = str(row.get("home_heroes") or "")
            a_heroes = str(row.get("away_heroes") or "")
            h_score = pd.to_numeric(row.get("home_score", 0), errors="coerce") or 0
            a_score = pd.to_numeric(row.get("away_score", 0), errors="coerce") or 0
            h_won = h_score > a_score
            for hid in h_heroes.split(","):
                hid = hid.strip()
                if hid:
                    self._hero_index.setdefault(hid, []).append((date, h_won))
            for hid in a_heroes.split(","):
                hid = hid.strip()
                if hid:
                    self._hero_index.setdefault(hid, []).append((date, not h_won))

    def _hero_winrates_before(self, before_date: str, min_games: int = 5) -> dict[str, float]:
        """O(H) lookup: compute hero win rates from pre-built index."""
        if not hasattr(self, "_hero_index"):
            return {}
        result: dict[str, float] = {}
        for hid, entries in self._hero_index.items():
            wins = total = 0
            for d, won in entries:
                if d >= before_date:
                    break
                total += 1
                if won:
                    wins += 1
            if total >= min_games:
                result[hid] = wins / total
        return result

    def _hero_draft_features(self, game: dict[str, Any], games: pd.DataFrame) -> dict[str, float]:
        """Draft-based features for Dota2: hero win rates, draft quality, meta picks."""
        self._build_hero_winrate_index(games)
        date = str(game.get("date", ""))
        home_heroes_str = str(game.get("home_heroes") or "")
        away_heroes_str = str(game.get("away_heroes") or "")

        defaults = {
            "home_draft_quality": 0.0, "away_draft_quality": 0.0, "draft_quality_diff": 0.0,
            "home_meta_picks": 0.0, "away_meta_picks": 0.0,
            "home_avg_hero_wr": 0.5, "away_avg_hero_wr": 0.5,
            "home_max_hero_wr": 0.5, "away_max_hero_wr": 0.5,
            "home_min_hero_wr": 0.5, "away_min_hero_wr": 0.5,
        }

        if not home_heroes_str or not away_heroes_str:
            return defaults

        hero_wr = self._hero_winrates_before(date)
        if not hero_wr:
            return defaults

        # Determine meta heroes (top 20% win rate with 10+ games)
        wr_vals = sorted(hero_wr.values(), reverse=True)
        meta_threshold = wr_vals[max(0, len(wr_vals) // 5)] if wr_vals else 0.55

        def _team_draft_stats(heroes_str: str) -> dict:
            heroes = [h.strip() for h in heroes_str.split(",") if h.strip()]
            if not heroes:
                return {"quality": 0.0, "meta": 0.0, "avg_wr": 0.5, "max_wr": 0.5, "min_wr": 0.5}
            wrs = [hero_wr.get(h, 0.5) for h in heroes]
            meta_count = sum(1 for h in heroes if hero_wr.get(h, 0.5) >= meta_threshold)
            return {
                "quality": sum(wrs),
                "meta": float(meta_count),
                "avg_wr": float(np.mean(wrs)),
                "max_wr": float(max(wrs)),
                "min_wr": float(min(wrs)),
            }

        h_stats = _team_draft_stats(home_heroes_str)
        a_stats = _team_draft_stats(away_heroes_str)

        return {
            "home_draft_quality": h_stats["quality"],
            "away_draft_quality": a_stats["quality"],
            "draft_quality_diff": h_stats["quality"] - a_stats["quality"],
            "home_meta_picks": h_stats["meta"],
            "away_meta_picks": a_stats["meta"],
            "home_avg_hero_wr": h_stats["avg_wr"],
            "away_avg_hero_wr": a_stats["avg_wr"],
            "home_max_hero_wr": h_stats["max_wr"],
            "away_max_hero_wr": a_stats["max_wr"],
            "home_min_hero_wr": h_stats["min_wr"],
            "away_min_hero_wr": a_stats["min_wr"],
        }

    # ── Main Extraction ───────────────────────────────────

    # Major/prestigious tournament name patterns → tier score (1=regional, 5=world championship)
    _TIER_PATTERNS: list[tuple[int, list[str]]] = [
        (5, ["world championship", "worlds", "the international", "pgl major", "iem katowice",
             "iem cologne", "blast premier world", "iem world championship"]),
        (4, ["esl pro league", "blast premier", "iem", "esl one", "dreamhack masters",
             "pro league", "lcs championship", "lcq", "msl"]),
        (3, ["esl challenger", "faceit league", "cct", "european pro league", "regional major",
             "regional qualifier", "lfl", "nbl", "united21", "dream league"]),
        (2, ["esea", "national", "open qualifier", "open cup", "community cup"]),
    ]

    def _tournament_tier(self, venue: str) -> float:
        """Return 1-5 prestige score based on tournament/venue name."""
        v = (venue or "").lower()
        for tier, patterns in self._TIER_PATTERNS:
            if any(p in v for p in patterns):
                return float(tier)
        return 1.0  # default regional/unknown

    def _load_schedule_fatigue(self, season: int) -> pd.DataFrame:
        """Load schedule fatigue file for a season (team-level rows)."""
        try:
            df = self._reader.load(self.sport, "schedule_fatigue", season=season)
            if not df.empty:
                return df
        except Exception:
            pass
        return pd.DataFrame()

    def _fatigue_features(self, team_id: str, game_id: str, season: int) -> dict[str, float]:
        """Return rest/fatigue metrics for a team from schedule_fatigue parquet."""
        df = self._load_schedule_fatigue(season)
        if df.empty:
            return {
                "rest_days": 7.0, "is_back_to_back": 0.0,
                "games_last_7d": 0.0, "games_last_14d": 0.0, "fatigue_score": 0.0,
            }
        row = df[(df["team_id"].astype(str) == str(team_id)) &
                 (df["game_id"].astype(str) == str(game_id))]
        if row.empty:
            # Fall back to team last entry in df sorted by date
            team_rows = df[df["team_id"].astype(str) == str(team_id)]
            if team_rows.empty:
                return {
                    "rest_days": 7.0, "is_back_to_back": 0.0,
                    "games_last_7d": 0.0, "games_last_14d": 0.0, "fatigue_score": 0.0,
                }
            row = team_rows.sort_values("date").iloc[[-1]]
        r = row.iloc[0]
        return {
            "rest_days": float(r.get("rest_days") or 7.0),
            "is_back_to_back": float(bool(r.get("is_back_to_back", 0))),
            "games_last_7d": float(r.get("games_last_7d") or 0.0),
            "games_last_14d": float(r.get("games_last_14d") or 0.0),
            "fatigue_score": float(r.get("fatigue_score") or 0.0),
        }

    def _quality_weighted_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 15,
    ) -> dict[str, float]:
        """Win rate weighted by opponent recent form quality.

        Wins vs strong opponents (high win%) are weighted more.
        Returns ``quality_form`` in [-1, 1] and ``quality_win_rate`` in [0, 1].
        """
        defaults = {"quality_form": 0.0, "quality_win_rate": 0.5}
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return defaults

        is_home = recent["home_team_id"].astype(str) == str(team_id)
        h_sc = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        a_sc = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)
        wins = np.where(is_home, h_sc > a_sc, a_sc > h_sc).astype(float)
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))

        opp_qual: dict[str, float] = {}
        for opp_id in set(opp_ids):
            opp_hist = self._team_games_before(games, str(opp_id), date, limit=20)
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
        return {"quality_form": quality_form, "quality_win_rate": quality_win_rate}

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        # Use full cross-season history for form/H2H so early-season games have signal
        all_games_df = self._load_all_games()
        games_df = all_games_df  # alias for methods that use games_df param
        odds_df = self.load_odds(season)
        player_stats = self.load_player_stats(season)

        # "home" = blue/CT side, "away" = red/T side
        h_id = str(game.get("home_team_id", ""))
        a_id = str(game.get("away_team_id", ""))
        date = str(game.get("date", ""))
        game_id = str(game.get("id", ""))

        features: dict[str, Any] = {
            "game_id": game_id,
            "date": date,
            "home_team_id": h_id,
            "away_team_id": a_id,
            "home_score": pd.to_numeric(game.get("home_score"), errors="coerce"),
            "away_score": pd.to_numeric(game.get("away_score"), errors="coerce"),
        }

        # Overall form — use cross-season history for richer signal
        h_form = self.team_form(h_id, date, all_games_df)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, all_games_df)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        # H2H (overall + home-site advantage)
        h2h = self.head_to_head(h_id, a_id, all_games_df, date=date)
        features.update(h2h)
        h2h_home = self.head_to_head_at_home(h_id, a_id, all_games_df, date=date)
        features.update(h2h_home)

        # Tournament tier (1=regional, 5=world championship)
        features["tournament_tier"] = self._tournament_tier(str(game.get("venue", "")))

        features["home_momentum"] = self.momentum(h_id, date, all_games_df)
        features["away_momentum"] = self.momentum(a_id, date, all_games_df)
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]

        # K/D/A and duration — also use cross-season history
        h_stats = self._match_stats(h_id, date, all_games_df)
        features.update({f"home_{k}": v for k, v in h_stats.items()})
        a_stats = self._match_stats(a_id, date, all_games_df)
        features.update({f"away_{k}": v for k, v in a_stats.items()})

        # Objectives
        h_obj = self._objective_features(h_id, date, all_games_df)
        features.update({f"home_{k}": v for k, v in h_obj.items()})
        a_obj = self._objective_features(a_id, date, all_games_df)
        features.update({f"away_{k}": v for k, v in a_obj.items()})

        # Economy
        h_econ = self._economy_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_econ.items()})
        a_econ = self._economy_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_econ.items()})

        # Vision
        h_vis = self._vision_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_vis.items()})
        a_vis = self._vision_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_vis.items()})

        # Side (blue/red)
        h_side = self._side_features(h_id, date, games_df, is_blue_side=True)
        features["home_blue_side_win_pct"] = h_side["side_win_pct"]
        features["home_blue_side_matches"] = float(h_side["side_matches"])
        a_side = self._side_features(a_id, date, games_df, is_blue_side=False)
        features["away_red_side_win_pct"] = a_side["side_win_pct"]
        features["away_red_side_matches"] = float(a_side["side_matches"])

        # Short-window (5-game) form for momentum context
        h_form5 = self.team_form(h_id, date, games_df, window=5)
        a_form5 = self.team_form(a_id, date, games_df, window=5)
        features["home_form5_win_pct"] = h_form5.get("win_pct", 0.0)
        features["away_form5_win_pct"] = a_form5.get("win_pct", 0.0)

        # ELO ratings
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Individual player rolling stats (aggregated per team) — O(1) via indexed lookup
        h_ps = self._player_aggregate_features(h_id, date, player_stats, season=season, current_game_id=game_id)
        features.update({f"home_{k}": v for k, v in h_ps.items()})
        a_ps = self._player_aggregate_features(a_id, date, player_stats, season=season, current_game_id=game_id)
        features.update({f"away_{k}": v for k, v in a_ps.items()})

        # Player advantage differentials
        features["player_kda_diff"] = features.get("home_player_avg_kda", 0.0) - features.get("away_player_avg_kda", 0.0)
        features["player_gpm_diff"] = features.get("home_player_avg_gpm", 0.0) - features.get("away_player_avg_gpm", 0.0)
        features["player_rating_diff"] = features.get("home_player_avg_rating", 0.0) - features.get("away_player_avg_rating", 0.0)
        # Team-level offensive/objective differentials
        features["kills_pg_diff"] = features.get("home_kills_pg", 0.0) - features.get("away_kills_pg", 0.0)
        features["kda_diff"] = features.get("home_kda", 0.0) - features.get("away_kda", 0.0)
        features["objectives_pg_diff"] = features.get("home_objectives_pg", 0.0) - features.get("away_objectives_pg", 0.0)
        features["first_objective_diff"] = features.get("home_first_objective_rate", 0.0) - features.get("away_first_objective_rate", 0.0)
        features["gold_per_min_diff"] = features.get("home_gold_per_min", 0.0) - features.get("away_gold_per_min", 0.0)
        features["towers_pg_diff"] = features.get("home_towers_pg", 0.0) - features.get("away_towers_pg", 0.0)

        # Hero / draft features (Dota2 only)
        if self.sport == "dota2" and "home_heroes" in games_df.columns:
            draft = self._hero_draft_features(game, games_df)
            features.update(draft)

        # Home/Away (blue/red side) split form — blue side historically wins more
        h_home_form = self.home_away_form(h_id, date, all_games_df, is_home=True)
        features.update({f"home_home_{k}": v for k, v in h_home_form.items()})
        a_away_form = self.home_away_form(a_id, date, all_games_df, is_home=False)
        features.update({f"away_away_{k}": v for k, v in a_away_form.items()})
        features["ha_win_pct_diff"] = (
            features.get("home_home_ha_win_pct", 0.5) - features.get("away_away_ha_win_pct", 0.5)
        )
        features["ha_ppg_diff"] = (
            features.get("home_home_ha_ppg", 0.0) - features.get("away_away_ha_ppg", 0.0)
        )

        # Schedule fatigue (rest days, back-to-back, games density)
        h_fat = self._fatigue_features(h_id, game_id, season)
        features.update({f"home_{k}": v for k, v in h_fat.items()})
        a_fat = self._fatigue_features(a_id, game_id, season)
        features.update({f"away_{k}": v for k, v in a_fat.items()})
        features["rest_days_diff"] = features["home_rest_days"] - features["away_rest_days"]
        features["fatigue_score_diff"] = features["away_fatigue_score"] - features["home_fatigue_score"]

        # Quality-weighted form (wins vs strong opponents count more)
        h_qf = self._quality_weighted_form(h_id, date, all_games_df)
        features.update({f"home_{k}": v for k, v in h_qf.items()})
        a_qf = self._quality_weighted_form(a_id, date, all_games_df)
        features.update({f"away_{k}": v for k, v in a_qf.items()})
        features["quality_form_diff"] = h_qf["quality_form"] - a_qf["quality_form"]
        features["quality_win_rate_diff"] = h_qf["quality_win_rate"] - a_qf["quality_win_rate"]

        # Odds
        odds = self._odds_features(game_id, odds_df)
        features.update(odds)

        return features

    def get_feature_names(self) -> list[str]:
        return [
            # Form
            "home_form_win_pct", "home_form_ppg", "home_form_opp_ppg",
            "home_form_avg_margin", "home_form_games_played",
            "away_form_win_pct", "away_form_ppg", "away_form_opp_ppg",
            "away_form_avg_margin", "away_form_games_played",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            "h2h_home_games", "h2h_home_win_pct", "h2h_home_avg_margin",
            # Tournament context
            "tournament_tier",
            # Momentum
            "home_momentum", "away_momentum", "momentum_diff",
            # K/D/A
            "home_kills_pg", "home_deaths_pg", "home_assists_pg", "home_kda",
            "home_avg_duration_min", "home_duration_std",
            "away_kills_pg", "away_deaths_pg", "away_assists_pg", "away_kda",
            "away_avg_duration_min", "away_duration_std",
            # Objectives
            "home_objectives_pg", "home_first_objective_rate", "home_towers_pg", "home_dragons_pg",
            "away_objectives_pg", "away_first_objective_rate", "away_towers_pg", "away_dragons_pg",
            # Economy
            "home_gold_per_min", "home_cs_per_min", "home_gold_diff_at_15",
            "away_gold_per_min", "away_cs_per_min", "away_gold_diff_at_15",
            # Vision
            "home_wards_placed_pg", "home_wards_destroyed_pg", "home_vision_score_pg",
            "away_wards_placed_pg", "away_wards_destroyed_pg", "away_vision_score_pg",
            # Side
            "home_blue_side_win_pct", "home_blue_side_matches",
            "away_red_side_win_pct", "away_red_side_matches",
            # Short-window form
            "home_form5_win_pct", "away_form5_win_pct",
            # Individual player rolling stats
            "home_player_avg_kda", "home_player_avg_gpm", "home_player_avg_rating",
            "home_player_form_std", "home_player_avg_damage", "home_player_star_kda",
            "away_player_avg_kda", "away_player_avg_gpm", "away_player_avg_rating",
            "away_player_form_std", "away_player_avg_damage", "away_player_star_kda",
            # Player advantage differentials
            "player_kda_diff", "player_gpm_diff", "player_rating_diff",
            # Team-level offensive/objective differentials
            "kills_pg_diff", "kda_diff", "objectives_pg_diff",
            "first_objective_diff", "gold_per_min_diff", "towers_pg_diff",
            # Hero / draft features (Dota2)
            "home_draft_quality", "away_draft_quality", "draft_quality_diff",
            "home_meta_picks", "away_meta_picks",
            "home_avg_hero_wr", "away_avg_hero_wr",
            "home_max_hero_wr", "away_max_hero_wr",
            "home_min_hero_wr", "away_min_hero_wr",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
            # Home/Away split form (blue/red side advantage in eSports)
            "home_home_ha_win_pct", "home_home_ha_ppg", "home_home_ha_opp_ppg",
            "home_home_ha_avg_margin", "home_home_ha_games_played",
            "away_away_ha_win_pct", "away_away_ha_ppg", "away_away_ha_opp_ppg",
            "away_away_ha_avg_margin", "away_away_ha_games_played",
            "ha_win_pct_diff", "ha_ppg_diff",
            # Schedule fatigue
            "home_rest_days", "home_is_back_to_back", "home_games_last_7d",
            "home_games_last_14d", "home_fatigue_score",
            "away_rest_days", "away_is_back_to_back", "away_games_last_7d",
            "away_games_last_14d", "away_fatigue_score",
            "rest_days_diff", "fatigue_score_diff",
        ]
