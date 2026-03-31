# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Baseball
# ──────────────────────────────────────────────────────────
#
# Covers MLB.  Produces ~40 features per game including
# pitcher matchup stats, bullpen usage, lineup strength,
# park factors, platoon splits, and defensive efficiency.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)

# Rough park factors (runs multiplier, 1.0 = neutral)
# Numeric team ID → MLB abbreviation (for joining with player_stats)
_TEAM_ID_TO_ABBREV: dict[str, str] = {
    '1': 'BAL', '2': 'BOS', '3': 'LAA', '4': 'CHW', '5': 'CLE',
    '6': 'DET', '7': 'KC', '8': 'MIL', '9': 'MIN', '10': 'NYY',
    '11': 'ATH', '12': 'SEA', '13': 'TEX', '14': 'TOR', '15': 'ATL',
    '16': 'CHC', '17': 'CIN', '18': 'HOU', '19': 'LAD', '20': 'WSH',
    '21': 'NYM', '22': 'PHI', '23': 'PIT', '24': 'STL', '25': 'SD',
    '26': 'SF', '27': 'COL', '28': 'MIA', '29': 'ARI', '30': 'TB',
}

_PARK_FACTORS: dict[str, float] = {
    "coors_field": 1.30,
    "fenway_park": 1.10,
    "yankee_stadium": 1.08,
    "great_american_ball_park": 1.12,
    "globe_life_field": 1.05,
    "oracle_park": 0.88,
    "petco_park": 0.90,
    "tropicana_field": 0.92,
    "oakland_coliseum": 0.90,
    "kauffman_stadium": 0.93,
    "loanDepot_park": 0.95,
}


class BaseballExtractor(BaseFeatureExtractor):
    """Feature extractor for baseball (MLB)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_pstats_cache: pd.DataFrame | None = None
        # Pre-built indexes: {abbrev: DataFrame sorted by date}
        self._pitcher_idx: dict[str, pd.DataFrame] = {}
        self._batter_idx: dict[str, pd.DataFrame] = {}

    def _load_all_player_stats(self) -> pd.DataFrame:
        """Load and cache all MLB player stats parquets, building per-team indexes."""
        if self._all_pstats_cache is not None:
            return self._all_pstats_cache
        sport_dir = self.data_dir / "normalized" / self.sport
        frames = []
        for p in sorted(sport_dir.glob("player_stats_*.parquet")):
            try:
                df = pd.read_parquet(p)
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                frames.append(df)
            except Exception:
                pass
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not combined.empty and "date" in combined.columns:
            combined.sort_values("date", inplace=True, ignore_index=True)
            combined = combined.reset_index(drop=True)
            # Build per-team pitcher/batter indexes for O(log n) lookups
            team_col = "team_id" if "team_id" in combined.columns else None
            if team_col:
                pitchers = combined[combined["era"].notna()] if "era" in combined.columns else combined.iloc[0:0]
                batters = combined[combined["ab"].notna() & (combined.get("ab", pd.Series()) > 0)] if "ab" in combined.columns else combined.iloc[0:0]
                for abbrev, grp in pitchers.groupby(team_col):
                    self._pitcher_idx[str(abbrev)] = grp.reset_index(drop=True)
                for abbrev, grp in batters.groupby(team_col):
                    self._batter_idx[str(abbrev)] = grp.reset_index(drop=True)
        self._all_pstats_cache = combined
        return combined

    def _get_team_stats_before(
        self, idx: dict[str, pd.DataFrame], abbrev: str, date: str, window: int
    ) -> pd.DataFrame:
        """Return the last `window` rows for `abbrev` before `date` using pre-built index."""
        team_df = idx.get(abbrev)
        if team_df is None or team_df.empty:
            return pd.DataFrame()
        ts = pd.Timestamp(date)
        # Binary search on sorted date column
        dates_ns = team_df["date"].values.astype("int64")
        ts_ns = ts.value
        cutoff = int(np.searchsorted(dates_ns, ts_ns, side="left"))
        if cutoff == 0:
            return pd.DataFrame()
        return team_df.iloc[max(0, cutoff - window): cutoff]

    def _team_pitching_stats(
        self, team_id: str, date: str, window: int = 10
    ) -> dict[str, float]:
        """Rolling team pitching stats from player_stats files."""
        defaults = {"sp_era": 0.0, "sp_k_per_9": 0.0, "sp_bb_per_9": 0.0,
                    "sp_whip": 0.0, "sp_win_pct": 0.0, "sp_k_bb_ratio": 0.0,
                    "sp_era_plus": 100.0}
        abbrev = _TEAM_ID_TO_ABBREV.get(str(team_id))
        if not abbrev:
            return defaults
        self._load_all_player_stats()  # ensure indexes built
        recent = self._get_team_stats_before(self._pitcher_idx, abbrev, date, window)
        if recent.empty:
            return defaults

        def _avg(col: str) -> float:
            vals = pd.to_numeric(recent.get(col, pd.Series(dtype=float)), errors="coerce").dropna()
            vals = vals[np.isfinite(vals)]  # drop inf values
            return float(vals.mean()) if len(vals) > 0 else 0.0

        era = _avg("era")
        ip = _avg("innings")
        ks = _avg("strikeouts")
        bbs = _avg("walks") or _avg("bb")  # 'bb' is the column in normalized data
        k9 = float(ks / ip * 9) if ip > 0 else 0.0
        bb9 = float(bbs / ip * 9) if ip > 0 else 0.0
        # K/BB ratio: strikeout dominance vs walk tendency (key pitcher control metric)
        k_bb_ratio = float(ks / bbs) if bbs > 0 else (ks if ks > 0 else 0.0)
        # ERA+ proxy: 4.5 is roughly league-average ERA; ERA+ = 100 * (league_avg / ERA)
        era_plus = float(min(200.0, 100.0 * 4.5 / era)) if era > 0 else 100.0
        wins = recent.get("win", pd.Series(dtype=float))
        losses = recent.get("loss", pd.Series(dtype=float))
        w = pd.to_numeric(wins, errors="coerce").fillna(0).sum()
        l = pd.to_numeric(losses, errors="coerce").fillna(0).sum()
        # WHIP: use direct column if available, else compute from hits+(walks or bb)/innings
        whip = _avg("whip")
        if whip == 0.0 and ip > 0:
            hits = _avg("hits")
            walks = _avg("walks") or _avg("bb")
            whip = float((hits + walks) / ip) if ip > 0 else 0.0
        return {
            "sp_era": era,
            "sp_k_per_9": k9,
            "sp_bb_per_9": bb9,
            "sp_k_bb_ratio": k_bb_ratio,
            "sp_era_plus": era_plus,
            "sp_whip": whip,
            "sp_win_pct": float(w / (w + l)) if (w + l) > 0 else 0.0,
        }

    def _team_batting_from_stats(
        self, team_id: str, date: str, window: int = 10
    ) -> dict[str, float]:
        """Rolling team batting stats from player_stats files."""
        defaults = {"ps_avg": 0.0, "ps_obp": 0.0, "ps_slg": 0.0, "ps_ops": 0.0,
                    "ps_hr_pg": 0.0, "ps_bb_rate": 0.0, "ps_k_rate": 0.0}
        abbrev = _TEAM_ID_TO_ABBREV.get(str(team_id))
        if not abbrev:
            return defaults
        self._load_all_player_stats()  # ensure indexes built
        recent = self._get_team_stats_before(self._batter_idx, abbrev, date, window * 9)
        if recent.empty:
            return defaults

        def _sum(col: str) -> float:
            return float(pd.to_numeric(recent.get(col, pd.Series(dtype=float)), errors="coerce").fillna(0).sum())

        ab = _sum("ab")
        hits = _sum("hits")
        bb = _sum("bb")
        hr = _sum("hr")
        so = _sum("so")
        n = len(recent)
        avg = float(hits / ab) if ab > 0 else 0.0
        slg_vals = pd.to_numeric(recent.get("slg", pd.Series(dtype=float)), errors="coerce").dropna()
        slg = float(slg_vals.mean()) if len(slg_vals) > 0 else avg
        obp_vals = pd.to_numeric(recent.get("obp", pd.Series(dtype=float)), errors="coerce").dropna()
        obp = float(obp_vals.mean()) if len(obp_vals) > 0 else 0.0
        return {
            "ps_avg": avg,
            "ps_obp": obp,
            "ps_slg": slg,
            "ps_ops": obp + slg,
            "ps_hr_pg": float(hr / max(n / 9, 1)),
            "ps_bb_rate": float(bb / (ab + bb)) if (ab + bb) > 0 else 0.0,
            "ps_k_rate": float(so / ab) if ab > 0 else 0.0,
        }

    # ── Pitcher Features ──────────────────────────────────

    def _pitcher_features(
        self,
        game: dict[str, Any],
        prefix: str,
    ) -> dict[str, float]:
        """Starting pitcher stats from the game dict.
        Uses normalized game-level columns (era, whip, innings_pitched, strikeouts, walks).
        Falls back to starter-specific keys if available."""
        def _g(key: str, fallback: str = "") -> float:
            v = game.get(f"{prefix}{key}") or game.get(f"{prefix}{fallback}") or 0.0
            return float(pd.to_numeric(v, errors="coerce") or 0.0)

        era = _g("era", "starter_era")
        whip = _g("whip", "starter_whip")
        ip = _g("innings_pitched", "starter_ip")
        ks = _g("pitching_strikeouts", "strikeouts")
        bbs = _g("pitching_walks", "walks")
        wins = _g("starter_wins")
        losses = _g("starter_losses")
        total_dec = wins + losses

        # Compute K/9, BB/9 from raw counts if whip/k9 not in game dict
        k9 = _g("starter_k9") or (float(ks / ip * 9) if ip > 0 else 0.0)
        bb9 = _g("starter_bb9") or (float(bbs / ip * 9) if ip > 0 else 0.0)
        # Compute WHIP from components if not available directly
        if whip == 0.0 and ip > 0:
            hits = _g("hits", "home_hits") or _g("away_hits")
            whip = float((bbs + hits) / ip)

        return {
            "era": era,
            "whip": whip,
            "k9": k9,
            "bb9": bb9,
            "ip_season": ip,
            "k_bb_ratio": float(k9 / bb9) if bb9 > 0 else 0.0,
            "win_pct": float(wins / total_dec) if total_dec > 0 else 0.0,
        }

    def _bullpen_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 7,
    ) -> dict[str, float]:
        """Recent bullpen workload and effectiveness."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"bullpen_era": 0.0, "bullpen_ip_avg": 0.0, "bullpen_usage": 0.0}

        is_home = (recent["home_team_id"] == team_id).values

        def _col(home_col: str, away_col: str) -> pd.Series:
            _zero = pd.Series(np.zeros(len(recent)))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(0) if home_col in recent.columns else _zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(0) if away_col in recent.columns else _zero
            return pd.Series(np.where(is_home, h.values, a.values))

        # Use team ERA and innings_pitched as bullpen proxy (no separate bullpen columns)
        bp_era = _col("home_bullpen_era", "away_bullpen_era")
        bp_ip = _col("home_bullpen_ip", "away_bullpen_ip")
        # Fallback to game-level ERA / IP if bullpen-specific columns are absent
        if bp_era.sum() == 0:
            bp_era = _col("home_era", "away_era")
        if bp_ip.sum() == 0:
            bp_ip = _col("home_innings_pitched", "away_innings_pitched")

        return {
            "bullpen_era": float(bp_era.mean()),
            "bullpen_ip_avg": float(bp_ip.mean()),
            "bullpen_usage": float(bp_ip.sum()),
        }

    # ── Batting Features ──────────────────────────────────

    def _batting_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Lineup-level batting stats (vectorized)."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "batting_avg": 0.0, "obp": 0.0, "slg": 0.0, "ops": 0.0,
                "iso": 0.0, "k_rate": 0.0, "bb_rate": 0.0,
                "runs_pg": 0.0, "hits_pg": 0.0, "hr_pg": 0.0, "bb_pg": 0.0, "k_pg": 0.0,
            }

        is_home = (recent["home_team_id"] == team_id).values

        def _col(hcol: str, acol: str | None = None) -> np.ndarray:
            if acol is None:
                acol = hcol.replace("home_", "away_")
            _zero = np.zeros(len(recent))
            h = pd.to_numeric(recent[hcol], errors="coerce").fillna(0).values if hcol in recent.columns else _zero
            a = pd.to_numeric(recent[acol], errors="coerce").fillna(0).values if acol in recent.columns else _zero
            return np.where(is_home, h, a).astype(float)

        hits = _col("home_hits", "away_hits").sum()
        abs_ = _col("home_at_bats", "away_at_bats").sum()
        bb = _col("home_walks", "away_walks").sum()
        hbp = _col("home_hbp", "away_hbp").sum()
        sf = _col("home_sac_flies", "away_sac_flies").sum()
        tb = _col("home_total_bases", "away_total_bases").sum()
        hr = _col("home_home_runs", "away_home_runs").sum()
        k = _col("home_strikeouts", "away_strikeouts").sum()
        # runs: try home_runs first, fall back to home_score
        _zero = np.zeros(len(recent))
        hr_col = "home_runs" if "home_runs" in recent.columns else ("home_score" if "home_score" in recent.columns else None)
        ar_col = "away_runs" if "away_runs" in recent.columns else ("away_score" if "away_score" in recent.columns else None)
        h_runs = pd.to_numeric(recent[hr_col], errors="coerce").fillna(0).values if hr_col else _zero
        a_runs = pd.to_numeric(recent[ar_col], errors="coerce").fillna(0).values if ar_col else _zero
        runs = float(np.where(is_home, h_runs, a_runs).sum())

        n = len(recent)
        ba = float(hits / abs_) if abs_ > 0 else 0.0
        pa = abs_ + bb + hbp + sf
        obp = float((hits + bb + hbp) / pa) if pa > 0 else 0.0
        slg = float(tb / abs_) if abs_ > 0 else 0.0

        return {
            "batting_avg": ba, "obp": obp, "slg": slg, "ops": obp + slg,
            "iso": slg - ba,
            "k_rate": float(k / abs_) if abs_ > 0 else 0.0,
            "bb_rate": float(bb / pa) if pa > 0 else 0.0,
            "runs_pg": float(runs / n), "hits_pg": float(hits / n),
            "hr_pg": float(hr / n), "bb_pg": float(bb / n), "k_pg": float(k / n),
        }

    # ── Other Features ────────────────────────────────────

    def _park_factor(self, game: dict[str, Any]) -> float:
        venue = str(game.get("venue", game.get("stadium", ""))).lower().replace(" ", "_")
        for key, factor in _PARK_FACTORS.items():
            if key.lower() in venue:
                return factor
        return 1.0

    def _weather_features(self, game: dict[str, Any]) -> dict[str, float]:
        """Parse weather string into numeric features.
        
        Common formats: "72°F, Wind 12 mph Out to RF", "Roof Closed", "Dome"
        """
        weather = str(game.get("weather", "")).lower()
        result = {
            "weather_temp": 72.0,   # default comfortable temperature
            "weather_wind": 0.0,
            "weather_dome": 0.0,
            "weather_precip": 0.0,
            "weather_cold": 0.0,
            "weather_hot": 0.0,
            "weather_wind_out": 0.0,
            "weather_wind_in": 0.0,
        }
        if not weather or weather in ("nan", "none", ""):
            return result

        # Dome / roof closed
        if any(w in weather for w in ("dome", "roof closed", "retractable", "indoor")):
            result["weather_dome"] = 1.0
            return result

        # Temperature
        import re
        temp_match = re.search(r"(\d+)\s*[°°f]", weather)
        if temp_match:
            temp = float(temp_match.group(1))
            result["weather_temp"] = temp
            result["weather_cold"] = 1.0 if temp < 50 else 0.0
            result["weather_hot"] = 1.0 if temp > 90 else 0.0

        # Wind speed
        wind_match = re.search(r"wind\s+(\d+)\s*mph", weather)
        if wind_match:
            result["weather_wind"] = float(wind_match.group(1))

        # Wind direction
        if any(w in weather for w in ("out to", "blowing out", "wind out")):
            result["weather_wind_out"] = 1.0
        elif any(w in weather for w in ("in from", "blowing in", "wind in")):
            result["weather_wind_in"] = 1.0

        # Precipitation
        if any(w in weather for w in ("rain", "drizzle", "shower", "precip")):
            result["weather_precip"] = 1.0

        return result

    def _pythagorean_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 20,
        exponent: float = 1.83,
    ) -> dict[str, float]:
        """Pythagorean win expectation: W% ≈ RS^e / (RS^e + RA^e).
        
        Uses rolling window of recent games to compute expected win% vs actual win%.
        """
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"pythag_wpct": 0.0, "pythag_diff": 0.0, "run_diff_pg": 0.0}

        is_home = (recent["home_team_id"] == team_id).values
        h_score = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0).values
        a_score = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0).values

        rs = float(np.where(is_home, h_score, a_score).sum())  # runs scored
        ra = float(np.where(is_home, a_score, h_score).sum())  # runs allowed
        n = len(recent)

        if rs + ra == 0:
            pythag_wpct = 0.5
        else:
            rs_exp = (rs ** exponent)
            ra_exp = (ra ** exponent)
            pythag_wpct = rs_exp / (rs_exp + ra_exp) if (rs_exp + ra_exp) > 0 else 0.5

        # Actual win%
        wins = float(np.where(is_home, h_score > a_score, a_score > h_score).sum())
        actual_wpct = wins / n if n > 0 else 0.5

        return {
            "pythag_wpct": pythag_wpct,
            "pythag_diff": pythag_wpct - actual_wpct,  # +ve = underperforming (due for regression)
            "run_diff_pg": (rs - ra) / n if n > 0 else 0.0,
        }

    def _standings_features(
        self,
        team_id: str,
        season: int,
        games: pd.DataFrame,
        date: str,
    ) -> dict[str, float]:
        """Season-level win%, games back, and streak from standings or rolling games."""
        try:
            standings_path = self.data_dir / self.sport / f"standings_{season}.parquet"
            if standings_path.exists():
                std = pd.read_parquet(standings_path)
                row = std[std["team_id"].astype(str) == str(team_id)]
                if not row.empty:
                    row = row.iloc[0]
                    wins = float(row.get("wins", 0))
                    losses = float(row.get("losses", 0))
                    total = wins + losses
                    return {
                        "season_win_pct": wins / total if total > 0 else 0.5,
                        "season_wins": wins,
                        "season_losses": losses,
                        "games_back": float(row.get("games_back", 0)),
                    }
        except Exception:
            pass

        # Fallback: compute from games df
        team_games = self._team_games_before(games, team_id, date, limit=162)
        if team_games.empty:
            return {"season_win_pct": 0.5, "season_wins": 0.0, "season_losses": 0.0, "games_back": 0.0}
        is_home = (team_games["home_team_id"] == team_id).values
        h_score = pd.to_numeric(team_games["home_score"], errors="coerce").fillna(0).values
        a_score = pd.to_numeric(team_games["away_score"], errors="coerce").fillna(0).values
        wins = float(np.where(is_home, h_score > a_score, a_score > h_score).sum())
        n = len(team_games)
        return {
            "season_win_pct": wins / n if n > 0 else 0.5,
            "season_wins": wins,
            "season_losses": float(n - wins),
            "games_back": 0.0,
        }

    def _platoon_splits(
        self,
        game: dict[str, Any],
    ) -> dict[str, float]:
        """Lefty/righty matchup features."""
        h_throws = str(game.get("home_starter_throws", "R")).upper()
        a_throws = str(game.get("away_starter_throws", "R")).upper()
        return {
            "home_starter_is_lefty": 1.0 if h_throws == "L" else 0.0,
            "away_starter_is_lefty": 1.0 if a_throws == "L" else 0.0,
            "same_handedness": 1.0 if h_throws == a_throws else 0.0,
        }

    def _defensive_efficiency(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Defensive efficiency ratio and errors (vectorized)."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"def_efficiency": 0.0, "errors_pg": 0.0}

        is_home = (recent["home_team_id"] == team_id).values
        _zero = np.zeros(len(recent))
        h_err = pd.to_numeric(recent["home_errors"], errors="coerce").fillna(0).values if "home_errors" in recent.columns else _zero
        a_err = pd.to_numeric(recent["away_errors"], errors="coerce").fillna(0).values if "away_errors" in recent.columns else _zero
        h_bip = pd.to_numeric(recent["home_balls_in_play"], errors="coerce").fillna(0).values if "home_balls_in_play" in recent.columns else _zero
        a_bip = pd.to_numeric(recent["away_balls_in_play"], errors="coerce").fillna(0).values if "away_balls_in_play" in recent.columns else _zero

        errors = float(np.where(is_home, h_err, a_err).sum())
        bip = float(np.where(is_home, h_bip, a_bip).sum())
        n = len(recent)

        return {
            "def_efficiency": float(1.0 - errors / bip) if bip > 0 else 0.0,
            "errors_pg": float(errors / n) if n > 0 else 0.0,
        }

    def _short_window_runs(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 5,
    ) -> dict[str, float]:
        """Runs scored and allowed in last `window` games."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"runs_l5": 0.0, "runs_against_l5": 0.0, "run_diff_l5": 0.0}

        is_home = (recent["home_team_id"] == team_id).values
        rs = np.where(
            is_home,
            pd.to_numeric(recent["home_score"], errors="coerce").fillna(0).values,
            pd.to_numeric(recent["away_score"], errors="coerce").fillna(0).values,
        )
        ra = np.where(
            is_home,
            pd.to_numeric(recent["away_score"], errors="coerce").fillna(0).values,
            pd.to_numeric(recent["home_score"], errors="coerce").fillna(0).values,
        )
        n = len(recent)
        runs_pg = float(rs.sum() / n)
        runs_against_pg = float(ra.sum() / n)
        return {
            "runs_l5": runs_pg,
            "runs_against_l5": runs_against_pg,
            "run_diff_l5": runs_pg - runs_against_pg,
        }

    # ── Team Game-Level Pitching ─────────────────────────

    def _team_pitching_performance(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling team pitching performance from game-level pitching stats."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {
            "team_whip": 0.0, "team_era": 0.0, "team_k_rate": 0.0,
            "team_bb_rate": 0.0, "team_hr_rate": 0.0, "team_go_ao_ratio": 1.0,
        }
        if recent.empty:
            return defaults

        is_home = (recent["home_team_id"] == team_id).values

        def _col(hcol: str, acol: str | None = None) -> np.ndarray:
            if acol is None:
                acol = hcol.replace("home_", "away_")
            _zero = np.zeros(len(recent))
            h = pd.to_numeric(recent[hcol], errors="coerce").fillna(0).values if hcol in recent.columns else _zero
            a = pd.to_numeric(recent[acol], errors="coerce").fillna(0).values if acol in recent.columns else _zero
            return np.where(is_home, h, a).astype(float)

        whip = _col("home_whip", "away_whip")
        era = _col("home_era", "away_era")
        pk = _col("home_pitching_strikeouts", "away_pitching_strikeouts")
        pbb = _col("home_pitching_walks", "away_pitching_walks")
        phr = _col("home_pitching_home_runs", "away_pitching_home_runs")
        bf = _col("home_batters_faced", "away_batters_faced")
        go = _col("home_ground_outs", "away_ground_outs")
        fo = _col("home_fly_outs", "away_fly_outs")

        total_bf = float(bf.sum())
        total_fo = float(fo.sum())
        total_go = float(go.sum())

        # Filter out zero WHIP/ERA (games without data)
        valid_whip = whip[whip > 0]
        valid_era = era[era > 0]

        return {
            "team_whip": float(valid_whip.mean()) if len(valid_whip) > 0 else 0.0,
            "team_era": float(valid_era.mean()) if len(valid_era) > 0 else 0.0,
            "team_k_rate": float(pk.sum() / total_bf) if total_bf > 0 else 0.0,
            "team_bb_rate": float(pbb.sum() / total_bf) if total_bf > 0 else 0.0,
            "team_hr_rate": float(phr.sum() / total_bf) if total_bf > 0 else 0.0,
            "team_go_ao_ratio": float(total_go / total_fo) if total_fo > 0 else 1.0,
        }

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self.load_games(season)
        odds_df = self.load_odds(season)

        h_id, a_id = self._resolve_game_team_ids(game, games_df)
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
            # Inning scores — passed through as targets for extra-market models
            "home_i1": pd.to_numeric(game.get("home_i1"), errors="coerce"),
            "home_i2": pd.to_numeric(game.get("home_i2"), errors="coerce"),
            "home_i3": pd.to_numeric(game.get("home_i3"), errors="coerce"),
            "home_i4": pd.to_numeric(game.get("home_i4"), errors="coerce"),
            "home_i5": pd.to_numeric(game.get("home_i5"), errors="coerce"),
            "home_i6": pd.to_numeric(game.get("home_i6"), errors="coerce"),
            "home_i7": pd.to_numeric(game.get("home_i7"), errors="coerce"),
            "home_i8": pd.to_numeric(game.get("home_i8"), errors="coerce"),
            "home_i9": pd.to_numeric(game.get("home_i9"), errors="coerce"),
            "away_i1": pd.to_numeric(game.get("away_i1"), errors="coerce"),
            "away_i2": pd.to_numeric(game.get("away_i2"), errors="coerce"),
            "away_i3": pd.to_numeric(game.get("away_i3"), errors="coerce"),
            "away_i4": pd.to_numeric(game.get("away_i4"), errors="coerce"),
            "away_i5": pd.to_numeric(game.get("away_i5"), errors="coerce"),
            "away_i6": pd.to_numeric(game.get("away_i6"), errors="coerce"),
            "away_i7": pd.to_numeric(game.get("away_i7"), errors="coerce"),
            "away_i8": pd.to_numeric(game.get("away_i8"), errors="coerce"),
            "away_i9": pd.to_numeric(game.get("away_i9"), errors="coerce"),
        }

        # Common features
        h_form = self.team_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)
        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)

        h_splits = self.home_away_splits(h_id, games_df, season)
        features["home_home_win_pct"] = h_splits["home_win_pct"]
        a_splits = self.home_away_splits(a_id, games_df, season)
        features["away_away_win_pct"] = a_splits["away_win_pct"]

        # Rest
        features["home_rest_days"] = float(self.rest_days(h_id, date, games_df))
        features["away_rest_days"] = float(self.rest_days(a_id, date, games_df))

        # Pitcher matchup — try game dict first, then rolling player stats
        h_pitch = self._pitcher_features(game, "home_")
        features.update({f"home_sp_{k}": v for k, v in h_pitch.items()})
        a_pitch = self._pitcher_features(game, "away_")
        features.update({f"away_sp_{k}": v for k, v in a_pitch.items()})

        # Enhance with rolling player-stats pitching if game-dict data is zero
        h_ps_pitch = self._team_pitching_stats(h_id, date)
        if features.get("home_sp_era", 0.0) == 0.0:
            features.update({f"home_{k}": v for k, v in h_ps_pitch.items()})
        else:
            features.update({f"home_{k}": v for k, v in h_ps_pitch.items()})
        a_ps_pitch = self._team_pitching_stats(a_id, date)
        if features.get("away_sp_era", 0.0) == 0.0:
            features.update({f"away_{k}": v for k, v in a_ps_pitch.items()})
        else:
            features.update({f"away_{k}": v for k, v in a_ps_pitch.items()})

        # Bullpen
        h_bp = self._bullpen_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_bp.items()})
        a_bp = self._bullpen_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_bp.items()})

        # Batting — game-level + player-stats rolling
        h_bat = self._batting_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_bat.items()})
        a_bat = self._batting_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_bat.items()})

        h_bat_ps = self._team_batting_from_stats(h_id, date)
        features.update({f"home_{k}": v for k, v in h_bat_ps.items()})
        a_bat_ps = self._team_batting_from_stats(a_id, date)
        features.update({f"away_{k}": v for k, v in a_bat_ps.items()})

        # Park & platoon
        features["park_factor"] = self._park_factor(game)
        platoon = self._platoon_splits(game)
        features.update(platoon)

        # Weather
        weather = self._weather_features(game)
        features.update(weather)

        # Pythagorean expectation (run differential based win%)
        h_pythag = self._pythagorean_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_pythag.items()})
        a_pythag = self._pythagorean_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_pythag.items()})

        # Season standings (win%, games back)
        h_std = self._standings_features(h_id, season, games_df, date)
        features.update({f"home_{k}": v for k, v in h_std.items()})
        a_std = self._standings_features(a_id, season, games_df, date)
        features.update({f"away_{k}": v for k, v in a_std.items()})

        # Defense
        h_def = self._defensive_efficiency(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_def.items()})
        a_def = self._defensive_efficiency(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_def.items()})

        # Team pitching performance (rolling WHIP, ERA, K%, BB%, HR%, GO/AO)
        h_tp = self._team_pitching_performance(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_tp.items()})
        a_tp = self._team_pitching_performance(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_tp.items()})

        # Short-window runs (last 5)
        h_sw = self._short_window_runs(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_sw.items()})
        a_sw = self._short_window_runs(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_sw.items()})

        # ELO ratings
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Odds — with team-name fallback for ID mismatch
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

        # ── Inning Rolling Stats (MLB) ────────────────────
        h_per = self._period_rolling_stats(h_id, date, games_df, n=10, period_scheme="innings")
        features.update({f"home_{k}": v for k, v in h_per.items()})
        a_per = self._period_rolling_stats(a_id, date, games_df, n=10, period_scheme="innings")
        features.update({f"away_{k}": v for k, v in a_per.items()})
        features["period_first_ppg_diff"] = h_per["period_first_ppg"] - a_per["period_first_ppg"]

        # Safety: replace any inf/-inf with 0
        for k, v in features.items():
            if isinstance(v, float) and not np.isfinite(v):
                features[k] = 0.0

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
            # Momentum & splits
            "home_momentum", "away_momentum",
            "home_home_win_pct", "away_away_win_pct",
            # Rest
            "home_rest_days", "away_rest_days",
            # Pitcher matchup (k_per_9/bb_per_9/era_plus from rolling player_stats)
            "home_sp_era", "home_sp_k_per_9", "home_sp_bb_per_9",
            "home_sp_k_bb_ratio", "home_sp_era_plus",
            "home_sp_whip", "home_sp_win_pct",
            "away_sp_era", "away_sp_k_per_9", "away_sp_bb_per_9",
            "away_sp_k_bb_ratio", "away_sp_era_plus",
            "away_sp_whip", "away_sp_win_pct",
            # Bullpen
            "home_bullpen_era", "home_bullpen_ip_avg", "home_bullpen_usage",
            "away_bullpen_era", "away_bullpen_ip_avg", "away_bullpen_usage",
            # Batting
            "home_batting_avg", "home_obp", "home_slg", "home_ops",
            "home_iso", "home_k_rate", "home_bb_rate",
            "home_runs_pg", "home_hits_pg", "home_hr_pg", "home_bb_pg", "home_k_pg",
            "away_batting_avg", "away_obp", "away_slg", "away_ops",
            "away_iso", "away_k_rate", "away_bb_rate",
            "away_runs_pg", "away_hits_pg", "away_hr_pg", "away_bb_pg", "away_k_pg",
            # Park & platoon
            "park_factor",
            "home_starter_is_lefty", "away_starter_is_lefty", "same_handedness",
            # Defense
            "home_def_efficiency", "home_errors_pg",
            "away_def_efficiency", "away_errors_pg",
            # Short-window runs (last 5)
            "home_runs_l5", "home_runs_against_l5", "home_run_diff_l5",
            "away_runs_l5", "away_runs_against_l5", "away_run_diff_l5",
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
        ]
