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

# MLB Stats API team IDs (used in pitcher_game_stats) → abbreviation
_MLB_API_TEAM_MAP: dict[str, str] = {
    '108': 'LAA', '109': 'ARI', '110': 'BAL', '111': 'BOS', '112': 'CHC',
    '113': 'CIN', '114': 'CLE', '115': 'COL', '116': 'DET', '117': 'HOU',
    '118': 'KC',  '119': 'LAD', '120': 'WSH', '121': 'NYM', '133': 'ATH',
    '134': 'PIT', '135': 'SD',  '136': 'SEA', '137': 'SF',  '138': 'STL',
    '139': 'TB',  '140': 'TEX', '141': 'TOR', '142': 'MIN', '143': 'PHI',
    '144': 'ATL', '145': 'CHW', '146': 'MIA', '147': 'NYY', '158': 'MIL',
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
        self._all_games_cache: pd.DataFrame | None = None
        self._all_pstats_cache: pd.DataFrame | None = None
        self._pitcher_game_cache: pd.DataFrame | None = None
        # Pre-built indexes: {abbrev: DataFrame sorted by date}
        self._pitcher_idx: dict[str, pd.DataFrame] = {}
        self._batter_idx: dict[str, pd.DataFrame] = {}
        self._pitcher_game_by_team: dict[str, pd.DataFrame] = {}

    def _load_pitcher_game_stats(self) -> pd.DataFrame:
        """Load and cache pitcher_game_stats parquets via DuckDB reader."""
        if self._pitcher_game_cache is not None:
            return self._pitcher_game_cache
        try:
            combined = self._reader.load_all_seasons(self.sport, "pitcher_game_stats")
        except Exception:
            combined = pd.DataFrame()
        if not combined.empty:
            for col in ("game_id", "team_id", "player_id"):
                if col in combined.columns:
                    combined[col] = combined[col].astype(str)
            if "team_id" in combined.columns:
                combined["team_abbrev"] = combined["team_id"].map(_MLB_API_TEAM_MAP).fillna(combined["team_id"])
        self._pitcher_game_cache = combined
        # Build per-team index keyed by abbreviation
        if not combined.empty and "team_abbrev" in combined.columns:
            for abbrev, grp in combined.groupby("team_abbrev"):
                self._pitcher_game_by_team[str(abbrev)] = grp.reset_index(drop=True)
        return combined

    def _starting_pitcher_stats(
        self,
        team_id: str,
        game_id: str,
        date: str,
        window: int = 5,
    ) -> dict[str, float]:
        """Get rolling stats for the team's starting pitcher in a specific game.

        Uses pitcher_game_stats where starter = player with most innings in game.
        Then computes rolling ERA, K rate, BB rate, HR/9 from last `window` starts.
        """
        defaults = {
            "spg_era_l5": 0.0,
            "spg_k_rate": 0.0,
            "spg_bb_rate": 0.0,
            "spg_hr9": 0.0,
            "spg_ip_avg": 0.0,
            "spg_whip": 0.0,
        }
        try:
            all_pg = self._load_pitcher_game_stats()
            if all_pg.empty:
                return defaults

            # Map ESPN numeric team_id to abbreviation (same key as pitcher_game_by_team)
            abbrev = _TEAM_ID_TO_ABBREV.get(str(team_id), str(team_id))
            team_data = self._pitcher_game_by_team.get(abbrev)
            if team_data is None or team_data.empty:
                return defaults

            # Find who started this specific game (most innings pitched)
            # Note: pitcher_game_stats game_ids are MLB API IDs, not ESPN IDs
            # We can't match by game_id across ID systems; use team context only
            starter_id = None

            # Get this starter's recent starts (excluding current game)
            if starter_id and "player_id" in team_data.columns:
                starter_data = team_data.loc[team_data["player_id"] == starter_id].copy()
            else:
                # Use all pitchers for this team; filter to probable starters (IP >= 3)
                if "innings" in team_data.columns:
                    ip_col = pd.to_numeric(team_data["innings"], errors="coerce").fillna(0)
                    starter_data = team_data.loc[ip_col >= 3.0].copy()
                    if len(starter_data) < 3:
                        starter_data = team_data.copy()
                else:
                    starter_data = team_data.copy()

            if starter_data.empty or len(starter_data) < 1:
                return defaults

            # Take last window appearances
            recent = starter_data.tail(window)

            def _sum(col: str) -> float:
                if col in recent.columns:
                    return float(pd.to_numeric(recent[col], errors="coerce").fillna(0).sum())
                return 0.0

            ip_total = _sum("innings")
            er_total = _sum("earned_runs")
            k_total = _sum("strikeouts")
            bb_total = _sum("walks")
            hr_total = _sum("home_runs_allowed")
            bf_total = _sum("batters_faced")

            era = float(er_total / ip_total * 9) if ip_total > 0 else 0.0
            k_rate = float(k_total / bf_total) if bf_total > 0 else 0.0
            bb_rate = float(bb_total / bf_total) if bf_total > 0 else 0.0
            hr9 = float(hr_total / ip_total * 9) if ip_total > 0 else 0.0
            ip_avg = float(ip_total / len(recent)) if len(recent) > 0 else 0.0
            hits_proxy = er_total * 1.3  # rough proxy when hits not available
            whip = float((bb_total + hits_proxy) / ip_total) if ip_total > 0 else 0.0

            return {
                "spg_era_l5": min(era, 15.0),
                "spg_k_rate": k_rate,
                "spg_bb_rate": bb_rate,
                "spg_hr9": min(hr9, 5.0),
                "spg_ip_avg": ip_avg,
                "spg_whip": min(whip, 5.0),
            }
        except Exception:
            return defaults

    def _load_all_player_stats(self) -> pd.DataFrame:
        """Load and cache all MLB player stats parquets, building per-team indexes."""
        if self._all_pstats_cache is not None:
            return self._all_pstats_cache
        try:
            combined = self._reader.load_all_seasons(self.sport, "player_stats")
        except Exception:
            combined = pd.DataFrame()
        if not combined.empty and "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
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
                    "sp_era_plus": 100.0, "sp_fip": 4.5, "sp_era_trend": 0.0,
                    "sp_k_trend": 0.0, "sp_batters_faced_pg": 0.0}
        abbrev = _TEAM_ID_TO_ABBREV.get(str(team_id))
        if not abbrev:
            return defaults
        self._load_all_player_stats()  # ensure indexes built
        # Filter to likely starters (avg IP >= 2.0 per appearance)
        raw_recent = self._get_team_stats_before(self._pitcher_idx, abbrev, date, window * 3)
        if not raw_recent.empty and "innings" in raw_recent.columns:
            ip_col = pd.to_numeric(raw_recent["innings"], errors="coerce").fillna(0)
            starters = raw_recent[ip_col >= 2.0]
            recent = starters.tail(window) if len(starters) >= max(window // 3, 3) else raw_recent.tail(window)
        else:
            recent = self._get_team_stats_before(self._pitcher_idx, abbrev, date, window)
        if recent.empty:
            return defaults

        def _avg(col: str) -> float:
            vals = pd.to_numeric(recent.get(col, pd.Series(dtype=float)), errors="coerce").dropna()
            vals = vals[np.isfinite(vals)]  # drop inf values
            return float(vals.mean()) if len(vals) > 0 else 0.0

        # Prefer computing ERA from earned_runs/innings (more reliable than raw ERA column)
        earned_r_series = pd.to_numeric(recent.get("earned_runs", pd.Series(dtype=float)), errors="coerce").dropna()
        ip_series = pd.to_numeric(recent.get("innings", pd.Series(dtype=float)), errors="coerce").dropna()

        # Align series by index
        def _ip_to_decimal(ip_val: float) -> float:
            whole = int(ip_val)
            frac = ip_val - whole
            return whole + (frac / 0.3)  # .1 = 1/3, .2 = 2/3

        if len(earned_r_series) > 0 and len(ip_series) > 0:
            common_idx = earned_r_series.index.intersection(ip_series.index)
            total_er = earned_r_series.loc[common_idx].sum()
            total_ip = ip_series.loc[common_idx].sum()
            total_ip_dec = sum(_ip_to_decimal(v) for v in ip_series.loc[common_idx] if v >= 0)
            computed_era = float(total_er / total_ip_dec * 9) if total_ip_dec > 0.1 else 0.0
            computed_era = min(computed_era, 27.0)  # cap at 27 ERA (pathological games)
        else:
            computed_era = 0.0
            total_ip_dec = 0.0
            common_idx = pd.Index([])

        era = computed_era if computed_era > 0 else _avg("era")
        ks = _avg("strikeouts")
        bbs = _avg("walks") or _avg("bb")
        hr_avg = _avg("hr")
        ip = total_ip_dec / max(len(recent), 1) if total_ip_dec > 0 else _avg("innings")
        k9 = float(ks / ip * 9) if ip > 0 else 0.0
        bb9 = float(bbs / ip * 9) if ip > 0 else 0.0
        k_bb_ratio = float(ks / bbs) if bbs > 0 else (ks if ks > 0 else 0.0)
        # ERA+ proxy: 4.5 is roughly league-average ERA; ERA+ = 100 * (league_avg / ERA)
        era_plus = float(min(200.0, 100.0 * 4.5 / era)) if era > 0 else 100.0
        # WHIP: compute from hits+walks/innings (win/loss/whip columns are null)
        hits_avg = _avg("hits")
        walks_avg = _avg("walks") or _avg("bb")
        whip = float((hits_avg + walks_avg) / ip) if ip > 0 else 0.0
        # FIP (Fielding Independent Pitching): isolates pitcher from defense
        # FIP = (13*HR + 3*BB - 2*K) / IP + FIP_constant (~3.2 league avg)
        fip_constant = 3.2
        fip = float((13 * hr_avg + 3 * bbs - 2 * ks) / ip + fip_constant) if ip > 0 else 4.5
        fip = max(0.0, min(fip, 10.0))  # cap to reasonable range
        # Batters faced per game (workload indicator)
        bf_avg = _avg("batters_faced") or (ip * 4.2 if ip > 0 else 0.0)
        # ERA trend: compare recent 3 starts vs last 10 (negative = improving)
        recent3 = recent.tail(3)
        er3 = pd.to_numeric(recent3.get("earned_runs", pd.Series(dtype=float)), errors="coerce").dropna()
        ip3 = pd.to_numeric(recent3.get("innings", pd.Series(dtype=float)), errors="coerce").dropna()
        if len(er3) > 0 and len(ip3) > 0:
            common3 = er3.index.intersection(ip3.index)
            ip3_dec = sum(_ip_to_decimal(v) for v in ip3.loc[common3] if v >= 0)
            era3 = float(er3.loc[common3].sum() / ip3_dec * 9) if ip3_dec > 0.1 else era
            era3 = min(era3, 27.0)
        else:
            era3 = era
        era_trend = era3 - era  # positive = worsening trend, negative = improving
        # K trend
        k3_avg = float(pd.to_numeric(recent3.get("strikeouts", pd.Series(dtype=float)), errors="coerce").dropna().mean()) if len(recent3) > 0 else ks
        k_trend = k3_avg - ks  # positive = striking out more recently
        # Quality starts proxy: fraction of outings where ERA < 4.5 (computed per appearance)
        if not ip_series.empty and not earned_r_series.empty and len(common_idx) > 0:
            per_game_era = []
            for idx_v in common_idx:
                ip_v = ip_series.loc[idx_v]
                er_v = earned_r_series.loc[idx_v]
                ip_dec = _ip_to_decimal(float(ip_v))
                if ip_dec > 0.1:
                    per_game_era.append(float(er_v) / ip_dec * 9)
            qs_rate = float(sum(1 for e in per_game_era if e < 4.5) / len(per_game_era)) if per_game_era else 0.5
        else:
            qs_rate = 0.5
        return {
            "sp_era": era,
            "sp_k_per_9": k9,
            "sp_bb_per_9": bb9,
            "sp_k_bb_ratio": k_bb_ratio,
            "sp_era_plus": era_plus,
            "sp_whip": whip,
            "sp_fip": fip,
            "sp_win_pct": qs_rate,   # repurpose sp_win_pct as quality-start rate (since win/loss null)
            "sp_qs_rate": qs_rate,   # explicit quality start rate feature
            "sp_avg_ip": ip,         # average innings per start (stamina / bullpen usage indicator)
            "sp_era_trend": era_trend,  # ERA trend (recent 3 vs 10; negative = improving)
            "sp_k_trend": k_trend,      # K trend (recent 3 vs 10; positive = better recently)
            "sp_batters_faced_pg": bf_avg,  # avg batters faced per game
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
            std = self.load_standings(season)
            if not std.empty:
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

    def _advanced_batting_team(
        self,
        team_id: str,
        season: int,
    ) -> dict[str, float]:
        """Aggregate woba/wrc_plus/iso/babip/bb_pct/k_pct for a team+season.

        Tries season-specific parquet first (more current), falls back to
        the aggregate Lahman-based advanced_batting.parquet.
        """
        defaults = {"woba": 0.0, "wrc_plus": 100.0, "iso": 0.0, "babip": 0.0, "bb_pct": 0.0, "k_pct": 0.0}
        cache_key = f"adv_batting_{season}"
        if not hasattr(self, "_adv_batting_season_cache"):
            self._adv_batting_season_cache: dict[str, pd.DataFrame] = {}
        if cache_key not in self._adv_batting_season_cache:
            df: pd.DataFrame | None = None
            # Try via DuckDB reader (season-specific curated first)
            try:
                df = self.load_advanced_batting(season)
            except Exception:
                df = None
            if df is None or df.empty:
                # Fall back to aggregate via reader (all seasons)
                if not hasattr(self, "_adv_batting_cache"):
                    try:
                        full = self._reader.load_all_seasons(self.sport, "advanced_batting")
                        if not full.empty:
                            full["season"] = pd.to_numeric(full["season"], errors="coerce")
                        self._adv_batting_cache = full
                    except Exception:
                        self._adv_batting_cache = pd.DataFrame()
                agg = self._adv_batting_cache
                if not agg.empty and "season" in agg.columns:
                    df = agg[agg["season"] == int(season)].copy()
                else:
                    df = pd.DataFrame()
            self._adv_batting_season_cache[cache_key] = df if df is not None else pd.DataFrame()

        adv = self._adv_batting_season_cache[cache_key]
        if adv.empty:
            return defaults
        # Translate numeric ESPN team_id → abbreviation for advanced_batting lookup
        lookup_id = str(team_id)
        abbrev = _TEAM_ID_TO_ABBREV.get(lookup_id, lookup_id)
        mask = adv["team_id"].astype(str) == abbrev
        team_df = adv[mask].copy()
        if team_df.empty:
            # Also try with the original numeric ID (in case some files already use it)
            mask2 = adv["team_id"].astype(str) == lookup_id
            team_df = adv[mask2].copy()
        if team_df.empty:
            return defaults
        team_df["plate_appearances"] = pd.to_numeric(team_df["plate_appearances"], errors="coerce").fillna(0)
        pa = team_df["plate_appearances"].values
        total_pa = pa.sum()
        if total_pa <= 0:
            return defaults
        def _wavg(col: str) -> float:
            if col not in team_df.columns:
                return 0.0
            vals = pd.to_numeric(team_df[col], errors="coerce").fillna(0).values
            return float(np.dot(vals, pa) / total_pa)
        return {
            "woba": _wavg("woba"),
            "wrc_plus": _wavg("wrc_plus") if "wrc_plus" in team_df.columns else 100.0,
            "iso": _wavg("iso"),
            "babip": _wavg("babip"),
            "bb_pct": _wavg("bb_pct"),
            "k_pct": _wavg("k_pct"),
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

    def _clutch_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 20,
    ) -> dict[str, float]:
        """One-run game record, extra innings rate, first-to-score rate."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {
            "one_run_win_pct": 0.5,
            "extra_innings_rate": 0.1,
            "first_to_score_rate": 0.5,
            "late_innings_scoring": 0.0,
        }
        if recent.empty:
            return defaults

        n = len(recent)
        hs = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        as_ = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)
        margin = (hs - as_).abs()

        # One-run games record
        one_run_mask = margin == 1
        one_run_games = recent[one_run_mask]
        if len(one_run_games) > 0:
            one_run_home = one_run_games[one_run_games["home_team_id"].astype(str) == str(team_id)]
            one_run_away = one_run_games[one_run_games["away_team_id"].astype(str) == str(team_id)]
            one_run_wins = 0
            if not one_run_home.empty:
                one_run_wins += int((
                    pd.to_numeric(one_run_home["home_score"], errors="coerce").fillna(0) >
                    pd.to_numeric(one_run_home["away_score"], errors="coerce").fillna(0)
                ).sum())
            if not one_run_away.empty:
                one_run_wins += int((
                    pd.to_numeric(one_run_away["away_score"], errors="coerce").fillna(0) >
                    pd.to_numeric(one_run_away["home_score"], errors="coerce").fillna(0)
                ).sum())
            one_run_wpct = float(one_run_wins / len(one_run_games))
        else:
            one_run_wpct = 0.5

        # Extra innings rate (home_extras or away_extras > 0)
        extras_h = pd.to_numeric(recent.get("home_extras", pd.Series(0, index=recent.index)), errors="coerce").fillna(0)
        extras_a = pd.to_numeric(recent.get("away_extras", pd.Series(0, index=recent.index)), errors="coerce").fillna(0)
        extras_rate = float(((extras_h > 0) | (extras_a > 0)).mean())

        # First-to-score rate: team scores in first inning more often than not
        home_i1 = pd.to_numeric(recent.get("home_i1", pd.Series(np.nan, index=recent.index)), errors="coerce")
        away_i1 = pd.to_numeric(recent.get("away_i1", pd.Series(np.nan, index=recent.index)), errors="coerce")
        valid_i1 = home_i1.notna() & away_i1.notna()
        if valid_i1.sum() > 0:
            i1_games = recent[valid_i1]
            i1_h = home_i1[valid_i1]
            i1_a = away_i1[valid_i1]
            is_home_team = i1_games["home_team_id"].astype(str) == str(team_id)
            team_i1 = pd.Series(np.where(is_home_team, i1_h, i1_a), index=i1_games.index)
            first_to_score = float((team_i1 > 0).mean())
        else:
            first_to_score = 0.5

        # Late-innings scoring: runs scored in innings 7-9 per game
        late_cols_h = ["home_i7", "home_i8", "home_i9"]
        late_cols_a = ["away_i7", "away_i8", "away_i9"]
        is_home = recent["home_team_id"].astype(str) == str(team_id)
        late_runs = 0.0
        late_valid = 0
        for col_h, col_a in zip(late_cols_h, late_cols_a):
            col = col_h if True else col_a
            team_col = recent.apply(
                lambda r, ch=col_h, ca=col_a, tid=str(team_id): (
                    pd.to_numeric(r.get(ch, 0), errors="coerce") or 0.0
                    if str(r.get("home_team_id", "")) == tid else
                    pd.to_numeric(r.get(ca, 0), errors="coerce") or 0.0
                ), axis=1
            )
            valid = team_col.notna()
            if valid.sum() > 0:
                late_runs += float(team_col[valid].mean())
                late_valid += 1
        late_scoring = late_runs / late_valid if late_valid > 0 else 0.0

        return {
            "one_run_win_pct": one_run_wpct,
            "extra_innings_rate": extras_rate,
            "first_to_score_rate": first_to_score,
            "late_innings_scoring": late_scoring,
        }

    def _team_game_stats_rolling(
        self,
        team_id: str,
        date: str,
        season: int,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling batting/pitching stats from team_game_stats (100% fill rate).

        Provides per-game averages for: runs, hits, HR, walks, K, ERA, WHIP
        and derived metrics (K/BB ratio, runs-per-hit efficiency, scoring rate).
        """
        defaults: dict[str, float] = {
            "tgs_runs_pg": 0.0,
            "tgs_hits_pg": 0.0,
            "tgs_hr_pg": 0.0,
            "tgs_walks_pg": 0.0,
            "tgs_k_pg": 0.0,
            "tgs_era": 4.50,
            "tgs_whip": 1.30,
            "tgs_k_bb_ratio": 1.0,
            "tgs_run_efficiency": 0.0,
            "tgs_scoring_rate": 0.0,
        }
        try:
            frames = []
            for s in range(max(2020, season - 2), season + 1):
                try:
                    tgs_s = self._reader.load(self.sport, "team_game_stats", season=s)
                    if not tgs_s.empty:
                        frames.append(tgs_s)
                except Exception:
                    pass
            if not frames:
                return defaults
            tgs = pd.concat(frames, ignore_index=True)
            if "team_id" not in tgs.columns:
                return defaults

            # Filter to this team's games before date
            tgs["team_id"] = tgs["team_id"].astype(str)
            tid_str = str(team_id)

            # We need to join with game dates; use game_id to look up dates
            all_g = self._load_all_games()
            if "id" not in all_g.columns:
                return defaults
            date_map = all_g.set_index(all_g["id"].astype(str))["date"].to_dict()
            tgs["_game_date"] = tgs["game_id"].astype(str).map(date_map)
            team_rows = tgs.loc[tgs["team_id"] == tid_str].copy()
            team_rows = team_rows.dropna(subset=["_game_date"])
            team_rows = team_rows.loc[team_rows["_game_date"] < date]
            team_rows = team_rows.sort_values("_game_date")

            if len(team_rows) < 3:
                return defaults

            recent = team_rows.tail(window)

            def _avg(col: str, default: float = 0.0) -> float:
                if col not in recent.columns:
                    return default
                return float(pd.to_numeric(recent[col], errors="coerce").fillna(default).mean())

            runs = _avg("runs")
            hits = _avg("hits")
            hr = _avg("home_runs")
            walks = _avg("walks")
            ks = _avg("strikeouts")
            era = _avg("era", 4.50)
            whip = _avg("whip", 1.30)

            k_bb = float(ks / walks) if walks > 0.1 else ks
            run_eff = float(runs / hits) if hits > 0.5 else 0.0
            scoring = 1.0 if runs > 4.5 else (0.0 if runs < 3.0 else 0.5)

            return {
                "tgs_runs_pg": min(runs, 20.0),
                "tgs_hits_pg": min(hits, 25.0),
                "tgs_hr_pg": min(hr, 5.0),
                "tgs_walks_pg": min(walks, 10.0),
                "tgs_k_pg": min(ks, 15.0),
                "tgs_era": min(era, 10.0),
                "tgs_whip": min(whip, 3.0),
                "tgs_k_bb_ratio": min(k_bb, 10.0),
                "tgs_run_efficiency": min(run_eff, 2.0),
                "tgs_scoring_rate": scoring,
            }
        except Exception:
            return defaults

    def _batter_rolling_form(
        self,
        team_id: str,
        date: str,
        season: int,
        window: int = 10,
    ) -> dict[str, float]:
        """Team-level rolling batting form using batter_game_stats.

        Aggregates all batters for the team per game to compute team BA, OBP,
        SLG, and OPS over the last ``window`` games before ``date``.
        These composite batting stats are strong predictors of offensive output.
        """
        defaults: dict[str, float] = {
            "bat_ba": 0.250,
            "bat_obp": 0.320,
            "bat_slg": 0.400,
            "bat_ops": 0.720,
            "bat_hr_pg": 1.0,
            "bat_k_pct": 0.22,
            "bat_bb_pct": 0.09,
            "bat_iso": 0.150,
            "bat_woba": 0.320,
            "bat_runs_pg": 4.5,
            "bat_rbi_pg": 4.5,
        }
        try:
            frames = []
            for s in range(max(2020, season - 1), season + 1):
                try:
                    bgs_s = self._reader.load(self.sport, "batter_game_stats", season=s)
                    if not bgs_s.empty:
                        frames.append(bgs_s)
                except Exception:
                    pass
            if not frames:
                return defaults
            bgs = pd.concat(frames, ignore_index=True)
            if "team_id" not in bgs.columns:
                return defaults

            # batter_game_stats uses MLB Stats API team IDs (108-158).
            # team_id here is an ESPN ID (1-30) — must convert via abbreviation.
            abbrev = _TEAM_ID_TO_ABBREV.get(str(team_id))
            if not abbrev:
                return defaults
            # Build reverse map: abbrev → MLB API ID
            abbrev_to_api = {v: k for k, v in _MLB_API_TEAM_MAP.items()}
            api_id = abbrev_to_api.get(abbrev)
            if not api_id:
                return defaults

            # Map game_id → date using a batter_game_stats game_id → games parquet join.
            # batter_game_stats game_ids are MLB Stats API IDs, but all_games uses ESPN IDs.
            # Use the normalized games parquet `home_team_id` to find dates by matching.
            # Fallback: join on season + team identity to find game dates.
            all_g = self._load_all_games()
            if "id" not in all_g.columns:
                return defaults

            # Build ESPN game_id → date from games parquet
            espn_date_map = all_g.set_index(all_g["id"].astype(str))["date"].to_dict()

            # batter_game_stats game_ids may be MLB API game IDs (not ESPN).
            # Check if any match ESPN IDs; if not, build a date map from
            # games that have matching team + season.
            bgs["_api_team_id"] = bgs["team_id"].astype(str)
            team_bgs = bgs.loc[bgs["_api_team_id"] == str(api_id)].copy()
            if team_bgs.empty:
                return defaults

            # Try mapping game_id → date via ESPN map first
            team_bgs["_gdate"] = team_bgs["game_id"].astype(str).map(espn_date_map)
            
            # If date map produced nothing (MLB API vs ESPN game IDs),
            # use the season's games filtered to this team to infer dates.
            if team_bgs["_gdate"].isna().all():
                # Build MLB API game_id → date from matching games for this team
                team_games = all_g[
                    (all_g["season"].astype(str).isin([str(s) for s in range(max(2020, season-1), season+1)])) &
                    (
                        (all_g.get("home_team_id", pd.Series()).astype(str) == str(team_id)) |
                        (all_g.get("away_team_id", pd.Series()).astype(str) == str(team_id))
                    )
                ].copy()
                if team_games.empty:
                    return defaults
                # Sort games for this team and assign sequence to batter game rows
                team_games_sorted = team_games.sort_values("date").reset_index(drop=True)
                team_bgs_sorted = team_bgs.sort_values("game_id").reset_index(drop=True)
                min_len = min(len(team_games_sorted), len(team_bgs_sorted))
                date_seq = team_games_sorted["date"].iloc[:min_len].values
                team_bgs = team_bgs_sorted.iloc[:min_len].copy()
                team_bgs["_gdate"] = date_seq

            # Aggregate per game
            def _n(c: str) -> pd.Series:
                return pd.to_numeric(team_bgs[c], errors="coerce").fillna(0) if c in team_bgs.columns else pd.Series(0.0, index=team_bgs.index)

            team_bgs = team_bgs.dropna(subset=["_gdate"])
            team_bgs = team_bgs.loc[team_bgs["_gdate"].astype(str) < str(date)]
            if team_bgs.empty:
                return defaults

            game_agg = team_bgs.groupby("_gdate").agg(
                ab=("ab", "sum"),
                hits=("hits", "sum"),
                hr=("hr", "sum"),
                bb=("bb", "sum"),
                so=("so", "sum"),
                hbp=("hbp", "sum"),
                pa=("pa", "sum"),
                total_bases=("total_bases", "sum"),
                doubles=("doubles", "sum"),
                triples=("triples", "sum"),
                runs=("runs", "sum") if "runs" in team_bgs.columns else ("ab", "sum"),
                rbi=("rbi", "sum") if "rbi" in team_bgs.columns else ("ab", "sum"),
            ).reset_index()

            game_agg = game_agg.sort_values("_gdate").tail(window)
            if len(game_agg) < 3:
                return defaults

            n_games = len(game_agg)
            ab = float(game_agg["ab"].sum())
            hits = float(game_agg["hits"].sum())
            hr = float(game_agg["hr"].sum())
            bb = float(game_agg["bb"].sum())
            so = float(game_agg["so"].sum())
            hbp = float(game_agg["hbp"].sum())
            pa = float(game_agg["pa"].sum())
            tb = float(game_agg["total_bases"].sum())
            doubles = float(game_agg.get("doubles", pd.Series(0)).sum()) if "doubles" in game_agg.columns else 0.0
            triples = float(game_agg.get("triples", pd.Series(0)).sum()) if "triples" in game_agg.columns else 0.0
            runs_total = float(game_agg["runs"].sum()) if "runs" in game_agg.columns else 0.0
            rbi_total = float(game_agg["rbi"].sum()) if "rbi" in game_agg.columns else 0.0

            ba = hits / ab if ab > 0 else 0.250
            obp_denom = ab + bb + hbp
            obp = (hits + bb + hbp) / obp_denom if obp_denom > 0 else 0.320
            slg = tb / ab if ab > 0 else 0.400
            ops = obp + slg
            hr_pg = hr / n_games if n_games > 0 else 1.0
            k_pct = so / pa if pa > 0 else 0.22
            bb_pct = bb / pa if pa > 0 else 0.09
            # ISO (isolated power): extra-base hit rate
            iso = slg - ba
            # wOBA (simplified): weighted on-base accounting for hit quality
            singles = max(hits - hr - doubles - triples, 0.0)
            woba_num = 0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hr
            woba_denom = ab + bb + hbp
            woba = woba_num / woba_denom if woba_denom > 0 else 0.320
            runs_pg = runs_total / n_games if n_games > 0 else 4.5
            rbi_pg = rbi_total / n_games if n_games > 0 else 4.5

            return {
                "bat_ba": ba,
                "bat_obp": obp,
                "bat_slg": slg,
                "bat_ops": ops,
                "bat_hr_pg": hr_pg,
                "bat_k_pct": k_pct,
                "bat_bb_pct": bb_pct,
                "bat_iso": iso,
                "bat_woba": woba,
                "bat_runs_pg": runs_pg,
                "bat_rbi_pg": rbi_pg,
            }
        except Exception:
            return defaults

    def _series_context(
        self, home_id: str, away_id: str, date: str, games: pd.DataFrame
    ) -> dict[str, float]:
        """Determine game number in the current series (1, 2, 3, 4).

        Teams playing consecutive days at the same venue are in a series.
        Game 1 (fresh SP) vs. game 3-4 (bullpen-heavy) has different dynamics.
        """
        defaults = {"series_game_num": 1.0, "is_series_opener": 1.0, "is_series_finale": 0.0}
        try:
            # Find recent completed games between these two teams in the last 6 days
            cutoff_6d = str(pd.Timestamp(date) - pd.Timedelta(days=6))
            mask = (
                (
                    (games["home_team_id"].astype(str) == str(home_id)) &
                    (games["away_team_id"].astype(str) == str(away_id))
                ) | (
                    (games["home_team_id"].astype(str) == str(away_id)) &
                    (games["away_team_id"].astype(str) == str(home_id))
                )
            )
            matchups = games.loc[mask].copy()
            if "date" in matchups.columns:
                matchups = matchups.loc[
                    (matchups["date"].astype(str) >= cutoff_6d) &
                    (matchups["date"].astype(str) < date)
                ]
            game_num = float(len(matchups) + 1)
            is_opener = 1.0 if game_num == 1 else 0.0
            is_finale = 1.0 if game_num >= 3 else 0.0
            return {
                "series_game_num": min(game_num, 5.0),
                "is_series_opener": is_opener,
                "is_series_finale": is_finale,
            }
        except Exception:
            return defaults

    def _quality_weighted_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Win form weighted by opponent quality (opponent win% up to prediction date).

        Uses single lookup per unique opponent — O(n × log n) not O(n²).
        """
        defaults = {"mlb_quality_form": 0.0, "mlb_quality_win_rate": 0.5}
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
        return {"mlb_quality_form": quality_form, "mlb_quality_win_rate": quality_win_rate}

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        # Use cross-season history for form/H2H/momentum so early-season games have signal
        games_df = self._load_all_games()
        current_season_games = self.load_games(season)  # used only for splits
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
            # Raw per-game hit totals for hits market target (excluded from feature matrix)
            "home_hits_game": pd.to_numeric(game.get("home_hits"), errors="coerce"),
            "away_hits_game": pd.to_numeric(game.get("away_hits"), errors="coerce"),
        }

        # Common features — use full cross-season history for richer signal
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

        h_splits = self.home_away_splits(h_id, current_season_games, season)
        features["home_home_win_pct"] = h_splits["home_win_pct"]
        a_splits = self.home_away_splits(a_id, current_season_games, season)
        features["away_away_win_pct"] = a_splits["away_win_pct"]

        # Rest
        features["home_rest_days"] = float(self.rest_days(h_id, date, games_df))
        features["away_rest_days"] = float(self.rest_days(a_id, date, games_df))

        # Pitcher matchup — use rolling player-stats only (safe, pre-game data).
        # NOTE: _pitcher_features(game, prefix) removed — it reads post-game
        # box-score stats (ERA, IP, K, BB for THIS game) → data leakage.
        h_ps_pitch = self._team_pitching_stats(h_id, date)
        features.update({f"home_{k}": v for k, v in h_ps_pitch.items()})
        a_ps_pitch = self._team_pitching_stats(a_id, date)
        features.update({f"away_{k}": v for k, v in a_ps_pitch.items()})

        # Populate sp_ keys from safe rolling stats (for delta computation)
        features["home_sp_era"] = h_ps_pitch.get("sp_era", 0.0)
        features["home_sp_whip"] = h_ps_pitch.get("sp_whip", 0.0)
        features["home_sp_k9"] = h_ps_pitch.get("sp_k_per_9", 0.0)
        features["home_sp_bb9"] = h_ps_pitch.get("sp_bb_per_9", 0.0)
        features["home_sp_ip_season"] = 0.0  # no pre-game equivalent
        features["home_sp_k_bb_ratio"] = h_ps_pitch.get("sp_k_bb_ratio", 0.0)
        features["home_sp_win_pct"] = h_ps_pitch.get("sp_win_pct", 0.0)
        features["home_sp_fip"] = h_ps_pitch.get("sp_fip", 4.5)
        features["home_sp_era_trend"] = h_ps_pitch.get("sp_era_trend", 0.0)
        features["home_sp_k_trend"] = h_ps_pitch.get("sp_k_trend", 0.0)
        features["home_sp_batters_faced_pg"] = h_ps_pitch.get("sp_batters_faced_pg", 0.0)
        features["away_sp_era"] = a_ps_pitch.get("sp_era", 0.0)
        features["away_sp_whip"] = a_ps_pitch.get("sp_whip", 0.0)
        features["away_sp_k9"] = a_ps_pitch.get("sp_k_per_9", 0.0)
        features["away_sp_bb9"] = a_ps_pitch.get("sp_bb_per_9", 0.0)
        features["away_sp_ip_season"] = 0.0  # no pre-game equivalent
        features["away_sp_k_bb_ratio"] = a_ps_pitch.get("sp_k_bb_ratio", 0.0)
        features["away_sp_win_pct"] = a_ps_pitch.get("sp_win_pct", 0.0)
        features["away_sp_fip"] = a_ps_pitch.get("sp_fip", 4.5)
        features["away_sp_era_trend"] = a_ps_pitch.get("sp_era_trend", 0.0)
        features["away_sp_k_trend"] = a_ps_pitch.get("sp_k_trend", 0.0)
        features["away_sp_batters_faced_pg"] = a_ps_pitch.get("sp_batters_faced_pg", 0.0)
        # Pitcher matchup differentials (home advantage = lower ERA/FIP better)
        features["sp_era_diff"] = features["home_sp_era"] - features["away_sp_era"]
        features["sp_fip_diff"] = features["home_sp_fip"] - features["away_sp_fip"]
        features["sp_whip_diff"] = features["home_sp_whip"] - features["away_sp_whip"]

        # Starting pitcher game-level rolling stats (from pitcher_game_stats_*.parquet)
        h_spg = self._starting_pitcher_stats(h_id, game_id, date)
        features.update({f"home_{k}": v for k, v in h_spg.items()})
        a_spg = self._starting_pitcher_stats(a_id, game_id, date)
        features.update({f"away_{k}": v for k, v in a_spg.items()})
        features["spg_era_diff"] = h_spg["spg_era_l5"] - a_spg["spg_era_l5"]
        features["spg_k_rate_diff"] = h_spg["spg_k_rate"] - a_spg["spg_k_rate"]
        features["spg_bb_rate_diff"] = a_spg["spg_bb_rate"] - h_spg["spg_bb_rate"]

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

        # Advanced batting (woba, wrc+, iso, bb%, k%)
        h_adv = self._advanced_batting_team(h_id, int(season))
        features.update({f"home_{k}": v for k, v in h_adv.items()})
        a_adv = self._advanced_batting_team(a_id, int(season))
        features.update({f"away_{k}": v for k, v in a_adv.items()})

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
        features["period_first_win_pct_diff"] = h_per["period_first_win_pct"] - a_per["period_first_win_pct"]
        features["period_first_opp_ppg_diff"] = h_per["period_first_opp_ppg"] - a_per["period_first_opp_ppg"]

        # ── Key differentials ─────────────────────────────
        features["era_diff"] = features.get("away_sp_era", 0.0) - features.get("home_sp_era", 0.0)  # lower ERA is better
        features["whip_diff"] = features.get("away_sp_whip", 0.0) - features.get("home_sp_whip", 0.0)
        features["era_plus_diff"] = features.get("home_sp_era_plus", 100.0) - features.get("away_sp_era_plus", 100.0)
        features["qs_rate_diff"] = features.get("home_sp_qs_rate", 0.5) - features.get("away_sp_qs_rate", 0.5)
        features["k9_diff"] = features.get("home_sp_k_per_9", 0.0) - features.get("away_sp_k_per_9", 0.0)
        features["bb9_diff"] = features.get("away_sp_bb_per_9", 0.0) - features.get("home_sp_bb_per_9", 0.0)  # lower walks is better
        features["woba_diff"] = features.get("home_woba", 0.0) - features.get("away_woba", 0.0)
        features["wrc_plus_diff"] = features.get("home_wrc_plus", 100.0) - features.get("away_wrc_plus", 100.0)
        features["iso_diff"] = features.get("home_iso", 0.0) - features.get("away_iso", 0.0)
        features["babip_diff"] = features.get("home_babip", 0.0) - features.get("away_babip", 0.0)
        features["k_rate_diff"] = features.get("away_k_rate", 0.0) - features.get("home_k_rate", 0.0)  # lower K rate is better for offense
        features["bb_rate_diff"] = features.get("home_bb_rate", 0.0) - features.get("away_bb_rate", 0.0)
        features["bullpen_era_diff"] = features.get("away_bullpen_era", 0.0) - features.get("home_bullpen_era", 0.0)
        features["pythag_diff"] = features.get("home_pythag_wpct", 0.0) - features.get("away_pythag_wpct", 0.0)
        features["ops_diff"] = features.get("home_ops", 0.0) - features.get("away_ops", 0.0)
        features["runs_pg_diff"] = features.get("home_runs_pg", 0.0) - features.get("away_runs_pg", 0.0)
        features["bb_pct_diff"] = features.get("home_bb_pct", 0.0) - features.get("away_bb_pct", 0.0)
        features["k_pct_diff"] = features.get("away_k_pct", 0.0) - features.get("home_k_pct", 0.0)  # lower K% is better for offense

        # Injury burden
        h_inj = self._injury_features(h_id, season)
        a_inj = self._injury_features(a_id, season)
        for k, v in h_inj.items():
            features[f"home_{k}"] = v
        for k, v in a_inj.items():
            features[f"away_{k}"] = v
        features["injury_severity_diff"] = a_inj["injury_severity_score"] - h_inj["injury_severity_score"]

        # Strength of schedule (average opponent win% over recent games)
        features["home_sos"] = self._strength_of_schedule(h_id, date, games_df, season)
        features["away_sos"] = self._strength_of_schedule(a_id, date, games_df, season)
        features["sos_diff"] = features["home_sos"] - features["away_sos"]

        # Clutch / close-game features
        h_clutch = self._clutch_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_clutch.items()})
        a_clutch = self._clutch_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_clutch.items()})
        features["one_run_win_pct_diff"] = h_clutch["one_run_win_pct"] - a_clutch["one_run_win_pct"]
        features["first_to_score_diff"] = h_clutch["first_to_score_rate"] - a_clutch["first_to_score_rate"]
        features["late_innings_diff"] = h_clutch["late_innings_scoring"] - a_clutch["late_innings_scoring"]

        # Team game stats rolling (100% fill rate, per-game averages)
        h_tgs = self._team_game_stats_rolling(h_id, date, int(season))
        features.update({f"home_{k}": v for k, v in h_tgs.items()})
        a_tgs = self._team_game_stats_rolling(a_id, date, int(season))
        features.update({f"away_{k}": v for k, v in a_tgs.items()})
        features["tgs_runs_diff"] = h_tgs["tgs_runs_pg"] - a_tgs["tgs_runs_pg"]
        features["tgs_era_diff"] = a_tgs["tgs_era"] - h_tgs["tgs_era"]  # positive = home advantage
        features["tgs_whip_diff"] = a_tgs["tgs_whip"] - h_tgs["tgs_whip"]
        features["tgs_k_bb_diff"] = h_tgs["tgs_k_bb_ratio"] - a_tgs["tgs_k_bb_ratio"]

        # Rolling batting form: BA/OBP/SLG from batter_game_stats
        h_bat = self._batter_rolling_form(h_id, date, int(season))
        features.update({f"home_{k}": v for k, v in h_bat.items()})
        a_bat = self._batter_rolling_form(a_id, date, int(season))
        features.update({f"away_{k}": v for k, v in a_bat.items()})
        features["bat_ops_diff"] = h_bat["bat_ops"] - a_bat["bat_ops"]
        features["bat_ba_diff"] = h_bat["bat_ba"] - a_bat["bat_ba"]
        features["bat_hr_pg_diff"] = h_bat["bat_hr_pg"] - a_bat["bat_hr_pg"]
        features["bat_iso_diff"] = h_bat["bat_iso"] - a_bat["bat_iso"]
        features["bat_woba_diff"] = h_bat["bat_woba"] - a_bat["bat_woba"]
        features["bat_runs_pg_diff"] = h_bat["bat_runs_pg"] - a_bat["bat_runs_pg"]
        features["bat_rbi_pg_diff"] = h_bat["bat_rbi_pg"] - a_bat["bat_rbi_pg"]

        # Series context (game 1/2/3/4 in same-venue series)
        series = self._series_context(h_id, a_id, date, games_df)
        features.update(series)

        # Quality-weighted form (SOS-adjusted recent form)
        h_qf = self._quality_weighted_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_qf.items()})
        a_qf = self._quality_weighted_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_qf.items()})
        features["mlb_quality_form_diff"] = h_qf["mlb_quality_form"] - a_qf["mlb_quality_form"]
        features["mlb_quality_win_rate_diff"] = h_qf["mlb_quality_win_rate"] - a_qf["mlb_quality_win_rate"]

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
            "home_momentum", "away_momentum", "momentum_diff",
            "home_home_win_pct", "away_away_win_pct",
            # Home/Away rolling form
            "home_home_ha_win_pct", "home_home_ha_ppg", "home_home_ha_opp_ppg",
            "home_home_ha_avg_margin", "home_home_ha_games_played",
            "away_away_ha_win_pct", "away_away_ha_ppg", "away_away_ha_opp_ppg",
            "away_away_ha_avg_margin", "away_away_ha_games_played",
            "ha_win_pct_diff", "ha_ppg_diff",
            # Rest
            "home_rest_days", "away_rest_days",
            # Pitcher matchup (k_per_9/bb_per_9/era_plus from rolling player_stats)
            "home_sp_era", "home_sp_k_per_9", "home_sp_bb_per_9",
            "home_sp_k_bb_ratio", "home_sp_era_plus",
            "home_sp_whip", "home_sp_win_pct", "home_sp_qs_rate", "home_sp_avg_ip",
            "home_sp_fip", "home_sp_era_trend", "home_sp_k_trend", "home_sp_batters_faced_pg",
            "away_sp_era", "away_sp_k_per_9", "away_sp_bb_per_9",
            "away_sp_k_bb_ratio", "away_sp_era_plus",
            "away_sp_whip", "away_sp_win_pct", "away_sp_qs_rate", "away_sp_avg_ip",
            "away_sp_fip", "away_sp_era_trend", "away_sp_k_trend", "away_sp_batters_faced_pg",
            "sp_era_diff", "sp_fip_diff", "sp_whip_diff",
            # Pitcher game-level rolling (pitcher_game_stats)
            "home_spg_era_l5", "home_spg_k_rate", "home_spg_bb_rate", "home_spg_hr9", "home_spg_ip_avg", "home_spg_whip",
            "away_spg_era_l5", "away_spg_k_rate", "away_spg_bb_rate", "away_spg_hr9", "away_spg_ip_avg", "away_spg_whip",
            "spg_era_diff", "spg_k_rate_diff", "spg_bb_rate_diff",
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
            # Key differentials
            "era_diff", "whip_diff", "era_plus_diff", "qs_rate_diff", "k9_diff", "bb9_diff",
            "woba_diff", "wrc_plus_diff",
            "iso_diff", "babip_diff", "k_rate_diff", "bb_rate_diff", "bullpen_era_diff",
            "pythag_diff", "ops_diff", "runs_pg_diff",
            # Advanced batting (from advanced_batting.parquet)
            "home_woba", "home_wrc_plus", "home_babip", "home_bb_pct", "home_k_pct",
            "away_woba", "away_wrc_plus", "away_babip", "away_bb_pct", "away_k_pct",
            "bb_pct_diff", "k_pct_diff",
            # Inning rolling stats (1st inning early scoring)
            "home_period_first_ppg", "away_period_first_ppg",
            "home_period_first_opp_ppg", "away_period_first_opp_ppg",
            "home_period_first_win_pct", "away_period_first_win_pct",
            "period_first_ppg_diff", "period_first_opp_ppg_diff", "period_first_win_pct_diff",
            # Injury burden
            "home_injury_count", "home_injury_severity_score", "home_injury_out_count",
            "home_injury_dtd_count", "home_injury_questionable_count",
            "away_injury_count", "away_injury_severity_score", "away_injury_out_count",
            "away_injury_dtd_count", "away_injury_questionable_count",
            "injury_severity_diff",
            # Strength of schedule
            "home_sos", "away_sos", "sos_diff",
            # Clutch / close-game features
            "home_one_run_win_pct", "home_extra_innings_rate",
            "home_first_to_score_rate", "home_late_innings_scoring",
            "away_one_run_win_pct", "away_extra_innings_rate",
            "away_first_to_score_rate", "away_late_innings_scoring",
            "one_run_win_pct_diff", "first_to_score_diff", "late_innings_diff",
            # Team game stats rolling (tgs_*)
            "home_tgs_runs_pg", "home_tgs_hits_pg", "home_tgs_hr_pg",
            "home_tgs_walks_pg", "home_tgs_k_pg", "home_tgs_era", "home_tgs_whip",
            "home_tgs_k_bb_ratio", "home_tgs_run_efficiency", "home_tgs_scoring_rate",
            "away_tgs_runs_pg", "away_tgs_hits_pg", "away_tgs_hr_pg",
            "away_tgs_walks_pg", "away_tgs_k_pg", "away_tgs_era", "away_tgs_whip",
            "away_tgs_k_bb_ratio", "away_tgs_run_efficiency", "away_tgs_scoring_rate",
            "tgs_runs_diff", "tgs_era_diff", "tgs_whip_diff", "tgs_k_bb_diff",
            # Series context
            "series_game_num", "is_series_opener", "is_series_finale",
            # Rolling batting form (BA/OBP/SLG/OPS from batter_game_stats)
            "home_bat_ba", "home_bat_obp", "home_bat_slg", "home_bat_ops",
            "home_bat_hr_pg", "home_bat_k_pct", "home_bat_bb_pct",
            "home_bat_iso", "home_bat_woba", "home_bat_runs_pg", "home_bat_rbi_pg",
            "away_bat_ba", "away_bat_obp", "away_bat_slg", "away_bat_ops",
            "away_bat_hr_pg", "away_bat_k_pct", "away_bat_bb_pct",
            "away_bat_iso", "away_bat_woba", "away_bat_runs_pg", "away_bat_rbi_pg",
            "bat_ops_diff", "bat_ba_diff", "bat_hr_pg_diff",
            "bat_iso_diff", "bat_woba_diff", "bat_runs_pg_diff", "bat_rbi_pg_diff",
        ]
