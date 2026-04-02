# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Motorsport
# ──────────────────────────────────────────────────────────
#
# Covers Formula 1.  Produces ~25 features per race including
# qualifying pace, race pace, pit strategy, grid position,
# constructor performance, track history, and weather.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)


class MotorsportExtractor(BaseFeatureExtractor):
    """Feature extractor for motorsport (F1)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_games_cache: pd.DataFrame | None = None

    # ── Helpers ────────────────────────────────────────────

    def _qualifying_features(
        self,
        game: dict[str, Any],
        prefix: str,
    ) -> dict[str, float]:
        """Qualifying session performance."""
        grid_pos = pd.to_numeric(game.get(f"{prefix}grid_position", 0), errors="coerce") or 0.0
        q_time = pd.to_numeric(game.get(f"{prefix}q_time_ms", 0), errors="coerce") or 0.0
        pole_gap = pd.to_numeric(game.get(f"{prefix}gap_to_pole_ms", 0), errors="coerce") or 0.0
        q_position = pd.to_numeric(game.get(f"{prefix}q_position", 0), errors="coerce") or 0.0

        return {
            "grid_position": grid_pos,
            "q_time_ms": q_time,
            "gap_to_pole_ms": pole_gap,
            "q_position": q_position,
        }

    def _race_pace(
        self,
        driver_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 5,
    ) -> dict[str, float]:
        """Average race finishing position and consistency."""
        recent = self._team_games_before(games, driver_id, date, limit=window)
        if recent.empty:
            return {"avg_finish": 0.0, "finish_std": 0.0, "podium_rate": 0.0, "dnf_rate": 0.0}

        is_home = recent["home_team_id"] == driver_id
        records = recent.to_dict("records")

        finishes, dnfs = [], 0
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            pos = pd.to_numeric(row.get(f"{p}finish_position", row.get("finish_position", 0)), errors="coerce")
            status = str(row.get(f"{p}status", row.get("status", "finished"))).lower()
            if pos and pos > 0 and "dnf" not in status and "ret" not in status:
                finishes.append(float(pos))
            else:
                dnfs += 1

        n = len(recent)
        podiums = sum(1 for f in finishes if f <= 3)

        return {
            "avg_finish": float(np.mean(finishes)) if finishes else 0.0,
            "finish_std": float(np.std(finishes)) if len(finishes) > 1 else 0.0,
            "podium_rate": float(podiums / n),
            "dnf_rate": float(dnfs / n),
        }

    def _pit_stop_features(
        self,
        game: dict[str, Any],
        prefix: str,
    ) -> dict[str, float]:
        """Pit stop efficiency features."""
        avg_pit_time = pd.to_numeric(game.get(f"{prefix}avg_pit_time_s", 0), errors="coerce") or 0.0
        pit_stops = pd.to_numeric(game.get(f"{prefix}pit_stops", 0), errors="coerce") or 0.0

        return {
            "avg_pit_time_s": avg_pit_time,
            "pit_stops": pit_stops,
        }

    def _constructor_features(
        self,
        game: dict[str, Any],
        prefix: str,
        standings: pd.DataFrame,
    ) -> dict[str, float]:
        """Constructor/team championship standing."""
        constructor_id = game.get(f"{prefix}constructor_id", game.get(f"{prefix}team_id", ""))
        feats: dict[str, float] = {
            "constructor_points": 0.0,
            "constructor_position": 0.0,
        }
        if standings.empty or "team_id" not in standings.columns or not constructor_id:
            return feats

        row = standings.loc[standings["team_id"] == str(constructor_id)]
        if not row.empty:
            r = row.iloc[0]
            feats["constructor_points"] = float(pd.to_numeric(r.get("points", 0), errors="coerce") or 0.0)
            feats["constructor_position"] = float(pd.to_numeric(r.get("position", 0), errors="coerce") or 0.0)

        return feats

    def _track_history(
        self,
        driver_id: str,
        circuit_id: str,
        games: pd.DataFrame,
        date: str,
        window: int = 5,
    ) -> dict[str, float]:
        """Driver's history at this specific circuit."""
        if not circuit_id:
            return {"track_avg_finish": 0.0, "track_races": 0}

        all_prev = self._team_games_before(games, driver_id, date)
        track_mask = all_prev.get("circuit_id", pd.Series("", index=all_prev.index)) == circuit_id
        track_games = all_prev.loc[track_mask].head(window)

        if track_games.empty:
            return {"track_avg_finish": 0.0, "track_races": 0}

        is_home = track_games["home_team_id"] == driver_id
        records = track_games.to_dict("records")

        finishes = []
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            pos = pd.to_numeric(row.get(f"{p}finish_position", 0), errors="coerce")
            if pos and pos > 0:
                finishes.append(float(pos))

        return {
            "track_avg_finish": float(np.mean(finishes)) if finishes else 0.0,
            "track_races": len(track_games),
        }

    def _weather_features(self, game: dict[str, Any]) -> dict[str, float]:
        """Weather conditions for the race."""
        temp = pd.to_numeric(game.get("temperature", 25), errors="coerce") or 25.0
        is_wet = 1.0 if str(game.get("weather", "")).lower() in ("rain", "wet", "damp") else 0.0
        track_temp = pd.to_numeric(game.get("track_temperature", temp + 10), errors="coerce") or 35.0

        return {
            "temperature": float(temp),
            "is_wet": is_wet,
            "track_temperature": float(track_temp),
        }

    # ── Per-driver history (uses player_stats instead of games) ──

    def _driver_history_from_stats(
        self,
        player_stats: pd.DataFrame,
        driver_id: str,
        date: Any,
        window: int = 5,
    ) -> dict[str, float]:
        """Compute historical race performance from player_stats."""
        _empty = {"avg_finish": 0.0, "finish_std": 0.0, "podium_rate": 0.0, "dnf_rate": 0.0,
                  "avg_points": 0.0, "win_rate": 0.0, "form_trend": 0.0,
                  "avg_laps_led": 0.0, "avg_running_pos": 0.0}
        if player_stats.empty or not driver_id:
            return _empty

        pid_col = next((c for c in ("player_id", "driver_id") if c in player_stats.columns), None)
        date_col = "date" if "date" in player_stats.columns else None
        if pid_col is None:
            return _empty

        mask = player_stats[pid_col].astype(str) == driver_id
        driver_rows = player_stats[mask]
        if date_col:
            driver_rows = driver_rows[driver_rows[date_col].astype(str) < str(date)]
        driver_rows = driver_rows.sort_values(date_col or pid_col, ascending=False).head(window)

        if driver_rows.empty:
            return _empty

        finishes = pd.to_numeric(driver_rows.get("finish_position", pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        points_list = pd.to_numeric(driver_rows.get("points", pd.Series(dtype=float)), errors="coerce").fillna(0).tolist()
        dnf_list = pd.to_numeric(driver_rows.get("dnf", pd.Series(dtype=float)), errors="coerce").fillna(0).tolist()
        laps_led_list = pd.to_numeric(driver_rows.get("laps_led", pd.Series(dtype=float)), errors="coerce").fillna(0).tolist()
        avg_running_list = pd.to_numeric(driver_rows.get("avg_running_position", pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        n = len(driver_rows)

        return {
            "avg_finish": float(np.mean(finishes)) if finishes else 0.0,
            "finish_std": float(np.std(finishes)) if len(finishes) > 1 else 0.0,
            "podium_rate": float(sum(1 for f in finishes if f <= 3) / n),
            "dnf_rate": float(sum(dnf_list) / n),
            "avg_points": float(np.mean(points_list)) if points_list else 0.0,
            "win_rate": float(sum(1 for f in finishes if f == 1) / n),
            # positive = improving (lower finish number = better)
            "form_trend": float(np.polyfit(range(len(finishes)), finishes, 1)[0]) * -1.0 if len(finishes) >= 3 else 0.0,
            "avg_laps_led": float(np.mean(laps_led_list)) if laps_led_list else 0.0,
            "avg_running_pos": float(np.mean(avg_running_list)) if avg_running_list else 0.0,
        }

    def _driver_track_history(
        self,
        player_stats: pd.DataFrame,
        driver_id: str,
        circuit_id: str,
        date: Any,
        games_df: pd.DataFrame,
        window: int = 5,
    ) -> dict[str, float]:
        """Driver's history at a specific circuit, via player_stats + games join."""
        if player_stats.empty or not circuit_id or not driver_id:
            return {"track_avg_finish": 0.0, "track_races": 0}

        pid_col = next((c for c in ("player_id", "driver_id") if c in player_stats.columns), None)
        if pid_col is None:
            return {"track_avg_finish": 0.0, "track_races": 0}

        # Get game_ids for this circuit
        circuit_games = games_df[games_df.get("circuit_id", pd.Series("", index=games_df.index)) == circuit_id]
        if circuit_games.empty:
            return {"track_avg_finish": 0.0, "track_races": 0}

        id_col = next((c for c in ("id", "game_id") if c in circuit_games.columns), None)
        if id_col is None:
            return {"track_avg_finish": 0.0, "track_races": 0}

        circuit_ids = set(circuit_games[id_col].astype(str))
        game_id_col = "game_id" if "game_id" in player_stats.columns else None
        if game_id_col is None:
            return {"track_avg_finish": 0.0, "track_races": 0}

        mask = (player_stats[pid_col].astype(str) == driver_id) & \
               (player_stats[game_id_col].astype(str).isin(circuit_ids))
        hist = player_stats[mask]
        if "date" in hist.columns:
            hist = hist[hist["date"].astype(str) < str(date)]
        hist = hist.head(window)

        finishes = pd.to_numeric(hist.get("finish_position", pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        return {
            "track_avg_finish": float(np.mean(finishes)) if finishes else 0.0,
            "track_races": len(hist),
        }

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self.load_games(season)
        standings = self.load_team_stats(season)
        odds_df = self.load_odds(season)
        # Use pre-enriched player_stats passed from extract_all if available
        player_stats = game.pop("_player_stats", None)
        if player_stats is None:
            player_stats = self.load_player_stats(season)

        driver_id = str(game.get("home_team_id", game.get("driver_id", game.get("player_id", ""))))
        date = str(game.get("date", ""))
        # game_id = race_id + "_" + driver_id for uniqueness
        race_id = str(game.get("id", game.get("race_id", "")))
        game_id = f"{race_id}_{driver_id}" if driver_id else race_id
        circuit_id = str(game.get("circuit_id", ""))

        # Points scored in THIS race (set by extract_all before calling us)
        home_pts = float(pd.to_numeric(game.get("home_points", game.get("points", 0)), errors="coerce") or 0.0)
        home_finish = float(pd.to_numeric(game.get("home_finish_position", game.get("finish_position", 0)), errors="coerce") or 0.0)

        features: dict[str, Any] = {
            "game_id": game_id,
            "date": date,
            "home_team_id": driver_id,
            "away_team_id": str(game.get("constructor", "")),
            "home_score": home_pts,
            "away_score": 0.0,
        }

        # Grid position this race
        features["grid_position"] = float(
            pd.to_numeric(game.get("home_grid_position", game.get("grid_position", 0)), errors="coerce") or 0.0
        )
        features["q_time_ms"] = 0.0
        features["gap_to_pole_ms"] = 0.0
        features["q_position"] = features["grid_position"]

        # Historical race pace from player_stats (uses enriched dates)
        pace = self._driver_history_from_stats(player_stats, driver_id, date)
        features.update(pace)

        # Pit stops this race
        features["avg_pit_time_s"] = 0.0
        features["pit_stops"] = float(
            pd.to_numeric(game.get("home_pit_stops", game.get("pit_stops", 0)), errors="coerce") or 0.0
        )

        # Average speed this race (available in player_stats)
        features["avg_speed_kph"] = float(
            pd.to_numeric(game.get("home_avg_speed_kph", 0), errors="coerce") or 0.0
        )

        # Laps completed vs total — partial completion = DNF indicator
        total_laps = float(pd.to_numeric(game.get("total_laps", 0), errors="coerce") or 0.0)
        laps_done = float(pd.to_numeric(game.get("home_laps_completed", 0), errors="coerce") or 0.0)
        features["laps_completed"] = laps_done
        features["laps_completion_pct"] = float(laps_done / total_laps) if total_laps > 0 else 0.0

        # Constructor standings
        const = self._constructor_features(game, "home_", standings)
        features.update(const)

        # Track history from player_stats + games join
        track = self._driver_track_history(player_stats, driver_id, circuit_id, date, games_df)
        features.update(track)

        # Season championship points (cumulative before this race)
        features["driver_points"] = float(
            pd.to_numeric(game.get("home_driver_points", game.get("driver_points", 0)), errors="coerce") or 0.0
        )

        # DNF flag and fastest lap
        features["dnf"] = float(pd.to_numeric(game.get("home_dnf", 0), errors="coerce") or 0.0)
        features["fastest_lap"] = float(1.0 if game.get("home_fastest_lap") else 0.0)

        # Podium and points finish flags
        features["podium"] = float(1.0 if home_finish > 0 and home_finish <= 3 else 0.0)
        features["points_finish"] = float(1.0 if home_finish > 0 and home_finish <= 10 else 0.0)

        # Weather
        weather = self._weather_features(game)
        features.update(weather)

        # Tire strategy
        features["tire_compound"] = float(
            {"soft": 0, "medium": 1, "hard": 2, "intermediate": 3, "wet": 4}.get(
                str(game.get("home_tire_compound", "medium")).lower(), 1
            )
        )

        # Race-level context (new: round_number, total_laps, red_flag_count, race_pit_stops_total)
        features["safety_car_count"] = float(pd.to_numeric(game.get("safety_car_count", 0), errors="coerce") or 0.0)
        features["dnf_count"] = float(pd.to_numeric(game.get("dnf_count", 0), errors="coerce") or 0.0)
        features["round_number"] = float(pd.to_numeric(game.get("round_number", 0), errors="coerce") or 0.0)
        features["total_laps"] = total_laps
        features["red_flag_count"] = float(pd.to_numeric(game.get("red_flag_count", 0), errors="coerce") or 0.0)
        features["race_pit_stops_total"] = float(pd.to_numeric(game.get("race_pit_stops_total", 0), errors="coerce") or 0.0)

        # Odds (race-level, same for all drivers)
        odds = self._odds_features(race_id, odds_df)
        features.update(odds)

        return features

    def extract_all(
        self, season: int, *, existing_game_ids: set[str] | None = None,
    ) -> pd.DataFrame:
        """Override: expand each race into per-driver rows using player_stats."""
        games = self.load_games(season)
        player_stats = self.load_player_stats(season)

        if games.empty:
            logger.warning("No F1 races found for season %d", season)
            return pd.DataFrame()

        if player_stats.empty:
            logger.warning("No player_stats for %s season %d — using race-level features only", self.sport, season)
            # Fallback: extract race-level features without per-driver expansion
            status_col = "status" if "status" in games.columns else None
            if status_col:
                games = games[games[status_col].str.lower().isin(
                    {"final", "closed", "complete", "finished"}
                )]
            id_col = next((c for c in ("id", "game_id") if c in games.columns), None)
            if id_col is None or games.empty:
                return pd.DataFrame()
            features: list[dict[str, Any]] = []
            for _, race in games.iterrows():
                race_dict = race.to_dict()
                race_dict["season"] = season
                race_id = str(race_dict.get(id_col, ""))
                if existing_game_ids and race_id in existing_game_ids:
                    continue
                try:
                    f = self.extract_game_features(race_dict)
                    features.append(f)
                except Exception as e:
                    logger.debug("%s race %s: %s", self.sport, race_id, e)
            return pd.DataFrame(features) if features else pd.DataFrame()

        # Only process completed races
        status_col = "status" if "status" in games.columns else None
        if status_col:
            games = games[games[status_col].str.lower().isin(
                {"final", "closed", "complete", "finished"}
            )]

        id_col = next((c for c in ("id", "game_id") if c in games.columns), None)
        game_id_col = "game_id" if "game_id" in player_stats.columns else None

        if id_col is None or game_id_col is None:
            logger.error("F1: missing id columns — id_col=%s game_id_col=%s", id_col, game_id_col)
            return pd.DataFrame()

        # Build round-based id alias: {season}_{round_number} for games that use openf1_XXXX ids
        # player_stats use {season}_{round} format — build id→round_id and round_id→date maps
        round_col = "round_number" if "round_number" in games.columns else None
        date_col_g = "date" if "date" in games.columns else None
        round_id_map: dict[str, str] = {}  # openf1_id → season_round id
        date_map: dict[str, Any] = {}  # both id forms → date
        if round_col and date_col_g:
            for _, row in games.iterrows():
                raw_id = str(row.get(id_col, ""))
                rnd = row.get(round_col)
                date_val = row.get(date_col_g)
                round_id = f"{season}_{int(rnd)}" if pd.notna(rnd) else None
                if round_id:
                    round_id_map[raw_id] = round_id
                    date_map[round_id] = date_val
                date_map[raw_id] = date_val

        # Enrich player_stats with race dates for correct rolling history
        if "date" not in player_stats.columns or player_stats["date"].isna().all():
            player_stats = player_stats.copy()
            player_stats["date"] = player_stats[game_id_col].map(date_map)

        features: list[dict[str, Any]] = []
        success, failed = 0, 0

        for _, race in games.iterrows():
            race_dict = race.to_dict()
            race_dict["season"] = season
            race_id = str(race_dict.get(id_col, ""))

            # Incremental: skip already-extracted races
            if existing_game_ids and race_id in existing_game_ids:
                continue

            round_id = round_id_map.get(race_id, race_id)

            # Find all drivers in this race — try both raw id and round-based id
            race_drivers = player_stats[player_stats[game_id_col].astype(str) == race_id]
            if race_drivers.empty:
                race_drivers = player_stats[player_stats[game_id_col].astype(str) == round_id]
            if race_drivers.empty:
                logger.debug("F1: no player_stats for race %s / %s", race_id, round_id)
                continue

            pid_col = next((c for c in ("player_id", "driver_id") if c in race_drivers.columns), None)
            if pid_col is None:
                continue

            for _, driver in race_drivers.iterrows():
                driver_dict = {**race_dict}
                driver_dict["id"] = race_id
                driver_dict["race_id"] = race_id
                driver_dict["home_team_id"] = str(driver.get(pid_col, ""))
                driver_dict["home_team"] = str(driver.get("player_name", ""))
                driver_dict["constructor"] = str(driver.get("constructor", driver.get("team_id", "")))
                driver_dict["home_finish_position"] = float(driver.get("finish_position") or 0)
                driver_dict["home_grid_position"] = float(driver.get("grid_position") or 0)
                driver_dict["home_points"] = float(driver.get("points") or 0)
                driver_dict["home_dnf"] = float(driver.get("dnf") or 0)
                driver_dict["home_fastest_lap"] = float(1 if driver.get("fastest_lap") else 0)
                driver_dict["home_pit_stops"] = float(driver.get("pit_stops") or 0)
                driver_dict["home_avg_speed_kph"] = float(driver.get("avg_speed_kph") or 0)
                driver_dict["home_laps_completed"] = float(driver.get("laps") or 0)
                driver_dict["home_laps_led"] = float(driver.get("laps_led") or 0)
                driver_dict["home_avg_running_position"] = float(driver.get("avg_running_position") or 0)
                # Race-level context from games_df
                driver_dict["round_number"] = float(race_dict.get("round_number") or 0)
                driver_dict["total_laps"] = float(race_dict.get("total_laps") or 0)
                driver_dict["red_flag_count"] = float(race_dict.get("red_flag_count") or 0)
                driver_dict["race_pit_stops_total"] = float(race_dict.get("pit_stops_total") or 0)
                # Labels
                driver_dict["home_score"] = driver_dict["home_points"]
                driver_dict["away_score"] = 0.0
                # Pass enriched player_stats for correct rolling history
                driver_dict["_player_stats"] = player_stats

                try:
                    f = self.extract_game_features(driver_dict)
                    features.append(f)
                    success += 1
                except Exception as e:
                    failed += 1
                    logger.warning("F1 driver %s race %s: %s", driver_dict.get("home_team"), race_id, e)

        logger.info("F1 season %d: %d driver-race rows (%d failed)", season, success, failed)
        return pd.DataFrame(features) if features else pd.DataFrame()

    def get_feature_names(self) -> list[str]:
        # NOTE: current-race outcome columns (podium, points_finish, dnf, fastest_lap,
        # laps_completed, laps_completion_pct, avg_speed_kph, pit_stops, avg_pit_time_s,
        # safety_car_count, dnf_count, red_flag_count, race_pit_stops_total) are excluded
        # from features — they are targets or unknown before the race.
        return [
            # Qualifying/Grid (available before race)
            "grid_position", "q_time_ms", "gap_to_pole_ms", "q_position",
            # Historical race pace
            "avg_finish", "finish_std", "podium_rate", "dnf_rate", "avg_points", "win_rate", "form_trend",
            "avg_laps_led", "avg_running_pos",
            # Constructor standings (before race)
            "constructor_points", "constructor_position",
            # Track history
            "track_avg_finish", "track_races",
            # Championship points before race
            "driver_points",
            # Weather (available before race or at start)
            "temperature", "is_wet", "track_temperature",
            # Tire strategy (announced pre-race for opening stint)
            "tire_compound",
            # Race calendar context (known pre-race)
            "round_number", "total_laps",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
        ]
