"""AutoBet configuration with environment variable overrides.

Every setting can be overridden via an environment variable with the
``AUTOBET_`` prefix.  For example ``AUTOBET_ENABLED=true`` enables the bot,
``AUTOBET_KELLY_FRACTION=0.20`` adjusts the Kelly fraction, etc.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any


def _bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _float(key: str, default: float) -> float:
    v = os.getenv(key)
    return float(v) if v is not None else default


def _int(key: str, default: int) -> int:
    v = os.getenv(key)
    return int(v) if v is not None else default


def _str(key: str, default: str) -> str:
    return os.getenv(key, default)


def _list(key: str, default: list[str]) -> list[str]:
    v = os.getenv(key)
    if v is None:
        return default
    return [s.strip() for s in v.split(",") if s.strip()]


def _set(key: str, default: set[str]) -> set[str]:
    v = os.getenv(key)
    if v is None:
        return default
    return {s.strip().lower() for s in v.split(",") if s.strip()}


def _json_dict(key: str, default: dict) -> dict:
    v = os.getenv(key)
    if v is None:
        return default
    try:
        return json.loads(v)
    except (json.JSONDecodeError, TypeError):
        return default


def _tuple_int(key: str, default: tuple[int, int]) -> tuple[int, int]:
    v = os.getenv(key)
    if v is None:
        return default
    try:
        parts = [int(x.strip()) for x in v.split(",")]
        return (parts[0], parts[1])
    except (ValueError, IndexError):
        return default


@dataclass
class AutobetConfig:
    """Complete configuration for the autobet paper-trading bot."""

    # ── Core ────────────────────────────────────────────────────────────
    enabled: bool = True
    sports: list[str] = field(
        default_factory=lambda: [
            # Basketball — high accuracy (75-90%)
            "nba", "wnba", "ncaab", "ncaaw",
            # Football — high accuracy (75-86%)
            "nfl", "ncaaf",
            # Hockey / Baseball
            "nhl", "mlb",
            # Soccer — good accuracy (65-80%)
            "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl",
            # Combat / Tennis
            "ufc", "atp", "wta",
            # Esports
            "csgo", "dota2", "lol", "valorant",
        ]
    )
    backend_url: str = "http://localhost:8000"
    internal_api_key: str = ""

    # ── Timing ──────────────────────────────────────────────────────────
    betting_cycle_seconds: int = 300  # 5 minutes
    grading_cycle_seconds: int = 1800  # 30 minutes
    timing_mode: str = "same_day"  # "window" | "same_day" | "anytime"
    pregame_window_minutes: tuple[int, int] = (60, 90)  # for "window" mode
    allow_live: bool = False
    prediction_max_age_hours: float = 18.0

    # ── Thresholds — Global ─────────────────────────────────────────────
    min_confidence: float = 0.65
    min_edge: float = 0.10

    # ── Thresholds — Per sport overrides ────────────────────────────────
    min_confidence_by_sport: dict[str, float] = field(
        default_factory=lambda: {
            # Basketball — strong models
            "nba": 0.65, "wnba": 0.64, "ncaab": 0.68, "ncaaw": 0.70,
            # Football
            "nfl": 0.65, "ncaaf": 0.68,
            # Hockey / Baseball
            "nhl": 0.63, "mlb": 0.60,
            # Soccer — decent accuracy
            "epl": 0.63, "laliga": 0.63, "bundesliga": 0.66,
            "seriea": 0.63, "ligue1": 0.63, "mls": 0.63,
            "ucl": 0.62, "nwsl": 0.61,
            # Combat / Tennis — lower accuracy, require higher confidence
            "ufc": 0.65, "atp": 0.65, "wta": 0.65,
            # Esports — lower accuracy, require higher confidence
            "csgo": 0.67, "dota2": 0.64, "lol": 0.64, "valorant": 0.64,
        }
    )
    min_edge_by_sport: dict[str, float] = field(
        default_factory=lambda: {
            "nba": 0.10, "wnba": 0.09, "ncaab": 0.10, "ncaaw": 0.10,
            "nfl": 0.10, "ncaaf": 0.10,
            "nhl": 0.09, "mlb": 0.08,
            "epl": 0.09, "laliga": 0.09, "bundesliga": 0.09,
            "seriea": 0.09, "ligue1": 0.09, "mls": 0.09,
            "ucl": 0.08, "nwsl": 0.08,
            "ufc": 0.12, "atp": 0.12, "wta": 0.12,
            "csgo": 0.13, "dota2": 0.11, "lol": 0.11, "valorant": 0.11,
        }
    )

    # ── Thresholds — Per bet type ───────────────────────────────────────
    min_confidence_by_bet_type: dict[str, float] = field(
        default_factory=lambda: {
            "winner": 0.65,
            "spread": 0.58,
            "total": 0.55,
            "prop": 0.65,
            "draw": 0.60,           # draw needs real probability (not model outputting ~0)
            "overtime": 0.62,
            "halftime_winner": 0.62,
            "esports_clean_sweep": 0.65,
            "esports_map_total": 0.62,
        }
    )
    min_edge_by_bet_type: dict[str, float] = field(
        default_factory=lambda: {
            "winner": 0.10,
            "spread": 0.06,
            "total": 0.04,
            "prop": 0.10,
            "draw": 0.08,
            "overtime": 0.08,
            "halftime_winner": 0.07,
            "esports_clean_sweep": 0.10,
            "esports_map_total": 0.08,
        }
    )

    # ── 5-Tier Conviction System ────────────────────────────────────────
    # Tiers: S (Platinum) > A (Gold) > B (Silver) > C (Bronze) > D (Copper)
    # Each tier has confidence + edge thresholds and a Kelly multiplier.
    # Bets below Tier D minimums are rejected entirely.
    tier_s_confidence: float = 0.82   # S: Platinum — maximum conviction
    tier_s_edge: float = 0.18
    tier_a_confidence: float = 0.70   # A: Gold — high conviction
    tier_a_edge: float = 0.13
    tier_b_confidence: float = 0.62   # B: Silver — moderate conviction
    tier_b_edge: float = 0.08
    tier_c_confidence: float = 0.55   # C: Bronze — low-moderate conviction
    tier_c_edge: float = 0.05
    tier_d_confidence: float = 0.52   # D: Copper — minimum threshold
    tier_d_edge: float = 0.03

    # Kept for backward-compat / env-var fallback
    min_conviction_confidence: float = 0.70
    min_conviction_edge: float = 0.13

    # Kelly fraction multipliers per tier (applied on top of kelly_fraction)
    tier_s_kelly_mult: float = 1.00   # Full allowed Kelly
    tier_a_kelly_mult: float = 0.80
    tier_b_kelly_mult: float = 0.60
    tier_c_kelly_mult: float = 0.40
    tier_d_kelly_mult: float = 0.25

    # ── Bankroll & Sizing ───────────────────────────────────────────────
    bankroll_dollars: float = 100.0
    dynamic_bankroll: bool = True
    kelly_fraction: float = 0.25  # Quarter-Kelly base
    min_stake_units: float = 0.50
    max_stake_units: float = 5.00
    tier_b_fraction: float = 0.60  # legacy — tier_b_kelly_mult takes precedence

    # ── Parlays ─────────────────────────────────────────────────────────
    parlay_enabled: bool = True
    min_parlay_legs: int = 2
    max_parlay_legs: int = 4
    min_parlay_confidence: float = 0.72
    min_parlay_edge: float = 0.17
    parlay_min_combined_confidence: float = 0.58
    parlay_max_prop_legs: int = 2
    parlay_max_same_direction_fraction: float = 0.67
    parlay_hit_rate_weight: float = 0.72
    parlay_leg_penalties: dict[int, float] = field(
        default_factory=lambda: {2: 1.00, 3: 0.85, 4: 0.72, 5: 0.58}
    )

    # ── Lotto ───────────────────────────────────────────────────────────
    lotto_enabled: bool = True
    lotto_once_per_day: bool = True
    lotto_min_confidence: float = 0.58
    lotto_min_edge: float = 0.12
    lotto_min_legs: int = 3
    lotto_max_legs: int = 5
    lotto_stake_units: float = 1.0

    # ── Ladder ──────────────────────────────────────────────────────────
    ladder_enabled: bool = True
    ladder_once_per_day: bool = True
    ladder_base_stake: float = 1.0
    ladder_max_rungs: int = 5
    ladder_min_confidence: float = 0.65
    ladder_min_edge: float = 0.10
    ladder_min_payout_multiplier: float = 2.0

    # ── Limits ──────────────────────────────────────────────────────────
    max_bets_per_sport: int = 3
    max_parlays_per_day: int = 6
    max_props_per_day: int = 20

    # ── Dynamic thresholds ──────────────────────────────────────────────
    dynamic_thresholds_enabled: bool = True
    dynamic_lookback_bets: int = 50
    dynamic_adjustment_cap: float = 0.05  # max ±5% shift
    target_win_rate: dict[str, float] = field(
        default_factory=lambda: {
            "winner": 0.56,
            "spread": 0.54,
            "total": 0.53,
        }
    )

    # ── Vendors ─────────────────────────────────────────────────────────
    excluded_vendors: set[str] = field(
        default_factory=lambda: {"polymarket", "kalshi", "fallback"}
    )

    # ── Logging / Alerts ────────────────────────────────────────────────
    log_level: str = "INFO"
    discord_webhook_url: str = ""
    discord_enabled: bool = False

    @classmethod
    def from_env(cls) -> AutobetConfig:
        """Build config from environment variables with ``AUTOBET_`` prefix."""
        # Build a defaults instance to pull default values from (needed for
        # fields that use default_factory which aren't class-level attrs).
        d = cls()
        return cls(
            enabled=_bool("AUTOBET_ENABLED", d.enabled),
            sports=_list("AUTOBET_SPORTS", d.sports),
            backend_url=_str("AUTOBET_BACKEND_URL", d.backend_url),
            internal_api_key=_str("AUTOBET_INTERNAL_API_KEY", d.internal_api_key),
            betting_cycle_seconds=_int(
                "AUTOBET_BETTING_CYCLE_SECONDS", d.betting_cycle_seconds
            ),
            grading_cycle_seconds=_int(
                "AUTOBET_GRADING_CYCLE_SECONDS", d.grading_cycle_seconds
            ),
            timing_mode=_str("AUTOBET_TIMING_MODE", d.timing_mode),
            pregame_window_minutes=_tuple_int(
                "AUTOBET_PREGAME_WINDOW_MINUTES", d.pregame_window_minutes
            ),
            allow_live=_bool("AUTOBET_ALLOW_LIVE", d.allow_live),
            prediction_max_age_hours=_float(
                "AUTOBET_PREDICTION_MAX_AGE_HOURS", d.prediction_max_age_hours
            ),
            min_confidence=_float("AUTOBET_MIN_CONFIDENCE", d.min_confidence),
            min_edge=_float("AUTOBET_MIN_EDGE", d.min_edge),
            min_confidence_by_sport=_json_dict(
                "AUTOBET_MIN_CONFIDENCE_BY_SPORT",
                d.min_confidence_by_sport,
            ),
            min_edge_by_sport=_json_dict(
                "AUTOBET_MIN_EDGE_BY_SPORT",
                d.min_edge_by_sport,
            ),
            min_confidence_by_bet_type=_json_dict(
                "AUTOBET_MIN_CONFIDENCE_BY_BET_TYPE",
                d.min_confidence_by_bet_type,
            ),
            min_edge_by_bet_type=_json_dict(
                "AUTOBET_MIN_EDGE_BY_BET_TYPE",
                d.min_edge_by_bet_type,
            ),
            min_conviction_confidence=_float(
                "AUTOBET_MIN_CONVICTION_CONFIDENCE", d.min_conviction_confidence
            ),
            min_conviction_edge=_float(
                "AUTOBET_MIN_CONVICTION_EDGE", d.min_conviction_edge
            ),
            tier_s_confidence=_float("AUTOBET_TIER_S_CONFIDENCE", d.tier_s_confidence),
            tier_s_edge=_float("AUTOBET_TIER_S_EDGE", d.tier_s_edge),
            tier_a_confidence=_float("AUTOBET_TIER_A_CONFIDENCE", d.tier_a_confidence),
            tier_a_edge=_float("AUTOBET_TIER_A_EDGE", d.tier_a_edge),
            tier_b_confidence=_float("AUTOBET_TIER_B_CONFIDENCE", d.tier_b_confidence),
            tier_b_edge=_float("AUTOBET_TIER_B_EDGE", d.tier_b_edge),
            tier_c_confidence=_float("AUTOBET_TIER_C_CONFIDENCE", d.tier_c_confidence),
            tier_c_edge=_float("AUTOBET_TIER_C_EDGE", d.tier_c_edge),
            tier_d_confidence=_float("AUTOBET_TIER_D_CONFIDENCE", d.tier_d_confidence),
            tier_d_edge=_float("AUTOBET_TIER_D_EDGE", d.tier_d_edge),
            tier_s_kelly_mult=_float("AUTOBET_TIER_S_KELLY_MULT", d.tier_s_kelly_mult),
            tier_a_kelly_mult=_float("AUTOBET_TIER_A_KELLY_MULT", d.tier_a_kelly_mult),
            tier_b_kelly_mult=_float("AUTOBET_TIER_B_KELLY_MULT", d.tier_b_kelly_mult),
            tier_c_kelly_mult=_float("AUTOBET_TIER_C_KELLY_MULT", d.tier_c_kelly_mult),
            tier_d_kelly_mult=_float("AUTOBET_TIER_D_KELLY_MULT", d.tier_d_kelly_mult),
            bankroll_dollars=_float("AUTOBET_BANKROLL_DOLLARS", d.bankroll_dollars),
            dynamic_bankroll=_bool("AUTOBET_DYNAMIC_BANKROLL", d.dynamic_bankroll),
            kelly_fraction=_float("AUTOBET_KELLY_FRACTION", d.kelly_fraction),
            min_stake_units=_float("AUTOBET_MIN_STAKE_UNITS", d.min_stake_units),
            max_stake_units=_float("AUTOBET_MAX_STAKE_UNITS", d.max_stake_units),
            tier_b_fraction=_float("AUTOBET_TIER_B_FRACTION", d.tier_b_fraction),
            parlay_enabled=_bool("AUTOBET_PARLAY_ENABLED", d.parlay_enabled),
            min_parlay_legs=_int("AUTOBET_MIN_PARLAY_LEGS", d.min_parlay_legs),
            max_parlay_legs=_int("AUTOBET_MAX_PARLAY_LEGS", d.max_parlay_legs),
            min_parlay_confidence=_float(
                "AUTOBET_MIN_PARLAY_CONFIDENCE", d.min_parlay_confidence
            ),
            min_parlay_edge=_float("AUTOBET_MIN_PARLAY_EDGE", d.min_parlay_edge),
            parlay_min_combined_confidence=_float(
                "AUTOBET_PARLAY_MIN_COMBINED_CONFIDENCE",
                d.parlay_min_combined_confidence,
            ),
            parlay_max_prop_legs=_int(
                "AUTOBET_PARLAY_MAX_PROP_LEGS", d.parlay_max_prop_legs
            ),
            parlay_max_same_direction_fraction=_float(
                "AUTOBET_PARLAY_MAX_SAME_DIRECTION_FRACTION",
                d.parlay_max_same_direction_fraction,
            ),
            parlay_hit_rate_weight=_float(
                "AUTOBET_PARLAY_HIT_RATE_WEIGHT", d.parlay_hit_rate_weight
            ),
            parlay_leg_penalties=_json_dict(
                "AUTOBET_PARLAY_LEG_PENALTIES",
                d.parlay_leg_penalties,
            ),
            lotto_enabled=_bool("AUTOBET_LOTTO_ENABLED", d.lotto_enabled),
            lotto_once_per_day=_bool(
                "AUTOBET_LOTTO_ONCE_PER_DAY", d.lotto_once_per_day
            ),
            lotto_min_confidence=_float(
                "AUTOBET_LOTTO_MIN_CONFIDENCE", d.lotto_min_confidence
            ),
            lotto_min_edge=_float("AUTOBET_LOTTO_MIN_EDGE", d.lotto_min_edge),
            lotto_min_legs=_int("AUTOBET_LOTTO_MIN_LEGS", d.lotto_min_legs),
            lotto_max_legs=_int("AUTOBET_LOTTO_MAX_LEGS", d.lotto_max_legs),
            lotto_stake_units=_float(
                "AUTOBET_LOTTO_STAKE_UNITS", d.lotto_stake_units
            ),
            ladder_enabled=_bool("AUTOBET_LADDER_ENABLED", d.ladder_enabled),
            ladder_once_per_day=_bool(
                "AUTOBET_LADDER_ONCE_PER_DAY", d.ladder_once_per_day
            ),
            ladder_base_stake=_float(
                "AUTOBET_LADDER_BASE_STAKE", d.ladder_base_stake
            ),
            ladder_max_rungs=_int("AUTOBET_LADDER_MAX_RUNGS", d.ladder_max_rungs),
            ladder_min_confidence=_float(
                "AUTOBET_LADDER_MIN_CONFIDENCE", d.ladder_min_confidence
            ),
            ladder_min_edge=_float("AUTOBET_LADDER_MIN_EDGE", d.ladder_min_edge),
            ladder_min_payout_multiplier=_float(
                "AUTOBET_LADDER_MIN_PAYOUT_MULTIPLIER",
                d.ladder_min_payout_multiplier,
            ),
            max_bets_per_sport=_int(
                "AUTOBET_MAX_BETS_PER_SPORT", d.max_bets_per_sport
            ),
            max_parlays_per_day=_int(
                "AUTOBET_MAX_PARLAYS_PER_DAY", d.max_parlays_per_day
            ),
            max_props_per_day=_int("AUTOBET_MAX_PROPS_PER_DAY", d.max_props_per_day),
            dynamic_thresholds_enabled=_bool(
                "AUTOBET_DYNAMIC_THRESHOLDS_ENABLED", d.dynamic_thresholds_enabled
            ),
            dynamic_lookback_bets=_int(
                "AUTOBET_DYNAMIC_LOOKBACK_BETS", d.dynamic_lookback_bets
            ),
            dynamic_adjustment_cap=_float(
                "AUTOBET_DYNAMIC_ADJUSTMENT_CAP", d.dynamic_adjustment_cap
            ),
            target_win_rate=_json_dict(
                "AUTOBET_TARGET_WIN_RATE",
                d.target_win_rate,
            ),
            excluded_vendors=_set("AUTOBET_EXCLUDED_VENDORS", d.excluded_vendors),
            log_level=_str("AUTOBET_LOG_LEVEL", d.log_level),
            discord_webhook_url=_str(
                "AUTOBET_DISCORD_WEBHOOK_URL", d.discord_webhook_url
            ),
            discord_enabled=_bool("AUTOBET_DISCORD_ENABLED", d.discord_enabled),
        )

    def get_min_confidence(self, sport: str, bet_type: str) -> float:
        """Resolve minimum confidence for a sport + bet_type pair."""
        by_type = self.min_confidence_by_bet_type.get(bet_type)
        by_sport = self.min_confidence_by_sport.get(sport)
        if by_type is not None and by_sport is not None:
            return max(by_type, by_sport)
        return by_type or by_sport or self.min_confidence

    def get_min_edge(self, sport: str, bet_type: str) -> float:
        """Resolve minimum edge for a sport + bet_type pair."""
        by_type = self.min_edge_by_bet_type.get(bet_type)
        by_sport = self.min_edge_by_sport.get(sport)
        if by_type is not None and by_sport is not None:
            return max(by_type, by_sport)
        return by_type or by_sport or self.min_edge
