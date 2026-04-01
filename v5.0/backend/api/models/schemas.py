# ──────────────────────────────────────────────────────────
# V5.0 Backend — Canonical Pydantic V2 Schemas
# ──────────────────────────────────────────────────────────
#
# Every normalized data type flowing through the pipeline
# and returned by the API is defined here.  Sport-specific
# player stats use a discriminated union keyed on `category`.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import datetime as _dt
from datetime import date, datetime
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ── Shared helpers ────────────────────────────────────────

def _coerce_str(v: object) -> str:
    """Accept int/float IDs and coerce to str."""
    if v is None:
        return v  # type: ignore[return-value]
    return str(v)


class _Base(BaseModel):
    """Project-wide base: frozen, populate-by-name, strip whitespace."""

    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        from_attributes=True,
    )

    source: str = Field(
        default="unknown",
        description="Provenance tag identifying which provider produced this record.",
    )


# ── Game ──────────────────────────────────────────────────

class Game(_Base):
    id: str = Field(..., description="Globally unique game identifier.")
    sport: str
    season: str
    date: _dt.date
    status: str = Field(
        default="scheduled",
        description="scheduled | in_progress | final | postponed | cancelled",
    )
    home_team: str
    away_team: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_team_id: Optional[str] = None
    away_team_id: Optional[str] = None
    venue: Optional[str] = None
    attendance: Optional[int] = None
    weather: Optional[str] = None
    broadcast: Optional[str] = None
    start_time: Optional[datetime] = None
    period: Optional[str] = None
    is_neutral_site: bool = False

    # Quarter/period scores (basketball, football, hockey)
    home_q1: Optional[int] = None
    home_q2: Optional[int] = None
    home_q3: Optional[int] = None
    home_q4: Optional[int] = None
    home_ot: Optional[int] = None
    away_q1: Optional[int] = None
    away_q2: Optional[int] = None
    away_q3: Optional[int] = None
    away_q4: Optional[int] = None
    away_ot: Optional[int] = None

    # Hockey period scores (3 periods + OT)
    home_p1: Optional[int] = None
    home_p2: Optional[int] = None
    home_p3: Optional[int] = None
    away_p1: Optional[int] = None
    away_p2: Optional[int] = None
    away_p3: Optional[int] = None

    # Inning scores (MLB — 9 innings + extras)
    home_i1: Optional[int] = None
    home_i2: Optional[int] = None
    home_i3: Optional[int] = None
    home_i4: Optional[int] = None
    home_i5: Optional[int] = None
    home_i6: Optional[int] = None
    home_i7: Optional[int] = None
    home_i8: Optional[int] = None
    home_i9: Optional[int] = None
    home_extras: Optional[int] = None
    away_i1: Optional[int] = None
    away_i2: Optional[int] = None
    away_i3: Optional[int] = None
    away_i4: Optional[int] = None
    away_i5: Optional[int] = None
    away_i6: Optional[int] = None
    away_i7: Optional[int] = None
    away_i8: Optional[int] = None
    away_i9: Optional[int] = None
    away_extras: Optional[int] = None

    # Half scores (college basketball, soccer, WNBA)
    home_h1_score: Optional[int] = None
    home_h2_score: Optional[int] = None
    away_h1_score: Optional[int] = None
    away_h2_score: Optional[int] = None

    # Per-game team stats
    home_rebounds: Optional[float] = None
    home_assists: Optional[float] = None
    home_fg_pct: Optional[float] = None
    home_ft_pct: Optional[float] = None
    home_three_pct: Optional[float] = None
    home_fgm: Optional[float] = None
    home_fga: Optional[float] = None
    home_ftm: Optional[float] = None
    home_fta: Optional[float] = None
    home_three_m: Optional[float] = None
    home_three_a: Optional[float] = None
    home_three_pointers_made: Optional[float] = None
    home_three_pointers_attempted: Optional[float] = None
    home_free_throws_made: Optional[float] = None
    home_free_throws_attempted: Optional[float] = None
    home_turnovers: Optional[float] = None
    home_steals: Optional[float] = None
    home_blocks: Optional[float] = None
    home_fouls: Optional[float] = None
    home_offensive_rebounds: Optional[float] = None
    home_defensive_rebounds: Optional[float] = None
    home_fast_break_points: Optional[float] = None
    home_points_in_paint: Optional[float] = None
    home_turnover_points: Optional[float] = None
    home_second_chance_points: Optional[float] = None
    home_largest_lead: Optional[float] = None
    home_technical_fouls: Optional[float] = None
    home_flagrant_fouls: Optional[float] = None
    away_rebounds: Optional[float] = None
    away_assists: Optional[float] = None
    away_fg_pct: Optional[float] = None
    away_ft_pct: Optional[float] = None
    away_three_pct: Optional[float] = None
    away_fgm: Optional[float] = None
    away_fga: Optional[float] = None
    away_ftm: Optional[float] = None
    away_fta: Optional[float] = None
    away_three_m: Optional[float] = None
    away_three_a: Optional[float] = None
    away_three_pointers_made: Optional[float] = None
    away_three_pointers_attempted: Optional[float] = None
    away_free_throws_made: Optional[float] = None
    away_free_throws_attempted: Optional[float] = None
    away_turnovers: Optional[float] = None
    away_steals: Optional[float] = None
    away_blocks: Optional[float] = None
    away_fouls: Optional[float] = None
    away_offensive_rebounds: Optional[float] = None
    away_defensive_rebounds: Optional[float] = None
    away_fast_break_points: Optional[float] = None
    away_points_in_paint: Optional[float] = None
    away_turnover_points: Optional[float] = None
    away_second_chance_points: Optional[float] = None
    away_largest_lead: Optional[float] = None
    away_technical_fouls: Optional[float] = None
    away_flagrant_fouls: Optional[float] = None

    # NFL-specific team stats
    home_first_downs: Optional[float] = None
    home_first_downs_passing: Optional[float] = None
    home_first_downs_rushing: Optional[float] = None
    home_total_plays: Optional[float] = None
    home_total_yards: Optional[float] = None
    home_passing_yards: Optional[float] = None
    home_rushing_yards: Optional[float] = None
    home_rushing_attempts: Optional[float] = None
    home_completions: Optional[float] = None
    home_pass_attempts: Optional[float] = None
    home_fumbles_lost: Optional[float] = None
    home_penalties: Optional[float] = None
    home_penalty_yards: Optional[float] = None
    home_third_down_conv: Optional[float] = None
    home_third_down_att: Optional[float] = None
    home_fourth_down_conv: Optional[float] = None
    home_fourth_down_att: Optional[float] = None
    home_possession_seconds: Optional[float] = None
    home_sacks_allowed: Optional[float] = None
    home_yards_per_play: Optional[float] = None
    home_passing_touchdowns: Optional[float] = None
    home_rushing_touchdowns: Optional[float] = None
    home_interceptions_thrown: Optional[float] = None
    home_receiving_yards: Optional[float] = None
    home_sacks: Optional[float] = None
    home_tackles: Optional[float] = None
    home_field_goals_made: Optional[float] = None
    home_field_goals_attempted: Optional[float] = None
    home_extra_points_made: Optional[float] = None
    home_extra_points_attempted: Optional[float] = None
    home_punt_yards: Optional[float] = None
    home_completion_pct: Optional[float] = None
    home_third_down_pct: Optional[float] = None
    away_first_downs: Optional[float] = None
    away_first_downs_passing: Optional[float] = None
    away_first_downs_rushing: Optional[float] = None
    away_total_plays: Optional[float] = None
    away_total_yards: Optional[float] = None
    away_passing_yards: Optional[float] = None
    away_rushing_yards: Optional[float] = None
    away_rushing_attempts: Optional[float] = None
    away_completions: Optional[float] = None
    away_pass_attempts: Optional[float] = None
    away_fumbles_lost: Optional[float] = None
    away_penalties: Optional[float] = None
    away_penalty_yards: Optional[float] = None
    away_third_down_conv: Optional[float] = None
    away_third_down_att: Optional[float] = None
    away_fourth_down_conv: Optional[float] = None
    away_fourth_down_att: Optional[float] = None
    away_possession_seconds: Optional[float] = None
    away_sacks_allowed: Optional[float] = None
    away_yards_per_play: Optional[float] = None
    away_passing_touchdowns: Optional[float] = None
    away_rushing_touchdowns: Optional[float] = None
    away_interceptions_thrown: Optional[float] = None
    away_receiving_yards: Optional[float] = None
    away_sacks: Optional[float] = None
    away_tackles: Optional[float] = None
    away_field_goals_made: Optional[float] = None
    away_field_goals_attempted: Optional[float] = None
    away_extra_points_made: Optional[float] = None
    away_extra_points_attempted: Optional[float] = None
    away_punt_yards: Optional[float] = None
    away_completion_pct: Optional[float] = None
    away_third_down_pct: Optional[float] = None

    # NHL-specific team stats
    home_penalty_minutes: Optional[float] = None
    home_power_play_goals: Optional[float] = None
    home_power_play_pct: Optional[float] = None
    home_penalty_kill_pct: Optional[float] = None
    home_shots_on_goal: Optional[float] = None
    home_penalty_count: Optional[float] = None
    away_penalty_minutes: Optional[float] = None
    away_power_play_goals: Optional[float] = None
    away_power_play_pct: Optional[float] = None
    away_penalty_kill_pct: Optional[float] = None
    away_shots_on_goal: Optional[float] = None
    away_penalty_count: Optional[float] = None
    home_blocked_shots: Optional[float] = None
    home_hits_nhl: Optional[float] = None
    home_takeaways: Optional[float] = None
    home_giveaways: Optional[float] = None
    home_faceoffs_won: Optional[float] = None
    home_faceoffs_lost: Optional[float] = None
    away_blocked_shots: Optional[float] = None
    away_hits_nhl: Optional[float] = None
    away_takeaways: Optional[float] = None
    away_giveaways: Optional[float] = None
    away_faceoffs_won: Optional[float] = None
    away_faceoffs_lost: Optional[float] = None
    home_faceoff_pct: Optional[float] = None
    away_faceoff_pct: Optional[float] = None
    home_shorthanded_goals: Optional[float] = None
    away_shorthanded_goals: Optional[float] = None
    home_shootout_goals: Optional[float] = None
    away_shootout_goals: Optional[float] = None
    home_power_play_attempts: Optional[float] = None
    away_power_play_attempts: Optional[float] = None
    home_penalties_nhl: Optional[float] = None
    away_penalties_nhl: Optional[float] = None

    # NFL-specific additional stats
    home_total_drives: Optional[float] = None
    away_total_drives: Optional[float] = None
    home_defensive_tds: Optional[float] = None
    away_defensive_tds: Optional[float] = None
    home_fourth_down_pct: Optional[float] = None
    away_fourth_down_pct: Optional[float] = None
    home_red_zone_pct: Optional[float] = None
    away_red_zone_pct: Optional[float] = None

    # NFL EPA (Expected Points Added) from nflfastr
    home_passing_epa: Optional[float] = None
    away_passing_epa: Optional[float] = None
    home_rushing_epa: Optional[float] = None
    away_rushing_epa: Optional[float] = None
    home_total_epa: Optional[float] = None
    away_total_epa: Optional[float] = None
    home_air_yards: Optional[float] = None
    away_air_yards: Optional[float] = None
    home_yac: Optional[float] = None
    away_yac: Optional[float] = None

    # NBA advanced derived stats
    home_true_shooting_pct: Optional[float] = None
    away_true_shooting_pct: Optional[float] = None
    home_effective_fg_pct: Optional[float] = None
    away_effective_fg_pct: Optional[float] = None
    home_possessions: Optional[float] = None
    away_possessions: Optional[float] = None

    # NHL derived stats
    home_save_pct: Optional[float] = None
    away_save_pct: Optional[float] = None
    home_shooting_pct: Optional[float] = None
    away_shooting_pct: Optional[float] = None
    # NHL goalie stats (home_saves/away_saves shared with soccer above)
    home_shots_against: Optional[float] = None
    away_shots_against: Optional[float] = None
    home_goals_against: Optional[float] = None
    away_goals_against: Optional[float] = None

    # Dota2 draft data
    home_heroes: Optional[str] = None      # comma-separated hero IDs (picks)
    away_heroes: Optional[str] = None
    home_bans: Optional[str] = None        # comma-separated hero IDs (bans)
    away_bans: Optional[str] = None
    gold_adv_10: Optional[float] = None    # radiant gold advantage at 10 min
    gold_adv_20: Optional[float] = None
    gold_adv_30: Optional[float] = None
    xp_adv_10: Optional[float] = None
    xp_adv_20: Optional[float] = None
    xp_adv_30: Optional[float] = None

    # MLB-specific batting stats
    home_at_bats: Optional[float] = None
    home_runs: Optional[float] = None
    home_runs_scored: Optional[float] = None
    home_hits: Optional[float] = None
    home_rbis: Optional[float] = None
    home_rbi: Optional[float] = None
    home_home_runs: Optional[float] = None
    home_walks: Optional[float] = None
    home_strikeouts: Optional[float] = None
    home_stolen_bases: Optional[float] = None
    home_batting_avg: Optional[float] = None
    home_obp: Optional[float] = None
    home_slg: Optional[float] = None
    home_earned_runs: Optional[float] = None
    home_pitches: Optional[float] = None
    home_strikes: Optional[float] = None
    home_pitching_walks: Optional[float] = None
    home_pitching_strikeouts: Optional[float] = None
    home_pitching_home_runs: Optional[float] = None
    home_innings_pitched: Optional[float] = None
    home_era: Optional[float] = None
    away_at_bats: Optional[float] = None
    away_runs: Optional[float] = None
    away_runs_scored: Optional[float] = None
    away_hits: Optional[float] = None
    away_rbis: Optional[float] = None
    away_rbi: Optional[float] = None
    away_home_runs: Optional[float] = None
    away_walks: Optional[float] = None
    away_strikeouts: Optional[float] = None
    away_stolen_bases: Optional[float] = None
    away_batting_avg: Optional[float] = None
    away_obp: Optional[float] = None
    away_slg: Optional[float] = None
    away_earned_runs: Optional[float] = None
    away_pitches: Optional[float] = None
    away_strikes: Optional[float] = None
    away_pitching_walks: Optional[float] = None
    away_pitching_strikeouts: Optional[float] = None
    away_pitching_home_runs: Optional[float] = None
    away_innings_pitched: Optional[float] = None
    away_era: Optional[float] = None

    # MLB-specific extended stats (mlbstats)
    home_caught_stealing: Optional[float] = None
    home_plate_appearances: Optional[float] = None
    home_left_on_base: Optional[float] = None
    home_sac_flies: Optional[float] = None
    home_sac_bunts: Optional[float] = None
    home_total_bases: Optional[float] = None
    home_ground_into_double_play: Optional[float] = None
    home_doubles: Optional[float] = None
    home_triples: Optional[float] = None
    home_errors: Optional[float] = None
    home_pitches_thrown: Optional[float] = None
    away_caught_stealing: Optional[float] = None
    away_plate_appearances: Optional[float] = None
    away_left_on_base: Optional[float] = None
    away_sac_flies: Optional[float] = None
    away_sac_bunts: Optional[float] = None
    away_total_bases: Optional[float] = None
    away_ground_into_double_play: Optional[float] = None
    away_doubles: Optional[float] = None
    away_triples: Optional[float] = None
    away_errors: Optional[float] = None
    away_pitches_thrown: Optional[float] = None
    home_ops: Optional[float] = None
    away_ops: Optional[float] = None
    home_whip: Optional[float] = None
    away_whip: Optional[float] = None
    home_k_rate: Optional[float] = None
    away_k_rate: Optional[float] = None
    home_bb_rate: Optional[float] = None
    away_bb_rate: Optional[float] = None
    home_iso: Optional[float] = None
    away_iso: Optional[float] = None
    # MLB additional pitching
    home_pitching_hit_batsmen: Optional[float] = None
    away_pitching_hit_batsmen: Optional[float] = None
    home_wild_pitches: Optional[float] = None
    away_wild_pitches: Optional[float] = None
    home_batters_faced: Optional[float] = None
    away_batters_faced: Optional[float] = None
    home_ground_outs: Optional[float] = None
    away_ground_outs: Optional[float] = None
    home_fly_outs: Optional[float] = None
    away_fly_outs: Optional[float] = None

    # Soccer-specific team stats
    home_possession: Optional[float] = None
    home_shots: Optional[float] = None
    home_total_shots: Optional[float] = None
    home_shots_on_target: Optional[float] = None
    home_shot_pct: Optional[float] = None
    home_shot_conversion_rate: Optional[float] = None
    home_shots_on_target_pct: Optional[float] = None
    home_corners: Optional[float] = None
    home_offsides: Optional[float] = None
    home_saves: Optional[float] = None
    home_yellow_cards: Optional[float] = None
    home_red_cards: Optional[float] = None
    home_pass_accuracy: Optional[float] = None
    home_passes_completed: Optional[float] = None
    home_accurate_passes: Optional[float] = None
    home_total_passes: Optional[float] = None
    home_pass_pct: Optional[float] = None
    home_accurate_crosses: Optional[float] = None
    home_total_crosses: Optional[float] = None
    home_cross_pct: Optional[float] = None
    home_effective_tackles: Optional[float] = None
    home_tackle_pct: Optional[float] = None
    home_interceptions: Optional[float] = None
    home_clearances: Optional[float] = None
    home_total_clearances: Optional[float] = None
    home_long_balls: Optional[float] = None
    home_accurate_long_balls: Optional[float] = None
    home_longball_pct: Optional[float] = None
    home_total_goals: Optional[float] = None
    home_goal_assists: Optional[float] = None
    home_goals_conceded: Optional[float] = None
    home_clean_sheet: Optional[int] = None
    home_penalty_goals: Optional[float] = None
    home_penalty_shots: Optional[float] = None
    away_possession: Optional[float] = None
    away_shots: Optional[float] = None
    away_total_shots: Optional[float] = None
    away_shots_on_target: Optional[float] = None
    away_shot_pct: Optional[float] = None
    away_shot_conversion_rate: Optional[float] = None
    away_shots_on_target_pct: Optional[float] = None
    away_corners: Optional[float] = None
    away_offsides: Optional[float] = None
    away_saves: Optional[float] = None
    away_yellow_cards: Optional[float] = None
    away_red_cards: Optional[float] = None
    away_pass_accuracy: Optional[float] = None
    away_passes_completed: Optional[float] = None
    away_accurate_passes: Optional[float] = None
    away_total_passes: Optional[float] = None
    away_pass_pct: Optional[float] = None
    away_accurate_crosses: Optional[float] = None
    away_total_crosses: Optional[float] = None
    away_cross_pct: Optional[float] = None
    away_effective_tackles: Optional[float] = None
    away_tackle_pct: Optional[float] = None
    away_interceptions: Optional[float] = None
    away_clearances: Optional[float] = None
    away_total_clearances: Optional[float] = None
    away_long_balls: Optional[float] = None
    away_accurate_long_balls: Optional[float] = None
    away_longball_pct: Optional[float] = None
    away_total_goals: Optional[float] = None
    away_goal_assists: Optional[float] = None
    away_goals_conceded: Optional[float] = None
    away_clean_sheet: Optional[int] = None
    away_penalty_goals: Optional[float] = None
    away_penalty_shots: Optional[float] = None

    # Soccer xG (expected goals)
    home_xg: Optional[float] = None
    away_xg: Optional[float] = None

    # Football derived stats
    home_turnover_margin: Optional[float] = None
    away_turnover_margin: Optional[float] = None

    # UFC/Combat-specific stats
    home_knockdowns: Optional[float] = None
    home_sig_strikes_landed: Optional[float] = None
    home_sig_strikes_attempted: Optional[float] = None
    home_sig_strike_pct: Optional[float] = None
    home_total_strikes_landed: Optional[float] = None
    home_total_strikes_attempted: Optional[float] = None
    home_takedown_pct: Optional[float] = None
    home_submission_attempts: Optional[float] = None
    home_control_time_seconds: Optional[float] = None
    home_finish_round: Optional[float] = None
    away_knockdowns: Optional[float] = None
    away_sig_strikes_landed: Optional[float] = None
    away_sig_strikes_attempted: Optional[float] = None
    away_sig_strike_pct: Optional[float] = None
    away_total_strikes_landed: Optional[float] = None
    away_total_strikes_attempted: Optional[float] = None
    away_takedown_pct: Optional[float] = None
    away_submission_attempts: Optional[float] = None
    away_control_time_seconds: Optional[float] = None
    away_finish_round: Optional[float] = None

    # Tennis game-level stats (home=player1, away=player2)
    home_aces: Optional[float] = None
    away_aces: Optional[float] = None
    home_double_faults: Optional[float] = None
    away_double_faults: Optional[float] = None
    home_first_serve_pct: Optional[float] = None
    away_first_serve_pct: Optional[float] = None
    home_first_serve_won_pct: Optional[float] = None
    away_first_serve_won_pct: Optional[float] = None
    home_second_serve_won_pct: Optional[float] = None
    away_second_serve_won_pct: Optional[float] = None
    home_serve_points: Optional[float] = None
    away_serve_points: Optional[float] = None
    home_service_games: Optional[float] = None
    away_service_games: Optional[float] = None
    home_break_points_won: Optional[float] = None
    away_break_points_won: Optional[float] = None
    home_break_points_saved: Optional[float] = None
    away_break_points_saved: Optional[float] = None
    home_break_points_faced: Optional[float] = None
    away_break_points_faced: Optional[float] = None
    home_break_points_total: Optional[float] = None
    away_break_points_total: Optional[float] = None
    home_break_point_conversion_pct: Optional[float] = None
    away_break_point_conversion_pct: Optional[float] = None
    home_break_point_save_pct: Optional[float] = None
    away_break_point_save_pct: Optional[float] = None
    home_ace_df_ratio: Optional[float] = None
    away_ace_df_ratio: Optional[float] = None
    total_sets: Optional[int] = None
    home_sets_won: Optional[int] = None
    away_sets_won: Optional[int] = None
    duration_minutes: Optional[int] = None
    surface: Optional[str] = None
    best_of: Optional[int] = None

    # Esports game-level stats
    home_kills: Optional[float] = None
    away_kills: Optional[float] = None
    home_deaths: Optional[float] = None
    away_deaths: Optional[float] = None
    home_kill_death_ratio: Optional[float] = None
    away_kill_death_ratio: Optional[float] = None
    home_gold: Optional[float] = None
    away_gold: Optional[float] = None
    home_gold_per_min: Optional[float] = None
    away_gold_per_min: Optional[float] = None
    duration: Optional[float] = None
    game_duration: Optional[float] = None
    map_name: Optional[str] = None
    total_rounds: Optional[int] = None

    # ── F1 / Racing fields ──
    race_name: Optional[str] = None
    circuit_name: Optional[str] = None
    circuit_id: Optional[str] = None
    round_number: Optional[int] = None
    total_laps: Optional[int] = None
    winner_name: Optional[str] = None
    winner_team: Optional[str] = None
    winner_time: Optional[str] = None
    fastest_lap_driver: Optional[str] = None
    fastest_lap_time: Optional[str] = None
    fastest_lap_number: Optional[int] = None
    pole_position_driver: Optional[str] = None
    dnf_count: Optional[int] = None
    safety_car_count: Optional[int] = None
    red_flag_count: Optional[int] = None
    pit_stops_total: Optional[int] = None

    # FiveThirtyEight ELO ratings (NBA, NFL)
    home_elo_pre: Optional[float] = None         # Pre-game ELO rating (elo_i)
    home_elo_post: Optional[float] = None        # Post-game ELO rating (elo_n)
    away_elo_pre: Optional[float] = None
    away_elo_post: Optional[float] = None
    home_win_equiv: Optional[float] = None       # Win equivalent
    away_win_equiv: Optional[float] = None
    home_forecast: Optional[float] = None        # Pre-game win probability
    away_forecast: Optional[float] = None

    # ── Universal derived / enrichment fields ──
    # Computed at normalisation time from raw stats; available on all sports.
    result: Optional[str] = None                 # "home_win" | "away_win" | "draw"
    score_diff: Optional[int] = None             # home_score - away_score
    total_score: Optional[int] = None            # home_score + away_score
    overtime: bool = False                       # True if game went to OT / extra time
    day_of_week: Optional[int] = None            # 0=Monday … 6=Sunday
    is_weekend: Optional[bool] = None            # Saturday or Sunday
    home_rest_days: Optional[int] = None         # Days since team's previous game
    away_rest_days: Optional[int] = None

    # Sport-specific advanced analytics (computed during normalisation)
    # Basketball
    home_pace: Optional[float] = None            # Possessions per 48 min (NBA) / 40 min (college)
    away_pace: Optional[float] = None
    home_offensive_rating: Optional[float] = None  # Points per 100 possessions
    away_offensive_rating: Optional[float] = None
    home_defensive_rating: Optional[float] = None  # Opp points per 100 possessions
    away_defensive_rating: Optional[float] = None
    home_net_rating: Optional[float] = None      # offensive_rating - defensive_rating
    away_net_rating: Optional[float] = None

    # Soccer / Hockey
    home_xg: Optional[float] = None             # Expected goals (home)
    away_xg: Optional[float] = None             # Expected goals (away)
    xg_diff: Optional[float] = None             # home_xg - away_xg
    xg_total: Optional[float] = None            # home_xg + away_xg
    home_goals_per_shot: Optional[float] = None  # Shooting efficiency
    away_goals_per_shot: Optional[float] = None

    # NFL / NCAAF
    home_yards_diff: Optional[int] = None        # home_total_yards - away_total_yards
    away_yards_diff: Optional[int] = None
    home_turnover_margin: Optional[int] = None   # turnovers forced - turnovers committed
    away_turnover_margin: Optional[int] = None
    home_scoring_efficiency: Optional[float] = None  # pts / total_plays
    away_scoring_efficiency: Optional[float] = None

    # Tennis
    home_sets_won: Optional[int] = None
    away_sets_won: Optional[int] = None

    _coerce_id = field_validator("id", "home_team_id", "away_team_id", mode="before")(_coerce_str)


# ── Team ──────────────────────────────────────────────────

class Team(_Base):
    id: str
    sport: str
    name: str
    abbreviation: Optional[str] = None
    city: Optional[str] = None
    conference: Optional[str] = None
    division: Optional[str] = None
    league: Optional[str] = None
    logo_url: Optional[str] = None
    color_primary: Optional[str] = None
    color_secondary: Optional[str] = None
    venue_name: Optional[str] = None
    founded_year: Optional[int] = None

    _coerce_id = field_validator("id", mode="before")(_coerce_str)


# ── Player ────────────────────────────────────────────────

class Player(_Base):
    id: str
    sport: str
    name: str
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    team_abbreviation: Optional[str] = None
    position: Optional[str] = None
    jersey_number: Optional[int] = None
    height: Optional[str] = None
    weight: Optional[int] = None
    birth_date: Optional[_dt.date] = None
    birth_place: Optional[str] = None
    nationality: Optional[str] = None
    experience_years: Optional[int] = None
    college: Optional[str] = None
    age: Optional[int] = None
    status: str = Field(default="active", description="active | inactive | injured | retired")
    headshot_url: Optional[str] = None

    _coerce_id = field_validator("id", "team_id", mode="before")(_coerce_str)


# ── Standing ──────────────────────────────────────────────

class Standing(_Base):
    team_id: str
    team_name: Optional[str] = None
    sport: str
    season: str
    wins: Optional[int] = 0
    losses: Optional[int] = 0
    ties: Optional[int] = None
    otl: Optional[int] = None
    pct: Optional[float] = None
    games_played: Optional[int] = None
    points_for: Optional[int] = None
    points_against: Optional[int] = None
    points: Optional[int] = None
    rank: Optional[int] = None
    group: Optional[str] = None
    conference: Optional[str] = None
    division: Optional[str] = None
    conference_rank: Optional[int] = None
    division_rank: Optional[int] = None
    overall_rank: Optional[int] = None
    streak: Optional[str] = None
    last_ten: Optional[str] = None
    home_record: Optional[str] = None
    away_record: Optional[str] = None
    clinch_status: Optional[str] = None

    _coerce_id = field_validator("team_id", mode="before")(_coerce_str)


# ── Odds ──────────────────────────────────────────────────

class Odds(_Base):
    game_id: str
    sport: str
    bookmaker: str
    date: Optional[str] = None
    commence_time: Optional[str] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    h2h_home: Optional[float] = None
    h2h_away: Optional[float] = None
    h2h_draw: Optional[float] = None
    spread_home: Optional[float] = None
    spread_away: Optional[float] = None
    spread_home_line: Optional[float] = None
    spread_away_line: Optional[float] = None
    total_over: Optional[float] = None
    total_under: Optional[float] = None
    total_line: Optional[float] = None
    timestamp: Optional[datetime] = None
    is_live: bool = False

    _coerce_id = field_validator("game_id", mode="before")(_coerce_str)


class MarketSignal(_Base):
    game_id: str
    sport: str
    season: str
    bookmaker: str
    date: Optional[str] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    source_count: Optional[int] = None
    observation_count: Optional[int] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    open_h2h_home: Optional[float] = None
    close_h2h_home: Optional[float] = None
    h2h_home_move: Optional[float] = None
    h2h_home_range: Optional[float] = None
    open_h2h_away: Optional[float] = None
    close_h2h_away: Optional[float] = None
    h2h_away_move: Optional[float] = None
    h2h_away_range: Optional[float] = None
    open_spread_home: Optional[float] = None
    close_spread_home: Optional[float] = None
    spread_home_move: Optional[float] = None
    spread_home_range: Optional[float] = None
    open_total_line: Optional[float] = None
    close_total_line: Optional[float] = None
    total_line_move: Optional[float] = None
    total_line_range: Optional[float] = None
    aggregate_abs_move: Optional[float] = None
    market_regime: Optional[str] = Field(default=None, description="stable | moving | volatile")

    _coerce_id = field_validator("game_id", mode="before")(_coerce_str)


class ScheduleFatigue(_Base):
    game_id: str
    sport: str
    season: str
    date: Optional[str] = None
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    opponent_id: Optional[str] = None
    opponent_name: Optional[str] = None
    is_home: Optional[int] = None
    rest_days: Optional[float] = None
    is_back_to_back: Optional[int] = None
    games_last_7d: Optional[int] = None
    games_last_14d: Optional[int] = None
    home_away_switch: Optional[int] = None
    away_streak_before: Optional[int] = None
    home_streak_before: Optional[int] = None
    fatigue_score: Optional[float] = None
    fatigue_level: Optional[str] = Field(default=None, description="low | medium | high")

    _coerce_id = field_validator("game_id", "team_id", "opponent_id", mode="before")(_coerce_str)


# ── Player Prop ───────────────────────────────────────────

class PlayerProp(_Base):
    game_id: str
    player_id: str
    sport: str
    market: str = Field(..., description="e.g. points, rebounds, strikeouts …")
    line: float
    over_price: Optional[float] = None
    under_price: Optional[float] = None
    bookmaker: str = "unknown"
    timestamp: Optional[datetime] = None
    player_name: Optional[str] = None
    team_id: Optional[str] = None

    _coerce_id = field_validator("game_id", "player_id", "team_id", mode="before")(_coerce_str)


# ── Injury ────────────────────────────────────────────────

class Injury(_Base):
    player_id: str
    sport: str
    team_id: Optional[str] = None
    player_name: Optional[str] = None
    status: str = Field(
        ..., description="out | doubtful | questionable | probable | day_to_day"
    )
    description: Optional[str] = None
    body_part: Optional[str] = None
    return_date: Optional[_dt.date] = None
    reported_at: Optional[datetime] = None

    _coerce_id = field_validator("player_id", "team_id", mode="before")(_coerce_str)


# ── Player Game Stats (discriminated union) ───────────────

class _PlayerGameStatsBase(_Base):
    game_id: str
    player_id: str
    sport: str
    team_id: Optional[str] = None
    player_name: Optional[str] = None
    season: Optional[str] = None
    date: Optional[_dt.date] = None
    minutes: Optional[float] = None

    _coerce_id = field_validator("game_id", "player_id", "team_id", mode="before")(_coerce_str)


class BasketballStats(_PlayerGameStatsBase):
    category: Literal["basketball"] = "basketball"
    pts: Optional[int] = None
    reb: Optional[int] = None
    ast: Optional[int] = None
    stl: Optional[int] = None
    blk: Optional[int] = None
    to: Optional[int] = None
    fg_pct: Optional[float] = None
    ft_pct: Optional[float] = None
    three_pct: Optional[float] = None
    fgm: Optional[int] = None
    fga: Optional[int] = None
    ftm: Optional[int] = None
    fta: Optional[int] = None
    three_m: Optional[int] = None
    three_a: Optional[int] = None
    oreb: Optional[int] = None
    dreb: Optional[int] = None
    pf: Optional[int] = None
    plus_minus: Optional[int] = None
    min: Optional[float] = None
    position: Optional[str] = None
    off_rating: Optional[float] = None
    def_rating: Optional[float] = None
    net_rating: Optional[float] = None
    ast_pct: Optional[float] = None
    ast_to: Optional[float] = None
    ts_pct: Optional[float] = None
    efg_pct: Optional[float] = None
    usg_pct: Optional[float] = None
    # FiveThirtyEight RAPTOR / WAR
    raptor_offense: Optional[float] = None
    raptor_defense: Optional[float] = None
    raptor_total: Optional[float] = None
    war_total: Optional[float] = None
    war_reg_season: Optional[float] = None
    war_playoffs: Optional[float] = None
    pace_impact: Optional[float] = None
    poss: Optional[int] = None


class FootballStats(_PlayerGameStatsBase):
    category: Literal["football"] = "football"
    pass_yds: Optional[int] = None
    pass_td: Optional[int] = None
    pass_att: Optional[int] = None
    pass_cmp: Optional[int] = None
    pass_int: Optional[int] = None
    rush_yds: Optional[int] = None
    rush_td: Optional[int] = None
    rush_att: Optional[int] = None
    rec_yds: Optional[int] = None
    rec_td: Optional[int] = None
    receptions: Optional[int] = None
    targets: Optional[int] = None
    tackles: Optional[int] = None
    sacks: Optional[float] = None
    interceptions: Optional[int] = None
    fumbles: Optional[int] = None
    fumbles_lost: Optional[int] = None
    fumbles_rec: Optional[int] = None
    pass_rating: Optional[float] = None
    position: Optional[str] = None
    rec_avg: Optional[float] = None
    rec_long: Optional[int] = None
    rush_avg: Optional[float] = None
    rush_long: Optional[int] = None
    kr_no: Optional[int] = None
    kr_yds: Optional[int] = None
    kr_avg: Optional[float] = None
    kr_long: Optional[int] = None
    pr_no: Optional[int] = None
    pr_yds: Optional[int] = None
    pr_avg: Optional[float] = None


class BaseballStats(_PlayerGameStatsBase):
    category: Literal["baseball"] = "baseball"
    ab: Optional[int] = None
    hits: Optional[int] = None
    hr: Optional[int] = None
    rbi: Optional[int] = None
    sb: Optional[int] = None
    runs: Optional[int] = None
    bb: Optional[int] = None
    so: Optional[int] = None
    avg: Optional[float] = None
    obp: Optional[float] = None
    slg: Optional[float] = None
    ops: Optional[float] = None
    era: Optional[float] = None
    strikeouts: Optional[int] = None
    walks: Optional[int] = None
    innings: Optional[float] = None
    earned_runs: Optional[int] = None
    whip: Optional[float] = None
    win: Optional[bool] = None
    loss: Optional[bool] = None
    save: Optional[bool] = None
    # Extended batting (MLB Stats API)
    doubles: Optional[int] = None
    triples: Optional[int] = None
    pa: Optional[int] = None
    cs: Optional[int] = None
    hbp: Optional[int] = None
    sac_flies: Optional[int] = None
    sac_bunts: Optional[int] = None
    lob: Optional[int] = None
    total_bases: Optional[int] = None
    gidp: Optional[int] = None
    # Extended pitching (MLB Stats API)
    holds: Optional[int] = None
    blown_saves: Optional[int] = None
    pitches: Optional[int] = None
    batters_faced: Optional[int] = None
    wild_pitches: Optional[int] = None


class HockeyStats(_PlayerGameStatsBase):
    category: Literal["hockey"] = "hockey"
    goals: Optional[int] = None
    assists: Optional[int] = None
    points: Optional[int] = None
    shots: Optional[int] = None
    saves: Optional[int] = None
    save_pct: Optional[float] = None
    goals_against: Optional[int] = None
    toi: Optional[str] = None
    hits: Optional[int] = None
    blocked_shots: Optional[int] = None
    pim: Optional[int] = None
    plus_minus: Optional[int] = None
    faceoff_pct: Optional[float] = None
    pp_goals: Optional[int] = None
    sh_goals: Optional[int] = None
    takeaways: Optional[int] = None
    giveaways: Optional[int] = None
    shot_misses: Optional[int] = None
    pp_toi: Optional[str] = None
    sh_toi: Optional[str] = None
    position: Optional[str] = None


class SoccerStats(_PlayerGameStatsBase):
    category: Literal["soccer"] = "soccer"
    goals: Optional[int] = None
    assists: Optional[int] = None
    shots: Optional[int] = None
    shots_on_target: Optional[int] = None
    passes: Optional[int] = None
    pass_pct: Optional[float] = None
    tackles: Optional[int] = None
    interceptions: Optional[int] = None
    fouls: Optional[int] = None
    fouls_committed: Optional[int] = None
    fouls_suffered: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None
    offsides: Optional[int] = None
    own_goals: Optional[int] = None
    saves: Optional[int] = None
    goals_conceded: Optional[int] = None
    shots_faced: Optional[int] = None
    starter: Optional[bool] = None
    position: Optional[str] = None
    xg: Optional[float] = None
    xa: Optional[float] = None
    key_passes: Optional[int] = None
    dribbles_completed: Optional[int] = None
    aerial_duels_won: Optional[int] = None


class MMAStats(_PlayerGameStatsBase):
    category: Literal["mma"] = "mma"
    strikes_landed: Optional[int] = None
    strikes_attempted: Optional[int] = None
    sig_strikes: Optional[int] = None
    sig_strikes_attempted: Optional[int] = None
    takedowns: Optional[int] = None
    takedowns_attempted: Optional[int] = None
    sub_attempts: Optional[int] = None
    knockdowns: Optional[int] = None
    control_time: Optional[str] = None
    result: Optional[str] = None
    method: Optional[str] = None
    round_finished: Optional[int] = None
    finish_round: Optional[int] = None
    finish_time: Optional[str] = None
    weight_class: Optional[str] = None
    sig_strikes_landed: Optional[int] = None
    total_strikes_landed: Optional[int] = None
    total_strikes_attempted: Optional[int] = None


class TennisStats(_PlayerGameStatsBase):
    category: Literal["tennis"] = "tennis"
    aces: Optional[int] = None
    double_faults: Optional[int] = None
    first_serve_pct: Optional[float] = None
    second_serve_pct: Optional[float] = None
    break_points_won: Optional[int] = None
    break_points_faced: Optional[int] = None
    sets_won: Optional[int] = None
    sets_lost: Optional[int] = None
    games_won: Optional[int] = None
    games_lost: Optional[int] = None
    tiebreaks_won: Optional[int] = None
    winners: Optional[int] = None
    unforced_errors: Optional[int] = None
    net_points_won: Optional[int] = None
    result: Optional[str] = None
    won: Optional[bool] = None
    total_sets: Optional[int] = None


class F1Stats(_PlayerGameStatsBase):
    category: Literal["motorsport"] = "motorsport"
    grid_position: Optional[int] = None
    finish_position: Optional[int] = None
    laps: Optional[int] = None
    points: Optional[float] = None
    fastest_lap: Optional[str] = None
    pit_stops: Optional[int] = None
    status: Optional[str] = None
    interval: Optional[str] = None
    avg_speed_kph: Optional[float] = None
    dnf: bool = False
    constructor: Optional[str] = None
    team_name: Optional[str] = None


class EsportsStats(_PlayerGameStatsBase):
    category: Literal["esports"] = "esports"
    kills: Optional[int] = None
    deaths: Optional[int] = None
    assists: Optional[int] = None
    cs_per_min: Optional[float] = None
    gold_per_min: Optional[float] = None
    vision_score: Optional[float] = None
    damage: Optional[int] = None
    kda: Optional[float] = None
    first_bloods: Optional[int] = None
    turrets_destroyed: Optional[int] = None
    objectives: Optional[int] = None
    headshot_pct: Optional[float] = None
    adr: Optional[float] = None
    rating: Optional[float] = None
    team_name: Optional[str] = None
    opponent_id: Optional[str] = None
    opponent_name: Optional[str] = None


class GolfStats(_PlayerGameStatsBase):
    category: Literal["golf"] = "golf"
    score: Optional[int] = None
    score_to_par: Optional[int] = None
    rounds: Optional[int] = None
    birdies: Optional[int] = None
    bogeys: Optional[int] = None
    eagles: Optional[int] = None
    pars: Optional[int] = None
    fairway_pct: Optional[float] = None
    gir_pct: Optional[float] = None
    putts: Optional[int] = None
    position: Optional[int] = None
    earnings: Optional[float] = None


PlayerGameStats = Annotated[
    Union[
        BasketballStats,
        FootballStats,
        BaseballStats,
        HockeyStats,
        SoccerStats,
        MMAStats,
        TennisStats,
        F1Stats,
        EsportsStats,
        GolfStats,
    ],
    Field(discriminator="category"),
]


# ── Prediction ────────────────────────────────────────────

class Prediction(_Base):
    game_id: str
    sport: str
    model: str = Field(..., description="Name/version of the ML model that produced this.")
    home_win_prob: Optional[float] = None
    away_win_prob: Optional[float] = None
    draw_prob: Optional[float] = None
    predicted_spread: Optional[float] = None
    predicted_total: Optional[float] = None
    confidence: Optional[float] = None
    features_hash: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    _coerce_id = field_validator("game_id", mode="before")(_coerce_str)


# ── News ──────────────────────────────────────────────────

class News(_Base):
    id: str
    sport: str
    headline: str
    summary: Optional[str] = None
    body: Optional[str] = None
    url: Optional[str] = None
    image_url: Optional[str] = None
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    related_team: Optional[str] = None
    related_player: Optional[str] = None
    tags: list[str] = Field(default_factory=list)

    _coerce_id = field_validator("id", mode="before")(_coerce_str)


# ── Weather ───────────────────────────────────────────────

class Weather(_Base):
    game_id: str
    temp_f: Optional[float] = None
    wind_mph: Optional[float] = None
    wind_direction: Optional[str] = None
    humidity_pct: Optional[float] = None
    precipitation: Optional[float] = None
    condition: Optional[str] = None
    dome: bool = False

    _coerce_id = field_validator("game_id", mode="before")(_coerce_str)


# ── Convenience collections ───────────────────────────────

ALL_SCHEMA_CLASSES: list[type[_Base]] = [
    Game,
    Team,
    Player,
    Standing,
    Odds,
    MarketSignal,
    ScheduleFatigue,
    PlayerProp,
    Injury,
    BasketballStats,
    FootballStats,
    BaseballStats,
    HockeyStats,
    SoccerStats,
    MMAStats,
    TennisStats,
    F1Stats,
    EsportsStats,
    GolfStats,
    Prediction,
    News,
    Weather,
]

CATEGORY_STATS_MAP: dict[str, type[_PlayerGameStatsBase]] = {
    "basketball": BasketballStats,
    "football": FootballStats,
    "baseball": BaseballStats,
    "hockey": HockeyStats,
    "soccer": SoccerStats,
    "mma": MMAStats,
    "tennis": TennisStats,
    "motorsport": F1Stats,
    "esports": EsportsStats,
    "golf": GolfStats,
}
