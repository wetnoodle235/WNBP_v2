# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Models: Base data classes
# ──────────────────────────────────────────────────────────
"""
Core data classes shared across the ML pipeline.

* ``TrainingConfig``  — parameters that control a training run.
* ``PredictionResult`` — output of a game-level prediction.
* ``PropPrediction``   — output of a player-prop prediction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


# ── Training configuration ───────────────────────────────


@dataclass
class TrainingConfig:
    """Parameters for a single training run."""

    sport: str
    seasons: list[int] = field(default_factory=list)
    test_size: float = 0.2  # temporal split fraction
    min_samples: int = 100  # refuse to train with fewer rows
    mode: str = "joint"  # "joint" (home+away score) or "separate" (winner/total/spread)
    random_seed: int = 42
    n_cv_folds: int = 5  # for time-series cross-validation
    drop_dead_features: bool = True  # drop features with ≤5 % non-zero
    dead_feature_threshold: float = 0.05

    def __post_init__(self) -> None:
        if self.mode not in ("joint", "separate"):
            raise ValueError(f"mode must be 'joint' or 'separate', got {self.mode!r}")
        if not 0 < self.test_size < 1:
            raise ValueError(f"test_size must be in (0, 1), got {self.test_size}")


# ── Game prediction result ───────────────────────────────


@dataclass
class PredictionResult:
    """Output of a game-level prediction.

    Extended with halftime, period, draw/OT, and first-score markets so
    that the autobet engine can surface every standard betting market.
    """

    game_id: str
    sport: str
    home_team: str
    away_team: str

    # ── Full-game moneyline ───────────────────────────────
    home_win_prob: float
    away_win_prob: float

    # ── Full-game score / spread / total ─────────────────
    predicted_home_score: float | None = None
    predicted_away_score: float | None = None
    predicted_total: float | None = None
    predicted_spread: float | None = None  # home − away

    # ── Draw / Tie / Overtime ─────────────────────────────
    # draw_prob: probability the game ends in a draw/tie at full time
    #   • Soccer: genuine draws (no OT in league play)
    #   • NBA:    probability of going to overtime (tie at end of regulation)
    #   • NFL:    probability of overtime period (tie or OT win)
    #   • NHL:    probability of OT/shootout
    draw_prob: float | None = None
    ot_prob: float | None = None  # synonym exposed separately for clarity

    # 3-way moneyline (home win / draw / away win)
    # populated for all sports; for non-draw sports draw_prob is very small
    three_way: dict[str, float] | None = None  # {'home': p, 'draw': p, 'away': p}

    # Double chance (home+draw, away+draw) — implicit derived in property
    # Winning margin bucket (e.g. {1-6: 0.25, 7-13: 0.35, 14+: 0.40})
    winning_margin_probs: dict[str, float] | None = None

    # ── Halftime ─────────────────────────────────────────
    # Available for NBA, NFL, NHL (periods 1+2), MLB (innings 1+2)
    halftime_home_win_prob: float | None = None
    halftime_away_win_prob: float | None = None
    halftime_draw_prob: float | None = None
    halftime_home_score: float | None = None
    halftime_away_score: float | None = None
    halftime_spread: float | None = None   # predicted home_half − away_half
    halftime_total: float | None = None    # predicted home_half + away_half

    # ── Per-period / Quarter / Inning ────────────────────
    # List of dicts: [{'period': 1, 'home': 1.8, 'away': 1.4,
    #                  'total': 3.2, 'spread': 0.4}, ...]
    # Labels: "quarter" (NBA/NFL), "period" (NHL), "inning" (MLB), "half" (soccer)
    period_label: str | None = None   # e.g. "quarter", "period", "inning", "half"
    period_predictions: list[dict] | None = None

    # ── First / Last score ───────────────────────────────
    first_score_home_prob: float | None = None   # P(home team scores first)
    first_score_team: str | None = None          # "home" | "away"
    last_score_home_prob: float | None = None    # P(home team scores last)
    last_score_team: str | None = None           # "home" | "away"

    # ── Method of victory (UFC / boxing) ─────────────────
    # {'ko_tko': p, 'submission': p, 'decision': p}
    method_probs: dict[str, float] | None = None

    # ── Both Teams to Score (BTTS) ────────────────────────
    # Probability that both teams score ≥1 goal/run (soccer, MLB)
    btts_prob: float | None = None  # P(home_score>0 AND away_score>0)

    # ── Clean Sheet ───────────────────────────────────────
    # Probability that a team keeps a clean sheet (concedes 0)
    home_clean_sheet_prob: float | None = None  # P(away_score=0)
    away_clean_sheet_prob: float | None = None  # P(home_score=0)

    # ── Winning Margin Buckets ────────────────────────────
    # e.g. {"1-3": 0.20, "4-6": 0.18, "7-13": 0.35, "14+": 0.27}
    winning_margin_probs: dict[str, float] | None = None

    # ── Score Race (who scores first N) ──────────────────
    # Tennis: probability of winning in straight sets (2-0, 3-0)
    # UFC: method + round prediction
    straight_sets_prob: float | None = None  # Tennis: P(2-0 or 3-0)
    decision_prob: float | None = None        # UFC: P(goes to decision)
    ko_tko_prob: float | None = None          # UFC: P(KO/TKO)
    submission_prob: float | None = None      # UFC: P(submission)

    # ── Winning Margin Bands ──────────────────────────────
    # Dict of band_name → probability (e.g. {"1-5": 0.28, "6-10": 0.35, ...})
    margin_band_probs: dict[str, float] | None = None
    dominant_win_prob: float | None = None    # P(winner wins by sport-specific large margin)
    large_margin_prob: float | None = None    # P(winner wins by sport-specific large margin)

    # ── Total Score Bands ─────────────────────────────────
    # {"low": p, "mid": p, "high": p}, cutoffs sport-specific
    total_band_probs: dict[str, float] | None = None
    total_over_median_prob: float | None = None  # P(total > historical median)

    # ── Second Half ───────────────────────────────────────
    second_half_home_win_prob: float | None = None   # P(home wins 2nd half)
    second_half_total: float | None = None            # predicted 2nd half total

    # ── Regulation Result (pre-OT) ───────────────────────
    regulation_home_win_prob: float | None = None    # P(home leads at end of regulation)
    regulation_draw_prob: float | None = None        # P(tied after regulation → OT)
    regulation_away_win_prob: float | None = None    # P(away leads at end of regulation)

    # ── Team Totals ───────────────────────────────────────
    home_team_total: float | None = None              # predicted home team score
    away_team_total: float | None = None              # predicted away team score
    home_team_total_over_prob: float | None = None    # P(home scores > median line)
    away_team_total_over_prob: float | None = None    # P(away scores > median line)

    # ── Comeback / Momentum ───────────────────────────────
    comeback_home_prob: float | None = None   # P(home wins if trailing at half)
    comeback_away_prob: float | None = None   # P(away wins if trailing at half)

    # ── Esports-specific ──────────────────────────────────
    # Available for CSGO, Dota2, LoL, Valorant
    esports_clean_sweep_prob: float | None = None   # P(winner takes all maps, e.g. 2-0 or 3-0)
    esports_map_total: float | None = None           # predicted total maps played
    esports_map_total_over2_prob: float | None = None  # P(series goes 3+ maps)


    # ── Meta ─────────────────────────────────────────────
    confidence: float = 0.50  # 0.50 – 0.99
    n_models: int = 0
    consensus: float = 0.0  # fraction of models agreeing with majority
    model_votes: dict[str, str] = field(default_factory=dict)  # name → "home"|"away"
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # ── Derived properties ────────────────────────────────

    @property
    def confidence_tier(self) -> str:
        """5-tier confidence bracket: S (Platinum) > A (Gold) > B (Silver) > C (Bronze) > D (Copper)."""
        c = self.confidence
        if c >= 0.82:
            return "S"   # Platinum
        if c >= 0.70:
            return "A"   # Gold
        if c >= 0.62:
            return "B"   # Silver
        if c >= 0.55:
            return "C"   # Bronze
        return "D"       # Copper

    @property
    def confidence_tier_label(self) -> str:
        """Human-readable tier label."""
        return {
            "S": "Platinum",
            "A": "Gold",
            "B": "Silver",
            "C": "Bronze",
            "D": "Copper",
        }.get(self.confidence_tier, "Copper")

    @property
    def predicted_winner(self) -> str:
        return self.home_team if self.home_win_prob >= 0.5 else self.away_team

    @property
    def home_or_draw_prob(self) -> float | None:
        """Probability of home win OR draw (double-chance)."""
        if self.draw_prob is None:
            return None
        return min(1.0, self.home_win_prob + self.draw_prob)

    @property
    def away_or_draw_prob(self) -> float | None:
        """Probability of away win OR draw (double-chance)."""
        if self.draw_prob is None:
            return None
        return min(1.0, self.away_win_prob + self.draw_prob)


# ── Player prop prediction result ────────────────────────


@dataclass
class PropPrediction:
    """Output of a single player-prop prediction."""

    game_id: str
    player_id: str
    player_name: str
    sport: str

    prop_type: str  # e.g. "points", "rebounds", "assists", "pra"
    line: float  # book line (e.g. 24.5)
    predicted_value: float  # model prediction (e.g. 27.3)
    edge: float  # predicted_value − line
    over_prob: float  # probability of going over
    under_prob: float  # probability of going under

    confidence: float = 0.50
    confidence_tier: str = "D"   # S / A / B / C / D  (5-tier bracket)
    confidence_label: str = "Copper"  # Platinum / Gold / Silver / Bronze / Copper
    recommendation: str = "PASS"  # "OVER" / "UNDER" / "PASS"
    n_models: int = 0
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
