"""Dynamic threshold adaptation based on recent betting performance.

Adjusts confidence and edge thresholds per sport × bet_type so the bot
self-corrects: tightens thresholds when losing, relaxes when winning.
Uses an exponential moving average over the most recent graded bets.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import AutobetConfig
    from .ledger import Ledger

logger = logging.getLogger("autobet.meta")

_DECAY_HALF_LIFE_BETS: int = 25  # half-life in number of bets


class MetaEvaluator:
    """Adapts confidence/edge thresholds based on recent performance."""

    def __init__(self, config: AutobetConfig, ledger: Ledger):
        self.config = config
        self.ledger = ledger

    def get_adjusted_thresholds(
        self, sport: str, bet_type: str
    ) -> tuple[float, float]:
        """Return ``(min_confidence, min_edge)`` adjusted for recent performance.

        * win_rate > target → relax thresholds (accept more bets)
        * win_rate < target → tighten thresholds (higher quality only)

        The adjustment is bounded by ``config.dynamic_adjustment_cap``.
        """
        base_conf = self.config.get_min_confidence(sport, bet_type)
        base_edge = self.config.get_min_edge(sport, bet_type)

        if not self.config.dynamic_thresholds_enabled:
            return base_conf, base_edge

        recent = self.ledger.get_recent_bets(
            sport=sport, bet_type=bet_type, limit=self.config.dynamic_lookback_bets
        )
        if len(recent) < 10:
            return base_conf, base_edge

        win_rate = self._ema_win_rate(recent)
        target = self.config.target_win_rate.get(bet_type, 0.55)
        gap = win_rate - target
        cap = self.config.dynamic_adjustment_cap

        # Positive gap (winning more than target) → relax thresholds (subtract)
        # Negative gap (losing) → tighten thresholds (add)
        if abs(gap) < 0.02:
            # Dead-band: no adjustment when close to target
            return base_conf, base_edge

        adjustment = max(-cap, min(cap, -gap))

        adj_conf = round(max(0.50, min(0.95, base_conf + adjustment)), 4)
        adj_edge = round(max(0.02, min(0.30, base_edge + adjustment * 0.5)), 4)

        if abs(adjustment) > 0.001:
            logger.debug(
                "Dynamic threshold %s/%s: win_rate=%.3f target=%.3f "
                "→ conf %.3f→%.3f  edge %.3f→%.3f",
                sport,
                bet_type,
                win_rate,
                target,
                base_conf,
                adj_conf,
                base_edge,
                adj_edge,
            )

        return adj_conf, adj_edge

    @staticmethod
    def _ema_win_rate(bets: list) -> float:
        """Exponential moving average win rate, recent bets weighted more.

        Bets are ordered most-recent-first from ``ledger.get_recent_bets``.
        """
        if not bets:
            return 0.0
        ln2 = math.log(2)
        total_weight = 0.0
        weighted_wins = 0.0
        for i, bet in enumerate(bets):
            weight = math.exp(-i * ln2 / _DECAY_HALF_LIFE_BETS)
            total_weight += weight
            if bet.status == "won":
                weighted_wins += weight
            elif bet.status == "push":
                weighted_wins += weight * 0.5
        return weighted_wins / total_weight if total_weight > 0 else 0.0
