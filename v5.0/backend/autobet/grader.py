"""Bet grading — settles pending paper bets against final game scores.

Runs on the grading cycle (every 30 minutes) to check if games backing
pending bets have completed, then grades each leg and computes P&L.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import AutobetConfig
from .engine import _backend_get
from .ledger import Ledger
from .models import BetLeg, PaperBet

logger = logging.getLogger("autobet.grader")


class Grader:
    """Grade pending paper bets against final scores."""

    def __init__(self, config: AutobetConfig, ledger: Ledger):
        self.config = config
        self.ledger = ledger

    async def run_grading(self) -> dict[str, int]:
        """Grade all pending bets whose games have finished.

        Returns a summary dict: ``{"graded": N, "won": W, "lost": L, ...}``
        """
        pending = self.ledger.get_pending()
        if not pending:
            logger.debug("No pending bets to grade")
            return {"graded": 0, "won": 0, "lost": 0, "push": 0, "void": 0, "skipped": 0}

        # Collect all sports we need scores for
        sports_needed = {leg.sport for bet in pending for leg in bet.legs}
        scores_cache: dict[str, dict] = {}  # game_id → game dict with scores

        for sport in sports_needed:
            try:
                resp = await _backend_get(
                    self.config, f"/v1/{sport}/games", status="final"
                )
                for g in resp.get("data", []):
                    gid = g.get("id", "")
                    if gid:
                        scores_cache[gid] = g
            except Exception as exc:
                logger.warning("Failed to fetch scores for %s: %s", sport, exc)

        summary = {"graded": 0, "won": 0, "lost": 0, "push": 0, "void": 0, "skipped": 0}

        for bet in pending:
            leg_results = []
            all_resolved = True

            for leg in bet.legs:
                game = scores_cache.get(leg.game_id)
                if not game:
                    all_resolved = False
                    continue

                status_str = game.get("status", "")
                if status_str not in ("final",):
                    all_resolved = False
                    continue

                result = self.grade_leg(leg, game)
                leg.result = result
                leg_results.append(result)

            if not all_resolved and not leg_results:
                summary["skipped"] += 1
                continue

            # For parlays: all legs must be resolved
            if bet.is_parlay and not all_resolved:
                summary["skipped"] += 1
                continue

            bet_status, pnl = self._settle_bet(bet, leg_results)
            self.ledger.grade_bet(bet.id, bet_status, pnl)
            summary["graded"] += 1
            summary[bet_status] = summary.get(bet_status, 0) + 1

        if summary["graded"] > 0:
            logger.info(
                "Grading complete: %d graded (%d W / %d L / %d P / %d V)",
                summary["graded"],
                summary["won"],
                summary["lost"],
                summary["push"],
                summary["void"],
            )
        return summary

    # ── leg grading ─────────────────────────────────────────────────────

    def grade_leg(self, leg: BetLeg, game: dict) -> str:
        """Grade a single bet leg against a completed game.

        Returns ``'won'``, ``'lost'``, ``'push'``, or ``'void'``.
        """
        home_score = game.get("home_score")
        away_score = game.get("away_score")
        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")

        if home_score is None or away_score is None:
            return "void"

        home_score = float(home_score)
        away_score = float(away_score)
        margin = home_score - away_score  # positive = home won

        bet_type = leg.bet_type.lower()

        if bet_type == "winner":
            return self._grade_winner(leg, margin, home_team, away_team)
        elif bet_type == "spread":
            return self._grade_spread(leg, margin, home_team)
        elif bet_type == "total":
            return self._grade_total(leg, home_score + away_score)
        elif bet_type == "prop":
            return self._grade_prop(leg, game)
        elif bet_type == "draw":
            return self._grade_draw(margin)
        elif bet_type == "overtime":
            return self._grade_overtime(game)
        elif bet_type == "halftime_winner":
            return self._grade_halftime_winner(leg, game, home_team, away_team)
        elif bet_type in ("esports_clean_sweep", "esports_map_total"):
            return self._grade_esports_market(leg, game)
        else:
            logger.warning("Unknown bet type %s for leg %s", bet_type, leg.game_id)
            return "void"

    @staticmethod
    def _grade_winner(
        leg: BetLeg, margin: float, home_team: str, away_team: str
    ) -> str:
        """Grade a moneyline/winner bet."""
        selection = leg.selection.strip().lower()
        home_won = margin > 0
        away_won = margin < 0

        if margin == 0:
            return "push"

        # Try to match selection against home or away team
        if selection == home_team.lower() or "home" in selection:
            return "won" if home_won else "lost"
        elif selection == away_team.lower() or "away" in selection:
            return "won" if away_won else "lost"

        # Fallback: if selection partially matches team name
        if home_team.lower() in selection or selection in home_team.lower():
            return "won" if home_won else "lost"
        if away_team.lower() in selection or selection in away_team.lower():
            return "won" if away_won else "lost"

        logger.warning(
            "Cannot match winner selection '%s' to %s/%s",
            leg.selection,
            home_team,
            away_team,
        )
        return "void"

    @staticmethod
    def _grade_spread(leg: BetLeg, margin: float, home_team: str) -> str:
        """Grade a spread bet.

        The ``selection`` field contains the team and the spread line, e.g.
        "LAL -3.5".  The ``line`` field stores the numeric spread.
        """
        line = leg.line
        if line is None:
            return "void"

        selection_lower = leg.selection.lower()
        is_home = home_team.lower() in selection_lower

        # Adjusted margin for the picked side
        if is_home:
            adjusted = margin + line  # line is already signed
        else:
            adjusted = -margin + line

        if adjusted > 0:
            return "won"
        elif adjusted < 0:
            return "lost"
        return "push"

    @staticmethod
    def _grade_total(leg: BetLeg, actual_total: float) -> str:
        """Grade an over/under total bet."""
        line = leg.line
        if line is None:
            return "void"

        selection_lower = leg.selection.lower()
        is_over = "over" in selection_lower

        diff = actual_total - line
        if diff == 0:
            return "push"
        if is_over:
            return "won" if diff > 0 else "lost"
        else:
            return "won" if diff < 0 else "lost"

    @staticmethod
    def _grade_prop(leg: BetLeg, game: dict) -> str:
        """Grade a player prop bet.

        Looks for player stats in the game data.  If player stats are not
        available in the game response, the bet is voided.
        """
        # Player props require box score data which may be nested
        # under "player_stats" or "box_score" in the game dict
        player_stats = game.get("player_stats", game.get("box_score", []))
        if not player_stats:
            return "void"

        target_player = (leg.player_id or "").lower()
        target_name = (leg.player_name or "").lower()
        market = (leg.prop_market or "").lower()
        line = leg.line

        if line is None:
            return "void"

        # Find matching player
        stat_value: float | None = None
        for ps in player_stats if isinstance(player_stats, list) else [player_stats]:
            pid = str(ps.get("player_id", "")).lower()
            pname = str(ps.get("player_name", "")).lower()

            if target_player and pid == target_player:
                stat_value = _extract_stat(ps, market)
                break
            if target_name and target_name in pname:
                stat_value = _extract_stat(ps, market)
                break

        if stat_value is None:
            return "void"

        selection_lower = leg.selection.lower()
        is_over = "over" in selection_lower

        diff = stat_value - line
        if diff == 0:
            return "push"
        if is_over:
            return "won" if diff > 0 else "lost"
        return "won" if diff < 0 else "lost"

    # ── Extra-market graders ────────────────────────────────────────────────

    @staticmethod
    def _grade_draw(margin: float) -> str:
        """Grade a draw/tie bet: won if final scores are equal."""
        return "won" if margin == 0.0 else "lost"

    @staticmethod
    def _grade_overtime(game: dict) -> str:
        """Grade an OT bet: won if game went to overtime."""
        ot_score = (game.get("home_ot") or 0) + (game.get("away_ot") or 0)
        if ot_score > 0:
            return "won"
        if game.get("overtime") or game.get("went_to_ot"):
            return "won"
        return "lost"

    @staticmethod
    def _grade_halftime_winner(
        leg: BetLeg, game: dict, home_team: str, away_team: str
    ) -> str:
        """Grade a halftime winner bet using q1+q2 scores."""
        ht_home = (game.get("home_q1") or 0) + (game.get("home_q2") or 0)
        ht_away = (game.get("away_q1") or 0) + (game.get("away_q2") or 0)
        if ht_home == 0 and ht_away == 0:
            ht_home = game.get("halftime_home_score") or 0
            ht_away = game.get("halftime_away_score") or 0
        if ht_home == 0 and ht_away == 0:
            return "void"
        ht_margin = float(ht_home) - float(ht_away)
        selection = leg.selection.lower()
        if ht_margin > 0 and (home_team.lower() in selection or selection == "home"):
            return "won"
        if ht_margin < 0 and (away_team.lower() in selection or selection == "away"):
            return "won"
        if ht_margin == 0:
            return "push"
        return "lost"

    @staticmethod
    def _grade_esports_market(leg: BetLeg, game: dict) -> str:
        """Grade esports map-based markets (clean sweep / map total)."""
        home_maps = float(game.get("home_score") or 0)
        away_maps = float(game.get("away_score") or 0)
        total_maps = home_maps + away_maps
        if total_maps == 0:
            return "void"

        bet_type = leg.bet_type.lower()
        if bet_type == "esports_clean_sweep":
            return "won" if min(home_maps, away_maps) == 0 else "lost"

        if bet_type == "esports_map_total":
            line = leg.line if leg.line is not None else 2.5
            selection = leg.selection.lower()
            is_over = "over" in selection
            diff = total_maps - line
            if diff == 0:
                return "push"
            return "won" if (diff > 0) == is_over else "lost"

        return "void"

    # ── P&L settlement ──────────────────────────────────────────────────

    def _settle_bet(
        self, bet: PaperBet, leg_results: list[str]
    ) -> tuple[str, float]:
        """Determine final bet status and P&L from leg results.

        Returns ``(status, pnl_units)``.
        """
        if not leg_results:
            return "void", 0.0

        # Check for voids
        void_count = leg_results.count("void")
        if void_count == len(leg_results):
            return "void", 0.0

        # Remove void/push legs for parlay settlement
        active_results = [r for r in leg_results if r not in ("void",)]

        if not active_results:
            return "void", 0.0

        # Single bet
        if bet.leg_count == 1:
            result = active_results[0]
            if result == "won":
                pnl = bet.stake_units * (bet.implied_odds - 1.0)
            elif result == "lost":
                pnl = -bet.stake_units
            else:  # push
                pnl = 0.0
            return result, round(pnl, 2)

        # Parlay: remove push legs, all remaining must win
        parlay_results = [r for r in active_results if r != "push"]
        if not parlay_results:
            return "push", 0.0

        if any(r == "lost" for r in parlay_results):
            return "lost", round(-bet.stake_units, 2)

        if all(r == "won" for r in parlay_results):
            # Recalculate parlay odds excluding push/void legs
            active_legs = [
                lg
                for lg, res in zip(bet.legs, leg_results)
                if res not in ("void", "push")
            ]
            combined_odds = 1.0
            for lg in active_legs:
                combined_odds *= lg.odds_decimal
            pnl = bet.stake_units * (combined_odds - 1.0)
            return "won", round(pnl, 2)

        return "lost", round(-bet.stake_units, 2)


# ── helper ──────────────────────────────────────────────────────────────────

_STAT_ALIASES: dict[str, list[str]] = {
    "points": ["pts", "points", "score"],
    "rebounds": ["reb", "rebounds", "total_rebounds"],
    "assists": ["ast", "assists"],
    "steals": ["stl", "steals"],
    "blocks": ["blk", "blocks"],
    "threes": ["fg3m", "three_pointers_made", "threes"],
    "pra": ["pts", "reb", "ast"],  # composite
    "strikeouts": ["so", "k", "strikeouts"],
    "hits": ["h", "hits"],
    "runs": ["r", "runs"],
    "passing_yards": ["pass_yds", "passing_yards"],
    "rushing_yards": ["rush_yds", "rushing_yards"],
    "receiving_yards": ["rec_yds", "receiving_yards"],
    "goals": ["g", "goals"],
    "shots_on_goal": ["sog", "shots_on_goal"],
    "saves": ["sv", "saves"],
}


def _extract_stat(player_stats: dict, market: str) -> float | None:
    """Extract a stat value from a player's stats dict by market name."""
    market_lower = market.lower().replace(" ", "_")

    # Handle composite stats (e.g., PRA = points + rebounds + assists)
    if market_lower == "pra":
        aliases = _STAT_ALIASES.get("pra", [])
        total = 0.0
        found_any = False
        for alias in aliases:
            val = player_stats.get(alias)
            if val is not None:
                total += float(val)
                found_any = True
        return total if found_any else None

    # Try direct lookup
    val = player_stats.get(market_lower)
    if val is not None:
        return float(val)

    # Try aliases
    aliases = _STAT_ALIASES.get(market_lower, [])
    for alias in aliases:
        val = player_stats.get(alias)
        if val is not None:
            return float(val)

    return None
