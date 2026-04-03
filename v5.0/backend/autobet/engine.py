"""Core betting engine — candidate selection, Kelly sizing, parlay construction.

This is the heart of the autobet system.  Each cycle:
1. Fetches games, predictions, and odds from the v5.0 backend API.
2. Filters candidates by confidence + edge thresholds (sport/bet-type aware).
3. Assigns conviction tiers (A = strong, B = moderate).
4. Sizes bets via fractional Kelly criterion.
5. Constructs parlays from Tier-A candidates.
6. Optionally builds a daily lotto ticket and ladder challenge.
"""

from __future__ import annotations

import itertools
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .config import AutobetConfig
from .ledger import Ledger
from .meta_evaluator import MetaEvaluator
from .models import BetCandidate, BetLeg, PaperBet

logger = logging.getLogger("autobet.engine")

# ── Odds conversion helpers ─────────────────────────────────────────────────


def american_to_decimal(american: int) -> float:
    """Convert American odds to decimal odds.

    >>> american_to_decimal(-110)
    1.9090909090909092
    >>> american_to_decimal(150)
    2.5
    """
    if american >= 100:
        return 1.0 + american / 100.0
    elif american <= -100:
        return 1.0 + 100.0 / abs(american)
    return 2.0  # even money fallback


def american_to_implied_prob(american: int) -> float:
    """Convert American odds to implied probability (no-vig).

    >>> round(american_to_implied_prob(-110), 4)
    0.5238
    >>> round(american_to_implied_prob(150), 4)
    0.4
    """
    if american <= -100:
        return abs(american) / (abs(american) + 100.0)
    elif american >= 100:
        return 100.0 / (american + 100.0)
    return 0.5


def decimal_to_implied_prob(decimal_odds: float) -> float:
    """Implied probability from decimal odds."""
    if decimal_odds <= 1.0:
        return 1.0
    return 1.0 / decimal_odds


# ── HTTP client ──────────────────────────────────────────────────────────────

_CLIENT: httpx.AsyncClient | None = None


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None or _CLIENT.is_closed:
        _CLIENT = httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0))
    return _CLIENT


async def _backend_get(
    config: AutobetConfig, path: str, **params: Any
) -> dict:
    """GET from the v5.0 backend API with retries."""
    url = f"{config.backend_url.rstrip('/')}{path}"
    headers = {}
    if config.internal_api_key:
        headers["X-API-Key"] = config.internal_api_key

    client = await _get_client()
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPError, Exception) as exc:
            last_err = exc
            if attempt < 2:
                import asyncio

                await asyncio.sleep(2 ** attempt)
                logger.warning("Backend retry %d for %s: %s", attempt + 1, path, exc)
    logger.error("Backend request failed after 3 attempts: %s – %s", path, last_err)
    return {}


# ═══════════════════════════════════════════════════════════════════════════════
# BettingEngine
# ═══════════════════════════════════════════════════════════════════════════════


class BettingEngine:
    """Main bet selection and sizing engine."""

    def __init__(self, config: AutobetConfig, ledger: Ledger):
        self.config = config
        self.ledger = ledger
        self.meta = MetaEvaluator(config, ledger)

    # ── public entry point ──────────────────────────────────────────────

    async def run_cycle(self) -> list[PaperBet]:
        """Execute one full betting cycle. Returns list of placed bets."""
        if not self.config.enabled:
            logger.debug("Autobet disabled, skipping cycle")
            return []

        placed: list[PaperBet] = []
        all_candidates: list[BetCandidate] = []

        for sport in self.config.sports:
            try:
                candidates = await self._process_sport(sport)
                if not candidates:
                    continue

                # Enforce per-sport daily limit
                today_sport = self.ledger.get_today_bets(sport=sport)
                core_today = [b for b in today_sport if b.strategy == "core"]
                remaining = self.config.max_bets_per_sport - len(core_today)
                if remaining <= 0:
                    logger.debug(
                        "%s: daily limit reached (%d), skipping",
                        sport,
                        self.config.max_bets_per_sport,
                    )
                    continue

                # Size and place the best candidates
                for cand in candidates[:remaining]:
                    stake = self.size_bet(cand)
                    if stake < self.config.min_stake_units:
                        continue

                    bet = PaperBet(
                        id=PaperBet.new_id(),
                        placed_at=PaperBet.now_iso(),
                        sport=sport,
                        bet_type=cand.leg.bet_type,
                        legs=[cand.leg],
                        stake_units=round(stake, 2),
                        model_confidence=cand.leg.model_confidence,
                        model_edge=cand.leg.model_edge,
                        implied_odds=cand.leg.odds_decimal,
                        rationale=cand.rationale,
                        strategy="core",
                    )
                    self.ledger.place_bet(bet)
                    placed.append(bet)

                all_candidates.extend(candidates)

            except Exception as exc:
                logger.error("Error processing %s: %s", sport, exc, exc_info=True)

        # ── Parlays ─────────────────────────────────────────────────────
        if self.config.parlay_enabled and all_candidates:
            try:
                parlays = self.build_parlays(all_candidates)
                for p in parlays:
                    self.ledger.place_bet(p)
                    placed.append(p)
            except Exception as exc:
                logger.error("Parlay construction error: %s", exc, exc_info=True)

        # ── Lotto ───────────────────────────────────────────────────────
        if self.config.lotto_enabled and all_candidates:
            try:
                lotto = self.build_lotto(all_candidates)
                if lotto:
                    self.ledger.place_bet(lotto)
                    placed.append(lotto)
            except Exception as exc:
                logger.error("Lotto construction error: %s", exc, exc_info=True)

        # ── Ladder ──────────────────────────────────────────────────────
        if self.config.ladder_enabled and all_candidates:
            try:
                ladder = self.build_ladder(all_candidates)
                if ladder:
                    self.ledger.place_bet(ladder)
                    placed.append(ladder)
            except Exception as exc:
                logger.error("Ladder construction error: %s", exc, exc_info=True)

        if placed:
            logger.info("Cycle complete: %d bets placed", len(placed))
        else:
            logger.debug("Cycle complete: no qualifying bets")

        return placed

    # ── sport processing ────────────────────────────────────────────────

    async def _process_sport(self, sport: str) -> list[BetCandidate]:
        """Fetch data and extract candidates for a single sport."""
        games_resp = await _backend_get(self.config, f"/v1/{sport}/games")
        preds_resp = await _backend_get(self.config, f"/v1/{sport}/predictions")
        odds_resp = await _backend_get(self.config, f"/v1/{sport}/odds")

        games = games_resp.get("data", [])
        predictions = preds_resp.get("data", [])
        odds = odds_resp.get("data", [])

        if not predictions or not odds:
            logger.debug("%s: no predictions or odds available", sport)
            return []

        eligible_games = self._filter_games_by_timing(games)
        if not eligible_games:
            logger.debug("%s: no games in timing window", sport)
            return []

        eligible_ids = {g["id"] for g in eligible_games}
        preds_filtered = [p for p in predictions if p.get("game_id") in eligible_ids]

        return self.select_bets(preds_filtered, odds, games, sport)

    def _filter_games_by_timing(self, games: list[dict]) -> list[dict]:
        """Filter games based on the configured timing mode."""
        now = datetime.now(timezone.utc)
        result = []

        for g in games:
            status = g.get("status", "")
            if status in ("final", "postponed", "cancelled"):
                continue
            if status == "in_progress" and not self.config.allow_live:
                continue

            start_str = g.get("start_time")
            if not start_str:
                if self.config.timing_mode == "anytime":
                    result.append(g)
                continue

            try:
                start = datetime.fromisoformat(
                    start_str.replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                continue

            if self.config.timing_mode == "window":
                lo, hi = self.config.pregame_window_minutes
                earliest = start - timedelta(minutes=hi)
                latest = start - timedelta(minutes=lo)
                if earliest <= now <= latest:
                    result.append(g)
            elif self.config.timing_mode == "same_day":
                if start.date() == now.date() and start > now:
                    result.append(g)
            else:  # "anytime"
                if start > now:
                    result.append(g)

        return result

    # ── candidate selection ─────────────────────────────────────────────

    def select_bets(
        self,
        predictions: list[dict],
        odds: list[dict],
        games: list[dict],
        sport: str,
    ) -> list[BetCandidate]:
        """Apply confidence and edge filters to produce ranked candidates."""
        # Index odds by game_id for fast lookup
        odds_by_game: dict[str, list[dict]] = {}
        for o in odds:
            gid = o.get("game_id", "")
            odds_by_game.setdefault(gid, []).append(o)

        # Index games by id
        games_by_id: dict[str, dict] = {g["id"]: g for g in games}

        candidates: list[BetCandidate] = []

        for pred in predictions:
            game_id = pred.get("game_id", "")
            game_odds = odds_by_game.get(game_id, [])
            if not game_odds:
                continue

            game = games_by_id.get(game_id, {})
            home_team = game.get("home_team", pred.get("home_team", "HOME"))
            away_team = game.get("away_team", pred.get("away_team", "AWAY"))

            # ── Winner bets ─────────────────────────────────────────────
            home_prob = pred.get("home_win_prob")
            away_prob = pred.get("away_win_prob")
            if home_prob is not None and away_prob is not None:
                # Pick the side with higher model probability
                if home_prob >= away_prob:
                    pick_team, pick_prob = home_team, home_prob
                    best = self._best_odds_for(
                        game_odds, "h2h_home", sport
                    )
                else:
                    pick_team, pick_prob = away_team, away_prob
                    best = self._best_odds_for(
                        game_odds, "h2h_away", sport
                    )

                if best:
                    cand = self._evaluate_candidate(
                        sport=sport,
                        game_id=game_id,
                        bet_type="winner",
                        selection=pick_team,
                        line=None,
                        model_prob=pick_prob,
                        decimal_odds=best["odds"],
                        american_odds=best.get("american", 0),
                        bookmaker=best["bookmaker"],
                        home_team=home_team,
                        away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)

            # ── Spread bets ─────────────────────────────────────────────
            pred_spread = pred.get("predicted_spread")
            if pred_spread is not None:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    spread_home = odds_row.get("spread_home")
                    spread_home_line = odds_row.get("spread_home_line")
                    spread_away = odds_row.get("spread_away")
                    spread_away_line = odds_row.get("spread_away_line")

                    if spread_home is not None and spread_home_line is not None:
                        # Home covers if actual margin > -spread
                        # Model says margin will be pred_spread (negative = home favored)
                        cover_prob = self._spread_cover_prob(
                            pred_spread, spread_home, pred.get("confidence", 0.5)
                        )
                        if cover_prob > 0.5:
                            dec = self._american_line_to_decimal(spread_home_line)
                            cand = self._evaluate_candidate(
                                sport=sport,
                                game_id=game_id,
                                bet_type="spread",
                                selection=f"{home_team} {spread_home:+.1f}",
                                line=spread_home,
                                model_prob=cover_prob,
                                decimal_odds=dec,
                                american_odds=int(spread_home_line),
                                bookmaker=bk,
                                home_team=home_team,
                                away_team=away_team,
                            )
                            if cand:
                                candidates.append(cand)
                                break  # one spread bet per game

                        if spread_away is not None and spread_away_line is not None:
                            away_cover = 1.0 - cover_prob
                            if away_cover > 0.5:
                                dec = self._american_line_to_decimal(spread_away_line)
                                cand = self._evaluate_candidate(
                                    sport=sport,
                                    game_id=game_id,
                                    bet_type="spread",
                                    selection=f"{away_team} {spread_away:+.1f}",
                                    line=spread_away,
                                    model_prob=away_cover,
                                    decimal_odds=dec,
                                    american_odds=int(spread_away_line),
                                    bookmaker=bk,
                                    home_team=home_team,
                                    away_team=away_team,
                                )
                                if cand:
                                    candidates.append(cand)
                                    break

            # ── Total (over/under) bets ─────────────────────────────────
            pred_total = pred.get("predicted_total")
            if pred_total is not None:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    total_line = odds_row.get("total_over")
                    total_odds_line = odds_row.get("total_line")
                    if total_line is None or total_odds_line is None:
                        continue

                    diff = pred_total - total_line
                    if abs(diff) < 0.5:
                        continue  # too close to call

                    if diff > 0:
                        selection, model_prob = "over", min(
                            0.95, 0.5 + abs(diff) / (total_line * 0.1 + 5)
                        )
                    else:
                        selection, model_prob = "under", min(
                            0.95, 0.5 + abs(diff) / (total_line * 0.1 + 5)
                        )

                    # Use the overall confidence as a soft cap
                    overall_conf = pred.get("confidence", 0.5)
                    model_prob = min(model_prob, overall_conf + 0.10)

                    dec = self._american_line_to_decimal(total_odds_line)
                    cand = self._evaluate_candidate(
                        sport=sport,
                        game_id=game_id,
                        bet_type="total",
                        selection=f"{selection} {total_line}",
                        line=total_line,
                        model_prob=model_prob,
                        decimal_odds=dec,
                        american_odds=int(total_odds_line),
                        bookmaker=bk,
                        home_team=home_team,
                        away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break  # one total bet per game

            # ── Draw bets (soccer, NFL OT, etc.) ────────────────────────────
            draw_prob = pred.get("draw_prob")
            if draw_prob is not None and draw_prob > 0.01:  # skip near-zero
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    draw_odds = odds_row.get("h2h_draw") or odds_row.get("draw_odds")
                    if draw_odds is None:
                        continue
                    dec = float(draw_odds) if draw_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport,
                        game_id=game_id,
                        bet_type="draw",
                        selection="draw",
                        line=None,
                        model_prob=draw_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk,
                        home_team=home_team,
                        away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Overtime / Draw-no-bet ────────────────────────────────────
            ot_prob = pred.get("ot_prob")
            if ot_prob is not None and ot_prob > 0.05:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    ot_odds = odds_row.get("ot_odds") or odds_row.get("overtime_odds")
                    if ot_odds is None:
                        continue
                    dec = float(ot_odds) if ot_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport,
                        game_id=game_id,
                        bet_type="overtime",
                        selection="overtime",
                        line=None,
                        model_prob=ot_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk,
                        home_team=home_team,
                        away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Halftime winner ───────────────────────────────────────────
            ht_home_prob = pred.get("halftime_home_win_prob")
            ht_away_prob = pred.get("halftime_away_win_prob")
            if ht_home_prob is not None and ht_away_prob is not None:
                if ht_home_prob >= ht_away_prob:
                    ht_team, ht_prob = home_team, ht_home_prob
                    ht_odds_key = "h1_home_odds"
                else:
                    ht_team, ht_prob = away_team, ht_away_prob
                    ht_odds_key = "h1_away_odds"
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    ht_odds = odds_row.get(ht_odds_key) or odds_row.get("halftime_winner_odds")
                    if ht_odds is None:
                        continue
                    dec = float(ht_odds) if ht_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport,
                        game_id=game_id,
                        bet_type="halftime_winner",
                        selection=ht_team,
                        line=None,
                        model_prob=ht_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk,
                        home_team=home_team,
                        away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Esports extra markets ─────────────────────────────────────
            # Clean sweep: winner takes all maps (2-0, 3-0)
            cs_prob = pred.get("esports_clean_sweep_prob")
            if cs_prob is not None:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    cs_odds = odds_row.get("clean_sweep_odds") or odds_row.get("2_0_odds")
                    if cs_odds is None:
                        continue
                    dec = float(cs_odds) if cs_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport,
                        game_id=game_id,
                        bet_type="esports_clean_sweep",
                        selection="clean_sweep",
                        line=None,
                        model_prob=cs_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk,
                        home_team=home_team,
                        away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # Map total over 2 (series goes to 3+ maps)
            mt_o2_prob = pred.get("esports_map_total_over2_prob")
            if mt_o2_prob is not None:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    mt_odds = odds_row.get("maps_over2_odds") or odds_row.get("series_3plus_odds")
                    if mt_odds is None:
                        continue
                    dec = float(mt_odds) if mt_odds > 1.0 else None
                    if dec is None:
                        continue
                    pick_side = "over 2" if mt_o2_prob >= 0.5 else "under 2"
                    pick_prob = mt_o2_prob if mt_o2_prob >= 0.5 else 1.0 - mt_o2_prob
                    cand = self._evaluate_candidate(
                        sport=sport,
                        game_id=game_id,
                        bet_type="esports_map_total",
                        selection=f"maps {pick_side}",
                        line=2.5,
                        model_prob=pick_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk,
                        home_team=home_team,
                        away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # Decider map: series goes to final map (bo3 map3 / bo5 map5)
            dec_prob = pred.get("esports_decider_map_prob")
            if dec_prob is not None and dec_prob > 0.15:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    dec_odds = odds_row.get("decider_map_odds") or odds_row.get("map3_odds")
                    if dec_odds is None:
                        continue
                    dec_o = float(dec_odds) if dec_odds > 1.0 else None
                    if dec_o is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport, game_id=game_id, bet_type="esports_decider_map",
                        selection="decider_map", line=None, model_prob=dec_prob,
                        decimal_odds=dec_o,
                        american_odds=int((dec_o - 1) * 100) if dec_o >= 2.0 else int(-100 / (dec_o - 1)),
                        bookmaker=bk, home_team=home_team, away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # Home/Away dominant win (clean sweep 2-0 or 3-0)
            for dom_key, dom_side in [
                ("esports_home_dominant_prob", home_team),
                ("esports_away_dominant_prob", away_team),
            ]:
                dom_prob = pred.get(dom_key)
                if dom_prob is not None and dom_prob > 0.15:
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        side = "home" if "home" in dom_key else "away"
                        d_odds = odds_row.get(f"{side}_clean_sweep_odds") or odds_row.get(f"{side}_2_0_odds")
                        if d_odds is None:
                            continue
                        d_o = float(d_odds) if d_odds > 1.0 else None
                        if d_o is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="esports_dominant_win",
                            selection=f"{dom_side}_clean_sweep", line=None, model_prob=dom_prob,
                            decimal_odds=d_o,
                            american_odds=int((d_o - 1) * 100) if d_o >= 2.0 else int(-100 / (d_o - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── BTTS (both teams to score) ────────────────────────────────
            btts_p = pred.get("btts_prob")
            if btts_p is not None:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    btts_odds = odds_row.get("btts_yes_odds") or odds_row.get("btts_odds")
                    if btts_odds is None:
                        continue
                    dec = float(btts_odds) if btts_odds > 1.0 else None
                    if dec is None:
                        continue
                    pick_prob = btts_p if btts_p >= 0.5 else 1.0 - btts_p
                    selection = "btts_yes" if btts_p >= 0.5 else "btts_no"
                    cand = self._evaluate_candidate(
                        sport=sport, game_id=game_id, bet_type="btts",
                        selection=selection, line=None, model_prob=pick_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk, home_team=home_team, away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Clean sheet ───────────────────────────────────────────────
            for cs_side, cs_key, odds_key in [
                ("home", "home_clean_sheet_prob", "home_clean_sheet_odds"),
                ("away", "away_clean_sheet_prob", "away_clean_sheet_odds"),
            ]:
                cs_p = pred.get(cs_key)
                if cs_p is not None and cs_p > 0.1:
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        cs_odds = odds_row.get(odds_key)
                        if cs_odds is None:
                            continue
                        dec = float(cs_odds) if cs_odds > 1.0 else None
                        if dec is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="clean_sheet",
                            selection=f"{cs_side}_clean_sheet", line=None, model_prob=cs_p,
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── First / Last score ────────────────────────────────────────
            for fs_key, bet_t in [
                ("first_score_home_prob", "first_score"),
                ("last_score_home_prob", "last_score"),
            ]:
                fs_p = pred.get(fs_key)
                if fs_p is not None:
                    pick_prob = fs_p if fs_p >= 0.5 else 1.0 - fs_p
                    pick_team = home_team if fs_p >= 0.5 else away_team
                    pick_key = ("first_score_home_odds" if fs_p >= 0.5 else "first_score_away_odds") \
                        if bet_t == "first_score" else \
                        ("last_score_home_odds" if fs_p >= 0.5 else "last_score_away_odds")
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        odds_val = odds_row.get(pick_key) or odds_row.get(f"{bet_t}_odds")
                        if odds_val is None:
                            continue
                        dec = float(odds_val) if odds_val > 1.0 else None
                        if dec is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type=bet_t,
                            selection=pick_team, line=None, model_prob=pick_prob,
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── Second half winner ────────────────────────────────────────
            sh_home_p = pred.get("second_half_home_win_prob")
            if sh_home_p is not None:
                sh_away_p = 1.0 - sh_home_p
                if sh_home_p >= sh_away_p:
                    sh_team, sh_prob = home_team, sh_home_p
                    sh_odds_key = "second_half_home_odds"
                else:
                    sh_team, sh_prob = away_team, sh_away_p
                    sh_odds_key = "second_half_away_odds"
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    sh_odds = odds_row.get(sh_odds_key) or odds_row.get("second_half_winner_odds")
                    if sh_odds is None:
                        continue
                    dec = float(sh_odds) if sh_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport, game_id=game_id, bet_type="second_half_winner",
                        selection=sh_team, line=None, model_prob=sh_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk, home_team=home_team, away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Regulation result (3-way: home / draw / away) ─────────────
            reg_h = pred.get("regulation_home_win_prob")
            reg_d = pred.get("regulation_draw_prob")
            reg_a = pred.get("regulation_away_win_prob")
            if reg_h is not None and reg_d is not None and reg_a is not None:
                best_p = max(reg_h, reg_d, reg_a)
                if reg_h == best_p:
                    reg_sel, reg_prob = "regulation_home", reg_h
                    reg_odds_key = "regulation_home_odds"
                elif reg_d == best_p:
                    reg_sel, reg_prob = "regulation_draw", reg_d
                    reg_odds_key = "regulation_draw_odds"
                else:
                    reg_sel, reg_prob = "regulation_away", reg_a
                    reg_odds_key = "regulation_away_odds"
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    reg_odds = odds_row.get(reg_odds_key)
                    if reg_odds is None:
                        continue
                    dec = float(reg_odds) if reg_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport, game_id=game_id, bet_type="regulation_result",
                        selection=reg_sel, line=None, model_prob=reg_prob,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk, home_team=home_team, away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Comeback bets ─────────────────────────────────────────────
            for cb_key, cb_team in [
                ("comeback_home_prob", home_team),
                ("comeback_away_prob", away_team),
            ]:
                cb_p = pred.get(cb_key)
                if cb_p is not None and cb_p > 0.15:
                    side = "home" if "home" in cb_key else "away"
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        cb_odds = odds_row.get(f"comeback_{side}_odds") or odds_row.get("comeback_odds")
                        if cb_odds is None:
                            continue
                        dec = float(cb_odds) if cb_odds > 1.0 else None
                        if dec is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="comeback",
                            selection=f"{cb_team}_comeback", line=None, model_prob=cb_p,
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── Margin band bets ──────────────────────────────────────────
            margin_bands = pred.get("margin_band_probs")
            if margin_bands:
                best_band = max(margin_bands, key=lambda k: margin_bands[k])
                best_bp = margin_bands[best_band]
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    mb_odds = odds_row.get(f"margin_{best_band}_odds") or odds_row.get("margin_band_odds")
                    if mb_odds is None:
                        continue
                    dec = float(mb_odds) if mb_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport, game_id=game_id, bet_type="margin_band",
                        selection=f"margin_{best_band}", line=None, model_prob=best_bp,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk, home_team=home_team, away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Large margin win ──────────────────────────────────────────
            lm_p = pred.get("large_margin_prob")
            if lm_p is not None and lm_p > 0.1:
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    lm_odds = odds_row.get("large_margin_odds") or odds_row.get("dominant_win_odds")
                    if lm_odds is None:
                        continue
                    dec = float(lm_odds) if lm_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport, game_id=game_id, bet_type="large_margin",
                        selection="dominant_win", line=None, model_prob=lm_p,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk, home_team=home_team, away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Total band (low/mid/high) ──────────────────────────────────
            total_bands = pred.get("total_band_probs")
            if total_bands:
                best_tb = max(total_bands, key=lambda k: total_bands[k])
                best_tbp = total_bands[best_tb]
                for odds_row in game_odds:
                    bk = odds_row.get("bookmaker", "")
                    if bk.lower() in self.config.excluded_vendors:
                        continue
                    tb_odds = odds_row.get(f"total_{best_tb}_odds") or odds_row.get("total_band_odds")
                    if tb_odds is None:
                        continue
                    dec = float(tb_odds) if tb_odds > 1.0 else None
                    if dec is None:
                        continue
                    cand = self._evaluate_candidate(
                        sport=sport, game_id=game_id, bet_type="total_band",
                        selection=f"total_{best_tb}", line=None, model_prob=best_tbp,
                        decimal_odds=dec,
                        american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                        bookmaker=bk, home_team=home_team, away_team=away_team,
                    )
                    if cand:
                        candidates.append(cand)
                        break

            # ── Per-period/quarter/inning markets ─────────────────────────
            period_preds = pred.get("period_predictions")
            if period_preds:
                period_label = pred.get("period_label", "period")
                for pp in period_preds:
                    pi = pp.get("period", 1)
                    # Period winner
                    pp_home = pp.get("home_win_prob")
                    pp_away = pp.get("away_win_prob")
                    if pp_home is not None and pp_away is not None:
                        pick_p = pp_home if pp_home >= pp_away else pp_away
                        pick_t = home_team if pp_home >= pp_away else away_team
                        side = "home" if pp_home >= pp_away else "away"
                        for odds_row in game_odds:
                            bk = odds_row.get("bookmaker", "")
                            if bk.lower() in self.config.excluded_vendors:
                                continue
                            pp_odds = odds_row.get(f"{period_label}{pi}_{side}_odds") \
                                or odds_row.get(f"period{pi}_winner_odds")
                            if pp_odds is None:
                                continue
                            dec = float(pp_odds) if pp_odds > 1.0 else None
                            if dec is None:
                                continue
                            cand = self._evaluate_candidate(
                                sport=sport, game_id=game_id,
                                bet_type=f"{period_label}_winner",
                                selection=f"{period_label}{pi}_{pick_t}",
                                line=None, model_prob=pick_p,
                                decimal_odds=dec,
                                american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                                bookmaker=bk, home_team=home_team, away_team=away_team,
                            )
                            if cand:
                                candidates.append(cand)
                                break
                    # Period total
                    pp_total = pp.get("total")
                    if pp_total is not None:
                        for odds_row in game_odds:
                            bk = odds_row.get("bookmaker", "")
                            if bk.lower() in self.config.excluded_vendors:
                                continue
                            pt_line = odds_row.get(f"{period_label}{pi}_total_line")
                            pt_over = odds_row.get(f"{period_label}{pi}_over_odds")
                            pt_under = odds_row.get(f"{period_label}{pi}_under_odds")
                            if pt_line is None or pt_over is None:
                                continue
                            over = pp_total > float(pt_line)
                            pick_prob = 0.55 if over else 0.55  # base prob without exact model
                            dec = float(pt_over) if over else float(pt_under or pt_over)
                            if dec is None or dec <= 1.0:
                                continue
                            cand = self._evaluate_candidate(
                                sport=sport, game_id=game_id,
                                bet_type=f"{period_label}_total",
                                selection=f"{period_label}{pi}_{'over' if over else 'under'}",
                                line=float(pt_line), model_prob=pick_prob,
                                decimal_odds=dec,
                                american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                                bookmaker=bk, home_team=home_team, away_team=away_team,
                            )
                            if cand:
                                candidates.append(cand)
                                break

            # ── UFC method of victory ──────────────────────────────────────
            if sport == "ufc":
                for ufc_key, ufc_bet in [
                    ("ko_tko_prob", "ko_tko"), ("submission_prob", "submission"),
                    ("decision_prob", "decision"),
                ]:
                    ufc_p = pred.get(ufc_key)
                    if ufc_p is not None and ufc_p > 0.1:
                        for odds_row in game_odds:
                            bk = odds_row.get("bookmaker", "")
                            if bk.lower() in self.config.excluded_vendors:
                                continue
                            ufc_odds = odds_row.get(f"{ufc_bet}_odds") or odds_row.get(f"method_{ufc_bet}_odds")
                            if ufc_odds is None:
                                continue
                            dec = float(ufc_odds) if ufc_odds > 1.0 else None
                            if dec is None:
                                continue
                            cand = self._evaluate_candidate(
                                sport=sport, game_id=game_id, bet_type="method_of_victory",
                                selection=ufc_bet, line=None, model_prob=ufc_p,
                                decimal_odds=dec,
                                american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                                bookmaker=bk, home_team=home_team, away_team=away_team,
                            )
                            if cand:
                                candidates.append(cand)
                                break

            # ── Tennis straight sets ───────────────────────────────────────
            if sport in ("atp", "wta"):
                ss_p = pred.get("straight_sets_prob")
                if ss_p is not None and ss_p > 0.1:
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        ss_odds = odds_row.get("straight_sets_odds") or odds_row.get("clean_sweep_odds")
                        if ss_odds is None:
                            continue
                        dec = float(ss_odds) if ss_odds > 1.0 else None
                        if dec is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="straight_sets",
                            selection="straight_sets", line=None, model_prob=ss_p,
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── NBA/NCAAB race-to-X points ─────────────────────────────────
            if sport in ("nba", "ncaab", "ncaaw", "wnba"):
                race_p = pred.get("race_to_20_prob")
                if race_p is not None and race_p > 0.1:
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        side = "home" if race_p >= 0.5 else "away"
                        r_odds = odds_row.get(f"race_to_20_{side}_odds") or odds_row.get("race_to_20_odds")
                        if r_odds is None:
                            continue
                        dec = float(r_odds) if r_odds > 1.0 else None
                        if dec is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="race_to_points",
                            selection=f"race_to_20_{side}", line=20.0, model_prob=max(race_p, 1 - race_p),
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── Soccer: exact goal band (0-1 / 2-3 / 4+) ─────────────────
            if sport in ("epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl"):
                total_goals_p = pred.get("total_goals")
                if total_goals_p is not None:
                    # 0-1 goals band
                    low_p = pred.get("goals_0_1_prob")
                    if low_p is not None and low_p > 0.2:
                        for odds_row in game_odds:
                            bk = odds_row.get("bookmaker", "")
                            if bk.lower() in self.config.excluded_vendors:
                                continue
                            lg_odds = odds_row.get("goals_0_1_odds") or odds_row.get("exact_goals_0_1_odds")
                            if lg_odds is None:
                                continue
                            dec = float(lg_odds) if lg_odds > 1.0 else None
                            if dec is None:
                                continue
                            cand = self._evaluate_candidate(
                                sport=sport, game_id=game_id, bet_type="exact_goals_band",
                                selection="0_to_1_goals", line=1.0, model_prob=low_p,
                                decimal_odds=dec,
                                american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                                bookmaker=bk, home_team=home_team, away_team=away_team,
                            )
                            if cand:
                                candidates.append(cand)
                                break

            # ── NFL/NCAAF: anytime touchdown scorer market ─────────────────
            if sport in ("nfl", "ncaaf"):
                td_scorer_key = "home_anytime_td_prob"
                td_p = pred.get(td_scorer_key)
                if td_p is not None and td_p > 0.3:
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        td_odds = odds_row.get("anytime_td_scorer_odds") or odds_row.get("td_scorer_odds")
                        if td_odds is None:
                            continue
                        dec = float(td_odds) if td_odds > 1.0 else None
                        if dec is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="anytime_td_scorer",
                            selection="home_team_anytime_td", line=None, model_prob=td_p,
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── MLB: first 5 innings winner ────────────────────────────────
            if sport == "mlb":
                f5_p = pred.get("first_5_innings_winner_prob")
                if f5_p is not None and f5_p > 0.1:
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        side = "home" if f5_p >= 0.5 else "away"
                        f5_odds = odds_row.get(f"first_5_innings_{side}_odds") or odds_row.get("first_5_winner_odds")
                        if f5_odds is None:
                            continue
                        dec = float(f5_odds) if f5_odds > 1.0 else None
                        if dec is None:
                            continue
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="first_5_innings_winner",
                            selection=f"first_5_{side}", line=None, model_prob=max(f5_p, 1 - f5_p),
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

            # ── NHL: first period total goals ──────────────────────────────
            if sport == "nhl":
                fp_total = pred.get("first_period_total_goals")
                if fp_total is not None:
                    for odds_row in game_odds:
                        bk = odds_row.get("bookmaker", "")
                        if bk.lower() in self.config.excluded_vendors:
                            continue
                        fp_line = odds_row.get("first_period_total_line", 1.5)
                        fp_over = odds_row.get("first_period_total_over_odds")
                        fp_under = odds_row.get("first_period_total_under_odds")
                        if fp_over is None:
                            continue
                        over = float(fp_total) > float(fp_line)
                        dec = float(fp_over) if over else float(fp_under or fp_over)
                        if dec is None or dec <= 1.0:
                            continue
                        # Use win probability as proxy for confidence
                        fp_prob = max(pred.get("win_prob", 0.55), 0.55)
                        cand = self._evaluate_candidate(
                            sport=sport, game_id=game_id, bet_type="first_period_total",
                            selection=f"first_period_{'over' if over else 'under'}",
                            line=float(fp_line), model_prob=fp_prob,
                            decimal_odds=dec,
                            american_odds=int((dec - 1) * 100) if dec >= 2.0 else int(-100 / (dec - 1)),
                            bookmaker=bk, home_team=home_team, away_team=away_team,
                        )
                        if cand:
                            candidates.append(cand)
                            break

        # Sort: S first, then A, B, C, D; within tier by edge descending
        _tier_order = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4}
        candidates.sort(key=lambda c: (_tier_order.get(c.tier, 9), -c.leg.model_edge))
        return candidates

    def _evaluate_candidate(
        self,
        *,
        sport: str,
        game_id: str,
        bet_type: str,
        selection: str,
        line: float | None,
        model_prob: float,
        decimal_odds: float,
        american_odds: int,
        bookmaker: str,
        home_team: str = "",
        away_team: str = "",
        player_id: str | None = None,
        player_name: str | None = None,
        prop_market: str | None = None,
    ) -> BetCandidate | None:
        """Evaluate a single candidate against thresholds. Returns None if rejected."""
        if decimal_odds <= 1.0:
            return None

        implied_prob = decimal_to_implied_prob(decimal_odds)
        edge = model_prob - implied_prob

        # Get thresholds (possibly dynamically adjusted)
        min_conf, min_edge = self.meta.get_adjusted_thresholds(sport, bet_type)

        if model_prob < min_conf:
            return None
        if edge < min_edge:
            return None

        # Determine 5-tier conviction bracket
        # S (Platinum) > A (Gold) > B (Silver) > C (Bronze) > D (Copper)
        c = self.config
        if model_prob >= c.tier_s_confidence and edge >= c.tier_s_edge:
            tier = "S"
        elif model_prob >= c.tier_a_confidence and edge >= c.tier_a_edge:
            tier = "A"
        elif model_prob >= c.tier_b_confidence and edge >= c.tier_b_edge:
            tier = "B"
        elif model_prob >= c.tier_c_confidence and edge >= c.tier_c_edge:
            tier = "C"
        else:
            tier = "D"

        ev_per_unit = model_prob * (decimal_odds - 1) - (1 - model_prob)

        leg = BetLeg(
            game_id=game_id,
            sport=sport,
            bet_type=bet_type,
            selection=selection,
            line=line,
            odds_american=american_odds,
            odds_decimal=decimal_odds,
            bookmaker=bookmaker,
            model_confidence=round(model_prob, 4),
            model_edge=round(edge, 4),
            home_team=home_team,
            away_team=away_team,
            player_id=player_id,
            player_name=player_name,
            prop_market=prop_market,
        )

        rationale = (
            f"{tier} {bet_type} {selection}: "
            f"conf={model_prob:.1%} edge={edge:.1%} "
            f"EV={ev_per_unit:+.3f}/unit @ {bookmaker}"
        )

        return BetCandidate(
            leg=leg,
            tier=tier,
            ev_per_unit=ev_per_unit,
            rationale=rationale,
        )

    # ── Kelly sizing ────────────────────────────────────────────────────

    def size_bet(self, candidate: BetCandidate) -> float:
        """Kelly criterion sizing with tier adjustment and clamping.

        Kelly formula:  f* = (b·p − q) / b
        where b = decimal_odds − 1, p = model_prob, q = 1 − p.

        We apply a fractional Kelly (default quarter-Kelly) and optionally
        reduce further for Tier B bets.
        """
        p = candidate.leg.model_confidence
        b = candidate.leg.odds_decimal - 1.0
        q = 1.0 - p

        if b <= 0:
            return 0.0

        full_kelly = (b * p - q) / b
        if full_kelly <= 0:
            return 0.0

        # Get effective bankroll
        if self.config.dynamic_bankroll:
            bankroll = self.ledger.get_bankroll(self.config.bankroll_dollars)
            bankroll = max(bankroll, self.config.bankroll_dollars * 0.25)
        else:
            bankroll = self.config.bankroll_dollars

        raw_stake = full_kelly * self.config.kelly_fraction * bankroll

        # 5-tier Kelly multiplier (S=1.0 → D=0.25)
        _tier_mult = {
            "S": self.config.tier_s_kelly_mult,
            "A": self.config.tier_a_kelly_mult,
            "B": self.config.tier_b_kelly_mult,
            "C": self.config.tier_c_kelly_mult,
            "D": self.config.tier_d_kelly_mult,
        }
        raw_stake *= _tier_mult.get(candidate.tier, self.config.tier_d_kelly_mult)

        # Clamp
        stake = max(self.config.min_stake_units, min(self.config.max_stake_units, raw_stake))

        candidate.kelly_stake = round(stake, 2)
        return stake

    # ── Parlay construction ─────────────────────────────────────────────

    def build_parlays(self, candidates: list[BetCandidate]) -> list[PaperBet]:
        """Construct optimal parlays from Tier-A candidates.

        Rules:
        - Only Tier A legs are eligible
        - No two legs from the same game
        - Respect max prop legs, max same direction fraction
        - Score by weighted combination of hit rate and payout
        """
        daily_parlays = self.ledger.get_daily_count(strategy="core", bet_type="parlay")
        if daily_parlays >= self.config.max_parlays_per_day:
            return []

        # Filter eligible legs: Tier S and A can form parlays
        eligible = [
            c
            for c in candidates
            if c.tier in ("S", "A")
            and c.leg.model_confidence >= self.config.min_parlay_confidence
            and c.leg.model_edge >= self.config.min_parlay_edge
        ]
        if len(eligible) < self.config.min_parlay_legs:
            return []

        best_parlays: list[PaperBet] = []
        remaining_slots = self.config.max_parlays_per_day - daily_parlays

        # Try all valid combinations from min to max legs
        for n_legs in range(self.config.min_parlay_legs, self.config.max_parlay_legs + 1):
            if len(eligible) < n_legs:
                break
            scored_combos = self._score_parlay_combos(eligible, n_legs)
            for combo, score in scored_combos:
                if len(best_parlays) >= remaining_slots:
                    break
                # Avoid overlapping games with already-selected parlays
                combo_game_ids = {c.leg.game_id for c in combo}
                overlaps = any(
                    combo_game_ids & {lg.game_id for lg in p.legs}
                    for p in best_parlays
                )
                if overlaps:
                    continue

                parlay = self._combo_to_parlay(combo)
                if parlay:
                    best_parlays.append(parlay)

        return best_parlays

    def _score_parlay_combos(
        self, eligible: list[BetCandidate], n_legs: int
    ) -> list[tuple[list[BetCandidate], float]]:
        """Score all valid N-leg combinations and return sorted (best first)."""
        results = []
        for combo in itertools.combinations(eligible, n_legs):
            combo_list = list(combo)
            # No duplicate games
            game_ids = [c.leg.game_id for c in combo_list]
            if len(set(game_ids)) != len(game_ids):
                continue

            # Max prop legs
            prop_count = sum(1 for c in combo_list if c.leg.bet_type == "prop")
            if prop_count > self.config.parlay_max_prop_legs:
                continue

            # Max same direction fraction
            directions = [c.leg.selection.split()[0].lower() if c.leg.selection else "" for c in combo_list]
            if directions:
                from collections import Counter

                most_common_count = Counter(directions).most_common(1)[0][1]
                if most_common_count / len(directions) > self.config.parlay_max_same_direction_fraction:
                    continue

            # Combined confidence and odds
            combined_conf = math.prod(c.leg.model_confidence for c in combo_list)
            combined_odds = math.prod(c.leg.odds_decimal for c in combo_list)

            if combined_conf < self.config.parlay_min_combined_confidence:
                continue

            combined_ev = combined_conf * (combined_odds - 1) - (1 - combined_conf)
            if combined_ev < 0:
                continue

            # Leg penalty
            penalty = self.config.parlay_leg_penalties.get(n_legs, 0.50)
            w = self.config.parlay_hit_rate_weight
            score = (w * combined_conf + (1 - w) * combined_ev) * penalty

            results.append((combo_list, score))

        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def _combo_to_parlay(self, combo: list[BetCandidate]) -> PaperBet | None:
        """Convert a scored combo into a PaperBet."""
        legs = [c.leg for c in combo]
        combined_conf = math.prod(lg.model_confidence for lg in legs)
        combined_odds = math.prod(lg.odds_decimal for lg in legs)
        combined_edge = combined_conf - decimal_to_implied_prob(combined_odds)

        # Size with Kelly on combined odds/confidence
        b = combined_odds - 1.0
        q = 1.0 - combined_conf
        full_kelly = (b * combined_conf - q) / b if b > 0 else 0.0
        if full_kelly <= 0:
            return None

        if self.config.dynamic_bankroll:
            bankroll = self.ledger.get_bankroll(self.config.bankroll_dollars)
            bankroll = max(bankroll, self.config.bankroll_dollars * 0.25)
        else:
            bankroll = self.config.bankroll_dollars

        raw_stake = full_kelly * self.config.kelly_fraction * bankroll
        stake = max(self.config.min_stake_units, min(self.config.max_stake_units, raw_stake))

        sports = list({lg.sport for lg in legs})
        sport_label = sports[0] if len(sports) == 1 else "multi"

        picks = ", ".join(f"{lg.selection}" for lg in legs)
        rationale = (
            f"Parlay ({len(legs)}L): {picks} | "
            f"conf={combined_conf:.1%} odds={combined_odds:.2f} "
            f"edge={combined_edge:.1%}"
        )

        return PaperBet(
            id=PaperBet.new_id(),
            placed_at=PaperBet.now_iso(),
            sport=sport_label,
            bet_type="parlay",
            legs=legs,
            stake_units=round(stake, 2),
            model_confidence=round(combined_conf, 4),
            model_edge=round(combined_edge, 4),
            implied_odds=round(combined_odds, 4),
            rationale=rationale,
            strategy="core",
        )

    # ── Lotto ticket ────────────────────────────────────────────────────

    def build_lotto(self, candidates: list[BetCandidate]) -> PaperBet | None:
        """Daily lotto ticket — relaxed thresholds, higher payout potential."""
        if self.config.lotto_once_per_day:
            if self.ledger.get_daily_count(strategy="lotto_daily") > 0:
                return None

        # Use relaxed thresholds for lotto
        lotto_legs = [
            c
            for c in candidates
            if c.leg.model_confidence >= self.config.lotto_min_confidence
            and c.leg.model_edge >= self.config.lotto_min_edge
        ]

        if len(lotto_legs) < self.config.lotto_min_legs:
            return None

        # Sort by edge and take the best legs
        lotto_legs.sort(key=lambda c: c.leg.model_edge, reverse=True)
        n_legs = min(self.config.lotto_max_legs, len(lotto_legs))
        selected = lotto_legs[:n_legs]

        # Ensure no duplicate games
        seen_games: set[str] = set()
        deduped: list[BetCandidate] = []
        for c in selected:
            if c.leg.game_id not in seen_games:
                seen_games.add(c.leg.game_id)
                deduped.append(c)

        if len(deduped) < self.config.lotto_min_legs:
            return None

        legs = [c.leg for c in deduped]
        combined_conf = math.prod(lg.model_confidence for lg in legs)
        combined_odds = math.prod(lg.odds_decimal for lg in legs)
        combined_edge = combined_conf - decimal_to_implied_prob(combined_odds)

        sports = list({lg.sport for lg in legs})
        sport_label = sports[0] if len(sports) == 1 else "multi"

        picks = ", ".join(f"{lg.selection}" for lg in legs)
        rationale = (
            f"Lotto ({len(legs)}L): {picks} | "
            f"conf={combined_conf:.1%} payout={combined_odds:.2f}x"
        )

        return PaperBet(
            id=PaperBet.new_id(),
            placed_at=PaperBet.now_iso(),
            sport=sport_label,
            bet_type="parlay",
            legs=legs,
            stake_units=self.config.lotto_stake_units,
            model_confidence=round(combined_conf, 4),
            model_edge=round(combined_edge, 4),
            implied_odds=round(combined_odds, 4),
            rationale=rationale,
            strategy="lotto_daily",
        )

    # ── Ladder challenge ────────────────────────────────────────────────

    def build_ladder(self, candidates: list[BetCandidate]) -> PaperBet | None:
        """Daily ladder challenge — sequential bets with escalating stakes.

        Selects a sequence of high-confidence legs ordered by start time
        (earliest first).  Each rung's hypothetical payout rolls into the
        next.  The full ladder is stored as a single multi-leg bet.
        """
        if self.config.ladder_once_per_day:
            if self.ledger.get_daily_count(strategy="ladder_daily") > 0:
                return None

        ladder_legs = [
            c
            for c in candidates
            if c.tier in ("S", "A")
            and c.leg.model_confidence >= self.config.ladder_min_confidence
            and c.leg.model_edge >= self.config.ladder_min_edge
        ]

        if not ladder_legs:
            return None

        # Sort by confidence descending, pick best unique games
        ladder_legs.sort(key=lambda c: c.leg.model_confidence, reverse=True)
        seen_games: set[str] = set()
        selected: list[BetCandidate] = []
        cumulative_payout = 1.0

        for c in ladder_legs:
            if c.leg.game_id in seen_games:
                continue
            if len(selected) >= self.config.ladder_max_rungs:
                break
            cumulative_payout *= c.leg.odds_decimal
            seen_games.add(c.leg.game_id)
            selected.append(c)

        if len(selected) < 2:
            return None

        if cumulative_payout < self.config.ladder_min_payout_multiplier:
            return None

        legs = [c.leg for c in selected]
        combined_conf = math.prod(lg.model_confidence for lg in legs)
        combined_odds = math.prod(lg.odds_decimal for lg in legs)
        combined_edge = combined_conf - decimal_to_implied_prob(combined_odds)

        sports = list({lg.sport for lg in legs})
        sport_label = sports[0] if len(sports) == 1 else "multi"

        rationale = (
            f"Ladder ({len(legs)} rungs): "
            + " → ".join(f"{lg.selection}@{lg.odds_decimal:.2f}" for lg in legs)
            + f" | total payout={combined_odds:.2f}x"
        )

        return PaperBet(
            id=PaperBet.new_id(),
            placed_at=PaperBet.now_iso(),
            sport=sport_label,
            bet_type="parlay",
            legs=legs,
            stake_units=self.config.ladder_base_stake,
            model_confidence=round(combined_conf, 4),
            model_edge=round(combined_edge, 4),
            implied_odds=round(combined_odds, 4),
            rationale=rationale,
            strategy="ladder_daily",
        )

    # ── helpers ─────────────────────────────────────────────────────────

    def _best_odds_for(
        self, game_odds: list[dict], field: str, sport: str
    ) -> dict | None:
        """Find the best decimal odds for a field across bookmakers."""
        best: dict | None = None
        best_dec = 0.0
        for o in game_odds:
            bk = o.get("bookmaker", "")
            if bk.lower() in self.config.excluded_vendors:
                continue
            val = o.get(field)
            if val is None or val <= 1.0:
                continue
            if val > best_dec:
                best_dec = val
                # Try to get corresponding American odds
                am = self._decimal_to_american(val)
                best = {"odds": val, "bookmaker": bk, "american": am}
        return best

    @staticmethod
    def _spread_cover_prob(
        pred_spread: float, line: float, base_confidence: float
    ) -> float:
        """Estimate probability of covering a spread.

        ``pred_spread`` is the model's predicted margin (negative = home favored).
        ``line`` is the bookmaker's spread for the home team.
        """
        # Margin advantage = how much better the model thinks vs. the line
        margin = -(pred_spread) - (-line)  # positive = model favors home covering
        # Logistic transform centred on base_confidence
        raw = 0.5 + margin / 15.0  # rough sigmoid approximation
        return max(0.30, min(0.90, raw * base_confidence + (1 - base_confidence) * 0.5))

    @staticmethod
    def _american_line_to_decimal(american_line: float) -> float:
        """Convert an American odds line (may be float) to decimal."""
        am = int(round(american_line))
        return american_to_decimal(am)

    @staticmethod
    def _decimal_to_american(decimal_odds: float) -> int:
        """Convert decimal odds back to American."""
        if decimal_odds >= 2.0:
            return int(round((decimal_odds - 1) * 100))
        elif decimal_odds > 1.0:
            return int(round(-100 / (decimal_odds - 1)))
        return -110  # fallback
