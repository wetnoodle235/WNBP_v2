#!/usr/bin/env python3
# V5.0 — Live Prediction Model
# Adjusts pre-game win probabilities in real-time based on current
# game state: score, time remaining, momentum, and sport-specific factors.
#
# Usage:
#   python3 scripts/live_model.py --daemon          # poll every 60s
#   python3 scripts/live_model.py --once             # single pass
#   python3 scripts/live_model.py --sport nba --once # one sport, once

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
LIVE_DIR = DATA_DIR / "live_predictions"
PREDICTIONS_DIR = DATA_DIR / "predictions"

sys.path.insert(0, str(BACKEND_DIR))

from config import SPORT_DEFINITIONS, SPORT_SEASON_START, get_current_season

logger = logging.getLogger("live_model")

SUPPORTED_SPORTS = [
    "nba", "nhl", "mlb", "nfl",
    "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl",
    "ncaab", "ncaaf", "wnba",
]

# ── Math helpers ──────────────────────────────────────────


def sigmoid(x: float) -> float:
    """Numerically stable sigmoid."""
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    ex = math.exp(x)
    return ex / (1.0 + ex)


def logit(p: float) -> float:
    """Inverse sigmoid. Clamp to avoid log(0)."""
    p = max(1e-6, min(1.0 - 1e-6, p))
    return math.log(p / (1.0 - p))


# ── Sport category mapping ───────────────────────────────

def _sport_category(sport: str) -> str:
    defn = SPORT_DEFINITIONS.get(sport, {})
    return defn.get("category", "other")


# ── Data loading ─────────────────────────────────────────


def _load_todays_games(sport: str) -> list[dict]:
    """Load games from normalized parquet for today's date."""
    try:
        import pandas as pd

        season = get_current_season(sport)
        parquet = DATA_DIR / "normalized" / sport / f"games_{season}.parquet"
        if not parquet.exists():
            parquet = DATA_DIR / "normalized" / sport / "games.parquet"
        if not parquet.exists():
            return []

        df = pd.read_parquet(parquet)
        today = date.today().isoformat()
        if "date" in df.columns:
            df = df[df["date"].astype(str) == today]
        return df.to_dict(orient="records")
    except Exception:
        logger.debug("Could not load games for %s", sport, exc_info=True)
        return []


def _load_pregame_predictions(sport: str) -> dict[str, dict]:
    """Load pre-game predictions keyed by game_id.

    Checks today's prediction file first, then yesterday's.
    """
    result: dict[str, dict] = {}
    for offset in (0, 1):
        day = date.today() if offset == 0 else date.today() - __import__("datetime").timedelta(days=1)
        path = PREDICTIONS_DIR / f"{day.isoformat()}.json"
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
            for pred in data.get("predictions", []):
                if pred.get("sport") == sport and pred.get("game_id"):
                    result.setdefault(pred["game_id"], pred)
        except Exception:
            logger.debug("Could not load predictions from %s", path, exc_info=True)
    return result


# ── Live game filtering ──────────────────────────────────


def _is_live(game: dict) -> bool:
    """Check if game is currently in progress."""
    status = str(game.get("status", "")).lower()
    return status in ("in_progress", "live", "in progress", "active")


# ── Time remaining parsers (sport-specific) ──────────────


@dataclass
class GameState:
    """Normalized in-progress game state."""
    game_id: str = ""
    sport: str = ""
    home_team: str = ""
    away_team: str = ""
    home_score: int = 0
    away_score: int = 0
    period: str = ""
    time_remaining: str = ""
    total_minutes: float = 48.0
    elapsed_minutes: float = 0.0
    remaining_minutes: float = 48.0
    pre_game_home_wp: float = 0.5
    predicted_home_score: float = 0.0
    predicted_away_score: float = 0.0
    extra: dict = field(default_factory=dict)


def _parse_time_remaining(text: str) -> float:
    """Parse 'MM:SS' or 'M:SS' into minutes (float)."""
    text = str(text).strip()
    if not text or text == "0":
        return 0.0
    parts = text.split(":")
    try:
        if len(parts) == 2:
            return float(parts[0]) + float(parts[1]) / 60.0
        return float(parts[0])
    except (ValueError, IndexError):
        return 0.0


def _nba_game_state(game: dict) -> GameState:
    """Extract NBA game state (4 × 12 min quarters + OT)."""
    gs = GameState(total_minutes=48.0, sport="nba")
    period_str = str(game.get("period", "")).strip()

    # Period number
    period_num = 0
    if period_str:
        for ch in period_str:
            if ch.isdigit():
                period_num = int(ch)
                break
    if "ot" in period_str.lower():
        period_num = max(period_num, 5)

    time_left_in_period = _parse_time_remaining(game.get("time_remaining", "12:00"))
    minutes_per_period = 12.0
    completed = max(0, period_num - 1) * minutes_per_period
    gs.elapsed_minutes = completed + (minutes_per_period - time_left_in_period)
    gs.remaining_minutes = max(0.1, gs.total_minutes - gs.elapsed_minutes)
    gs.period = period_str or f"Q{period_num}"
    gs.time_remaining = game.get("time_remaining", "")
    return gs


def _nfl_game_state(game: dict) -> GameState:
    """Extract NFL game state (4 × 15 min quarters)."""
    gs = GameState(total_minutes=60.0, sport="nfl")
    period_str = str(game.get("period", "")).strip()
    period_num = 0
    for ch in period_str:
        if ch.isdigit():
            period_num = int(ch)
            break

    time_left = _parse_time_remaining(game.get("time_remaining", "15:00"))
    completed = max(0, period_num - 1) * 15.0
    gs.elapsed_minutes = completed + (15.0 - time_left)
    gs.remaining_minutes = max(0.1, gs.total_minutes - gs.elapsed_minutes)
    gs.period = period_str or f"Q{period_num}"
    gs.time_remaining = game.get("time_remaining", "")
    return gs


def _nhl_game_state(game: dict) -> GameState:
    """Extract NHL game state (3 × 20 min periods)."""
    gs = GameState(total_minutes=60.0, sport="nhl")
    period_str = str(game.get("period", "")).strip()
    period_num = 0
    for ch in period_str:
        if ch.isdigit():
            period_num = int(ch)
            break
    if "ot" in period_str.lower():
        period_num = max(period_num, 4)

    time_left = _parse_time_remaining(game.get("time_remaining", "20:00"))
    completed = max(0, period_num - 1) * 20.0
    gs.elapsed_minutes = completed + (20.0 - time_left)
    gs.remaining_minutes = max(0.1, gs.total_minutes - gs.elapsed_minutes)
    gs.period = period_str or f"P{period_num}"
    gs.time_remaining = game.get("time_remaining", "")
    return gs


def _mlb_game_state(game: dict) -> GameState:
    """Extract MLB game state (9 innings)."""
    gs = GameState(total_minutes=9.0, sport="mlb")  # "minutes" = innings
    period_str = str(game.get("period", "")).strip()
    inning = 1
    for ch in period_str:
        if ch.isdigit():
            inning = int(ch)
            break
    is_top = "top" in period_str.lower()
    gs.elapsed_minutes = max(0, inning - 1) + (0.0 if is_top else 0.5)
    gs.remaining_minutes = max(0.5, 9.0 - gs.elapsed_minutes)
    gs.period = period_str or f"{'Top' if is_top else 'Bot'} {inning}"
    gs.time_remaining = ""
    return gs


def _soccer_game_state(game: dict) -> GameState:
    """Extract soccer game state (90 min + stoppage)."""
    gs = GameState(total_minutes=90.0, sport="soccer")
    period_str = str(game.get("period", "")).strip()
    time_rem = _parse_time_remaining(game.get("time_remaining", "45:00"))

    half = 1
    if "2" in period_str or "second" in period_str.lower():
        half = 2
    if "et" in period_str.lower() or "extra" in period_str.lower():
        half = 3

    if half == 1:
        gs.elapsed_minutes = 45.0 - time_rem
    elif half == 2:
        gs.elapsed_minutes = 45.0 + (45.0 - time_rem)
    else:
        gs.elapsed_minutes = 90.0 + (30.0 - time_rem)

    gs.remaining_minutes = max(0.1, gs.total_minutes - min(gs.elapsed_minutes, 90.0))
    gs.period = period_str or f"{'1st' if half == 1 else '2nd'} Half"
    gs.time_remaining = game.get("time_remaining", "")
    return gs


def _generic_game_state(game: dict) -> GameState:
    """Fallback for sports without specific parsing."""
    gs = GameState(total_minutes=60.0, sport="generic")
    gs.period = str(game.get("period", ""))
    gs.time_remaining = str(game.get("time_remaining", ""))
    gs.elapsed_minutes = 30.0
    gs.remaining_minutes = 30.0
    return gs


def _extract_game_state(game: dict, sport: str) -> GameState:
    """Route to sport-specific parser and fill common fields."""
    cat = _sport_category(sport)

    if sport in ("nba", "ncaab", "ncaaw", "wnba"):
        gs = _nba_game_state(game)
        if sport == "ncaab":
            gs.total_minutes = 40.0  # 2 × 20 halves
    elif sport in ("nfl", "ncaaf"):
        gs = _nfl_game_state(game)
    elif sport == "nhl":
        gs = _nhl_game_state(game)
    elif sport == "mlb":
        gs = _mlb_game_state(game)
    elif cat == "soccer":
        gs = _soccer_game_state(game)
    else:
        gs = _generic_game_state(game)

    gs.game_id = str(game.get("id", ""))
    gs.sport = sport
    gs.home_team = str(game.get("home_team", ""))
    gs.away_team = str(game.get("away_team", ""))
    gs.home_score = int(game.get("home_score", 0) or 0)
    gs.away_score = int(game.get("away_score", 0) or 0)
    return gs


# ── Win Probability Models ───────────────────────────────


def _nba_live_wp(gs: GameState) -> float:
    """NBA live win probability for home team.

    Uses log5 with score differential weighted by time remaining.
    """
    pre_logit = logit(gs.pre_game_home_wp)
    score_diff = gs.home_score - gs.away_score
    time_factor = math.sqrt(gs.remaining_minutes + 1.0)
    home_court = 0.03

    score_weight = 0.35
    live_logit = pre_logit + score_weight * (score_diff / time_factor) + home_court
    return sigmoid(live_logit)


def _nfl_live_wp(gs: GameState) -> float:
    """NFL live win probability for home team.

    Score differential weighted heavier in later quarters.
    """
    pre_logit = logit(gs.pre_game_home_wp)
    score_diff = gs.home_score - gs.away_score

    # Quarter weight: scores matter more late
    pct_elapsed = gs.elapsed_minutes / max(gs.total_minutes, 1.0)
    quarter_weight = 0.3 + 0.5 * pct_elapsed  # 0.3 → 0.8 as game progresses

    score_factor = 0.15
    live_logit = pre_logit + quarter_weight * score_factor * score_diff

    # Possession bonus for trailing team late
    if pct_elapsed > 0.75 and score_diff < 0:
        live_logit -= 0.03  # trailing away team might have ball

    return sigmoid(live_logit)


def _nhl_live_wp(gs: GameState) -> float:
    """NHL live win probability for home team.

    Goals are rare — each one shifts probability significantly.
    """
    pre_logit = logit(gs.pre_game_home_wp)
    goal_diff = gs.home_score - gs.away_score
    periods_remaining = gs.remaining_minutes / 20.0

    live_logit = pre_logit + goal_diff * 0.8 / math.sqrt(periods_remaining + 0.5)
    return sigmoid(live_logit)


def _mlb_live_wp(gs: GameState) -> float:
    """MLB live win probability using inning-based expectancy.

    Empirical-style: run differential vs innings remaining with
    leverage index for late-game situations.
    """
    pre_logit = logit(gs.pre_game_home_wp)
    run_diff = gs.home_score - gs.away_score
    innings_remaining = gs.remaining_minutes  # "minutes" = innings

    # Late innings amplify run differential
    leverage = 1.0
    inning_est = 9.0 - innings_remaining
    if inning_est >= 7:
        leverage = 1.5
    if inning_est >= 8:
        leverage = 2.0

    factor = 0.4 * leverage
    denom = math.sqrt(innings_remaining + 0.5)
    live_logit = pre_logit + factor * (run_diff / denom)
    return sigmoid(live_logit)


def _soccer_live_wp(gs: GameState) -> float:
    """Soccer live win probability with time decay.

    Goals are rare — each shifts probability significantly.
    """
    pre_logit = logit(gs.pre_game_home_wp)
    goal_diff = gs.home_score - gs.away_score
    minutes_remaining = gs.remaining_minutes

    live_logit = pre_logit + goal_diff * 1.2 / math.sqrt(minutes_remaining / 15.0 + 0.5)
    return sigmoid(live_logit)


def _generic_live_wp(gs: GameState) -> float:
    """Fallback live win probability."""
    pre_logit = logit(gs.pre_game_home_wp)
    score_diff = gs.home_score - gs.away_score
    time_factor = math.sqrt(gs.remaining_minutes + 1.0)
    live_logit = pre_logit + 0.2 * (score_diff / time_factor)
    return sigmoid(live_logit)


def compute_live_wp(gs: GameState) -> float:
    """Dispatch to the correct sport-specific model."""
    cat = _sport_category(gs.sport)
    if gs.sport in ("nba", "ncaab", "ncaaw", "wnba"):
        return _nba_live_wp(gs)
    if gs.sport in ("nfl", "ncaaf"):
        return _nfl_live_wp(gs)
    if gs.sport == "nhl":
        return _nhl_live_wp(gs)
    if gs.sport == "mlb":
        return _mlb_live_wp(gs)
    if cat == "soccer":
        return _soccer_live_wp(gs)
    return _generic_live_wp(gs)


# ── Predicted final score ────────────────────────────────


def _predicted_final(gs: GameState, live_home_wp: float) -> tuple[float, float]:
    """Project final scores from current pace and pre-game prediction."""
    if gs.elapsed_minutes <= 0.1:
        return gs.predicted_home_score, gs.predicted_away_score

    pct_elapsed = min(gs.elapsed_minutes / max(gs.total_minutes, 1.0), 0.99)
    pct_remaining = 1.0 - pct_elapsed

    # Blend current pace with pre-game prediction (pace dominates late)
    pace_weight = pct_elapsed
    home_pace = gs.home_score / pct_elapsed if pct_elapsed > 0.05 else gs.predicted_home_score
    away_pace = gs.away_score / pct_elapsed if pct_elapsed > 0.05 else gs.predicted_away_score

    pred_home = pace_weight * home_pace + (1.0 - pace_weight) * gs.predicted_home_score
    pred_away = pace_weight * away_pace + (1.0 - pace_weight) * gs.predicted_away_score
    return round(pred_home, 1), round(pred_away, 1)


# ── Momentum ─────────────────────────────────────────────


def _compute_momentum(gs: GameState) -> tuple[str, float, list[str]]:
    """Determine momentum direction and key factors.

    Without play-by-play data we derive momentum from score
    differentials relative to the pre-game expectation.
    """
    factors: list[str] = []
    score_diff = gs.home_score - gs.away_score
    pre_spread = gs.predicted_home_score - gs.predicted_away_score

    # How much the live score deviates from expected
    deviation = score_diff - pre_spread
    pct = gs.elapsed_minutes / max(gs.total_minutes, 1.0)

    # Momentum score: deviation normalized by game progress
    momentum_raw = deviation / max(math.sqrt(gs.total_minutes), 1.0)
    momentum_score = max(-1.0, min(1.0, momentum_raw))

    if abs(score_diff) > 0:
        leader = "Home" if score_diff > 0 else "Away"
        factors.append(f"{leader} team leads by {abs(score_diff)}")

    if deviation > 3:
        factors.append("Home outperforming pre-game expectation")
    elif deviation < -3:
        factors.append("Away outperforming pre-game expectation")

    if pct > 0.6 and abs(score_diff) > 10:
        factors.append("Comfortable lead in second half")

    if pct > 0.75:
        factors.append("Late-game situation")

    direction = "home" if momentum_score > 0.02 else ("away" if momentum_score < -0.02 else "neutral")
    return direction, round(abs(momentum_score), 4), factors


# ── Main processing ──────────────────────────────────────


def _process_sport(sport: str) -> dict[str, Any]:
    """Process a single sport: find live games, compute predictions."""
    games = _load_todays_games(sport)
    live_games = [g for g in games if _is_live(g)]
    predictions = _load_pregame_predictions(sport)

    result_games: list[dict[str, Any]] = []
    for game in live_games:
        gs = _extract_game_state(game, sport)

        # Attach pre-game prediction
        pred = predictions.get(gs.game_id, {})
        gs.pre_game_home_wp = float(pred.get("home_win_prob", 0.5))
        gs.predicted_home_score = float(pred.get("predicted_home_score", 0.0))
        gs.predicted_away_score = float(pred.get("predicted_away_score", 0.0))

        # Compute live adjustments
        live_home_wp = compute_live_wp(gs)
        live_away_wp = round(1.0 - live_home_wp, 4)
        live_home_wp = round(live_home_wp, 4)

        pred_home_final, pred_away_final = _predicted_final(gs, live_home_wp)
        momentum_dir, momentum_score, key_factors = _compute_momentum(gs)

        result_games.append({
            "game_id": gs.game_id,
            "home_team": gs.home_team,
            "away_team": gs.away_team,
            "home_score": gs.home_score,
            "away_score": gs.away_score,
            "period": gs.period,
            "time_remaining": gs.time_remaining,
            "pre_game_home_wp": round(gs.pre_game_home_wp, 4),
            "live_home_wp": live_home_wp,
            "live_away_wp": live_away_wp,
            "predicted_final_home": pred_home_final,
            "predicted_final_away": pred_away_final,
            "momentum": momentum_dir,
            "momentum_score": momentum_score,
            "key_factors": key_factors,
        })

    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "games": result_games,
    }


def _write_output(sport: str, data: dict) -> Path:
    """Write live prediction JSON for a sport."""
    LIVE_DIR.mkdir(parents=True, exist_ok=True)
    out = LIVE_DIR / f"{sport}_live.json"
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    tmp.rename(out)
    return out


# ── Run loop ─────────────────────────────────────────────


def run_once(sports: list[str] | None = None) -> dict[str, int]:
    """Single pass over all (or specified) sports. Returns game counts."""
    if sports is None:
        sports = SUPPORTED_SPORTS

    counts: dict[str, int] = {}
    for sport in sports:
        try:
            data = _process_sport(sport)
            n = len(data["games"])
            counts[sport] = n
            if n > 0:
                path = _write_output(sport, data)
                logger.info("%s: %d live game(s) → %s", sport, n, path)
            else:
                # Write empty file to signal "checked, nothing live"
                _write_output(sport, data)
                logger.debug("%s: no live games", sport)
        except Exception:
            logger.exception("Error processing %s", sport)
            counts[sport] = -1
    return counts


def run_daemon(sports: list[str] | None = None, interval: int = 60) -> None:
    """Poll loop — runs until interrupted."""
    shutdown = False

    def _handle_signal(signum: int, frame: Any) -> None:
        nonlocal shutdown
        logger.info("Received signal %d — shutting down", signum)
        shutdown = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("Live model daemon started (interval=%ds, sports=%s)",
                interval, sports or "all")
    while not shutdown:
        t0 = time.monotonic()
        counts = run_once(sports)
        total_live = sum(v for v in counts.values() if v > 0)
        elapsed = time.monotonic() - t0
        logger.info("Pass complete: %d live game(s) across %d sport(s) in %.1fs",
                     total_live, len(counts), elapsed)

        # Sleep in small increments so we can respond to signals
        deadline = time.monotonic() + interval
        while not shutdown and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))

    logger.info("Daemon stopped.")


# ── CLI ──────────────────────────────────────────────────


def _setup_logging(verbose: bool = False) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_file = LOG_DIR / f"live_model_{date.today().isoformat()}.log"
    handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt,
                        handlers=handlers, force=True)
    logger.setLevel(level)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Live prediction model — adjusts win probabilities during games",
    )
    parser.add_argument("--daemon", action="store_true",
                        help="Run as daemon, polling every --interval seconds")
    parser.add_argument("--once", action="store_true",
                        help="Single pass: check all sports once and exit")
    parser.add_argument("--sport", type=str, default=None,
                        help="Limit to a single sport (e.g. nba)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Poll interval in seconds (default: 60)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Debug-level logging")
    args = parser.parse_args()

    _setup_logging(verbose=args.verbose)

    if not args.daemon and not args.once:
        args.once = True
        logger.info("No mode specified — defaulting to --once")

    sports = [args.sport] if args.sport else None
    if args.sport and args.sport not in SUPPORTED_SPORTS:
        logger.warning("Sport '%s' not in supported list; will attempt anyway", args.sport)
        sports = [args.sport]

    if args.daemon:
        run_daemon(sports=sports, interval=args.interval)
    else:
        counts = run_once(sports=sports)
        total = sum(v for v in counts.values() if v > 0)
        logger.info("Done. %d live game(s) found across %d sport(s).",
                     total, sum(1 for v in counts.values() if v >= 0))


if __name__ == "__main__":
    main()
