#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# V5.0 — Odds Collection Scheduler
# ──────────────────────────────────────────────────────────
#
# Three collection modes:
#   opening  — 12:05 AM EST daily, captures day's opening lines
#   closing  — 5 minutes before each game, captures final lines
#   current  — every N minutes (default 60), continuous snapshots
#
# Providers:
#   odds     — unified ESPN collector (always available; DK only per game)
#   oddsapi  — The Odds API (requires ODDSAPI_KEY; opening/closing only)
#   sgo      — SportsGameOdds (requires SGO_API_KEY; opening/closing only)
#
# IMPORTANT: Only ESPN (odds) is used for current/hourly snapshots.
# oddsapi and sgo are used for opening + closing snapshots only to
# avoid exhausting daily/monthly API quotas.
#
# Usage:
#   python3 scripts/odds_scheduler.py --mode opening --once
#   python3 scripts/odds_scheduler.py --mode closing --sport nba
#   python3 scripts/odds_scheduler.py --mode current --interval 60
#   python3 scripts/odds_scheduler.py --mode all
#
# Environment:
#   ODDSAPI_KEY  — The Odds API key (optional)
#   SGO_API_KEY  — SportsGameOdds API key (optional)
#
# Cron examples in scripts/crontab.example
# ──────────────────────────────────────────────────────────
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

# ── Path setup ───────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
IMPORTERS_DIR = PROJECT_ROOT / "importers"
DATA_DIR = PROJECT_ROOT / "data"

sys.path.insert(0, str(BACKEND_DIR))

# ── Constants ────────────────────────────────────────────

EST = timezone(timedelta(hours=-5))

# Providers used for opening + closing snapshots (multi-bookmaker; quota-limited).
# Built dynamically in main() based on available API keys.
ODDS_PROVIDERS: list[str] = ["odds", "oddsapi"]

# SGO supports a subset of sports. Others fall back to ESPN-only.
SGO_SPORTS = {"nba", "mlb", "nfl", "nhl", "ncaab", "wnba", "ncaaf"}

# Sports supported by odds providers (matches importers/src/providers/odds/config.ts)
ODDS_SPORTS = [
    "nba", "wnba", "ncaab", "ncaaf", "nfl", "mlb", "nhl",
    "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls",
    "ufc", "atp",
]

DEFAULT_INTERVAL_MINUTES = 60
CLOSING_MINUTES_BEFORE = 5
SUBPROCESS_TIMEOUT = 300

# ── Logging ──────────────────────────────────────────────

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("odds_scheduler")


def _setup_logging(level: str = "info") -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger.setLevel(log_level)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    log_file = LOG_DIR / f"odds_scheduler_{date.today().isoformat()}.log"
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)


# ── TypeScript CLI runner ────────────────────────────────


def _run_importer(
    provider: str,
    snapshot_type: str,
    sports: list[str] | None = None,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    """Invoke the TypeScript importer CLI via subprocess.

    Returns dict with keys: success, stdout, stderr, duration_s.
    """
    season = str(datetime.now(EST).year)
    cmd = [
        "npx", "tsx", "src/cli.ts",
        f"--provider={provider}",
        f"--snapshot={snapshot_type}",
        f"--seasons={season}",
        "--endpoints=odds",
    ]
    if sports:
        cmd.append(f"--sports={','.join(sports)}")
    if extra_args:
        cmd.extend(extra_args)

    logger.debug("Running: %s", " ".join(cmd))
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(IMPORTERS_DIR),
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT,
        )
        elapsed = time.monotonic() - t0
        return {
            "success": proc.returncode == 0,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip()[-500:] if proc.stderr else "",
            "duration_s": round(elapsed, 1),
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "stdout": "",
            "stderr": f"timeout after {SUBPROCESS_TIMEOUT}s",
            "duration_s": SUBPROCESS_TIMEOUT,
        }
    except FileNotFoundError:
        return {
            "success": False,
            "stdout": "",
            "stderr": "npx/tsx not found — is Node.js installed?",
            "duration_s": 0,
        }


# ── Game schedule helpers ────────────────────────────────


def _load_todays_games(sport: str) -> list[dict[str, Any]]:
    """Load today's game schedule from raw ESPN data.

    Returns list of dicts with at minimum: game_id, start_time (ISO string).
    """
    today = date.today().isoformat()
    season = str(datetime.now(EST).year)

    # Try normalized games parquet first (most reliable)
    games_parquet = DATA_DIR / "normalized" / sport / f"games_{season}.parquet"
    if games_parquet.exists():
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(games_parquet)
            df_dicts = table.to_pydict()
            games = []
            n = len(df_dicts.get("game_id", []))
            for i in range(n):
                row = {k: v[i] for k, v in df_dicts.items()}
                game_date = str(row.get("date", row.get("game_date", "")))[:10]
                if game_date == today:
                    games.append({
                        "game_id": str(row.get("game_id", "")),
                        "start_time": str(row.get("start_time", row.get("datetime", ""))),
                        "home_team": str(row.get("home_team", "")),
                        "away_team": str(row.get("away_team", "")),
                    })
            return games
        except Exception as e:
            logger.debug("Could not read %s: %s", games_parquet, e)

    # Fallback: raw ESPN JSON
    espn_dir = DATA_DIR / "raw" / "espn" / sport / season / "games"
    if espn_dir.is_dir():
        games = []
        for f in sorted(espn_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text())
                events = data if isinstance(data, list) else data.get("events", [data])
                for ev in events:
                    game_date = str(ev.get("date", ""))[:10]
                    if game_date == today:
                        games.append({
                            "game_id": str(ev.get("id", ev.get("game_id", f.stem))),
                            "start_time": ev.get("date", ev.get("start_time", "")),
                            "home_team": ev.get("home_team", ""),
                            "away_team": ev.get("away_team", ""),
                        })
            except (json.JSONDecodeError, KeyError):
                continue
        return games

    return []


def _parse_game_time(time_str: str) -> datetime | None:
    """Parse an ISO datetime string into a timezone-aware datetime."""
    if not time_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(time_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


# ── Odds history tracking ────────────────────────────────


def _record_collection(
    sport: str,
    snapshot_type: str,
    source: str,
    result: dict[str, Any],
    game_id: str | None = None,
) -> None:
    """Append a collection record to the odds history parquet.

    Schema: timestamp, source, type, sport, game_id, success, duration_s, error
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.debug("pyarrow not installed — skipping history tracking")
        return

    out_dir = DATA_DIR / "normalized" / sport
    out_dir.mkdir(parents=True, exist_ok=True)
    history_path = out_dir / "odds_history.parquet"

    record = {
        "timestamp": [datetime.now(timezone.utc).isoformat()],
        "source": [source],
        "type": [snapshot_type],
        "sport": [sport],
        "game_id": [game_id or ""],
        "success": [result["success"]],
        "duration_s": [result["duration_s"]],
        "error": [result.get("stderr", "")[:200] if not result["success"] else ""],
    }

    schema = pa.schema([
        ("timestamp", pa.string()),
        ("source", pa.string()),
        ("type", pa.string()),
        ("sport", pa.string()),
        ("game_id", pa.string()),
        ("success", pa.bool_()),
        ("duration_s", pa.float64()),
        ("error", pa.string()),
    ])

    new_table = pa.table(record, schema=schema)

    if history_path.exists():
        try:
            existing = pq.read_table(history_path, schema=schema)
            combined = pa.concat_tables([existing, new_table])
        except Exception:
            combined = new_table
    else:
        combined = new_table

    pq.write_table(combined, history_path)


# ── Collection modes ─────────────────────────────────────


def collect_opening(sports: list[str]) -> dict[str, Any]:
    """Collect opening odds for all games — run at 12:05 AM EST daily."""
    logger.info("═══ Opening odds collection ═══")
    results: dict[str, Any] = {"mode": "opening", "sports": {}, "errors": []}

    for provider in ODDS_PROVIDERS:
        for sport in sports:
            # Skip sports unsupported by this provider
            if provider == "sgo" and sport not in SGO_SPORTS:
                continue
            logger.info("  %s/%s — fetching opening lines …", provider, sport)
            result = _run_importer(provider, "opening", sports=[sport])

            if result["success"]:
                logger.info("  %s/%s ✓ (%.1fs)", provider, sport, result["duration_s"])
            else:
                logger.warning(
                    "  %s/%s ✗ — %s", provider, sport, result["stderr"][:120]
                )
                results["errors"].append(
                    {"provider": provider, "sport": sport, "error": result["stderr"]}
                )

            _record_collection(sport, "opening", provider, result)
            results["sports"][f"{provider}/{sport}"] = result["success"]

    ok = sum(1 for v in results["sports"].values() if v)
    total = len(results["sports"])
    logger.info("Opening collection done: %d/%d succeeded", ok, total)
    return results


def collect_closing(sports: list[str]) -> dict[str, Any]:
    """Collect closing odds 5 minutes before each game starts."""
    logger.info("═══ Closing odds collection ═══")
    now = datetime.now(timezone.utc)
    results: dict[str, Any] = {"mode": "closing", "games_checked": 0, "collected": 0, "errors": []}

    for sport in sports:
        games = _load_todays_games(sport)
        if not games:
            logger.debug("  %s — no games found for today", sport)
            continue

        for game in games:
            results["games_checked"] += 1
            start = _parse_game_time(game.get("start_time", ""))
            if start is None:
                continue

            minutes_until = (start - now).total_seconds() / 60
            window_open = -2 <= minutes_until <= CLOSING_MINUTES_BEFORE + 2

            if not window_open:
                # Not in the closing window — log only at debug level
                if minutes_until > 0:
                    logger.debug(
                        "  %s %s — %.0f min until start, skipping",
                        sport, game["game_id"], minutes_until,
                    )
                continue

            gid = game["game_id"]
            logger.info(
                "  %s %s — %.1f min to start, collecting closing odds",
                sport, gid, minutes_until,
            )

            for provider in ODDS_PROVIDERS:
                # Skip sports unsupported by this provider
                if provider == "sgo" and sport not in SGO_SPORTS:
                    continue
                result = _run_importer(provider, "closing", sports=[sport])
                if result["success"]:
                    logger.info("    %s ✓ (%.1fs)", provider, result["duration_s"])
                    results["collected"] += 1
                else:
                    logger.warning("    %s ✗ — %s", provider, result["stderr"][:120])
                    results["errors"].append({
                        "provider": provider, "sport": sport,
                        "game_id": gid, "error": result["stderr"],
                    })
                _record_collection(sport, "closing", provider, result, game_id=gid)

    logger.info(
        "Closing collection done: checked %d games, collected %d",
        results["games_checked"], results["collected"],
    )
    return results


def collect_current(sports: list[str]) -> dict[str, Any]:
    """Collect current odds snapshot — runs on configurable interval.

    Only uses the ESPN (``odds``) provider to avoid burning quota on
    oddsapi or SGO with frequent snapshots.
    """
    logger.info("═══ Current odds snapshot ═══")
    results: dict[str, Any] = {"mode": "current", "sports": {}, "errors": []}

    # Current/hourly snapshots: ESPN only (no quota cost)
    current_providers = ["odds"]

    for provider in current_providers:
        for sport in sports:
            logger.info("  %s/%s — snapshot …", provider, sport)
            result = _run_importer(provider, "current", sports=[sport])

            if result["success"]:
                logger.info("  %s/%s ✓ (%.1fs)", provider, sport, result["duration_s"])
            else:
                logger.warning(
                    "  %s/%s ✗ — %s", provider, sport, result["stderr"][:120]
                )
                results["errors"].append(
                    {"provider": provider, "sport": sport, "error": result["stderr"]}
                )

            _record_collection(sport, "current", provider, result)
            results["sports"][f"{provider}/{sport}"] = result["success"]

    ok = sum(1 for v in results["sports"].values() if v)
    total = len(results["sports"])
    logger.info("Current snapshot done: %d/%d succeeded", ok, total)
    return results


# ── Loop runner ──────────────────────────────────────────


def _closing_loop(sports: list[str], interval_minutes: int) -> None:
    """Continuously check for games approaching start and collect closing odds.

    Polls every `interval_minutes` (default: 1 minute for closing to catch
    the 5-minute pre-game window).
    """
    # For closing mode, poll frequently so we don't miss the window
    poll_interval = min(interval_minutes, 1) * 60
    logger.info(
        "Closing-odds loop started — polling every %d seconds", poll_interval
    )
    while True:
        try:
            collect_closing(sports)
        except Exception as e:
            logger.error("Closing loop error: %s", e)
        time.sleep(poll_interval)


def _current_loop(sports: list[str], interval_minutes: int) -> None:
    """Collect current odds on a repeating interval."""
    logger.info(
        "Current-odds loop started — interval %d minutes", interval_minutes
    )
    while True:
        try:
            collect_current(sports)
        except Exception as e:
            logger.error("Current loop error: %s", e)
        logger.info("Sleeping %d minutes until next snapshot …", interval_minutes)
        time.sleep(interval_minutes * 60)


# ── Normalization trigger ────────────────────────────────


def _run_normalization(sports: list[str]) -> None:
    """Run odds normalization after collection to update parquet files."""
    try:
        from normalization.normalizer import Normalizer

        normalizer = Normalizer()
        season = str(datetime.now(EST).year)
        for sport in sports:
            try:
                count = normalizer.normalize_odds(sport, season)
                logger.info("  Normalized %s odds: %d records", sport, count)
            except Exception as e:
                logger.warning("  Normalization failed for %s: %s", sport, e)
    except ImportError:
        logger.debug("Normalizer not available — skipping normalization step")


# ── CLI ──────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Odds collection scheduler for V5.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  %(prog)s --mode opening --once           Collect opening odds once
  %(prog)s --mode closing --sport nba      Monitor NBA closing odds
  %(prog)s --mode current --interval 60    Snapshot every 60 min (loop)
  %(prog)s --mode current --interval 30    Snapshot every 30 min (loop)
  %(prog)s --mode all --once               Run all three modes once
  %(prog)s --mode opening --sport nba,nfl  Opening odds for NBA+NFL only
""",
    )
    p.add_argument(
        "--mode",
        choices=["opening", "closing", "current", "all"],
        default="all",
        help="Collection mode (default: all)",
    )
    p.add_argument(
        "--sport", "--sports",
        type=str,
        default=None,
        help="Comma-separated sports to collect (default: all supported)",
    )
    p.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_MINUTES,
        help=f"Minutes between current-odds polls (default: {DEFAULT_INTERVAL_MINUTES})",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (no looping)",
    )
    p.add_argument(
        "--normalize",
        action="store_true",
        help="Run normalization after collection",
    )
    p.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default="info",
        help="Logging verbosity (default: info)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    _setup_logging(args.log_level)

    sports = args.sport.split(",") if args.sport else ODDS_SPORTS
    # Validate sports
    invalid = [s for s in sports if s not in ODDS_SPORTS]
    if invalid:
        logger.error("Unknown sports: %s (supported: %s)", invalid, ODDS_SPORTS)
        sys.exit(1)

    # Check for API keys
    oddsapi_active = False
    if os.environ.get("ODDSAPI_KEY"):
        # Validate the key with a quick API call
        import urllib.request, urllib.error
        test_url = f"https://api.the-odds-api.com/v4/sports/?apiKey={os.environ['ODDSAPI_KEY']}"
        try:
            with urllib.request.urlopen(test_url, timeout=10) as resp:
                if resp.status == 200:
                    oddsapi_active = True
                    logger.info("ODDSAPI_KEY validated — OddsAPI provider enabled")
        except urllib.error.HTTPError as e:
            logger.warning("ODDSAPI_KEY validation failed (HTTP %s) — disabling oddsapi provider", e.code)
        except Exception as e:
            logger.warning("ODDSAPI_KEY validation failed (%s) — disabling oddsapi provider", e)
    else:
        logger.info("ODDSAPI_KEY not set — ESPN-only mode")

    if not oddsapi_active and "oddsapi" in ODDS_PROVIDERS:
        ODDS_PROVIDERS.remove("oddsapi")
        logger.info("OddsAPI provider removed from active providers; using ESPN odds only")

    sgo_active = False
    if os.environ.get("SGO_API_KEY"):
        # SportsGameOdds — always trust the key (no pre-validation endpoint)
        sgo_active = True
        if "sgo" not in ODDS_PROVIDERS:
            ODDS_PROVIDERS.append("sgo")
        logger.info("SGO_API_KEY detected — SportsGameOdds provider enabled for opening/closing")

    logger.info(
        "Odds scheduler: mode=%s, sports=%d, interval=%dm, once=%s",
        args.mode, len(sports), args.interval, args.once,
    )

    mode = args.mode

    if mode == "all":
        collect_opening(sports)
        collect_closing(sports)
        collect_current(sports)
        if args.normalize:
            _run_normalization(sports)
        if not args.once:
            # After initial run, loop on current + closing
            logger.info("Entering continuous loop (current + closing) …")
            while True:
                try:
                    collect_current(sports)
                    collect_closing(sports)
                except Exception as e:
                    logger.error("Loop error: %s", e)
                time.sleep(args.interval * 60)
    elif mode == "opening":
        collect_opening(sports)
        if args.normalize:
            _run_normalization(sports)
    elif mode == "closing":
        collect_closing(sports)
        if args.normalize:
            _run_normalization(sports)
        if not args.once:
            _closing_loop(sports, args.interval)
    elif mode == "current":
        collect_current(sports)
        if args.normalize:
            _run_normalization(sports)
        if not args.once:
            _current_loop(sports, args.interval)

    logger.info("Done.")


if __name__ == "__main__":
    main()
