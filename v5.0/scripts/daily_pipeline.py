#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# V5.0 — Daily Pipeline (Parallel-Optimized)
# ──────────────────────────────────────────────────────────
#
# Comprehensive daily orchestration with parallel processing:
#   1. Import data (TypeScript importers — parallel by provider)
#   2. Normalize into unified parquets (parallel by sport)
#   3. Accuracy analysis on previous day's predictions
#   4. Feature extraction (parallel by sport)
#   5. Model training (conditional)
#   6. Generate predictions (parallel by sport)
#   7. Generate daily report with timing summary
#
# Cron entry (6 AM daily):
#   0 6 * * * cd /home/derek/Documents/stock/v5.0 && python3 scripts/daily_pipeline.py
# ──────────────────────────────────────────────────────────
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _run_subprocess(
    cmd: list[str],
    cwd: str | Path,
    timeout: int,
) -> subprocess.CompletedProcess:
    """Run a subprocess with proper timeout handling.

    Unlike subprocess.run(timeout=...) which leaves zombie children,
    this uses Popen + process groups so we can kill the entire tree on timeout.
    """
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,  # new process group for clean kill
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
    except subprocess.TimeoutExpired:
        # Kill entire process group (the training process + any children)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, OSError):
            proc.kill()
        proc.wait(timeout=5)
        raise

# ── Path setup ───────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
IMPORTERS_DIR = PROJECT_ROOT / "importers"
DATA_DIR = PROJECT_ROOT / "data"

sys.path.insert(0, str(BACKEND_DIR))

from config import get_current_season, SPORT_SEASON_START

# ── Logging ──────────────────────────────────────────────

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("daily_pipeline")


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_file = LOG_DIR / f"daily_pipeline_{date.today().isoformat()}.log"
    handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)
    logger.setLevel(level)


# ── Season detection (delegates to backend/config.py) ────


def _season_for_sport(sport: str, target: date) -> str:
    """Determine the season year for *sport* on *target* date.

    ESPN and our internal system share the same season labelling:
    - End-year sports (NBA, NHL, NCAAB, NCAAW): "2026" = 2025-26 season
    - Start-year sports (NFL, NCAAF, soccer): "2025" = the 2025 season
    - Calendar-year sports (MLB, etc.): "2026" = 2026 season
    """
    sport_lower = sport.lower()
    start_month = SPORT_SEASON_START.get(sport_lower, 1)

    # End-year sports: NBA start=10, NHL start=10, NCAAB/NCAAW start=11
    # Oct 2025 starts → season "2026"; in March 2026 → still "2026"
    _END_YEAR_SPORTS = {"nba", "nhl", "ncaab", "ncaaw"}
    if sport_lower in _END_YEAR_SPORTS:
        if target.month >= start_month:
            return str(target.year + 1)  # new season just started
        else:
            return str(target.year)  # mid-season, end year is this year

    # Start-year sports: NFL start=9, NCAAF start=8, soccer start=8-9
    # Sep 2025 starts → season "2025"; in March 2026 → still "2025"
    _START_YEAR_SPORTS = {
        "nfl", "ncaaf",
        "epl", "laliga", "bundesliga", "seriea", "ligue1", "ucl",
        "ligamx", "europa",
        "wnba",  # WNBA starts May, labeled by start year
    }
    if sport_lower in _START_YEAR_SPORTS:
        if target.month >= start_month:
            return str(target.year)  # new season started this year
        else:
            return str(target.year - 1)  # still in season from last year

    # Calendar-year sports (MLB, MLS, NWSL, F1, UFC, tennis, esports, golf)
    return str(target.year)


def is_sport_in_season(sport: str, target_date: date) -> bool:
    """Check if a sport is likely active on *target_date*.

    Uses ``SPORT_SEASON_START`` from the backend config as the single
    source of truth.  Estimates a ~2-month off-season window before each
    sport's start month.  Year-round sports (start=1) and unknown sports
    are always considered active.
    """
    start_month = SPORT_SEASON_START.get(sport.lower())
    if start_month is None or start_month == 1:
        return True  # year-round or unknown → always active
    # Off-season approximation: the 2 months immediately before start
    off_month_1 = ((start_month - 3) % 12) + 1
    off_month_2 = ((start_month - 2) % 12) + 1
    return target_date.month not in (off_month_1, off_month_2)


def get_active_sports(all_sports: list[str] | set[str], target_date: date) -> list[str]:
    """Return only the sports whose season covers *target_date*."""
    return sorted(s for s in all_sports if is_sport_in_season(s, target_date))


# ── Parallel worker functions (module-level for pickling) ─


def _worker_import_provider(
    provider: str,
    sport_filter: str | None,
    season: str,
    importers_dir: str,
    timeout: int = 180,
    max_retries: int = 1,
    recent_days: int = 3,
) -> dict[str, Any]:
    """Import data from a single provider (runs in a worker thread).

    Retries up to *max_retries* times on failure with exponential backoff.
    Timeouts are not retried (they'd just timeout again).
    """
    # Daily pipeline: games + standings + odds change daily. Injuries/news
    # are lightweight and useful.  Scoreboard feeds into games merge.
    # Skip teams/rosters/player_stats (slow, rarely change).
    endpoints = "games,standings,odds,injuries,news,scoreboard"
    cmd = [
        "npx", "tsx", "src/cli.ts",
        f"--provider={provider}",
        f"--seasons={season}",
        f"--endpoints={endpoints}",
        f"--recent-days={recent_days}",  # yesterday/today/tomorrow window
    ]
    if sport_filter:
        cmd.append(f"--sports={sport_filter}")

    label = f"{provider}" + (f"({sport_filter})" if sport_filter else "")

    for attempt in range(max_retries + 1):
        t0 = time.monotonic()
        if attempt > 0:
            backoff = 5 * (2 ** (attempt - 1))
            logger.info("  [import] %s retry %d/%d (backoff %ds)", label, attempt, max_retries, backoff)
            time.sleep(backoff)

        logger.info("  [import] Starting %s …", label)
        try:
            proc = _run_subprocess(cmd, cwd=importers_dir, timeout=timeout)
            elapsed = round(time.monotonic() - t0, 2)
            if proc.returncode == 0:
                logger.info("  [import] %s ✓ (%.1fs)", label, elapsed)
                return {"provider": label, "status": "ok", "duration_s": elapsed}
            err = proc.stderr.strip()[-500:] if proc.stderr else "unknown error"
            logger.warning(
                "  [import] %s failed (exit %d, %.1fs): %s",
                label, proc.returncode, elapsed, err,
            )
            if attempt == max_retries:
                return {"provider": label, "status": "error", "error": err, "duration_s": elapsed}
        except subprocess.TimeoutExpired:
            elapsed = round(time.monotonic() - t0, 2)
            logger.warning("  [import] %s timed out (%ds) — not retrying", label, timeout)
            # Don't retry timeouts — they'd just timeout again
            return {"provider": label, "status": "error", "error": f"timeout ({timeout}s)", "duration_s": elapsed}
        except FileNotFoundError:
            return {"provider": label, "status": "error", "error": "npx/tsx not found", "duration_s": 0.0}

    return {"provider": label, "status": "error", "error": "max retries exceeded", "duration_s": 0.0}

# ESPN sports — one per import process for proper per-sport season detection
# and to avoid one slow sport blocking others in the same group.
_ESPN_SPORTS = [
    "nba", "nhl", "mlb", "nfl",
    "ncaab", "ncaaf", "ncaaw",
    "epl", "laliga", "bundesliga", "seriea", "ligue1", "ucl",
    "mls", "nwsl", "ligamx", "europa",
    "eredivisie", "primeiraliga", "championship", "bundesliga2", "serieb", "ligue2",
    "worldcup", "euros",
    "wnba", "golf", "lpga", "f1", "nascar", "atp", "wta",
]

# Providers known to be dead/unreachable — skip automatically
_DISABLED_PROVIDERS = {
    "oddsapi",          # API key deactivated
}


def _worker_normalize_sport(
    sport: str,
    season: str,
    backend_dir: str,
    daily_only: bool = False,
) -> dict[str, Any]:
    """Normalize a single sport in a subprocess for process-level parallelism.

    When *daily_only* is True, only data types that change day-to-day are
    normalized (games, player_stats, standings, odds, etc.) — skipping slow
    static types like teams, players, ratings, weather.
    """
    t0 = time.monotonic()
    logger.info("  [normalize] Starting %s …", sport)
    try:
        dt_filter = ""
        if daily_only:
            dt_filter = "data_types=n.DAILY_DATA_TYPES, "
        script = (
            "import json; "
            "from normalization.normalizer import Normalizer; "
            f"n = Normalizer(); "
            f"result = n.run_sport('{sport}', seasons=['{season}'], {dt_filter}); "
            "print(json.dumps(result, default=str))"
        )
        proc = _run_subprocess(
            [sys.executable, "-c", script],
            cwd=backend_dir, timeout=300,
        )
        elapsed = round(time.monotonic() - t0, 2)
        if proc.returncode != 0:
            err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
            logger.warning("  [normalize] %s failed (%.1fs): %s", sport, elapsed, err)
            return {"sport": sport, "status": "error", "error": err, "duration_s": elapsed}
        logger.info("  [normalize] %s ✓ (%.1fs)", sport, elapsed)
        return {"sport": sport, "status": "ok", "duration_s": elapsed, "output": proc.stdout.strip()}
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - t0, 2)
        return {"sport": sport, "status": "error", "error": "timeout (300s)", "duration_s": elapsed}


_FEATURE_EXTRACT_TIMEOUT = 900  # 15 min — NCAAF has ~3800 games and takes ~560s


def _worker_extract_features(
    sport: str,
    season: str,
    backend_dir: str,
    data_dir: str,
    fallback_season: str | None = None,
) -> dict[str, Any]:
    """Extract features for a single sport — in-process only when cached.

    Uses a quick pre-check to determine if incremental extraction will be
    near-instant (all games already extracted).  If so, runs in-process to
    avoid ~4s subprocess overhead.  Otherwise uses a subprocess for true
    parallelism (avoids GIL serialization of CPU-heavy work).
    """
    t0 = time.monotonic()
    logger.info("  [features] Starting %s …", sport)

    features_dir = Path(data_dir) / "features"
    features_dir.mkdir(parents=True, exist_ok=True)
    output_path = features_dir / f"{sport}_{season}.parquet"

    # --- decide: in-process (cached) vs subprocess (real work) -------------
    use_inprocess = False
    if output_path.exists():
        try:
            import pandas as pd
            existing_count = len(pd.read_parquet(output_path, columns=["game_id"]))
            # Check how many completed games exist in normalized data
            norm_path = Path(data_dir) / "normalized" / sport / f"games_{season}.parquet"
            if norm_path.exists():
                games = pd.read_parquet(norm_path, columns=["home_score", "away_score"])
                has_scores = (
                    games["home_score"].notna() & games["away_score"].notna()
                    & ((games["home_score"] > 0) | (games["away_score"] > 0))
                )
                completed_count = int(has_scores.sum())
                # If we already have features for all (or nearly all) completed games,
                # incremental extraction will be near-instant → use in-process
                if existing_count >= completed_count - 2:
                    use_inprocess = True
        except Exception:
            pass  # fall through to subprocess

    if use_inprocess:
        try:
            if backend_dir not in sys.path:
                sys.path.insert(0, backend_dir)
            from ml.feature_extraction import extract as _fe_extract
            df = _fe_extract(sport, Path(data_dir), seasons=[int(season)],
                             incremental=True, output_path=output_path)

            if df is not None and len(df) > 0:
                elapsed = round(time.monotonic() - t0, 2)
                logger.info("  [features] %s ✓ (%.1fs, in-process)", sport, elapsed)
                return {"sport": sport, "status": "ok", "duration_s": elapsed,
                        "output": f"{len(df)} rows"}

            if fallback_season:
                logger.info("  [features] %s season %s empty — trying fallback %s",
                            sport, season, fallback_season)
            else:
                elapsed = round(time.monotonic() - t0, 2)
                return {"sport": sport, "status": "error",
                        "error": "no data (in-process)", "duration_s": elapsed}
        except Exception as exc:
            logger.debug("  [features] %s in-process failed (%s) — falling back to subprocess",
                         sport, exc)

    # --- subprocess path (real work or fallback) ---------------------------
    def _extract(s: str) -> tuple[subprocess.CompletedProcess, float]:
        out = features_dir / f"{sport}_{s}.parquet"
        cmd = [
            sys.executable, "-m", "ml.feature_extraction",
            "--sport", sport, "--seasons", s, "--output", str(out),
            "--incremental",
        ]
        proc = _run_subprocess(cmd, cwd=backend_dir, timeout=_FEATURE_EXTRACT_TIMEOUT)
        return proc, round(time.monotonic() - t0, 2)

    try:
        proc, elapsed = _extract(season)
        if proc.returncode == 0:
            logger.info("  [features] %s ✓ (%.1fs)", sport, elapsed)
            return {"sport": sport, "status": "ok", "duration_s": elapsed,
                    "output": proc.stdout.strip()}

        err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
        if fallback_season:
            logger.info("  [features] %s season %s failed (%s) — retrying with fallback %s",
                        sport, season, err[:80], fallback_season)
            try:
                proc2, elapsed2 = _extract(fallback_season)
                if proc2.returncode == 0:
                    logger.info("  [features] %s (fallback %s) ✓ (%.1fs)",
                                sport, fallback_season, elapsed2)
                    return {"sport": sport, "status": "ok", "duration_s": elapsed2,
                            "output": proc2.stdout.strip()}
                err = proc2.stderr.strip()[-300:] if proc2.stderr else "unknown"
            except subprocess.TimeoutExpired:
                elapsed = round(time.monotonic() - t0, 2)
                return {"sport": sport, "status": "error",
                        "error": f"timeout ({_FEATURE_EXTRACT_TIMEOUT}s) on fallback",
                        "duration_s": elapsed}

        logger.debug("  [features] %s skipped (%.1fs): %s", sport, elapsed, err)
        return {"sport": sport, "status": "error", "error": err, "duration_s": elapsed}
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - t0, 2)
        return {"sport": sport, "status": "error",
                "error": f"timeout ({_FEATURE_EXTRACT_TIMEOUT}s)", "duration_s": elapsed}
def _worker_train_sport(
    sport: str,
    season: str,
    backend_dir: str,
    data_dir: str,
) -> dict[str, Any]:
    """Train models for a single sport in a subprocess."""
    t0 = time.monotonic()
    logger.info("  [train] Starting %s …", sport)
    try:
        season_int = int(season)
        seasons_str = ",".join(str(y) for y in range(2020, season_int + 1))
        script = (
            "import json; "
            "from ml.models.base import TrainingConfig; "
            "from ml.train import Trainer; "
            f"from pathlib import Path; "
            f"config = TrainingConfig(sport='{sport}', seasons=[{seasons_str}]); "
            f"trainer = Trainer(config, Path('{data_dir}')); "
            "trainer.train_joint(); "
            f"print(json.dumps({{'sport': '{sport}', 'status': 'ok'}}))"
        )
        # Large sports (NBA 8K×183, NCAAF 18K×125) need 30-40 min for full
        # classifier+regression training.  Small sports finish in <2 min.
        _TRAIN_TIMEOUT = 2400  # 40 minutes max per sport
        proc = _run_subprocess(
            [sys.executable, "-c", script],
            cwd=backend_dir, timeout=_TRAIN_TIMEOUT,
        )
        elapsed = round(time.monotonic() - t0, 2)
        if proc.returncode != 0:
            err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
            logger.debug("  [train] %s skipped (%.1fs): %s", sport, elapsed, err)
            return {"sport": sport, "status": "error", "error": err, "duration_s": elapsed}
        logger.info("  [train] %s ✓ (%.1fs)", sport, elapsed)
        return {"sport": sport, "status": "ok", "duration_s": elapsed}
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - t0, 2)
        return {"sport": sport, "status": "error", "error": f"timeout ({_TRAIN_TIMEOUT}s)", "duration_s": elapsed}


def _worker_train_player_props(
    sport: str,
    season: str,
    backend_dir: str,
) -> dict[str, Any]:
    """Train player props model for a single sport in a subprocess."""
    t0 = time.monotonic()
    logger.info("  [train/props] Starting %s …", sport)
    try:
        season_int = int(season)
        seasons_str = ",".join(str(y) for y in range(2020, season_int + 1))
        script = (
            "from ml.train_player_props import train_player_props, save_models; "
            f"bundle = train_player_props('{sport}', [{seasons_str}]); "
            f"save_models(bundle, '{sport}')"
        )
        proc = _run_subprocess(
            [sys.executable, "-c", script],
            cwd=backend_dir, timeout=1200,
        )
        elapsed = round(time.monotonic() - t0, 2)
        if proc.returncode != 0:
            err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
            logger.debug("  [train/props] %s skipped (%.1fs): %s", sport, elapsed, err)
            return {"sport": sport, "task": "player_props", "status": "error", "error": err, "duration_s": elapsed}
        logger.info("  [train/props] %s ✓ (%.1fs)", sport, elapsed)
        return {"sport": sport, "task": "player_props", "status": "ok", "duration_s": elapsed}
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - t0, 2)
        return {"sport": sport, "task": "player_props", "status": "error", "error": "timeout (1200s)", "duration_s": elapsed}


def _worker_train_golf(
    backend_dir: str,
    data_dir: str,
) -> dict[str, Any]:
    """Train golf model in a subprocess."""
    t0 = time.monotonic()
    logger.info("  [train/golf] Starting …")
    try:
        script = (
            "from pathlib import Path; "
            "from ml.train_golf import train_golf; "
            f"data_dir = Path('{data_dir}'); "
            f"models_dir = Path('{backend_dir}') / 'ml' / 'models' / 'golf'; "
            "models_dir.mkdir(parents=True, exist_ok=True); "
            "train_golf(data_dir, models_dir)"
        )
        proc = _run_subprocess(
            [sys.executable, "-c", script],
            cwd=backend_dir, timeout=1800,
        )
        elapsed = round(time.monotonic() - t0, 2)
        if proc.returncode != 0:
            err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
            logger.debug("  [train/golf] skipped (%.1fs): %s", elapsed, err)
            return {"sport": "golf", "task": "golf", "status": "error", "error": err, "duration_s": elapsed}
        logger.info("  [train/golf] ✓ (%.1fs)", elapsed)
        return {"sport": "golf", "task": "golf", "status": "ok", "duration_s": elapsed}
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - t0, 2)
        return {"sport": "golf", "task": "golf", "status": "error", "error": "timeout (1800s)", "duration_s": elapsed}


def _worker_predict_sport(
    sport: str,
    backend_dir: str,
    predict_dates: list[str] | None = None,
) -> dict[str, Any]:
    """Generate predictions for a single sport in a subprocess.

    Passes comma-separated dates so model loads ONCE per sport.
    """
    t0 = time.monotonic()
    logger.info("  [predict] Starting %s …", sport)
    dates = predict_dates or [date.today().isoformat()]
    dates_csv = ",".join(dates)
    try:
        proc = _run_subprocess(
            [sys.executable, "-m", "ml.train", "predict",
             "--sport", sport, "--date", dates_csv],
            cwd=backend_dir, timeout=180,
        )
        elapsed = round(time.monotonic() - t0, 2)
        if proc.returncode != 0:
            err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
            logger.debug("  [predict] %s partial (%.1fs): %s", sport, elapsed, err)
            return {"sport": sport, "status": "ok", "duration_s": elapsed, "warning": err}
        logger.info("  [predict] %s ✓ (%.1fs, %d dates)", sport, elapsed, len(dates))
        return {"sport": sport, "status": "ok", "duration_s": elapsed}
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - t0, 2)
        logger.warning("  [predict] %s timeout (%.1fs)", sport, elapsed)
        return {"sport": sport, "status": "ok", "duration_s": elapsed, "warning": "timeout"}


def _worker_predict_player_props(
    sport: str,
    backend_dir: str,
    predict_dates: list[str],
) -> dict[str, Any]:
    """Generate player prop predictions for a single sport in a subprocess.

    Passes comma-separated dates so model loads ONCE per sport.
    """
    t0 = time.monotonic()
    logger.info("  [player_props] Starting %s …", sport)
    dates_csv = ",".join(predict_dates)
    try:
        proc = _run_subprocess(
            [sys.executable, "-m", "ml.train", "predict-props",
             "--sport", sport, "--date", dates_csv],
            cwd=backend_dir, timeout=900,
        )
        elapsed = round(time.monotonic() - t0, 2)
        if proc.returncode != 0:
            err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
            logger.debug("  [player_props] %s partial (%.1fs): %s", sport, elapsed, err)
        else:
            logger.info("  [player_props] %s ✓ (%.1fs)", sport, elapsed)
        return {"sport": sport, "status": "ok", "duration_s": elapsed}
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - t0, 2)
        logger.warning("  [player_props] %s timeout (%.1fs)", sport, elapsed)
        return {"sport": sport, "status": "ok", "duration_s": elapsed, "warning": "timeout"}





@dataclass
class StepResult:
    name: str
    status: str = "skipped"  # ok / error / skipped / dry_run
    duration_s: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


def _sport_has_games_on_dates(sport: str, dates: list[str]) -> bool:
    """Quick check if a sport has any games on the given date(s).

    Reads the normalized games parquet schema-efficiently to check for
    scheduled/upcoming games without loading the entire dataset.
    Returns True if any games found, False otherwise.
    """
    try:
        import pandas as pd
        norm_dir = DATA_DIR / "normalized" / sport
        if not norm_dir.exists():
            return False
        parquet_files = sorted(norm_dir.glob("games_*.parquet"))
        if not parquet_files:
            return False
        # Only check the most recent season file (games today would be there)
        latest = parquet_files[-1]
        df = pd.read_parquet(latest, columns=["date", "status"])
        date_col = pd.to_datetime(df["date"], errors="coerce").dt.date
        target_dates = {date.fromisoformat(d) for d in dates}
        mask = date_col.isin(target_dates)
        return int(mask.sum()) > 0
    except Exception:
        # On any error, assume games exist (don't skip predictions)
        return True


# Sports with infrequent schedules (weekly/biweekly/event-based)
# These need a wider lookahead window so we don't miss upcoming events.
_EVENT_SPORTS = frozenset({
    "f1", "indycar", "nascar",          # race weekends every 1-3 weeks
    "ufc",                               # fight cards every 1-2 weeks
    "golf", "lpga",                      # weekly tournaments
    "ncaaf", "nfl",                      # weekly games (Thu/Sat/Sun/Mon)
    "worldcup", "euros",                 # tournament phases
})

# Lookahead days: event sports check 7 days ahead, daily sports check 2 days
_EVENT_LOOKAHEAD_DAYS = 7
_DAILY_LOOKAHEAD_DAYS = 2


def _prediction_dates_for_sport(sport: str, target_date: date) -> list[str]:
    """Return the date range to check for upcoming games.

    Event sports (F1, UFC, golf, NFL, etc.) look 7 days ahead.
    Daily sports (MLB, NBA, NHL, etc.) look 2 days ahead (today + tomorrow).
    """
    days = _EVENT_LOOKAHEAD_DAYS if sport in _EVENT_SPORTS else _DAILY_LOOKAHEAD_DAYS
    return [(target_date + timedelta(days=d)).isoformat() for d in range(days)]


class Pipeline:
    """Daily pipeline orchestrator with parallel processing support."""

    def __init__(
        self,
        target_date: date,
        dry_run: bool = False,
        sport_filter: str | None = None,
        parallel: bool = True,
        max_workers: int = 8,
        import_timeout: int = 900,
        smart_seasons: bool = True,
        recent_days: int = 3,
        train_max_age_hours: int = 24,
        force_normalize: bool = False,
    ) -> None:
        self.target_date = target_date
        self.dry_run = dry_run
        self.sport_filter = sport_filter
        self.parallel = parallel
        self.max_workers = max_workers
        self.import_timeout = import_timeout
        self.smart_seasons = smart_seasons
        self.recent_days = recent_days
        self.train_max_age_hours = train_max_age_hours
        self.force_normalize = force_normalize
        # Per-sport season is computed via _season_for_sport(); keep a
        # default for provider-level imports that span multiple sports.
        if sport_filter and sport_filter != "all":
            self.season = _season_for_sport(sport_filter, target_date)
        else:
            self.season = str(target_date.year)
        self.steps: list[StepResult] = []
        self.step_timings: dict[str, float] = {}

    def _sports_list(self, all_sports: list[str] | set[str]) -> list[str]:
        """Return sports filtered by --sport, smart season detection, or all."""
        if self.sport_filter and self.sport_filter != "all":
            return [s for s in sorted(all_sports) if s == self.sport_filter]
        candidates = sorted(all_sports)
        if self.smart_seasons:
            active = get_active_sports(candidates, self.target_date)
            skipped = set(candidates) - set(active)
            if skipped:
                logger.info("  Skipping out-of-season sports: %s", ", ".join(sorted(skipped)))
            return active
        return candidates

    # ── Helpers ──────────────────────────────────────────

    def _get_target_season(self, sport: str) -> int:
        """Return the integer season year for *sport* based on target_date."""
        return int(_season_for_sport(sport, self.target_date))

    def _run_step(self, name: str, fn, *args, **kwargs) -> StepResult:
        """Execute a step with timing, error handling, and dry-run support."""
        logger.info("═" * 60)
        logger.info("  STEP: %s", name)
        logger.info("═" * 60)
        result = StepResult(name=name)
        t0 = time.monotonic()
        try:
            if self.dry_run:
                logger.info("  [DRY RUN] Would execute: %s", name)
                result.status = "dry_run"
            else:
                details = fn(*args, **kwargs)
                result.status = "ok"
                result.details = details or {}
        except Exception as exc:
            logger.error("  FAILED: %s — %s", name, exc, exc_info=True)
            result.status = "error"
            result.error = str(exc)
        result.duration_s = round(time.monotonic() - t0, 2)
        self.step_timings[name] = result.duration_s
        logger.info(
            "  %s completed in %.1fs [%s]", name, result.duration_s, result.status
        )
        self.steps.append(result)
        return result

    def _run_parallel(
        self,
        label: str,
        worker_fn,
        items: list[tuple],
        item_label_fn=None,
    ) -> list[dict[str, Any]]:
        """Run *worker_fn* for each item, in parallel or sequentially.

        Uses ThreadPoolExecutor for orchestration — actual parallelism comes
        from the subprocess calls inside each worker function.
        """
        if not items:
            return []

        results: list[dict[str, Any]] = []
        if item_label_fn is None:
            item_label_fn = lambda args: args[0]

        if not self.parallel or len(items) == 1:
            for item_args in items:
                results.append(worker_fn(*item_args))
            return results

        workers = min(self.max_workers, len(items))
        logger.info(
            "  Running %d %s task(s) in parallel (max_workers=%d)",
            len(items), label, workers,
        )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_label = {}
            for item_args in items:
                future = executor.submit(worker_fn, *item_args)
                future_to_label[future] = item_label_fn(item_args)

            for future in as_completed(future_to_label):
                lbl = future_to_label[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    logger.error("  [%s] %s raised exception: %s", label, lbl, exc)
                    results.append({"label": lbl, "status": "error", "error": str(exc)})

        return results

    # ── Step 1: Import data (parallel by provider) ──────

    def step_import(self) -> dict[str, Any]:
        """Import data from all active providers via the TypeScript CLI.

        ESPN is split into one-sport-per-process for proper per-sport season
        detection and to avoid cross-sport timeout cascades.  Only in-season
        sports are imported when smart_seasons is enabled.
        """
        providers = _get_enabled_providers()
        # Filter out known-dead providers
        providers = [p for p in providers if p not in _DISABLED_PROVIDERS]
        results: dict[str, Any] = {"providers_attempted": len(providers), "errors": []}

        # Pre-compute in-season sports once for filtering
        active_sports = set(get_active_sports(_ESPN_SPORTS, self.target_date)) if self.smart_seasons else set(_ESPN_SPORTS)

        import_args: list[tuple] = []
        active_sports_csv = ",".join(sorted(active_sports)) if self.smart_seasons else None
        for provider in providers:
            if provider == "espn" and not self.sport_filter:
                # One ESPN process per in-season sport with correct per-sport season
                for sport in _ESPN_SPORTS:
                    if sport not in active_sports:
                        continue
                    sport_season = _season_for_sport(sport, self.target_date)
                    # Event sports (F1, UFC, golf, NFL) use wider import window
                    # to capture schedule changes for upcoming events
                    sport_recent_days = max(self.recent_days, _EVENT_LOOKAHEAD_DAYS) if sport in _EVENT_SPORTS else self.recent_days
                    import_args.append(
                        ("espn", sport, sport_season, str(IMPORTERS_DIR), self.import_timeout, 1, sport_recent_days)
                    )
            else:
                # Pass in-season sports filter to non-ESPN providers too
                sports_arg = self.sport_filter or active_sports_csv
                import_args.append(
                    (provider, sports_arg, self.season, str(IMPORTERS_DIR), self.import_timeout, 1, self.recent_days)
                )

        skipped_sports = set(_ESPN_SPORTS) - active_sports
        if skipped_sports:
            logger.info("  Import: skipping %d off-season ESPN sports: %s", len(skipped_sports), ", ".join(sorted(skipped_sports)))

        results["providers_attempted"] = len(import_args)
        worker_results = self._run_parallel(
            "import", _worker_import_provider, import_args,
        )

        for r in worker_results:
            if r.get("status") != "ok":
                results["errors"].append(
                    {"provider": r.get("provider"), "error": r.get("error")}
                )

        results["providers_succeeded"] = results["providers_attempted"] - len(results["errors"])
        return results

    # ── Step 2: Normalize (parallel by sport) ────────────

    def step_normalize(self) -> dict[str, Any]:
        """Normalize raw data into unified parquet files."""
        if not self.parallel:
            # Sequential mode — use original in-process approach
            from normalization import Normalizer

            normalizer = Normalizer()
            stats = normalizer.run_all(seasons=[self.season])
            total_rows = sum(v for sport in stats.values() for v in sport.values())
            sports_with_data = sum(1 for s in stats.values() if any(v > 0 for v in s.values()))
            return {
                "total_rows": total_rows,
                "sports_processed": len(stats),
                "sports_with_data": sports_with_data,
                "per_sport": {
                    sport: sum(counts.values())
                    for sport, counts in stats.items()
                    if any(counts.values())
                },
            }

        # Parallel mode — each sport normalized in its own subprocess
        # Pre-check: skip sports with no raw data directory
        from config import ALL_SPORTS

        sports = self._sports_list(ALL_SPORTS)
        norm_args = []
        skipped_fresh = []
        for sport in sports:
            raw_sport_dir = DATA_DIR / "raw" / "espn" / sport
            if not raw_sport_dir.exists():
                logger.debug("  [normalize] Skipping %s — no raw data dir", sport)
                continue
            season = _season_for_sport(sport, self.target_date)
            # Smart-skip: if normalized parquet exists and is newer than all
            # raw data files for this season, skip re-normalizing
            # (bypass with --force-normalize)
            norm_dir = DATA_DIR / "normalized" / sport
            norm_games = norm_dir / f"games_{season}.parquet"
            # Some sports (ncaab, ncaaw) don't produce games_*.parquet —
            # fall back to any parquet file in the normalized directory.
            if not norm_games.exists():
                norm_games = max(
                    (f for f in norm_dir.glob("*.parquet")),
                    key=lambda f: f.stat().st_mtime,
                    default=None,
                )
            if norm_games is not None and not self.force_normalize:
                norm_mtime = norm_games.stat().st_mtime
                raw_season_dir = raw_sport_dir / str(season)
                needs_update = False
                if raw_season_dir.exists():
                    # Check if any raw file is newer (sample up to 50 for speed)
                    for i, fpath in enumerate(raw_season_dir.rglob("*.json")):
                        if fpath.stat().st_mtime > norm_mtime:
                            needs_update = True
                            break
                        if i >= 50:
                            break
                else:
                    needs_update = True  # no raw dir → first normalization
                if not needs_update:
                    skipped_fresh.append(sport)
                    continue

            norm_args.append(
                (sport, season, str(BACKEND_DIR), True)
            )

        if skipped_fresh:
            logger.info("  [normalize] Fresh (no new raw data): %s", ", ".join(sorted(skipped_fresh)))

        # Cap normalize concurrency at 6 — each worker spawns a subprocess, so
        # running all 22+ sports simultaneously can cause OOM / C++ crashes.
        saved_workers = self.max_workers
        self.max_workers = min(self.max_workers, 6)
        worker_results = self._run_parallel("normalize", _worker_normalize_sport, norm_args)
        self.max_workers = saved_workers

        errors = []
        succeeded = 0
        failed_sports = {r.get("sport") for r in worker_results if r.get("status") != "ok"}
        retry_args = [a for a in norm_args if a[0] in failed_sports]

        for r in worker_results:
            if r.get("status") == "ok":
                succeeded += 1

        # Retry failed sports sequentially to avoid resource pressure
        if retry_args:
            logger.info("  [normalize] Retrying %d failed sport(s): %s",
                        len(retry_args), ", ".join(a[0] for a in retry_args))
            for args in retry_args:
                r = _worker_normalize_sport(*args)
                if r.get("status") == "ok":
                    succeeded += 1
                    logger.info("  [normalize] %s ✓ (retry)", args[0])
                else:
                    errors.append(r)

        return {
            "sports_processed": len(sports),
            "sports_succeeded": succeeded,
            "errors": errors,
        }

    # ── Step 3: Accuracy analysis ────────────────────────

    def step_accuracy(self) -> dict[str, Any]:
        """Analyze accuracy of previous day's predictions against actual results."""
        yesterday = (self.target_date - timedelta(days=1)).isoformat()
        predictions_dir = DATA_DIR / "predictions"
        reports_dir = DATA_DIR / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        pred_file = predictions_dir / f"{yesterday}.json"
        if not pred_file.exists():
            logger.info("  No predictions file for %s — skipping accuracy", yesterday)
            return {"status": "no_predictions", "date": yesterday}

        with open(pred_file) as f:
            predictions = json.load(f)

        if not isinstance(predictions, list):
            predictions = predictions.get("predictions", [])

        # Handle legacy string-serialized PredictionResult objects
        # Fast path: skip regex machinery if no legacy strings exist
        has_legacy = any(isinstance(p, str) for p in predictions)
        if has_legacy:
            import re as _re
            _legacy_pat = _re.compile(
                r"([a-z_]+)=((?:'[^']*'|None|True|False|[-\d.]+(?:e[-+]?\d+)?|\{[^}]*\}|\[[^\]]*\]))"
            )
            cleaned: list[dict[str, Any]] = []
            for p in predictions:
                if isinstance(p, dict):
                    cleaned.append(p)
                elif isinstance(p, str) and p.startswith("PredictionResult("):
                    try:
                        inner = p[len("PredictionResult("):-1]
                        d: dict[str, Any] = {}
                        for m in _legacy_pat.finditer(inner):
                            k, v = m.group(1), m.group(2)
                            if v.startswith("'") and v.endswith("'"):
                                d[k] = v[1:-1]
                            elif v == "None":
                                d[k] = None
                            elif v in ("True", "False"):
                                d[k] = v == "True"
                            else:
                                try:
                                    d[k] = float(v)
                                except ValueError:
                                    d[k] = v
                        if d:
                            cleaned.append(d)
                    except Exception:
                        continue
            predictions = cleaned
        else:
            predictions = [p for p in predictions if isinstance(p, dict)]

        # Filter to specific sport if --sport was provided
        if self.sport_filter:
            predictions = [p for p in predictions if p.get("sport") == self.sport_filter]

        from services.data_service import DataService

        svc = DataService()
        total = 0
        evaluated = 0
        correct = 0
        brier_scores: list[float] = []
        sport_stats: dict[str, dict[str, int]] = {}
        # Cache games per sport to avoid repeated parquet reads
        _games_cache: dict[str, list[dict]] = {}
        # Index game_id → game dict for O(1) lookups
        _games_index: dict[str, dict[str, dict]] = {}

        for pred in predictions:
            if not isinstance(pred, dict):
                continue
            total += 1
            sport = pred.get("sport", "unknown")
            if sport not in sport_stats:
                sport_stats[sport] = {"total": 0, "evaluated": 0, "correct": 0}
            sport_stats[sport]["total"] += 1

            game_id = pred.get("game_id")
            home_prob = pred.get("home_win_prob")
            if home_prob is None or game_id is None:
                continue

            # Try to find actual result from normalized data
            try:
                if sport not in _games_cache:
                    # Only load columns needed for accuracy checking
                    _games_cache[sport] = svc.get_games(
                        sport, date=yesterday,
                        columns=["id", "game_id", "date", "home_score", "away_score"],
                    )
                    # Build O(1) lookup index by game ID
                    idx: dict[str, dict] = {}
                    for g in _games_cache[sport]:
                        gid = str(g.get("id") or g.get("game_id", ""))
                        if gid:
                            idx[gid] = g
                    _games_index[sport] = idx
                actual = _games_index.get(sport, {}).get(str(game_id))
            except Exception:
                actual = None

            if not actual:
                continue

            home_score = actual.get("home_score")
            away_score = actual.get("away_score")
            if home_score is None or away_score is None:
                continue

            try:
                home_score = float(home_score)
                away_score = float(away_score)
            except (ValueError, TypeError):
                continue

            if home_score == away_score:
                continue  # skip draws for winner accuracy

            evaluated += 1
            sport_stats[sport]["evaluated"] += 1

            predicted_home = float(home_prob) > 0.5
            actual_home = home_score > away_score
            if predicted_home == actual_home:
                correct += 1
                sport_stats[sport]["correct"] += 1

            brier = (float(home_prob) - (1.0 if actual_home else 0.0)) ** 2
            brier_scores.append(brier)

        accuracy = correct / evaluated if evaluated else None
        avg_brier = sum(brier_scores) / len(brier_scores) if brier_scores else None

        report = {
            "date": yesterday,
            "total_predictions": total,
            "evaluated": evaluated,
            "correct": correct,
            "accuracy": round(accuracy, 4) if accuracy is not None else None,
            "brier_score": round(avg_brier, 4) if avg_brier is not None else None,
            "by_sport": sport_stats,
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        }

        report_path = reports_dir / f"accuracy_{yesterday}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("  Accuracy report saved to %s", report_path)

        return report

    # ── Step 4: Feature extraction (parallel by sport) ───

    def step_features(self) -> dict[str, Any]:
        """Extract features for all sports with available data."""
        if not self.parallel:
            # Sequential mode — original in-process approach
            from config import ALL_SPORTS
            from features import extract_features

            features_dir = DATA_DIR / "features"
            features_dir.mkdir(parents=True, exist_ok=True)

            results: dict[str, Any] = {"sports": {}, "errors": []}

            for sport in self._sports_list(ALL_SPORTS):
                season = _season_for_sport(sport, self.target_date)
                season_int = int(season)
                try:
                    df = extract_features(sport, season_int, DATA_DIR)
                    if df is not None and len(df) > 0:
                        out_path = features_dir / f"{sport}_{season}.parquet"
                        df.to_parquet(out_path, compression="snappy")
                        results["sports"][sport] = {
                            "games": len(df),
                            "features": len(df.columns),
                        }
                        logger.info("  %s: %d games, %d features", sport, len(df), len(df.columns))
                except Exception as exc:
                    logger.debug("  %s: skipped (%s)", sport, exc)
                    results["errors"].append({"sport": sport, "error": str(exc)})

            results["sports_extracted"] = len(results["sports"])
            return results

        # Parallel mode — each sport in its own subprocess
        # Pre-check: skip sports with no normalized games (avoids subprocess overhead)
        from config import ALL_SPORTS

        sports = self._sports_list(ALL_SPORTS)
        feature_args = []
        skipped_no_games: list[str] = []
        for sport in sports:
            season = _season_for_sport(sport, self.target_date)
            games_parquet = DATA_DIR / "normalized" / sport / f"games_{season}.parquet"
            if not games_parquet.exists():
                logger.debug("  [features] Skipping %s — no games_%s.parquet", sport, season)
                continue
            if games_parquet.stat().st_size < 500:
                logger.debug("  [features] Skipping %s — games parquet too small", sport)
                continue
            # Smart-skip: if sport has no upcoming games within its lookahead window
            # AND model is fresh, skip feature extraction.
            # Event sports (F1, UFC, golf, NFL) use 7-day lookahead.
            lookahead_dates = _prediction_dates_for_sport(sport, self.target_date)
            model_path = PROJECT_ROOT / "ml" / "models" / sport / "joint_models.pkl"
            features_all = DATA_DIR / "features" / f"{sport}_all.parquet"
            if (
                not _sport_has_games_on_dates(sport, lookahead_dates)
                and model_path.exists()
                and features_all.exists()
                and (time.time() - model_path.stat().st_mtime) < 86400  # < 24h old
            ):
                skipped_no_games.append(sport)
                continue
            # Provide prior-season fallback in case the current season has no
            # completed games yet (e.g. MLS/NWSL at the start of a new year).
            prior_season = str(int(season) - 1)
            prior_parquet = DATA_DIR / "normalized" / sport / f"games_{prior_season}.parquet"
            fallback = prior_season if prior_parquet.exists() else None
            feature_args.append((sport, season, str(BACKEND_DIR), str(DATA_DIR), fallback))

        if skipped_no_games:
            logger.info("  [features] Smart-skip (no games + fresh model): %s",
                        ", ".join(skipped_no_games))

        worker_results = self._run_parallel("features", _worker_extract_features, feature_args)

        results = {"sports": {}, "errors": []}
        for r in worker_results:
            if r.get("status") == "ok":
                results["sports"][r["sport"]] = {"status": "ok"}
            else:
                results["errors"].append(r)
        results["sports_extracted"] = len(results["sports"])
        return results
    
    # ── Step 4b: Consolidate feature stores (rebuild _all.parquet files) ────
    
    def step_consolidate_features(self) -> dict[str, Any]:
        """Consolidate seasonal feature parquets into combined _all.parquet files.
        
        This rebuilds the multi-season feature stores that models use for training,
        ensuring fresh data is available rather than stale consolidated files.
        Runs in-process (no subprocess) to eliminate startup overhead.
        """
        logger.info("  Consolidating feature stores...")
        try:
            # Import directly to avoid subprocess startup overhead (~300ms saved)
            sys.path.insert(0, str(SCRIPT_DIR))
            from consolidate_features import consolidate_sport

            features_dir = DATA_DIR / "features"
            if not features_dir.is_dir():
                logger.info("  No features directory — skipping consolidation")
                return {"status": "skipped", "reason": "no_features_dir"}

            # Discover sports with seasonal files
            sports = set()
            for fpath in features_dir.glob("*_*.parquet"):
                parts = fpath.stem.split("_")
                if len(parts) >= 2 and parts[-1].isdigit():
                    sports.add("_".join(parts[:-1]))

            if not sports:
                logger.info("  No seasonal features found to consolidate")
                return {"status": "skipped", "reason": "no_seasonal_files"}

            # Apply sport filter if set
            if self.sport_filter and self.sport_filter != "all":
                sports = {s for s in sports if s == self.sport_filter}

            # Parallelize across sports (each writes to a separate _all.parquet)
            sport_list = sorted(sports)
            workers = min(len(sport_list), 6)
            results = []
            if workers > 1:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = {
                        pool.submit(consolidate_sport, s, features_dir, True): s
                        for s in sport_list
                    }
                    for fut in as_completed(futs):
                        results.append(fut.result())
            else:
                for s in sport_list:
                    results.append(consolidate_sport(s, features_dir, True))

            successful = sum(1 for r in results if r.get("status") == "consolidated")
            for r in results:
                if r.get("status") == "consolidated":
                    logger.info("  ✓ %s: %d files → %d rows", r["sport"], r.get("files_combined", 0), r.get("rows_output", 0))

            return {"status": "ok", "consolidated": successful, "total": len(results)}
        except Exception as e:
            logger.warning("  Consolidation error: %s", str(e))
            return {"status": "error", "error": str(e)}

    def step_pregame_features(self) -> dict[str, Any]:
        """Pre-extract features for upcoming (unplayed) games.

        After normal feature extraction produces features for completed games,
        this step extracts features for today's and tomorrow's unplayed games
        and appends them to the seasonal parquet.  This lets the predict step
        use the fast batch path instead of expensive inline extraction
        (~20s → ~0.1s per sport).
        """
        import pandas as _pd
        from datetime import timedelta as _td

        if BACKEND_DIR not in sys.path:
            sys.path.insert(0, str(BACKEND_DIR))
        from features.registry import get_extractor, EXTRACTORS

        target_dates = [
            self.target_date.isoformat(),
            (self.target_date + _td(days=1)).isoformat(),
        ]

        sports = self._sports_list(EXTRACTORS.keys())
        features_dir = DATA_DIR / "features"
        total_added = 0
        sports_updated = []

        for sport in sports:
            try:
                # Find the seasonal parquet
                season_str = str(self._get_target_season(sport))
                output_path = features_dir / f"{sport}_{season_str}.parquet"
                if not output_path.exists():
                    continue

                # Load existing game_ids
                existing_df = _pd.read_parquet(output_path)
                if "game_id" not in existing_df.columns:
                    continue
                existing_ids = set(existing_df["game_id"].astype(str))

                # Find upcoming games not yet in features
                norm_dir = DATA_DIR / "normalized" / sport
                upcoming_ids = []
                upcoming_games = _pd.DataFrame()
                for s in [int(season_str), int(season_str) + 1, int(season_str) - 1]:
                    gp = norm_dir / f"games_{s}.parquet"
                    if gp.exists():
                        gdf = _pd.read_parquet(gp)
                        if "date" in gdf.columns:
                            gdf["date"] = _pd.to_datetime(gdf["date"])
                            mask = gdf["date"].dt.date.astype(str).isin(target_dates)
                            upcoming_games = gdf[mask]
                            break

                if upcoming_games.empty:
                    continue

                id_col = "game_id" if "game_id" in upcoming_games.columns else "id"
                missing = upcoming_games[
                    ~upcoming_games[id_col].astype(str).isin(existing_ids)
                ]
                if missing.empty:
                    continue

                # Extract features for missing upcoming games
                extractor = get_extractor(sport, DATA_DIR)
                new_rows = []
                for _, game in missing.iterrows():
                    gid = str(game[id_col])
                    try:
                        row = extractor.extract_game_features(game.to_dict())
                        if row is not None:
                            if isinstance(row, dict) and row:
                                new_rows.append(row)
                            elif isinstance(row, _pd.DataFrame) and not row.empty:
                                new_rows.append(row.iloc[0].to_dict())
                    except Exception:
                        pass

                if not new_rows:
                    continue

                new_df = _pd.DataFrame(new_rows)
                # Ensure game_id column exists
                if "game_id" not in new_df.columns and id_col in new_df.columns:
                    new_df["game_id"] = new_df[id_col]

                # Align columns with existing parquet
                for col in existing_df.columns:
                    if col not in new_df.columns:
                        new_df[col] = 0
                new_df = new_df[[c for c in existing_df.columns if c in new_df.columns]]

                # Match dtypes to existing parquet (prevents ArrowTypeError)
                for col in new_df.columns:
                    if col in existing_df.columns:
                        edtype = existing_df[col].dtype
                        try:
                            if edtype == object:
                                new_df[col] = new_df[col].astype(str)
                            else:
                                new_df[col] = new_df[col].astype(edtype)
                        except (ValueError, TypeError):
                            new_df[col] = new_df[col].astype(str)

                # Append to parquet
                combined = _pd.concat([existing_df, new_df], ignore_index=True)
                if "game_id" in combined.columns:
                    combined = combined.drop_duplicates(subset=["game_id"], keep="last")
                combined.to_parquet(output_path, index=False)

                total_added += len(new_df)
                sports_updated.append(f"{sport}:{len(new_df)}")
                logger.info("  [pregame] %s: +%d upcoming game features", sport, len(new_df))

            except Exception as exc:
                logger.warning("  [pregame] %s failed: %s", sport, exc)

        if sports_updated:
            logger.info("  Pre-game features: %s (%d total)", ", ".join(sports_updated), total_added)
        return {"status": "ok", "added": total_added, "sports": sports_updated}

    # ── Step 5: Model training (all-in-one parallel) ────────

    def step_train(self) -> dict[str, Any]:
        """Train all models in parallel: game models, player props, and golf.

        Collects all training tasks (game models per sport, player props per
        sport, golf) into a single batch and runs them concurrently via
        _run_parallel for maximum throughput.
        """
        results: dict[str, Any] = {"trained": [], "skipped": [], "errors": []}

        if not self.parallel:
            # Sequential fallback — original in-process approach
            from features.registry import EXTRACTORS
            from ml.models.base import TrainingConfig
            from ml.train import Trainer

            for sport in self._sports_list(EXTRACTORS.keys()):
                season = _season_for_sport(sport, self.target_date)
                season_int = int(season)
                seasons = _training_seasons(season_int)
                all_file = DATA_DIR / "features" / f"{sport}_all.parquet"
                season_file = DATA_DIR / "features" / f"{sport}_{season}.parquet"
                if not all_file.exists() and not season_file.exists():
                    results["skipped"].append(sport)
                    continue
                try:
                    config = TrainingConfig(sport=sport, seasons=seasons)
                    trainer = Trainer(config, DATA_DIR)
                    trainer.train_joint()
                    results["trained"].append(sport)
                    logger.info("  %s: trained ✓", sport)
                except Exception as exc:
                    logger.warning("  %s: training failed (%s)", sport, exc)
                    results["errors"].append({"sport": sport, "error": str(exc)})
            return results

        # Parallel mode — collect ALL training tasks and run concurrently
        from features.registry import EXTRACTORS

        train_args: list[tuple] = []
        now_ts = time.time()
        max_age_secs = self.train_max_age_hours * 3600
        models_root = PROJECT_ROOT / "ml" / "models"

        # Game model training per sport (only if feature data exists)
        for sport in self._sports_list(EXTRACTORS.keys()):
            season = _season_for_sport(sport, self.target_date)
            all_file = DATA_DIR / "features" / f"{sport}_all.parquet"
            season_file = DATA_DIR / "features" / f"{sport}_{season}.parquet"
            if not all_file.exists() and not season_file.exists():
                results["skipped"].append(sport)
                continue
            # Skip retraining if model is fresh (within --train-max-age hours)
            model_file = models_root / sport / "joint_models.pkl"
            if max_age_secs > 0 and model_file.exists():
                age = now_ts - model_file.stat().st_mtime
                if age < max_age_secs:
                    logger.info("  [train] %s model fresh (%.1fh old, max %dh) — skipping",
                                sport, age / 3600, self.train_max_age_hours)
                    results["skipped"].append(sport)
                    continue
            train_args.append(
                (sport, season, str(BACKEND_DIR), str(DATA_DIR))
            )

        # Player props training — all sports with prop specs
        from ml.train_player_props import _PROP_SPECS  # noqa: F811
        _trainable_sports = {a[0] for a in train_args}
        props_sports = [s for s in sorted(_PROP_SPECS) if s in _trainable_sports]
        props_args = [
            (sport, _season_for_sport(sport, self.target_date), str(BACKEND_DIR))
            for sport in props_sports
        ]

        # Golf training (if feature data exists)
        golf_data = DATA_DIR / "features" / "golf_all.parquet"
        has_golf = golf_data.exists()
        if has_golf and max_age_secs > 0:
            golf_model = models_root / "golf" / "joint_models.pkl"
            if golf_model.exists() and (now_ts - golf_model.stat().st_mtime) < max_age_secs:
                logger.info("  [train] golf model fresh — skipping")
                has_golf = False

        # Run ALL training workers in one parallel batch using a unified wrapper
        all_tasks: list[tuple] = []

        # Game models
        for args in train_args:
            all_tasks.append(("game", args))

        # Player props
        for args in props_args:
            all_tasks.append(("props", args))

        # Golf
        if has_golf:
            all_tasks.append(("golf", (str(BACKEND_DIR), str(DATA_DIR))))

        if not all_tasks:
            logger.info("  No sports with feature data — skipping training")
            return results

        # Dispatch to appropriate workers
        def _dispatch_train(task_type: str, args: tuple) -> dict[str, Any]:
            if task_type == "game":
                return _worker_train_sport(*args)
            elif task_type == "props":
                return _worker_train_player_props(*args)
            elif task_type == "golf":
                return _worker_train_golf(*args)
            return {"status": "error", "error": f"unknown task type: {task_type}"}

        # Training processes are CPU-heavy (each spawns multiprocessing workers).
        # Cap at 4 concurrent training subprocesses to avoid OOM/contention.
        workers = min(4, len(all_tasks))
        logger.info(
            "  Running %d training task(s) in parallel (max_workers=%d): "
            "%d game, %d props, %d golf",
            len(all_tasks), workers,
            len(train_args), len(props_args), 1 if has_golf else 0,
        )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_label = {}
            for task_type, args in all_tasks:
                future = executor.submit(_dispatch_train, task_type, args)
                label = f"{task_type}/{args[0]}" if task_type != "golf" else "golf"
                future_to_label[future] = label

            for future in as_completed(future_to_label):
                lbl = future_to_label[future]
                try:
                    r = future.result()
                    if r.get("status") == "ok":
                        results["trained"].append(lbl)
                    else:
                        results["errors"].append(r)
                except Exception as exc:
                    logger.error("  [train] %s raised: %s", lbl, exc)
                    results["errors"].append({"label": lbl, "error": str(exc)})

        return results

    # ── Step 6: Predictions (parallel by sport) ──────────

    def step_predict(self) -> dict[str, Any]:
        """Generate predictions for upcoming games (daily sports: 2 days, event sports: 7 days)."""
        if not self.parallel:
            # Sequential mode — original in-process approach
            from features.registry import EXTRACTORS
            from ml.predictors.game_predictor import GamePredictor

            predictions_dir = DATA_DIR / "predictions"
            predictions_dir.mkdir(parents=True, exist_ok=True)

            all_predictions: dict[str, list[dict[str, Any]]] = {}

            for sport in self._sports_list(EXTRACTORS.keys()):
                models_dir = PROJECT_ROOT / "ml" / "models" / sport
                if not (models_dir / "joint_models.pkl").exists() and not (
                    models_dir / "separate_models.pkl"
                ).exists():
                    continue

                target_dates = _prediction_dates_for_sport(sport, self.target_date)

                try:
                    predictor = GamePredictor(sport, models_dir, DATA_DIR)
                except Exception:
                    logger.debug("  %s: could not load predictor", sport)
                    continue

                for target in target_dates:
                    try:
                        preds = predictor.predict_date(target)
                        for p in preds:
                            if target not in all_predictions:
                                all_predictions[target] = []
                            all_predictions[target].append(asdict(p))
                    except Exception as exc:
                        logger.debug("  %s/%s: prediction failed (%s)", sport, target, exc)

            results: dict[str, Any] = {"dates": {}}
            for pred_date, preds in all_predictions.items():
                out_path = predictions_dir / f"{pred_date}.json"
                with open(out_path, "w") as f:
                    json.dump(
                        {"date": pred_date, "predictions": preds,
                         "generated_at": datetime.now(tz=timezone.utc).isoformat()},
                        f, indent=2, default=str,
                    )
                results["dates"][pred_date] = len(preds)
                logger.info("  %s: %d predictions saved", pred_date, len(preds))

            results["total_predictions"] = sum(results["dates"].values())
            return results

        # Parallel mode — each sport predicted in its own subprocess
        # Pre-check: skip sports with no trained model files or no upcoming games
        from features.registry import EXTRACTORS

        sports = self._sports_list(EXTRACTORS.keys())
        predict_args = []
        skipped_no_games = []
        for sport in sports:
            models_dir = PROJECT_ROOT / "ml" / "models" / sport
            if not models_dir.exists():
                logger.debug("  [predict] Skipping %s — no models dir", sport)
                continue
            has_model = (models_dir / "joint_models.pkl").exists() or (models_dir / "separate_models.pkl").exists()
            if not has_model:
                logger.debug("  [predict] Skipping %s — no trained models", sport)
                continue
            # Game-day detection: event sports (F1, UFC, golf, NFL) check 7 days ahead
            lookahead_dates = _prediction_dates_for_sport(sport, self.target_date)
            if not _sport_has_games_on_dates(sport, lookahead_dates):
                skipped_no_games.append(sport)
                continue
            predict_args.append((sport, str(BACKEND_DIR), lookahead_dates))

        if skipped_no_games:
            logger.info("  [predict] No upcoming games: %s", ", ".join(sorted(skipped_no_games)))

        worker_results = self._run_parallel("predict", _worker_predict_sport, predict_args)

        results = {"sports": {}, "errors": []}
        for r in worker_results:
            if r.get("status") == "ok":
                results["sports"][r["sport"]] = {"status": "ok"}
            else:
                results["errors"].append(r)
        results["total_sports_predicted"] = len(results["sports"])

        # Consolidate per-sport prediction parquets into combined JSON files
        self._consolidate_predictions(sports)

        return results

    def _consolidate_predictions(self, sports: list[str]) -> None:
        """Merge per-sport prediction parquets into combined daily JSON files.

        The parallel predict worker saves to ``normalized/{sport}/predictions.parquet``.
        The website/API reads from ``data/predictions/{date}.json``.
        """
        import pandas as pd

        predictions_dir = DATA_DIR / "predictions"
        predictions_dir.mkdir(parents=True, exist_ok=True)

        # Collect all dates that could have predictions (sport-aware lookahead)
        target_dates: set[str] = set()
        for sport in sports:
            for d in _prediction_dates_for_sport(sport, self.target_date):
                target_dates.add(d)

        # Read all sport parquets concurrently (I/O bound)
        def _read_sport(sport: str) -> list[tuple[str, list[dict]]]:
            parquet_path = DATA_DIR / "normalized" / sport / "predictions.parquet"
            if not parquet_path.exists():
                return []
            try:
                df = pd.read_parquet(parquet_path)
                if df.empty or "date" not in df.columns:
                    return []
                results = []
                for pred_date in target_dates:
                    day_df = df[df["date"].astype(str) == pred_date]
                    if day_df.empty:
                        continue
                    records = day_df.to_dict("records")
                    for rec in records:
                        rec["sport"] = sport
                    results.append((pred_date, records))
                return results
            except Exception:
                logger.debug("Could not read predictions parquet for %s", sport)
                return []

        all_preds: dict[str, list[dict]] = {}
        with ThreadPoolExecutor(max_workers=min(len(sports), 8)) as pool:
            for sport_results in pool.map(_read_sport, sports):
                for pred_date, records in sport_results:
                    all_preds.setdefault(pred_date, []).extend(records)

        for pred_date, preds in all_preds.items():
            out_path = predictions_dir / f"{pred_date}.json"
            with open(out_path, "w") as f:
                json.dump(
                    {"date": pred_date, "predictions": preds,
                     "generated_at": datetime.now(tz=timezone.utc).isoformat()},
                    f, indent=2, default=str,
                )
            logger.info("  Consolidated %d predictions for %s", len(preds), pred_date)

    # ── Step 6b: Player prop predictions ────────────────────────────────

    def step_predict_props(self) -> dict[str, Any]:
        """Generate player prop predictions for sports that have a trained player_props.pkl model."""
        from config import ALL_SPORTS

        sports = self._sports_list(ALL_SPORTS)
        props_args: list[tuple] = []
        skipped_no_games: list[str] = []
        for sport in sports:
            models_dir = PROJECT_ROOT / "ml" / "models" / sport
            if not (models_dir / "player_props.pkl").exists():
                continue
            # Game-day detection: event sports use 7-day lookahead
            lookahead_dates = _prediction_dates_for_sport(sport, self.target_date)
            if not _sport_has_games_on_dates(sport, lookahead_dates):
                skipped_no_games.append(sport)
                continue
            props_args.append((sport, str(BACKEND_DIR), lookahead_dates))

        if skipped_no_games:
            logger.info("  [player_props] No upcoming games: %s", ", ".join(sorted(skipped_no_games)))

        if not props_args:
            logger.info("  No sports with player props models — skipping")
            return {"sports": {}, "total_sports_predicted": 0}

        worker_results = self._run_parallel("player_props", _worker_predict_player_props, props_args)

        results: dict[str, Any] = {"sports": {}, "errors": []}
        for r in worker_results:
            s = r.get("sport", "?")
            if r.get("status") == "ok":
                results["sports"][s] = {"status": "ok", "duration_s": r.get("duration_s")}
            elif r.get("status") not in ("no_model", "no_games", "no_prop_types"):
                results["errors"].append(r)

        self._consolidate_player_props(list(results["sports"].keys()))
        results["total_sports_predicted"] = len(results["sports"])
        return results

    def _consolidate_player_props(self, sports: list[str]) -> None:
        """Merge per-sport player_props parquets into combined daily JSON files.

        Reads ``normalized/{sport}/player_props.parquet`` and writes
        ``data/predictions/{date}_player_props.json``.
        """
        import pandas as pd

        predictions_dir = DATA_DIR / "predictions"
        predictions_dir.mkdir(parents=True, exist_ok=True)

        # Collect all dates that could have predictions (sport-aware lookahead)
        target_dates: set[str] = set()
        for sport in sports:
            for d in _prediction_dates_for_sport(sport, self.target_date):
                target_dates.add(d)

        def _read_sport(sport: str) -> list[tuple[str, list[dict]]]:
            parquet_path = DATA_DIR / "normalized" / sport / "player_props.parquet"
            if not parquet_path.exists():
                return []
            try:
                df = pd.read_parquet(parquet_path)
                if df.empty or "date" not in df.columns:
                    return []
                results = []
                for pred_date in target_dates:
                    day_df = df[df["date"].astype(str) == pred_date]
                    if day_df.empty:
                        continue
                    records = day_df.to_dict("records")
                    for rec in records:
                        rec["sport"] = sport
                    results.append((pred_date, records))
                return results
            except Exception:
                logger.debug("Could not read player_props parquet for %s", sport)
                return []

        all_preds: dict[str, list[dict]] = {}
        if sports:
            with ThreadPoolExecutor(max_workers=min(len(sports), 8)) as pool:
                for sport_results in pool.map(_read_sport, sports):
                    for pred_date, records in sport_results:
                        all_preds.setdefault(pred_date, []).extend(records)

        for pred_date, preds in all_preds.items():
            out_path = predictions_dir / f"{pred_date}_player_props.json"
            with open(out_path, "w") as f:
                json.dump(
                    {"date": pred_date, "player_props": preds,
                     "generated_at": datetime.now(tz=timezone.utc).isoformat()},
                    f, indent=2, default=str,
                )
            logger.info("  Consolidated %d player prop predictions for %s", len(preds), pred_date)

    # ── Step 7a: Diagnostics (model perf + weak sports) ──────────────────

    def step_diagnostics(self) -> dict[str, Any]:
        """Run model performance diagnostic, weak sports analysis, and markdown summary.

        Non-blocking: failures here produce a warning log but never fail the pipeline.
        Runs only when feature parquets exist so fresh data is always analyzed.
        """
        results: dict[str, Any] = {}

        def _run(label: str, cmd: list[str]) -> dict[str, Any]:
            t0 = time.monotonic()
            try:
                proc = _run_subprocess(
                    cmd, cwd=str(PROJECT_ROOT), timeout=120,
                )
                elapsed = round(time.monotonic() - t0, 2)
                if proc.returncode == 0:
                    logger.info("  [diagnostics] %s ✓ (%.1fs)", label, elapsed)
                    return {"status": "ok", "duration_s": elapsed}
                err = (proc.stderr or proc.stdout or "unknown").strip()[-300:]
                logger.warning("  [diagnostics] %s failed (exit %d): %s", label, proc.returncode, err)
                return {"status": "error", "error": err, "duration_s": elapsed}
            except subprocess.TimeoutExpired:
                elapsed = round(time.monotonic() - t0, 2)
                logger.warning("  [diagnostics] %s timed out (120s)", label)
                return {"status": "timeout", "duration_s": elapsed}
            except Exception as exc:
                logger.warning("  [diagnostics] %s raised: %s", label, exc)
                return {"status": "error", "error": str(exc), "duration_s": 0.0}

        py = sys.executable
        data_arg = str(DATA_DIR)

        # Run model_perf and weak_sports in parallel (independent subprocesses)
        features_dir = DATA_DIR / "features"
        has_features = any(features_dir.glob("*_all.parquet"))

        if has_features:
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_model = pool.submit(
                    _run,
                    "model_performance_diagnostic",
                    [py, str(SCRIPT_DIR / "model_performance_diagnostic.py")],
                )
                fut_weak = pool.submit(
                    _run,
                    "weak_sports_feature_analysis",
                    [py, str(SCRIPT_DIR / "weak_sports_feature_analysis.py"),
                     "--data-dir", data_arg, "--max-critical", "2"],
                )
                results["model_perf"] = fut_model.result()
                results["weak_sports"] = fut_weak.result()

            # weak_sports_summary depends on weak_sports completing successfully
            if results["weak_sports"].get("status") == "ok":
                results["weak_sports_summary"] = _run(
                    "generate_weak_sports_summary",
                    [py, str(SCRIPT_DIR / "generate_weak_sports_summary.py"),
                     "--data-dir", data_arg],
                )
        else:
            results["model_perf"] = _run(
                "model_performance_diagnostic",
                [py, str(SCRIPT_DIR / "model_performance_diagnostic.py")],
            )
            logger.info("  [diagnostics] Skipping weak sports — no _all.parquet found")
            results["weak_sports"] = {"status": "skipped"}

        return results

    # ── Step 8: Backtest (automated daily evaluation) ──────────────

    def step_backtest(self) -> dict[str, Any]:
        """Run 7-day backtest to track model accuracy trends.

        Non-blocking: failures produce a warning but never fail the pipeline.
        """
        py = sys.executable
        backtest_script = SCRIPT_DIR / "backtest.py"
        if not backtest_script.exists():
            logger.debug("  [backtest] backtest.py not found — skipping")
            return {"status": "skipped", "reason": "no_script"}

        try:
            proc = _run_subprocess(
                [py, str(backtest_script), "--days", "7"],
                cwd=str(PROJECT_ROOT), timeout=300,
            )
            elapsed_msg = proc.stdout.strip()[-500:] if proc.stdout else ""
            if proc.returncode == 0:
                logger.info("  [backtest] completed")
                # Try to read the backtest output
                try:
                    report_path = DATA_DIR / "reports" / f"backtest_{self.target_date.isoformat()}.json"
                    if report_path.exists():
                        with open(report_path) as f:
                            bt_data = json.load(f)
                        total = bt_data.get("total_evaluated", 0)
                        correct = bt_data.get("total_correct", 0)
                        acc = round(correct / total * 100, 1) if total > 0 else 0
                        logger.info("  [backtest] 7-day accuracy: %d/%d (%.1f%%)", correct, total, acc)
                        return {"status": "ok", "evaluated": total, "correct": correct, "accuracy": acc}
                except Exception:
                    pass
                return {"status": "ok", "output": elapsed_msg}
            else:
                err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
                logger.warning("  [backtest] failed: %s", err)
                return {"status": "error", "error": err}
        except subprocess.TimeoutExpired:
            return {"status": "error", "error": "timeout (300s)"}
        except Exception as e:
            logger.warning("  [backtest] error: %s", e)
            return {"status": "error", "error": str(e)}

    # ── Step 9: Season Simulator ──────────────────────────────

    def step_season_simulator(self) -> dict[str, Any]:
        """Run Monte Carlo season simulations for in-season sports.

        Non-blocking: failures produce a warning but never fail the pipeline.
        Runs with 1000 simulations for speed (daily cadence makes full 10K unnecessary).
        """
        py = sys.executable
        sim_script = SCRIPT_DIR / "season_simulator.py"
        if not sim_script.exists():
            logger.debug("  [simulator] season_simulator.py not found — skipping")
            return {"status": "skipped", "reason": "no_script"}

        # Only simulate sports that are in season, have models, and are supported
        from features.registry import EXTRACTORS
        sports = self._sports_list(EXTRACTORS.keys())
        models_root = PROJECT_ROOT / "ml" / "models"

        # Get the list of sports the simulator actually supports
        # Must match season_simulator.py's VALID set exactly
        # esports (csgo/dota2/lol/valorant), golf/lpga/indycar/ligamx/europa not supported
        _SIMULATABLE = {
            "nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "wnba", "ncaaw",
            "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls",
            "nwsl", "ufc", "atp", "wta", "f1",
        }
        sim_sports = [
            s for s in sports
            if s in _SIMULATABLE and (models_root / s / "joint_models.pkl").exists()
        ]
        if not sim_sports:
            return {"status": "skipped", "reason": "no_sports"}

        sport_arg = ",".join(sim_sports)
        try:
            proc = _run_subprocess(
                [py, str(sim_script), "--sport", sport_arg, "--simulations", "1000"],
                cwd=str(PROJECT_ROOT), timeout=600,
            )
            if proc.returncode == 0:
                logger.info("  [simulator] completed for %d sports", len(sim_sports))
                return {"status": "ok", "sports": sim_sports, "simulations": 1000}
            else:
                err = proc.stderr.strip()[-300:] if proc.stderr else "unknown"
                logger.warning("  [simulator] failed: %s", err)
                return {"status": "error", "error": err}
        except subprocess.TimeoutExpired:
            return {"status": "error", "error": "timeout (600s)"}
        except Exception as e:
            logger.warning("  [simulator] error: %s", e)
            return {"status": "error", "error": str(e)}

    # ── Step 7b: Daily report ─────────────────────────────

    def step_report(self) -> dict[str, Any]:
        """Generate a summary report for the day."""
        reports_dir = DATA_DIR / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        # Gather game-day counts per sport for today
        game_day_info = {}
        try:
            from features.registry import EXTRACTORS
            sports = self._sports_list(EXTRACTORS.keys())
            target_str = [self.target_date.isoformat()]
            for sport in sports:
                if _sport_has_games_on_dates(sport, target_str):
                    norm_dir = DATA_DIR / "normalized" / sport
                    parquet_files = sorted(norm_dir.glob("games_*.parquet"))
                    if parquet_files:
                        try:
                            import pandas as pd
                            df = pd.read_parquet(parquet_files[-1], columns=["date"])
                            dates = pd.to_datetime(df["date"], errors="coerce").dt.date
                            count = int((dates == self.target_date).sum())
                            if count > 0:
                                game_day_info[sport] = count
                        except Exception:
                            game_day_info[sport] = -1  # error reading
        except Exception:
            pass

        # Gather accuracy trend (last 7 days)
        accuracy_trend = []
        try:
            for i in range(1, 8):
                d = (self.target_date - timedelta(days=i)).isoformat()
                acc_file = reports_dir / f"accuracy_{d}.json"
                if acc_file.exists():
                    with open(acc_file) as f:
                        acc_data = json.load(f)
                    ev = acc_data.get("evaluated", 0)
                    co = acc_data.get("correct", 0)
                    if ev > 0:
                        accuracy_trend.append({
                            "date": d, "evaluated": ev, "correct": co,
                            "accuracy": round(co / ev, 4),
                        })
        except Exception:
            pass

        report = {
            "date": self.target_date.isoformat(),
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "dry_run": self.dry_run,
            "parallel": self.parallel,
            "max_workers": self.max_workers,
            "sport_filter": self.sport_filter,
            "games_today": game_day_info,
            "total_games_today": sum(v for v in game_day_info.values() if v > 0),
            "accuracy_trend": accuracy_trend,
            "steps": [
                {
                    "name": s.name,
                    "status": s.status,
                    "duration_s": s.duration_s,
                    "error": s.error,
                    "details": s.details,
                }
                for s in self.steps
            ],
            "timing": self.step_timings,
            "summary": {
                "total_steps": len(self.steps),
                "ok": sum(1 for s in self.steps if s.status == "ok"),
                "errors": sum(1 for s in self.steps if s.status == "error"),
                "skipped": sum(1 for s in self.steps if s.status == "skipped"),
                "total_duration_s": round(sum(s.duration_s for s in self.steps), 2),
            },
        }

        if game_day_info:
            logger.info("  [report] Games today: %s", ", ".join(f"{s}:{n}" for s, n in sorted(game_day_info.items())))

        report_path = reports_dir / f"daily_{self.target_date.isoformat()}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("Daily report saved to %s", report_path)
        return report

    # ── Full pipeline ────────────────────────────────────

    def _print_timing_summary(self, total_time: float) -> None:
        """Print a compact one-line timing breakdown."""
        parts = []
        # Friendly display names
        aliases = {
            "import": "Import",
            "normalize": "Normalize",
            "accuracy_analysis": "Accuracy",
            "feature_extraction": "Features",
            "consolidate_features": "Consolidate",
            "training": "Train",
            "predictions": "Predict",
            "diagnostics": "Diagnostics",
            "player_props": "Props",
            "backtest": "Backtest",
            "season_simulator": "Simulator",
        }
        for step_name, duration in self.step_timings.items():
            display = aliases.get(step_name, step_name)
            parts.append(f"{display}: {duration:.0f}s")
        parts.append(f"Total: {total_time:.0f}s")
        logger.info("  %s", ", ".join(parts))

    def run(
        self,
        skip_import: bool = False,
        skip_train: bool = False,
        skip_backtest: bool = False,
        skip_simulate: bool = False,
        only_report: bool = False,
    ) -> dict[str, Any]:
        """Execute the full daily pipeline."""
        logger.info("═" * 60)
        logger.info("  V5.0 Daily Pipeline — %s", self.target_date.isoformat())
        logger.info("  Mode: %s  |  Workers: %d  |  Recent days: %d", "parallel" if self.parallel else "sequential", self.max_workers, self.recent_days)
        logger.info("  Dry run: %s", self.dry_run)
        if self.sport_filter:
            logger.info("  Sport filter: %s", self.sport_filter)
        if self.smart_seasons:
            logger.info("  Smart season detection: enabled")
        logger.info("═" * 60)

        t0 = time.monotonic()

        if only_report:
            self._run_step("accuracy_analysis", self.step_accuracy)
            report = self.step_report()
            return report

        # Step 1: Import
        if skip_import:
            logger.info("Skipping import (--skip-import)")
            self.steps.append(StepResult(name="import", status="skipped"))
        else:
            self._run_step("import", self.step_import)

        # Step 2: Normalize
        self._run_step("normalize", self.step_normalize)

        # Steps 3+4: Accuracy + Features in parallel (both read normalized data, no conflicts)
        if self.parallel:
            accuracy_result = [None]
            features_result = [None]

            def _accuracy_thread():
                accuracy_result[0] = self._run_step("accuracy_analysis", self.step_accuracy)

            def _features_thread():
                features_result[0] = self._run_step("feature_extraction", self.step_features)

            t_acc = threading.Thread(target=_accuracy_thread, name="accuracy")
            t_feat = threading.Thread(target=_features_thread, name="features")
            t_acc.start()
            t_feat.start()
            t_acc.join()
            t_feat.join()
        else:
            # Sequential fallback
            self._run_step("accuracy_analysis", self.step_accuracy)
            self._run_step("feature_extraction", self.step_features)

        # Step 4b: Pre-extract features for upcoming unplayed games
        # (runs before consolidation so _all.parquet includes them)
        self._run_step("pregame_features", self.step_pregame_features)

        # Step 4c: Consolidate feature stores  
        self._run_step("consolidate_features", self.step_consolidate_features)

        # Step 5: Train (game models + player props + golf — all parallel)
        if skip_train:
            logger.info("Skipping training (--skip-train)")
            self.steps.append(StepResult(name="training", status="skipped"))
        else:
            self._run_step("training", self.step_train)

        # Step 6+7: Predict + Diagnostics + Player Props in parallel
        # - diagnostics reads models/features only (no conflicts)
        # - player props uses its own player_props.pkl model and writes to
        #   its own parquet — fully independent of game predictions
        if self.parallel:

            def _predict_thread():
                self._run_step("predictions", self.step_predict)

            def _diagnostics_thread():
                self._run_step("diagnostics", self.step_diagnostics)

            def _props_thread():
                self._run_step("player_props", self.step_predict_props)

            t_pred = threading.Thread(target=_predict_thread, name="predict")
            t_diag = threading.Thread(target=_diagnostics_thread, name="diagnostics")
            t_props = threading.Thread(target=_props_thread, name="props")
            t_pred.start()
            t_diag.start()
            t_props.start()
            t_pred.join()
            t_diag.join()
            t_props.join()
        else:
            self._run_step("predictions", self.step_predict)
            self._run_step("diagnostics", self.step_diagnostics)
            self._run_step("player_props", self.step_predict_props)

        # Step 8+9: Backtest + Season Simulator in parallel (both read-only)
        if self.parallel and not skip_backtest and not skip_simulate:
            def _backtest_thread():
                self._run_step("backtest", self.step_backtest)
            def _simulate_thread():
                self._run_step("season_simulator", self.step_season_simulator)
            t_bt = threading.Thread(target=_backtest_thread, name="backtest")
            t_sim = threading.Thread(target=_simulate_thread, name="simulator")
            t_bt.start()
            t_sim.start()
            t_bt.join()
            t_sim.join()
        else:
            if not skip_backtest:
                self._run_step("backtest", self.step_backtest)
            else:
                logger.info("Skipping backtest (--no-backtest)")
                self.steps.append(StepResult(name="backtest", status="skipped"))
            if not skip_simulate:
                self._run_step("season_simulator", self.step_season_simulator)
            else:
                logger.info("Skipping simulator (--no-simulate)")
                self.steps.append(StepResult(name="season_simulator", status="skipped"))

        # Step 7b: Report
        report = self.step_report()

        total_time = round(time.monotonic() - t0, 2)
        logger.info("═" * 60)
        logger.info("  Pipeline complete in %.1fs", total_time)
        ok = sum(1 for s in self.steps if s.status == "ok")
        errs = sum(1 for s in self.steps if s.status == "error")
        logger.info("  Results: %d ok, %d errors, %d skipped", ok, errs, len(self.steps) - ok - errs)
        self._print_timing_summary(total_time)
        logger.info("═" * 60)

        return report


# ── Utilities ────────────────────────────────────────────


def _get_enabled_providers() -> list[str]:
    """Discover enabled providers by scanning the providers directory.

    Uses direct directory listing instead of launching a subprocess, saving
    ~5 seconds of startup overhead per pipeline run.
    """
    providers_dir = IMPORTERS_DIR / "src" / "providers"
    if providers_dir.is_dir():
        return sorted(
            d.name for d in providers_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        )
    return ["espn"]


def _training_seasons(current_year: int) -> list[int]:
    """Return all available seasons from 2020 to current year."""
    return list(range(2020, current_year + 1))


# ── CLI ──────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="daily_pipeline",
        description="V5.0 Daily Pipeline — import, normalize, train, predict, report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python3 scripts/daily_pipeline.py                       # Full pipeline (parallel, today)
  python3 scripts/daily_pipeline.py --date 2025-03-15     # Specific date
  python3 scripts/daily_pipeline.py --sport nba           # Only NBA
  python3 scripts/daily_pipeline.py --sport all            # All sports (ignore season detection)
  python3 scripts/daily_pipeline.py --sequential           # Debug: run everything sequentially
  python3 scripts/daily_pipeline.py --max-workers 12       # More parallelism
  python3 scripts/daily_pipeline.py --recent-days 7        # Import last 7 days instead of 3
  python3 scripts/daily_pipeline.py --skip-import          # Skip data import
  python3 scripts/daily_pipeline.py --skip-train           # Skip model training
  python3 scripts/daily_pipeline.py --train-max-age 12     # Retrain only if model >12h old (0=always)
  python3 scripts/daily_pipeline.py --no-backtest          # Skip daily backtest
  python3 scripts/daily_pipeline.py --no-simulate          # Skip season simulator
  python3 scripts/daily_pipeline.py --only-report          # Only accuracy + report
  python3 scripts/daily_pipeline.py --dry-run              # Preview without executing
  python3 scripts/daily_pipeline.py --no-smart-seasons     # Process all sports regardless of date

Cron:
  0 6 * * * cd /home/derek/Documents/stock/v5.0 && python3 scripts/daily_pipeline.py
""",
    )
    parser.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=date.today(),
        help="Target date (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--sport",
        type=str,
        default=None,
        help="Run pipeline for a specific sport only (e.g. nba, nhl, mlb, or 'all')",
    )

    # Parallelism flags
    parallel_group = parser.add_mutually_exclusive_group()
    parallel_group.add_argument(
        "--parallel",
        action="store_true",
        default=True,
        help="Run parallelizable steps concurrently (default)",
    )
    parallel_group.add_argument(
        "--sequential",
        action="store_true",
        help="Run all steps sequentially (useful for debugging)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        metavar="N",
        help="Max parallel workers for concurrent steps (default: 8)",
    )
    parser.add_argument(
        "--import-timeout",
        type=int,
        default=600,
        metavar="SEC",
        help="Timeout per import provider in seconds (default: 600)",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=3,
        metavar="N",
        help="Only import data from the last N days (default: 3 = yesterday/today/tomorrow)",
    )

    # Season detection
    parser.add_argument(
        "--no-smart-seasons",
        action="store_true",
        help="Disable smart season detection — process all sports regardless of date",
    )

    # Existing flags
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Skip the data import step",
    )
    parser.add_argument(
        "--force-normalize",
        action="store_true",
        help="Force re-normalization of all sports even if parquets are up to date",
    )
    parser.add_argument(
        "--skip-train",
        action="store_true",
        help="Skip the model training step",
    )
    parser.add_argument(
        "--train-max-age",
        type=int,
        default=24,
        metavar="HOURS",
        help="Skip retraining sports whose models are younger than HOURS (default: 24, 0=always retrain)",
    )
    parser.add_argument(
        "--only-report",
        action="store_true",
        help="Only run accuracy analysis and generate report",
    )
    parser.add_argument(
        "--no-backtest",
        action="store_true",
        help="Skip the daily backtest step",
    )
    parser.add_argument(
        "--no-simulate",
        action="store_true",
        help="Skip the season simulator step",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview pipeline steps without executing them",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(verbose=args.verbose)

    use_parallel = not args.sequential

    pipeline = Pipeline(
        target_date=args.date,
        dry_run=args.dry_run,
        sport_filter=args.sport,
        parallel=use_parallel,
        max_workers=args.max_workers,
        import_timeout=args.import_timeout,
        smart_seasons=not args.no_smart_seasons,
        recent_days=args.recent_days,
        train_max_age_hours=args.train_max_age,
        force_normalize=args.force_normalize,
    )
    report = pipeline.run(
        skip_import=args.skip_import,
        skip_train=args.skip_train,
        skip_backtest=args.no_backtest,
        skip_simulate=args.no_simulate,
        only_report=args.only_report,
    )

    # Only treat failures in core steps as pipeline-level errors.
    # Import errors (dead providers) and diagnostics failures are expected and non-critical.
    _CORE_STEPS = {"normalize", "feature_extraction", "predictions", "consolidate_features"}
    core_errors = sum(
        1 for s in pipeline.steps
        if s.status == "error" and s.name in _CORE_STEPS
    )
    if core_errors > 0:
        logger.error("%d core step(s) failed — exiting 1", core_errors)
    return 1 if core_errors > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
