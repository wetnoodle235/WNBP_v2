#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# V5.0 — Game Schedule Checker
# ──────────────────────────────────────────────────────────
#
# Utility to check which sports have games today/tonight.
# Used by:
#   - Odds closing scheduler (to know when to collect closing odds)
#   - Live importer (to know when to start polling)
#   - collect_odds.sh wrapper
#
# Usage:
#   python3 scripts/game_schedule.py                  # all sports
#   python3 scripts/game_schedule.py --sports nba,nhl # specific
#   python3 scripts/game_schedule.py --json           # JSON output
#
# As a library:
#   from game_schedule import get_todays_games, sports_with_live_games
# ──────────────────────────────────────────────────────────
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
BACKEND_DIR = PROJECT_ROOT / "backend"

sys.path.insert(0, str(BACKEND_DIR))

logger = logging.getLogger("game_schedule")

EST = timezone(timedelta(hours=-5))

# ESPN API sport group/slug mappings
ESPN_SPORT_MAP: dict[str, tuple[str, str]] = {
    "nba": ("basketball", "nba"),
    "wnba": ("basketball", "wnba"),
    "ncaab": ("basketball", "mens-college-basketball"),
    "ncaaf": ("football", "college-football"),
    "nfl": ("football", "nfl"),
    "mlb": ("baseball", "mlb"),
    "nhl": ("hockey", "nhl"),
    "epl": ("soccer", "eng.1"),
    "laliga": ("soccer", "esp.1"),
    "bundesliga": ("soccer", "ger.1"),
    "seriea": ("soccer", "ita.1"),
    "ligue1": ("soccer", "fra.1"),
    "mls": ("soccer", "usa.1"),
}

# Default sports to check
DEFAULT_SPORTS = list(ESPN_SPORT_MAP.keys())


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


def _load_games_from_parquet(sport: str, today_str: str) -> list[dict[str, Any]]:
    """Load today's games from normalized parquet files."""
    season = str(datetime.now(EST).year)
    games_parquet = DATA_DIR / "normalized" / sport / f"games_{season}.parquet"
    if not games_parquet.exists():
        return []

    try:
        import pyarrow.parquet as pq

        table = pq.read_table(games_parquet)
        df_dicts = table.to_pydict()
        games: list[dict[str, Any]] = []
        n = len(df_dicts.get("game_id", []))
        for i in range(n):
            row = {k: v[i] for k, v in df_dicts.items()}
            game_date = str(row.get("date", row.get("game_date", "")))[:10]
            if game_date == today_str:
                start_time_str = str(
                    row.get("start_time", row.get("datetime", ""))
                )
                games.append({
                    "game_id": str(row.get("game_id", "")),
                    "home_team": str(row.get("home_team", "")),
                    "away_team": str(row.get("away_team", "")),
                    "start_time": start_time_str,
                    "status": str(row.get("status", "scheduled")),
                })
        return games
    except Exception as e:
        logger.debug("Could not read parquet for %s: %s", sport, e)
        return []


def _load_games_from_raw(sport: str, today_str: str) -> list[dict[str, Any]]:
    """Fallback: load games from raw ESPN JSON files."""
    season = str(datetime.now(EST).year)
    espn_dir = DATA_DIR / "raw" / "espn" / sport / season / "games"
    if not espn_dir.is_dir():
        return []

    games: list[dict[str, Any]] = []
    for f in sorted(espn_dir.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            events = data if isinstance(data, list) else data.get("events", [data])
            for ev in events:
                game_date = str(ev.get("date", ""))[:10]
                if game_date == today_str:
                    games.append({
                        "game_id": str(ev.get("id", ev.get("game_id", f.stem))),
                        "home_team": str(ev.get("home_team", "")),
                        "away_team": str(ev.get("away_team", "")),
                        "start_time": ev.get("date", ev.get("start_time", "")),
                        "status": str(ev.get("status", "scheduled")),
                    })
        except (json.JSONDecodeError, KeyError):
            continue
    return games


def get_todays_games(
    sports: list[str] | None = None,
    target_date: date | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Check schedule for today's games across all specified sports.

    Returns:
        {sport: [{game_id, home_team, away_team, start_time, status}, ...]}
    """
    sports = sports or DEFAULT_SPORTS
    target = target_date or date.today()
    today_str = target.isoformat()
    result: dict[str, list[dict[str, Any]]] = {}

    for sport in sports:
        games = _load_games_from_parquet(sport, today_str)
        if not games:
            games = _load_games_from_raw(sport, today_str)
        if games:
            result[sport] = games

    return result


def sports_with_live_games(
    sports: list[str] | None = None,
) -> list[str]:
    """Return list of sports that have games happening now or soon (within 2 hours)."""
    now = datetime.now(timezone.utc)
    schedule = get_todays_games(sports)
    live_sports: list[str] = []

    for sport, games in schedule.items():
        for game in games:
            start = _parse_game_time(game.get("start_time", ""))
            if start is None:
                continue
            delta = (start - now).total_seconds() / 3600
            status = game.get("status", "")
            # Game is live, or starts within 2 hours, or started < 5 hours ago
            if status == "in_progress" or -5 <= delta <= 2:
                live_sports.append(sport)
                break

    return live_sports


def upcoming_game_times(
    sports: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return flat list of all today's games sorted by start time."""
    schedule = get_todays_games(sports)
    all_games: list[dict[str, Any]] = []

    for sport, games in schedule.items():
        for game in games:
            game_copy = {**game, "sport": sport}
            all_games.append(game_copy)

    all_games.sort(
        key=lambda g: g.get("start_time", "9999")
    )
    return all_games


def games_starting_within(
    minutes: int,
    sports: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return games starting within the next N minutes."""
    now = datetime.now(timezone.utc)
    upcoming = upcoming_game_times(sports)
    result: list[dict[str, Any]] = []

    for game in upcoming:
        start = _parse_game_time(game.get("start_time", ""))
        if start is None:
            continue
        delta_min = (start - now).total_seconds() / 60
        if 0 <= delta_min <= minutes:
            game["minutes_until_start"] = round(delta_min, 1)
            result.append(game)

    return result


# ── CLI ──────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check today's game schedule across sports",
    )
    parser.add_argument(
        "--sports",
        type=str,
        default=None,
        help="Comma-separated sports (default: all)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output as JSON",
    )
    parser.add_argument(
        "--live-only",
        action="store_true",
        help="Only show sports with live/imminent games",
    )
    parser.add_argument(
        "--closing-window",
        type=int,
        default=None,
        metavar="MINUTES",
        help="Show games starting within N minutes (for closing odds)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sports = args.sports.split(",") if args.sports else None

    if args.closing_window is not None:
        games = games_starting_within(args.closing_window, sports)
        if args.json_output:
            print(json.dumps(games, indent=2, default=str))
        else:
            if not games:
                print(f"No games starting within {args.closing_window} minutes.")
            else:
                print(f"Games starting within {args.closing_window} minutes:")
                for g in games:
                    print(
                        f"  {g['sport']:8s} {g['away_team']:25s} @ "
                        f"{g['home_team']:25s}  "
                        f"({g['minutes_until_start']:.0f} min)"
                    )
        return

    if args.live_only:
        live = sports_with_live_games(sports)
        if args.json_output:
            print(json.dumps(live))
        else:
            if live:
                print("Sports with live/imminent games:", ", ".join(live))
            else:
                print("No live games right now.")
        return

    schedule = get_todays_games(sports)
    if args.json_output:
        print(json.dumps(schedule, indent=2, default=str))
        return

    if not schedule:
        print("No games scheduled for today.")
        return

    total = 0
    for sport, games in sorted(schedule.items()):
        total += len(games)
        print(f"\n{sport.upper()} — {len(games)} game(s):")
        for g in games:
            start = _parse_game_time(g.get("start_time", ""))
            time_str = start.astimezone(EST).strftime("%I:%M %p ET") if start else "TBD"
            print(
                f"  {g['away_team']:25s} @ {g['home_team']:25s}  {time_str}"
            )

    print(f"\nTotal: {total} game(s) across {len(schedule)} sport(s)")


if __name__ == "__main__":
    main()
