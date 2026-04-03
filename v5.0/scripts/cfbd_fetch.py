#!/usr/bin/env python3
"""
cfbd_fetch.py — College Football Data API fetcher

Uses the official cfbd-python client (which requires pydantic 1.x) via an
isolated virtual environment so the main FastAPI environment is not affected.

Usage:
    python3 scripts/cfbd_fetch.py [--seasons 2020-2026] [--output data/raw/cfbd]

The script creates (once) /tmp/cfbd_venv with cfbd + pydantic 1.x, then
runs the fetch in a subprocess inside that venv.

Endpoints collected:
  - games (schedule + scores)
  - teams
  - drives (per-game drive summaries)
  - plays (individual play-by-play)
  - stats/season (team season stats)
  - stats/game (player game stats)
  - recruiting/players
  - recruiting/teams (class rankings)
  - player/usage (snap counts)
  - ratings/sp (SP+ ratings by team/season)
  - ratings/elo (Elo ratings)
  - weather (game weather snapshots)
  - betting/lines (historical spread / total / ML per game)
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import venv
from pathlib import Path

VENV_DIR = Path("/tmp/cfbd_venv")
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_ROOT = PROJECT_ROOT / "data" / "raw" / "cfbd"

CFBD_PACKAGES = [
    "cfbd==1.3.1",          # official Python client
    "pydantic>=1.9,<2",     # cfbd requires pydantic 1.x
    "python-dateutil>=2.9",
]

CFBD_WORKER = """
import cfbd
import json, os, sys, pathlib, time, traceback

API_KEY = os.environ.get("CFBD_API_KEY", "")
output_root = pathlib.Path(sys.argv[1])
seasons = list(range(int(sys.argv[2]), int(sys.argv[3]) + 1))

def cfg():
    c = cfbd.Configuration()
    c.api_key["Authorization"] = API_KEY
    c.api_key_prefix["Authorization"] = "Bearer"
    return cfbd.ApiClient(c)

def save(path: pathlib.Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, default=str)

def to_dict(obj):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return obj

errors = []
with cfg() as client:
    games_api = cfbd.GamesApi(client)
    teams_api = cfbd.TeamsApi(client)
    stats_api = cfbd.StatsApi(client)
    recruit_api = cfbd.RecruitingApi(client)
    ratings_api = cfbd.RatingsApi(client)
    betting_api = cfbd.BettingApi(client)
    players_api = cfbd.PlayersApi(client)

    for season in seasons:
        print(f"  CFBD season {season}...", flush=True)
        try:
            # Games (all, includes scores after week completes)
            games = games_api.get_games(year=season)
            save(output_root / str(season) / "games.json", [to_dict(g) for g in games])
            time.sleep(0.5)

            # Team season stats
            ts = stats_api.get_team_season_stats(year=season)
            save(output_root / str(season) / "team_season_stats.json", [to_dict(s) for s in ts])
            time.sleep(0.5)

            # SP+ ratings
            try:
                sp = ratings_api.get_sp_ratings(year=season)
                save(output_root / str(season) / "sp_ratings.json", [to_dict(r) for r in sp])
                time.sleep(0.4)
            except Exception as e:
                errors.append(f"{season}/sp_ratings: {e}")

            # Elo ratings
            try:
                elo = ratings_api.get_elo_ratings(year=season)
                save(output_root / str(season) / "elo_ratings.json", [to_dict(r) for r in elo])
                time.sleep(0.4)
            except Exception as e:
                errors.append(f"{season}/elo_ratings: {e}")

            # Betting lines
            try:
                lines = betting_api.get_lines(year=season)
                save(output_root / str(season) / "betting_lines.json", [to_dict(l) for l in lines])
                time.sleep(0.5)
            except Exception as e:
                errors.append(f"{season}/betting_lines: {e}")

            # Recruiting class (top 100 players)
            try:
                recruits = recruit_api.get_recruiting_players(year=season)
                save(output_root / str(season) / "recruiting_players.json", [to_dict(r) for r in recruits])
                time.sleep(0.5)
                recruit_teams = recruit_api.get_recruiting_teams(year=season)
                save(output_root / str(season) / "recruiting_teams.json", [to_dict(r) for r in recruit_teams])
                time.sleep(0.4)
            except Exception as e:
                errors.append(f"{season}/recruiting: {e}")

            # Player usage (snap counts)
            try:
                usage = players_api.get_player_usage(year=season)
                save(output_root / str(season) / "player_usage.json", [to_dict(u) for u in usage])
                time.sleep(0.5)
            except Exception as e:
                errors.append(f"{season}/player_usage: {e}")

            # Drives (by week 1-15)
            week_drives = []
            for week in range(1, 16):
                try:
                    drives = games_api.get_drives(year=season, week=week)
                    week_drives.extend([to_dict(d) for d in drives])
                    time.sleep(0.3)
                except Exception:
                    break
            if week_drives:
                save(output_root / str(season) / "drives.json", week_drives)

        except Exception as e:
            errors.append(f"{season}: {traceback.format_exc()}")
            print(f"  ERROR season {season}: {e}", flush=True)

    # Teams (once, not per-season)
    try:
        teams = teams_api.get_teams()
        save(output_root / "teams.json", [to_dict(t) for t in teams])
    except Exception as e:
        errors.append(f"teams: {e}")

print("DONE. Errors:", json.dumps(errors), flush=True)
"""


def ensure_venv() -> Path:
    """Create the isolated venv if it doesn't exist or cfbd isn't installed."""
    python = VENV_DIR / "bin" / "python"
    if python.exists():
        try:
            subprocess.run([str(python), "-c", "import cfbd"], check=True, capture_output=True)
            return python
        except subprocess.CalledProcessError:
            pass  # reinstall

    print(f"Creating isolated venv at {VENV_DIR}...", flush=True)
    venv.create(str(VENV_DIR), with_pip=True, clear=True)

    pip = VENV_DIR / "bin" / "pip"
    subprocess.run(
        [str(pip), "install", "--quiet", "--upgrade", "pip"],
        check=True,
    )
    subprocess.run(
        [str(pip), "install", "--quiet"] + CFBD_PACKAGES,
        check=True,
    )
    return python


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch CFBD data via isolated venv")
    parser.add_argument("--seasons", default="2020-2026", help="Season range e.g. 2020-2026")
    parser.add_argument("--output", default=str(OUTPUT_ROOT), help="Output directory")
    parser.add_argument("--api-key", default=os.environ.get("CFBD_API_KEY", ""),
                        help="CFBD API key (or set CFBD_API_KEY env var)")
    args = parser.parse_args()

    if not args.api_key:
        print(
            "WARNING: No CFBD API key provided. Set CFBD_API_KEY env var or use --api-key.\n"
            "Get a free key at: https://collegefootballdata.com/key",
            file=sys.stderr,
        )

    start_season, end_season = args.seasons.split("-", 1) if "-" in args.seasons else (args.seasons, args.seasons)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    python = ensure_venv()

    # Write worker script to a temp file
    worker_path = Path("/tmp/cfbd_worker.py")
    worker_path.write_text(CFBD_WORKER)

    print(f"Fetching CFBD seasons {start_season}–{end_season} → {output}", flush=True)

    env = os.environ.copy()
    if args.api_key:
        env["CFBD_API_KEY"] = args.api_key

    result = subprocess.run(
        [str(python), str(worker_path), str(output), start_season, end_season],
        env=env,
        capture_output=False,
    )

    if result.returncode != 0:
        print(f"Worker exited with code {result.returncode}", file=sys.stderr)
        return result.returncode

    print("CFBD fetch complete.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
