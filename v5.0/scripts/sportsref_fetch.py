#!/usr/bin/env python3
"""
Sports Reference data fetcher — scripts/sportsref_fetch.py

Uses sportsipy (pydantic 1.x) in an isolated venv to pull Sports Reference data
without polluting the main FastAPI environment.

Creates a self-contained venv at /tmp/sportsref_venv if not present,
then fetches requested sport data and writes JSON to data/raw/sportsref/.

Usage:
    python3 scripts/sportsref_fetch.py --sport nba --season 2024
    python3 scripts/sportsref_fetch.py --sport nfl --season 2024
    python3 scripts/sportsref_fetch.py --sport mlb --season 2024
    python3 scripts/sportsref_fetch.py --sport nhl --season 2024

Supported sports: nba, nfl, mlb, nhl (basketball/baseball/football/hockey reference)
"""

import argparse
import json
import os
import subprocess
import sys
import venv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "raw" / "sportsref"
VENV_PATH = Path("/tmp/sportsref_venv")

FETCH_SCRIPT = """
import json
import sys

sport = sys.argv[1]
season = int(sys.argv[2])
out_path = sys.argv[3]

try:
    results = {}

    if sport == "nba":
        from sportsipy.nba.teams import Teams
        teams = Teams(year=season)
        results["teams"] = [t._stats for t in teams if hasattr(t, "_stats")]
        # Try roster/player stats
        try:
            from sportsipy.nba.roster import Roster
            player_stats = []
            for t in list(teams)[:3]:  # sample first 3 teams
                try:
                    r = Roster(t.abbreviation, year=season)
                    for p in r.players:
                        ps = p._stats if hasattr(p, "_stats") else {}
                        ps["team"] = t.abbreviation
                        player_stats.append(ps)
                except Exception:
                    pass
            results["players"] = player_stats
        except Exception:
            pass

    elif sport == "nfl":
        from sportsipy.nfl.teams import Teams
        teams = Teams(year=season)
        results["teams"] = [t._stats for t in teams if hasattr(t, "_stats")]

    elif sport == "mlb":
        from sportsipy.mlb.teams import Teams
        teams = Teams(year=season)
        results["teams"] = [t._stats for t in teams if hasattr(t, "_stats")]

    elif sport == "nhl":
        from sportsipy.nhl.teams import Teams
        teams = Teams(year=season)
        results["teams"] = [t._stats for t in teams if hasattr(t, "_stats")]

    elif sport == "ncaab":
        from sportsipy.ncaab.teams import Teams
        teams = Teams(year=season)
        results["teams"] = [t._stats for t in teams if hasattr(t, "_stats")]

    elif sport == "ncaaf":
        from sportsipy.ncaaf.teams import Teams
        teams = Teams(year=season)
        results["teams"] = [t._stats for t in teams if hasattr(t, "_stats")]

    with open(out_path, "w") as f:
        json.dump(results, f)
    print(f"OK:{len(results.get('teams', []))} teams")

except Exception as e:
    print(f"ERROR:{e}")
    sys.exit(1)
"""


def ensure_venv() -> Path:
    """Create isolated venv with sportsipy if not present."""
    python_bin = VENV_PATH / "bin" / "python3"
    if python_bin.exists():
        return python_bin

    print(f"[sportsref] Creating isolated venv at {VENV_PATH}...")
    venv.create(str(VENV_PATH), with_pip=True)

    subprocess.run(
        [str(python_bin), "-m", "pip", "install", "sportsipy", "--quiet"],
        check=True
    )
    print("[sportsref] Venv ready.")
    return python_bin


def fetch_sport(sport: str, season: int, out_dir: Path) -> Path:
    """Fetch data for a sport/season using isolated venv."""
    python_bin = ensure_venv()

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{sport}_{season}.json"

    # Write the inline script to a temp file
    script_path = Path("/tmp/_sportsref_fetch_inner.py")
    script_path.write_text(FETCH_SCRIPT)

    result = subprocess.run(
        [str(python_bin), str(script_path), sport, str(season), str(out_path)],
        capture_output=True, text=True, timeout=120
    )

    if result.returncode != 0:
        raise RuntimeError(f"Fetch failed: {result.stderr or result.stdout}")

    output = result.stdout.strip()
    if output.startswith("ERROR:"):
        raise RuntimeError(f"Fetch error: {output[6:]}")

    print(f"[sportsref] {sport}/{season}: {output}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Sports Reference data fetcher (isolated venv)")
    parser.add_argument("--sport", required=True, choices=["nba", "nfl", "mlb", "nhl", "ncaab", "ncaaf"])
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--seasons", type=int, nargs="*", default=None, help="Multiple seasons")
    parser.add_argument("--out-dir", type=str, default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_DIR / args.sport
    seasons = args.seasons or [args.season]

    for season in seasons:
        try:
            out_path = fetch_sport(args.sport, season, out_dir)
            print(f"[sportsref] Saved: {out_path}")
        except Exception as e:
            print(f"[sportsref] ERROR {args.sport}/{season}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
