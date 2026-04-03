#!/usr/bin/env python3
"""Fill empty NBA curated entities by reading raw JSON data.

Handles: box_scores, advanced, leaders, contracts
Reads from data/raw/nbastats/ and data/raw/espn/
Writes to data/normalized_curated/nba/{entity}/season={YYYY}/

Usage:
    python scripts/fill_curated_gaps.py
    python scripts/fill_curated_gaps.py --entity box_scores
    python scripts/fill_curated_gaps.py --entity advanced --entity leaders
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import traceback
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_NBASTATS = PROJECT_ROOT / "data" / "raw" / "nbastats" / "nba"
RAW_ESPN = PROJECT_ROOT / "data" / "raw" / "espn" / "nba"
CURATED_BASE = PROJECT_ROOT / "data" / "normalized_curated" / "nba"

ENTITIES = ["box_scores", "advanced", "leaders", "contracts"]
SEASON_TYPES = ["regular-season", "playoffs"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_season_year(season_dir: str) -> int:
    """Convert '2024-25' → 2024."""
    return int(season_dir.split("-")[0])


def parse_minutes(raw: str) -> float:
    """Convert ISO 8601 duration 'PT32M15.00S' → 32.25 minutes."""
    if not raw or not isinstance(raw, str):
        return 0.0
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?", raw)
    if not m:
        return 0.0
    hours = int(m.group(1) or 0)
    mins = int(m.group(2) or 0)
    secs = float(m.group(3) or 0)
    return round(hours * 60 + mins + secs / 60, 2)


def season_type_label(st: str) -> str:
    """'regular-season' → 'regular_season', 'playoffs' → 'playoffs'."""
    return st.replace("-", "_")


def get_season_dirs() -> list[str]:
    """List season directories like ['2020-21', '2021-22', ...]."""
    if not RAW_NBASTATS.is_dir():
        return []
    return sorted(
        d for d in os.listdir(RAW_NBASTATS)
        if re.match(r"\d{4}-\d{2}", d) and (RAW_NBASTATS / d).is_dir()
    )


def load_json(path: Path) -> dict | None:
    """Load JSON with graceful error handling."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  WARNING: skipping {path}: {e}", file=sys.stderr)
        return None


def write_parquet(df: pd.DataFrame, entity: str, season_year: int) -> None:
    """Write DataFrame as zstd-compressed parquet."""
    out_dir = CURATED_BASE / entity / f"season={season_year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="zstd")
    print(f"    → wrote {out_path.relative_to(PROJECT_ROOT)} ({len(df):,} rows)")


# ---------------------------------------------------------------------------
# Entity: box_scores
# ---------------------------------------------------------------------------

def fill_box_scores() -> None:
    print("\n═══ box_scores ═══")
    for season_dir in get_season_dirs():
        season_year = parse_season_year(season_dir)
        rows: list[dict] = []

        for st in SEASON_TYPES:
            games_dir = RAW_NBASTATS / season_dir / st / "games"
            if not games_dir.is_dir():
                continue

            game_ids = sorted(
                g for g in os.listdir(games_dir)
                if (games_dir / g / "boxscore.json").is_file()
            )

            for game_id in game_ids:
                data = load_json(games_dir / game_id / "boxscore.json")
                if not data or "game" not in data:
                    continue

                game = data["game"]
                gid = game.get("gameId", game_id)

                for side, label in [("homeTeam", "home"), ("awayTeam", "away")]:
                    team = game.get(side)
                    if not team:
                        continue
                    team_id = team.get("teamId")
                    team_tri = team.get("teamTricode", "")

                    for p in team.get("players", []):
                        stats = p.get("statistics", {})
                        rows.append({
                            "game_id": gid,
                            "season": season_year,
                            "season_type": season_type_label(st),
                            "player_id": p.get("personId"),
                            "player_name": p.get("name", ""),
                            "team_id": team_id,
                            "team_tricode": team_tri,
                            "home_away": label,
                            "starter": str(p.get("starter", "0")),
                            "position": p.get("position", ""),
                            "minutes": parse_minutes(stats.get("minutes", "")),
                            "points": stats.get("points", 0),
                            "assists": stats.get("assists", 0),
                            "rebounds": stats.get("reboundsTotal", 0),
                            "steals": stats.get("steals", 0),
                            "blocks": stats.get("blocks", 0),
                            "turnovers": stats.get("turnovers", 0),
                            "field_goals_made": stats.get("fieldGoalsMade", 0),
                            "field_goals_attempted": stats.get("fieldGoalsAttempted", 0),
                            "three_pointers_made": stats.get("threePointersMade", 0),
                            "three_pointers_attempted": stats.get("threePointersAttempted", 0),
                            "free_throws_made": stats.get("freeThrowsMade", 0),
                            "free_throws_attempted": stats.get("freeThrowsAttempted", 0),
                            "plus_minus": stats.get("plusMinusPoints", 0),
                            "offensive_rebounds": stats.get("reboundsOffensive", 0),
                            "defensive_rebounds": stats.get("reboundsDefensive", 0),
                            "personal_fouls": stats.get("foulsPersonal", 0),
                            "source": "nbastats",
                        })

        if rows:
            df = pd.DataFrame(rows)
            print(f"  season={season_year}: {len(df):,} player-game rows")
            write_parquet(df, "box_scores", season_year)
        else:
            print(f"  season={season_year}: no boxscore data found")


# ---------------------------------------------------------------------------
# Entity: advanced (shot charts + player advanced stats)
# ---------------------------------------------------------------------------

def _resultsets_to_df(data: dict, resultset_name: str) -> pd.DataFrame | None:
    """Convert NBA Stats resultSets format to DataFrame."""
    for rs in data.get("resultSets", []):
        if rs.get("name") == resultset_name:
            headers = [h.lower() for h in rs.get("headers", [])]
            row_set = rs.get("rowSet", [])
            if headers and row_set:
                return pd.DataFrame(row_set, columns=headers)
    return None


def _resultset_to_df(data: dict) -> pd.DataFrame | None:
    """Convert NBA Stats resultSet (singular) format to DataFrame."""
    rs = data.get("resultSet", {})
    headers = [h.lower() for h in rs.get("headers", [])]
    row_set = rs.get("rowSet", [])
    if headers and row_set:
        return pd.DataFrame(row_set, columns=headers)
    return None


def fill_advanced() -> None:
    print("\n═══ advanced ═══")
    for season_dir in get_season_dirs():
        season_year = parse_season_year(season_dir)
        frames: list[pd.DataFrame] = []

        for st in SEASON_TYPES:
            st_label = season_type_label(st)
            base = RAW_NBASTATS / season_dir / st

            # --- shot charts ---
            sc_path = base / "shot-charts.json"
            if sc_path.is_file():
                data = load_json(sc_path)
                if data:
                    df = _resultsets_to_df(data, "Shot_Chart_Detail")
                    if df is not None and len(df) > 0:
                        df["season"] = season_year
                        df["season_type"] = st_label
                        df["stat_type"] = "shot_chart"
                        df["source"] = "nbastats"
                        frames.append(df)
                        print(f"  season={season_year} {st_label} shot_chart: {len(df):,} rows")

            # --- player advanced stats ---
            adv_path = base / "player-stats" / "advanced.json"
            if adv_path.is_file():
                data = load_json(adv_path)
                if data:
                    df = _resultsets_to_df(data, "LeagueDashPlayerStats")
                    if df is not None and len(df) > 0:
                        df["season"] = season_year
                        df["season_type"] = st_label
                        df["stat_type"] = "advanced_stats"
                        df["source"] = "nbastats"
                        frames.append(df)
                        print(f"  season={season_year} {st_label} advanced_stats: {len(df):,} rows")

        if frames:
            combined = pd.concat(frames, ignore_index=True)
            write_parquet(combined, "advanced", season_year)
        else:
            print(f"  season={season_year}: no advanced data found")


# ---------------------------------------------------------------------------
# Entity: leaders
# ---------------------------------------------------------------------------

def fill_leaders() -> None:
    print("\n═══ leaders ═══")
    for season_dir in get_season_dirs():
        season_year = parse_season_year(season_dir)
        frames: list[pd.DataFrame] = []

        for st in SEASON_TYPES:
            st_label = season_type_label(st)
            # Prefer the direct path (season_aggregates is a duplicate)
            ll_path = RAW_NBASTATS / season_dir / st / "league-leaders.json"
            if not ll_path.is_file():
                continue

            data = load_json(ll_path)
            if not data:
                continue

            df = _resultset_to_df(data)
            if df is None or len(df) == 0:
                continue

            df["season"] = season_year
            df["season_type"] = st_label
            df["source"] = "nbastats"
            frames.append(df)
            print(f"  season={season_year} {st_label}: {len(df):,} rows")

        if frames:
            combined = pd.concat(frames, ignore_index=True)
            write_parquet(combined, "leaders", season_year)
        else:
            print(f"  season={season_year}: no leaders data found")


# ---------------------------------------------------------------------------
# Entity: contracts (from ESPN athletes + nbastats reference)
# ---------------------------------------------------------------------------

def _extract_draft_text(draft: dict | None) -> str:
    """Extract draft display text from ESPN draft object."""
    if not draft or not isinstance(draft, dict):
        return ""
    return draft.get("displayText", "")


def _extract_draft_fields(draft: dict | None) -> tuple[int | None, int | None, int | None]:
    """Return (draft_year, draft_round, draft_pick)."""
    if not draft or not isinstance(draft, dict):
        return None, None, None
    return (
        draft.get("year"),
        draft.get("round"),
        draft.get("selection"),
    )


def _build_nbastats_player_lookup() -> dict[int, dict]:
    """Build player_id → {name, team_id, team, ...} from nbastats reference."""
    lookup: dict[int, dict] = {}
    for season_dir in get_season_dirs():
        ref_path = RAW_NBASTATS / season_dir / "reference" / "all_players.json"
        if not ref_path.is_file():
            continue
        data = load_json(ref_path)
        if not data:
            continue

        # all_players.json is a resultSets-style file
        for rs in data.get("resultSets", []):
            headers = rs.get("headers", [])
            if not headers:
                continue
            for row in rs.get("rowSet", []):
                rec = dict(zip(headers, row))
                pid = rec.get("PERSON_ID")
                if pid:
                    lookup[int(pid)] = rec
    return lookup


def _try_resolve_espn_team(athlete: dict, nba_lookup: dict) -> tuple[str | None, str | None]:
    """Try to find team info for an ESPN athlete from nbastats reference."""
    espn_id = athlete.get("id")
    # Can't directly map ESPN → nbastats IDs without a crosswalk,
    # but the athlete file sometimes has team info embedded
    team = athlete.get("team", {})
    if isinstance(team, dict):
        # Some athletes have inline team data
        tid = team.get("id")
        name = team.get("displayName") or team.get("name")
        if tid and name:
            return str(tid), name
    return None, None


def fill_contracts() -> None:
    print("\n═══ contracts ═══")
    # ESPN only has athlete files in certain year directories
    espn_years = sorted(
        d for d in os.listdir(RAW_ESPN) if d.isdigit()
    ) if RAW_ESPN.is_dir() else []

    for year_str in espn_years:
        athletes_dir = RAW_ESPN / year_str / "athletes"
        if not athletes_dir.is_dir():
            continue

        files = sorted(
            f for f in os.listdir(athletes_dir) if f.endswith(".json")
        )
        if not files:
            continue

        season_year = int(year_str)
        rows: list[dict] = []

        for fname in files:
            data = load_json(athletes_dir / fname)
            if not data:
                continue

            athlete = data.get("athlete", {})
            if not athlete or not isinstance(athlete, dict):
                continue

            player_id = athlete.get("id", "")
            full_name = athlete.get("fullName", "")
            status_obj = athlete.get("status", {})
            status = status_obj.get("type", "unknown") if isinstance(status_obj, dict) else "unknown"

            # Position
            pos_obj = athlete.get("position", {})
            position = pos_obj.get("abbreviation", "") if isinstance(pos_obj, dict) else ""

            # Draft
            draft_obj = athlete.get("draft")
            draft_text = _extract_draft_text(draft_obj)
            draft_year, draft_round, draft_pick = _extract_draft_fields(draft_obj)

            # Experience
            exp_obj = athlete.get("experience", {})
            experience_years = exp_obj.get("years") if isinstance(exp_obj, dict) else None

            # Team (best effort from ESPN data)
            team_id, team_name = _try_resolve_espn_team(athlete, {})

            rows.append({
                "player_id": str(player_id),
                "player_name": full_name,
                "team_id": team_id,
                "team_name": team_name,
                "season": season_year,
                "status": status,
                "position": position,
                "jersey": athlete.get("jersey", ""),
                "experience_years": experience_years,
                "draft_year": draft_year,
                "draft_round": draft_round,
                "draft_pick": draft_pick,
                "draft_info": draft_text,
                "source": "espn",
            })

        if rows:
            df = pd.DataFrame(rows)
            print(f"  season={season_year}: {len(df):,} athletes")
            write_parquet(df, "contracts", season_year)
        else:
            print(f"  season={season_year}: no athlete data found")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

FILL_FUNCTIONS = {
    "box_scores": fill_box_scores,
    "advanced": fill_advanced,
    "leaders": fill_leaders,
    "contracts": fill_contracts,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill empty NBA curated entities from raw JSON data."
    )
    parser.add_argument(
        "--sport", default="nba",
        help="Sport to process (default: nba)",
    )
    parser.add_argument(
        "--entity", action="append", dest="entities",
        choices=ENTITIES,
        help="Entity to fill (repeatable). Omit to fill all.",
    )
    args = parser.parse_args()

    if args.sport != "nba":
        print(f"Sport '{args.sport}' is not yet supported.", file=sys.stderr)
        sys.exit(1)

    targets = args.entities or ENTITIES
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Filling entities: {', '.join(targets)}")

    for entity in targets:
        try:
            FILL_FUNCTIONS[entity]()
        except Exception:
            print(f"\nERROR processing {entity}:", file=sys.stderr)
            traceback.print_exc()
            continue

    print("\n✓ Done.")


if __name__ == "__main__":
    main()
