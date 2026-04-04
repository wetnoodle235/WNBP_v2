#!/usr/bin/env python3
"""Fill remaining entity gaps for the normalized_curated layer (second pass).

Entities filled:
  Soccer (epl, laliga, bundesliga, seriea, ligue1, mls):
      player_props   – from oddsapi props JSON
  NCAAW:
      injuries       – from ESPN snapshots
      rankings       – from ESPN reference
  LoL:
      tournament_roster – from PandaScore tournaments + expected_roster

Usage:
    cd v5.0/backend && python3 ../scripts/fill_wave5_gaps_v2.py
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_BASE = PROJECT_ROOT / "data" / "raw"
CURATED_BASE = PROJECT_ROOT / "data" / "normalized_curated"

SOCCER_LEAGUES = ["epl", "laliga", "bundesliga", "seriea", "ligue1", "mls"]
SEARCH_YEARS = [2026, 2025, 2024]

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

PLAYER_PROPS_SCHEMA = pa.schema([
    ("prop_id", pa.string()),
    ("game_id", pa.string()),
    ("player_name", pa.string()),
    ("market", pa.string()),
    ("outcome", pa.string()),
    ("line", pa.float64()),
    ("price", pa.float64()),
    ("bookmaker", pa.string()),
    ("timestamp", pa.string()),
])

INJURIES_SCHEMA = pa.schema([
    ("injury_id", pa.string()),
    ("player_id", pa.string()),
    ("player_name", pa.string()),
    ("team_id", pa.string()),
    ("team_name", pa.string()),
    ("status", pa.string()),
    ("description", pa.string()),
    ("date", pa.string()),
    ("type", pa.string()),
])

RANKINGS_SCHEMA = pa.schema([
    ("ranking_id", pa.string()),
    ("team_id", pa.string()),
    ("team_name", pa.string()),
    ("rank", pa.int32()),
    ("poll_name", pa.string()),
    ("week", pa.int32()),
    ("record", pa.string()),
    ("points", pa.int32()),
    ("previous_rank", pa.int32()),
])

TOURNAMENT_ROSTER_SCHEMA = pa.schema([
    ("tournament_id", pa.string()),
    ("tournament_name", pa.string()),
    ("team_id", pa.string()),
    ("team_name", pa.string()),
    ("player_id", pa.string()),
    ("player_name", pa.string()),
    ("role", pa.string()),
])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | list | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: skipping {path}: {exc}", file=sys.stderr)
        return None


def _safe_float(val, default=None) -> float | None:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=None) -> int | None:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def write_parquet(
    rows: list[dict],
    schema: pa.Schema,
    sport: str,
    entity: str,
    season_year: int = 2024,
) -> int:
    """Build a PyArrow table from *rows* conforming to *schema* and write it."""
    out_dir = CURATED_BASE / sport / entity / f"season={season_year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part.parquet"

    if rows:
        arrays = []
        for field in schema:
            vals = [r.get(field.name) for r in rows]
            arrays.append(pa.array(vals, type=field.type))
        table = pa.table(schema=schema, data=arrays)
    else:
        table = schema.empty_table()

    pq.write_table(table, out_path, use_dictionary=True, compression="snappy")
    rel = out_path.relative_to(PROJECT_ROOT)
    print(f"    → wrote {rel}  ({len(rows):,} rows)")
    return len(rows)


# ===================================================================
# 1. Soccer player_props
# ===================================================================

def fill_soccer_player_props(league: str) -> int:
    """Parse oddsapi props files for a single soccer league."""
    rows: list[dict] = []

    for year in SEARCH_YEARS:
        props_dir = RAW_BASE / "oddsapi" / league / str(year) / "props"
        if not props_dir.is_dir():
            continue
        for fp in sorted(props_dir.glob("*.json")):
            data = load_json(fp)
            if not data or not isinstance(data, dict):
                continue

            game_id = str(data.get("eventId", data.get("id", fp.stem)))
            commence = data.get("commenceTime", "")

            for bk in data.get("bookmakers", []):
                if not isinstance(bk, dict):
                    continue
                bk_key = bk.get("key", "")
                bk_title = bk.get("title", bk_key)

                for market in bk.get("markets", []):
                    if not isinstance(market, dict):
                        continue
                    mk_key = market.get("key", "")
                    if "player" not in mk_key:
                        continue
                    ts = market.get("lastUpdate", commence)

                    for oc in market.get("outcomes", []):
                        if not isinstance(oc, dict):
                            continue
                        prop_id = (
                            f"{game_id}_{bk_key}_{mk_key}"
                            f"_{oc.get('description', '')}_{oc.get('name', '')}"
                        )
                        rows.append({
                            "prop_id": prop_id,
                            "game_id": game_id,
                            "player_name": oc.get("description", "") or oc.get("name", ""),
                            "market": mk_key,
                            "outcome": oc.get("name", ""),
                            "line": _safe_float(oc.get("point")),
                            "price": _safe_float(oc.get("price")),
                            "bookmaker": bk_title,
                            "timestamp": ts or "",
                        })

    return write_parquet(rows, PLAYER_PROPS_SCHEMA, league, "player_props")


# ===================================================================
# 2. NCAAW injuries
# ===================================================================

def fill_ncaaw_injuries() -> int:
    """Parse ESPN injury snapshots across all available year folders."""
    rows: list[dict] = []
    seen: set[str] = set()

    for year_dir in sorted(RAW_BASE.glob("espn/ncaaw/*/snapshots/injuries")):
        for fp in year_dir.rglob("*.json"):
            data = load_json(fp)
            if not data or not isinstance(data, dict):
                continue

            # ESPN nests injuries at data["injuries"]["injuries"]
            outer = data.get("injuries", data)
            if isinstance(outer, dict):
                injury_list = outer.get("injuries", [])
            elif isinstance(outer, list):
                injury_list = outer
            else:
                continue

            if not isinstance(injury_list, list):
                continue

            for inj in injury_list:
                if not isinstance(inj, dict):
                    continue
                athlete = inj.get("athlete", {})
                if not isinstance(athlete, dict):
                    athlete = {}
                team_raw = inj.get("team", {})
                if not isinstance(team_raw, dict):
                    team_raw = {}
                inj_type = inj.get("type", {})
                type_str = (
                    inj_type.get("name", "")
                    if isinstance(inj_type, dict) else str(inj_type)
                )

                injury_id = str(inj.get("id", f"{athlete.get('id', '')}_{inj.get('date', '')}"))
                if injury_id in seen:
                    continue
                seen.add(injury_id)

                rows.append({
                    "injury_id": injury_id,
                    "player_id": str(athlete.get("id", "")),
                    "player_name": athlete.get("displayName", ""),
                    "team_id": str(team_raw.get("id", "")),
                    "team_name": team_raw.get("displayName", team_raw.get("name", "")),
                    "status": inj.get("status", ""),
                    "description": inj.get("longComment", "") or inj.get("shortComment", ""),
                    "date": inj.get("date", ""),
                    "type": type_str,
                })

    return write_parquet(rows, INJURIES_SCHEMA, "ncaaw", "injuries")


# ===================================================================
# 3. NCAAW rankings
# ===================================================================

def fill_ncaaw_rankings() -> int:
    """Parse ESPN rankings reference files for NCAAW."""
    rows: list[dict] = []
    seen: set[str] = set()

    # Gather all rankings JSON files across year folders
    candidates: list[Path] = []
    for year in SEARCH_YEARS:
        base = RAW_BASE / "espn" / "ncaaw" / str(year) / "reference"
        if base.is_dir():
            fp = base / "rankings.json"
            if fp.is_file():
                candidates.append(fp)
            sub = base / "rankings" / "rankings.json"
            if sub.is_file() and sub != fp:
                candidates.append(sub)

    for fp in candidates:
        data = load_json(fp)
        if not data or not isinstance(data, dict):
            continue

        rankings_outer = data.get("rankings", data)
        if isinstance(rankings_outer, dict):
            rankings_list = rankings_outer.get("rankings", [])
        elif isinstance(rankings_outer, list):
            rankings_list = rankings_outer
        else:
            continue

        if not isinstance(rankings_list, list):
            continue

        for poll in rankings_list:
            if not isinstance(poll, dict):
                continue
            poll_name = poll.get("shortName", "") or poll.get("name", "")

            # Determine week number from occurrence
            occurrence = poll.get("occurrence", {})
            week = _safe_int(
                occurrence.get("number") if isinstance(occurrence, dict) else None,
                0,
            )

            ranks = poll.get("ranks", [])
            if not isinstance(ranks, list):
                continue

            for entry in ranks:
                if not isinstance(entry, dict):
                    continue
                team = entry.get("team", {})
                if not isinstance(team, dict):
                    team = {}
                rank = _safe_int(entry.get("current"), 0)
                team_id = str(team.get("id", ""))
                ranking_id = f"{poll_name}_{week}_{team_id}"
                if ranking_id in seen:
                    continue
                seen.add(ranking_id)

                rows.append({
                    "ranking_id": ranking_id,
                    "team_id": team_id,
                    "team_name": (
                        team.get("nickname", "")
                        or team.get("displayName", "")
                        or team.get("name", "")
                    ),
                    "rank": rank,
                    "poll_name": poll_name,
                    "week": week,
                    "record": entry.get("recordSummary", ""),
                    "points": _safe_int(entry.get("points"), 0),
                    "previous_rank": _safe_int(entry.get("previous"), 0),
                })

    return write_parquet(rows, RANKINGS_SCHEMA, "ncaaw", "rankings")


# ===================================================================
# 4. LoL tournament_roster
# ===================================================================

def fill_lol_tournament_roster() -> int:
    """Build tournament rosters from PandaScore tournaments + expected_roster.

    Falls back to joining the teams reference file when expected_roster
    is missing on a tournament.
    """
    tourn_file = RAW_BASE / "pandascore" / "lol" / "2024" / "tournaments.json"
    teams_file = (
        RAW_BASE / "pandascore" / "lol" / "2024" / "reference" / "teams" / "teams.json"
    )

    if not tourn_file.is_file():
        print("  LoL tournaments.json not found — skipping")
        return 0

    tournaments = load_json(tourn_file)
    if not tournaments or not isinstance(tournaments, list):
        return 0

    # Build team_id → players lookup from teams reference
    teams_lookup: dict[int, dict] = {}
    if teams_file.is_file():
        teams_data = load_json(teams_file)
        if teams_data and isinstance(teams_data, list):
            for team in teams_data:
                if isinstance(team, dict) and team.get("players"):
                    teams_lookup[team["id"]] = team

    rows: list[dict] = []
    seen: set[str] = set()

    for t in tournaments:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id", ""))
        tname = t.get("name", "")

        # Prefer expected_roster (has players inline)
        expected_roster = t.get("expected_roster", [])
        if isinstance(expected_roster, list) and expected_roster:
            for entry in expected_roster:
                if not isinstance(entry, dict):
                    continue
                team_info = entry.get("team", {})
                if not isinstance(team_info, dict):
                    continue
                team_id = str(team_info.get("id", ""))
                team_name = team_info.get("name", "")

                for player in entry.get("players", []):
                    if not isinstance(player, dict):
                        continue
                    pid = str(player.get("id", ""))
                    key = f"{tid}_{team_id}_{pid}"
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append({
                        "tournament_id": tid,
                        "tournament_name": tname,
                        "team_id": team_id,
                        "team_name": team_name,
                        "player_id": pid,
                        "player_name": player.get("name", "") or player.get("slug", ""),
                        "role": player.get("role", ""),
                    })
        else:
            # Fallback: use teams[] list + teams_lookup for players
            for team_ref in t.get("teams", []):
                if not isinstance(team_ref, dict):
                    continue
                team_id_int = team_ref.get("id")
                team_id = str(team_id_int or "")
                team_name = team_ref.get("name", "")

                # Players from inline team (rare) or lookup
                players = team_ref.get("players", [])
                if not players and team_id_int in teams_lookup:
                    players = teams_lookup[team_id_int].get("players", [])
                    if not team_name:
                        team_name = teams_lookup[team_id_int].get("name", "")

                for player in (players if isinstance(players, list) else []):
                    if not isinstance(player, dict):
                        continue
                    pid = str(player.get("id", ""))
                    key = f"{tid}_{team_id}_{pid}"
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append({
                        "tournament_id": tid,
                        "tournament_name": tname,
                        "team_id": team_id,
                        "team_name": team_name,
                        "player_id": pid,
                        "player_name": player.get("name", "") or player.get("slug", ""),
                        "role": player.get("role", ""),
                    })

    return write_parquet(rows, TOURNAMENT_ROSTER_SCHEMA, "lol", "tournament_roster")


# ===================================================================
# Main
# ===================================================================

def main() -> None:
    print("=" * 60)
    print("fill_wave5_gaps_v2  —  second-pass gap fill")
    print("=" * 60)

    total = 0

    # 1. Soccer player_props
    print("\n[1/4] Soccer player_props")
    for league in SOCCER_LEAGUES:
        print(f"  {league}:")
        try:
            total += fill_soccer_player_props(league)
        except Exception:
            traceback.print_exc()

    # 2. NCAAW injuries
    print("\n[2/4] NCAAW injuries")
    try:
        total += fill_ncaaw_injuries()
    except Exception:
        traceback.print_exc()

    # 3. NCAAW rankings
    print("\n[3/4] NCAAW rankings")
    try:
        total += fill_ncaaw_rankings()
    except Exception:
        traceback.print_exc()

    # 4. LoL tournament_roster
    print("\n[4/4] LoL tournament_roster")
    try:
        total += fill_lol_tournament_roster()
    except Exception:
        traceback.print_exc()

    print(f"\nDone — {total:,} total rows written.")


if __name__ == "__main__":
    main()
