#!/usr/bin/env python3
"""Fill empty curated entities for Soccer leagues, NCAAW, UFC, CS2, and LoL.

Parses raw JSON data and derives aggregates from existing player_stats
parquets. Only processes season/year 2024 (dev mode).

Entities filled:
  Soccer (epl, laliga, bundesliga, seriea, ligue1, mls, ucl, europa, ligamx, nwsl):
         rosters, match_events, team_stats, transfers, player_props
  NCAAW: advanced, bracket, injuries, leaders, odds, player_props, plays,
         rankings, team_stats
  UFC:   events, fights, leagues, rankings, weight_classes, player_props
  CS2:   matches, match_maps, tournaments, tournament_teams,
         player_map_stats, player_accuracy, team_round_stats, team_map_pool
  LoL:   matches, match_maps, tournaments, champions, items, runes, spells,
         team_stats, champion_stats, tournament_roster

Usage:
    python scripts/fill_wave5_gaps.py
    python scripts/fill_wave5_gaps.py --sport epl
    python scripts/fill_wave5_gaps.py --sport ufc --entity fights
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_BASE = PROJECT_ROOT / "data" / "raw"
CURATED_BASE = PROJECT_ROOT / "data" / "normalized_curated"

DEV_YEAR = 2024

SOCCER_LEAGUES = [
    "epl", "laliga", "bundesliga", "seriea", "ligue1",
    "mls", "ucl", "europa", "ligamx", "nwsl",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | list | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  WARNING: skipping {path}: {e}", file=sys.stderr)
        return None


def _coerce_value(val):
    """Coerce a value to a parquet-friendly type."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return val
    return str(val)


def write_parquet_rows(
    rows: list[dict],
    sport: str,
    entity: str,
    season_year: int | None = None,
    schema: pa.Schema | None = None,
) -> None:
    """Write rows as a zstd-compressed parquet file."""
    if season_year is not None:
        out_dir = CURATED_BASE / sport / entity / f"season={season_year}"
    else:
        out_dir = CURATED_BASE / sport / entity
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part.parquet"

    if rows:
        # Coerce all values and build column arrays
        all_keys = list(rows[0].keys())
        columns: dict[str, list] = {k: [] for k in all_keys}
        for r in rows:
            for k in all_keys:
                columns[k].append(_coerce_value(r.get(k)))
        table = pa.table(columns)
    elif schema is not None:
        table = schema.empty_table()
    else:
        print(f"    → skipped {entity}: no rows and no schema")
        return

    pq.write_table(table, out_path, compression="zstd")
    rel = out_path.relative_to(PROJECT_ROOT)
    print(f"    → wrote {rel} ({len(rows):,} rows)")


def _safe_int(val, default=None):
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val, default=None):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def read_player_stats_parquets(sport: str, season: int) -> list[dict]:
    """Read all player_stats parquet files for a sport+season into rows."""
    base = CURATED_BASE / sport / "player_stats" / f"season={season}"
    if not base.is_dir():
        # Try without season partition
        base = CURATED_BASE / sport / "player_stats"
        if not base.is_dir():
            print(f"  No player_stats for {sport} season={season}")
            return []
    rows: list[dict] = []
    for root, _dirs, files in os.walk(base):
        for fname in files:
            if not fname.endswith(".parquet"):
                continue
            fp = Path(root) / fname
            try:
                pf = pq.ParquetFile(fp)
                table = pf.read()
                new_cols = []
                for i, field in enumerate(table.schema):
                    col = table.column(i)
                    if pa.types.is_dictionary(field.type):
                        col = col.cast(field.type.value_type)
                    new_cols.append(col)
                table = pa.table(
                    {table.schema.field(i).name: new_cols[i]
                     for i in range(len(new_cols))}
                )
                batch = table.to_pydict()
                n = table.num_rows
                for i in range(n):
                    rows.append({col: batch[col][i] for col in batch})
            except Exception as e:
                print(f"  WARNING: reading {fp}: {e}", file=sys.stderr)
    print(f"  Read {len(rows):,} player_stats rows for {sport} season={season}")
    return rows


# ---------------------------------------------------------------------------
# Shared aggregate helpers
# ---------------------------------------------------------------------------

def fill_team_stats_from_player_stats(sport: str, year: int = DEV_YEAR) -> None:
    """Aggregate team-level stats from player_stats parquets."""
    ps_rows = read_player_stats_parquets(sport, year)
    if not ps_rows:
        return

    team_col = next((c for c in ["team_id", "team"] if c in ps_rows[0]), None)
    if not team_col:
        print(f"  No team column found in {sport} player_stats")
        return
    game_col = next((c for c in ["game_id", "match_id"] if c in ps_rows[0]), None)

    # Identify numeric columns
    numeric_cols: list[str] = []
    skip = {"game_id", "team_id", "player_id", "id"}
    for k, v in ps_rows[0].items():
        if k in skip:
            continue
        if isinstance(v, (int, float)) and v is not None:
            numeric_cols.append(k)
    numeric_cols = numeric_cols[:20]

    # Group by team
    teams: dict[str, list[dict]] = {}
    for r in ps_rows:
        tid = str(r.get(team_col, ""))
        teams.setdefault(tid, []).append(r)

    out_rows: list[dict] = []
    for tid, grp in teams.items():
        team_name = ""
        for c in ["team_name", "team"]:
            if c in grp[0]:
                team_name = str(grp[0][c])
                break
        games = len({str(r.get(game_col, "")) for r in grp}) if game_col else len(grp)
        row: dict = {
            "source": "derived", "id": tid, "sport": sport,
            "team_id": tid, "team_name": team_name,
            "season": str(year), "games_played": games,
            "players_used": len({str(r.get("id", "")) for r in grp}),
        }
        for col in numeric_cols:
            vals = [_safe_float(r.get(col), 0.0) for r in grp]
            row[f"total_{col}"] = round(sum(vals), 2)
            row[f"avg_{col}"] = round(sum(vals) / len(vals), 2) if vals else 0.0
        out_rows.append(row)

    if out_rows:
        write_parquet_rows(out_rows, sport, "team_stats", year)
        print(f"  {sport}/team_stats: {len(out_rows)} rows")


def fill_leaders_from_player_stats(sport: str, year: int = DEV_YEAR) -> None:
    """Generate leaders from player_stats parquets (top N per stat)."""
    ps_rows = read_player_stats_parquets(sport, year)
    if not ps_rows:
        return

    name_col = next((c for c in ["player_name", "name"] if c in ps_rows[0]), None)
    skip = {"game_id", "team_id", "player_id", "id"}
    numeric_cols: list[str] = []
    for k, v in ps_rows[0].items():
        if k in skip:
            continue
        if isinstance(v, (int, float)) and v is not None:
            numeric_cols.append(k)
    numeric_cols = numeric_cols[:15]

    out_rows: list[dict] = []
    for stat_col in numeric_cols:
        sorted_rows = sorted(
            ps_rows,
            key=lambda r: _safe_float(r.get(stat_col), 0.0),
            reverse=True,
        )
        for rank, r in enumerate(sorted_rows[:25], 1):
            out_rows.append({
                "source": "derived",
                "id": f"{stat_col}_{rank}",
                "sport": sport,
                "player_name": str(r[name_col]) if name_col else "",
                "stat_name": stat_col,
                "stat_value": _safe_float(r.get(stat_col), 0.0),
                "rank": rank,
                "season": str(year),
            })

    if out_rows:
        write_parquet_rows(out_rows, sport, "leaders", year)
        print(f"  {sport}/leaders: {len(out_rows)} rows")


def fill_advanced_from_player_stats(sport: str, year: int = DEV_YEAR) -> None:
    """Generate advanced per-player metrics from player_stats."""
    ps_rows = read_player_stats_parquets(sport, year)
    if not ps_rows:
        return

    name_col = next((c for c in ["player_name", "name"] if c in ps_rows[0]), None)
    game_col = next((c for c in ["game_id", "match_id"] if c in ps_rows[0]), None)
    player_col = next((c for c in ["player_id", "id"] if c in ps_rows[0]), None)
    if not player_col:
        return

    skip = {"game_id", "team_id", "player_id", "id"}
    numeric_cols: list[str] = []
    for k, v in ps_rows[0].items():
        if k in skip:
            continue
        if isinstance(v, (int, float)) and v is not None:
            numeric_cols.append(k)
    numeric_cols = numeric_cols[:20]

    # Group by player
    players: dict[str, list[dict]] = {}
    for r in ps_rows:
        pid = str(r.get(player_col, ""))
        players.setdefault(pid, []).append(r)

    out_rows: list[dict] = []
    for pid, grp in players.items():
        games = len({str(r.get(game_col, "")) for r in grp}) if game_col else len(grp)
        row: dict = {
            "source": "derived", "id": pid, "sport": sport,
            "player_name": str(grp[0][name_col]) if name_col else "",
            "games_played": games,
            "season": str(year),
        }
        for col in numeric_cols:
            vals = [_safe_float(r.get(col), 0.0) for r in grp]
            row[f"avg_{col}"] = round(sum(vals) / len(vals), 2) if vals else 0.0
            row[f"total_{col}"] = round(sum(vals), 2)
            row[f"max_{col}"] = max(vals) if vals else 0.0
        out_rows.append(row)

    if out_rows:
        write_parquet_rows(out_rows, sport, "advanced", year)
        print(f"  {sport}/advanced: {len(out_rows)} rows")


def fill_rankings_from_fights(sport: str, year: int = DEV_YEAR) -> None:
    """Derive rankings from fight results."""
    fights_dir = RAW_BASE / "ufcstats" / sport / str(year) / "fights"
    if not fights_dir.is_dir():
        return
    wins: Counter = Counter()
    losses: Counter = Counter()
    fighter_names: dict[str, str] = {}
    for f in fights_dir.glob("*.json"):
        data = load_json(f)
        if not data:
            continue
        fights = data.get("fights", []) if isinstance(data, dict) else []
        for fight in fights:
            fighters = fight.get("fighters", [])
            result = fight.get("result", "")
            if len(fighters) == 2 and result == "win":
                w_raw = fighters[0]
                l_raw = fighters[1]
                if isinstance(w_raw, dict):
                    wid = str(w_raw.get("id", w_raw.get("name", "")))
                    fighter_names[wid] = w_raw.get("name", "")
                else:
                    wid = str(w_raw)
                    fighter_names[wid] = str(w_raw)
                if isinstance(l_raw, dict):
                    lid = str(l_raw.get("id", l_raw.get("name", "")))
                    fighter_names[lid] = l_raw.get("name", "")
                else:
                    lid = str(l_raw)
                    fighter_names[lid] = str(l_raw)
                wins[wid] += 1
                losses[lid] += 1

    all_fighters = set(wins.keys()) | set(losses.keys())
    ranked = sorted(all_fighters, key=lambda x: (wins[x], -losses[x]), reverse=True)
    rows: list[dict] = []
    for rank, fid in enumerate(ranked[:100], 1):
        rows.append({
            "source": "derived", "id": fid, "sport": sport,
            "fighter_name": fighter_names.get(fid, ""),
            "rank": rank, "wins": wins[fid], "losses": losses[fid],
            "season": str(year),
        })
    if rows:
        write_parquet_rows(rows, sport, "rankings", year)
        print(f"  {sport}/rankings: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Soccer leagues
# ---------------------------------------------------------------------------

def fill_soccer_rosters(league: str) -> None:
    roster_dir = RAW_BASE / "espn" / league / str(DEV_YEAR) / "rosters"
    if not roster_dir.is_dir():
        return
    rows: list[dict] = []
    for f in sorted(roster_dir.glob("*.json")):
        data = load_json(f)
        if not data:
            continue
        team_id = str(data.get("teamId", ""))
        team_name = data.get("teamName", "")
        for ath in data.get("athletes", []):
            pos = ath.get("position", {})
            bp = ath.get("birthPlace")
            nationality = ath.get("citizenship", "")
            if not nationality and isinstance(bp, dict):
                nationality = bp.get("country", "")
            rows.append({
                "source": "espn",
                "id": str(ath.get("id", "")),
                "sport": league,
                "team_id": team_id,
                "team_name": team_name,
                "player_name": ath.get("fullName") or ath.get("displayName", ""),
                "position": pos.get("name", "") if isinstance(pos, dict) else str(pos),
                "jersey_number": str(ath.get("jersey", "")),
                "height": ath.get("displayHeight", ""),
                "weight": ath.get("displayWeight", ""),
                "age": _safe_int(ath.get("age")),
                "birth_date": ath.get("dateOfBirth", ""),
                "nationality": nationality,
            })
    if rows:
        write_parquet_rows(rows, league, "rosters", DEV_YEAR)
        print(f"  {league}/rosters: {len(rows)} rows")


def fill_soccer_match_events(league: str) -> None:
    events_dir = RAW_BASE / "espn" / league / str(DEV_YEAR) / "events"
    if not events_dir.is_dir():
        return
    rows: list[dict] = []
    for game_json in events_dir.rglob("game.json"):
        data = load_json(game_json)
        if not data:
            continue
        game_id = str(data.get("eventId", ""))
        summary = data.get("summary", {})
        key_events = summary.get("keyEvents", [])
        plays = summary.get("plays", [])
        all_plays = key_events or plays
        if not all_plays:
            continue
        for i, play in enumerate(all_plays):
            clock = play.get("clock", {})
            play_type = play.get("type", {})
            participants = play.get("participants", [])
            team = play.get("team", {})
            player_name = ""
            if participants and isinstance(participants, list):
                first_p = participants[0] if participants else {}
                if isinstance(first_p, dict):
                    ath = first_p.get("athlete", {})
                    if isinstance(ath, dict):
                        player_name = ath.get("displayName", "")
            rows.append({
                "source": "espn",
                "id": f"{game_id}_{i}",
                "sport": league,
                "game_id": game_id,
                "minute": clock.get("displayValue", "") if isinstance(clock, dict) else str(clock),
                "event_type": play_type.get("text", "") if isinstance(play_type, dict) else str(play_type),
                "player_name": player_name,
                "team_name": team.get("displayName", "") if isinstance(team, dict) else "",
                "detail": play.get("text", ""),
                "period": play.get("period", {}).get("number", 0) if isinstance(play.get("period"), dict) else 0,
            })
    if rows:
        write_parquet_rows(rows, league, "match_events", DEV_YEAR)
        print(f"  {league}/match_events: {len(rows)} rows")


def fill_soccer_team_stats(league: str) -> None:
    fill_team_stats_from_player_stats(league, DEV_YEAR)


def fill_soccer_transfers(league: str) -> None:
    # Check statsbomb source first
    sb_dir = RAW_BASE / "statsbomb" / league / str(DEV_YEAR)
    if sb_dir.is_dir():
        rows: list[dict] = []
        for f in sb_dir.rglob("*.json"):
            data = load_json(f)
            if not data:
                continue
            transfers = data.get("transfers", []) if isinstance(data, dict) else []
            for t in transfers:
                player = t.get("player", {}) if isinstance(t, dict) else {}
                rows.append({
                    "source": "statsbomb",
                    "id": str(t.get("id", "")),
                    "sport": league,
                    "player_id": str(player.get("id", "")),
                    "player_name": player.get("name", ""),
                    "from_team": t.get("from_team", {}).get("name", "") if isinstance(t.get("from_team"), dict) else "",
                    "to_team": t.get("to_team", {}).get("name", "") if isinstance(t.get("to_team"), dict) else "",
                    "transfer_date": t.get("date", ""),
                    "transfer_type": t.get("type", ""),
                    "fee": t.get("fee", ""),
                })
        if rows:
            write_parquet_rows(rows, league, "transfers", DEV_YEAR)
            print(f"  {league}/transfers: {len(rows)} rows")
            return

    # No direct source available — write empty scaffold
    write_parquet_rows(
        [], league, "transfers", DEV_YEAR,
        schema=pa.schema([
            ("source", pa.string()), ("id", pa.string()), ("sport", pa.string()),
            ("player_id", pa.string()), ("player_name", pa.string()),
            ("from_team", pa.string()), ("to_team", pa.string()),
            ("transfer_date", pa.string()), ("transfer_type", pa.string()),
            ("fee", pa.string()),
        ]),
    )


def fill_soccer_player_props(league: str) -> None:
    odds_dir = RAW_BASE / "odds" / "providers" / "oddsapi" / league
    if not odds_dir.is_dir():
        return
    rows: list[dict] = []
    for f in odds_dir.rglob("*.json"):
        data = load_json(f)
        if not data:
            continue
        items = data if isinstance(data, list) else data.get("items", data.get("data", []))
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            bookmakers = item.get("bookmakers", [])
            event_id = str(item.get("id", ""))
            for bk in (bookmakers if isinstance(bookmakers, list) else []):
                for market in (bk.get("markets", []) if isinstance(bk, dict) else []):
                    mk_key = market.get("key", "") if isinstance(market, dict) else ""
                    if "player" not in mk_key:
                        continue
                    for outcome in (market.get("outcomes", []) if isinstance(market, dict) else []):
                        rows.append({
                            "source": "oddsapi",
                            "id": f"{event_id}_{bk.get('key', '')}_{mk_key}_{outcome.get('name', '')}",
                            "sport": league,
                            "event_id": event_id,
                            "bookmaker": bk.get("title", ""),
                            "market": mk_key,
                            "player_name": outcome.get("description", "") or outcome.get("name", ""),
                            "selection": outcome.get("name", ""),
                            "point": _safe_float(outcome.get("point")),
                            "price": _safe_float(outcome.get("price")),
                        })
    if rows:
        write_parquet_rows(rows, league, "player_props", DEV_YEAR)
        print(f"  {league}/player_props: {len(rows)} rows")


# ---------------------------------------------------------------------------
# NCAAW
# ---------------------------------------------------------------------------

def fill_ncaaw_rankings(year: int = DEV_YEAR) -> None:
    ref_dir = RAW_BASE / "espn" / "ncaaw" / str(year) / "reference"
    snap_dir = RAW_BASE / "espn" / "ncaaw" / str(year) / "snapshots" / "rankings"
    rows: list[dict] = []

    # Try reference/rankings.json
    if ref_dir.is_dir():
        fp = ref_dir / "rankings.json"
        if fp.is_file():
            data = load_json(fp)
            if data:
                rankings_list = data.get("rankings", data.get("data", []))
                if isinstance(rankings_list, list):
                    for r in rankings_list:
                        if isinstance(r, dict) and "ranks" in r:
                            poll_name = r.get("name", "")
                            for rank_entry in r["ranks"]:
                                team = rank_entry.get("team", {})
                                rows.append({
                                    "source": "espn",
                                    "id": f"{poll_name}_{rank_entry.get('current', 0)}",
                                    "sport": "ncaaw",
                                    "team_id": str(team.get("id", "")),
                                    "team_name": team.get("nickname", "") or team.get("displayName", ""),
                                    "rank": _safe_int(rank_entry.get("current"), 0),
                                    "previous_rank": _safe_int(rank_entry.get("previous"), 0),
                                    "poll_name": poll_name,
                                    "season": str(year),
                                    "record": rank_entry.get("recordSummary", ""),
                                    "points": _safe_int(rank_entry.get("points"), 0),
                                })

    # Try snapshots/rankings/
    if not rows and snap_dir is not None and snap_dir.is_dir():
        for f in snap_dir.rglob("*.json"):
            data = load_json(f)
            if not data:
                continue
            rankings_list = data.get("rankings", data.get("data", []))
            if isinstance(rankings_list, list):
                for r in rankings_list:
                    if isinstance(r, dict) and "ranks" in r:
                        poll_name = r.get("name", "")
                        for rank_entry in r["ranks"]:
                            team = rank_entry.get("team", {})
                            rows.append({
                                "source": "espn",
                                "id": f"{poll_name}_{rank_entry.get('current', 0)}",
                                "sport": "ncaaw",
                                "team_id": str(team.get("id", "")),
                                "team_name": team.get("nickname", "") or team.get("displayName", ""),
                                "rank": _safe_int(rank_entry.get("current"), 0),
                                "previous_rank": _safe_int(rank_entry.get("previous"), 0),
                                "poll_name": poll_name,
                                "season": str(year),
                                "record": rank_entry.get("recordSummary", ""),
                                "points": _safe_int(rank_entry.get("points"), 0),
                            })

    if rows:
        write_parquet_rows(rows, "ncaaw", "rankings", year)
        print(f"  ncaaw/rankings: {len(rows)} rows")


def fill_ncaaw_injuries(year: int = DEV_YEAR) -> None:
    snap_dir = RAW_BASE / "espn" / "ncaaw" / str(year) / "snapshots" / "injuries"
    if not snap_dir.is_dir():
        return
    rows: list[dict] = []
    for f in snap_dir.rglob("*.json"):
        data = load_json(f)
        if not data:
            continue
        injuries = data if isinstance(data, list) else data.get("items", data.get("injuries", []))
        if not isinstance(injuries, list):
            continue
        for inj in injuries:
            if not isinstance(inj, dict):
                continue
            athlete = inj.get("athlete", {})
            if not isinstance(athlete, dict):
                athlete = {}
            team_raw = inj.get("team", {})
            team_id = str(team_raw.get("id", "")) if isinstance(team_raw, dict) else ""
            inj_type = inj.get("type", {})
            rows.append({
                "source": "espn",
                "id": str(inj.get("id", "")),
                "sport": "ncaaw",
                "player_id": str(athlete.get("id", "")),
                "player_name": athlete.get("displayName", ""),
                "team_id": team_id,
                "status": inj.get("status", ""),
                "injury_type": inj_type.get("name", "") if isinstance(inj_type, dict) else str(inj_type),
                "detail": inj.get("longComment", "") or inj.get("shortComment", ""),
                "date": inj.get("date", ""),
                "season": str(year),
            })
    if rows:
        write_parquet_rows(rows, "ncaaw", "injuries", year)
        print(f"  ncaaw/injuries: {len(rows)} rows")


def fill_ncaaw_plays(year: int = DEV_YEAR) -> None:
    events_dir = RAW_BASE / "espn" / "ncaaw" / str(year) / "events"
    if not events_dir.is_dir():
        return
    rows: list[dict] = []
    for game_json in events_dir.rglob("game.json"):
        data = load_json(game_json)
        if not data:
            continue
        game_id = str(data.get("eventId", ""))
        summary = data.get("summary", {})
        plays = summary.get("plays", [])
        for i, play in enumerate(plays):
            clock = play.get("clock", {})
            play_type = play.get("type", {})
            team = play.get("team", {})
            rows.append({
                "source": "espn",
                "id": f"{game_id}_{i}",
                "sport": "ncaaw",
                "game_id": game_id,
                "clock": clock.get("displayValue", "") if isinstance(clock, dict) else str(clock),
                "period": play.get("period", {}).get("number", 0) if isinstance(play.get("period"), dict) else 0,
                "play_type": play_type.get("text", "") if isinstance(play_type, dict) else "",
                "text": play.get("text", ""),
                "scoring": play.get("scoringPlay", False),
                "score_value": _safe_int(play.get("scoreValue"), 0),
                "team_id": str(team.get("id", "")) if isinstance(team, dict) else "",
                "season": str(year),
            })
    if rows:
        write_parquet_rows(rows, "ncaaw", "plays", year)
        print(f"  ncaaw/plays: {len(rows)} rows")


def fill_ncaaw_team_stats(year: int = DEV_YEAR) -> None:
    fill_team_stats_from_player_stats("ncaaw", year)


def fill_ncaaw_leaders(year: int = DEV_YEAR) -> None:
    fill_leaders_from_player_stats("ncaaw", year)


def fill_ncaaw_advanced(year: int = DEV_YEAR) -> None:
    fill_advanced_from_player_stats("ncaaw", year)


def fill_ncaaw_odds(year: int = DEV_YEAR) -> None:
    events_dir = RAW_BASE / "espn" / "ncaaw" / str(year) / "events"
    if not events_dir.is_dir():
        return
    rows: list[dict] = []
    for odds_json in events_dir.rglob("odds.json"):
        data = load_json(odds_json)
        if not data:
            continue
        items = data if isinstance(data, list) else data.get("items", [])
        game_dir = odds_json.parent
        game_id = game_dir.name
        for item in (items if isinstance(items, list) else []):
            if not isinstance(item, dict):
                continue
            provider = item.get("provider", {})
            home_odds = item.get("homeTeamOdds", {})
            away_odds = item.get("awayTeamOdds", {})
            rows.append({
                "source": "espn",
                "id": f"{game_id}_{provider.get('id', '') if isinstance(provider, dict) else ''}",
                "sport": "ncaaw",
                "game_id": game_id,
                "provider_name": provider.get("name", "") if isinstance(provider, dict) else str(provider),
                "spread": _safe_float(item.get("spread"), 0.0),
                "over_under": _safe_float(item.get("overUnder"), 0.0),
                "home_ml": _safe_int(home_odds.get("moneyLine") if isinstance(home_odds, dict) else None, 0),
                "away_ml": _safe_int(away_odds.get("moneyLine") if isinstance(away_odds, dict) else None, 0),
                "season": str(year),
            })
    if rows:
        write_parquet_rows(rows, "ncaaw", "odds", year)
        print(f"  ncaaw/odds: {len(rows)} rows")


def fill_ncaaw_bracket(year: int = DEV_YEAR) -> None:
    ref_dir = RAW_BASE / "espn" / "ncaaw" / str(year) / "reference"
    if not ref_dir.is_dir():
        return
    fp = ref_dir / "bracket.json"
    if not fp.is_file():
        return
    data = load_json(fp)
    if not data:
        return
    rows: list[dict] = []
    rounds = data.get("rounds", data.get("data", []))
    if isinstance(rounds, list):
        for rnd in rounds:
            if not isinstance(rnd, dict):
                continue
            round_name = rnd.get("name", "")
            round_num = _safe_int(rnd.get("number"), 0)
            for matchup in rnd.get("matchups", rnd.get("games", [])):
                if not isinstance(matchup, dict):
                    continue
                teams = matchup.get("teams", matchup.get("competitors", []))
                t1 = teams[0] if len(teams) > 0 else {}
                t2 = teams[1] if len(teams) > 1 else {}
                rows.append({
                    "source": "espn",
                    "id": str(matchup.get("id", "")),
                    "sport": "ncaaw",
                    "round_name": round_name,
                    "round_number": round_num,
                    "team1_id": str(t1.get("id", t1.get("teamId", ""))),
                    "team1_name": t1.get("displayName", t1.get("name", "")),
                    "team1_seed": _safe_int(t1.get("seed"), 0),
                    "team2_id": str(t2.get("id", t2.get("teamId", ""))),
                    "team2_name": t2.get("displayName", t2.get("name", "")),
                    "team2_seed": _safe_int(t2.get("seed"), 0),
                    "winner_id": str(matchup.get("winnerId", "")),
                    "season": str(year),
                })
    if rows:
        write_parquet_rows(rows, "ncaaw", "bracket", year)
        print(f"  ncaaw/bracket: {len(rows)} rows")


def fill_ncaaw_player_props(year: int = DEV_YEAR) -> None:
    # Check for player props in odds data
    events_dir = RAW_BASE / "espn" / "ncaaw" / str(year) / "events"
    odds_dir = RAW_BASE / "odds" / "providers" / "oddsapi" / "ncaaw"
    rows: list[dict] = []

    # Try oddsapi
    if odds_dir.is_dir():
        for f in odds_dir.rglob("*.json"):
            data = load_json(f)
            if not data:
                continue
            items = data if isinstance(data, list) else data.get("data", [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                for bk in item.get("bookmakers", []):
                    if not isinstance(bk, dict):
                        continue
                    for market in bk.get("markets", []):
                        mk_key = market.get("key", "") if isinstance(market, dict) else ""
                        if "player" not in mk_key:
                            continue
                        for outcome in market.get("outcomes", []):
                            rows.append({
                                "source": "oddsapi",
                                "id": f"{item.get('id', '')}_{bk.get('key', '')}_{mk_key}_{outcome.get('name', '')}",
                                "sport": "ncaaw",
                                "event_id": str(item.get("id", "")),
                                "bookmaker": bk.get("title", ""),
                                "market": mk_key,
                                "player_name": outcome.get("description", "") or outcome.get("name", ""),
                                "selection": outcome.get("name", ""),
                                "point": _safe_float(outcome.get("point")),
                                "price": _safe_float(outcome.get("price")),
                                "season": str(year),
                            })

    # Also try embedded odds in events
    if not rows and events_dir is not None and events_dir.is_dir():
        for odds_json in events_dir.rglob("odds.json"):
            data = load_json(odds_json)
            if not data:
                continue
            items = data if isinstance(data, list) else data.get("items", [])
            game_id = odds_json.parent.name
            for item in (items if isinstance(items, list) else []):
                if not isinstance(item, dict):
                    continue
                player_props = item.get("playerProps", item.get("player_props", []))
                if not isinstance(player_props, list):
                    continue
                for pp in player_props:
                    if not isinstance(pp, dict):
                        continue
                    rows.append({
                        "source": "espn",
                        "id": f"{game_id}_{pp.get('id', '')}",
                        "sport": "ncaaw",
                        "event_id": game_id,
                        "bookmaker": "",
                        "market": pp.get("market", ""),
                        "player_name": pp.get("athlete", {}).get("displayName", "") if isinstance(pp.get("athlete"), dict) else "",
                        "selection": pp.get("selection", ""),
                        "point": _safe_float(pp.get("line")),
                        "price": _safe_float(pp.get("odds")),
                        "season": str(year),
                    })

    if rows:
        write_parquet_rows(rows, "ncaaw", "player_props", year)
        print(f"  ncaaw/player_props: {len(rows)} rows")


# ---------------------------------------------------------------------------
# UFC
# ---------------------------------------------------------------------------

def fill_ufc_events(year: int = DEV_YEAR) -> None:
    events_file = RAW_BASE / "ufcstats" / "ufc" / str(year) / "events.json"
    if not events_file.is_file():
        return
    data = load_json(events_file)
    if not data:
        return
    items = data if isinstance(data, list) else data.get("events", data.get("data", []))
    rows: list[dict] = []
    for ev in (items if isinstance(items, list) else []):
        if not isinstance(ev, dict):
            continue
        rows.append({
            "source": "ufcstats",
            "id": str(ev.get("id", "")),
            "sport": "ufc",
            "name": ev.get("name", ""),
            "date": ev.get("date", ""),
            "location": ev.get("location", ""),
            "url": ev.get("url", ""),
            "season": str(year),
        })
    if rows:
        write_parquet_rows(rows, "ufc", "events", year)
        print(f"  ufc/events: {len(rows)} rows")


def fill_ufc_fights(year: int = DEV_YEAR) -> None:
    fights_dir = RAW_BASE / "ufcstats" / "ufc" / str(year) / "fights"
    if not fights_dir.is_dir():
        return
    rows: list[dict] = []
    for f in sorted(fights_dir.glob("*.json")):
        data = load_json(f)
        if not data:
            continue
        event = data.get("event", {}) if isinstance(data, dict) else {}
        fights = data.get("fights", []) if isinstance(data, dict) else []
        for fight in fights:
            if not isinstance(fight, dict):
                continue
            fighters = fight.get("fighters", [])
            # Fighters can be strings or dicts depending on data source
            f1_raw = fighters[0] if len(fighters) > 0 else ""
            f2_raw = fighters[1] if len(fighters) > 1 else ""
            if isinstance(f1_raw, dict):
                f1_name = f1_raw.get("name", "")
                f1_id = str(f1_raw.get("id", ""))
            else:
                f1_name = str(f1_raw)
                f1_id = ""
            if isinstance(f2_raw, dict):
                f2_name = f2_raw.get("name", "")
                f2_id = str(f2_raw.get("id", ""))
            else:
                f2_name = str(f2_raw)
                f2_id = ""
            rows.append({
                "source": "ufcstats",
                "id": str(fight.get("id", "")),
                "sport": "ufc",
                "event_id": str(fight.get("eventId", "")),
                "event_name": event.get("name", "") if isinstance(event, dict) else "",
                "fighter_1_id": f1_id,
                "fighter_1_name": f1_name,
                "fighter_2_id": f2_id,
                "fighter_2_name": f2_name,
                "result": fight.get("result", ""),
                "method": fight.get("method", ""),
                "round": str(fight.get("round", "")),
                "time": fight.get("time", ""),
                "weight_class": fight.get("weightClass", ""),
                "date": event.get("date", "") if isinstance(event, dict) else "",
                "season": str(year),
            })
    if rows:
        write_parquet_rows(rows, "ufc", "fights", year)
        print(f"  ufc/fights: {len(rows)} rows")


def fill_ufc_weight_classes(year: int = DEV_YEAR) -> None:
    fights_dir = RAW_BASE / "ufcstats" / "ufc" / str(year) / "fights"
    if not fights_dir.is_dir():
        return
    wc_set: set[str] = set()
    for f in fights_dir.glob("*.json"):
        data = load_json(f)
        if not data:
            continue
        fights = data.get("fights", []) if isinstance(data, dict) else []
        for fight in fights:
            if not isinstance(fight, dict):
                continue
            wc = fight.get("weightClass", "")
            if wc:
                wc_set.add(wc)
    rows = [
        {"source": "derived", "id": wc.lower().replace(" ", "_"), "sport": "ufc", "name": wc}
        for wc in sorted(wc_set)
    ]
    if rows:
        write_parquet_rows(rows, "ufc", "weight_classes")
        print(f"  ufc/weight_classes: {len(rows)} rows")


def fill_ufc_rankings(year: int = DEV_YEAR) -> None:
    fill_rankings_from_fights("ufc", year)


def fill_ufc_leagues() -> None:
    rows = [{
        "source": "derived",
        "id": "ufc",
        "sport": "ufc",
        "name": "Ultimate Fighting Championship",
        "abbreviation": "UFC",
        "country": "US",
    }]
    write_parquet_rows(rows, "ufc", "leagues")
    print(f"  ufc/leagues: {len(rows)} rows")


def fill_ufc_player_props(year: int = DEV_YEAR) -> None:
    odds_dir = RAW_BASE / "odds" / "providers" / "oddsapi" / "ufc"
    if not odds_dir.is_dir():
        return
    rows: list[dict] = []
    for f in odds_dir.rglob("*.json"):
        data = load_json(f)
        if not data:
            continue
        items = data if isinstance(data, list) else data.get("data", [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            for bk in item.get("bookmakers", []):
                if not isinstance(bk, dict):
                    continue
                for market in bk.get("markets", []):
                    if not isinstance(market, dict):
                        continue
                    mk_key = market.get("key", "")
                    if "player" not in mk_key:
                        continue
                    for outcome in market.get("outcomes", []):
                        rows.append({
                            "source": "oddsapi",
                            "id": f"{item.get('id', '')}_{bk.get('key', '')}_{mk_key}_{outcome.get('name', '')}",
                            "sport": "ufc",
                            "event_id": str(item.get("id", "")),
                            "bookmaker": bk.get("title", ""),
                            "market": mk_key,
                            "player_name": outcome.get("description", "") or outcome.get("name", ""),
                            "selection": outcome.get("name", ""),
                            "point": _safe_float(outcome.get("point")),
                            "price": _safe_float(outcome.get("price")),
                            "season": str(year),
                        })
    if rows:
        write_parquet_rows(rows, "ufc", "player_props", year)
        print(f"  ufc/player_props: {len(rows)} rows")


# ---------------------------------------------------------------------------
# CS2/CSGO
# ---------------------------------------------------------------------------

def fill_csgo_matches(year: int = DEV_YEAR) -> None:
    matches_file = RAW_BASE / "pandascore" / "csgo" / str(year) / "matches.json"
    if not matches_file.is_file():
        return
    data = load_json(matches_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for m in data:
        if not isinstance(m, dict):
            continue
        opponents = m.get("opponents", [])
        team1 = opponents[0].get("opponent", {}) if len(opponents) > 0 and isinstance(opponents[0], dict) else {}
        team2 = opponents[1].get("opponent", {}) if len(opponents) > 1 and isinstance(opponents[1], dict) else {}
        results = m.get("results", [])
        t1_score = results[0].get("score", 0) if len(results) > 0 and isinstance(results[0], dict) else 0
        t2_score = results[1].get("score", 0) if len(results) > 1 and isinstance(results[1], dict) else 0
        winner = m.get("winner", {})
        tournament = m.get("tournament", {}) or {}
        league = m.get("league", {}) or {}
        rows.append({
            "source": "pandascore",
            "id": str(m.get("id", "")),
            "sport": "csgo",
            "slug": m.get("slug", ""),
            "name": m.get("name", ""),
            "tournament_id": str(tournament.get("id", "")),
            "tournament_name": tournament.get("name", ""),
            "league_id": str(league.get("id", "")),
            "league_name": league.get("name", ""),
            "team1_id": str(team1.get("id", "")),
            "team1_name": team1.get("name", ""),
            "team2_id": str(team2.get("id", "")),
            "team2_name": team2.get("name", ""),
            "team1_score": _safe_int(t1_score, 0),
            "team2_score": _safe_int(t2_score, 0),
            "winner_id": str(winner.get("id", "")) if isinstance(winner, dict) else "",
            "winner_name": winner.get("name", "") if isinstance(winner, dict) else "",
            "best_of": _safe_int(m.get("number_of_games"), 0),
            "status": m.get("status", ""),
            "match_type": m.get("match_type", ""),
            "begin_at": m.get("begin_at", ""),
            "end_at": m.get("end_at", ""),
            "forfeit": m.get("forfeit", False),
            "season": str(year),
        })
    if rows:
        write_parquet_rows(rows, "csgo", "matches", year)
        print(f"  csgo/matches: {len(rows)} rows")


def fill_csgo_match_maps(year: int = DEV_YEAR) -> None:
    matches_file = RAW_BASE / "pandascore" / "csgo" / str(year) / "matches.json"
    if not matches_file.is_file():
        return
    data = load_json(matches_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for m in data:
        if not isinstance(m, dict):
            continue
        match_id = str(m.get("id", ""))
        for game in (m.get("games", []) or []):
            if not isinstance(game, dict):
                continue
            winner = game.get("winner", {})
            rows.append({
                "source": "pandascore",
                "id": str(game.get("id", "")),
                "sport": "csgo",
                "match_id": match_id,
                "map_number": _safe_int(game.get("position"), 0),
                "map_name": "",
                "status": game.get("status", ""),
                "finished": game.get("finished", False),
                "begin_at": game.get("begin_at", ""),
                "end_at": game.get("end_at", ""),
                "winner_id": str(winner.get("id", "")) if isinstance(winner, dict) else "",
                "winner_type": game.get("winner_type", ""),
                "duration_seconds": _safe_int(game.get("length")),
                "forfeit": game.get("forfeit", False),
                "season": str(year),
            })
    if rows:
        write_parquet_rows(rows, "csgo", "match_maps", year)
        print(f"  csgo/match_maps: {len(rows)} rows")


def fill_csgo_tournaments(year: int = DEV_YEAR) -> None:
    tourn_file = RAW_BASE / "pandascore" / "csgo" / str(year) / "tournaments.json"
    if not tourn_file.is_file():
        return
    data = load_json(tourn_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for t in data:
        if not isinstance(t, dict):
            continue
        winner = t.get("winner_id")
        league = t.get("league", {}) or {}
        rows.append({
            "source": "pandascore",
            "id": str(t.get("id", "")),
            "sport": "csgo",
            "name": t.get("name", ""),
            "slug": t.get("slug", ""),
            "type": t.get("type", ""),
            "country": t.get("country", "") if t.get("country") else "",
            "begin_at": t.get("begin_at", ""),
            "end_at": t.get("end_at", ""),
            "status": "finished" if t.get("end_at") else "upcoming",
            "winner_id": str(winner) if winner else "",
            "league_id": str(league.get("id", "")),
            "league_name": league.get("name", ""),
            "has_detailed_stats": t.get("detailed_stats", False),
            "season": str(year),
        })
    if rows:
        write_parquet_rows(rows, "csgo", "tournaments", year)
        print(f"  csgo/tournaments: {len(rows)} rows")


def fill_csgo_tournament_teams(year: int = DEV_YEAR) -> None:
    tourn_file = RAW_BASE / "pandascore" / "csgo" / str(year) / "tournaments.json"
    if not tourn_file.is_file():
        return
    data = load_json(tourn_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for t in data:
        if not isinstance(t, dict):
            continue
        tourn_id = str(t.get("id", ""))
        tourn_name = t.get("name", "")
        for team in (t.get("teams", []) or []):
            if not isinstance(team, dict):
                continue
            rows.append({
                "source": "pandascore",
                "id": f"{tourn_id}_{team.get('id', '')}",
                "sport": "csgo",
                "tournament_id": tourn_id,
                "tournament_name": tourn_name,
                "team_id": str(team.get("id", "")),
                "team_name": team.get("name", ""),
                "team_slug": team.get("slug", ""),
                "season": str(year),
            })
    if rows:
        write_parquet_rows(rows, "csgo", "tournament_teams", year)
        print(f"  csgo/tournament_teams: {len(rows)} rows")


def fill_csgo_player_map_stats(year: int = DEV_YEAR) -> None:
    """Derive player map stats from player_stats parquets if available."""
    fill_advanced_from_player_stats("csgo", year)
    # Rename output entity
    src = CURATED_BASE / "csgo" / "advanced"
    dst = CURATED_BASE / "csgo" / "player_map_stats"
    if src.is_dir() and not dst.is_dir():
        src.rename(dst)
        print(f"  csgo/player_map_stats: renamed from advanced")


def fill_csgo_player_accuracy(year: int = DEV_YEAR) -> None:
    """Derive player accuracy from player_stats if headshot/accuracy data exists."""
    ps_rows = read_player_stats_parquets("csgo", year)
    if not ps_rows:
        return
    name_col = next((c for c in ["player_name", "name"] if c in ps_rows[0]), None)
    player_col = next((c for c in ["player_id", "id"] if c in ps_rows[0]), None)
    if not player_col:
        return

    # Group by player
    players: dict[str, list[dict]] = {}
    for r in ps_rows:
        pid = str(r.get(player_col, ""))
        players.setdefault(pid, []).append(r)

    rows: list[dict] = []
    for pid, grp in players.items():
        row: dict = {
            "source": "derived", "id": pid, "sport": "csgo",
            "player_name": str(grp[0][name_col]) if name_col else "",
            "season": str(year),
            "games_played": len(grp),
        }
        # Look for accuracy-related columns
        for col_pattern in ["headshot", "accuracy", "hits", "shots", "kills", "deaths", "assists"]:
            matching = [c for c in grp[0].keys() if col_pattern in c.lower() and isinstance(grp[0].get(c), (int, float))]
            for col in matching:
                vals = [_safe_float(r.get(col), 0.0) for r in grp]
                row[f"avg_{col}"] = round(sum(vals) / len(vals), 2) if vals else 0.0
                row[f"total_{col}"] = round(sum(vals), 2)
        rows.append(row)

    if rows:
        write_parquet_rows(rows, "csgo", "player_accuracy", year)
        print(f"  csgo/player_accuracy: {len(rows)} rows")


def fill_csgo_team_round_stats(year: int = DEV_YEAR) -> None:
    """Aggregate team round stats from player_stats."""
    fill_team_stats_from_player_stats("csgo", year)
    # Copy to team_round_stats entity
    src_dir = CURATED_BASE / "csgo" / "team_stats" / f"season={year}"
    dst_dir = CURATED_BASE / "csgo" / "team_round_stats" / f"season={year}"
    if src_dir.is_dir():
        dst_dir.mkdir(parents=True, exist_ok=True)
        src_file = src_dir / "part.parquet"
        if src_file.is_file():
            import shutil
            shutil.copy2(src_file, dst_dir / "part.parquet")
            print(f"  csgo/team_round_stats: copied from team_stats")


def fill_csgo_team_map_pool(year: int = DEV_YEAR) -> None:
    """Derive team map pool from matches data."""
    matches_file = RAW_BASE / "pandascore" / "csgo" / str(year) / "matches.json"
    if not matches_file.is_file():
        return
    data = load_json(matches_file)
    if not data or not isinstance(data, list):
        return

    # Collect maps played per team
    team_maps: dict[str, dict] = {}  # team_id -> {name, maps: set}
    for m in data:
        if not isinstance(m, dict):
            continue
        opponents = m.get("opponents", [])
        for opp in opponents:
            if not isinstance(opp, dict):
                continue
            team = opp.get("opponent", {})
            if not isinstance(team, dict):
                continue
            tid = str(team.get("id", ""))
            if tid not in team_maps:
                team_maps[tid] = {"name": team.get("name", ""), "maps": set(), "games": 0}
            team_maps[tid]["games"] += 1
            for game in (m.get("games", []) or []):
                if isinstance(game, dict):
                    map_name = game.get("map", {})
                    if isinstance(map_name, dict):
                        map_name = map_name.get("name", "")
                    if map_name:
                        team_maps[tid]["maps"].add(str(map_name))

    rows: list[dict] = []
    for tid, info in team_maps.items():
        rows.append({
            "source": "derived",
            "id": tid,
            "sport": "csgo",
            "team_id": tid,
            "team_name": info["name"],
            "maps_played": ",".join(sorted(info["maps"])) if info["maps"] else "",
            "map_count": len(info["maps"]),
            "total_matches": info["games"],
            "season": str(year),
        })
    if rows:
        write_parquet_rows(rows, "csgo", "team_map_pool", year)
        print(f"  csgo/team_map_pool: {len(rows)} rows")


# ---------------------------------------------------------------------------
# LoL
# ---------------------------------------------------------------------------

def fill_lol_matches(year: int = DEV_YEAR) -> None:
    matches_file = RAW_BASE / "pandascore" / "lol" / str(year) / "matches.json"
    if not matches_file.is_file():
        return
    data = load_json(matches_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for m in data:
        if not isinstance(m, dict):
            continue
        opponents = m.get("opponents", [])
        team1 = opponents[0].get("opponent", {}) if len(opponents) > 0 and isinstance(opponents[0], dict) else {}
        team2 = opponents[1].get("opponent", {}) if len(opponents) > 1 and isinstance(opponents[1], dict) else {}
        results = m.get("results", [])
        t1_score = results[0].get("score", 0) if len(results) > 0 and isinstance(results[0], dict) else 0
        t2_score = results[1].get("score", 0) if len(results) > 1 and isinstance(results[1], dict) else 0
        winner = m.get("winner", {})
        tournament = m.get("tournament", {}) or {}
        rows.append({
            "source": "pandascore",
            "id": str(m.get("id", "")),
            "sport": "lol",
            "slug": m.get("slug", ""),
            "name": m.get("name", ""),
            "tournament_id": str(tournament.get("id", "")),
            "tournament_name": tournament.get("name", ""),
            "team1_id": str(team1.get("id", "")),
            "team1_name": team1.get("name", ""),
            "team2_id": str(team2.get("id", "")),
            "team2_name": team2.get("name", ""),
            "team1_score": _safe_int(t1_score, 0),
            "team2_score": _safe_int(t2_score, 0),
            "winner_id": str(winner.get("id", "")) if isinstance(winner, dict) else "",
            "best_of": _safe_int(m.get("number_of_games"), 0),
            "status": m.get("status", ""),
            "begin_at": m.get("begin_at", ""),
            "end_at": m.get("end_at", ""),
            "season": str(year),
        })
    if rows:
        write_parquet_rows(rows, "lol", "matches", year)
        print(f"  lol/matches: {len(rows)} rows")


def fill_lol_match_maps(year: int = DEV_YEAR) -> None:
    matches_file = RAW_BASE / "pandascore" / "lol" / str(year) / "matches.json"
    if not matches_file.is_file():
        return
    data = load_json(matches_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for m in data:
        if not isinstance(m, dict):
            continue
        match_id = str(m.get("id", ""))
        for game in (m.get("games", []) or []):
            if not isinstance(game, dict):
                continue
            winner = game.get("winner", {})
            rows.append({
                "source": "pandascore",
                "id": str(game.get("id", "")),
                "sport": "lol",
                "match_id": match_id,
                "game_number": _safe_int(game.get("position"), 0),
                "status": game.get("status", ""),
                "begin_at": game.get("begin_at", ""),
                "end_at": game.get("end_at", ""),
                "duration_seconds": _safe_int(game.get("length")),
                "winner_id": str(winner.get("id", "")) if isinstance(winner, dict) else "",
                "forfeit": game.get("forfeit", False),
                "season": str(year),
            })
    if rows:
        write_parquet_rows(rows, "lol", "match_maps", year)
        print(f"  lol/match_maps: {len(rows)} rows")


def fill_lol_tournaments(year: int = DEV_YEAR) -> None:
    tourn_file = RAW_BASE / "pandascore" / "lol" / str(year) / "tournaments.json"
    if not tourn_file.is_file():
        return
    data = load_json(tourn_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for t in data:
        if not isinstance(t, dict):
            continue
        league = t.get("league", {}) or {}
        rows.append({
            "source": "pandascore",
            "id": str(t.get("id", "")),
            "sport": "lol",
            "name": t.get("name", ""),
            "slug": t.get("slug", ""),
            "begin_at": t.get("begin_at", ""),
            "end_at": t.get("end_at", ""),
            "league_id": str(league.get("id", "")),
            "league_name": league.get("name", ""),
            "season": str(year),
        })
    if rows:
        write_parquet_rows(rows, "lol", "tournaments", year)
        print(f"  lol/tournaments: {len(rows)} rows")


def fill_lol_tournament_roster(year: int = DEV_YEAR) -> None:
    tourn_file = RAW_BASE / "pandascore" / "lol" / str(year) / "tournaments.json"
    if not tourn_file.is_file():
        return
    data = load_json(tourn_file)
    if not data or not isinstance(data, list):
        return
    rows: list[dict] = []
    for t in data:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id", ""))
        for team in (t.get("teams", []) or []):
            if not isinstance(team, dict):
                continue
            team_id = str(team.get("id", ""))
            for player in (team.get("players", []) or []):
                if not isinstance(player, dict):
                    continue
                rows.append({
                    "source": "pandascore",
                    "id": f"{tid}_{team_id}_{player.get('id', '')}",
                    "sport": "lol",
                    "tournament_id": tid,
                    "team_id": team_id,
                    "team_name": team.get("name", ""),
                    "player_id": str(player.get("id", "")),
                    "player_name": player.get("name", "") or player.get("slug", ""),
                    "role": player.get("role", ""),
                    "season": str(year),
                })
    if rows:
        write_parquet_rows(rows, "lol", "tournament_roster", year)
        print(f"  lol/tournament_roster: {len(rows)} rows")


def fill_lol_champions() -> None:
    champ_dir = RAW_BASE / "riot" / "lol" / "reference" / "champions"
    if not champ_dir.is_dir():
        return
    files = sorted(champ_dir.glob("*.json"))
    if not files:
        return
    data = load_json(files[-1])  # Latest version
    if not data:
        return
    champ_data = data.get("data", data)
    if not isinstance(champ_data, dict):
        return
    rows: list[dict] = []
    for key, champ in champ_data.items():
        if not isinstance(champ, dict):
            continue
        stats = champ.get("stats", {})
        if not isinstance(stats, dict):
            stats = {}
        tags = champ.get("tags", [])
        rows.append({
            "source": "riot",
            "id": str(champ.get("key", "")),
            "sport": "lol",
            "name": champ.get("name", ""),
            "title": champ.get("title", ""),
            "champion_key": key,
            "tags": ",".join(tags) if isinstance(tags, list) else str(tags),
            "partype": champ.get("partype", ""),
            "hp": _safe_float(stats.get("hp"), 0.0),
            "attack_damage": _safe_float(stats.get("attackdamage"), 0.0),
            "armor": _safe_float(stats.get("armor"), 0.0),
            "magic_resist": _safe_float(stats.get("spellblock"), 0.0),
            "move_speed": _safe_float(stats.get("movespeed"), 0.0),
            "attack_range": _safe_float(stats.get("attackrange"), 0.0),
        })
    if rows:
        write_parquet_rows(rows, "lol", "champions")
        print(f"  lol/champions: {len(rows)} rows")


def fill_lol_items() -> None:
    items_dir = RAW_BASE / "riot" / "lol" / "reference" / "items"
    if not items_dir.is_dir():
        return
    files = sorted(items_dir.glob("*.json"))
    if not files:
        return
    data = load_json(files[-1])
    if not data:
        return
    item_data = data.get("data", data)
    if not isinstance(item_data, dict):
        return
    rows: list[dict] = []
    for item_id, item in item_data.items():
        if not isinstance(item, dict):
            continue
        gold = item.get("gold", {})
        if not isinstance(gold, dict):
            gold = {}
        tags = item.get("tags", [])
        rows.append({
            "source": "riot",
            "id": item_id,
            "sport": "lol",
            "name": item.get("name", ""),
            "description": item.get("plaintext", ""),
            "total_gold": _safe_int(gold.get("total"), 0),
            "base_gold": _safe_int(gold.get("base"), 0),
            "purchasable": gold.get("purchasable", False),
            "tags": ",".join(tags) if isinstance(tags, list) else str(tags),
        })
    if rows:
        write_parquet_rows(rows, "lol", "items")
        print(f"  lol/items: {len(rows)} rows")


def fill_lol_runes() -> None:
    runes_dir = RAW_BASE / "riot" / "lol" / "reference" / "runes"
    if not runes_dir.is_dir():
        return
    files = sorted(runes_dir.glob("*.json"))
    if not files:
        return
    data = load_json(files[-1])
    if not data:
        return
    rows: list[dict] = []
    if isinstance(data, list):
        for tree in data:
            if not isinstance(tree, dict):
                continue
            tree_name = tree.get("name", "")
            tree_id = str(tree.get("id", ""))
            for slot_idx, slot in enumerate(tree.get("slots", [])):
                if not isinstance(slot, dict):
                    continue
                for rune in slot.get("runes", []):
                    if not isinstance(rune, dict):
                        continue
                    rows.append({
                        "source": "riot",
                        "id": str(rune.get("id", "")),
                        "sport": "lol",
                        "name": rune.get("name", ""),
                        "tree_name": tree_name,
                        "tree_id": tree_id,
                        "slot": slot_idx,
                        "rune_type": "keystone" if slot_idx == 0 else "minor",
                    })
    elif isinstance(data, dict):
        # Alternative format: dict of rune trees
        for tree_key, tree in data.items():
            if not isinstance(tree, dict):
                continue
            tree_name = tree.get("name", tree_key)
            tree_id = str(tree.get("id", tree_key))
            for slot_idx, slot in enumerate(tree.get("slots", [])):
                if not isinstance(slot, dict):
                    continue
                for rune in slot.get("runes", []):
                    if not isinstance(rune, dict):
                        continue
                    rows.append({
                        "source": "riot",
                        "id": str(rune.get("id", "")),
                        "sport": "lol",
                        "name": rune.get("name", ""),
                        "tree_name": tree_name,
                        "tree_id": tree_id,
                        "slot": slot_idx,
                        "rune_type": "keystone" if slot_idx == 0 else "minor",
                    })
    if rows:
        write_parquet_rows(rows, "lol", "runes")
        print(f"  lol/runes: {len(rows)} rows")


def fill_lol_spells() -> None:
    spells_dir = RAW_BASE / "riot" / "lol" / "reference" / "summoner_spells"
    if not spells_dir.is_dir():
        return
    files = sorted(spells_dir.glob("*.json"))
    if not files:
        return
    data = load_json(files[-1])
    if not data:
        return
    spell_data = data.get("data", data)
    if not isinstance(spell_data, dict):
        return
    rows: list[dict] = []
    for key, spell in spell_data.items():
        if not isinstance(spell, dict):
            continue
        desc = spell.get("description", "")
        rows.append({
            "source": "riot",
            "id": str(spell.get("key", key)),
            "sport": "lol",
            "name": spell.get("name", ""),
            "description": desc[:200] if isinstance(desc, str) else "",
            "cooldown": spell.get("cooldownBurn", ""),
            "spell_key": key,
        })
    if rows:
        write_parquet_rows(rows, "lol", "spells")
        print(f"  lol/spells: {len(rows)} rows")


def fill_lol_team_stats(year: int = DEV_YEAR) -> None:
    fill_team_stats_from_player_stats("lol", year)


def fill_lol_champion_stats(year: int = DEV_YEAR) -> None:
    """Derive champion-level aggregates from player_stats if champion data present."""
    ps_rows = read_player_stats_parquets("lol", year)
    if not ps_rows:
        return
    champ_col = next((c for c in ["champion", "champion_name", "champion_id"] if c in ps_rows[0]), None)
    if not champ_col:
        print("  No champion column found in lol player_stats")
        return

    # Group by champion
    champs: dict[str, list[dict]] = {}
    for r in ps_rows:
        cid = str(r.get(champ_col, ""))
        if cid:
            champs.setdefault(cid, []).append(r)

    skip = {"game_id", "team_id", "player_id", "id", "champion", "champion_name", "champion_id"}
    numeric_cols: list[str] = []
    for k, v in ps_rows[0].items():
        if k in skip:
            continue
        if isinstance(v, (int, float)) and v is not None:
            numeric_cols.append(k)
    numeric_cols = numeric_cols[:20]

    rows: list[dict] = []
    for cid, grp in champs.items():
        row: dict = {
            "source": "derived", "id": cid, "sport": "lol",
            "champion": cid,
            "games_played": len(grp),
            "season": str(year),
        }
        for col in numeric_cols:
            vals = [_safe_float(r.get(col), 0.0) for r in grp]
            row[f"avg_{col}"] = round(sum(vals) / len(vals), 2) if vals else 0.0
            row[f"total_{col}"] = round(sum(vals), 2)
        rows.append(row)

    if rows:
        write_parquet_rows(rows, "lol", "champion_stats", year)
        print(f"  lol/champion_stats: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill empty curated entities for Wave 5 sports."
    )
    parser.add_argument(
        "--sport", type=str, default=None,
        help="Run only for a specific sport (e.g. epl, ncaaw, ufc, csgo, lol)",
    )
    parser.add_argument(
        "--entity", type=str, default=None,
        help="Run only for a specific entity (e.g. rosters, matches)",
    )
    args = parser.parse_args()

    print("=== Wave 5 Gap Fill ===")

    # ---- Soccer ----
    if not args.sport or args.sport in SOCCER_LEAGUES:
        print("\n--- Soccer Leagues ---")
        for league in SOCCER_LEAGUES:
            if args.sport and args.sport != league:
                continue
            print(f"\n  [{league.upper()}]")
            if not args.entity or args.entity == "rosters":
                fill_soccer_rosters(league)
            if not args.entity or args.entity == "match_events":
                fill_soccer_match_events(league)
            if not args.entity or args.entity == "team_stats":
                fill_soccer_team_stats(league)
            if not args.entity or args.entity == "transfers":
                fill_soccer_transfers(league)
            if not args.entity or args.entity == "player_props":
                fill_soccer_player_props(league)

    # ---- NCAAW ----
    if not args.sport or args.sport == "ncaaw":
        print("\n--- NCAAW ---")
        if not args.entity or args.entity == "rankings":
            fill_ncaaw_rankings()
        if not args.entity or args.entity == "injuries":
            fill_ncaaw_injuries()
        if not args.entity or args.entity == "plays":
            fill_ncaaw_plays()
        if not args.entity or args.entity == "team_stats":
            fill_ncaaw_team_stats()
        if not args.entity or args.entity == "leaders":
            fill_ncaaw_leaders()
        if not args.entity or args.entity == "advanced":
            fill_ncaaw_advanced()
        if not args.entity or args.entity == "odds":
            fill_ncaaw_odds()
        if not args.entity or args.entity == "bracket":
            fill_ncaaw_bracket()
        if not args.entity or args.entity == "player_props":
            fill_ncaaw_player_props()

    # ---- UFC ----
    if not args.sport or args.sport == "ufc":
        print("\n--- UFC ---")
        if not args.entity or args.entity == "events":
            fill_ufc_events()
        if not args.entity or args.entity == "fights":
            fill_ufc_fights()
        if not args.entity or args.entity == "weight_classes":
            fill_ufc_weight_classes()
        if not args.entity or args.entity == "rankings":
            fill_ufc_rankings()
        if not args.entity or args.entity == "leagues":
            fill_ufc_leagues()
        if not args.entity or args.entity == "player_props":
            fill_ufc_player_props()

    # ---- CS2/CSGO ----
    if not args.sport or args.sport == "csgo":
        print("\n--- CS2/CSGO ---")
        if not args.entity or args.entity == "matches":
            fill_csgo_matches()
        if not args.entity or args.entity == "match_maps":
            fill_csgo_match_maps()
        if not args.entity or args.entity == "tournaments":
            fill_csgo_tournaments()
        if not args.entity or args.entity == "tournament_teams":
            fill_csgo_tournament_teams()
        if not args.entity or args.entity == "player_map_stats":
            fill_csgo_player_map_stats()
        if not args.entity or args.entity == "player_accuracy":
            fill_csgo_player_accuracy()
        if not args.entity or args.entity == "team_round_stats":
            fill_csgo_team_round_stats()
        if not args.entity or args.entity == "team_map_pool":
            fill_csgo_team_map_pool()

    # ---- LoL ----
    if not args.sport or args.sport == "lol":
        print("\n--- LoL ---")
        if not args.entity or args.entity == "matches":
            fill_lol_matches()
        if not args.entity or args.entity == "match_maps":
            fill_lol_match_maps()
        if not args.entity or args.entity == "tournaments":
            fill_lol_tournaments()
        if not args.entity or args.entity == "tournament_roster":
            fill_lol_tournament_roster()
        if not args.entity or args.entity == "champions":
            fill_lol_champions()
        if not args.entity or args.entity == "items":
            fill_lol_items()
        if not args.entity or args.entity == "runes":
            fill_lol_runes()
        if not args.entity or args.entity == "spells":
            fill_lol_spells()
        if not args.entity or args.entity == "team_stats":
            fill_lol_team_stats()
        if not args.entity or args.entity == "champion_stats":
            fill_lol_champion_stats()

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
