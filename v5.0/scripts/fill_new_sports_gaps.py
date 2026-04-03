#!/usr/bin/env python3
"""Fill empty curated entities for NHL, WNBA, and NCAAB.

Parses raw JSON data and derives aggregates from existing player_stats
parquets. Only processes season/year 2024 (dev mode).

Entities filled:
  NHL:   box_scores, plays, team_stats, leaders, draft
  WNBA:  box_scores, plays, team_stats, leaders, odds, player_props, advanced
  NCAAB: rankings, injuries, plays, team_stats, leaders, bracket,
         player_props, advanced

Usage:
    python scripts/fill_new_sports_gaps.py
    python scripts/fill_new_sports_gaps.py --sport nhl
    python scripts/fill_new_sports_gaps.py --sport wnba --entity box_scores
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_NHL = PROJECT_ROOT / "data" / "raw" / "nhl" / "nhl"
RAW_ESPN_WNBA = PROJECT_ROOT / "data" / "raw" / "espn" / "wnba"
RAW_ESPN_NCAAB = PROJECT_ROOT / "data" / "raw" / "espn" / "ncaab"
CURATED_BASE = PROJECT_ROOT / "data" / "normalized_curated"

DEV_YEAR = 2024  # Only process this year for dev speed

NHL_SEASON_TYPES = ["regular", "playoffs"]
ESPN_SEASON_TYPES = ["regular", "postseason"]

NHL_ENTITIES = ["box_scores", "plays", "team_stats", "leaders", "draft"]
WNBA_ENTITIES = [
    "box_scores", "plays", "team_stats", "leaders",
    "odds", "player_props", "advanced",
]
NCAAB_ENTITIES = [
    "rankings", "injuries", "plays", "team_stats", "leaders",
    "bracket", "player_props", "advanced",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  WARNING: skipping {path}: {e}", file=sys.stderr)
        return None


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
        columns: dict[str, list] = {}
        for col in rows[0]:
            columns[col] = [r.get(col) for r in rows]
        table = pa.table(columns)
    elif schema is not None:
        table = schema.empty_table()
    else:
        print(f"    → skipped {entity}: no rows and no schema")
        return

    pq.write_table(table, out_path, compression="zstd")
    rel = out_path.relative_to(PROJECT_ROOT)
    print(f"    → wrote {rel} ({len(rows):,} rows)")


def write_empty_parquet(
    sport: str,
    entity: str,
    columns: dict[str, pa.DataType],
    season_year: int | None = None,
) -> None:
    """Create an empty (0-row) parquet with the given schema columns."""
    schema = pa.schema([(name, dtype) for name, dtype in columns.items()])
    write_parquet_rows([], sport, entity, season_year=season_year, schema=schema)


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
                # Cast dictionary-encoded columns to plain types
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
# NHL: box_scores
# ---------------------------------------------------------------------------

def fill_nhl_box_scores() -> None:
    print("\n── NHL box_scores ──")
    year = DEV_YEAR
    rows: list[dict] = []

    for season_type in NHL_SEASON_TYPES:
        games_dir = RAW_NHL / str(year) / "games" / season_type
        if not games_dir.is_dir():
            print(f"  {games_dir.name}: not found")
            continue
        game_ids = sorted(
            g for g in os.listdir(games_dir)
            if (games_dir / g / "boxscore.json").is_file()
        )
        print(f"  {season_type}: {len(game_ids)} games")

        for gid in game_ids:
            data = load_json(games_dir / gid / "boxscore.json")
            if not data:
                continue
            game_id = str(data.get("id", gid))
            game_date = data.get("gameDate", "")
            season_raw = data.get("season", 0)
            season_val = season_raw // 10000 if season_raw > 99999999 else year

            away = data.get("awayTeam", {})
            home = data.get("homeTeam", {})
            pgs = data.get("playerByGameStats", {})

            for side_key, team_info in [("awayTeam", away), ("homeTeam", home)]:
                team_id = str(team_info.get("id", ""))
                team_name = team_info.get("commonName", {})
                if isinstance(team_name, dict):
                    team_name = team_name.get("default", "")
                team_abbrev = team_info.get("abbrev", "")

                side_stats = pgs.get(side_key, {})
                skaters = (
                    side_stats.get("forwards", [])
                    + side_stats.get("defense", [])
                )
                for p in skaters:
                    name = p.get("name", {})
                    if isinstance(name, dict):
                        name = name.get("default", "")
                    rows.append({
                        "game_id": game_id,
                        "season": season_val,
                        "date": game_date,
                        "season_type": season_type,
                        "team_id": team_id,
                        "team_name": team_name,
                        "team_abbrev": team_abbrev,
                        "player_id": str(p.get("playerId", "")),
                        "player_name": name,
                        "position": p.get("position", ""),
                        "stat_type": "skater",
                        "goals": _safe_int(p.get("goals"), 0),
                        "assists": _safe_int(p.get("assists"), 0),
                        "points": _safe_int(p.get("points"), 0),
                        "plus_minus": _safe_int(p.get("plusMinus"), 0),
                        "pim": _safe_int(p.get("pim"), 0),
                        "hits": _safe_int(p.get("hits"), 0),
                        "shots": _safe_int(p.get("sog"), 0),
                        "blocked_shots": _safe_int(p.get("blockedShots"), 0),
                        "takeaways": _safe_int(p.get("takeaways"), 0),
                        "giveaways": _safe_int(p.get("giveaways"), 0),
                        "power_play_goals": _safe_int(p.get("powerPlayGoals"), 0),
                        "faceoff_win_pct": _safe_float(p.get("faceoffWinningPctg")),
                        "toi": p.get("toi", ""),
                        "shifts": _safe_int(p.get("shifts"), 0),
                        # Goalie-only fields null for skaters
                        "saves": None,
                        "goals_against": None,
                        "shots_against": None,
                        "save_pct": None,
                        "decision": None,
                        "source": "nhl",
                    })

                for p in side_stats.get("goalies", []):
                    name = p.get("name", {})
                    if isinstance(name, dict):
                        name = name.get("default", "")
                    rows.append({
                        "game_id": game_id,
                        "season": season_val,
                        "date": game_date,
                        "season_type": season_type,
                        "team_id": team_id,
                        "team_name": team_name,
                        "team_abbrev": team_abbrev,
                        "player_id": str(p.get("playerId", "")),
                        "player_name": name,
                        "position": "G",
                        "stat_type": "goalie",
                        "goals": None,
                        "assists": None,
                        "points": None,
                        "plus_minus": None,
                        "pim": _safe_int(p.get("pim"), 0),
                        "hits": None,
                        "shots": None,
                        "blocked_shots": None,
                        "takeaways": None,
                        "giveaways": None,
                        "power_play_goals": None,
                        "faceoff_win_pct": None,
                        "toi": p.get("toi", ""),
                        "shifts": None,
                        "saves": _safe_int(p.get("saves")),
                        "goals_against": _safe_int(p.get("goalsAgainst")),
                        "shots_against": _safe_int(p.get("shotsAgainst")),
                        "save_pct": _safe_float(p.get("savePctg")),
                        "decision": p.get("decision", ""),
                        "source": "nhl",
                    })

    if rows:
        write_parquet_rows(rows, "nhl", "box_scores", season_year=year)
    else:
        print("  No box_score rows extracted")


# ---------------------------------------------------------------------------
# NHL: plays
# ---------------------------------------------------------------------------

def fill_nhl_plays() -> None:
    print("\n── NHL plays ──")
    year = DEV_YEAR
    rows: list[dict] = []

    for season_type in NHL_SEASON_TYPES:
        games_dir = RAW_NHL / str(year) / "games" / season_type
        if not games_dir.is_dir():
            continue
        game_ids = sorted(
            g for g in os.listdir(games_dir)
            if (games_dir / g / "pbp.json").is_file()
        )
        print(f"  {season_type}: {len(game_ids)} games with pbp")

        for gid in game_ids:
            data = load_json(games_dir / gid / "pbp.json")
            if not data:
                continue
            game_id = str(data.get("id", gid))
            game_date = data.get("gameDate", "")
            season_raw = data.get("season", 0)
            season_val = season_raw // 10000 if season_raw > 99999999 else year

            away_id = str(data.get("awayTeam", {}).get("id", ""))
            home_id = str(data.get("homeTeam", {}).get("id", ""))

            for play in data.get("plays", []):
                details = play.get("details", {})
                pd_desc = play.get("periodDescriptor", {})
                rows.append({
                    "game_id": game_id,
                    "season": season_val,
                    "date": game_date,
                    "season_type": season_type,
                    "event_id": _safe_int(play.get("eventId")),
                    "period": _safe_int(pd_desc.get("number")),
                    "period_type": pd_desc.get("periodType", ""),
                    "time_in_period": play.get("timeInPeriod", ""),
                    "time_remaining": play.get("timeRemaining", ""),
                    "event_type_code": _safe_int(play.get("typeCode")),
                    "event_type": play.get("typeDescKey", ""),
                    "sort_order": _safe_int(play.get("sortOrder")),
                    "situation_code": play.get("situationCode", ""),
                    "away_team_id": away_id,
                    "home_team_id": home_id,
                    "event_owner_team_id": str(details.get("eventOwnerTeamId", "")),
                    "x_coord": _safe_int(details.get("xCoord")),
                    "y_coord": _safe_int(details.get("yCoord")),
                    "zone_code": details.get("zoneCode", ""),
                    "shot_type": details.get("shotType", ""),
                    "scoring_player_id": str(details.get("scoringPlayerId", "")),
                    "assist1_player_id": str(details.get("assist1PlayerId", "")),
                    "assist2_player_id": str(details.get("assist2PlayerId", "")),
                    "goalie_in_net_id": str(details.get("goalieInNetId", "")),
                    "shooting_player_id": str(details.get("shootingPlayerId", "")),
                    "blocking_player_id": str(details.get("blockingPlayerId", "")),
                    "committed_by_player_id": str(
                        details.get("committedByPlayerId", "")
                    ),
                    "drawn_by_player_id": str(details.get("drawnByPlayerId", "")),
                    "penalty_type": details.get("descKey", ""),
                    "penalty_duration": _safe_int(details.get("duration")),
                    "away_score": _safe_int(details.get("awayScore")),
                    "home_score": _safe_int(details.get("homeScore")),
                    "source": "nhl",
                })

    if rows:
        write_parquet_rows(rows, "nhl", "plays", season_year=year)
    else:
        print("  No play rows extracted")


# ---------------------------------------------------------------------------
# NHL: team_stats (derived from player_stats)
# ---------------------------------------------------------------------------

def fill_nhl_team_stats() -> None:
    print("\n── NHL team_stats ──")
    ps = read_player_stats_parquets("nhl", DEV_YEAR)
    if not ps:
        return

    # Group by team_id
    teams: dict[str, list[dict]] = {}
    for r in ps:
        tid = r.get("team_id", "")
        if not tid:
            continue
        teams.setdefault(tid, []).append(r)

    rows: list[dict] = []
    for tid, players in teams.items():
        # Count unique game_ids for games_played
        game_ids = {r.get("game_id") for r in players if r.get("game_id")}
        numeric_cols = [
            "goals", "assists", "points", "shots", "saves",
            "goals_against", "hits", "blocked_shots", "pim",
            "plus_minus", "pp_goals", "takeaways", "giveaways",
            "shot_misses",
        ]
        agg: dict[str, float] = {}
        for col in numeric_cols:
            vals = [_safe_float(r.get(col), 0.0) for r in players if r.get(col) is not None]
            agg[col] = sum(vals)

        rows.append({
            "team_id": tid,
            "season": DEV_YEAR,
            "games_played": len(game_ids),
            **agg,
            "source": "derived",
        })

    if rows:
        write_parquet_rows(rows, "nhl", "team_stats", season_year=DEV_YEAR)
    else:
        print("  No team_stats rows derived")


# ---------------------------------------------------------------------------
# NHL: leaders (derived from player_stats)
# ---------------------------------------------------------------------------

def fill_nhl_leaders() -> None:
    print("\n── NHL leaders ──")
    ps = read_player_stats_parquets("nhl", DEV_YEAR)
    if not ps:
        return

    # Aggregate per player across all games
    player_agg: dict[str, dict] = {}
    for r in ps:
        pid = r.get("player_id", "")
        if not pid:
            continue
        if pid not in player_agg:
            player_agg[pid] = {
                "player_id": pid,
                "player_name": r.get("player_name", ""),
                "team_id": r.get("team_id", ""),
                "position": r.get("position", ""),
                "games_played": 0,
                "goals": 0.0,
                "assists": 0.0,
                "points": 0.0,
                "shots": 0.0,
                "hits": 0.0,
                "blocked_shots": 0.0,
                "saves": 0.0,
                "pim": 0.0,
                "plus_minus": 0.0,
            }
        pa_row = player_agg[pid]
        pa_row["games_played"] += 1
        for col in ["goals", "assists", "points", "shots", "hits",
                     "blocked_shots", "saves", "pim", "plus_minus"]:
            pa_row[col] += _safe_float(r.get(col), 0.0)

    players = list(player_agg.values())
    categories = ["goals", "assists", "points", "shots", "hits",
                   "blocked_shots", "saves", "pim"]
    rows: list[dict] = []
    for cat in categories:
        ranked = sorted(players, key=lambda p: p.get(cat, 0), reverse=True)[:25]
        for rank, p in enumerate(ranked, 1):
            rows.append({
                "season": DEV_YEAR,
                "category": cat,
                "rank": rank,
                "player_id": p["player_id"],
                "player_name": p["player_name"],
                "team_id": p["team_id"],
                "position": p.get("position", ""),
                "value": p.get(cat, 0),
                "games_played": p["games_played"],
                "source": "derived",
            })

    if rows:
        write_parquet_rows(rows, "nhl", "leaders", season_year=DEV_YEAR)
    else:
        print("  No leaders rows derived")


# ---------------------------------------------------------------------------
# NHL: draft (no raw data — empty schema)
# ---------------------------------------------------------------------------

def fill_nhl_draft() -> None:
    print("\n── NHL draft ──")
    draft_dir = RAW_NHL / str(DEV_YEAR) / "draft"
    if draft_dir.is_dir():
        print(f"  Found draft dir: {draft_dir}")
        # If data exists in the future, parse it here
    else:
        print("  No raw draft data — writing empty parquet")

    write_empty_parquet("nhl", "draft", {
        "season": pa.int32(),
        "round": pa.int32(),
        "pick": pa.int32(),
        "overall": pa.int32(),
        "player_id": pa.string(),
        "player_name": pa.string(),
        "team_id": pa.string(),
        "team_name": pa.string(),
        "position": pa.string(),
        "source": pa.string(),
    }, season_year=DEV_YEAR)


# ---------------------------------------------------------------------------
# ESPN box_scores helper (WNBA + NCAAB share the same JSON shape)
# ---------------------------------------------------------------------------

def _parse_espn_box_scores(
    raw_base: Path,
    sport: str,
    year: int,
    season_types: list[str],
) -> list[dict]:
    """Parse ESPN event game.json files into box_score rows."""
    rows: list[dict] = []
    for st in season_types:
        events_dir = raw_base / str(year) / "events" / st
        if not events_dir.is_dir():
            continue
        # Walk all subdirectories to find game.json files
        count = 0
        for root, _dirs, files in os.walk(events_dir):
            if "game.json" not in files:
                continue
            game_path = Path(root) / "game.json"
            data = load_json(game_path)
            if not data:
                continue
            event_id = str(data.get("eventId", ""))
            season_val = _safe_int(data.get("season"), year)

            # Extract date from path or data
            game_date = ""
            header = data.get("header", {})
            competitions = header.get("competitions", [])
            if competitions:
                game_date = competitions[0].get("date", "")[:10]

            summary = data.get("summary", {})
            boxscore = summary.get("boxscore", {})
            players_list = boxscore.get("players", [])

            for team_entry in players_list:
                team_info = team_entry.get("team", {})
                team_id = str(team_info.get("id", ""))
                team_name = team_info.get("displayName", "")

                for stat_group in team_entry.get("statistics", []):
                    keys = stat_group.get("keys", [])
                    names = stat_group.get("names", [])
                    labels = names or keys

                    for ath_entry in stat_group.get("athletes", []):
                        athlete = ath_entry.get("athlete", {})
                        player_id = str(athlete.get("id", ""))
                        player_name = athlete.get("displayName", "")
                        starter = ath_entry.get("starter", False)
                        stats_arr = ath_entry.get("stats", [])

                        stat_dict: dict[str, str] = {}
                        for idx, val in enumerate(stats_arr):
                            key = keys[idx] if idx < len(keys) else f"stat_{idx}"
                            stat_dict[key] = val

                        rows.append({
                            "game_id": event_id,
                            "season": season_val,
                            "date": game_date,
                            "season_type": st,
                            "team_id": team_id,
                            "team_name": team_name,
                            "player_id": player_id,
                            "player_name": player_name,
                            "starter": starter,
                            "minutes": stat_dict.get("minutes", ""),
                            "points": _safe_int(stat_dict.get("points")),
                            "rebounds": _safe_int(stat_dict.get("rebounds")),
                            "assists": _safe_int(stat_dict.get("assists")),
                            "steals": _safe_int(stat_dict.get("steals")),
                            "blocks": _safe_int(stat_dict.get("blocks")),
                            "turnovers": _safe_int(stat_dict.get("turnovers")),
                            "offensive_rebounds": _safe_int(
                                stat_dict.get("offensiveRebounds")
                            ),
                            "defensive_rebounds": _safe_int(
                                stat_dict.get("defensiveRebounds")
                            ),
                            "fouls": _safe_int(stat_dict.get("fouls")),
                            "plus_minus": stat_dict.get("plusMinus", ""),
                            "fg": stat_dict.get(
                                "fieldGoalsMade-fieldGoalsAttempted", ""
                            ),
                            "three_pt": stat_dict.get(
                                "threePointFieldGoalsMade-"
                                "threePointFieldGoalsAttempted",
                                "",
                            ),
                            "ft": stat_dict.get(
                                "freeThrowsMade-freeThrowsAttempted", ""
                            ),
                            "source": "espn",
                        })
            count += 1

        print(f"  {st}: parsed {count} games for {sport}")

    return rows


# ---------------------------------------------------------------------------
# ESPN plays helper (WNBA + NCAAB share the same JSON shape)
# ---------------------------------------------------------------------------

def _parse_espn_plays(
    raw_base: Path,
    sport: str,
    year: int,
    season_types: list[str],
) -> list[dict]:
    """Parse ESPN event game.json summary.plays into play rows."""
    rows: list[dict] = []
    for st in season_types:
        events_dir = raw_base / str(year) / "events" / st
        if not events_dir.is_dir():
            continue
        count = 0
        for root, _dirs, files in os.walk(events_dir):
            if "game.json" not in files:
                continue
            data = load_json(Path(root) / "game.json")
            if not data:
                continue
            event_id = str(data.get("eventId", ""))
            season_val = _safe_int(data.get("season"), year)

            game_date = ""
            header = data.get("header", {})
            comps = header.get("competitions", [])
            if comps:
                game_date = comps[0].get("date", "")[:10]

            plays = data.get("summary", {}).get("plays", [])
            if not plays:
                continue

            for play in plays:
                period = play.get("period", {})
                clock = play.get("clock", {})
                play_type = play.get("type", {})
                team = play.get("team", {})
                coord = play.get("coordinate", {})

                rows.append({
                    "game_id": event_id,
                    "season": season_val,
                    "date": game_date,
                    "season_type": st,
                    "play_id": str(play.get("id", "")),
                    "sequence_number": _safe_int(play.get("sequenceNumber")),
                    "play_type_id": str(play_type.get("id", "")),
                    "play_type": play_type.get("text", ""),
                    "text": play.get("text", ""),
                    "short_text": play.get("shortDescription", ""),
                    "period": _safe_int(period.get("number")),
                    "period_display": period.get("displayValue", ""),
                    "clock": clock.get("displayValue", ""),
                    "away_score": _safe_int(play.get("awayScore")),
                    "home_score": _safe_int(play.get("homeScore")),
                    "scoring_play": play.get("scoringPlay", False),
                    "score_value": _safe_int(play.get("scoreValue"), 0),
                    "team_id": str(team.get("id", "")),
                    "x_coord": _safe_float(coord.get("x")) if coord else None,
                    "y_coord": _safe_float(coord.get("y")) if coord else None,
                    "source": "espn",
                })
            count += 1

        print(f"  {st}: parsed plays from {count} games for {sport}")

    return rows


# ---------------------------------------------------------------------------
# Derive team_stats from player_stats (WNBA + NCAAB basketball pattern)
# ---------------------------------------------------------------------------

def _derive_basketball_team_stats(
    sport: str,
    stat_cols: list[str],
) -> None:
    print(f"\n── {sport.upper()} team_stats ──")
    ps = read_player_stats_parquets(sport, DEV_YEAR)
    if not ps:
        return

    teams: dict[str, list[dict]] = {}
    for r in ps:
        tid = r.get("team_id", "")
        if not tid:
            continue
        teams.setdefault(tid, []).append(r)

    rows: list[dict] = []
    for tid, players in teams.items():
        game_ids = {r.get("game_id") for r in players if r.get("game_id")}
        agg: dict[str, float] = {}
        for col in stat_cols:
            vals = [_safe_float(r.get(col), 0.0) for r in players if r.get(col) is not None]
            agg[col] = sum(vals)

        rows.append({
            "team_id": tid,
            "season": DEV_YEAR,
            "games_played": len(game_ids),
            **agg,
            "source": "derived",
        })

    if rows:
        write_parquet_rows(rows, sport, "team_stats", season_year=DEV_YEAR)
    else:
        print("  No team_stats rows derived")


# ---------------------------------------------------------------------------
# Derive leaders from player_stats (WNBA + NCAAB basketball pattern)
# ---------------------------------------------------------------------------

def _derive_basketball_leaders(
    sport: str,
    stat_cols: list[str],
) -> None:
    print(f"\n── {sport.upper()} leaders ──")
    ps = read_player_stats_parquets(sport, DEV_YEAR)
    if not ps:
        return

    player_agg: dict[str, dict] = {}
    for r in ps:
        pid = r.get("player_id", "")
        if not pid:
            continue
        if pid not in player_agg:
            player_agg[pid] = {
                "player_id": pid,
                "player_name": r.get("player_name", ""),
                "team_id": r.get("team_id", ""),
                "position": r.get("position", ""),
                "games_played": 0,
            }
            for col in stat_cols:
                player_agg[pid][col] = 0.0
        pa_row = player_agg[pid]
        pa_row["games_played"] += 1
        for col in stat_cols:
            pa_row[col] += _safe_float(r.get(col), 0.0)

    players = list(player_agg.values())
    rows: list[dict] = []
    for cat in stat_cols:
        ranked = sorted(players, key=lambda p: p.get(cat, 0), reverse=True)[:25]
        for rank, p in enumerate(ranked, 1):
            rows.append({
                "season": DEV_YEAR,
                "category": cat,
                "rank": rank,
                "player_id": p["player_id"],
                "player_name": p["player_name"],
                "team_id": p["team_id"],
                "position": p.get("position", ""),
                "value": p.get(cat, 0),
                "games_played": p["games_played"],
                "source": "derived",
            })

    if rows:
        write_parquet_rows(rows, sport, "leaders", season_year=DEV_YEAR)
    else:
        print("  No leaders rows derived")


# ---------------------------------------------------------------------------
# WNBA entities
# ---------------------------------------------------------------------------

WNBA_STAT_COLS = ["pts", "reb", "ast", "stl", "blk", "to", "fgm", "fga",
                   "ftm", "fta", "three_m", "three_a", "oreb", "dreb", "pf"]

def fill_wnba_box_scores() -> None:
    print("\n── WNBA box_scores ──")
    rows = _parse_espn_box_scores(
        RAW_ESPN_WNBA, "wnba", DEV_YEAR, ESPN_SEASON_TYPES,
    )
    if rows:
        write_parquet_rows(rows, "wnba", "box_scores", season_year=DEV_YEAR)
    else:
        print("  No box_score rows extracted")


def fill_wnba_plays() -> None:
    print("\n── WNBA plays ──")
    rows = _parse_espn_plays(
        RAW_ESPN_WNBA, "wnba", DEV_YEAR, ESPN_SEASON_TYPES,
    )
    if rows:
        write_parquet_rows(rows, "wnba", "plays", season_year=DEV_YEAR)
    else:
        print("  No play rows extracted")


def fill_wnba_team_stats() -> None:
    _derive_basketball_team_stats("wnba", WNBA_STAT_COLS)


def fill_wnba_leaders() -> None:
    _derive_basketball_leaders("wnba", WNBA_STAT_COLS)


def fill_wnba_odds() -> None:
    print("\n── WNBA odds (no raw data) ──")
    write_empty_parquet("wnba", "odds", {
        "game_id": pa.string(),
        "season": pa.int32(),
        "date": pa.string(),
        "team_id": pa.string(),
        "team_name": pa.string(),
        "spread": pa.float64(),
        "over_under": pa.float64(),
        "moneyline": pa.int32(),
        "provider": pa.string(),
        "source": pa.string(),
    }, season_year=DEV_YEAR)


def fill_wnba_player_props() -> None:
    print("\n── WNBA player_props (no raw data) ──")
    write_empty_parquet("wnba", "player_props", {
        "game_id": pa.string(),
        "season": pa.int32(),
        "date": pa.string(),
        "player_id": pa.string(),
        "player_name": pa.string(),
        "prop_type": pa.string(),
        "line": pa.float64(),
        "over_odds": pa.int32(),
        "under_odds": pa.int32(),
        "provider": pa.string(),
        "source": pa.string(),
    }, season_year=DEV_YEAR)


def fill_wnba_advanced() -> None:
    print("\n── WNBA advanced (no raw data) ──")
    write_empty_parquet("wnba", "advanced", {
        "player_id": pa.string(),
        "player_name": pa.string(),
        "team_id": pa.string(),
        "season": pa.int32(),
        "games_played": pa.int32(),
        "minutes": pa.float64(),
        "off_rating": pa.float64(),
        "def_rating": pa.float64(),
        "net_rating": pa.float64(),
        "ts_pct": pa.float64(),
        "efg_pct": pa.float64(),
        "usg_pct": pa.float64(),
        "pace": pa.float64(),
        "source": pa.string(),
    }, season_year=DEV_YEAR)


# ---------------------------------------------------------------------------
# NCAAB entities
# ---------------------------------------------------------------------------

NCAAB_STAT_COLS = ["pts", "reb", "ast", "stl", "blk", "to", "fgm", "fga",
                    "ftm", "fta", "three_m", "three_a", "oreb", "dreb", "pf"]

def fill_ncaab_rankings() -> None:
    print("\n── NCAAB rankings ──")
    rpath = RAW_ESPN_NCAAB / str(DEV_YEAR) / "reference" / "rankings" / "rankings.json"
    if not rpath.is_file():
        print(f"  Rankings file not found: {rpath}")
        write_empty_parquet("ncaab", "rankings", {
            "season": pa.int32(),
            "poll_name": pa.string(),
            "rank": pa.int32(),
            "team_id": pa.string(),
            "team_name": pa.string(),
            "team_abbreviation": pa.string(),
            "points": pa.int32(),
            "first_place_votes": pa.int32(),
            "previous_rank": pa.int32(),
            "trend": pa.string(),
            "source": pa.string(),
        }, season_year=DEV_YEAR)
        return

    data = load_json(rpath)
    if not data:
        return

    rows: list[dict] = []
    rankings_obj = data.get("rankings", {})
    polls = rankings_obj.get("rankings", [])

    for poll in polls:
        poll_name = poll.get("name", "")
        for entry in poll.get("ranks", []):
            team = entry.get("team", {})
            rows.append({
                "season": DEV_YEAR,
                "poll_name": poll_name,
                "poll_week": poll.get("occurrence", {}).get("value", ""),
                "rank": _safe_int(entry.get("current")),
                "previous_rank": _safe_int(entry.get("previous")),
                "points": _safe_int(entry.get("points")),
                "first_place_votes": _safe_int(entry.get("firstPlaceVotes"), 0),
                "trend": entry.get("trend", ""),
                "team_id": str(team.get("id", "")),
                "team_name": team.get("nickname", "") or team.get("name", ""),
                "team_location": team.get("location", ""),
                "team_abbreviation": team.get("abbreviation", ""),
                "source": "espn",
            })

    if rows:
        write_parquet_rows(rows, "ncaab", "rankings", season_year=DEV_YEAR)
    else:
        print("  No ranking rows extracted")


def fill_ncaab_injuries() -> None:
    print("\n── NCAAB injuries ──")
    injuries_dir = RAW_ESPN_NCAAB / str(DEV_YEAR) / "snapshots" / "injuries"
    if not injuries_dir.is_dir():
        print(f"  Injuries dir not found: {injuries_dir}")
        write_empty_parquet("ncaab", "injuries", {
            "season": pa.int32(),
            "date": pa.string(),
            "player_id": pa.string(),
            "player_name": pa.string(),
            "team_id": pa.string(),
            "team_name": pa.string(),
            "status": pa.string(),
            "injury_type": pa.string(),
            "detail": pa.string(),
            "source": pa.string(),
        }, season_year=DEV_YEAR)
        return

    rows: list[dict] = []
    for root, _dirs, files in os.walk(injuries_dir):
        for fname in sorted(files):
            if not fname.endswith(".json"):
                continue
            snap_date = fname.replace(".json", "")
            data = load_json(Path(root) / fname)
            if not data:
                continue

            injuries_wrap = data.get("injuries", {})
            injury_list = injuries_wrap.get("injuries", [])
            if not injury_list:
                # Sometimes injuries are at top level
                injury_list = injuries_wrap if isinstance(injuries_wrap, list) else []

            for entry in injury_list:
                # ESPN injuries can be team-grouped or flat
                team_info = entry.get("team", {})
                team_id = str(team_info.get("id", ""))
                team_name = team_info.get("displayName", "")

                for item in entry.get("injuries", []):
                    athlete = item.get("athlete", {})
                    rows.append({
                        "season": DEV_YEAR,
                        "date": snap_date,
                        "player_id": str(athlete.get("id", "")),
                        "player_name": athlete.get("displayName", ""),
                        "team_id": team_id,
                        "team_name": team_name,
                        "status": item.get("status", ""),
                        "injury_type": item.get("type", ""),
                        "detail": item.get("details", {}).get("detail", "")
                        if isinstance(item.get("details"), dict)
                        else str(item.get("details", "")),
                        "source": "espn",
                    })

    if rows:
        write_parquet_rows(rows, "ncaab", "injuries", season_year=DEV_YEAR)
    else:
        print("  No injury rows found — writing empty parquet")
        write_empty_parquet("ncaab", "injuries", {
            "season": pa.int32(),
            "date": pa.string(),
            "player_id": pa.string(),
            "player_name": pa.string(),
            "team_id": pa.string(),
            "team_name": pa.string(),
            "status": pa.string(),
            "injury_type": pa.string(),
            "detail": pa.string(),
            "source": pa.string(),
        }, season_year=DEV_YEAR)


def fill_ncaab_plays() -> None:
    print("\n── NCAAB plays ──")
    rows = _parse_espn_plays(
        RAW_ESPN_NCAAB, "ncaab", DEV_YEAR, ESPN_SEASON_TYPES,
    )
    if rows:
        write_parquet_rows(rows, "ncaab", "plays", season_year=DEV_YEAR)
    else:
        print("  No play rows extracted")


def fill_ncaab_team_stats() -> None:
    _derive_basketball_team_stats("ncaab", NCAAB_STAT_COLS)


def fill_ncaab_leaders() -> None:
    _derive_basketball_leaders("ncaab", NCAAB_STAT_COLS)


def fill_ncaab_bracket() -> None:
    print("\n── NCAAB bracket (no raw data) ──")
    write_empty_parquet("ncaab", "bracket", {
        "season": pa.int32(),
        "round": pa.int32(),
        "game_number": pa.int32(),
        "region": pa.string(),
        "seed_1": pa.int32(),
        "team_1_id": pa.string(),
        "team_1_name": pa.string(),
        "score_1": pa.int32(),
        "seed_2": pa.int32(),
        "team_2_id": pa.string(),
        "team_2_name": pa.string(),
        "score_2": pa.int32(),
        "winner_id": pa.string(),
        "source": pa.string(),
    }, season_year=DEV_YEAR)


def fill_ncaab_player_props() -> None:
    print("\n── NCAAB player_props (no raw data) ──")
    write_empty_parquet("ncaab", "player_props", {
        "game_id": pa.string(),
        "season": pa.int32(),
        "date": pa.string(),
        "player_id": pa.string(),
        "player_name": pa.string(),
        "prop_type": pa.string(),
        "line": pa.float64(),
        "over_odds": pa.int32(),
        "under_odds": pa.int32(),
        "provider": pa.string(),
        "source": pa.string(),
    }, season_year=DEV_YEAR)


def fill_ncaab_advanced() -> None:
    print("\n── NCAAB advanced (no raw data) ──")
    write_empty_parquet("ncaab", "advanced", {
        "player_id": pa.string(),
        "player_name": pa.string(),
        "team_id": pa.string(),
        "season": pa.int32(),
        "games_played": pa.int32(),
        "minutes": pa.float64(),
        "off_rating": pa.float64(),
        "def_rating": pa.float64(),
        "net_rating": pa.float64(),
        "ts_pct": pa.float64(),
        "efg_pct": pa.float64(),
        "usg_pct": pa.float64(),
        "pace": pa.float64(),
        "source": pa.string(),
    }, season_year=DEV_YEAR)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

NHL_HANDLERS: dict[str, callable] = {
    "box_scores": fill_nhl_box_scores,
    "plays": fill_nhl_plays,
    "team_stats": fill_nhl_team_stats,
    "leaders": fill_nhl_leaders,
    "draft": fill_nhl_draft,
}

WNBA_HANDLERS: dict[str, callable] = {
    "box_scores": fill_wnba_box_scores,
    "plays": fill_wnba_plays,
    "team_stats": fill_wnba_team_stats,
    "leaders": fill_wnba_leaders,
    "odds": fill_wnba_odds,
    "player_props": fill_wnba_player_props,
    "advanced": fill_wnba_advanced,
}

NCAAB_HANDLERS: dict[str, callable] = {
    "rankings": fill_ncaab_rankings,
    "injuries": fill_ncaab_injuries,
    "plays": fill_ncaab_plays,
    "team_stats": fill_ncaab_team_stats,
    "leaders": fill_ncaab_leaders,
    "bracket": fill_ncaab_bracket,
    "player_props": fill_ncaab_player_props,
    "advanced": fill_ncaab_advanced,
}

SPORT_HANDLERS: dict[str, dict[str, callable]] = {
    "nhl": NHL_HANDLERS,
    "wnba": WNBA_HANDLERS,
    "ncaab": NCAAB_HANDLERS,
}

ALL_SPORTS = ["nhl", "wnba", "ncaab"]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill empty curated entities for NHL, WNBA, NCAAB.",
    )
    parser.add_argument(
        "--sport",
        action="append",
        dest="sports",
        choices=ALL_SPORTS,
        help="Sport to process (repeatable). Omit to process all.",
    )
    parser.add_argument(
        "--entity",
        action="append",
        dest="entities",
        help="Entity to fill (repeatable). Omit to fill all for chosen sports.",
    )
    args = parser.parse_args()

    sports = args.sports or ALL_SPORTS
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Dev year: {DEV_YEAR}")
    print(f"Sports: {', '.join(sports)}")

    for sport in sports:
        handlers = SPORT_HANDLERS[sport]
        targets = args.entities or list(handlers.keys())
        # Filter to only valid entities for this sport
        valid = [e for e in targets if e in handlers]
        if not valid:
            print(f"\n  No matching entities for {sport.upper()}")
            continue

        print(f"\n{'=' * 60}")
        print(f"  {sport.upper()} — filling: {', '.join(valid)}")
        print(f"{'=' * 60}")

        for entity in valid:
            try:
                handlers[entity]()
            except Exception:
                print(f"\nERROR processing {sport}/{entity}:", file=sys.stderr)
                traceback.print_exc()

    print("\n✓ Done.")


if __name__ == "__main__":
    main()
