#!/usr/bin/env python3
"""
fill_all_gaps.py — Fill ALL remaining empty entities in normalized_curated from raw data.

Usage:
    python3 v5.0/scripts/fill_all_gaps.py

Writes parquet files using pyarrow to:
    v5.0/data/normalized_curated/{sport}/{entity}/[season={year}/]part.parquet
"""

import csv
import glob
import json
import os
import sys
import traceback
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Base paths
# ---------------------------------------------------------------------------
BASE = Path(__file__).resolve().parent.parent          # v5.0/
RAW = BASE / "data" / "raw"
CURATED = BASE / "data" / "normalized_curated"

SUMMARY: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _has_data(path: Path) -> bool:
    """Return True if *path* (a directory) already has a parquet with rows."""
    for pf in path.rglob("*.parquet"):
        try:
            meta = pq.read_metadata(str(pf))
            if meta.num_rows > 0:
                return True
        except Exception:
            pass
    return False


def _write(sport: str, entity: str, table: pa.Table, *, season: int | None = None):
    """Write a pyarrow Table to the curated directory. Skip if data exists."""
    if season is not None:
        out_dir = CURATED / sport / entity / f"season={season}"
    else:
        out_dir = CURATED / sport / entity
    if _has_data(out_dir):
        return
    os.makedirs(out_dir, exist_ok=True)
    pq.write_table(table, str(out_dir / "part.parquet"))
    n = len(table)
    tag = f"{sport}/{entity}" + (f"/season={season}" if season else "")
    print(f"  {tag}: {n} rows")
    SUMMARY[tag] = n


def _safe_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ===================================================================
# 1. SOCCER TRANSFERS (player-team snapshots)
# ===================================================================
SOCCER_LEAGUES = {
    "epl": "thesportsdb/epl",
    "laliga": "thesportsdb/laliga",
    "bundesliga": "thesportsdb/bundesliga",
    "seriea": "thesportsdb/seriea",
    "ligue1": "thesportsdb/ligue1",
    "mls": "thesportsdb/mls",
    "nwsl": "thesportsdb/nwsl",
    "ucl": "thesportsdb/ucl",
    "europa": "thesportsdb/europa",
    "ligamx": "thesportsdb/ligamx",
}


def fill_soccer_transfers():
    print("[Soccer Transfers]")
    for league, raw_prefix in SOCCER_LEAGUES.items():
        try:
            player_files = sorted(glob.glob(str(RAW / raw_prefix / "*/players_*.json")))
            if not player_files:
                continue
            rows: list[dict] = []
            for pf in player_files:
                data = _load_json(pf)
                season = data.get("season", os.path.basename(os.path.dirname(pf)))
                for p in data.get("players", []):
                    rows.append({
                        "player_id": str(p.get("id", "")),
                        "player_name": str(p.get("name", "")),
                        "team_id": str(p.get("team_id", "")),
                        "team_name": str(p.get("team", "")),
                        "position": str(p.get("position", "")),
                        "nationality": str(p.get("nationality", "")),
                        "status": str(p.get("status", "")),
                        "season": str(season),
                    })
            if rows:
                table = pa.table({
                    "player_id": pa.array([r["player_id"] for r in rows], pa.string()),
                    "player_name": pa.array([r["player_name"] for r in rows], pa.string()),
                    "team_id": pa.array([r["team_id"] for r in rows], pa.string()),
                    "team_name": pa.array([r["team_name"] for r in rows], pa.string()),
                    "position": pa.array([r["position"] for r in rows], pa.string()),
                    "nationality": pa.array([r["nationality"] for r in rows], pa.string()),
                    "status": pa.array([r["status"] for r in rows], pa.string()),
                    "season": pa.array([r["season"] for r in rows], pa.string()),
                })
                _write(league, "transfers", table)
        except Exception:
            traceback.print_exc()


# ===================================================================
# 2. F1
# ===================================================================
def _ergast_rounds(year: int = 2024):
    """Yield (round_num, round_dir) for each ergast round."""
    pattern = str(RAW / "ergast" / "f1" / str(year) / "rounds" / "round_*")
    for d in sorted(glob.glob(pattern)):
        rn = os.path.basename(d).replace("round_", "")
        yield _safe_int(rn), d


def fill_f1_session_results():
    rows: list[dict] = []
    for rnd, rdir in _ergast_rounds():
        rp = os.path.join(rdir, "results.json")
        if not os.path.exists(rp):
            continue
        try:
            data = _load_json(rp)
            races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
            if not races:
                continue
            race = races[0]
            for res in race.get("Results", []):
                drv = res.get("Driver", {})
                con = res.get("Constructor", {})
                time_obj = res.get("Time", {})
                rows.append({
                    "result_id": f"r{rnd}_{res.get('position', '0')}",
                    "session_id": f"race_{rnd}",
                    "driver_id": drv.get("driverId", ""),
                    "driver_name": f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                    "team_name": con.get("name", ""),
                    "position": _safe_int(res.get("position")),
                    "grid": _safe_int(res.get("grid")),
                    "points": _safe_float(res.get("points")),
                    "laps_completed": _safe_int(res.get("laps")),
                    "time_str": time_obj.get("time", "") if isinstance(time_obj, dict) else "",
                    "status": res.get("status", ""),
                    "round": rnd,
                })
        except Exception:
            traceback.print_exc()
    if rows:
        table = pa.table({
            "result_id": pa.array([r["result_id"] for r in rows], pa.string()),
            "session_id": pa.array([r["session_id"] for r in rows], pa.string()),
            "driver_id": pa.array([r["driver_id"] for r in rows], pa.string()),
            "driver_name": pa.array([r["driver_name"] for r in rows], pa.string()),
            "team_name": pa.array([r["team_name"] for r in rows], pa.string()),
            "position": pa.array([r["position"] for r in rows], pa.int32()),
            "grid": pa.array([r["grid"] for r in rows], pa.int32()),
            "points": pa.array([r["points"] for r in rows], pa.float64()),
            "laps_completed": pa.array([r["laps_completed"] for r in rows], pa.int32()),
            "time_str": pa.array([r["time_str"] for r in rows], pa.string()),
            "status": pa.array([r["status"] for r in rows], pa.string()),
            "round": pa.array([r["round"] for r in rows], pa.int32()),
        })
        _write("f1", "session_results", table, season=2024)


def fill_f1_qualifying():
    rows: list[dict] = []
    for rnd, rdir in _ergast_rounds():
        qp = os.path.join(rdir, "qualifying.json")
        if not os.path.exists(qp):
            continue
        try:
            data = _load_json(qp)
            races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
            if not races:
                continue
            race = races[0]
            for qr in race.get("QualifyingResults", []):
                drv = qr.get("Driver", {})
                con = qr.get("Constructor", {})
                rows.append({
                    "qualifying_id": f"q{rnd}_{qr.get('position', '0')}",
                    "round": rnd,
                    "driver_id": drv.get("driverId", ""),
                    "driver_name": f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                    "team_name": con.get("name", ""),
                    "position": _safe_int(qr.get("position")),
                    "q1_time": qr.get("Q1", ""),
                    "q2_time": qr.get("Q2", ""),
                    "q3_time": qr.get("Q3", ""),
                })
        except Exception:
            traceback.print_exc()
    if rows:
        table = pa.table({
            "qualifying_id": pa.array([r["qualifying_id"] for r in rows], pa.string()),
            "round": pa.array([r["round"] for r in rows], pa.int32()),
            "driver_id": pa.array([r["driver_id"] for r in rows], pa.string()),
            "driver_name": pa.array([r["driver_name"] for r in rows], pa.string()),
            "team_name": pa.array([r["team_name"] for r in rows], pa.string()),
            "position": pa.array([r["position"] for r in rows], pa.int32()),
            "q1_time": pa.array([r["q1_time"] for r in rows], pa.string()),
            "q2_time": pa.array([r["q2_time"] for r in rows], pa.string()),
            "q3_time": pa.array([r["q3_time"] for r in rows], pa.string()),
        })
        _write("f1", "qualifying", table, season=2024)


def fill_f1_lap_times():
    rows: list[dict] = []
    for rnd, rdir in _ergast_rounds():
        lp = os.path.join(rdir, "laps.json")
        if not os.path.exists(lp):
            continue
        try:
            data = _load_json(lp)
            races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
            if not races:
                continue
            race = races[0]
            for lap in race.get("Laps", []):
                lap_num = _safe_int(lap.get("number"))
                for timing in lap.get("Timings", []):
                    rows.append({
                        "lap_id": f"l{rnd}_{lap_num}_{timing.get('driverId', '')}",
                        "round": rnd,
                        "driver_id": timing.get("driverId", ""),
                        "lap_number": lap_num,
                        "position": _safe_int(timing.get("position")),
                        "time_str": timing.get("time", ""),
                    })
        except Exception:
            traceback.print_exc()
    if rows:
        table = pa.table({
            "lap_id": pa.array([r["lap_id"] for r in rows], pa.string()),
            "round": pa.array([r["round"] for r in rows], pa.int32()),
            "driver_id": pa.array([r["driver_id"] for r in rows], pa.string()),
            "lap_number": pa.array([r["lap_number"] for r in rows], pa.int32()),
            "position": pa.array([r["position"] for r in rows], pa.int32()),
            "time_str": pa.array([r["time_str"] for r in rows], pa.string()),
        })
        _write("f1", "lap_times", table, season=2024)


def fill_f1_pit_stops():
    rows: list[dict] = []
    for rnd, rdir in _ergast_rounds():
        pp = os.path.join(rdir, "pitstops.json")
        if not os.path.exists(pp):
            continue
        try:
            data = _load_json(pp)
            races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
            if not races:
                continue
            race = races[0]
            for ps in race.get("PitStops", []):
                rows.append({
                    "pit_stop_id": f"ps{rnd}_{ps.get('driverId', '')}_{ps.get('stop', '0')}",
                    "round": rnd,
                    "driver_id": ps.get("driverId", ""),
                    "stop_number": _safe_int(ps.get("stop")),
                    "lap": _safe_int(ps.get("lap")),
                    "time_of_day": ps.get("time", ""),
                    "duration": ps.get("duration", ""),
                })
        except Exception:
            traceback.print_exc()
    if rows:
        table = pa.table({
            "pit_stop_id": pa.array([r["pit_stop_id"] for r in rows], pa.string()),
            "round": pa.array([r["round"] for r in rows], pa.int32()),
            "driver_id": pa.array([r["driver_id"] for r in rows], pa.string()),
            "stop_number": pa.array([r["stop_number"] for r in rows], pa.int32()),
            "lap": pa.array([r["lap"] for r in rows], pa.int32()),
            "time_of_day": pa.array([r["time_of_day"] for r in rows], pa.string()),
            "duration": pa.array([r["duration"] for r in rows], pa.string()),
        })
        _write("f1", "pit_stops", table, season=2024)


def fill_f1_driver_standings():
    sp = RAW / "ergast" / "f1" / "2024" / "standings" / "driver_standings.json"
    if not sp.exists():
        return
    try:
        data = _load_json(sp)
        standings_lists = (
            data.get("MRData", {})
            .get("StandingsTable", {})
            .get("StandingsLists", [])
        )
        rows: list[dict] = []
        for sl in standings_lists:
            for ds in sl.get("DriverStandings", []):
                drv = ds.get("Driver", {})
                constructors = ds.get("Constructors", [])
                team = constructors[0].get("name", "") if constructors else ""
                rows.append({
                    "standing_id": f"ds_{drv.get('driverId', '')}",
                    "driver_id": drv.get("driverId", ""),
                    "driver_name": f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                    "team_name": team,
                    "position": _safe_int(ds.get("position")),
                    "points": _safe_float(ds.get("points")),
                    "wins": _safe_int(ds.get("wins")),
                })
        if rows:
            table = pa.table({
                "standing_id": pa.array([r["standing_id"] for r in rows], pa.string()),
                "driver_id": pa.array([r["driver_id"] for r in rows], pa.string()),
                "driver_name": pa.array([r["driver_name"] for r in rows], pa.string()),
                "team_name": pa.array([r["team_name"] for r in rows], pa.string()),
                "position": pa.array([r["position"] for r in rows], pa.int32()),
                "points": pa.array([r["points"] for r in rows], pa.float64()),
                "wins": pa.array([r["wins"] for r in rows], pa.int32()),
            })
            _write("f1", "driver_standings", table, season=2024)
    except Exception:
        traceback.print_exc()


def fill_f1_team_standings():
    sp = RAW / "ergast" / "f1" / "2024" / "standings" / "constructor_standings.json"
    if not sp.exists():
        return
    try:
        data = _load_json(sp)
        standings_lists = (
            data.get("MRData", {})
            .get("StandingsTable", {})
            .get("StandingsLists", [])
        )
        rows: list[dict] = []
        for sl in standings_lists:
            for cs in sl.get("ConstructorStandings", []):
                con = cs.get("Constructor", {})
                rows.append({
                    "standing_id": f"cs_{con.get('constructorId', '')}",
                    "team_id": con.get("constructorId", ""),
                    "team_name": con.get("name", ""),
                    "nationality": con.get("nationality", ""),
                    "position": _safe_int(cs.get("position")),
                    "points": _safe_float(cs.get("points")),
                    "wins": _safe_int(cs.get("wins")),
                })
        if rows:
            table = pa.table({
                "standing_id": pa.array([r["standing_id"] for r in rows], pa.string()),
                "team_id": pa.array([r["team_id"] for r in rows], pa.string()),
                "team_name": pa.array([r["team_name"] for r in rows], pa.string()),
                "nationality": pa.array([r["nationality"] for r in rows], pa.string()),
                "position": pa.array([r["position"] for r in rows], pa.int32()),
                "points": pa.array([r["points"] for r in rows], pa.float64()),
                "wins": pa.array([r["wins"] for r in rows], pa.int32()),
            })
            _write("f1", "team_standings", table, season=2024)
    except Exception:
        traceback.print_exc()


def fill_f1_events():
    """Build events from ergast race.json + openf1 meeting.json."""
    rows: list[dict] = []
    # Ergast rounds provide schedule detail
    for rnd, rdir in _ergast_rounds():
        rp = os.path.join(rdir, "race.json")
        if not os.path.exists(rp):
            # Fallback to results.json for raceName/circuit
            rp = os.path.join(rdir, "results.json")
            if not os.path.exists(rp):
                continue
            try:
                data = _load_json(rp)
                races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
                if not races:
                    continue
                race = races[0]
            except Exception:
                continue
        else:
            try:
                race = _load_json(rp)
            except Exception:
                continue

        circuit = race.get("Circuit", {})
        loc = circuit.get("Location", {})
        rows.append({
            "event_id": f"f1_2024_r{rnd}",
            "round": rnd,
            "name": race.get("raceName", ""),
            "date": race.get("date", ""),
            "time": race.get("time", ""),
            "circuit_id": circuit.get("circuitId", ""),
            "circuit_name": circuit.get("circuitName", ""),
            "locality": loc.get("locality", ""),
            "country": loc.get("country", ""),
        })

    # Enrich with openf1 meeting metadata if available
    meeting_files = sorted(glob.glob(
        str(RAW / "openf1" / "f1" / "2024" / "season_phases" / "championship"
            / "meetings" / "meeting_*" / "meeting.json")
    ))
    openf1_by_name: dict[str, dict] = {}
    for mf in meeting_files:
        try:
            md = _load_json(mf)
            openf1_by_name[md.get("meeting_name", "").lower()] = md
        except Exception:
            pass

    for row in rows:
        key = row["name"].lower().replace("grand prix", "grand prix")
        if key in openf1_by_name:
            md = openf1_by_name[key]
            row["meeting_key"] = str(md.get("meeting_key", ""))
            row["official_name"] = md.get("meeting_official_name", "")
        else:
            row["meeting_key"] = ""
            row["official_name"] = ""

    if rows:
        table = pa.table({
            "event_id": pa.array([r["event_id"] for r in rows], pa.string()),
            "round": pa.array([r["round"] for r in rows], pa.int32()),
            "name": pa.array([r["name"] for r in rows], pa.string()),
            "date": pa.array([r["date"] for r in rows], pa.string()),
            "time": pa.array([r["time"] for r in rows], pa.string()),
            "circuit_id": pa.array([r["circuit_id"] for r in rows], pa.string()),
            "circuit_name": pa.array([r["circuit_name"] for r in rows], pa.string()),
            "locality": pa.array([r["locality"] for r in rows], pa.string()),
            "country": pa.array([r["country"] for r in rows], pa.string()),
            "meeting_key": pa.array([r["meeting_key"] for r in rows], pa.string()),
            "official_name": pa.array([r["official_name"] for r in rows], pa.string()),
        })
        _write("f1", "events", table, season=2024)


def fill_f1_sessions():
    """Build sessions from openf1 session.json files."""
    session_files = sorted(glob.glob(
        str(RAW / "openf1" / "f1" / "2024" / "season_phases" / "championship"
            / "meetings" / "meeting_*" / "sessions" / "session_*" / "session.json")
    ))
    rows: list[dict] = []
    for sf in session_files:
        try:
            s = _load_json(sf)
            rows.append({
                "session_key": str(s.get("session_key", "")),
                "session_type": s.get("session_type", ""),
                "session_name": s.get("session_name", ""),
                "date_start": s.get("date_start", ""),
                "date_end": s.get("date_end", ""),
                "meeting_key": str(s.get("meeting_key", "")),
                "circuit_key": str(s.get("circuit_key", "")),
                "circuit_short_name": s.get("circuit_short_name", ""),
                "country_code": s.get("country_code", ""),
                "country_name": s.get("country_name", ""),
                "location": s.get("location", ""),
                "gmt_offset": s.get("gmt_offset", ""),
            })
        except Exception:
            continue
    if rows:
        table = pa.table({
            "session_key": pa.array([r["session_key"] for r in rows], pa.string()),
            "session_type": pa.array([r["session_type"] for r in rows], pa.string()),
            "session_name": pa.array([r["session_name"] for r in rows], pa.string()),
            "date_start": pa.array([r["date_start"] for r in rows], pa.string()),
            "date_end": pa.array([r["date_end"] for r in rows], pa.string()),
            "meeting_key": pa.array([r["meeting_key"] for r in rows], pa.string()),
            "circuit_key": pa.array([r["circuit_key"] for r in rows], pa.string()),
            "circuit_short_name": pa.array([r["circuit_short_name"] for r in rows], pa.string()),
            "country_code": pa.array([r["country_code"] for r in rows], pa.string()),
            "country_name": pa.array([r["country_name"] for r in rows], pa.string()),
            "location": pa.array([r["location"] for r in rows], pa.string()),
            "gmt_offset": pa.array([r["gmt_offset"] for r in rows], pa.string()),
        })
        _write("f1", "sessions", table, season=2024)


def fill_f1_openf1_extras():
    """Check openf1 session dirs for weather, race_control, position, etc."""
    session_dirs = sorted(glob.glob(
        str(RAW / "openf1" / "f1" / "2024" / "season_phases" / "championship"
            / "meetings" / "meeting_*" / "sessions" / "session_*")
    ))
    # weather
    weather_rows: list[dict] = []
    race_control_rows: list[dict] = []
    position_rows: list[dict] = []

    for sd in session_dirs:
        # Read session key
        sj = os.path.join(sd, "session.json")
        session_key = ""
        if os.path.exists(sj):
            try:
                sdata = _load_json(sj)
                session_key = str(sdata.get("session_key", ""))
            except Exception:
                pass

        for fname in os.listdir(sd):
            fpath = os.path.join(sd, fname)
            if not fname.endswith(".json") or fname == "session.json":
                continue
            try:
                content = _load_json(fpath)
            except Exception:
                continue

            base = fname.replace(".json", "")
            if base == "weather" and isinstance(content, list):
                for entry in content:
                    weather_rows.append({
                        "session_key": session_key,
                        "date": str(entry.get("date", "")),
                        "air_temperature": _safe_float(entry.get("air_temperature")),
                        "track_temperature": _safe_float(entry.get("track_temperature")),
                        "humidity": _safe_float(entry.get("humidity")),
                        "pressure": _safe_float(entry.get("pressure")),
                        "rainfall": str(entry.get("rainfall", "")),
                        "wind_direction": _safe_int(entry.get("wind_direction")),
                        "wind_speed": _safe_float(entry.get("wind_speed")),
                    })
            elif base == "race_control" and isinstance(content, list):
                for entry in content:
                    race_control_rows.append({
                        "session_key": session_key,
                        "date": str(entry.get("date", "")),
                        "category": str(entry.get("category", "")),
                        "flag": str(entry.get("flag", "")),
                        "message": str(entry.get("message", "")),
                        "scope": str(entry.get("scope", "")),
                        "driver_number": str(entry.get("driver_number", "")),
                        "lap_number": _safe_int(entry.get("lap_number")),
                    })
            elif base == "position" and isinstance(content, list):
                for entry in content:
                    position_rows.append({
                        "session_key": session_key,
                        "date": str(entry.get("date", "")),
                        "driver_number": str(entry.get("driver_number", "")),
                        "position": _safe_int(entry.get("position")),
                    })

    if weather_rows:
        table = pa.table({
            "session_key": pa.array([r["session_key"] for r in weather_rows], pa.string()),
            "date": pa.array([r["date"] for r in weather_rows], pa.string()),
            "air_temperature": pa.array([r["air_temperature"] for r in weather_rows], pa.float64()),
            "track_temperature": pa.array([r["track_temperature"] for r in weather_rows], pa.float64()),
            "humidity": pa.array([r["humidity"] for r in weather_rows], pa.float64()),
            "pressure": pa.array([r["pressure"] for r in weather_rows], pa.float64()),
            "rainfall": pa.array([r["rainfall"] for r in weather_rows], pa.string()),
            "wind_direction": pa.array([r["wind_direction"] for r in weather_rows], pa.int32()),
            "wind_speed": pa.array([r["wind_speed"] for r in weather_rows], pa.float64()),
        })
        _write("f1", "weather", table, season=2024)

    if race_control_rows:
        table = pa.table({
            "session_key": pa.array([r["session_key"] for r in race_control_rows], pa.string()),
            "date": pa.array([r["date"] for r in race_control_rows], pa.string()),
            "category": pa.array([r["category"] for r in race_control_rows], pa.string()),
            "flag": pa.array([r["flag"] for r in race_control_rows], pa.string()),
            "message": pa.array([r["message"] for r in race_control_rows], pa.string()),
            "scope": pa.array([r["scope"] for r in race_control_rows], pa.string()),
            "driver_number": pa.array([r["driver_number"] for r in race_control_rows], pa.string()),
            "lap_number": pa.array([r["lap_number"] for r in race_control_rows], pa.int32()),
        })
        _write("f1", "race_control", table, season=2024)

    if position_rows:
        table = pa.table({
            "session_key": pa.array([r["session_key"] for r in position_rows], pa.string()),
            "date": pa.array([r["date"] for r in position_rows], pa.string()),
            "driver_number": pa.array([r["driver_number"] for r in position_rows], pa.string()),
            "position": pa.array([r["position"] for r in position_rows], pa.int32()),
        })
        _write("f1", "position_history", table, season=2024)


def fill_f1():
    print("[F1]")
    fill_f1_session_results()
    fill_f1_qualifying()
    fill_f1_lap_times()
    fill_f1_pit_stops()
    fill_f1_driver_standings()
    fill_f1_team_standings()
    fill_f1_events()
    fill_f1_sessions()
    fill_f1_openf1_extras()


# ===================================================================
# 3. IndyCar
# ===================================================================
def fill_indycar():
    print("[IndyCar]")
    game_files = sorted(glob.glob(
        str(RAW / "espn" / "indycar" / "2024" / "events" / "regular" / "**" / "game.json"),
        recursive=True,
    ))

    # --- events ---
    event_rows: list[dict] = []
    driver_set: dict[str, dict] = {}
    for gf in game_files:
        try:
            data = _load_json(gf)
            event_id = str(data.get("eventId", ""))
            season = data.get("season", 2024)
            # Get event name from scoreboard or summary.event
            ev = data.get("scoreboard", data.get("summary", {}).get("event", {}))
            name = ev.get("name", "")
            date = ev.get("date", "")
            event_rows.append({
                "event_id": event_id,
                "name": name,
                "date": date,
                "season": _safe_int(season),
            })

            # Extract competitors as drivers
            header = data.get("summary", {}).get("header", {})
            for comp in header.get("competitions", []):
                for c in comp.get("competitors", []):
                    ath = c.get("athlete", {})
                    did = c.get("id", "")
                    if did and did not in driver_set:
                        flag = ath.get("flag", {})
                        driver_set[did] = {
                            "id": did,
                            "display_name": ath.get("fullName", ath.get("displayName", "")),
                            "short_name": ath.get("shortName", ""),
                            "country": flag.get("alt", ""),
                        }
        except Exception:
            continue

    if event_rows:
        table = pa.table({
            "event_id": pa.array([r["event_id"] for r in event_rows], pa.string()),
            "name": pa.array([r["name"] for r in event_rows], pa.string()),
            "date": pa.array([r["date"] for r in event_rows], pa.string()),
            "season": pa.array([r["season"] for r in event_rows], pa.int32()),
        })
        _write("indycar", "events", table, season=2024)

    # --- drivers (static) ---
    if driver_set:
        drivers = list(driver_set.values())
        table = pa.table({
            "id": pa.array([d["id"] for d in drivers], pa.string()),
            "display_name": pa.array([d["display_name"] for d in drivers], pa.string()),
            "short_name": pa.array([d["short_name"] for d in drivers], pa.string()),
            "country": pa.array([d["country"] for d in drivers], pa.string()),
            "source": pa.array(["espn"] * len(drivers), pa.string()),
        })
        _write("indycar", "drivers", table)

    # --- teams (static) — from reference/teams.json if available ---
    teams_path = RAW / "espn" / "indycar" / "2024" / "reference" / "teams.json"
    if teams_path.exists():
        try:
            td = _load_json(teams_path)
            teams = td.get("teams", [])
            if teams:
                table = pa.table({
                    "id": pa.array([str(t.get("id", "")) for t in teams], pa.string()),
                    "name": pa.array([t.get("displayName", t.get("name", "")) for t in teams], pa.string()),
                    "abbreviation": pa.array([t.get("abbreviation", "") for t in teams], pa.string()),
                    "source": pa.array(["espn"] * len(teams), pa.string()),
                })
                _write("indycar", "teams", table)
        except Exception:
            pass

    # --- circuits (static) — ESPN IndyCar doesn't have venue in competitions ---
    # Build from event names (best effort)
    circuit_set: dict[str, dict] = {}
    for gf in game_files:
        try:
            data = _load_json(gf)
            ev = data.get("scoreboard", data.get("summary", {}).get("event", {}))
            name = ev.get("name", "")
            eid = str(data.get("eventId", ""))
            if name and name not in circuit_set:
                circuit_set[name] = {
                    "circuit_id": eid,
                    "name": name,
                    "source": "espn",
                }
        except Exception:
            continue
    if circuit_set:
        circuits = list(circuit_set.values())
        table = pa.table({
            "circuit_id": pa.array([c["circuit_id"] for c in circuits], pa.string()),
            "name": pa.array([c["name"] for c in circuits], pa.string()),
            "source": pa.array([c["source"] for c in circuits], pa.string()),
        })
        _write("indycar", "circuits", table)


# ===================================================================
# 4. Golf & LPGA
# ===================================================================
def _fill_golf_sport(sport: str):
    """Fill tournaments, tournament_results, tournament_fields, courses, round_results for golf/lpga."""
    game_files = sorted(glob.glob(
        str(RAW / "espn" / sport / "2024" / "events" / "regular" / "**" / "game.json"),
        recursive=True,
    ))
    # Deduplicate by eventId (some events span multiple dates)
    events_by_id: dict[str, str] = {}
    for gf in game_files:
        try:
            data = _load_json(gf)
            eid = str(data.get("eventId", ""))
            if eid and eid not in events_by_id:
                events_by_id[eid] = gf
        except Exception:
            continue

    tournament_rows: list[dict] = []
    result_rows: list[dict] = []
    field_rows: list[dict] = []
    course_set: dict[str, dict] = {}
    round_result_rows: list[dict] = []

    for eid, gf in events_by_id.items():
        try:
            data = _load_json(gf)
            # Tournament info from summary.event
            ev = data.get("summary", {}).get("event", {})
            if not ev:
                ev = data.get("scoreboard", {})
            t_name = ev.get("name", ev.get("shortName", ""))
            t_date = ev.get("date", "")
            t_end = ev.get("endDate", "")
            tournament_rows.append({
                "tournament_id": eid,
                "name": t_name,
                "date": t_date,
                "end_date": t_end,
                "season": 2024,
            })

            # Competitors from summary.header.competitions[0].competitors
            header = data.get("summary", {}).get("header", {})
            competitors = []
            for comp in header.get("competitions", []):
                competitors.extend(comp.get("competitors", []))

            for idx, c in enumerate(competitors):
                ath = c.get("athlete", {})
                pid = c.get("id", str(idx))
                pname = ath.get("fullName", ath.get("displayName", ""))
                score = c.get("score", "")
                flag = ath.get("flag", {})

                result_rows.append({
                    "result_id": f"{eid}_{pid}",
                    "tournament_id": eid,
                    "player_id": pid,
                    "player_name": pname,
                    "country": flag.get("alt", ""),
                    "position": idx + 1,
                    "total_score": str(score),
                })

                field_rows.append({
                    "tournament_id": eid,
                    "player_id": pid,
                    "player_name": pname,
                })

                # Round results from linescores
                for ls in c.get("linescores", []):
                    period = _safe_int(ls.get("period"))
                    round_score = ls.get("value")
                    display = ls.get("displayValue", "")
                    if round_score is not None:
                        round_result_rows.append({
                            "result_id": f"{eid}_{pid}_R{period}",
                            "tournament_id": eid,
                            "player_id": pid,
                            "player_name": pname,
                            "round_number": period,
                            "score": _safe_int(round_score),
                            "to_par": display,
                        })

            # Course info from competitions venue if present
            for comp in header.get("competitions", []):
                venue = comp.get("venue", {})
                if venue:
                    vname = venue.get("fullName", venue.get("name", ""))
                    if vname and vname not in course_set:
                        course_set[vname] = {
                            "course_id": venue.get("id", vname),
                            "name": vname,
                            "source": "espn",
                        }
        except Exception:
            continue

    if tournament_rows:
        table = pa.table({
            "tournament_id": pa.array([r["tournament_id"] for r in tournament_rows], pa.string()),
            "name": pa.array([r["name"] for r in tournament_rows], pa.string()),
            "date": pa.array([r["date"] for r in tournament_rows], pa.string()),
            "end_date": pa.array([r["end_date"] for r in tournament_rows], pa.string()),
            "season": pa.array([r["season"] for r in tournament_rows], pa.int32()),
        })
        _write(sport, "tournaments", table, season=2024)

    if result_rows:
        table = pa.table({
            "result_id": pa.array([r["result_id"] for r in result_rows], pa.string()),
            "tournament_id": pa.array([r["tournament_id"] for r in result_rows], pa.string()),
            "player_id": pa.array([r["player_id"] for r in result_rows], pa.string()),
            "player_name": pa.array([r["player_name"] for r in result_rows], pa.string()),
            "country": pa.array([r["country"] for r in result_rows], pa.string()),
            "position": pa.array([r["position"] for r in result_rows], pa.int32()),
            "total_score": pa.array([r["total_score"] for r in result_rows], pa.string()),
        })
        _write(sport, "tournament_results", table, season=2024)

    if field_rows:
        table = pa.table({
            "tournament_id": pa.array([r["tournament_id"] for r in field_rows], pa.string()),
            "player_id": pa.array([r["player_id"] for r in field_rows], pa.string()),
            "player_name": pa.array([r["player_name"] for r in field_rows], pa.string()),
        })
        _write(sport, "tournament_fields", table, season=2024)

    if course_set:
        courses = list(course_set.values())
        table = pa.table({
            "course_id": pa.array([c["course_id"] for c in courses], pa.string()),
            "name": pa.array([c["name"] for c in courses], pa.string()),
            "source": pa.array([c["source"] for c in courses], pa.string()),
        })
        _write(sport, "courses", table)

    if round_result_rows:
        table = pa.table({
            "result_id": pa.array([r["result_id"] for r in round_result_rows], pa.string()),
            "tournament_id": pa.array([r["tournament_id"] for r in round_result_rows], pa.string()),
            "player_id": pa.array([r["player_id"] for r in round_result_rows], pa.string()),
            "player_name": pa.array([r["player_name"] for r in round_result_rows], pa.string()),
            "round_number": pa.array([r["round_number"] for r in round_result_rows], pa.int32()),
            "score": pa.array([r["score"] for r in round_result_rows], pa.int32()),
            "to_par": pa.array([r["to_par"] for r in round_result_rows], pa.string()),
        })
        _write(sport, "round_results", table, season=2024)


def fill_golf():
    print("[Golf]")
    _fill_golf_sport("golf")
    print("[LPGA]")
    _fill_golf_sport("lpga")


# ===================================================================
# 5. Dota2
# ===================================================================
def fill_dota2():
    print("[Dota2]")

    # --- items (static) — from opendota constants ---
    items_path = None
    for yr in ["2024", "2023", "2022"]:
        candidate = RAW / "opendota" / "dota2" / yr / "reference" / "constants" / "items.json"
        if candidate.exists():
            items_path = candidate
            break
    if items_path:
        try:
            data = _load_json(items_path)
            rows = []
            for key, val in data.items():
                rows.append({
                    "item_key": key,
                    "item_id": _safe_int(val.get("id")),
                    "cost": _safe_int(val.get("cost")),
                    "source": "opendota",
                })
            if rows:
                table = pa.table({
                    "item_key": pa.array([r["item_key"] for r in rows], pa.string()),
                    "item_id": pa.array([r["item_id"] for r in rows], pa.int32()),
                    "cost": pa.array([r["cost"] for r in rows], pa.int32()),
                    "source": pa.array([r["source"] for r in rows], pa.string()),
                })
                _write("dota2", "items", table)
        except Exception:
            traceback.print_exc()

    # --- hero_stats (season=2024) ---
    hs_path = RAW / "opendota" / "dota2" / "2024" / "reference" / "hero_stats.json"
    if hs_path.exists():
        try:
            data = _load_json(hs_path)
            rows = []
            for h in data:
                rows.append({
                    "hero_id": _safe_int(h.get("id")),
                    "name": h.get("name", ""),
                    "localized_name": h.get("localized_name", ""),
                    "primary_attr": h.get("primary_attr", ""),
                    "attack_type": h.get("attack_type", ""),
                    "base_health": _safe_int(h.get("base_health")),
                    "base_mana": _safe_int(h.get("base_mana")),
                    "base_armor": _safe_float(h.get("base_armor")),
                    "base_str": _safe_int(h.get("base_str")),
                    "base_agi": _safe_int(h.get("base_agi")),
                    "base_int": _safe_int(h.get("base_int")),
                    "move_speed": _safe_int(h.get("move_speed")),
                    "attack_range": _safe_int(h.get("attack_range")),
                    "pro_win": _safe_int(h.get("pro_win")),
                    "pro_pick": _safe_int(h.get("pro_pick")),
                    "pro_ban": _safe_int(h.get("pro_ban")),
                    "pub_win": _safe_int(h.get("pub_win")),
                    "pub_pick": _safe_int(h.get("pub_pick")),
                })
            if rows:
                table = pa.table({
                    "hero_id": pa.array([r["hero_id"] for r in rows], pa.int32()),
                    "name": pa.array([r["name"] for r in rows], pa.string()),
                    "localized_name": pa.array([r["localized_name"] for r in rows], pa.string()),
                    "primary_attr": pa.array([r["primary_attr"] for r in rows], pa.string()),
                    "attack_type": pa.array([r["attack_type"] for r in rows], pa.string()),
                    "base_health": pa.array([r["base_health"] for r in rows], pa.int32()),
                    "base_mana": pa.array([r["base_mana"] for r in rows], pa.int32()),
                    "base_armor": pa.array([r["base_armor"] for r in rows], pa.float64()),
                    "base_str": pa.array([r["base_str"] for r in rows], pa.int32()),
                    "base_agi": pa.array([r["base_agi"] for r in rows], pa.int32()),
                    "base_int": pa.array([r["base_int"] for r in rows], pa.int32()),
                    "move_speed": pa.array([r["move_speed"] for r in rows], pa.int32()),
                    "attack_range": pa.array([r["attack_range"] for r in rows], pa.int32()),
                    "pro_win": pa.array([r["pro_win"] for r in rows], pa.int32()),
                    "pro_pick": pa.array([r["pro_pick"] for r in rows], pa.int32()),
                    "pro_ban": pa.array([r["pro_ban"] for r in rows], pa.int32()),
                    "pub_win": pa.array([r["pub_win"] for r in rows], pa.int32()),
                    "pub_pick": pa.array([r["pub_pick"] for r in rows], pa.int32()),
                })
                _write("dota2", "hero_stats", table, season=2024)
        except Exception:
            traceback.print_exc()

    # --- tournaments (season=2024) from pandascore ---
    tp = RAW / "pandascore" / "dota2" / "2024" / "tournaments.json"
    if tp.exists():
        try:
            data = _load_json(tp)
            rows = []
            for t in data:
                rows.append({
                    "tournament_id": str(t.get("id", "")),
                    "name": t.get("name", ""),
                    "slug": t.get("slug", ""),
                    "tier": t.get("tier", ""),
                    "region": t.get("region", ""),
                    "prizepool": t.get("prizepool", ""),
                    "begin_at": t.get("begin_at", ""),
                    "end_at": t.get("end_at", ""),
                    "league_id": str(t.get("league_id", "")),
                    "serie_id": str(t.get("serie_id", "")),
                    "num_teams": len(t["teams"]) if isinstance(t.get("teams"), list) else _safe_int(t.get("teams")),
                })
            if rows:
                table = pa.table({
                    "tournament_id": pa.array([r["tournament_id"] for r in rows], pa.string()),
                    "name": pa.array([r["name"] for r in rows], pa.string()),
                    "slug": pa.array([r["slug"] for r in rows], pa.string()),
                    "tier": pa.array([r["tier"] for r in rows], pa.string()),
                    "region": pa.array([r["region"] for r in rows], pa.string()),
                    "prizepool": pa.array([r["prizepool"] for r in rows], pa.string()),
                    "begin_at": pa.array([r["begin_at"] for r in rows], pa.string()),
                    "end_at": pa.array([r["end_at"] for r in rows], pa.string()),
                    "league_id": pa.array([r["league_id"] for r in rows], pa.string()),
                    "serie_id": pa.array([r["serie_id"] for r in rows], pa.string()),
                    "num_teams": pa.array([r["num_teams"] for r in rows], pa.int32()),
                })
                _write("dota2", "tournaments", table, season=2024)
        except Exception:
            traceback.print_exc()

    # --- tournament_teams (season=2024) ---
    if tp.exists():
        try:
            data = _load_json(tp)
            rows = []
            for t in data:
                tid = str(t.get("id", ""))
                teams = t.get("teams", [])
                if not isinstance(teams, list):
                    continue
                for team in teams:
                    rows.append({
                        "tournament_id": tid,
                        "team_id": str(team.get("id", "")),
                        "team_name": team.get("name", ""),
                        "team_slug": team.get("slug", ""),
                        "location": team.get("location", ""),
                        "acronym": team.get("acronym", ""),
                    })
            if rows:
                table = pa.table({
                    "tournament_id": pa.array([r["tournament_id"] for r in rows], pa.string()),
                    "team_id": pa.array([r["team_id"] for r in rows], pa.string()),
                    "team_name": pa.array([r["team_name"] for r in rows], pa.string()),
                    "team_slug": pa.array([r["team_slug"] for r in rows], pa.string()),
                    "location": pa.array([r["location"] for r in rows], pa.string()),
                    "acronym": pa.array([r["acronym"] for r in rows], pa.string()),
                })
                _write("dota2", "tournament_teams", table, season=2024)
        except Exception:
            traceback.print_exc()

    # --- matches (season=2024) from pandascore ---
    mp = RAW / "pandascore" / "dota2" / "2024" / "matches.json"
    if mp.exists():
        try:
            data = _load_json(mp)
            rows = []
            for m in data:
                opps = m.get("opponents", [])
                team1 = ""
                team2 = ""
                if isinstance(opps, list) and len(opps) >= 1:
                    team1 = opps[0].get("opponent", {}).get("name", "") if isinstance(opps[0], dict) else ""
                if isinstance(opps, list) and len(opps) >= 2:
                    team2 = opps[1].get("opponent", {}).get("name", "") if isinstance(opps[1], dict) else ""
                rows.append({
                    "match_id": str(m.get("id", "")),
                    "name": m.get("name", ""),
                    "tournament_id": str(m.get("tournament_id", "")),
                    "serie_id": str(m.get("serie_id", "")),
                    "league_id": str(m.get("league_id", "")),
                    "status": m.get("status", ""),
                    "match_type": m.get("match_type", ""),
                    "number_of_games": _safe_int(m.get("number_of_games")),
                    "team1": team1,
                    "team2": team2,
                    "winner_id": str(m.get("winner_id", "")),
                    "begin_at": m.get("begin_at", ""),
                    "end_at": m.get("end_at", ""),
                    "detailed_stats": str(m.get("detailed_stats", "")),
                })
            if rows:
                table = pa.table({
                    "match_id": pa.array([r["match_id"] for r in rows], pa.string()),
                    "name": pa.array([r["name"] for r in rows], pa.string()),
                    "tournament_id": pa.array([r["tournament_id"] for r in rows], pa.string()),
                    "serie_id": pa.array([r["serie_id"] for r in rows], pa.string()),
                    "league_id": pa.array([r["league_id"] for r in rows], pa.string()),
                    "status": pa.array([r["status"] for r in rows], pa.string()),
                    "match_type": pa.array([r["match_type"] for r in rows], pa.string()),
                    "number_of_games": pa.array([r["number_of_games"] for r in rows], pa.int32()),
                    "team1": pa.array([r["team1"] for r in rows], pa.string()),
                    "team2": pa.array([r["team2"] for r in rows], pa.string()),
                    "winner_id": pa.array([r["winner_id"] for r in rows], pa.string()),
                    "begin_at": pa.array([r["begin_at"] for r in rows], pa.string()),
                    "end_at": pa.array([r["end_at"] for r in rows], pa.string()),
                    "detailed_stats": pa.array([r["detailed_stats"] for r in rows], pa.string()),
                })
                _write("dota2", "matches", table, season=2024)
        except Exception:
            traceback.print_exc()

    # --- match_maps (season=2024) — individual games within matches ---
    if mp.exists():
        try:
            data = _load_json(mp)
            rows = []
            for m in data:
                mid = str(m.get("id", ""))
                for g in m.get("games", []):
                    winner = g.get("winner", {})
                    rows.append({
                        "game_id": str(g.get("id", "")),
                        "match_id": mid,
                        "position": _safe_int(g.get("position")),
                        "status": g.get("status", ""),
                        "length_seconds": _safe_int(g.get("length")),
                        "winner_id": str(winner.get("id", "")) if isinstance(winner, dict) else "",
                        "begin_at": g.get("begin_at", ""),
                        "end_at": g.get("end_at", ""),
                        "forfeit": str(g.get("forfeit", "")),
                    })
            if rows:
                table = pa.table({
                    "game_id": pa.array([r["game_id"] for r in rows], pa.string()),
                    "match_id": pa.array([r["match_id"] for r in rows], pa.string()),
                    "position": pa.array([r["position"] for r in rows], pa.int32()),
                    "status": pa.array([r["status"] for r in rows], pa.string()),
                    "length_seconds": pa.array([r["length_seconds"] for r in rows], pa.int32()),
                    "winner_id": pa.array([r["winner_id"] for r in rows], pa.string()),
                    "begin_at": pa.array([r["begin_at"] for r in rows], pa.string()),
                    "end_at": pa.array([r["end_at"] for r in rows], pa.string()),
                    "forfeit": pa.array([r["forfeit"] for r in rows], pa.string()),
                })
                _write("dota2", "match_maps", table, season=2024)
        except Exception:
            traceback.print_exc()

    # --- player_stats (season=2024) from opendota pro_players ---
    pp = RAW / "opendota" / "dota2" / "2024" / "reference" / "pro_players.json"
    if pp.exists():
        try:
            data = _load_json(pp)
            rows = []
            for p in data:
                rows.append({
                    "account_id": str(p.get("account_id", "")),
                    "name": p.get("name", ""),
                    "persona_name": p.get("personaname", ""),
                    "country_code": p.get("country_code", p.get("loccountrycode", "")),
                    "team_id": str(p.get("team_id", "")),
                    "team_name": p.get("team_name", ""),
                    "team_tag": p.get("team_tag", ""),
                    "is_pro": str(p.get("is_pro", "")),
                    "fantasy_role": _safe_int(p.get("fantasy_role")),
                })
            if rows:
                table = pa.table({
                    "account_id": pa.array([r["account_id"] for r in rows], pa.string()),
                    "name": pa.array([r["name"] for r in rows], pa.string()),
                    "persona_name": pa.array([r["persona_name"] for r in rows], pa.string()),
                    "country_code": pa.array([r["country_code"] for r in rows], pa.string()),
                    "team_id": pa.array([r["team_id"] for r in rows], pa.string()),
                    "team_name": pa.array([r["team_name"] for r in rows], pa.string()),
                    "team_tag": pa.array([r["team_tag"] for r in rows], pa.string()),
                    "is_pro": pa.array([r["is_pro"] for r in rows], pa.string()),
                    "fantasy_role": pa.array([r["fantasy_role"] for r in rows], pa.int32()),
                })
                _write("dota2", "player_stats", table, season=2024)
        except Exception:
            traceback.print_exc()


# ===================================================================
# 6. Tennis (ATP & WTA)
# ===================================================================
def _fill_tennis_sport(sport: str):
    """Fill tournaments, matches, match_stats from TennisAbstract CSV + ESPN."""
    # --- tournaments (season=2024) from TennisAbstract ---
    csv_path = RAW / "tennisabstract" / sport / "2024" / "matches.csv"
    if not csv_path.exists():
        return

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)
    except Exception:
        traceback.print_exc()
        return

    # Tournaments
    tourney_set: dict[str, dict] = {}
    for r in all_rows:
        tid = r.get("tourney_id", "")
        if tid and tid not in tourney_set:
            tourney_set[tid] = {
                "tourney_id": tid,
                "tourney_name": r.get("tourney_name", ""),
                "surface": r.get("surface", ""),
                "draw_size": _safe_int(r.get("draw_size")),
                "tourney_level": r.get("tourney_level", ""),
                "tourney_date": r.get("tourney_date", ""),
            }
    if tourney_set:
        ts = list(tourney_set.values())
        table = pa.table({
            "tourney_id": pa.array([t["tourney_id"] for t in ts], pa.string()),
            "tourney_name": pa.array([t["tourney_name"] for t in ts], pa.string()),
            "surface": pa.array([t["surface"] for t in ts], pa.string()),
            "draw_size": pa.array([t["draw_size"] for t in ts], pa.int32()),
            "tourney_level": pa.array([t["tourney_level"] for t in ts], pa.string()),
            "tourney_date": pa.array([t["tourney_date"] for t in ts], pa.string()),
        })
        _write(sport, "tournaments", table, season=2024)

    # --- matches (season=2024) ---
    match_rows: list[dict] = []
    for r in all_rows:
        match_rows.append({
            "match_id": f"{r.get('tourney_id', '')}_{r.get('match_num', '')}",
            "tourney_id": r.get("tourney_id", ""),
            "tourney_name": r.get("tourney_name", ""),
            "surface": r.get("surface", ""),
            "round": r.get("round", ""),
            "winner_id": r.get("winner_id", ""),
            "winner_name": r.get("winner_name", ""),
            "winner_seed": r.get("winner_seed", ""),
            "winner_hand": r.get("winner_hand", ""),
            "winner_ioc": r.get("winner_ioc", ""),
            "winner_age": r.get("winner_age", ""),
            "loser_id": r.get("loser_id", ""),
            "loser_name": r.get("loser_name", ""),
            "loser_seed": r.get("loser_seed", ""),
            "loser_hand": r.get("loser_hand", ""),
            "loser_ioc": r.get("loser_ioc", ""),
            "loser_age": r.get("loser_age", ""),
            "score": r.get("score", ""),
            "best_of": r.get("best_of", ""),
            "minutes": r.get("minutes", ""),
        })
    if match_rows:
        table = pa.table({
            "match_id": pa.array([r["match_id"] for r in match_rows], pa.string()),
            "tourney_id": pa.array([r["tourney_id"] for r in match_rows], pa.string()),
            "tourney_name": pa.array([r["tourney_name"] for r in match_rows], pa.string()),
            "surface": pa.array([r["surface"] for r in match_rows], pa.string()),
            "round": pa.array([r["round"] for r in match_rows], pa.string()),
            "winner_id": pa.array([r["winner_id"] for r in match_rows], pa.string()),
            "winner_name": pa.array([r["winner_name"] for r in match_rows], pa.string()),
            "winner_seed": pa.array([r["winner_seed"] for r in match_rows], pa.string()),
            "winner_hand": pa.array([r["winner_hand"] for r in match_rows], pa.string()),
            "winner_ioc": pa.array([r["winner_ioc"] for r in match_rows], pa.string()),
            "winner_age": pa.array([r["winner_age"] for r in match_rows], pa.string()),
            "loser_id": pa.array([r["loser_id"] for r in match_rows], pa.string()),
            "loser_name": pa.array([r["loser_name"] for r in match_rows], pa.string()),
            "loser_seed": pa.array([r["loser_seed"] for r in match_rows], pa.string()),
            "loser_hand": pa.array([r["loser_hand"] for r in match_rows], pa.string()),
            "loser_ioc": pa.array([r["loser_ioc"] for r in match_rows], pa.string()),
            "loser_age": pa.array([r["loser_age"] for r in match_rows], pa.string()),
            "score": pa.array([r["score"] for r in match_rows], pa.string()),
            "best_of": pa.array([r["best_of"] for r in match_rows], pa.string()),
            "minutes": pa.array([r["minutes"] for r in match_rows], pa.string()),
        })
        _write(sport, "matches", table, season=2024)

    # --- match_stats (season=2024) ---
    stat_rows: list[dict] = []
    for r in all_rows:
        mid = f"{r.get('tourney_id', '')}_{r.get('match_num', '')}"
        stat_rows.append({
            "match_id": mid,
            "w_ace": _safe_int(r.get("w_ace")),
            "w_df": _safe_int(r.get("w_df")),
            "w_svpt": _safe_int(r.get("w_svpt")),
            "w_1stIn": _safe_int(r.get("w_1stIn")),
            "w_1stWon": _safe_int(r.get("w_1stWon")),
            "w_2ndWon": _safe_int(r.get("w_2ndWon")),
            "w_SvGms": _safe_int(r.get("w_SvGms")),
            "w_bpSaved": _safe_int(r.get("w_bpSaved")),
            "w_bpFaced": _safe_int(r.get("w_bpFaced")),
            "l_ace": _safe_int(r.get("l_ace")),
            "l_df": _safe_int(r.get("l_df")),
            "l_svpt": _safe_int(r.get("l_svpt")),
            "l_1stIn": _safe_int(r.get("l_1stIn")),
            "l_1stWon": _safe_int(r.get("l_1stWon")),
            "l_2ndWon": _safe_int(r.get("l_2ndWon")),
            "l_SvGms": _safe_int(r.get("l_SvGms")),
            "l_bpSaved": _safe_int(r.get("l_bpSaved")),
            "l_bpFaced": _safe_int(r.get("l_bpFaced")),
            "winner_rank": _safe_int(r.get("winner_rank")),
            "winner_rank_points": _safe_int(r.get("winner_rank_points")),
            "loser_rank": _safe_int(r.get("loser_rank")),
            "loser_rank_points": _safe_int(r.get("loser_rank_points")),
        })
    if stat_rows:
        table = pa.table({
            "match_id": pa.array([r["match_id"] for r in stat_rows], pa.string()),
            "w_ace": pa.array([r["w_ace"] for r in stat_rows], pa.int32()),
            "w_df": pa.array([r["w_df"] for r in stat_rows], pa.int32()),
            "w_svpt": pa.array([r["w_svpt"] for r in stat_rows], pa.int32()),
            "w_1stIn": pa.array([r["w_1stIn"] for r in stat_rows], pa.int32()),
            "w_1stWon": pa.array([r["w_1stWon"] for r in stat_rows], pa.int32()),
            "w_2ndWon": pa.array([r["w_2ndWon"] for r in stat_rows], pa.int32()),
            "w_SvGms": pa.array([r["w_SvGms"] for r in stat_rows], pa.int32()),
            "w_bpSaved": pa.array([r["w_bpSaved"] for r in stat_rows], pa.int32()),
            "w_bpFaced": pa.array([r["w_bpFaced"] for r in stat_rows], pa.int32()),
            "l_ace": pa.array([r["l_ace"] for r in stat_rows], pa.int32()),
            "l_df": pa.array([r["l_df"] for r in stat_rows], pa.int32()),
            "l_svpt": pa.array([r["l_svpt"] for r in stat_rows], pa.int32()),
            "l_1stIn": pa.array([r["l_1stIn"] for r in stat_rows], pa.int32()),
            "l_1stWon": pa.array([r["l_1stWon"] for r in stat_rows], pa.int32()),
            "l_2ndWon": pa.array([r["l_2ndWon"] for r in stat_rows], pa.int32()),
            "l_SvGms": pa.array([r["l_SvGms"] for r in stat_rows], pa.int32()),
            "l_bpSaved": pa.array([r["l_bpSaved"] for r in stat_rows], pa.int32()),
            "l_bpFaced": pa.array([r["l_bpFaced"] for r in stat_rows], pa.int32()),
            "winner_rank": pa.array([r["winner_rank"] for r in stat_rows], pa.int32()),
            "winner_rank_points": pa.array([r["winner_rank_points"] for r in stat_rows], pa.int32()),
            "loser_rank": pa.array([r["loser_rank"] for r in stat_rows], pa.int32()),
            "loser_rank_points": pa.array([r["loser_rank_points"] for r in stat_rows], pa.int32()),
        })
        _write(sport, "match_stats", table, season=2024)


def fill_tennis():
    print("[ATP]")
    _fill_tennis_sport("atp")
    print("[WTA]")
    _fill_tennis_sport("wta")


# ===================================================================
# 7. MLB (plays)
# ===================================================================
def fill_mlb():
    print("[MLB]")
    game_files = sorted(glob.glob(
        str(RAW / "espn" / "mlb" / "2024" / "events" / "regular" / "**" / "game.json"),
        recursive=True,
    ))

    play_rows: list[dict] = []
    for gf in game_files:
        try:
            data = _load_json(gf)
            event_id = str(data.get("eventId", ""))
            plays = data.get("summary", {}).get("plays", [])
            for p in plays:
                ptype = p.get("type", {})
                period = p.get("period", {})
                team = p.get("team", {})
                participants = p.get("participants", [])
                batter_id = ""
                batter_name = ""
                if participants:
                    ath = participants[0].get("athlete", {})
                    batter_id = str(ath.get("id", ""))
                    batter_name = ath.get("displayName", "")
                play_rows.append({
                    "play_id": str(p.get("id", "")),
                    "event_id": event_id,
                    "sequence": _safe_int(p.get("sequenceNumber")),
                    "type_id": str(ptype.get("id", "")),
                    "type_text": ptype.get("text", ""),
                    "type_type": ptype.get("type", ""),
                    "text": p.get("text", ""),
                    "inning": _safe_int(period.get("number")),
                    "inning_half": period.get("type", ""),
                    "away_score": _safe_int(p.get("awayScore")),
                    "home_score": _safe_int(p.get("homeScore")),
                    "scoring_play": str(p.get("scoringPlay", False)),
                    "score_value": _safe_int(p.get("scoreValue")),
                    "outs": _safe_int(p.get("outs")),
                    "team_id": str(team.get("id", "")),
                    "batter_id": batter_id,
                    "batter_name": batter_name,
                })
        except Exception:
            continue

    if play_rows:
        table = pa.table({
            "play_id": pa.array([r["play_id"] for r in play_rows], pa.string()),
            "event_id": pa.array([r["event_id"] for r in play_rows], pa.string()),
            "sequence": pa.array([r["sequence"] for r in play_rows], pa.int32()),
            "type_id": pa.array([r["type_id"] for r in play_rows], pa.string()),
            "type_text": pa.array([r["type_text"] for r in play_rows], pa.string()),
            "type_type": pa.array([r["type_type"] for r in play_rows], pa.string()),
            "text": pa.array([r["text"] for r in play_rows], pa.string()),
            "inning": pa.array([r["inning"] for r in play_rows], pa.int32()),
            "inning_half": pa.array([r["inning_half"] for r in play_rows], pa.string()),
            "away_score": pa.array([r["away_score"] for r in play_rows], pa.int32()),
            "home_score": pa.array([r["home_score"] for r in play_rows], pa.int32()),
            "scoring_play": pa.array([r["scoring_play"] for r in play_rows], pa.string()),
            "score_value": pa.array([r["score_value"] for r in play_rows], pa.int32()),
            "outs": pa.array([r["outs"] for r in play_rows], pa.int32()),
            "team_id": pa.array([r["team_id"] for r in play_rows], pa.string()),
            "batter_id": pa.array([r["batter_id"] for r in play_rows], pa.string()),
            "batter_name": pa.array([r["batter_name"] for r in play_rows], pa.string()),
        })
        _write("mlb", "plays", table, season=2024)


# ===================================================================
# 8. NCAAB
# ===================================================================
def fill_ncaab():
    print("[NCAAB]")

    # --- player_props (season=2024) from oddsapi ---
    # OddsAPI NCAAB props are in 2025/ and 2026/ dirs (season covers 2024-25)
    props_files = sorted(
        glob.glob(str(RAW / "oddsapi" / "ncaab" / "*/props/*.json"))
    )
    prop_rows: list[dict] = []
    for pf in props_files:
        try:
            data = _load_json(pf)
            event_id = data.get("eventId", "")
            home = data.get("homeTeam", "")
            away = data.get("awayTeam", "")
            commence = data.get("commenceTime", "")
            for bm in data.get("bookmakers", []):
                book_key = bm.get("key", "")
                book_title = bm.get("title", "")
                for mkt in bm.get("markets", []):
                    market_key = mkt.get("key", "")
                    for out in mkt.get("outcomes", []):
                        prop_rows.append({
                            "prop_id": f"{event_id}_{book_key}_{market_key}_{out.get('description','')}_{out.get('name','')}",
                            "event_id": str(event_id),
                            "home_team": home,
                            "away_team": away,
                            "commence_time": commence,
                            "bookmaker": book_title,
                            "market": market_key,
                            "outcome_name": out.get("name", ""),
                            "player_name": out.get("description", ""),
                            "price": _safe_int(out.get("price")),
                            "point": _safe_float(out.get("point")),
                        })
        except Exception:
            continue

    if prop_rows:
        table = pa.table({
            "prop_id": pa.array([r["prop_id"] for r in prop_rows], pa.string()),
            "event_id": pa.array([r["event_id"] for r in prop_rows], pa.string()),
            "home_team": pa.array([r["home_team"] for r in prop_rows], pa.string()),
            "away_team": pa.array([r["away_team"] for r in prop_rows], pa.string()),
            "commence_time": pa.array([r["commence_time"] for r in prop_rows], pa.string()),
            "bookmaker": pa.array([r["bookmaker"] for r in prop_rows], pa.string()),
            "market": pa.array([r["market"] for r in prop_rows], pa.string()),
            "outcome_name": pa.array([r["outcome_name"] for r in prop_rows], pa.string()),
            "player_name": pa.array([r["player_name"] for r in prop_rows], pa.string()),
            "price": pa.array([r["price"] for r in prop_rows], pa.int32()),
            "point": pa.array([r["point"] for r in prop_rows], pa.float64()),
        })
        _write("ncaab", "player_props", table, season=2024)

    # --- bracket (season=2024) from ESPN postseason ---
    post_files = sorted(glob.glob(
        str(RAW / "espn" / "ncaab" / "2024" / "events" / "postseason" / "**" / "game.json"),
        recursive=True,
    ))
    bracket_rows: list[dict] = []
    for gf in post_files:
        try:
            data = _load_json(gf)
            event_id = str(data.get("eventId", ""))
            header = data.get("summary", {}).get("header", {})
            for comp in header.get("competitions", []):
                date = comp.get("date", comp.get("startDate", ""))
                competitors = comp.get("competitors", [])
                home_team = ""
                away_team = ""
                home_score = ""
                away_score = ""
                for c in competitors:
                    team = c.get("team", {})
                    ha = c.get("homeAway", "")
                    name = team.get("displayName", team.get("name", ""))
                    score = c.get("score", "")
                    if ha == "home":
                        home_team = name
                        home_score = str(score)
                    else:
                        away_team = name
                        away_score = str(score)
                bracket_rows.append({
                    "event_id": event_id,
                    "date": date,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                })
        except Exception:
            continue

    if bracket_rows:
        table = pa.table({
            "event_id": pa.array([r["event_id"] for r in bracket_rows], pa.string()),
            "date": pa.array([r["date"] for r in bracket_rows], pa.string()),
            "home_team": pa.array([r["home_team"] for r in bracket_rows], pa.string()),
            "away_team": pa.array([r["away_team"] for r in bracket_rows], pa.string()),
            "home_score": pa.array([r["home_score"] for r in bracket_rows], pa.string()),
            "away_score": pa.array([r["away_score"] for r in bracket_rows], pa.string()),
        })
        _write("ncaab", "bracket", table, season=2024)

    # --- advanced (season=2024) — team stats from regular season boxscores ---
    reg_files = sorted(glob.glob(
        str(RAW / "espn" / "ncaab" / "2024" / "events" / "regular" / "**" / "game.json"),
        recursive=True,
    ))
    adv_rows: list[dict] = []
    for gf in reg_files:
        try:
            data = _load_json(gf)
            event_id = str(data.get("eventId", ""))
            boxscore = data.get("summary", {}).get("boxscore", {})
            # Player-level advanced stats
            for team_block in boxscore.get("players", []):
                team_info = team_block.get("team", {})
                team_name = team_info.get("displayName", "")
                team_id = str(team_info.get("id", ""))
                for stat_group in team_block.get("statistics", []):
                    names = stat_group.get("names", [])
                    keys = stat_group.get("keys", [])
                    for ath_entry in stat_group.get("athletes", []):
                        ath = ath_entry.get("athlete", {})
                        stats = ath_entry.get("stats", [])
                        row = {
                            "event_id": event_id,
                            "team_id": team_id,
                            "team_name": team_name,
                            "player_id": str(ath.get("id", "")),
                            "player_name": ath.get("displayName", ""),
                            "starter": str(ath_entry.get("starter", "")),
                        }
                        for i, val in enumerate(stats):
                            col = keys[i] if i < len(keys) else names[i] if i < len(names) else f"stat_{i}"
                            row[col] = val
                        adv_rows.append(row)
        except Exception:
            continue

    if adv_rows:
        # Collect all unique stat columns
        all_keys = set()
        base_keys = ["event_id", "team_id", "team_name", "player_id", "player_name", "starter"]
        for r in adv_rows:
            all_keys.update(r.keys())
        stat_keys = sorted(all_keys - set(base_keys))
        columns: dict[str, pa.Array] = {}
        for k in base_keys + stat_keys:
            columns[k] = pa.array([str(r.get(k, "")) for r in adv_rows], pa.string())
        table = pa.table(columns)
        _write("ncaab", "advanced", table, season=2024)


# ===================================================================
# 9. WNBA
# ===================================================================
def fill_wnba():
    print("[WNBA]")
    game_files = sorted(glob.glob(
        str(RAW / "espn" / "wnba" / "2024" / "events" / "regular" / "**" / "game.json"),
        recursive=True,
    ))

    adv_rows: list[dict] = []
    for gf in game_files:
        try:
            data = _load_json(gf)
            event_id = str(data.get("eventId", ""))
            boxscore = data.get("summary", {}).get("boxscore", {})
            for team_block in boxscore.get("players", []):
                team_info = team_block.get("team", {})
                team_name = team_info.get("displayName", "")
                team_id = str(team_info.get("id", ""))
                for stat_group in team_block.get("statistics", []):
                    names = stat_group.get("names", [])
                    keys = stat_group.get("keys", [])
                    for ath_entry in stat_group.get("athletes", []):
                        ath = ath_entry.get("athlete", {})
                        stats = ath_entry.get("stats", [])
                        row = {
                            "event_id": event_id,
                            "team_id": team_id,
                            "team_name": team_name,
                            "player_id": str(ath.get("id", "")),
                            "player_name": ath.get("displayName", ""),
                            "starter": str(ath_entry.get("starter", "")),
                        }
                        for i, val in enumerate(stats):
                            col = keys[i] if i < len(keys) else names[i] if i < len(names) else f"stat_{i}"
                            row[col] = val
                        adv_rows.append(row)
        except Exception:
            continue

    if adv_rows:
        base_keys = ["event_id", "team_id", "team_name", "player_id", "player_name", "starter"]
        all_keys = set()
        for r in adv_rows:
            all_keys.update(r.keys())
        stat_keys = sorted(all_keys - set(base_keys))
        columns: dict[str, pa.Array] = {}
        for k in base_keys + stat_keys:
            columns[k] = pa.array([str(r.get(k, "")) for r in adv_rows], pa.string())
        table = pa.table(columns)
        _write("wnba", "advanced", table, season=2024)


# ===================================================================
# Main
# ===================================================================
def main():
    print("=" * 60)
    print("fill_all_gaps.py — Filling empty normalized_curated entities")
    print("=" * 60)

    fill_soccer_transfers()
    fill_f1()
    fill_indycar()
    fill_golf()
    fill_dota2()
    fill_tennis()
    fill_mlb()
    fill_ncaab()
    fill_wnba()

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = 0
    for tag, n in sorted(SUMMARY.items()):
        print(f"  {tag}: {n} rows")
        total += n
    print(f"\nTotal entities written: {len(SUMMARY)}")
    print(f"Total rows written: {total:,}")


if __name__ == "__main__":
    main()
