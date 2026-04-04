#!/usr/bin/env python3
"""
fill_wave6_gaps.py
──────────────────
Populate ALL entities for wave-6 sport categories from raw data.
Sports: dota2, golf, lpga, atp, wta, f1, indycar
"""
import os, sys, json, hashlib
import glob as _glob
import pyarrow as pa
import pyarrow.parquet as pq

# ── paths ──────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
V5 = os.path.normpath(os.path.join(SCRIPT_DIR, ".."))
RAW = os.path.join(V5, "data", "raw")
OUT = os.path.join(V5, "data", "normalized_curated")

sys.path.insert(0, os.path.join(V5, "backend"))
from normalization.dota_schemas import DOTA_SCHEMAS
from normalization.golf_schemas import GOLF_SCHEMAS
from normalization.tennis_schemas import TENNIS_SCHEMAS
from normalization.racing_schemas import RACING_SCHEMAS

# ── helpers ────────────────────────────────────────────────────────────

def jload(path):
    """Load JSON, return None on failure."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  WARN could not load {path}: {e}")
        return None


def sint(v):
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def sfloat(v):
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def sstr(v):
    return str(v) if v is not None else None


def hid(s):
    """Deterministic int32-safe hash from a string key."""
    return int(hashlib.sha256(str(s).encode()).hexdigest()[:7], 16)


def existing_rows(path):
    if not os.path.exists(path):
        return -1
    try:
        return pq.read_metadata(path).num_rows
    except Exception:
        return -1


def _out_path(sport, entity, season=None):
    if season:
        d = os.path.join(OUT, sport, entity, f"season={season}")
    else:
        d = os.path.join(OUT, sport, entity)
    return os.path.join(d, "part.parquet")


def _write(sport, entity, schema, cols, season=None):
    """Build pa.Table from column dict and write parquet."""
    out = _out_path(sport, entity, season)
    nr = existing_rows(out)
    if nr > 0:
        print(f"  SKIP {sport}/{entity}: {nr} rows exist")
        return

    os.makedirs(os.path.dirname(out), exist_ok=True)

    # row count from first non-empty column
    n = 0
    for name in schema.names:
        c = cols.get(name)
        if c and len(c) > 0:
            n = len(c)
            break
    if n == 0:
        _empty(sport, entity, schema, season)
        return

    pa_cols = []
    for field in schema:
        raw = cols.get(field.name)
        if raw is None:
            raw = [None] * n
        # pad / truncate
        if len(raw) < n:
            raw = list(raw) + [None] * (n - len(raw))
        elif len(raw) > n:
            raw = list(raw)[:n]
        else:
            raw = list(raw)

        # non-nullable defaults
        if not field.nullable:
            if pa.types.is_integer(field.type):
                raw = [0 if v is None else v for v in raw]
            elif pa.types.is_floating(field.type):
                raw = [0.0 if v is None else v for v in raw]
            elif pa.types.is_boolean(field.type):
                raw = [False if v is None else v for v in raw]
            else:
                raw = ["" if v is None else v for v in raw]

        # type coercion
        try:
            pa_cols.append(pa.array(raw, type=field.type))
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
            safe = []
            for v in raw:
                if v is None:
                    safe.append(None)
                elif pa.types.is_integer(field.type):
                    safe.append(sint(v))
                elif pa.types.is_floating(field.type):
                    safe.append(sfloat(v))
                elif pa.types.is_boolean(field.type):
                    safe.append(bool(v) if v is not None else None)
                else:
                    safe.append(sstr(v))
            if not field.nullable:
                if pa.types.is_integer(field.type):
                    safe = [0 if v is None else v for v in safe]
                elif pa.types.is_floating(field.type):
                    safe = [0.0 if v is None else v for v in safe]
                elif pa.types.is_boolean(field.type):
                    safe = [False if v is None else v for v in safe]
                else:
                    safe = ["" if v is None else v for v in safe]
            pa_cols.append(pa.array(safe, type=field.type))

    tbl = pa.table(dict(zip(schema.names, pa_cols)), schema=schema)
    pq.write_table(tbl, out, use_dictionary=True, compression="snappy")
    print(f"  {sport}/{entity}: {n} rows")


def _empty(sport, entity, schema, season=None):
    out = _out_path(sport, entity, season)
    nr = existing_rows(out)
    if nr > 0:
        print(f"  SKIP {sport}/{entity}: {nr} rows exist")
        return
    os.makedirs(os.path.dirname(out), exist_ok=True)
    tbl = pa.table(
        {c: pa.array([], type=schema.field(c).type) for c in schema.names},
        schema=schema,
    )
    pq.write_table(tbl, out, use_dictionary=True, compression="snappy")
    print(f"  {sport}/{entity}: 0 rows (empty)")


# ── ESPN helper ────────────────────────────────────────────────────────

def _espn_game_files(sport_dir):
    """Find all game.json files under events/regular/."""
    pat = os.path.join(sport_dir, "events", "regular", "*", "*", "*", "game.json")
    return sorted(_glob.glob(pat))


def _extract_espn_flag_country(flag_url):
    """Extract 3-letter country code from ESPN flag URL."""
    if not flag_url:
        return None
    # .../countries/500/usa.png → usa
    parts = flag_url.rstrip("/").split("/")
    for i, p in enumerate(parts):
        if p == "countries" and i + 2 < len(parts):
            code = parts[i + 2].split(".")[0].upper()
            return code if len(code) <= 4 else None
    return None


# ══════════════════════════════════════════════════════════════════════
#  DOTA 2
# ══════════════════════════════════════════════════════════════════════

def fill_dota2():
    print("\n=== DOTA 2 ===")
    src = os.path.join(RAW, "pandascore", "dota2", "2024")
    od_ref = os.path.join(RAW, "opendota", "dota2", "2024", "reference")
    S = DOTA_SCHEMAS

    teams_raw = jload(os.path.join(src, "teams.json")) or []
    players_raw = jload(os.path.join(src, "players.json")) or []
    matches_raw = jload(os.path.join(src, "matches.json")) or []
    tournaments_raw = jload(os.path.join(src, "tournaments.json")) or []

    # ── 1. teams ───────────────────────────────────────────────────────
    _write("dota2", "teams", S["teams"], {
        "id":           [t["id"] for t in teams_raw],
        "name":         [t.get("name", "") for t in teams_raw],
        "slug":         [t.get("slug") for t in teams_raw],
        "country_name": [t.get("location") or None for t in teams_raw],
        "region":       [None] * len(teams_raw),
        "rank":         [None] * len(teams_raw),
        "total_money":  [None] * len(teams_raw),
        "tour_wins":    [None] * len(teams_raw),
        "source":       ["pandascore"] * len(teams_raw),
    })

    # ── 2. players ─────────────────────────────────────────────────────
    _write("dota2", "players", S["players"], {
        "id":           [p["id"] for p in players_raw],
        "nickname":     [p.get("name", "") for p in players_raw],
        "slug":         [p.get("slug") for p in players_raw],
        "first_name":   [p.get("first_name") for p in players_raw],
        "last_name":    [p.get("last_name") for p in players_raw],
        "birthday":     [None] * len(players_raw),
        "country_name": [p.get("nationality") for p in players_raw],
        "country_code": [None] * len(players_raw),
        "is_coach":     ["true" if p.get("role") == "coach" else "false"
                         for p in players_raw],
        "total_prize":  [None] * len(players_raw),
        "team_id":      [sint((p.get("current_team") or {}).get("id"))
                         for p in players_raw],
        "team_name":    [(p.get("current_team") or {}).get("name")
                         for p in players_raw],
        "source":       ["pandascore"] * len(players_raw),
    })

    # ── 3. heroes (OpenDota) ───────────────────────────────────────────
    heroes_path = os.path.join(od_ref, "heroes.json")
    heroes_raw = jload(heroes_path) if os.path.exists(heroes_path) else None
    if heroes_raw and len(heroes_raw) > 0:
        _write("dota2", "heroes", S["heroes"], {
            "id":             [h["id"] for h in heroes_raw],
            "name":           [h.get("name", "") for h in heroes_raw],
            "localized_name": [h.get("localized_name") for h in heroes_raw],
            "source":         ["opendota"] * len(heroes_raw),
        })
    else:
        _empty("dota2", "heroes", S["heroes"])

    # ── 4. items ───────────────────────────────────────────────────────
    # No items reference in opendota bulk download
    _empty("dota2", "items", S["items"])

    # ── 5. regions (from match→tournament.region) ──────────────────────
    region_set = set()
    for m in matches_raw:
        r = (m.get("tournament") or {}).get("region")
        if r:
            region_set.add(r)
    regions = sorted(region_set)
    if regions:
        _write("dota2", "regions", S["regions"], {
            "id":     [i + 1 for i in range(len(regions))],
            "name":   regions,
            "source": ["pandascore"] * len(regions),
        })
    else:
        _empty("dota2", "regions", S["regions"])

    # ── 6. matches ─────────────────────────────────────────────────────
    cols = {k: [] for k in S["matches"].names}
    for m in matches_raw:
        cols["id"].append(sint(m.get("id")))
        cols["slug"].append(m.get("slug"))
        t = m.get("tournament") or {}
        cols["tournament_id"].append(sint(t.get("id")))
        cols["tournament_name"].append(t.get("name"))
        cols["stage"].append(None)
        opps = m.get("opponents") or []
        o1 = (opps[0].get("opponent") or {}) if len(opps) >= 1 else {}
        o2 = (opps[1].get("opponent") or {}) if len(opps) >= 2 else {}
        cols["team1_id"].append(sint(o1.get("id")))
        cols["team1_name"].append(o1.get("name"))
        cols["team2_id"].append(sint(o2.get("id")))
        cols["team2_name"].append(o2.get("name"))
        w = m.get("winner") or {}
        cols["winner_id"].append(sint(w.get("id")) if w else None)
        cols["winner_name"].append(w.get("name") if w else None)
        res = m.get("results") or []
        cols["team1_score"].append(sint(res[0].get("score")) if len(res) >= 1 else None)
        cols["team2_score"].append(sint(res[1].get("score")) if len(res) >= 2 else None)
        cols["bo_type"].append(sstr(m.get("number_of_games")))
        cols["status"].append(m.get("status"))
        cols["start_date"].append(m.get("begin_at") or m.get("scheduled_at"))
        cols["end_date"].append(m.get("end_at"))
        cols["season"].append("2024")
        cols["source"].append("pandascore")
    _write("dota2", "matches", S["matches"], cols, season="2024")

    # ── 7. match_maps ──────────────────────────────────────────────────
    cols = {k: [] for k in S["match_maps"].names}
    for m in matches_raw:
        mid = sint(m.get("id"))
        for g in (m.get("games") or []):
            cols["id"].append(sint(g.get("id")))
            cols["match_id"].append(mid)
            cols["game_number"].append(sint(g.get("position")))
            gw = g.get("winner") or {}
            cols["winner_id"].append(sint(gw.get("id")) if gw else None)
            cols["winner_name"].append(gw.get("name") if gw else None)
            cols["loser_id"].append(None)
            cols["loser_name"].append(None)
            cols["duration"].append(sint(g.get("length")))
            cols["status"].append(g.get("status"))
            cols["begin_at"].append(g.get("begin_at"))
            cols["end_at"].append(g.get("end_at"))
            cols["season"].append("2024")
            cols["source"].append("pandascore")
    _write("dota2", "match_maps", S["match_maps"], cols, season="2024")

    # ── 8. tournaments ─────────────────────────────────────────────────
    _write("dota2", "tournaments", S["tournaments"], {
        "id":         [sint(t.get("id")) for t in tournaments_raw],
        "name":       [t.get("name", "") for t in tournaments_raw],
        "slug":       [t.get("slug") for t in tournaments_raw],
        "start_date": [t.get("begin_at") for t in tournaments_raw],
        "end_date":   [t.get("end_at") for t in tournaments_raw],
        "prize":      [sstr(t.get("prizepool")) if t.get("prizepool") else None
                       for t in tournaments_raw],
        "event_type": [t.get("type") for t in tournaments_raw],
        "tier":       [t.get("tier") for t in tournaments_raw],
        "status":     [None] * len(tournaments_raw),
        "season":     ["2024"] * len(tournaments_raw),
        "source":     ["pandascore"] * len(tournaments_raw),
    }, season="2024")

    # ── 9. tournament_teams (from matches opponents) ───────────────────
    tt_map = {}  # {tournament_id: {team_id: team_name}}
    for m in matches_raw:
        tid = sint((m.get("tournament") or {}).get("id"))
        if tid is None:
            continue
        tt_map.setdefault(tid, {})
        for opp in (m.get("opponents") or []):
            o = opp.get("opponent") or {}
            team_id = sint(o.get("id"))
            if team_id:
                tt_map[tid][team_id] = o.get("name", "")

    cols = {k: [] for k in S["tournament_teams"].names}
    for tid, teams in tt_map.items():
        for team_id, team_name in teams.items():
            cols["tournament_id"].append(tid)
            cols["team_id"].append(team_id)
            cols["team_name"].append(team_name)
            cols["season"].append("2024")
            cols["source"].append("pandascore")
    _write("dota2", "tournament_teams", S["tournament_teams"], cols, season="2024")

    # ── 10. tournament_rosters ─────────────────────────────────────────
    team_players = {}  # {team_id: [(player_id, name)]}
    for t in teams_raw:
        tid = sint(t.get("id"))
        if tid is None:
            continue
        for p in (t.get("players") or []):
            pid = sint(p.get("id"))
            if pid:
                team_players.setdefault(tid, []).append((pid, p.get("name", "")))

    cols = {k: [] for k in S["tournament_rosters"].names}
    for tourn_id, teams in tt_map.items():
        for team_id in teams:
            for pid, pname in team_players.get(team_id, []):
                cols["tournament_id"].append(tourn_id)
                cols["team_id"].append(team_id)
                cols["player_id"].append(pid)
                cols["player_name"].append(pname)
                cols["season"].append("2024")
                cols["source"].append("pandascore")
    _write("dota2", "tournament_rosters", S["tournament_rosters"], cols, season="2024")

    # ── 11-14. empty stat entities ─────────────────────────────────────
    for ent in ("player_match_stats", "player_stats", "hero_stats", "team_match_stats"):
        _empty("dota2", ent, S[ent], season="2024")


# ══════════════════════════════════════════════════════════════════════
#  GOLF / LPGA  (shared logic)
# ══════════════════════════════════════════════════════════════════════

def fill_golf_like(sport):
    """Fill golf or lpga entities from ESPN data."""
    print(f"\n=== {sport.upper()} ===")
    src = os.path.join(RAW, "espn", sport, "2024")
    S = GOLF_SCHEMAS

    game_files = _espn_game_files(src)
    print(f"  Found {len(game_files)} game.json files")

    players = {}       # id → dict
    courses = {}       # id → dict
    tournaments = {}   # id → dict
    results = []       # list of dicts

    for gf in game_files:
        data = jload(gf)
        if not data:
            continue
        event_id = sint(data.get("eventId") or data.get("id"))
        if not event_id:
            continue
        event_name = data.get("name") or data.get("shortName") or ""

        # Navigate to competitions
        header = (data.get("summary") or {}).get("header") or {}
        comps = header.get("competitions") or []
        # Fallback: summary.event.groupings[].competitions
        if not comps:
            ev = (data.get("summary") or {}).get("event") or {}
            for grp in (ev.get("groupings") or []):
                comps.extend(grp.get("competitions") or [])

        for comp in comps:
            start = comp.get("date")
            end = comp.get("endDate")
            st_obj = comp.get("status") or {}
            st_type = st_obj.get("type") or {}
            status = st_type.get("name") or st_type.get("description") or ""

            if event_id not in tournaments:
                # Try to get event name from higher-level
                ev_obj = (data.get("summary") or {}).get("event") or {}
                ename = ev_obj.get("name") or event_name
                tournaments[event_id] = {
                    "id": event_id, "season": "2024", "name": ename,
                    "start_date": start, "end_date": end,
                    "city": None, "state": None, "country": None,
                    "course_name": None, "purse": None,
                    "status": status, "champion_id": None, "champion_name": None,
                }

            # Extract courses from competition venue if present
            venue = comp.get("venue") or {}
            if venue.get("id"):
                cid = sint(venue["id"])
                if cid and cid not in courses:
                    courses[cid] = {
                        "id": cid, "name": venue.get("fullName", ""),
                        "city": (venue.get("address") or {}).get("city"),
                        "state": (venue.get("address") or {}).get("state"),
                        "country": (venue.get("address") or {}).get("country"),
                    }

            for c in (comp.get("competitors") or []):
                pid = sint(c.get("id"))
                if not pid:
                    continue
                ath = c.get("athlete") or {}
                display = ath.get("displayName") or ath.get("fullName") or ""
                parts = display.split(" ", 1)
                first = parts[0] if parts else ""
                last = parts[1] if len(parts) > 1 else ""

                flag_url = (ath.get("flag") or {}).get("href", "")
                country_code = _extract_espn_flag_country(flag_url)

                if pid not in players:
                    players[pid] = {
                        "id": pid, "first_name": first, "last_name": last,
                        "display_name": display, "country": country_code,
                        "country_code": country_code,
                    }

                # Parse score
                score_raw = c.get("score")
                par_rel = None
                if score_raw is not None:
                    try:
                        par_rel = int(str(score_raw).replace("E", "0").replace("+", ""))
                    except (ValueError, TypeError):
                        pass

                results.append({
                    "tournament_id": event_id,
                    "tournament_name": tournaments.get(event_id, {}).get("name", ""),
                    "player_id": pid,
                    "player_name": display,
                    "position": None,
                    "total_score": None,
                    "total_strokes": None,
                    "par_relative": par_rel,
                    "rounds_played": None,
                    "money": None,
                    "fedex_points": None,
                    "season": "2024",
                })

    # ── players ────────────────────────────────────────────────────────
    plist = list(players.values())
    _write(sport, "players", S["players"], {
        "id":               [p["id"] for p in plist],
        "first_name":       [p["first_name"] for p in plist],
        "last_name":        [p["last_name"] for p in plist],
        "display_name":     [p["display_name"] for p in plist],
        "country":          [p.get("country") for p in plist],
        "country_code":     [p.get("country_code") for p in plist],
        "height":           [None] * len(plist),
        "weight":           [None] * len(plist),
        "birth_date":       [None] * len(plist),
        "birthplace_city":  [None] * len(plist),
        "birthplace_state": [None] * len(plist),
        "birthplace_country": [None] * len(plist),
        "turned_pro":       [None] * len(plist),
        "school":           [None] * len(plist),
        "residence_city":   [None] * len(plist),
        "residence_state":  [None] * len(plist),
        "owgr":             [None] * len(plist),
        "active":           ["true"] * len(plist),
        "source":           ["espn"] * len(plist),
    })

    # ── courses ────────────────────────────────────────────────────────
    clist = list(courses.values())
    if clist:
        _write(sport, "courses", S["courses"], {
            "id":            [c["id"] for c in clist],
            "name":          [c["name"] for c in clist],
            "city":          [c.get("city") for c in clist],
            "state":         [c.get("state") for c in clist],
            "country":       [c.get("country") for c in clist],
            "par":           [None] * len(clist),
            "yardage":       [None] * len(clist),
            "established":   [None] * len(clist),
            "architect":     [None] * len(clist),
            "fairway_grass": [None] * len(clist),
            "rough_grass":   [None] * len(clist),
            "green_grass":   [None] * len(clist),
            "source":        ["espn"] * len(clist),
        })
    else:
        _empty(sport, "courses", S["courses"])

    # ── tournaments ────────────────────────────────────────────────────
    tlist = list(tournaments.values())
    _write(sport, "tournaments", S["tournaments"], {
        "id":            [t["id"] for t in tlist],
        "season":        [t["season"] for t in tlist],
        "name":          [t["name"] for t in tlist],
        "start_date":    [t["start_date"] for t in tlist],
        "end_date":      [t["end_date"] for t in tlist],
        "city":          [t["city"] for t in tlist],
        "state":         [t["state"] for t in tlist],
        "country":       [t["country"] for t in tlist],
        "course_name":   [t["course_name"] for t in tlist],
        "purse":         [t["purse"] for t in tlist],
        "status":        [t["status"] for t in tlist],
        "champion_id":   [t["champion_id"] for t in tlist],
        "champion_name": [t["champion_name"] for t in tlist],
        "source":        ["espn"] * len(tlist),
    }, season="2024")

    # ── tournament_results ─────────────────────────────────────────────
    if results:
        _write(sport, "tournament_results", S["tournament_results"], {
            "tournament_id":   [r["tournament_id"] for r in results],
            "tournament_name": [r["tournament_name"] for r in results],
            "player_id":       [r["player_id"] for r in results],
            "player_name":     [r["player_name"] for r in results],
            "position":        [r["position"] for r in results],
            "total_score":     [r["total_score"] for r in results],
            "total_strokes":   [r["total_strokes"] for r in results],
            "par_relative":    [r["par_relative"] for r in results],
            "rounds_played":   [r["rounds_played"] for r in results],
            "money":           [r["money"] for r in results],
            "fedex_points":    [r["fedex_points"] for r in results],
            "season":          [r["season"] for r in results],
            "source":          ["espn"] * len(results),
        }, season="2024")
    else:
        _empty(sport, "tournament_results", S["tournament_results"], season="2024")

    # ── empty entities ─────────────────────────────────────────────────
    for ent in ("course_holes", "tournament_fields", "round_results",
                "round_stats", "scorecards", "season_stats", "tee_times",
                "course_stats", "odds"):
        _empty(sport, ent, S[ent], season="2024")


# ══════════════════════════════════════════════════════════════════════
#  TENNIS (ATP / WTA)  – shared logic
# ══════════════════════════════════════════════════════════════════════

def fill_tennis(sport):
    """Fill atp or wta entities from ESPN data."""
    print(f"\n=== {sport.upper()} ===")
    src = os.path.join(RAW, "espn", sport, "2024")
    S = TENNIS_SCHEMAS

    game_files = _espn_game_files(src)
    print(f"  Found {len(game_files)} game.json files")

    players = {}       # id_str → dict
    tournaments = {}   # id_str → dict
    matches_list = []  # list of dicts
    rankings_list = []

    for gf in game_files:
        data = jload(gf)
        if not data:
            continue
        event_id = sstr(data.get("eventId") or data.get("id"))
        if not event_id:
            continue
        event_name = data.get("name") or data.get("shortName") or ""

        # Event-level data
        ev = (data.get("summary") or {}).get("event") or {}
        if not ev:
            # Fallback: header
            ev = (data.get("summary") or {}).get("header") or {}

        ev_name = ev.get("name") or event_name
        ev_start = ev.get("date")
        ev_end = ev.get("endDate")

        if event_id not in tournaments:
            tournaments[event_id] = {
                "id": event_id, "name": ev_name, "location": None,
                "surface": None, "category": None, "season": 2024,
                "start_date": ev_start, "end_date": ev_end,
                "prize_money": None, "prize_currency": None,
                "draw_size": None,
            }

        # Navigate groupings → competitions (individual matches)
        groupings = ev.get("groupings") or []
        for grp in groupings:
            for comp in (grp.get("competitions") or []):
                match_id = sstr(comp.get("id"))
                if not match_id:
                    continue
                competitors = comp.get("competitors") or []
                if len(competitors) < 2:
                    continue

                st = comp.get("status") or {}
                st_type = st.get("type") or {}
                is_completed = st_type.get("completed", False)
                match_status = st_type.get("name") or st_type.get("description") or ""

                p1 = competitors[0]
                p2 = competitors[1]
                p1_id = sstr(p1.get("id"))
                p2_id = sstr(p2.get("id"))
                p1_ath = p1.get("athlete") or {}
                p2_ath = p2.get("athlete") or {}
                p1_name = p1_ath.get("displayName") or ""
                p2_name = p2_ath.get("displayName") or ""

                # Determine winner
                winner_id = None
                winner_name = None
                if p1.get("winner"):
                    winner_id = p1_id
                    winner_name = p1_name
                elif p2.get("winner"):
                    winner_id = p2_id
                    winner_name = p2_name

                # Build score string from linescores
                score_parts = []
                p1_ls = p1.get("linescores") or []
                p2_ls = p2.get("linescores") or []
                num_sets = max(len(p1_ls), len(p2_ls))
                for i in range(num_sets):
                    s1 = sint(p1_ls[i].get("value")) if i < len(p1_ls) else 0
                    s2 = sint(p2_ls[i].get("value")) if i < len(p2_ls) else 0
                    score_parts.append(f"{s1 or 0}-{s2 or 0}")
                score = " ".join(score_parts) if score_parts else None

                # Collect players
                for pid, ath in [(p1_id, p1_ath), (p2_id, p2_ath)]:
                    if pid and pid not in players:
                        dn = ath.get("displayName") or ""
                        parts = dn.split(" ", 1)
                        flag_url = (ath.get("flag") or {}).get("href", "")
                        cc = _extract_espn_flag_country(flag_url)
                        players[pid] = {
                            "id": pid,
                            "first_name": parts[0] if parts else "",
                            "last_name": parts[1] if len(parts) > 1 else "",
                            "full_name": dn,
                            "country": cc, "country_code": cc,
                        }

                matches_list.append({
                    "id": match_id,
                    "tournament_id": event_id,
                    "tournament_name": ev_name,
                    "season": 2024,
                    "round": None,
                    "player1_id": p1_id, "player1_name": p1_name,
                    "player2_id": p2_id, "player2_name": p2_name,
                    "winner_id": winner_id, "winner_name": winner_name,
                    "score": score, "duration": None,
                    "number_of_sets": num_sets if num_sets > 0 else None,
                    "match_status": match_status,
                    "is_live": False,
                })

    # ── players ────────────────────────────────────────────────────────
    plist = list(players.values())
    _write(sport, "players", S["players"], {
        "id":           [p["id"] for p in plist],
        "first_name":   [p["first_name"] for p in plist],
        "last_name":    [p["last_name"] for p in plist],
        "full_name":    [p["full_name"] for p in plist],
        "country":      [p.get("country") for p in plist],
        "country_code": [p.get("country_code") for p in plist],
        "birth_place":  [None] * len(plist),
        "age":          [None] * len(plist),
        "height_cm":    [None] * len(plist),
        "weight_kg":    [None] * len(plist),
        "plays":        [None] * len(plist),
        "turned_pro":   [None] * len(plist),
        "source":       ["espn"] * len(plist),
    })

    # ── tournaments ────────────────────────────────────────────────────
    tlist = list(tournaments.values())
    _write(sport, "tournaments", S["tournaments"], {
        "id":             [t["id"] for t in tlist],
        "name":           [t["name"] for t in tlist],
        "location":       [t["location"] for t in tlist],
        "surface":        [t["surface"] for t in tlist],
        "category":       [t["category"] for t in tlist],
        "season":         [t["season"] for t in tlist],
        "start_date":     [t["start_date"] for t in tlist],
        "end_date":       [t["end_date"] for t in tlist],
        "prize_money":    [t["prize_money"] for t in tlist],
        "prize_currency": [t["prize_currency"] for t in tlist],
        "draw_size":      [t["draw_size"] for t in tlist],
        "source":         ["espn"] * len(tlist),
    }, season="2024")

    # ── matches ────────────────────────────────────────────────────────
    if matches_list:
        _write(sport, "matches", S["matches"], {
            "id":              [m["id"] for m in matches_list],
            "tournament_id":   [m["tournament_id"] for m in matches_list],
            "tournament_name": [m["tournament_name"] for m in matches_list],
            "season":          [m["season"] for m in matches_list],
            "round":           [m["round"] for m in matches_list],
            "player1_id":      [m["player1_id"] for m in matches_list],
            "player1_name":    [m["player1_name"] for m in matches_list],
            "player2_id":      [m["player2_id"] for m in matches_list],
            "player2_name":    [m["player2_name"] for m in matches_list],
            "winner_id":       [m["winner_id"] for m in matches_list],
            "winner_name":     [m["winner_name"] for m in matches_list],
            "score":           [m["score"] for m in matches_list],
            "duration":        [m["duration"] for m in matches_list],
            "number_of_sets":  [m["number_of_sets"] for m in matches_list],
            "match_status":    [m["match_status"] for m in matches_list],
            "is_live":         [m["is_live"] for m in matches_list],
            "source":          ["espn"] * len(matches_list),
        }, season="2024")
    else:
        _empty(sport, "matches", S["matches"], season="2024")

    # ── rankings ───────────────────────────────────────────────────────
    rank_file = os.path.join(src, "reference", "rankings", "rankings.json")
    if not os.path.exists(rank_file):
        rank_file = os.path.join(src, "reference", "rankings.json")
    rdata = jload(rank_file) if os.path.exists(rank_file) else None

    if rdata:
        # Navigate ESPN rankings structure
        rankings_obj = rdata.get("rankings") or rdata
        if isinstance(rankings_obj, dict):
            rankings_obj = rankings_obj.get("rankings") or []
        if isinstance(rankings_obj, dict):
            rankings_obj = [rankings_obj]
        if not isinstance(rankings_obj, list):
            rankings_obj = []

        for rgroup in rankings_obj:
            if not isinstance(rgroup, dict):
                continue
            ranks = rgroup.get("ranks") or rgroup.get("entries") or []
            rank_date = rgroup.get("date") or rdata.get("date") or "2024-01-01"
            for r in ranks:
                ath = r.get("athlete") or {}
                pid = sstr(ath.get("id"))
                if not pid:
                    continue
                pname = ath.get("displayName") or \
                    f"{ath.get('firstName','')} {ath.get('lastName','')}".strip()
                row_id = f"{pid}_{rank_date}"
                rankings_list.append({
                    "id": row_id, "player_id": pid, "player_name": pname,
                    "rank": sint(r.get("current") or r.get("rank")),
                    "points": sint(r.get("points")),
                    "movement": None, "ranking_date": rank_date,
                })

    if rankings_list:
        _write(sport, "rankings", S["rankings"], {
            "id":           [r["id"] for r in rankings_list],
            "player_id":    [r["player_id"] for r in rankings_list],
            "player_name":  [r["player_name"] for r in rankings_list],
            "rank":         [r["rank"] for r in rankings_list],
            "points":       [r["points"] for r in rankings_list],
            "movement":     [r["movement"] for r in rankings_list],
            "ranking_date": [r["ranking_date"] for r in rankings_list],
            "source":       ["espn"] * len(rankings_list),
        }, season="2024")
    else:
        _empty(sport, "rankings", S["rankings"], season="2024")

    # ── empty entities ─────────────────────────────────────────────────
    for ent in ("match_stats", "race", "head_to_head",
                "career_stats", "odds", "injuries"):
        _empty(sport, ent, S[ent], season="2024")


# ══════════════════════════════════════════════════════════════════════
#  F1
# ══════════════════════════════════════════════════════════════════════

def fill_f1():
    print("\n=== F1 ===")
    ergast = os.path.join(RAW, "ergast", "f1", "2024")
    openf1 = os.path.join(RAW, "openf1", "f1", "2024")
    S = RACING_SCHEMAS

    # ── 1. drivers (Ergast) ────────────────────────────────────────────
    dj = jload(os.path.join(ergast, "reference", "drivers.json"))
    drivers_raw = ((dj or {}).get("MRData", {})
                   .get("DriverTable", {}).get("Drivers", []))

    _write("f1", "drivers", S["drivers"], {
        "id":            [sstr(hid(d["driverId"])) for d in drivers_raw],
        "first_name":    [d.get("givenName") for d in drivers_raw],
        "last_name":     [d.get("familyName") for d in drivers_raw],
        "display_name":  [f"{d.get('givenName','')} {d.get('familyName','')}".strip()
                          for d in drivers_raw],
        "short_name":    [d.get("code") for d in drivers_raw],
        "country_code":  [None] * len(drivers_raw),
        "country_name":  [d.get("nationality") for d in drivers_raw],
        "racing_number": [d.get("permanentNumber") for d in drivers_raw],
        "source":        ["ergast"] * len(drivers_raw),
    })
    # Build lookup for standings
    driver_id_map = {d["driverId"]: sstr(hid(d["driverId"])) for d in drivers_raw}
    driver_name_map = {d["driverId"]: f"{d.get('givenName','')} {d.get('familyName','')}".strip()
                       for d in drivers_raw}

    # ── 2. teams / constructors (Ergast) ───────────────────────────────
    cj = jload(os.path.join(ergast, "reference", "constructors.json"))
    cons_raw = ((cj or {}).get("MRData", {})
                .get("ConstructorTable", {}).get("Constructors", []))

    _write("f1", "teams", S["teams"], {
        "id":           [sstr(hid(c["constructorId"])) for c in cons_raw],
        "name":         [c.get("name", "") for c in cons_raw],
        "display_name": [c.get("name", "") for c in cons_raw],
        "color":        [None] * len(cons_raw),
        "source":       ["ergast"] * len(cons_raw),
    })
    con_id_map = {c["constructorId"]: sstr(hid(c["constructorId"])) for c in cons_raw}
    con_name_map = {c["constructorId"]: c.get("name", "") for c in cons_raw}

    # ── 3. circuits (Ergast) ───────────────────────────────────────────
    cirj = jload(os.path.join(ergast, "reference", "circuits.json"))
    circs_raw = ((cirj or {}).get("MRData", {})
                 .get("CircuitTable", {}).get("Circuits", []))

    _write("f1", "circuits", S["circuits"], {
        "id":           [sstr(hid(c["circuitId"])) for c in circs_raw],
        "name":         [c.get("circuitName", "") for c in circs_raw],
        "short_name":   [c.get("circuitId") for c in circs_raw],
        "country_code": [None] * len(circs_raw),
        "country_name": [(c.get("Location") or {}).get("country")
                         for c in circs_raw],
        "source":       ["ergast"] * len(circs_raw),
    })

    # ── 4. events (OpenF1 meetings) ────────────────────────────────────
    meetings = jload(os.path.join(openf1, "reference", "meetings.json")) or []
    _write("f1", "events", S["events"], {
        "id":           [sstr(m.get("meeting_key")) for m in meetings],
        "name":         [m.get("meeting_name", "") for m in meetings],
        "short_name":   [m.get("meeting_official_name") for m in meetings],
        "season":       [2024] * len(meetings),
        "start_date":   [m.get("date_start") for m in meetings],
        "end_date":     [m.get("date_end") for m in meetings],
        "status":       [None] * len(meetings),
        "circuit_id":   [sstr(m.get("circuit_key")) for m in meetings],
        "circuit_name": [m.get("circuit_short_name") for m in meetings],
        "location":     [m.get("location") for m in meetings],
        "country_code": [m.get("country_code") for m in meetings],
        "country_name": [m.get("country_name") for m in meetings],
        "source":       ["openf1"] * len(meetings),
    }, season="2024")

    # ── 5. sessions (OpenF1) ───────────────────────────────────────────
    sessions = jload(os.path.join(openf1, "reference", "sessions.json")) or []
    _write("f1", "sessions", S["sessions"], {
        "id":         [sstr(s.get("session_key")) for s in sessions],
        "event_id":   [sstr(s.get("meeting_key")) for s in sessions],
        "event_name": [None] * len(sessions),
        "type":       [s.get("session_type") for s in sessions],
        "name":       [s.get("session_name") for s in sessions],
        "date":       [s.get("date_start") for s in sessions],
        "status":     [None] * len(sessions),
        "source":     ["openf1"] * len(sessions),
    }, season="2024")

    # ── 6. driver_standings (Ergast) ───────────────────────────────────
    dsj = jload(os.path.join(ergast, "standings", "driver_standings.json"))
    ds_raw = []
    if dsj:
        slist = ((dsj.get("MRData") or {}).get("StandingsTable") or {}).get("StandingsLists") or []
        for sl in slist:
            ds_raw.extend(sl.get("DriverStandings") or [])

    if ds_raw:
        _write("f1", "driver_standings", S["driver_standings"], {
            "id":          [sstr(hid(f"ds_{d['Driver']['driverId']}_2024"))
                            for d in ds_raw],
            "season":      [2024] * len(ds_raw),
            "driver_id":   [driver_id_map.get(d["Driver"]["driverId"],
                            sstr(hid(d["Driver"]["driverId"])))
                            for d in ds_raw],
            "driver_name": [driver_name_map.get(d["Driver"]["driverId"],
                            f"{d['Driver'].get('givenName','')} {d['Driver'].get('familyName','')}".strip())
                            for d in ds_raw],
            "team_id":     [con_id_map.get(d["Constructors"][0]["constructorId"],
                            sstr(hid(d["Constructors"][0]["constructorId"])))
                            if d.get("Constructors") else None
                            for d in ds_raw],
            "team_name":   [con_name_map.get(d["Constructors"][0]["constructorId"],
                            d["Constructors"][0].get("name",""))
                            if d.get("Constructors") else None
                            for d in ds_raw],
            "position":    [sint(d.get("position")) for d in ds_raw],
            "points":      [sfloat(d.get("points")) for d in ds_raw],
            "source":      ["ergast"] * len(ds_raw),
        }, season="2024")
    else:
        _empty("f1", "driver_standings", S["driver_standings"], season="2024")

    # ── 7. team_standings (Ergast constructor standings) ───────────────
    csj = jload(os.path.join(ergast, "standings", "constructor_standings.json"))
    cs_raw = []
    if csj:
        slist = ((csj.get("MRData") or {}).get("StandingsTable") or {}).get("StandingsLists") or []
        for sl in slist:
            cs_raw.extend(sl.get("ConstructorStandings") or [])

    if cs_raw:
        _write("f1", "team_standings", S["team_standings"], {
            "id":        [sstr(hid(f"cs_{c['Constructor']['constructorId']}_2024"))
                          for c in cs_raw],
            "season":    [2024] * len(cs_raw),
            "team_id":   [con_id_map.get(c["Constructor"]["constructorId"],
                          sstr(hid(c["Constructor"]["constructorId"])))
                          for c in cs_raw],
            "team_name": [con_name_map.get(c["Constructor"]["constructorId"],
                          c["Constructor"].get("name", ""))
                          for c in cs_raw],
            "position":  [sint(c.get("position")) for c in cs_raw],
            "points":    [sfloat(c.get("points")) for c in cs_raw],
            "source":    ["ergast"] * len(cs_raw),
        }, season="2024")
    else:
        _empty("f1", "team_standings", S["team_standings"], season="2024")

    # ── empty entities ─────────────────────────────────────────────────
    for ent in ("session_results", "qualifying", "lap_times", "pit_stops",
                "position_history", "timing_stats", "weather",
                "race_control", "tire_stints"):
        _empty("f1", ent, S[ent], season="2024")


# ══════════════════════════════════════════════════════════════════════
#  INDYCAR
# ══════════════════════════════════════════════════════════════════════

def fill_indycar():
    print("\n=== INDYCAR ===")
    src = os.path.join(RAW, "espn", "indycar", "2024")
    S = RACING_SCHEMAS

    # ── events from game.json files ────────────────────────────────────
    game_files = _espn_game_files(src)
    print(f"  Found {len(game_files)} game.json files")

    events = {}
    for gf in game_files:
        data = jload(gf)
        if not data:
            continue
        eid = sstr(data.get("eventId") or data.get("id"))
        if not eid or eid in events:
            continue
        ename = data.get("name") or data.get("shortName") or ""

        ev = (data.get("summary") or {}).get("event") or \
             (data.get("summary") or {}).get("header") or {}
        start = ev.get("date")
        end = ev.get("endDate")
        st = ((ev.get("status") or {}).get("type") or {}).get("name")

        events[eid] = {
            "id": eid, "name": ename, "short_name": ename,
            "season": 2024, "start_date": start, "end_date": end,
            "status": st, "circuit_id": None, "circuit_name": None,
            "location": None, "country_code": None, "country_name": None,
        }

    elist = list(events.values())
    if elist:
        _write("indycar", "events", S["events"], {
            "id":           [e["id"] for e in elist],
            "name":         [e["name"] for e in elist],
            "short_name":   [e["short_name"] for e in elist],
            "season":       [e["season"] for e in elist],
            "start_date":   [e["start_date"] for e in elist],
            "end_date":     [e["end_date"] for e in elist],
            "status":       [e["status"] for e in elist],
            "circuit_id":   [e["circuit_id"] for e in elist],
            "circuit_name": [e["circuit_name"] for e in elist],
            "location":     [e["location"] for e in elist],
            "country_code": [e["country_code"] for e in elist],
            "country_name": [e["country_name"] for e in elist],
            "source":       ["espn"] * len(elist),
        }, season="2024")
    else:
        _empty("indycar", "events", S["events"], season="2024")

    # ── empty entities for all other racing schemas ────────────────────
    for ent in ("drivers", "teams", "circuits"):
        _empty("indycar", ent, S[ent])
    for ent in ("sessions", "session_results", "qualifying", "lap_times",
                "pit_stops", "position_history", "timing_stats", "weather",
                "race_control", "tire_stints", "driver_standings",
                "team_standings"):
        _empty("indycar", ent, S[ent], season="2024")


# ══════════════════════════════════════════════════════════════════════
#  AUDIT
# ══════════════════════════════════════════════════════════════════════

def audit():
    print("\n" + "=" * 60)
    print("AUDIT — rows per entity per sport")
    print("=" * 60)
    sports = ["dota2", "golf", "lpga", "atp", "wta", "f1", "indycar"]
    total = 0
    for sport in sports:
        sport_dir = os.path.join(OUT, sport)
        if not os.path.isdir(sport_dir):
            print(f"  {sport}: (no directory)")
            continue
        print(f"\n  {sport}/")
        for entity in sorted(os.listdir(sport_dir)):
            edir = os.path.join(sport_dir, entity)
            if not os.path.isdir(edir):
                continue
            # Direct parquet
            pp = os.path.join(edir, "part.parquet")
            nr = existing_rows(pp)
            if nr >= 0:
                print(f"    {entity}: {nr}")
                total += max(nr, 0)
            # Season-partitioned
            for sub in sorted(os.listdir(edir)):
                sp = os.path.join(edir, sub, "part.parquet")
                snr = existing_rows(sp)
                if snr >= 0:
                    print(f"    {entity}/{sub}: {snr}")
                    total += max(snr, 0)
    print(f"\n  TOTAL rows across all entities: {total}")


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    print(f"Base: {V5}")
    print(f"Raw:  {RAW}")
    print(f"Out:  {OUT}")

    fill_dota2()
    fill_golf_like("golf")
    fill_golf_like("lpga")
    fill_tennis("atp")
    fill_tennis("wta")
    fill_f1()
    fill_indycar()
    audit()

    print("\nDone.")


if __name__ == "__main__":
    main()
