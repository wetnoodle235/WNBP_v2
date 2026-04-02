"""
Reorganize v5.0 raw sports data into a consistent hierarchical layout.

ESPN goals
----------
- Consolidate legacy top-level endpoint folders into:
  espn/{sport}/{season}/snapshots/{endpoint}/{YYYY-MM}/{YYYY-MM-DD}.json
- Consolidate legacy odds files into:
  espn/{sport}/{season}/events/{season_type}/{YYYY-MM}/{YYYY-MM-DD}/{event_id}/odds.json
- Keep large game manifests under:
  espn/{sport}/{season}/reference/games/all_games.json

footballdata goals
------------------
- Split games/all.json into per-matchday buckets for easier inspection.

weather goals
-------------
- Move legacy venue/date files into:
    weather/{sport}/{season}/dates/{YYYY-MM-DD}/cities/{city_slug}/weather.json
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("reorganize")

DATA_ROOT = Path(__file__).parent.parent / "data" / "raw"

ESPN_SEASON_TYPE = {
    1: "preseason",
    2: "regular",
    3: "postseason",
    4: "offseason",
}

ESPN_SNAPSHOT_ENDPOINTS = {
    "scoreboard",
    "news",
    "injuries",
    "transactions",
    "team_stats",
    "standings",
}

OPENDOTA_REFERENCE_FILES = {
    "teams.json",
    "pro_players.json",
    "leagues.json",
    "heroes.json",
    "hero_stats.json",
    "public_matches.json",
    "parsed_matches.json",
    "metadata.json",
    "distributions.json",
    "schema.json",
    "health.json",
}


def _openf1_phase(meeting: dict[str, Any] | None, session: dict[str, Any] | None = None) -> str:
    tokens: list[str] = []
    for payload in (meeting, session):
        if not isinstance(payload, dict):
            continue
        for key in ("meeting_name", "meeting_official_name", "session_name", "session_type"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                tokens.append(value.strip().lower())
    text = " ".join(tokens)
    return "testing" if ("test" in text or "day 1" in text or "day 2" in text or "day 3" in text) else "championship"


def _openf1_meeting_dir(season_dir: Path, meeting_key: Any, phase: str) -> Path:
    return season_dir / "season_phases" / phase / "meetings" / f"meeting_{meeting_key}"


def _openf1_session_dir(season_dir: Path, session: dict[str, Any], meetings_by_key: dict[int, dict[str, Any]]) -> Path | None:
    session_key = session.get("session_key")
    meeting_key = session.get("meeting_key")
    if session_key is None or meeting_key is None:
        return None
    try:
        meeting_key_int = int(meeting_key)
    except Exception:
        return None
    phase = _openf1_phase(meetings_by_key.get(meeting_key_int), session)
    return _openf1_meeting_dir(season_dir, meeting_key_int, phase) / "sessions" / f"session_{session_key}"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with open(path) as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
        return {}
    except Exception as exc:
        log.warning("  skip %s: %s", path, exc)
        return {}


def _ensure_move(src: Path, dest: Path, dry_run: bool) -> str:
    """Move src to dest with idempotent handling. Returns moved|exists|dry|error."""
    try:
        if dry_run:
            return "dry"
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            # Prefer keeping the already-organized destination copy.
            src.unlink(missing_ok=True)
            return "exists"
        shutil.move(str(src), str(dest))
        return "moved"
    except Exception as exc:
        log.warning("  move failed %s -> %s: %s", src, dest, exc)
        return "error"


def _iter_sport_seasons(root: Path):
    for sport_dir in sorted(root.iterdir()):
        if not sport_dir.is_dir():
            continue
        for season_dir in sorted(sport_dir.iterdir()):
            if season_dir.is_dir():
                yield sport_dir.name, season_dir.name, season_dir


def _normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return None


def _extract_event_date(payload: dict[str, Any], fallback_stem: str | None = None) -> str:
    candidates = [
        payload.get("date"),
        payload.get("eventDate"),
        payload.get("fetchedAt"),
    ]

    summary = payload.get("summary")
    if isinstance(summary, dict):
        header = summary.get("header")
        if isinstance(header, dict):
            comps = header.get("competitions")
            if isinstance(comps, list) and comps:
                comp0 = comps[0]
                if isinstance(comp0, dict):
                    candidates.append(comp0.get("date"))

    header = payload.get("header")
    if isinstance(header, dict):
        comps = header.get("competitions")
        if isinstance(comps, list) and comps:
            comp0 = comps[0]
            if isinstance(comp0, dict):
                candidates.append(comp0.get("date"))

    for cand in candidates:
        date = _normalize_date(cand)
        if date:
            return date

    if fallback_stem:
        date = _normalize_date(fallback_stem)
        if date:
            return date

    return "unknown-date"


def _extract_season_type(payload: dict[str, Any]) -> str:
    season_type = None

    summary = payload.get("summary")
    if isinstance(summary, dict):
        header = summary.get("header")
        if isinstance(header, dict):
            season = header.get("season")
            if isinstance(season, dict):
                season_type = season.get("type")

    if season_type is None:
        header = payload.get("header")
        if isinstance(header, dict):
            season = header.get("season")
            if isinstance(season, dict):
                season_type = season.get("type")

    try:
        return ESPN_SEASON_TYPE.get(int(season_type), "unknown")
    except Exception:
        return "unknown"


def _month_bucket(date_value: str) -> str:
    if len(date_value) >= 7 and date_value[4] == "-":
        return date_value[:7]
    return "unknown-month"


def _opendota_date_week(start_time: Any) -> tuple[str, str]:
    """Return (date, week_dir) from unix timestamp for OpenDota objects."""
    try:
        ts = int(start_time)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        date_iso = dt.strftime("%Y-%m-%d")
        week_no = dt.isocalendar()[1]
        return date_iso, f"week_{week_no:02d}"
    except Exception:
        return "unknown-date", "week_00"


def _write_json(path: Path, payload: Any, dry_run: bool) -> str:
    """Write JSON file idempotently. Returns written|exists|dry|error."""
    if dry_run:
        return "dry"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return "exists"
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2)
        return "written"
    except Exception as exc:
        log.warning("  write failed %s: %s", path, exc)
        return "error"


def _build_event_lookup(season_dir: Path) -> dict[str, Path]:
    lookup: dict[str, Path] = {}
    events_root = season_dir / "events"
    if not events_root.exists():
        return lookup

    for game_file in events_root.glob("*/*/*/*/game.json"):
        event_dir = game_file.parent
        event_id = event_dir.name
        lookup[event_id] = event_dir

    return lookup


def migrate_espn_games(espn_root: Path, dry_run: bool) -> None:
    moved = exists = errors = 0

    for sport, season, season_dir in _iter_sport_seasons(espn_root):
        games_dir = season_dir / "games"
        if not games_dir.is_dir():
            continue

        for manifest in sorted(games_dir.glob("all*.json")):
            target = season_dir / "reference" / "games" / manifest.name
            status = _ensure_move(manifest, target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1

        for gf in sorted(games_dir.glob("*.json")):
            if gf.name == "index.json" or gf.name.startswith("all"):
                continue
            payload = _load_json(gf)
            event_id = str(payload.get("eventId") or payload.get("id") or gf.stem)
            event_date = _extract_event_date(payload)
            month = _month_bucket(event_date)
            season_type = _extract_season_type(payload)
            target = season_dir / "events" / season_type / month / event_date / event_id / "game.json"

            status = _ensure_move(gf, target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1

        if not dry_run and games_dir.exists() and not any(games_dir.iterdir()):
            games_dir.rmdir()

        if moved or exists:
            log.info("ESPN games %s/%s: moved=%d exists=%d errors=%d", sport, season, moved, exists, errors)

    log.info("ESPN games total: moved=%d exists=%d errors=%d", moved, exists, errors)


def migrate_espn_odds(espn_root: Path, dry_run: bool) -> None:
    moved = exists = unresolved = errors = 0

    for sport, season, season_dir in _iter_sport_seasons(espn_root):
        odds_dir = season_dir / "odds"
        if not odds_dir.is_dir():
            continue

        event_lookup = _build_event_lookup(season_dir)

        for of in sorted(odds_dir.glob("*.json")):
            payload = _load_json(of)
            event_id = str(payload.get("eventId") or payload.get("id") or of.stem)

            event_dir = event_lookup.get(event_id)
            if event_dir is None:
                event_date = _extract_event_date(payload)
                month = _month_bucket(event_date)
                event_dir = season_dir / "events" / "unknown" / month / event_date / event_id
                unresolved += 1

            target = event_dir / "odds.json"
            status = _ensure_move(of, target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1

        if not dry_run and odds_dir.exists() and not any(odds_dir.iterdir()):
            odds_dir.rmdir()

    log.info(
        "ESPN odds total: moved=%d exists=%d unresolved_targets=%d errors=%d",
        moved,
        exists,
        unresolved,
        errors,
    )


def migrate_espn_snapshots(espn_root: Path, dry_run: bool) -> None:
    moved = exists = errors = 0

    for sport, season, season_dir in _iter_sport_seasons(espn_root):
        for endpoint in ESPN_SNAPSHOT_ENDPOINTS:
            endpoint_dir = season_dir / endpoint
            if not endpoint_dir.is_dir():
                continue

            for src in sorted(endpoint_dir.glob("*.json")):
                payload = _load_json(src)
                date_value = _extract_event_date(payload, fallback_stem=src.stem)
                month = _month_bucket(date_value)
                target = season_dir / "snapshots" / endpoint / month / f"{date_value}.json"
                status = _ensure_move(src, target, dry_run)
                if status == "moved":
                    moved += 1
                elif status == "exists":
                    exists += 1
                elif status == "error":
                    errors += 1

            if not dry_run and endpoint_dir.exists() and not any(endpoint_dir.iterdir()):
                endpoint_dir.rmdir()

    log.info("ESPN snapshots total: moved=%d exists=%d errors=%d", moved, exists, errors)


def migrate_espn_root_json(espn_root: Path, dry_run: bool) -> None:
    """Move loose season-root JSON files into reference/ to keep season roots clean."""
    moved = exists = errors = 0

    for _, _, season_dir in _iter_sport_seasons(espn_root):
        for src in sorted(season_dir.glob("*.json")):
            target = season_dir / "reference" / src.name
            status = _ensure_move(src, target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1

    log.info("ESPN root-json total: moved=%d exists=%d errors=%d", moved, exists, errors)


def split_footballdata_matchdays(fd_root: Path, dry_run: bool) -> None:
    split = preserved = errors = 0

    for sport, season, season_dir in _iter_sport_seasons(fd_root):
        all_json = season_dir / "games" / "all.json"
        if not all_json.exists():
            continue

        data = _load_json(all_json)
        matches = data.get("matches", [])
        if not isinstance(matches, list) or not matches:
            preserved += 1
            continue

        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for match in matches:
            if not isinstance(match, dict):
                continue
            stage = str(match.get("stage") or "regular_season").lower()
            group = match.get("group")
            matchday = match.get("matchday")

            if matchday is not None:
                try:
                    key = f"matchday_{int(matchday):02d}"
                except Exception:
                    key = f"matchday_{str(matchday).strip() or 'unknown'}"
            elif group:
                key = f"stage_{stage}/group_{str(group).lower()}"
            else:
                key = f"stage_{stage}"

            buckets[key].append(match)

        for bucket_key, bucket_matches in sorted(buckets.items()):
            payload = {
                "league": data.get("league"),
                "season": data.get("season"),
                "count": len(bucket_matches),
                "matches": bucket_matches,
            }
            target = season_dir / "games" / bucket_key / "matches.json"
            if dry_run:
                continue
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                with open(target, "w") as fh:
                    json.dump(payload, fh, indent=2)
            except Exception as exc:
                log.warning("  failed writing %s: %s", target, exc)
                errors += 1

        split += 1
        log.info("footballdata %s/%s: %d matches -> %d buckets", sport, season, len(matches), len(buckets))

    log.info("footballdata total: split=%d preserved=%d errors=%d", split, preserved, errors)


def migrate_opendota(opendota_root: Path, dry_run: bool) -> None:
    moved = written = exists = errors = 0

    for sport, season, season_dir in _iter_sport_seasons(opendota_root):
        if sport != "dota2":
            continue

        for fname in sorted(OPENDOTA_REFERENCE_FILES):
            src = season_dir / fname
            if not src.exists():
                continue
            target = season_dir / "reference" / fname
            status = _ensure_move(src, target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1

        pro_matches_src = season_dir / "pro_matches.json"
        if pro_matches_src.exists():
            pro_matches = _load_json(pro_matches_src)
            if isinstance(pro_matches, list):
                index_target = season_dir / "reference" / "pro_matches_index.json"
                idx_status = _write_json(index_target, pro_matches, dry_run)
                if idx_status == "written":
                    written += 1
                elif idx_status == "exists":
                    exists += 1
                elif idx_status == "error":
                    errors += 1

                for row in pro_matches:
                    if not isinstance(row, dict):
                        continue
                    match_id = str(row.get("match_id") or "")
                    if not match_id:
                        continue
                    date_iso, week_dir = _opendota_date_week(row.get("start_time"))
                    target = (
                        season_dir
                        / "season_types"
                        / "regular"
                        / "weeks"
                        / week_dir
                        / "dates"
                        / date_iso
                        / "pro_matches"
                        / f"{match_id}.json"
                    )
                    status = _write_json(target, row, dry_run)
                    if status == "written":
                        written += 1
                    elif status == "exists":
                        exists += 1
                    elif status == "error":
                        errors += 1

            if not dry_run:
                pro_matches_src.unlink(missing_ok=True)

        matches_dir = season_dir / "matches"
        if matches_dir.is_dir():
            for src in sorted(matches_dir.glob("*.json")):
                payload = _load_json(src)
                match_id = src.stem
                if isinstance(payload, dict) and payload.get("match_id"):
                    match_id = str(payload.get("match_id"))
                start_time = payload.get("start_time") if isinstance(payload, dict) else None
                date_iso, week_dir = _opendota_date_week(start_time)
                target = (
                    season_dir
                    / "season_types"
                    / "regular"
                    / "weeks"
                    / week_dir
                    / "dates"
                    / date_iso
                    / "matches"
                    / f"{match_id}.json"
                )
                status = _ensure_move(src, target, dry_run)
                if status == "moved":
                    moved += 1
                elif status == "exists":
                    exists += 1
                elif status == "error":
                    errors += 1

            if not dry_run and matches_dir.exists() and not any(matches_dir.iterdir()):
                matches_dir.rmdir()

        log.info(
            "opendota %s/%s: moved=%d written=%d exists=%d errors=%d",
            sport,
            season,
            moved,
            written,
            exists,
            errors,
        )

    log.info("opendota total: moved=%d written=%d exists=%d errors=%d", moved, written, exists, errors)


def migrate_openf1(openf1_root: Path, dry_run: bool) -> None:
    moved = written = exists = errors = 0

    for sport, season, season_dir in _iter_sport_seasons(openf1_root):
        if sport != "f1":
            continue

        meetings_by_key: dict[int, dict[str, Any]] = {}
        meetings_payload: Any = None

        meetings_src = season_dir / "meetings.json"
        meetings_target = season_dir / "reference" / "meetings.json"
        if meetings_src.exists():
            try:
                with open(meetings_src) as fh:
                    meetings_payload = json.load(fh)
            except Exception as exc:
                log.warning("  skip %s: %s", meetings_src, exc)
            status = _ensure_move(meetings_src, meetings_target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1
        elif meetings_target.exists():
            try:
                with open(meetings_target) as fh:
                    meetings_payload = json.load(fh)
            except Exception as exc:
                log.warning("  skip %s: %s", meetings_target, exc)

        if isinstance(meetings_payload, list):
            for meeting in meetings_payload:
                if not isinstance(meeting, dict):
                    continue
                meeting_key = meeting.get("meeting_key")
                if meeting_key is None:
                    continue
                try:
                    meetings_by_key[int(meeting_key)] = meeting
                except Exception:
                    continue
                phase = _openf1_phase(meeting)
                target = _openf1_meeting_dir(season_dir, meeting_key, phase) / "meeting.json"
                status = _write_json(target, meeting, dry_run)
                if status == "written":
                    written += 1
                elif status == "exists":
                    exists += 1
                elif status == "error":
                    errors += 1

        sessions_payload: Any = None
        sessions_src = season_dir / "sessions.json"
        sessions_target = season_dir / "reference" / "sessions.json"
        if sessions_src.exists():
            try:
                with open(sessions_src) as fh:
                    sessions_payload = json.load(fh)
            except Exception as exc:
                log.warning("  skip %s: %s", sessions_src, exc)
            status = _ensure_move(sessions_src, sessions_target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1
        elif sessions_target.exists():
            try:
                with open(sessions_target) as fh:
                    sessions_payload = json.load(fh)
            except Exception as exc:
                log.warning("  skip %s: %s", sessions_target, exc)

        sessions_by_key: dict[str, dict[str, Any]] = {}
        if isinstance(sessions_payload, list):
            for session in sessions_payload:
                if not isinstance(session, dict):
                    continue
                session_key = session.get("session_key")
                if session_key is None:
                    continue
                sessions_by_key[str(session_key)] = session
                target_dir = _openf1_session_dir(season_dir, session, meetings_by_key)
                if target_dir is None:
                    continue
                status = _write_json(target_dir / "session.json", session, dry_run)
                if status == "written":
                    written += 1
                elif status == "exists":
                    exists += 1
                elif status == "error":
                    errors += 1

        for src in sorted(season_dir.iterdir()):
            if not src.is_dir() or not src.name.isdigit():
                continue
            session = sessions_by_key.get(src.name)
            if session is None:
                continue
            target_dir = _openf1_session_dir(season_dir, session, meetings_by_key)
            if target_dir is None:
                continue
            for child in sorted(src.iterdir()):
                target = target_dir / child.name
                status = _ensure_move(child, target, dry_run)
                if status == "moved":
                    moved += 1
                elif status == "exists":
                    exists += 1
                elif status == "error":
                    errors += 1
            if not dry_run and src.exists() and not any(src.iterdir()):
                src.rmdir()

        log.info(
            "openf1 %s/%s: moved=%d written=%d exists=%d errors=%d",
            sport,
            season,
            moved,
            written,
            exists,
            errors,
        )

    log.info("openf1 total: moved=%d written=%d exists=%d errors=%d", moved, written, exists, errors)


def migrate_weather(weather_root: Path, dry_run: bool) -> None:
    """Migrate legacy weather files to date/city partitioned layout.

    Legacy examples:
      weather/{sport}/{season}/{venue_slug}/{YYYY-MM-DD}.json
      weather/{sport}/{season}/{venue_slug}/{YYYY-MM-DD}.json.tmp
    """
    moved = exists = errors = skipped = 0

    for sport, season, season_dir in _iter_sport_seasons(weather_root):
        dates_root = season_dir / "dates"
        for src in sorted(season_dir.glob("**/*.json")):
            if dates_root in src.parents:
                continue

            date_iso = _normalize_date(src.stem)
            if not date_iso:
                skipped += 1
                continue

            city_slug = src.parent.name
            if city_slug in {season, sport, "weather"}:
                city_slug = "unknown_city"

            target = season_dir / "dates" / date_iso / "cities" / city_slug / "weather.json"
            status = _ensure_move(src, target, dry_run)
            if status == "moved":
                moved += 1
            elif status == "exists":
                exists += 1
            elif status == "error":
                errors += 1

    log.info(
        "weather total: moved=%d exists=%d skipped=%d errors=%d",
        moved,
        exists,
        skipped,
        errors,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Reorganize v5.0 raw data layout")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without moving files")
    parser.add_argument(
        "--providers",
        default="espn,footballdata,opendota,openf1,weather",
        help="Comma-separated providers to process (default: espn,footballdata,opendota,openf1,weather)",
    )
    args = parser.parse_args()

    providers = {p.strip() for p in args.providers.split(",") if p.strip()}
    dry_run = args.dry_run

    if dry_run:
        log.info("DRY RUN - no files moved")

    if "espn" in providers:
        espn_root = DATA_ROOT / "espn"
        if espn_root.exists():
            migrate_espn_games(espn_root, dry_run)
            migrate_espn_odds(espn_root, dry_run)
            migrate_espn_snapshots(espn_root, dry_run)
            migrate_espn_root_json(espn_root, dry_run)
        else:
            log.warning("ESPN root not found: %s", espn_root)

    if "footballdata" in providers:
        fd_root = DATA_ROOT / "footballdata"
        if fd_root.exists():
            split_footballdata_matchdays(fd_root, dry_run)
        else:
            log.warning("footballdata root not found: %s", fd_root)

    if "opendota" in providers:
        opendota_root = DATA_ROOT / "opendota"
        if opendota_root.exists():
            migrate_opendota(opendota_root, dry_run)
        else:
            log.warning("opendota root not found: %s", opendota_root)

    if "openf1" in providers:
        openf1_root = DATA_ROOT / "openf1"
        if openf1_root.exists():
            migrate_openf1(openf1_root, dry_run)
        else:
            log.warning("openf1 root not found: %s", openf1_root)

    if "weather" in providers:
        weather_root = DATA_ROOT / "weather"
        if weather_root.exists():
            migrate_weather(weather_root, dry_run)
        else:
            log.warning("weather root not found: %s", weather_root)

    log.info("Done.")


if __name__ == "__main__":
    main()
