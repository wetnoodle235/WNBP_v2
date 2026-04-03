#!/usr/bin/env python3

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from config import ALL_SPORTS, SPORT_DEFINITIONS, get_current_season
from services.data_service import get_data_service
from services.media_mirror import MediaTarget, get_media_mirror_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sync_media_assets")


def _sync_sport(sport: str, season: str) -> tuple[int, int, int]:
    ds = get_data_service()
    media = get_media_mirror_service()

    synced = 0
    attempted = 0
    errors = 0

    teams = ds.get_teams(sport, season=season)
    for team in teams:
        source_url = team.get("logo_url")
        team_id = str(team.get("id", ""))
        if not source_url or not team_id:
            continue
        attempted += 1
        local = media.sync_target(
            MediaTarget(
                sport=sport,
                entity_type="team",
                entity_id=team_id,
                field_name="logo_url",
                source_url=str(source_url),
            )
        )
        if local:
            synced += 1
        else:
            errors += 1

    players = ds.get_players(sport, season=season)
    for player in players:
        source_url = player.get("headshot_url")
        player_id = str(player.get("id", ""))
        if not source_url or not player_id:
            continue
        attempted += 1
        local = media.sync_target(
            MediaTarget(
                sport=sport,
                entity_type="player",
                entity_id=player_id,
                field_name="headshot_url",
                source_url=str(source_url),
            )
        )
        if local:
            synced += 1
        else:
            errors += 1

    return attempted, synced, errors


def _sync_league_image(sport: str) -> tuple[int, int, int]:
    media = get_media_mirror_service()
    meta = SPORT_DEFINITIONS.get(sport, {})
    source_url = meta.get("image_url") or meta.get("logo_url")
    if not source_url:
        return 0, 0, 0

    local = media.sync_target(
        MediaTarget(
            sport=sport,
            entity_type="league",
            entity_id=sport,
            field_name="image_url",
            source_url=str(source_url),
        )
    )
    return (1, 1, 0) if local else (1, 0, 1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync mirrored team/player media assets to local PNG files")
    parser.add_argument("--sport", action="append", dest="sports", help="Sport key to sync (repeatable)")
    parser.add_argument("--season", default=None, help="Season year to read source records from")
    parser.add_argument("--include-leagues", action="store_true", help="Also sync league-level image/logo URLs from SPORT_DEFINITIONS")
    args = parser.parse_args()

    sports = args.sports or ALL_SPORTS
    total_attempted = 0
    total_synced = 0
    total_errors = 0

    for sport in sports:
        season = args.season or get_current_season(sport)
        attempted, synced, errors = _sync_sport(sport, season)
        if args.include_leagues:
            la, ls, le = _sync_league_image(sport)
            attempted += la
            synced += ls
            errors += le
        total_attempted += attempted
        total_synced += synced
        total_errors += errors
        logger.info(
            "sport=%s season=%s attempted=%s synced=%s errors=%s",
            sport,
            season,
            attempted,
            synced,
            errors,
        )

    logger.info(
        "done attempted=%s synced=%s errors=%s",
        total_attempted,
        total_synced,
        total_errors,
    )
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
