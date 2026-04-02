#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
from collections import defaultdict
from pathlib import Path

BASE = Path('/home/derek/Documents/stock/v5.0/data/raw/cfbdata/ncaaf')
SEASONS = ['2020', '2021', '2022', '2023', '2024', '2025', '2026']


def week_key(week: int | str) -> str:
    return f"week_{int(week):02d}"


def season_type_key(value: object) -> str:
    return 'postseason' if value == 'postseason' else 'regular'


def load_json(path: Path):
    if not path.exists():
        return None
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh, indent=2)


def remove_if_empty(path: Path) -> None:
    if path.exists() and path.is_dir() and not any(path.iterdir()):
        path.rmdir()


def build_game_meta(season_dir: Path) -> dict[str, dict[str, str | int]]:
    meta: dict[str, dict[str, str | int]] = {}
    games_dir = season_dir / 'games'
    if games_dir.exists():
        for season_type_dir in games_dir.iterdir():
            if not season_type_dir.is_dir():
                continue
            season_type = season_type_dir.name
            for week_dir in season_type_dir.iterdir():
                if not week_dir.is_dir():
                    continue
                week = int(week_dir.name.replace('week_', ''))
                for date_dir in week_dir.iterdir():
                    if not date_dir.is_dir():
                        continue
                    for file in date_dir.glob('*.json'):
                        if file.stem.startswith('_'):
                            continue
                        meta[file.stem] = {
                            'seasonType': season_type,
                            'week': week,
                            'date': date_dir.name,
                        }
    return meta


def move_week_root_partitions(season_dir: Path) -> None:
    for week_dir in season_dir.iterdir():
        if not week_dir.is_dir() or not week_dir.name.isdigit():
            continue
        week = int(week_dir.name)

        games_root = week_dir / 'games'
        if games_root.exists():
            for date_dir in games_root.iterdir():
                if not date_dir.is_dir():
                    continue
                for file in date_dir.glob('*.json'):
                    game = load_json(file)
                    if not isinstance(game, dict):
                        continue
                    season_type = season_type_key(game.get('seasonType'))
                    dst = season_dir / 'games' / season_type / week_key(week) / date_dir.name / file.name
                    if not dst.exists():
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(file), str(dst))
            remove_if_empty(games_root)

        plays_root = week_dir / 'plays'
        if plays_root.exists():
            for date_dir in plays_root.iterdir():
                if not date_dir.is_dir():
                    continue
                for file in date_dir.glob('*.json'):
                    stem = file.stem.replace('_plays', '')
                    season_type = 'postseason' if week >= 16 else 'regular'
                    dst = season_dir / 'plays' / season_type / week_key(week) / date_dir.name / f'{stem}.json'
                    if not dst.exists():
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(file), str(dst))
            remove_if_empty(plays_root)

        stats_root = week_dir / 'stats'
        if stats_root.exists():
            for file in stats_root.glob('*.json'):
                season_type = 'postseason' if 'postseason' in file.name else 'regular'
                dst = season_dir / 'stats_player_season' / season_type / week_key(week) / 'stats.json'
                if not dst.exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(file), str(dst))
            remove_if_empty(stats_root)

        remove_if_empty(week_dir)


def split_rankings(season_dir: Path) -> None:
    fp = season_dir / 'rankings' / 'rankings.json'
    data = load_json(fp)
    if not isinstance(data, list):
        return
    for row in data:
        if not isinstance(row, dict):
            continue
        week = row.get('week')
        if not week:
            continue
        season_type = season_type_key(row.get('seasonType'))
        dst = season_dir / 'rankings' / season_type / week_key(week) / 'rankings.json'
        if not dst.exists():
            write_json(dst, row)
    fp.unlink(missing_ok=True)


def split_by_game(season_dir: Path, endpoint: str, filename: str, game_meta: dict[str, dict[str, str | int]], game_id_field: str = 'id', date_field: str | None = None) -> None:
    fp = season_dir / endpoint / filename
    data = load_json(fp)
    if not isinstance(data, list):
        return
    grouped: dict[str, list | dict] = {}
    is_grouped = endpoint in {'drives', 'ppa_games', 'stats_game_advanced', 'stats_game_havoc'}
    for row in data:
        if not isinstance(row, dict):
            continue
        game_id = str(row.get(game_id_field) or '')
        if not game_id:
            continue
        meta = game_meta.get(game_id, {})
        season_type = season_type_key(row.get('seasonType') or meta.get('seasonType'))
        week = int(row.get('week') or meta.get('week') or 0)
        if not week:
            continue
        date = None
        if date_field:
            raw_date = row.get(date_field)
            if isinstance(raw_date, str) and raw_date:
                date = raw_date.split('T')[0]
        if not date:
            date = str(meta.get('date') or 'unknown')
        dst = season_dir / endpoint / season_type / week_key(week) / date / f'{game_id}.json'
        if dst.exists():
            continue
        if is_grouped:
            grouped.setdefault(str(dst), []).append(row)
        else:
            grouped[str(dst)] = row
    for dst_str, payload in grouped.items():
        write_json(Path(dst_str), payload)
    fp.unlink(missing_ok=True)


def split_weekly(season_dir: Path, endpoint: str, filename: str, season_type_field: str = 'seasonType', week_field: str = 'week', output_name: str = 'data.json') -> None:
    fp = season_dir / endpoint / filename
    data = load_json(fp)
    if not isinstance(data, list):
        return
    grouped: dict[tuple[str, int], list] = defaultdict(list)
    for row in data:
        if not isinstance(row, dict):
            continue
        week = row.get(week_field)
        if not week:
            continue
        season_type = season_type_key(row.get(season_type_field))
        grouped[(season_type, int(week))].append(row)
    for (season_type, week), rows in grouped.items():
        dst = season_dir / endpoint / season_type / week_key(week) / output_name
        if not dst.exists():
            write_json(dst, rows)
    fp.unlink(missing_ok=True)


def split_ppa_predicted(season_dir: Path) -> None:
    fp = season_dir / 'ppa_predicted' / 'ppa_predicted.json'
    data = load_json(fp)
    if not isinstance(data, list):
        return
    for row in data:
        if not isinstance(row, dict):
            continue
        down = row.get('down')
        distance = row.get('distance')
        payload = row.get('data')
        if down is None or distance is None or payload is None:
            continue
        dst = season_dir / 'ppa_predicted' / f'down_{down}' / f'distance_{distance}.json'
        if not dst.exists():
            write_json(dst, payload)
    fp.unlink(missing_ok=True)


def split_metrics_wp_root(season_dir: Path, game_meta: dict[str, dict[str, str | int]]) -> None:
    endpoint_dir = season_dir / 'metrics_wp'
    if not endpoint_dir.exists():
        return
    for file in list(endpoint_dir.glob('*.json')):
        if file.name.startswith('_'):
            continue
        game_id = file.stem
        meta = game_meta.get(game_id)
        if not meta:
            continue
        dst = season_dir / 'metrics_wp' / str(meta['seasonType']) / week_key(int(meta['week'])) / str(meta['date']) / file.name
        if not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file), str(dst))


def cleanup_empty_dirs(season_dir: Path) -> None:
    changed = True
    while changed:
        changed = False
        for path in sorted(season_dir.rglob('*'), reverse=True):
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()
                changed = True


def main() -> int:
    for season in SEASONS:
        season_dir = BASE / season
        if not season_dir.exists():
            continue
        print(f'Repartitioning {season}...')
        move_week_root_partitions(season_dir)
        game_meta = build_game_meta(season_dir)
        split_rankings(season_dir)
        split_by_game(season_dir, 'lines', 'lines.json', game_meta, game_id_field='id', date_field='startDate')
        split_by_game(season_dir, 'drives', 'drives.json', game_meta, game_id_field='gameId')
        split_by_game(season_dir, 'games_players', 'games_players.json', game_meta, game_id_field='id')
        split_by_game(season_dir, 'games_teams', 'games_teams.json', game_meta, game_id_field='id')
        split_by_game(season_dir, 'games_media', 'games_media.json', game_meta, game_id_field='id', date_field='startTime')
        split_by_game(season_dir, 'ppa_games', 'ppa_games.json', game_meta, game_id_field='gameId')
        split_by_game(season_dir, 'stats_game_advanced', 'stats_game_advanced.json', game_meta, game_id_field='gameId')
        split_by_game(season_dir, 'stats_game_havoc', 'stats_game_havoc.json', game_meta, game_id_field='gameId')
        split_by_game(season_dir, 'wp_pregame', 'wp_pregame.json', game_meta, game_id_field='gameId')
        split_weekly(season_dir, 'ppa_players_games', 'ppa_players_games.json', output_name='players.json')
        split_ppa_predicted(season_dir)
        split_metrics_wp_root(season_dir, game_meta)
        cleanup_empty_dirs(season_dir)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
