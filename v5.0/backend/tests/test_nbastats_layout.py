import json
from pathlib import Path

from normalization.normalizer import _nbastats_games, _nbastats_player_stats, _nbastats_players


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_nbastats_players_reads_reference_layout(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "reference" / "all_players.json",
        {
            "resultSets": [
                {
                    "headers": ["PERSON_ID", "DISPLAY_FIRST_LAST", "TEAM_ID", "TEAM_ABBREVIATION"],
                    "rowSet": [["2544", "LeBron James", "1610612747", "LAL"]],
                }
            ]
        },
    )

    records = _nbastats_players(tmp_path, "nba", "2025-26")

    assert len(records) == 1
    assert records[0]["id"] == "2544"
    assert records[0]["name"] == "LeBron James"
    assert records[0]["team_id"] == "1610612747"


def test_nbastats_games_reads_boxscore_layout(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "regular-season" / "games" / "0022501047" / "boxscore.json",
        {
            "game": {
                "gameId": "0022501047",
                "gameEt": "2026-03-24T19:00:00Z",
                "gameStatus": 3,
                "gameStatusText": "Final",
                "homeTeam": {
                    "teamId": "1610612766",
                    "teamCity": "Charlotte",
                    "teamName": "Hornets",
                    "score": 110,
                    "statistics": [
                        {"name": "reboundsTotal", "value": 44},
                        {"name": "assists", "value": 27},
                    ],
                },
                "awayTeam": {
                    "teamId": "1610612758",
                    "teamCity": "Sacramento",
                    "teamName": "Kings",
                    "score": 114,
                    "statistics": [
                        {"name": "reboundsTotal", "value": 41},
                        {"name": "assists", "value": 26},
                    ],
                },
            }
        },
    )

    records = _nbastats_games(tmp_path, "nba", "2025-26")

    assert len(records) == 1
    game = records[0]
    assert game["id"] == "nba_0022501047"
    assert game["date"] == "2026-03-24"
    assert game["home_team"] == "Charlotte Hornets"
    assert game["away_score"] == 114
    assert game["home_rebounds"] == 44


def test_nbastats_player_stats_reads_boxscore_layout(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "regular-season" / "season_aggregates" / "player-stats" / "advanced.json",
        {
            "resultSets": [
                {
                    "headers": ["PLAYER_ID", "OFF_RATING", "DEF_RATING", "NET_RATING", "USG_PCT"],
                    "rowSet": [["2544", 118.2, 109.5, 8.7, 31.1]],
                }
            ]
        },
    )
    _write_json(
        tmp_path / "regular-season" / "games" / "0022501047" / "boxscore.json",
        {
            "game": {
                "gameId": "0022501047",
                "homeTeam": {
                    "teamId": "1610612747",
                    "players": [
                        {
                            "personId": "2544",
                            "name": "LeBron James",
                            "plusMinusPoints": 12,
                            "statistics": [
                                {"name": "points", "value": 28},
                                {"name": "reboundsTotal", "value": 9},
                                {"name": "assists", "value": 11},
                                {"name": "steals", "value": 2},
                                {"name": "blocks", "value": 1},
                                {"name": "turnovers", "value": 3},
                                {"name": "fieldGoalsMade", "value": 10},
                                {"name": "fieldGoalsAttempted", "value": 18},
                                {"name": "fieldGoalsPercentage", "value": 55.6},
                                {"name": "freeThrowsMade", "value": 6},
                                {"name": "freeThrowsAttempted", "value": 7},
                                {"name": "threePointersMade", "value": 2},
                                {"name": "threePointersAttempted", "value": 5},
                                {"name": "threePointersPercentage", "value": 40.0},
                                {"name": "reboundsOffensive", "value": 1},
                                {"name": "reboundsDefensive", "value": 8},
                                {"name": "foulsPersonal", "value": 2},
                                {"name": "minutes", "value": "PT35M30.00S"},
                            ],
                        }
                    ],
                },
                "awayTeam": {"teamId": "1610612738", "players": []},
            }
        },
    )

    records = _nbastats_player_stats(tmp_path, "nba", "2025-26")

    assert len(records) == 1
    player = records[0]
    assert player["game_id"] == "0022501047"
    assert player["player_id"] == "2544"
    assert player["pts"] == 28
    assert player["ast"] == 11
    assert player["min"] == 35.5
    assert player["off_rating"] == 118.2
    assert player["usg_pct"] == 31.1