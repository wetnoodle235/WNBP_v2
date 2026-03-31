import json
from pathlib import Path

from normalization.normalizer import (
    _build_espn_name_stat_map,
    _build_espn_stat_map,
    _coalesce_player_stat_aliases,
    _espn_basketball_player_stats,
)


def test_build_espn_stat_map_uses_aliases() -> None:
    labels = ["Comp/Att", "Rating", "In 20", "TOV", "PLUS/MINUS"]
    values = ["22/31", "102.3", "3", "4", "+9"]
    aliases = {
        "compatt": ("C/ATT",),
        "rating": ("RTG",),
        "in20": ("IN 20",),
        "tov": ("TO",),
        "plusminus": ("+/-",),
    }

    stat_map = _build_espn_stat_map(labels, values, aliases)

    assert stat_map["C/ATT"] == "22/31"
    assert stat_map["RTG"] == "102.3"
    assert stat_map["IN 20"] == "3"
    assert stat_map["TO"] == "4"
    assert stat_map["+/-"] == "+9"


def test_basketball_parser_accepts_common_label_variants(tmp_path: Path) -> None:
    games_dir = tmp_path / "games"
    games_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "eventId": "evt-1",
        "summary": {
            "header": {
                "competitions": [{"date": "2025-01-02T00:00Z"}],
            },
            "boxscore": {
                "players": [
                    {
                        "team": {"abbreviation": "LAL"},
                        "statistics": [
                            {
                                "labels": [
                                    "MIN",
                                    "FGM-A",
                                    "3PM-A",
                                    "FTM-A",
                                    "OREB",
                                    "DREB",
                                    "REB",
                                    "AST",
                                    "STL",
                                    "BLK",
                                    "TOV",
                                    "PF",
                                    "PTS",
                                    "PLUS/MINUS",
                                ],
                                "athletes": [
                                    {
                                        "athlete": {
                                            "id": "p1",
                                            "displayName": "Player One",
                                            "position": {"abbreviation": "G"},
                                        },
                                        "stats": [
                                            "35",
                                            "8-17",
                                            "3-9",
                                            "4-5",
                                            "1",
                                            "5",
                                            "6",
                                            "7",
                                            "2",
                                            "1",
                                            "3",
                                            "2",
                                            "23",
                                            "+11",
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
        },
    }

    fp = games_dir / "game_1.json"
    fp.write_text(json.dumps(payload), encoding="utf-8")

    records = _espn_basketball_player_stats(tmp_path, "nba", "2025")
    assert len(records) == 1

    rec = records[0]
    assert rec["fg_made"] == 8
    assert rec["fg_attempted"] == 17
    assert rec["fg3_made"] == 3
    assert rec["fg3_attempted"] == 9
    assert rec["to"] == 3
    assert rec["plus_minus"] == 11


def test_build_espn_name_stat_map_uses_aliases() -> None:
    stats = [
        {"name": "Goals", "value": "2"},
        {"name": "Pass Accuracy", "value": "87%"},
        {"name": "Key Passes", "value": "4"},
    ]
    aliases = {
        "goals": ("totalGoals",),
        "passaccuracy": ("passAccuracy",),
        "keypasses": ("keyPasses",),
    }

    stat_map = _build_espn_name_stat_map(stats, aliases)

    assert stat_map["totalGoals"] == "2"
    assert stat_map["passAccuracy"] == "87%"
    assert stat_map["keyPasses"] == "4"


def test_coalesce_player_stat_aliases_for_multiple_categories() -> None:
    basketball = {
        "points": 28,
        "rebounds": 9,
        "assists": 7,
        "minutes_played": 36,
        "field_goal_pct": "51.2%",
    }
    _coalesce_player_stat_aliases(basketball, "basketball")
    assert basketball["pts"] == 28
    assert basketball["reb"] == 9
    assert basketball["ast"] == 7
    assert basketball["min"] == 36
    assert basketball["fg_pct"] == 51.2

    soccer = {
        "shots_on_goal": 3,
        "pass_accuracy": "84%",
        "fouls_committed": 2,
    }
    _coalesce_player_stat_aliases(soccer, "soccer")
    assert soccer["shots_on_target"] == 3
    assert soccer["pass_pct"] == 84.0
    assert soccer["fouls"] == 2
