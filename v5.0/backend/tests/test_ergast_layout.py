import json
from pathlib import Path

from normalization.normalizer import _ergast_games, _ergast_player_stats, _ergast_players


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_ergast_players_reads_reference_layout(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "reference" / "drivers.json",
        {
            "MRData": {
                "DriverTable": {
                    "Drivers": [
                        {
                            "driverId": "verstappen",
                            "givenName": "Max",
                            "familyName": "Verstappen",
                            "permanentNumber": "1",
                            "nationality": "Dutch",
                            "dateOfBirth": "1997-09-30",
                        }
                    ]
                }
            }
        },
    )

    records = _ergast_players(tmp_path, "f1", "2026")

    assert len(records) == 1
    assert records[0]["id"] == "verstappen"
    assert records[0]["name"] == "Max Verstappen"
    assert records[0]["jersey_number"] == 1


def test_ergast_games_reads_round_layout(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "rounds" / "round_01" / "results.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [
                        {
                            "season": "2026",
                            "round": "1",
                            "raceName": "Australian Grand Prix",
                            "Circuit": {
                                "circuitId": "albert_park",
                                "circuitName": "Albert Park Circuit",
                                "Location": {"locality": "Melbourne", "country": "Australia"},
                            },
                            "date": "2026-03-08",
                            "Results": [
                                {
                                    "position": "1",
                                    "grid": "1",
                                    "laps": "58",
                                    "status": "Finished",
                                    "Driver": {"driverId": "russell", "givenName": "George", "familyName": "Russell"},
                                    "Constructor": {"constructorId": "mercedes", "name": "Mercedes"},
                                    "Time": {"time": "1:23:06.801"},
                                    "FastestLap": {"rank": "1", "lap": "55", "Time": {"time": "1:22.100"}},
                                }
                            ],
                        }
                    ],
                }
            }
        },
    )
    _write_json(
        tmp_path / "rounds" / "round_01" / "qualifying.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [
                        {
                            "round": "1",
                            "QualifyingResults": [
                                {
                                    "position": "1",
                                    "Driver": {"driverId": "russell", "givenName": "George", "familyName": "Russell"},
                                }
                            ],
                        }
                    ],
                }
            }
        },
    )
    _write_json(
        tmp_path / "rounds" / "round_01" / "laps.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [
                        {
                            "round": "1",
                            "Laps": [
                                {"number": "1", "Timings": [{"driverId": "russell", "position": "1"}]},
                                {"number": "2", "Timings": [{"driverId": "russell", "position": "1"}]},
                            ],
                        }
                    ],
                }
            }
        },
    )
    _write_json(
        tmp_path / "rounds" / "round_01" / "pitstops.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [{"round": "1", "PitStops": [{"driverId": "russell"}, {"driverId": "russell"}]}],
                }
            }
        },
    )

    records = _ergast_games(tmp_path, "f1", "2026")

    assert len(records) == 1
    game = records[0]
    assert game["id"] == "2026_1"
    assert game["winner_name"] == "George Russell"
    assert game["total_laps"] == 58
    assert game["pit_stops_total"] == 2
    assert game["lead_changes"] is None
    assert game["unique_lap_leaders"] == 1


def test_ergast_player_stats_reads_round_layout(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "rounds" / "round_01" / "results.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [
                        {
                            "season": "2026",
                            "round": "1",
                            "raceName": "Australian Grand Prix",
                            "date": "2026-03-08",
                            "Results": [
                                {
                                    "position": "1",
                                    "grid": "2",
                                    "laps": "58",
                                    "status": "Finished",
                                    "points": "25",
                                    "Driver": {"driverId": "russell", "givenName": "George", "familyName": "Russell"},
                                    "Constructor": {"constructorId": "mercedes", "name": "Mercedes"},
                                    "FastestLap": {
                                        "rank": "1",
                                        "lap": "55",
                                        "Time": {"time": "1:22.100"},
                                        "AverageSpeed": {"speed": "210.5"},
                                    },
                                }
                            ],
                        }
                    ],
                }
            }
        },
    )
    _write_json(
        tmp_path / "rounds" / "round_01" / "qualifying.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [
                        {
                            "round": "1",
                            "QualifyingResults": [
                                {"position": "1", "Driver": {"driverId": "russell"}}
                            ],
                        }
                    ],
                }
            }
        },
    )
    _write_json(
        tmp_path / "rounds" / "round_01" / "laps.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [
                        {
                            "round": "1",
                            "Laps": [
                                {"number": "1", "Timings": [{"driverId": "russell", "position": "1"}]},
                                {"number": "2", "Timings": [{"driverId": "russell", "position": "2"}]},
                            ],
                        }
                    ],
                }
            }
        },
    )
    _write_json(
        tmp_path / "rounds" / "round_01" / "pitstops.json",
        {
            "MRData": {
                "RaceTable": {
                    "season": "2026",
                    "round": "1",
                    "Races": [{"round": "1", "PitStops": [{"driverId": "russell"}]}],
                }
            }
        },
    )

    records = _ergast_player_stats(tmp_path, "f1", "2026")

    assert len(records) == 1
    stat = records[0]
    assert stat["player_id"] == "russell"
    assert stat["qualifying_position"] == 1
    assert stat["pit_stops"] == 1
    assert stat["laps_led"] == 1
    assert stat["avg_running_position"] == 1.5
    assert stat["best_running_position"] == 1
    assert stat["worst_running_position"] == 2