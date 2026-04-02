from pathlib import Path

from normalization.normalizer import (
    _lahman_players,
    _lahman_player_stats,
    _lahman_standings,
)


def test_lahman_players_falls_back_to_master(tmp_path: Path) -> None:
    (tmp_path / "Master.csv").write_text(
        "playerID,birthYear,birthMonth,birthDay,birthCity,birthState,birthCountry,nameFirst,nameLast,weight\n"
        "testp01,1990,7,14,San Diego,CA,USA,Test,Player,205\n",
        encoding="utf-8",
    )

    records = _lahman_players(tmp_path, "mlb", "2025")

    assert len(records) == 1
    assert records[0]["id"] == "testp01"
    assert records[0]["name"] == "Test Player"
    assert str(records[0]["birth_date"]) == "1990-07-14"
    assert records[0]["birth_place"] == "San Diego, CA, USA"
    assert records[0]["nationality"] == "USA"


def test_lahman_standings_maps_team_season_records(tmp_path: Path) -> None:
    (tmp_path / "Teams.csv").write_text(
        "yearID,lgID,teamID,divID,Rank,G,W,L,name,DivWin,WCWin,LgWin,WSWin,R,RA,attendance\n"
        "2025,AL,BOS,E,2,162,92,70,Boston Red Sox,N,Y,N,N,750,680,2800000\n",
        encoding="utf-8",
    )

    records = _lahman_standings(tmp_path, "mlb", "2025")

    assert len(records) == 1
    rec = records[0]
    assert rec["team_id"] == "BOS"
    assert rec["team_name"] == "Boston Red Sox"
    assert rec["wins"] == 92
    assert rec["losses"] == 70
    assert rec["games_played"] == 162
    assert rec["rank"] == 2
    assert rec["group"] == "AL"
    assert rec["division"] == "E"
    assert rec["pct"] == 0.568
    assert rec["div_winner"] is False
    assert rec["wildcard"] is True
    assert rec["league_winner"] is False
    assert rec["ws_winner"] is False
    assert rec["runs_scored"] == 750
    assert rec["runs_allowed"] == 680
    assert rec["attendance"] == 2800000


def test_lahman_player_stats_includes_batting_and_pitching_fields(tmp_path: Path) -> None:
    (tmp_path / "Batting.csv").write_text(
        "playerID,yearID,stint,teamID,G,AB,H,2B,3B,HR,RBI,SB,R,BB,IBB,SO,CS,HBP,SH,SF,GIDP\n"
        "testp01,2025,1,BOS,75,100,30,5,2,4,20,3,18,10,1,25,1,2,1,3,4\n",
        encoding="utf-8",
    )
    (tmp_path / "Pitching.csv").write_text(
        "playerID,yearID,stint,teamID,W,L,G,GS,CG,SHO,SV,IPouts,H,BB,SO,ER,ERA,IBB,WP\n"
        "testp01,2025,1,BOS,1,0,39,0,0,0,2,27,8,3,11,4,4.00,1,2\n",
        encoding="utf-8",
    )

    records = _lahman_player_stats(tmp_path, "mlb", "2025")

    assert len(records) == 1
    rec = records[0]
    assert rec["game_id"] == "lahman-2025-testp01-BOS-1"
    assert rec["player_id"] == "testp01"
    assert rec["team_id"] == "BOS"
    assert rec["ab"] == 100
    assert rec["hits"] == 30
    assert rec["doubles"] == 5
    assert rec["triples"] == 2
    assert rec["hr"] == 4
    assert rec["pa"] == 116
    assert rec["hbp"] == 2
    assert rec["sac_bunts"] == 1
    assert rec["sac_flies"] == 3
    assert rec["gidp"] == 4
    assert rec["total_bases"] == 51
    assert rec["avg"] == 0.3
    assert rec["obp"] == 0.365
    assert rec["slg"] == 0.51
    assert rec["ops"] == 0.875
    assert rec["era"] == 4.0
    assert rec["strikeouts"] == 11
    assert rec["walks"] == 3
    assert rec["innings"] == 9.0
    assert rec["earned_runs"] == 4
    assert rec["whip"] == 1.222
    assert rec["win"] is True
    assert rec["loss"] is False
    assert rec["save"] is True
    assert rec["games"] == 75
    assert rec["ibb"] == 1
    assert rec["games_pitched"] == 39
    assert rec["games_started"] == 0
    assert rec["complete_games"] == 0
    assert rec["shutouts"] == 0
    assert rec["ibb_pitcher"] == 1
    assert rec["wp"] == 2