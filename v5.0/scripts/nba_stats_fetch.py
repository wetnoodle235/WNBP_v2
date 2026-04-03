#!/usr/bin/env python3
"""
NBA stats fetcher via nba_api.
Usage: python3 nba_stats_fetch.py --season 2024-25 --measure Base|Advanced --output /path.json
Outputs JSON to stdout or file.
"""
import argparse, json, sys, time

def fetch(season: str, measure: str) -> dict:
    from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashteamstats
    time.sleep(0.6)  # Respect stats.nba.com rate limits

    player_stats = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        measure_type_detailed_defense=measure,
        per_mode_detailed="PerGame",
    )
    player_df = player_stats.get_data_frames()[0]

    time.sleep(0.6)

    team_stats = leaguedashteamstats.LeagueDashTeamStats(
        season=season,
        measure_type_detailed_defense=measure,
        per_mode_detailed="PerGame",
    )
    team_df = team_stats.get_data_frames()[0]

    return {
        "season": season,
        "measure": measure,
        "players": json.loads(player_df.to_json(orient="records")),
        "teams": json.loads(team_df.to_json(orient="records")),
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True)
    parser.add_argument("--measure", default="Base", choices=["Base", "Advanced"])
    args = parser.parse_args()

    try:
        result = fetch(args.season, args.measure)
        json.dump(result, sys.stdout)
    except Exception as e:
        json.dump({"error": str(e)}, sys.stdout)
        sys.exit(1)

if __name__ == "__main__":
    main()
