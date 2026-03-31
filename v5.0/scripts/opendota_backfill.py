#!/usr/bin/env python3
"""Slowly backfill OpenDota pro matches + match details for 2023-2024.

OpenDota free rate limit: ~60 requests/min.
We use 1 request every 1.5 seconds to stay well under.
"""
import json
import time
import sys
import urllib.request
from pathlib import Path

BASE_URL = "https://api.opendota.com/api"
DATA_DIR = Path("/home/derek/Documents/stock/v5.0/data/raw/opendota/dota2")

# Match ID ranges per season (approximate)
SEASON_RANGES = {
    "2023": (6900000000, 7500000000),
    "2024": (7500000000, 8000000000),
}

DELAY = 3.0  # seconds between requests


def fetch_json(url: str) -> dict | list | None:
    """Fetch JSON with retry on 429."""
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "v5.0-backfill"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 30 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  HTTP {e.code} for {url}")
                return None
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(5)
    return None


def fetch_pro_matches(season: str) -> list[dict]:
    """Fetch pro match list for a season."""
    out_path = DATA_DIR / season / "pro_matches.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    if out_path.exists():
        with open(out_path) as f:
            existing = json.load(f)
        print(f"  Already have {len(existing)} pro matches for {season}")
        return existing
    
    min_id, max_id = SEASON_RANGES[season]
    all_matches = []
    less_than = max_id
    page = 0
    
    while True:
        url = f"{BASE_URL}/proMatches?less_than_match_id={less_than}"
        print(f"  Fetching pro matches page {page+1} (less_than={less_than})...")
        
        batch = fetch_json(url)
        if not batch or not isinstance(batch, list) or len(batch) == 0:
            break
        
        # Filter to season range
        filtered = [m for m in batch if m.get("match_id", 0) >= min_id]
        all_matches.extend(filtered)
        
        # If all matches are below our min, we're done
        if batch[-1].get("match_id", 0) < min_id:
            break
        
        less_than = batch[-1]["match_id"]
        page += 1
        time.sleep(DELAY)
        
        if page % 10 == 0:
            print(f"  Progress: {len(all_matches)} matches, page {page}")
    
    if all_matches:
        with open(out_path, "w") as f:
            json.dump(all_matches, f)
        print(f"  Saved {len(all_matches)} pro matches for {season}")
    else:
        print(f"  No matches found for {season}")
    
    return all_matches


def fetch_match_details(season: str, matches: list[dict]):
    """Fetch individual match details."""
    matches_dir = DATA_DIR / season / "matches"
    matches_dir.mkdir(parents=True, exist_ok=True)
    
    existing = set(int(f.stem) for f in matches_dir.glob("*.json"))
    to_fetch = [m for m in matches if m.get("match_id") and m["match_id"] not in existing]
    
    print(f"  {len(existing)} existing, {len(to_fetch)} to fetch for {season}")
    
    fetched = 0
    errors = 0
    for i, m in enumerate(to_fetch):
        mid = m["match_id"]
        url = f"{BASE_URL}/matches/{mid}"
        
        data = fetch_json(url)
        if data and isinstance(data, dict) and data.get("match_id"):
            out_path = matches_dir / f"{mid}.json"
            with open(out_path, "w") as f:
                json.dump(data, f)
            fetched += 1
        else:
            errors += 1
        
        if (i + 1) % 50 == 0:
            print(f"  Progress: {fetched} fetched, {errors} errors, {i+1}/{len(to_fetch)}")
        
        time.sleep(DELAY)
    
    print(f"  Done: {fetched} fetched, {errors} errors for {season}")


def main():
    seasons = sys.argv[1:] if len(sys.argv) > 1 else ["2024", "2023"]
    
    for season in seasons:
        print(f"\n=== Season {season} ===")
        matches = fetch_pro_matches(season)
        if matches:
            fetch_match_details(season, matches)


if __name__ == "__main__":
    main()
