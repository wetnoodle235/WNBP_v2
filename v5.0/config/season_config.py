# ──────────────────────────────────────────────────────────────────────
# Season Configuration — Canonical Season Mapping per Sport
# ──────────────────────────────────────────────────────────────────────
#
# Prevents season/year duplication.  Each sport defines:
#   - season_type: "end_year" (NBA/NHL = folder 2025 means 2024-25 season)
#                  "calendar_year" (MLB/WNBA = folder 2024 means 2024 season)
#   - start_month / end_month: when the season runs
#   - provider_year_to_season(year): maps a provider's calendar year → canonical season int
#   - provider_split_to_season(split): maps "2024-25" → canonical season int
#
# Canonical season is ALWAYS the start year of the season.
# NBA 2024-25 → season = 2024
# NFL 2024    → season = 2024
# MLB 2024    → season = 2024
# ──────────────────────────────────────────────────────────────────────
from __future__ import annotations


SEASON_CONFIG: dict[str, dict] = {
    # ── End-year sports (season spans two calendar years) ──────────
    # ESPN stores as single year = END year; nbastats stores as "START-END"
    "nba": {
        "season_type": "end_year",
        "start_month": 10,   # October
        "end_month": 6,      # June
        "description": "NBA season 2024-25: Oct 2024 → Jun 2025, canonical = 2024",
    },
    "nhl": {
        "season_type": "end_year",
        "start_month": 10,   # October
        "end_month": 6,      # June
        "description": "NHL season 2024-25: Oct 2024 → Jun 2025, canonical = 2024",
    },
    "ncaab": {
        "season_type": "end_year",
        "start_month": 11,   # November
        "end_month": 4,      # April
        "description": "NCAAB season 2024-25: Nov 2024 → Apr 2025, canonical = 2024",
    },
    "ncaaw": {
        "season_type": "end_year",
        "start_month": 11,
        "end_month": 4,
        "description": "NCAAW season 2024-25: Nov 2024 → Apr 2025, canonical = 2024",
    },

    # ── Calendar-year sports (season fits within one calendar year) ─
    "mlb": {
        "season_type": "calendar_year",
        "start_month": 3,    # March (Spring Training)
        "end_month": 11,     # November (World Series)
        "description": "MLB season 2024: Mar → Nov 2024, canonical = 2024",
    },
    "wnba": {
        "season_type": "calendar_year",
        "start_month": 5,    # May
        "end_month": 10,     # October
        "description": "WNBA season 2024: May → Oct 2024, canonical = 2024",
    },

    # ── Cross-year sports (start year = season label) ──────────────
    "nfl": {
        "season_type": "cross_year_start",
        "start_month": 9,    # September
        "end_month": 2,      # February (Super Bowl)
        "description": "NFL season 2024: Sep 2024 → Feb 2025, canonical = 2024",
    },
    "ncaaf": {
        "season_type": "cross_year_start",
        "start_month": 8,    # August
        "end_month": 1,      # January (bowls/CFP)
        "description": "NCAAF season 2024: Aug 2024 → Jan 2025, canonical = 2024",
    },

    # ── Soccer (cross-year, start year) ───────────────────────────
    "epl":        {"season_type": "cross_year_start", "start_month": 8, "end_month": 5},
    "laliga":     {"season_type": "cross_year_start", "start_month": 8, "end_month": 5},
    "bundesliga": {"season_type": "cross_year_start", "start_month": 8, "end_month": 5},
    "seriea":     {"season_type": "cross_year_start", "start_month": 8, "end_month": 5},
    "ligue1":     {"season_type": "cross_year_start", "start_month": 8, "end_month": 5},
    "ucl":        {"season_type": "cross_year_start", "start_month": 9, "end_month": 6},
    "mls":        {"season_type": "calendar_year",    "start_month": 2, "end_month": 12},
}


def provider_year_to_canonical_season(sport: str, provider_year: int | str) -> int:
    """Map a provider's year folder (e.g. ESPN '2025') to canonical season int.

    For end-year sports (NBA, NHL, NCAAB):
        ESPN year 2025 → season 2024  (the 2024-25 season)
    For calendar/cross-year sports:
        year 2024 → season 2024
    """
    yr = int(provider_year)
    cfg = SEASON_CONFIG.get(sport.lower(), {})
    stype = cfg.get("season_type", "calendar_year")

    if stype == "end_year":
        return yr - 1
    return yr


def split_season_to_canonical(sport: str, split: str) -> int:
    """Map a split-format season (e.g. '2024-25') to canonical season int.

    '2024-25' → 2024 for all sports.
    '2024'    → depends on sport type.
    """
    s = str(split).strip()
    if "-" in s:
        return int(s.split("-")[0])
    return provider_year_to_canonical_season(sport, s)


def canonical_season_to_provider_split(sport: str, season: int) -> str:
    """Convert canonical season → provider split format.

    NBA/NHL/NCAAB: 2024 → '2024-25'
    Others:        2024 → '2024'
    """
    cfg = SEASON_CONFIG.get(sport.lower(), {})
    stype = cfg.get("season_type", "calendar_year")

    if stype == "end_year":
        end = (season + 1) % 100
        return f"{season}-{end:02d}"
    return str(season)
