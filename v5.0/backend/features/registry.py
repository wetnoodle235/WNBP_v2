# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Registry
# ──────────────────────────────────────────────────────────
#
# Maps sport keys to the correct extractor class.  Provides
# factory functions for getting extractors and running batch
# feature extraction.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from features.base import BaseFeatureExtractor
from features.basketball import BasketballExtractor
from features.football import FootballExtractor
from features.baseball import BaseballExtractor
from features.hockey import HockeyExtractor
from features.soccer import SoccerExtractor
from features.combat import CombatExtractor
from features.tennis import TennisExtractor
from features.motorsport import MotorsportExtractor
from features.golf import GolfExtractor
from features.esports import EsportsExtractor

logger = logging.getLogger(__name__)

# ── Extractor Registry ────────────────────────────────────

EXTRACTORS: dict[str, type[BaseFeatureExtractor]] = {
    # Basketball
    "nba": BasketballExtractor,
    "wnba": BasketballExtractor,
    "ncaab": BasketballExtractor,
    "ncaaw": BasketballExtractor,
    # Football
    "nfl": FootballExtractor,
    "ncaaf": FootballExtractor,
    # Baseball
    "mlb": BaseballExtractor,
    # Hockey
    "nhl": HockeyExtractor,
    # Soccer
    "epl": SoccerExtractor,
    "laliga": SoccerExtractor,
    "bundesliga": SoccerExtractor,
    "seriea": SoccerExtractor,
    "ligue1": SoccerExtractor,
    "mls": SoccerExtractor,
    "ucl": SoccerExtractor,
    "nwsl": SoccerExtractor,
    "ligamx": SoccerExtractor,
    "europa": SoccerExtractor,
    # Combat
    "ufc": CombatExtractor,
    # Tennis
    "atp": TennisExtractor,
    "wta": TennisExtractor,
    # Motorsport
    "f1": MotorsportExtractor,
    "indycar": MotorsportExtractor,
    # Golf
    "golf": GolfExtractor,
    "lpga": GolfExtractor,
    # Esports
    "lol": EsportsExtractor,
    "csgo": EsportsExtractor,
    "dota2": EsportsExtractor,
    "valorant": EsportsExtractor,
}


# ── Factory Functions ─────────────────────────────────────

def get_extractor(sport: str, data_dir: Path) -> BaseFeatureExtractor:
    """Instantiate the correct feature extractor for *sport*.

    Parameters
    ----------
    sport:
        Lower-case sport key (e.g. ``"nba"``, ``"epl"``).
    data_dir:
        Root data directory containing ``normalized/`` parquet files.

    Raises
    ------
    ValueError
        If *sport* is not in the registry.
    """
    sport_lower = sport.lower()
    extractor_cls = EXTRACTORS.get(sport_lower)
    if extractor_cls is None:
        available = ", ".join(sorted(EXTRACTORS.keys()))
        raise ValueError(
            f"No feature extractor registered for sport {sport!r}. "
            f"Available: {available}"
        )
    return extractor_cls(sport=sport_lower, data_dir=data_dir)


def extract_features(sport: str, season: int, data_dir: Path) -> pd.DataFrame:
    """One-call convenience: extract all features for a sport + season.

    Parameters
    ----------
    sport:
        Lower-case sport key.
    season:
        Season identifier (e.g. ``2024``).
    data_dir:
        Root data directory.

    Returns
    -------
    DataFrame of feature vectors (one row per game/match/event).
    """
    extractor = get_extractor(sport, data_dir)
    logger.info("Extracting %s features for season %s", sport, season)
    df = extractor.extract_all(season)
    logger.info(
        "Extracted %d rows × %d columns for %s/%s",
        len(df),
        len(df.columns) if len(df) > 0 else 0,
        sport,
        season,
    )
    return df
