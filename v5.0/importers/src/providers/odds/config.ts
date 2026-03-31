// ──────────────────────────────────────────────────────────
// V5.0 Odds Collection System — Configuration & Types
// ──────────────────────────────────────────────────────────

import type { Sport } from "../../core/types.js";

// ── Collection configuration ────────────────────────────────

export const ODDS_CONFIG = {
  collection_interval_minutes: 60,
  opening_time_est: "00:05",
  closing_minutes_before: 5,
  enabled_sportsbooks: ["draftkings", "fanduel", "betmgm", "pointsbet", "caesars"],
  oddsapi_key: process.env.ODDSAPI_KEY ?? "",
};

// ── Types ───────────────────────────────────────────────────

export type OddsCollectionType = "opening" | "closing" | "snapshot";

export interface OddsRecord {
  game_id: string;
  sport: Sport;
  date: string;
  timestamp: string;
  type: OddsCollectionType;
  sportsbook: string;
  home_team: string;
  away_team: string;
  home_spread: number | null;
  away_spread: number | null;
  home_spread_odds: number | null;
  away_spread_odds: number | null;
  home_ml: number | null;
  away_ml: number | null;
  total: number | null;
  over_odds: number | null;
  under_odds: number | null;
}

// ── OddsAPI sport key mapping ───────────────────────────────

export const ODDSAPI_SPORT_KEYS: Partial<Record<Sport, string>> = {
  nba:        "basketball_nba",
  nfl:        "americanfootball_nfl",
  mlb:        "baseball_mlb",
  nhl:        "icehockey_nhl",
  ncaab:      "basketball_ncaab",
  ncaaf:      "americanfootball_ncaaf",
  wnba:       "basketball_wnba",
  epl:        "soccer_epl",
  laliga:     "soccer_spain_la_liga",
  bundesliga: "soccer_germany_bundesliga",
  seriea:     "soccer_italy_serie_a",
  ligue1:     "soccer_france_ligue_one",
  mls:        "soccer_usa_mls",
  ufc:        "mma_mixed_martial_arts",
  atp:        "tennis_atp_french_open",
};

export const SUPPORTED_SPORTS = Object.keys(ODDSAPI_SPORT_KEYS) as Sport[];
