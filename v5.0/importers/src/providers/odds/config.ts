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
  ucl:        "soccer_uefa_champs_league",
  europa:     "soccer_uefa_europa_league",
  nwsl:       "soccer_usa_nwsl",
  ligamx:     "soccer_mexico_ligamx",
  ufc:        "mma_mixed_martial_arts",
  atp:        "tennis_atp_french_open",
  wta:        "tennis_wta_french_open",
  f1:         "motorsport_formula1",
  indycar:    "motorsport_indycar",
  lol:        "esports_lol",
  csgo:       "esports_counter_strike",
  dota2:      "esports_dota2",
  valorant:   "esports_valorant",
  golf:       "golf_pga_championship",
  lpga:       "golf_lpga",
};

export const SUPPORTED_SPORTS = Object.keys(ODDSAPI_SPORT_KEYS) as Sport[];
