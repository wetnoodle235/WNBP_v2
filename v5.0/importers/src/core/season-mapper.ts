// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Season Mapper
// ──────────────────────────────────────────────────────────
// Maps calendar dates to the correct "season year" for every
// sport.  Cross-year leagues (NBA, NFL, EPL, …) label the
// season by start-year or end-year depending on convention.
// Ported from v4.0/util/sports/season-mapper.ts, adapted for v5.0.

import type { Sport } from "./types.js";

interface SeasonDateRange {
  /** Month the season starts (1-12) */
  startMonth: number;
  startDay: number;
  /** Month the season ends (1-12) */
  endMonth: number;
  endDay: number;
  /**
   * If true the season label is the year the season **starts** in.
   * NBA 2024-25 → label "2025" (start Oct 2024, useStartYear = false is wrong
   * for NBA convention — NBA labels by *end* year).
   *
   * For NFL the convention is to use the start year: "2024 NFL season" starts Sep 2024.
   * For soccer the convention is start year: "2024-25 EPL" → label "2024".
   */
  useEndYear: boolean;
}

const SEASON_CONFIGS: Record<string, SeasonDateRange> = {
  // Basketball — labelled by end year (2024-25 = "2025")
  nba:   { startMonth: 10, startDay: 1,  endMonth: 6,  endDay: 30, useEndYear: true },
  wnba:  { startMonth: 5,  startDay: 1,  endMonth: 10, endDay: 31, useEndYear: false },
  ncaab: { startMonth: 11, startDay: 1,  endMonth: 4,  endDay: 30, useEndYear: true },
  ncaaw: { startMonth: 11, startDay: 1,  endMonth: 4,  endDay: 30, useEndYear: true },

  // Football — labelled by start year
  nfl:   { startMonth: 9,  startDay: 1,  endMonth: 2,  endDay: 28, useEndYear: false },
  ncaaf: { startMonth: 8,  startDay: 1,  endMonth: 1,  endDay: 31, useEndYear: false },

  // Hockey — labelled by end year (2024-25 = "2025")
  nhl:   { startMonth: 10, startDay: 1,  endMonth: 6,  endDay: 30, useEndYear: true },

  // Baseball — calendar year
  mlb:   { startMonth: 3,  startDay: 1,  endMonth: 11, endDay: 15, useEndYear: false },

  // Soccer — labelled by start year (2024-25 = "2024")
  epl:        { startMonth: 8,  startDay: 1,  endMonth: 5, endDay: 31, useEndYear: false },
  laliga:     { startMonth: 8,  startDay: 1,  endMonth: 5, endDay: 31, useEndYear: false },
  bundesliga: { startMonth: 8,  startDay: 1,  endMonth: 5, endDay: 31, useEndYear: false },
  seriea:     { startMonth: 8,  startDay: 1,  endMonth: 5, endDay: 31, useEndYear: false },
  ligue1:     { startMonth: 8,  startDay: 1,  endMonth: 5, endDay: 31, useEndYear: false },
  mls:        { startMonth: 2,  startDay: 1,  endMonth: 12, endDay: 15, useEndYear: false },
  ucl:        { startMonth: 8,  startDay: 1,  endMonth: 6, endDay: 30, useEndYear: false },
  nwsl:       { startMonth: 3,  startDay: 1,  endMonth: 11, endDay: 30, useEndYear: false },

  // Combat / Other — calendar year
  ufc:  { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },
  golf: { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },

  // Motorsport
  f1: { startMonth: 3, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },

  // Tennis — calendar year
  atp: { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },
  wta: { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },

  // Esports — calendar year
  lol:      { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },
  csgo:     { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },
  dota2:    { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },
  valorant: { startMonth: 1, startDay: 1, endMonth: 12, endDay: 31, useEndYear: false },
};

/**
 * Determine the "season year" label for a given date and sport.
 *
 * Examples:
 *  - `getSeasonFromDate("2025-01-15", "nba")` → `2025`  (NBA 2024-25 season)
 *  - `getSeasonFromDate("2024-10-20", "nba")` → `2025`  (season just started)
 *  - `getSeasonFromDate("2025-09-10", "nfl")` → `2025`  (NFL uses start year)
 *  - `getSeasonFromDate("2024-09-15", "epl")` → `2024`  (EPL uses start year)
 */
export function getSeasonFromDate(dateStr: string, sport: Sport | string): number {
  const date = new Date(dateStr);
  if (isNaN(date.getTime())) return new Date().getFullYear();

  const year = date.getFullYear();
  const month = date.getMonth() + 1;

  const config = SEASON_CONFIGS[sport.toLowerCase()];
  if (!config) return year;

  // Cross-year sport: season spans two calendar years
  const crossYear = config.endMonth < config.startMonth;

  if (crossYear) {
    // Months from startMonth..12 are the first half of the season
    // Months from 1..endMonth are the second half
    const inFirstHalf = month >= config.startMonth;
    const inSecondHalf = month <= config.endMonth;

    if (inFirstHalf) {
      return config.useEndYear ? year + 1 : year;
    }
    if (inSecondHalf) {
      return config.useEndYear ? year : year - 1;
    }
    // Off-season: after endMonth, before startMonth
    // Treat as upcoming season
    return config.useEndYear ? year + 1 : year;
  }

  // Same-year sport (MLS, MLB, WNBA, tennis, esports, etc.)
  return year;
}

/**
 * Extract unique season labels from an array of date strings.
 */
export function getSeasonsFromDates(dates: string[], sport: Sport | string): number[] {
  const seasons = new Set<number>();
  for (const d of dates) {
    seasons.add(getSeasonFromDate(d, sport));
  }
  return Array.from(seasons).sort((a, b) => a - b);
}

/**
 * Get approximate start and end dates for a season.
 */
export function getSeasonBoundaries(
  season: number,
  sport: Sport | string,
): { startDate: Date; endDate: Date } {
  const config = SEASON_CONFIGS[sport.toLowerCase()];
  if (!config) {
    return {
      startDate: new Date(season, 0, 1),
      endDate: new Date(season, 11, 31),
    };
  }

  const crossYear = config.endMonth < config.startMonth;

  let startYear: number;
  let endYear: number;

  if (crossYear) {
    if (config.useEndYear) {
      // season label = end year → start year is season - 1
      startYear = season - 1;
      endYear = season;
    } else {
      // season label = start year → end year is season + 1
      startYear = season;
      endYear = season + 1;
    }
  } else {
    startYear = season;
    endYear = season;
  }

  return {
    startDate: new Date(startYear, config.startMonth - 1, config.startDay),
    endDate: new Date(endYear, config.endMonth - 1, config.endDay),
  };
}

/** List all sports that have season configuration. */
export function getSupportedSports(): string[] {
  return Object.keys(SEASON_CONFIGS);
}
