// ──────────────────────────────────────────────────────────
// OpenLigaDB Provider
// ──────────────────────────────────────────────────────────
// Free API for German soccer (Bundesliga & 2. Bundesliga),
// covering full match data: scores, goals, kickoff times,
// match locations. No API key required.
// API: https://api.openligadb.de

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "openligadb";
const BASE = "https://api.openligadb.de";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_000 };

const SPORT_LEAGUES: Partial<Record<Sport, Array<{ code: string; label: string }>>> = {
  bundesliga: [
    { code: "bl1", label: "bundesliga" },
    { code: "bl2", label: "2_bundesliga" },
  ],
};

const SUPPORTED_SPORTS = Object.keys(SPORT_LEAGUES) as Sport[];

interface OLDBGoal {
  goalID: number;
  comment: string | null;
  matchMinute: number | null;
  scoreTeam1: number;
  scoreTeam2: number;
  isOvertime: boolean;
  isOwnGoal: boolean;
  isPenalty: boolean;
  scorer: { playerId: number; playerName: string } | null;
}

interface OLDBTeam { teamId: number; teamName: string; shortName: string; teamIconUrl: string | null }

interface OLDBMatch {
  matchID: number;
  matchDateTimeUTC: string;
  group: { groupName: string; groupID: number; groupOrderID: number };
  team1: OLDBTeam;
  team2: OLDBTeam;
  matchResults: Array<{ pointsTeam1: number; pointsTeam2: number; resultTypeID: number }>;
  goals: OLDBGoal[];
  matchIsFinished: boolean;
  numberOfViewers: number | null;
}

function normalizeMatch(m: OLDBMatch): Record<string, unknown> {
  const final = m.matchResults.find((r) => r.resultTypeID === 2) ?? m.matchResults[0];
  return {
    match_id: m.matchID,
    date: m.matchDateTimeUTC,
    matchday: m.group.groupOrderID,
    matchday_name: m.group.groupName,
    home_team: m.team1.teamName,
    home_team_short: m.team1.shortName,
    away_team: m.team2.teamName,
    away_team_short: m.team2.shortName,
    home_score: final?.pointsTeam1 ?? null,
    away_score: final?.pointsTeam2 ?? null,
    finished: m.matchIsFinished,
    goals: m.goals.map((g) => ({
      minute: g.matchMinute,
      scorer: g.scorer?.playerName ?? null,
      home_score: g.scoreTeam1,
      away_score: g.scoreTeam2,
      own_goal: g.isOwnGoal,
      penalty: g.isPenalty,
      overtime: g.isOvertime,
    })),
    attendance: m.numberOfViewers,
  };
}

async function importLeague(
  sport: Sport,
  leagueCode: string,
  label: string,
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  if (dryRun) return { filesWritten: 0, errors: [] };

  const outPath = rawPath(dataDir, NAME, sport, season, `${label}.json`);
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  try {
    const data = await fetchJSON<OLDBMatch[]>(
      `${BASE}/getmatchdata/${leagueCode}/${season}`,
      NAME,
      RATE_LIMIT,
    );

    if (!Array.isArray(data) || data.length === 0) {
      return { filesWritten: 0, errors: [] };
    }

    const matches = data.map(normalizeMatch);
    writeJSON(outPath, {
      source: NAME,
      sport,
      season: String(season),
      league: label,
      league_code: leagueCode,
      count: matches.length,
      matches,
      fetched_at: new Date().toISOString(),
    });

    logger.progress(NAME, sport, label, `${matches.length} matches (${season})`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    const msg = `${label}/${season}: ${err instanceof Error ? err.message : String(err)}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }
}

const openligadb: Provider = {
  name: NAME,
  label: "OpenLigaDB (Bundesliga)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["matches"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    for (const sport of activeSports) {
      const leagues = SPORT_LEAGUES[sport] ?? [];
      for (const season of opts.seasons) {
        for (const { code, label } of leagues) {
          const r = await importLeague(sport, code, label, season, opts.dataDir, opts.dryRun);
          totalFiles += r.filesWritten;
          allErrors.push(...r.errors);
        }
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default openligadb;
