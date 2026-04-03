// ──────────────────────────────────────────────────────────
// TheSportsDB Provider
// ──────────────────────────────────────────────────────────
// Free public API (key=3) — team badges, logos, brand colors,
// fanart, player headshots & cutouts, venue photos, multi-language
// descriptions. High frontend enrichment value.
// Paid Patreon key unlocks higher resolution + more endpoints.

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "thesportsdb";
const API_KEY = process.env.THESPORTSDB_KEY ?? "3";
const BASE = `https://www.thesportsdb.com/api/v1/json/${API_KEY}`;
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_000 };

const LEAGUE_IDS: Partial<Record<Sport, { id: string; name: string }[]>> = {
  epl:        [{ id: "4328", name: "English Premier League" }],
  bundesliga: [{ id: "4331", name: "German Bundesliga" }],
  laliga:     [{ id: "4335", name: "Spanish La Liga" }],
  seriea:     [{ id: "4332", name: "Italian Serie A" }],
  ligue1:     [{ id: "4334", name: "French Ligue 1" }],
  mls:        [{ id: "4346", name: "Major League Soccer" }],
  ucl:        [{ id: "4480", name: "UEFA Champions League" }],
  nba:        [{ id: "4387", name: "NBA" }],
  wnba:       [{ id: "4389", name: "WNBA" }],
  nfl:        [{ id: "4391", name: "NFL" }],
  mlb:        [{ id: "4424", name: "MLB" }],
  nhl:        [{ id: "4380", name: "NHL" }],
  ncaab:      [{ id: "4386", name: "NCAA Basketball" }],
  ncaaf:      [{ id: "4390", name: "NCAA Football" }],
  atp:        [{ id: "4607", name: "ATP World Tour" }],
  f1:         [{ id: "4370", name: "Formula 1" }],
  ufc:        [{ id: "4443", name: "UFC" }],
};

const SUPPORTED_SPORTS = Object.keys(LEAGUE_IDS) as Sport[];

interface TeamRecord {
  idTeam: string;
  strTeam: string;
  strTeamShort?: string;
  strTeamAlternate?: string;
  intFormedYear?: string;
  strStadium?: string;
  idVenue?: string;
  intStadiumCapacity?: string;
  strLocation?: string;
  strWebsite?: string;
  strColour1?: string;
  strColour2?: string;
  strColour3?: string;
  strBadge?: string;
  strLogo?: string;
  strFanart1?: string;
  strFanart2?: string;
  strBanner?: string;
  strDescriptionEN?: string;
  strKeywords?: string;
  [key: string]: string | undefined;
}

interface PlayerRecord {
  idPlayer: string;
  strPlayer: string;
  strTeam?: string;
  idTeam?: string;
  strNationality?: string;
  strPosition?: string;
  strStatus?: string;
  dateBorn?: string;
  strHeight?: string;
  strWeight?: string;
  strThumb?: string;
  strCutout?: string;
  strDescriptionEN?: string;
  [key: string]: string | undefined;
}

async function fetchTeamsByLeague(leagueId: string): Promise<TeamRecord[]> {
  const data = await fetchJSON<{ teams: TeamRecord[] | null }>(
    `${BASE}/lookup_all_teams.php?id=${leagueId}`, NAME, RATE_LIMIT,
  );
  return data?.teams ?? [];
}

async function fetchPlayersByTeam(teamId: string): Promise<PlayerRecord[]> {
  const data = await fetchJSON<{ player: PlayerRecord[] | null }>(
    `${BASE}/lookup_all_players.php?id=${teamId}`, NAME, RATE_LIMIT,
  );
  return data?.player ?? [];
}

const thesportsdb: Provider = {
  name: NAME,
  label: "TheSportsDB (team/player metadata + images)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["teams", "players"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    // Metadata is not season-scoped — use the most recent season requested
    const season = Math.max(...opts.seasons);

    for (const sport of activeSports) {
      const leagues = LEAGUE_IDS[sport] ?? [];
      for (const league of leagues) {
        try {
          const teams = await fetchTeamsByLeague(league.id);
          if (teams.length === 0) continue;

          if (!opts.dryRun) {
            const teamsPath = rawPath(opts.dataDir, NAME, sport, season, `teams_${league.id}.json`);
            writeJSON(teamsPath, {
              source: NAME, sport, league: league.name, league_id: league.id,
              season: String(season), count: teams.length,
              fetched_at: new Date().toISOString(),
              teams: teams.map((t) => ({
                id: t.idTeam,
                name: t.strTeam,
                short: t.strTeamShort,
                alternate: t.strTeamAlternate,
                formed: t.intFormedYear,
                stadium: t.strStadium,
                stadium_id: t.idVenue,
                stadium_capacity: t.intStadiumCapacity,
                location: t.strLocation,
                website: t.strWebsite,
                color1: t.strColour1,
                color2: t.strColour2,
                color3: t.strColour3,
                badge_url: t.strBadge,
                logo_url: t.strLogo,
                fanart_url: t.strFanart1,
                fanart2_url: t.strFanart2,
                banner_url: t.strBanner,
                description: t.strDescriptionEN?.slice(0, 600),
                keywords: t.strKeywords,
              })),
            });
            filesWritten++;
          }
          logger.progress(NAME, sport, "teams", `${teams.length} teams — ${league.name}`);

          // Fetch roster/player images per team
          if (!opts.dryRun) {
            const allPlayers: PlayerRecord[] = [];
            for (const team of teams.slice(0, 32)) {
              try {
                const players = await fetchPlayersByTeam(team.idTeam);
                allPlayers.push(...players);
              } catch { /* skip teams with no player data */ }
            }
            if (allPlayers.length > 0) {
              const playersPath = rawPath(opts.dataDir, NAME, sport, season, `players_${league.id}.json`);
              writeJSON(playersPath, {
                source: NAME, sport, league: league.name, season: String(season),
                count: allPlayers.length, fetched_at: new Date().toISOString(),
                players: allPlayers.map((p) => ({
                  id: p.idPlayer,
                  name: p.strPlayer,
                  team: p.strTeam,
                  team_id: p.idTeam,
                  nationality: p.strNationality,
                  position: p.strPosition,
                  status: p.strStatus,
                  birth_date: p.dateBorn,
                  height: p.strHeight,
                  weight: p.strWeight,
                  thumb_url: p.strThumb,
                  cutout_url: p.strCutout,
                  description: p.strDescriptionEN?.slice(0, 400),
                })),
              });
              filesWritten++;
              logger.progress(NAME, sport, "players", `${allPlayers.length} players`);
            }
          }
        } catch (err) {
          const msg = `${sport}/${league.name}: ${err instanceof Error ? err.message : String(err)}`;
          logger.warn(msg, NAME);
          errors.push(msg);
        }
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten, errors, durationMs: Date.now() - start,
    };
  },
};

export default thesportsdb;
