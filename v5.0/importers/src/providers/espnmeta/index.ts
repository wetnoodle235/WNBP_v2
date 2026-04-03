// ──────────────────────────────────────────────────────────
// ESPN Metadata Provider
// ──────────────────────────────────────────────────────────
// Collects team & player metadata from ESPN's public site API:
// team logos (multiple variants), brand colors, player headshots,
// roster positions, injury status, jersey numbers.
// Zero rate-limiting issues — all public CDN endpoints.
// High frontend value: logos, colors, headshots in one place.

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "espnmeta";
const SITE_API = "https://site.api.espn.com/apis/site/v2/sports";
const RATE_LIMIT: RateLimitConfig = { requests: 4, perMs: 1_000 };

// ESPN sport/league path segments
const SPORT_PATHS: Partial<Record<Sport, { sport: string; league: string }>> = {
  nfl:        { sport: "football",   league: "nfl" },
  ncaaf:      { sport: "football",   league: "college-football" },
  nba:        { sport: "basketball", league: "nba" },
  wnba:       { sport: "basketball", league: "wnba" },
  ncaab:      { sport: "basketball", league: "mens-college-basketball" },
  mlb:        { sport: "baseball",   league: "mlb" },
  nhl:        { sport: "hockey",     league: "nhl" },
  ufc:        { sport: "mma",        league: "ufc" },
  golf:       { sport: "golf",       league: "pga" },
  epl:        { sport: "soccer",     league: "eng.1" },
  laliga:     { sport: "soccer",     league: "esp.1" },
  bundesliga: { sport: "soccer",     league: "ger.1" },
  seriea:     { sport: "soccer",     league: "ita.1" },
  ligue1:     { sport: "soccer",     league: "fra.1" },
  mls:        { sport: "soccer",     league: "usa.1" },
  ucl:        { sport: "soccer",     league: "uefa.champions" },
  europa:     { sport: "soccer",     league: "uefa.europa" },
  nwsl:       { sport: "soccer",     league: "usa.nwsl" },
};

const SUPPORTED_SPORTS = Object.keys(SPORT_PATHS) as Sport[];

interface EspnLogo {
  href: string;
  rel: string[];
  width: number;
  height: number;
}

interface EspnTeamRaw {
  id: string;
  uid: string;
  slug: string;
  abbreviation: string;
  displayName: string;
  shortDisplayName: string;
  name: string;
  nickname?: string;
  location: string;
  color?: string;
  alternateColor?: string;
  isActive: boolean;
  logos?: EspnLogo[];
  links?: Array<{ rel: string[]; href: string }>;
}

interface EspnAthleteRaw {
  id: string;
  uid: string;
  displayName: string;
  shortName?: string;
  firstName?: string;
  lastName?: string;
  fullName?: string;
  weight?: number;
  height?: number;
  age?: number;
  dateOfBirth?: string;
  birthPlace?: { city?: string; state?: string; country?: string };
  jersey?: string;
  active?: boolean;
  status?: { id: string; name: string; type: string; abbreviation: string };
  position?: { id: string; name: string; displayName: string; abbreviation: string };
  headshot?: { href: string; alt: string };
  team?: { id: string; displayName: string; abbreviation: string };
  [key: string]: unknown;
}

async function fetchTeams(sport: string, league: string): Promise<EspnTeamRaw[]> {
  const data = await fetchJSON<{
    sports: Array<{ leagues: Array<{ teams: Array<{ team: EspnTeamRaw }> }> }>;
  }>(`${SITE_API}/${sport}/${league}/teams?limit=200`, NAME, RATE_LIMIT);
  return (data?.sports?.[0]?.leagues?.[0]?.teams ?? []).map((t) => t.team);
}

async function fetchRoster(sport: string, league: string, teamId: string): Promise<EspnAthleteRaw[]> {
  const data = await fetchJSON<{
    athletes: Array<{ items?: EspnAthleteRaw[] } | EspnAthleteRaw[]>;
  }>(`${SITE_API}/${sport}/${league}/teams/${teamId}/roster`, NAME, RATE_LIMIT);

  const athleteGroups = data?.athletes ?? [];
  const all: EspnAthleteRaw[] = [];
  for (const group of athleteGroups) {
    if (Array.isArray(group)) {
      all.push(...group);
    } else if (group && typeof group === "object" && "items" in group) {
      all.push(...(group.items ?? []));
    }
  }
  return all;
}

function buildLogoMap(logos: EspnLogo[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (const logo of logos) {
    const key = logo.rel.filter((r) => r !== "full").join("_") || "default";
    if (!map[key]) map[key] = logo.href;
  }
  return map;
}

const espnmeta: Provider = {
  name: NAME,
  label: "ESPN Metadata (team logos, colors, player headshots)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["teams", "rosters"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    // Metadata is not season-scoped
    const season = Math.max(...opts.seasons);

    for (const sport of activeSports) {
      const paths = SPORT_PATHS[sport];
      if (!paths) continue;

      try {
        const teams = await fetchTeams(paths.sport, paths.league);
        if (teams.length === 0) {
          logger.warn(`No teams found for ${sport}`, NAME);
          continue;
        }

        const teamsMapped = teams.map((t) => ({
          id: t.id,
          uid: t.uid,
          slug: t.slug,
          abbreviation: t.abbreviation,
          display_name: t.displayName,
          short_name: t.shortDisplayName,
          name: t.name,
          nickname: t.nickname,
          location: t.location,
          color: t.color ? `#${t.color}` : null,
          alternate_color: t.alternateColor ? `#${t.alternateColor}` : null,
          is_active: t.isActive,
          logos: t.logos ? buildLogoMap(t.logos) : {},
        }));

        if (!opts.dryRun) {
          const teamsPath = rawPath(opts.dataDir, NAME, sport, season, "teams.json");
          writeJSON(teamsPath, {
            source: NAME, sport, season: String(season),
            count: teamsMapped.length, fetched_at: new Date().toISOString(),
            teams: teamsMapped,
          });
          filesWritten++;
        }
        logger.progress(NAME, sport, "teams", `${teams.length} teams`);

        // Fetch rosters for sports where per-player headshots are valuable
        // Skip soccer leagues (too many teams, use page-level batch instead)
        const rosterSports: Sport[] = ["nfl", "nba", "mlb", "nhl", "wnba", "ncaab", "ncaaf"];
        if (rosterSports.includes(sport) && !opts.dryRun) {
          const allAthletes: unknown[] = [];
          for (const team of teams) {
            try {
              const roster = await fetchRoster(paths.sport, paths.league, team.id);
              allAthletes.push(...roster.map((a) => ({
                id: a.id,
                name: a.displayName,
                short_name: a.shortName,
                jersey: a.jersey,
                age: a.age,
                birth_date: a.dateOfBirth,
                height_in: a.height,
                weight_lbs: a.weight,
                position: a.position?.displayName,
                position_abbr: a.position?.abbreviation,
                status: a.status?.name,
                status_type: a.status?.type,
                headshot_url: a.headshot?.href,
                team_id: team.id,
                team_name: team.displayName,
                team_abbr: team.abbreviation,
              })));
            } catch { /* skip teams that fail roster fetch */ }
          }

          if (allAthletes.length > 0) {
            const rosterPath = rawPath(opts.dataDir, NAME, sport, season, "roster.json");
            writeJSON(rosterPath, {
              source: NAME, sport, season: String(season),
              count: allAthletes.length, fetched_at: new Date().toISOString(),
              athletes: allAthletes,
            });
            filesWritten++;
            logger.progress(NAME, sport, "rosters", `${allAthletes.length} players`);
          }
        }
      } catch (err) {
        const msg = `${sport}: ${err instanceof Error ? err.message : String(err)}`;
        logger.warn(msg, NAME);
        errors.push(msg);
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten, errors, durationMs: Date.now() - start,
    };
  },
};

export default espnmeta;
