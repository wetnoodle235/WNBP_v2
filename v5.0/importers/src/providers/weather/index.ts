// ──────────────────────────────────────────────────────────
// Visual Crossing Weather Provider
// ──────────────────────────────────────────────────────────
// Supplementary weather data for outdoor-sport venues.
// Requires VISUAL_CROSSING_KEY env var.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "weather";
const BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline";
const API_KEY = process.env.VISUAL_CROSSING_KEY ?? "";

const SPORTS: readonly Sport[] = [
  "nfl", "ncaaf", "mlb",
  "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls",
  "f1", "atp", "wta", "golf",
] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const ENDPOINTS = ["venue_weather"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

// ── Known venue locations by sport ─────────────────────────

interface VenueInfo {
  name: string;
  location: string; // city,state or lat,long for API
}

const VENUES: Partial<Record<Sport, VenueInfo[]>> = {
  nfl: [
    { name: "arrowhead",   location: "Kansas City,MO" },
    { name: "sofi",        location: "Inglewood,CA" },
    { name: "lambeau",     location: "Green Bay,WI" },
    { name: "gillette",    location: "Foxborough,MA" },
    { name: "metlife",     location: "East Rutherford,NJ" },
    { name: "lumen",       location: "Seattle,WA" },
    { name: "soldier",     location: "Chicago,IL" },
    { name: "caesars",     location: "New Orleans,LA" },
    { name: "empower",     location: "Denver,CO" },
    { name: "lincoln",     location: "Philadelphia,PA" },
    { name: "raymond_james", location: "Tampa,FL" },
    { name: "highmark",    location: "Orchard Park,NY" },
    { name: "bank_of_america", location: "Charlotte,NC" },
    { name: "nissan",      location: "Nashville,TN" },
    { name: "us_bank",     location: "Minneapolis,MN" },
    { name: "att",         location: "Arlington,TX" },
  ],
  ncaaf: [
    { name: "michigan_stadium", location: "Ann Arbor,MI" },
    { name: "beaver_stadium",   location: "State College,PA" },
    { name: "ohio_stadium",     location: "Columbus,OH" },
    { name: "tiger_stadium",    location: "Baton Rouge,LA" },
    { name: "neyland",          location: "Knoxville,TN" },
    { name: "rose_bowl",        location: "Pasadena,CA" },
    { name: "kyle_field",       location: "College Station,TX" },
    { name: "doak_campbell",    location: "Tallahassee,FL" },
  ],
  mlb: [
    { name: "yankee_stadium",  location: "Bronx,NY" },
    { name: "fenway",          location: "Boston,MA" },
    { name: "dodger_stadium",  location: "Los Angeles,CA" },
    { name: "wrigley",         location: "Chicago,IL" },
    { name: "oracle_park",     location: "San Francisco,CA" },
    { name: "coors_field",     location: "Denver,CO" },
    { name: "minute_maid",     location: "Houston,TX" },
    { name: "citizens_bank",   location: "Philadelphia,PA" },
    { name: "petco_park",      location: "San Diego,CA" },
    { name: "truist_park",     location: "Atlanta,GA" },
    { name: "busch_stadium",   location: "St. Louis,MO" },
    { name: "kauffman",        location: "Kansas City,MO" },
  ],
  epl: [
    { name: "old_trafford",   location: "Manchester,UK" },
    { name: "anfield",        location: "Liverpool,UK" },
    { name: "stamford_bridge", location: "London,UK" },
    { name: "emirates",       location: "London,UK" },
    { name: "etihad",         location: "Manchester,UK" },
    { name: "tottenham",      location: "London,UK" },
  ],
  laliga: [
    { name: "bernabeu",     location: "Madrid,Spain" },
    { name: "camp_nou",     location: "Barcelona,Spain" },
    { name: "metropolitano", location: "Madrid,Spain" },
  ],
  bundesliga: [
    { name: "signal_iduna",  location: "Dortmund,Germany" },
    { name: "allianz_arena", location: "Munich,Germany" },
  ],
  seriea: [
    { name: "san_siro",    location: "Milan,Italy" },
    { name: "olimpico",    location: "Rome,Italy" },
    { name: "allianz_turin", location: "Turin,Italy" },
  ],
  ligue1: [
    { name: "parc_des_princes", location: "Paris,France" },
    { name: "velodrome",        location: "Marseille,France" },
  ],
  mls: [
    { name: "dignity_health", location: "Carson,CA" },
    { name: "red_bull_arena", location: "Harrison,NJ" },
    { name: "mercedes_benz",  location: "Atlanta,GA" },
    { name: "providence_park", location: "Portland,OR" },
  ],
  f1: [
    { name: "monaco",    location: "Monte Carlo,Monaco" },
    { name: "silverstone", location: "Silverstone,UK" },
    { name: "monza",     location: "Monza,Italy" },
    { name: "spa",       location: "Stavelot,Belgium" },
    { name: "cota",      location: "Austin,TX" },
    { name: "suzuka",    location: "Suzuka,Japan" },
    { name: "interlagos", location: "Sao Paulo,Brazil" },
    { name: "melbourne", location: "Melbourne,Australia" },
  ],
  atp: [
    { name: "melbourne_park",  location: "Melbourne,Australia" },
    { name: "roland_garros",   location: "Paris,France" },
    { name: "wimbledon",       location: "London,UK" },
    { name: "flushing_meadows", location: "Flushing,NY" },
    { name: "indian_wells",    location: "Indian Wells,CA" },
  ],
  wta: [
    { name: "melbourne_park",  location: "Melbourne,Australia" },
    { name: "roland_garros",   location: "Paris,France" },
    { name: "wimbledon",       location: "London,UK" },
    { name: "flushing_meadows", location: "Flushing,NY" },
  ],
  golf: [
    { name: "augusta",        location: "Augusta,GA" },
    { name: "pebble_beach",   location: "Pebble Beach,CA" },
    { name: "st_andrews",     location: "St Andrews,UK" },
    { name: "sawgrass",       location: "Ponte Vedra Beach,FL" },
    { name: "pinehurst",      location: "Pinehurst,NC" },
  ],
};

/** Season date ranges vary by sport for weather sampling. */
function seasonMonths(sport: Sport, season: number): string[] {
  const months: string[] = [];
  const addMonth = (y: number, m: number) => {
    months.push(`${y}-${String(m).padStart(2, "0")}-15`);
  };

  switch (sport) {
    case "nfl":
    case "ncaaf":
      // Sep–Jan
      for (let m = 9; m <= 12; m++) addMonth(season, m);
      addMonth(season + 1, 1);
      if (sport === "nfl") addMonth(season + 1, 2);
      break;
    case "mlb":
      // Apr–Oct
      for (let m = 4; m <= 10; m++) addMonth(season, m);
      break;
    case "epl": case "laliga": case "bundesliga": case "seriea": case "ligue1": case "mls":
      // Aug–May
      for (let m = 8; m <= 12; m++) addMonth(season, m);
      for (let m = 1; m <= 5; m++) addMonth(season + 1, m);
      break;
    case "f1":
      // Mar–Nov
      for (let m = 3; m <= 11; m++) addMonth(season, m);
      break;
    case "atp": case "wta":
      // Jan–Nov
      for (let m = 1; m <= 11; m++) addMonth(season, m);
      break;
    case "golf":
      // Jan–Oct
      for (let m = 1; m <= 10; m++) addMonth(season, m);
      break;
    default:
      for (let m = 1; m <= 12; m++) addMonth(season, m);
  }

  return months;
}

// ── Endpoint handler ───────────────────────────────────────

interface EndpointCtx {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

async function importVenueWeather(ctx: EndpointCtx): Promise<EndpointResult> {
  const venues = VENUES[ctx.sport];
  if (!venues || venues.length === 0) {
    logger.info(`No venues configured for ${ctx.sport}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  let filesWritten = 0;
  const errors: string[] = [];
  const dates = seasonMonths(ctx.sport, ctx.season);

  for (const venue of venues) {
    for (const date of dates) {
      const locationSlug = venue.name;
      const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, locationSlug, `${date}.json`);

      if (fileExists(outPath)) continue;
      if (ctx.dryRun) continue;

      try {
        const encodedLocation = encodeURIComponent(venue.location);
        const url = `${BASE_URL}/${encodedLocation}/${date}?unitGroup=us&include=hours&key=${API_KEY}&contentType=json`;
        const data = await fetchJSON(url, NAME, RATE_LIMIT);
        writeJSON(outPath, data);
        filesWritten++;
      } catch (err) {
        const msg = `${venue.name}/${date}: ${err instanceof Error ? err.message : String(err)}`;
        errors.push(msg);
      }
    }

    if (filesWritten > 0) {
      logger.progress(NAME, ctx.sport, "venue_weather", `${venue.name}: ${filesWritten} files`);
    }
  }

  logger.progress(NAME, ctx.sport, "venue_weather", `${filesWritten} total for ${ctx.season}`);
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  venue_weather: importVenueWeather,
};

// ── Provider ───────────────────────────────────────────────

const weather: Provider = {
  name: NAME,
  label: "Visual Crossing Weather",
  sports: SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    if (!API_KEY) {
      logger.error("VISUAL_CROSSING_KEY not set — skipping", NAME);
      return { provider: NAME, sport: "multi", filesWritten: 0, errors: ["Missing VISUAL_CROSSING_KEY"], durationMs: 0 };
    }

    const activeSports = opts.sports.length
      ? opts.sports.filter((s): s is Sport => (SPORTS as readonly Sport[]).includes(s))
      : [...SPORTS];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        logger.info(`${sport} / ${season}`, NAME);
        for (const ep of activeEndpoints) {
          try {
            const result = await ENDPOINT_FNS[ep]({ sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun });
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = `${ep}/${sport}/${season}: ${err instanceof Error ? err.message : String(err)}`;
            logger.error(msg, NAME);
            allErrors.push(msg);
          }
        }
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0] : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default weather;
