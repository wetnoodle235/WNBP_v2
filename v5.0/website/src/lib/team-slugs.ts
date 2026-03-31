/**
 * Team name → ESPN CDN slug mappings.
 */

const NBA_TEAM_SLUG: Record<string, string> = {
  hawks: "atl", celtics: "bos", nets: "bkn", hornets: "cha", bulls: "chi",
  cavaliers: "cle", mavericks: "dal", nuggets: "den", pistons: "det",
  warriors: "gs", rockets: "hou", pacers: "ind", clippers: "lac",
  lakers: "lal", grizzlies: "mem", heat: "mia", bucks: "mil",
  timberwolves: "min", pelicans: "no", knicks: "ny", thunder: "okc",
  magic: "orl", "76ers": "phi", suns: "phx", blazers: "por",
  trail_blazers: "por", kings: "sac", spurs: "sa", raptors: "tor",
  jazz: "uta", wizards: "wsh",
};

const NBA_ABBREV_FIX: Record<string, string> = {
  nop: "no", gsw: "gs", sas: "sa", nyk: "ny", bkn: "bkn",
  phx: "phx", uta: "uta", wsh: "wsh",
};

const MLB_TEAM_SLUG: Record<string, string> = {
  diamondbacks: "ari", braves: "atl", orioles: "bal", "red sox": "bos",
  cubs: "chc", "white sox": "chw", reds: "cin", guardians: "cle",
  rockies: "col", tigers: "det", astros: "hou", royals: "kc",
  angels: "laa", dodgers: "lad", marlins: "mia", brewers: "mil",
  twins: "min", mets: "nym", yankees: "nyy", athletics: "oak",
  phillies: "phi", pirates: "pit", padres: "sd", giants: "sf",
  mariners: "sea", cardinals: "stl", rays: "tb", rangers: "tex",
  "blue jays": "tor", nationals: "wsh",
};

export function toTeamSlug(name: string, sport: string, abbrev?: string): string {
  const lo = name.toLowerCase();
  const sportLo = sport.toLowerCase();

  if (abbrev) {
    const fixed = NBA_ABBREV_FIX[abbrev.toLowerCase()];
    if (fixed) return fixed;
    return abbrev.toLowerCase();
  }

  if (sportLo === "nba" || sportLo === "wnba" || sportLo === "ncaab" || sportLo === "ncaaw") {
    for (const [key, slug] of Object.entries(NBA_TEAM_SLUG)) {
      if (lo.includes(key)) return slug;
    }
  }

  if (sportLo === "mlb") {
    for (const [key, slug] of Object.entries(MLB_TEAM_SLUG)) {
      if (lo.includes(key)) return slug;
    }
  }

  // Fallback: use first 3 chars of last word
  const parts = lo.split(/\s+/);
  return parts[parts.length - 1].slice(0, 3);
}

export function toCanonicalAbbrev(abbrev: string): string {
  const fixed = NBA_ABBREV_FIX[abbrev.toLowerCase()];
  return fixed ?? abbrev.toLowerCase();
}
