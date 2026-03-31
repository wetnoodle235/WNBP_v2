/**
 * Sport definitions matching the backend SPORT_DEFINITIONS.
 * Colors reference CSS custom properties from globals.css.
 */

export type SportKey =
  | "nba" | "wnba" | "ncaab" | "ncaaw"
  | "nfl" | "ncaaf"
  | "mlb"
  | "nhl"
  | "epl" | "laliga" | "bundesliga" | "seriea" | "ligue1" | "mls" | "ucl" | "nwsl"
  | "f1"
  | "atp" | "wta"
  | "ufc"
  | "lol" | "csgo" | "dota2" | "valorant"
  | "golf";

export type SportCategory =
  | "basketball" | "football" | "baseball" | "hockey"
  | "soccer" | "motorsport" | "tennis" | "mma"
  | "esports" | "golf";

export interface SportDef {
  label: string;
  color: string;
  href: string;
  category: SportCategory;
  country: string;
}

export const SPORTS: Record<SportKey, SportDef> = {
  // Basketball
  nba:        { label: "NBA",        color: "var(--color-nba)",        href: "/nba",        category: "basketball", country: "US" },
  wnba:       { label: "WNBA",       color: "var(--color-wnba)",       href: "/wnba",       category: "basketball", country: "US" },
  ncaab:      { label: "NCAAB",      color: "var(--color-ncaab)",      href: "/ncaab",      category: "basketball", country: "US" },
  ncaaw:      { label: "NCAAW",      color: "var(--color-ncaaw)",      href: "/ncaaw",      category: "basketball", country: "US" },

  // Football
  nfl:        { label: "NFL",        color: "var(--color-nfl)",        href: "/nfl",        category: "football",   country: "US" },
  ncaaf:      { label: "NCAAF",      color: "var(--color-ncaaf)",      href: "/ncaaf",      category: "football",   country: "US" },

  // Baseball
  mlb:        { label: "MLB",        color: "var(--color-mlb)",        href: "/mlb",        category: "baseball",   country: "US" },

  // Hockey
  nhl:        { label: "NHL",        color: "var(--color-nhl)",        href: "/nhl",        category: "hockey",     country: "US" },

  // Soccer
  epl:        { label: "EPL",        color: "var(--color-epl)",        href: "/epl",        category: "soccer",     country: "GB" },
  laliga:     { label: "La Liga",    color: "var(--color-laliga)",     href: "/laliga",     category: "soccer",     country: "ES" },
  bundesliga: { label: "Bundesliga", color: "var(--color-bundesliga)", href: "/bundesliga", category: "soccer",     country: "DE" },
  seriea:     { label: "Serie A",    color: "var(--color-seriea)",     href: "/seriea",     category: "soccer",     country: "IT" },
  ligue1:     { label: "Ligue 1",    color: "var(--color-ligue1)",     href: "/ligue1",     category: "soccer",     country: "FR" },
  mls:        { label: "MLS",        color: "var(--color-mls)",        href: "/mls",        category: "soccer",     country: "US" },
  ucl:        { label: "UCL",        color: "var(--color-ucl)",        href: "/ucl",        category: "soccer",     country: "EU" },
  nwsl:       { label: "NWSL",       color: "var(--color-nwsl)",       href: "/nwsl",       category: "soccer",     country: "US" },

  // Motorsport
  f1:         { label: "F1",         color: "var(--color-f1)",         href: "/f1",         category: "motorsport", country: "INT" },

  // Tennis
  atp:        { label: "ATP",        color: "var(--color-atp)",        href: "/atp",        category: "tennis",     country: "INT" },
  wta:        { label: "WTA",        color: "var(--color-wta)",        href: "/wta",        category: "tennis",     country: "INT" },

  // MMA
  ufc:        { label: "UFC",        color: "var(--color-mma)",        href: "/ufc",        category: "mma",        country: "US" },

  // Esports
  lol:        { label: "LoL",        color: "var(--color-lol)",        href: "/lol",        category: "esports",    country: "INT" },
  csgo:       { label: "CS2",        color: "var(--color-cs2)",        href: "/csgo",       category: "esports",    country: "INT" },
  dota2:      { label: "Dota 2",     color: "var(--color-dota2)",      href: "/dota2",      category: "esports",    country: "INT" },
  valorant:   { label: "Valorant",   color: "var(--color-valorant)",   href: "/valorant",   category: "esports",    country: "INT" },

  // Golf
  golf:       { label: "PGA",        color: "var(--color-pga)",        href: "/golf",       category: "golf",       country: "US" },
};

export const ALL_SPORT_KEYS = Object.keys(SPORTS) as SportKey[];

export function isSportKey(s: string): s is SportKey {
  return s in SPORTS;
}

export function getSportDef(s: string): SportDef | undefined {
  return isSportKey(s) ? SPORTS[s] : undefined;
}

/** Get raw hex color for a sport (for inline styles where CSS vars don't work) */
const SPORT_HEX_COLORS: Record<string, string> = {
  nba: "#0055af", wnba: "#1d428a", ncaab: "#0c2661", ncaaw: "#7c2c3d",
  nfl: "#013369", ncaaf: "#003055",
  mlb: "#002d72",
  nhl: "#000000",
  epl: "#38003c", laliga: "#0066cc", bundesliga: "#d00000", seriea: "#1f4e8c",
  ligue1: "#0052cc", mls: "#0a2d4a", ucl: "#001f5b", nwsl: "#c72c48",
  f1: "#e10600",
  atp: "#0066cc", wta: "#a0006e",
  ufc: "#8b0000",
  lol: "#0a42d4", csgo: "#1f4e2f", dota2: "#922f1f", valorant: "#ff4655",
  golf: "#1a6b1a",
};

export function getSportHexColor(sport: string): string {
  return SPORT_HEX_COLORS[sport] ?? "#666666";
}

/** Nav categories for TopNav sport dropdowns */
export const NAV_CATEGORIES = [
  { label: "Basketball", sports: ["nba", "wnba", "ncaab", "ncaaw"] as SportKey[] },
  { label: "Football",   sports: ["nfl", "ncaaf"] as SportKey[] },
  { label: "Baseball",   sports: ["mlb"] as SportKey[] },
  { label: "Hockey",     sports: ["nhl"] as SportKey[] },
  { label: "Soccer",     sports: ["epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl"] as SportKey[] },
  { label: "Tennis",     sports: ["atp", "wta"] as SportKey[] },
  { label: "F1",         sports: ["f1"] as SportKey[] },
  { label: "MMA",        sports: ["ufc"] as SportKey[] },
  { label: "Esports",    sports: ["lol", "csgo", "dota2", "valorant"] as SportKey[] },
  { label: "Golf",       sports: ["golf"] as SportKey[] },
] as const;
