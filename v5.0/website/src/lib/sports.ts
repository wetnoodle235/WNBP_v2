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
  | "ligamx" | "europa" | "eredivisie" | "primeiraliga" | "championship"
  | "bundesliga2" | "serieb" | "ligue2" | "worldcup" | "euros"
  | "f1" | "indycar"
  | "atp" | "wta"
  | "ufc"
  | "lol" | "csgo" | "dota2" | "valorant"
  | "golf" | "lpga" | "pga";

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
  ligamx:     { label: "Liga MX",    color: "var(--color-ligamx, #0f7a3d)", href: "/ligamx", category: "soccer", country: "MX" },
  europa:     { label: "Europa",     color: "var(--color-europa, #b07a00)", href: "/europa", category: "soccer", country: "EU" },
  eredivisie: { label: "Eredivisie", color: "var(--color-eredivisie, #e67e00)", href: "/eredivisie", category: "soccer", country: "NL" },
  primeiraliga: { label: "Primeira Liga", color: "var(--color-primeiraliga, #00723f)", href: "/primeiraliga", category: "soccer", country: "PT" },
  championship: { label: "Championship", color: "var(--color-championship, #003a70)", href: "/championship", category: "soccer", country: "GB" },
  bundesliga2: { label: "Bundesliga 2", color: "var(--color-bundesliga2, #b00020)", href: "/bundesliga2", category: "soccer", country: "DE" },
  serieb:      { label: "Serie B", color: "var(--color-serieb, #2f5fa8)", href: "/serieb", category: "soccer", country: "IT" },
  ligue2:      { label: "Ligue 2", color: "var(--color-ligue2, #0b3b75)", href: "/ligue2", category: "soccer", country: "FR" },
  worldcup:    { label: "World Cup", color: "var(--color-worldcup, #822433)", href: "/worldcup", category: "soccer", country: "INT" },
  euros:       { label: "Euros", color: "var(--color-euros, #004f9f)", href: "/euros", category: "soccer", country: "EU" },

  // Motorsport
  f1:         { label: "F1",         color: "var(--color-f1)",         href: "/f1",         category: "motorsport", country: "INT" },
  indycar:    { label: "IndyCar",    color: "var(--color-indycar, #0056a2)", href: "/indycar", category: "motorsport", country: "US" },

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
  golf:       { label: "PGA Tour",   color: "var(--color-pga)",        href: "/golf",       category: "golf",       country: "US" },
  lpga:       { label: "LPGA",       color: "var(--color-lpga, #6a0dad)", href: "/lpga",    category: "golf",       country: "US" },
  pga:        { label: "PGA",        color: "var(--color-pga)",        href: "/pga",        category: "golf",       country: "US" },
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
  ligamx: "#0f7a3d", europa: "#b07a00", eredivisie: "#e67e00", primeiraliga: "#00723f",
  championship: "#003a70", bundesliga2: "#b00020", serieb: "#2f5fa8", ligue2: "#0b3b75",
  worldcup: "#822433", euros: "#004f9f",
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
  {
    label: "Soccer",
    sports: [
      "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl",
      "ligamx", "europa", "eredivisie", "primeiraliga", "championship",
      "bundesliga2", "serieb", "ligue2", "worldcup", "euros",
    ] as SportKey[],
  },
  { label: "Tennis",     sports: ["atp", "wta"] as SportKey[] },
  { label: "Racing",     sports: ["f1", "indycar"] as SportKey[] },
  { label: "MMA",        sports: ["ufc"] as SportKey[] },
  { label: "Esports",    sports: ["lol", "csgo", "dota2", "valorant"] as SportKey[] },
  { label: "Golf",       sports: ["golf", "lpga"] as SportKey[] },
] as const;
