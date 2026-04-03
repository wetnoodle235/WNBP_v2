/**
 * Sport display configuration — provides display names, full names, icons,
 * colors, and category groupings for all supported sports.
 */

interface SportDisplayInfo {
  name: string;
  fullName: string;
  icon: string;
  color: string;
  category: string;
}

const SPORT_DISPLAY_CONFIG: Record<string, SportDisplayInfo> = {
  // Major US Sports
  nba: { name: "NBA", fullName: "National Basketball Association", icon: "🏀", color: "#c9082a", category: "Major US Sports" },
  mlb: { name: "MLB", fullName: "Major League Baseball", icon: "⚾", color: "#002d72", category: "Major US Sports" },
  nfl: { name: "NFL", fullName: "National Football League", icon: "🏈", color: "#013369", category: "Major US Sports" },
  nhl: { name: "NHL", fullName: "National Hockey League", icon: "🏒", color: "#000000", category: "Major US Sports" },
  wnba: { name: "WNBA", fullName: "Women's National Basketball Association", icon: "🏀", color: "#ff6600", category: "Major US Sports" },

  // College Sports
  ncaab: { name: "NCAAB", fullName: "NCAA Men's Basketball", icon: "🏀", color: "#0a2240", category: "College Sports" },
  ncaaw: { name: "NCAAW", fullName: "NCAA Women's Basketball", icon: "🏀", color: "#7a003c", category: "College Sports" },
  ncaaf: { name: "NCAAF", fullName: "NCAA Football", icon: "🏈", color: "#1a1a2e", category: "College Sports" },

  // Soccer
  mls: { name: "MLS", fullName: "Major League Soccer", icon: "⚽", color: "#1b365d", category: "Soccer" },
  epl: { name: "EPL", fullName: "English Premier League", icon: "⚽", color: "#3d195b", category: "Soccer" },
  bundesliga: { name: "Bundesliga", fullName: "Bundesliga (Germany)", icon: "⚽", color: "#d20515", category: "Soccer" },
  laliga: { name: "La Liga", fullName: "La Liga (Spain)", icon: "⚽", color: "#ee8707", category: "Soccer" },
  ligue1: { name: "Ligue 1", fullName: "Ligue 1 (France)", icon: "⚽", color: "#091c3e", category: "Soccer" },
  seriea: { name: "Serie A", fullName: "Serie A (Italy)", icon: "⚽", color: "#024494", category: "Soccer" },
  ucl: { name: "UCL", fullName: "UEFA Champions League", icon: "⚽", color: "#081c3b", category: "Soccer" },
  nwsl: { name: "NWSL", fullName: "National Women's Soccer League", icon: "⚽", color: "#003087", category: "Soccer" },
  ligamx: { name: "Liga MX", fullName: "Liga MX (Mexico)", icon: "⚽", color: "#0f7a3d", category: "Soccer" },
  europa: { name: "Europa", fullName: "UEFA Europa League", icon: "⚽", color: "#b07a00", category: "Soccer" },
  eredivisie: { name: "Eredivisie", fullName: "Eredivisie (Netherlands)", icon: "⚽", color: "#e67e00", category: "Soccer" },
  primeiraliga: { name: "Primeira Liga", fullName: "Primeira Liga (Portugal)", icon: "⚽", color: "#00723f", category: "Soccer" },
  championship: { name: "Championship", fullName: "EFL Championship (England)", icon: "⚽", color: "#003a70", category: "Soccer" },
  bundesliga2: { name: "Bundesliga 2", fullName: "2. Bundesliga (Germany)", icon: "⚽", color: "#b00020", category: "Soccer" },
  serieb: { name: "Serie B", fullName: "Serie B (Italy)", icon: "⚽", color: "#2f5fa8", category: "Soccer" },
  ligue2: { name: "Ligue 2", fullName: "Ligue 2 (France)", icon: "⚽", color: "#0b3b75", category: "Soccer" },
  worldcup: { name: "World Cup", fullName: "FIFA World Cup", icon: "⚽", color: "#822433", category: "Soccer" },
  euros: { name: "Euros", fullName: "UEFA European Championship", icon: "⚽", color: "#004f9f", category: "Soccer" },

  // Individual Sports
  atp: { name: "ATP", fullName: "ATP Tour (Men's Tennis)", icon: "🎾", color: "#00a651", category: "Individual Sports" },
  wta: { name: "WTA", fullName: "WTA Tour (Women's Tennis)", icon: "🎾", color: "#71205e", category: "Individual Sports" },
  pga: { name: "PGA", fullName: "PGA Tour", icon: "⛳", color: "#003865", category: "Individual Sports" },
  mma: { name: "MMA", fullName: "Mixed Martial Arts", icon: "🥊", color: "#d20a0a", category: "Individual Sports" },
  f1: { name: "F1", fullName: "Formula 1", icon: "🏎️", color: "#e10600", category: "Racing" },
  indycar: { name: "IndyCar", fullName: "NTT IndyCar Series", icon: "🏎️", color: "#0056a2", category: "Racing" },
  ufc: { name: "UFC", fullName: "Ultimate Fighting Championship", icon: "🥊", color: "#d20a0a", category: "Individual Sports" },
  golf: { name: "PGA Tour", fullName: "PGA Tour", icon: "⛳", color: "#003865", category: "Golf" },
  lpga: { name: "LPGA", fullName: "LPGA Tour", icon: "⛳", color: "#6a0dad", category: "Golf" },

  // Esports
  cs2: { name: "CS2", fullName: "Counter-Strike 2", icon: "🎮", color: "#de9b35", category: "Esports" },
  lol: { name: "LoL", fullName: "League of Legends", icon: "🎮", color: "#00a0e3", category: "Esports" },
  dota2: { name: "Dota 2", fullName: "Dota 2", icon: "🎮", color: "#b8342a", category: "Esports" },
  valorant: { name: "Valorant", fullName: "Valorant", icon: "🎮", color: "#ff4655", category: "Esports" },
  csgo: { name: "CS:GO", fullName: "Counter-Strike 2", icon: "🎮", color: "#de9b35", category: "Esports" },
};

export const SPORT_CATEGORIES = [
  { label: "Major US Sports", icon: "🇺🇸", sports: ["nba", "nfl", "mlb", "nhl", "wnba"] },
  { label: "College Sports", icon: "🎓", sports: ["ncaab", "ncaaw", "ncaaf"] },
  {
    label: "Soccer",
    icon: "⚽",
    sports: [
      "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl",
      "ligamx", "europa", "eredivisie", "primeiraliga", "championship",
      "bundesliga2", "serieb", "ligue2", "worldcup", "euros",
    ],
  },
  { label: "Individual Sports", icon: "🏆", sports: ["atp", "wta", "ufc", "mma"] },
  { label: "Racing", icon: "🏎️", sports: ["f1", "indycar"] },
  { label: "Golf", icon: "⛳", sports: ["golf", "lpga", "pga"] },
  { label: "Esports", icon: "🎮", sports: ["lol", "csgo", "dota2", "valorant"] },
];

export function getDisplayName(sport: string): string {
  return SPORT_DISPLAY_CONFIG[sport.toLowerCase()]?.name ?? sport.toUpperCase();
}

export function getSportColor(sport: string): string {
  return SPORT_DISPLAY_CONFIG[sport.toLowerCase()]?.color ?? "var(--color-text-muted)";
}

export function getSportIcon(sport: string): string {
  return SPORT_DISPLAY_CONFIG[sport.toLowerCase()]?.icon ?? "🏅";
}

export function getSportFullName(sport: string): string {
  return SPORT_DISPLAY_CONFIG[sport.toLowerCase()]?.fullName ?? sport.toUpperCase();
}

export function getLeagueLogoUrl(sport: string): string | undefined {
  // External league logo endpoints are inconsistent and frequently 404.
  // Prefer proxied mirrored assets and icon fallback in UI components.
  return undefined;
}

/** Get a sport-themed gradient for hero/card backgrounds */
export function getSportGradient(sport: string): string {
  const color = getSportColor(sport);
  return `linear-gradient(135deg, ${color}22, ${color}08)`;
}
