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
  leagueLogoUrl?: string;
}

const SPORT_DISPLAY_CONFIG: Record<string, SportDisplayInfo> = {
  // Major US Sports
  nba: { name: "NBA", fullName: "National Basketball Association", icon: "🏀", color: "#c9082a", category: "Major US Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/nba.png" },
  mlb: { name: "MLB", fullName: "Major League Baseball", icon: "⚾", color: "#002d72", category: "Major US Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/mlb.png" },
  nfl: { name: "NFL", fullName: "National Football League", icon: "🏈", color: "#013369", category: "Major US Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/nfl.png" },
  nhl: { name: "NHL", fullName: "National Hockey League", icon: "🏒", color: "#000000", category: "Major US Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/nhl.png" },
  wnba: { name: "WNBA", fullName: "Women's National Basketball Association", icon: "🏀", color: "#ff6600", category: "Major US Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/wnba.png" },

  // College Sports
  ncaab: { name: "NCAAB", fullName: "NCAA Men's Basketball", icon: "🏀", color: "#0a2240", category: "College Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/ncb.png" },
  ncaaw: { name: "NCAAW", fullName: "NCAA Women's Basketball", icon: "🏀", color: "#7a003c", category: "College Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/ncw.png" },
  ncaaf: { name: "NCAAF", fullName: "NCAA Football", icon: "🏈", color: "#1a1a2e", category: "College Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/college-football.png" },

  // Soccer
  mls: { name: "MLS", fullName: "Major League Soccer", icon: "⚽", color: "#1b365d", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/usa.1.png" },
  epl: { name: "EPL", fullName: "English Premier League", icon: "⚽", color: "#3d195b", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/eng.1.png" },
  bundesliga: { name: "Bundesliga", fullName: "Bundesliga (Germany)", icon: "⚽", color: "#d20515", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/ger.1.png" },
  laliga: { name: "La Liga", fullName: "La Liga (Spain)", icon: "⚽", color: "#ee8707", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/esp.1.png" },
  ligue1: { name: "Ligue 1", fullName: "Ligue 1 (France)", icon: "⚽", color: "#091c3e", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/fra.1.png" },
  seriea: { name: "Serie A", fullName: "Serie A (Italy)", icon: "⚽", color: "#024494", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/ita.1.png" },
  ucl: { name: "UCL", fullName: "UEFA Champions League", icon: "⚽", color: "#081c3b", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/uefa.champions.png" },
  nwsl: { name: "NWSL", fullName: "National Women's Soccer League", icon: "⚽", color: "#003087", category: "Soccer", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/usa.nwsl.png" },

  // Individual Sports
  atp: { name: "ATP", fullName: "ATP Tour (Men's Tennis)", icon: "🎾", color: "#00a651", category: "Individual Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/atp.png" },
  wta: { name: "WTA", fullName: "WTA Tour (Women's Tennis)", icon: "🎾", color: "#71205e", category: "Individual Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/wta.png" },
  pga: { name: "PGA", fullName: "PGA Tour", icon: "⛳", color: "#003865", category: "Individual Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/pga.png" },
  mma: { name: "MMA", fullName: "Mixed Martial Arts", icon: "🥊", color: "#d20a0a", category: "Individual Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/mma.png" },
  f1: { name: "F1", fullName: "Formula 1", icon: "🏎️", color: "#e10600", category: "Individual Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/f1.png" },
  ufc: { name: "UFC", fullName: "Ultimate Fighting Championship", icon: "🥊", color: "#d20a0a", category: "Individual Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/ufc.png" },
  golf: { name: "Golf", fullName: "PGA Tour", icon: "⛳", color: "#003865", category: "Individual Sports", leagueLogoUrl: "https://a.espncdn.com/i/teamlogos/leagues/500/pga.png" },

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
  { label: "Soccer", icon: "⚽", sports: ["epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl"] },
  { label: "Individual Sports", icon: "🏆", sports: ["f1", "atp", "wta", "ufc", "golf"] },
  { label: "Esports", icon: "🎮", sports: ["lol", "cs2", "dota2", "valorant"] },
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
  return SPORT_DISPLAY_CONFIG[sport.toLowerCase()]?.leagueLogoUrl;
}

/** Get a sport-themed gradient for hero/card backgrounds */
export function getSportGradient(sport: string): string {
  const color = getSportColor(sport);
  return `linear-gradient(135deg, ${color}22, ${color}08)`;
}
