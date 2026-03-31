/**
 * Pure formatting utilities.
 */

export function formatProbability(p: number): string {
  if (p >= 1) return `${p.toFixed(1)}%`;
  return `${(p * 100).toFixed(1)}%`;
}

export function probLabel(p: number): string {
  const pct = p >= 1 ? p : p * 100;
  if (pct >= 70) return "HIGH";
  if (pct >= 55) return "MED";
  return "LOW";
}

export function formatOdds(odds: number): string {
  return odds >= 0 ? `+${odds}` : String(odds);
}

export function oddsToImpliedProb(odds: number): number {
  if (odds > 0) return 100 / (odds + 100);
  return Math.abs(odds) / (Math.abs(odds) + 100);
}

export function formatLine(line: number): string {
  return line % 1 === 0 ? String(line) : line.toFixed(1);
}

export function formatTotal(total: number): string {
  return total.toFixed(1);
}

export function formatRecord(wins: number, losses: number, ties?: number): string {
  return ties != null && ties > 0 ? `${wins}-${losses}-${ties}` : `${wins}-${losses}`;
}

export function formatWinPct(wins: number, losses: number): string {
  const total = wins + losses;
  return total > 0 ? (wins / total).toFixed(3) : ".000";
}

export function formatGameTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
  } catch {
    return iso;
  }
}

export function formatGameDate(iso: string): string {
  try {
    return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric" });
  } catch {
    return iso;
  }
}

export function formatGameDateTime(iso: string): string {
  try {
    return new Date(iso).toLocaleString("en-US", {
      month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
      timeZoneName: "short",
    });
  } catch {
    return iso;
  }
}

export function formatRelativeTime(iso: string): string {
  try {
    const diff = Date.now() - new Date(iso).getTime();
    const mins = Math.floor(diff / 60_000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins}m ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.floor(hours / 24);
    return `${days}d ago`;
  } catch {
    return iso;
  }
}

export function formatPlayerNameShort(name: string): string {
  const parts = name.split(" ");
  if (parts.length < 2) return name;
  return `${parts[0][0]}. ${parts.slice(1).join(" ")}`;
}

export function formatStat(value: number, decimals = 1): string {
  return value.toFixed(decimals);
}

export function formatCompact(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return String(value);
}

/** Format a dollar amount — e.g. 1234.5 → "$1,234.50" */
export function formatDollars(amount: number): string {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(amount);
}

/** Format a signed percentage change — e.g. 0.15 → "+15.0%", -0.032 → "-3.2%" */
export function formatPercentChange(change: number): string {
  const pct = change * 100;
  const sign = pct >= 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}%`;
}

/** Format a number with commas — e.g. 12345 → "12,345" */
export function formatNumber(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

/** Ordinal suffix — e.g. 1 → "1st", 22 → "22nd" */
export function formatOrdinal(n: number): string {
  const s = ["th", "st", "nd", "rd"];
  const v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

/** Clamp value between min and max */
export function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

/** Validate a URL is safe to use in href (prevent javascript: XSS) */
export function sanitizeUrl(url: string | null | undefined): string {
  if (!url) return "#";
  const trimmed = url.trim();
  if (!trimmed) return "#";
  // Only allow http(s) and relative URLs
  if (trimmed.startsWith("https://") || trimmed.startsWith("http://") || trimmed.startsWith("/")) {
    // Block protocol-relative URLs that could bypass
    if (trimmed.startsWith("//")) return "#";
    return trimmed;
  }
  return "#";
}

/** Format a file size in bytes to human-readable form */
export function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

/** Truncate text to a max length with ellipsis */
export function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.slice(0, maxLength - 1).trimEnd() + "…";
}

/** Pluralize a word based on count */
export function pluralize(count: number, singular: string, plural?: string): string {
  return count === 1 ? singular : (plural ?? `${singular}s`);
}

/** Generate initials from a name (e.g. "John Doe" → "JD") */
export function getInitials(name: string, maxChars = 2): string {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, maxChars)
    .map((w) => w[0].toUpperCase())
    .join("");
}

/** Format a countdown until a target time — e.g. "2h 15m" or "23m" */
export function formatTimeUntil(isoOrMs: string | number): string {
  const target = typeof isoOrMs === "string" ? new Date(isoOrMs).getTime() : isoOrMs;
  const diff = target - Date.now();
  if (diff <= 0) return "now";
  const mins = Math.floor(diff / 60_000);
  if (mins < 60) return `${mins}m`;
  const hours = Math.floor(mins / 60);
  const remainMins = mins % 60;
  if (hours < 24) return remainMins > 0 ? `${hours}h ${remainMins}m` : `${hours}h`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

/** Map a win/loss record to a quality tier */
export function recordTier(wins: number, losses: number): "elite" | "good" | "average" | "poor" {
  const total = wins + losses;
  if (total === 0) return "average";
  const pct = wins / total;
  if (pct >= 0.7) return "elite";
  if (pct >= 0.55) return "good";
  if (pct >= 0.4) return "average";
  return "poor";
}

/** Format a spread value — e.g. -5.5 → "-5.5", 3 → "+3" */
export function formatSpread(spread: number): string {
  if (spread === 0) return "PK";
  return spread > 0 ? `+${spread}` : String(spread);
}

/** Capitalize first letter of each word */
export function titleCase(text: string): string {
  return text.replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Format a duration in seconds to a human-readable string (e.g. "2h 15m") */
export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return s > 0 ? `${m}m ${s}s` : `${m}m`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.round((seconds % 3600) / 60);
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

/** Mask an email address for display (e.g. "d***@gmail.com") */
export function maskEmail(email: string): string {
  const [local, domain] = email.split("@");
  if (!local || !domain) return email;
  return `${local[0]}${"•".repeat(Math.min(local.length - 1, 4))}@${domain}`;
}

/** Join an array of strings with commas and "and" for the last item */
export function formatListJoin(items: string[]): string {
  if (items.length === 0) return "";
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, and ${items[items.length - 1]}`;
}

// ── Player prop label formatting ────────────────────────────

const STAT_LABELS: Record<string, string> = {
  pts:               "Points",
  ast:               "Assists",
  reb:               "Rebounds",
  dreb:              "Def Reb",
  oreb:              "Off Reb",
  blk:               "Blocks",
  stl:               "Steals",
  to:                "TO",
  tov:               "TO",
  "3pt":             "3PM",
  "3pm":             "3PM",
  fg:                "FG Made",
  fga:               "FG Att",
  ft:                "FT Made",
  fta:               "FT Att",
  min:               "Minutes",
  pra:               "Pts+Reb+Ast",
  pr:                "Pts+Reb",
  pa:                "Pts+Ast",
  ra:                "Reb+Ast",
  pts_reb_ast:       "Pts+Reb+Ast",
  pts_reb:           "Pts+Reb",
  pts_ast:           "Pts+Ast",
  reb_ast:           "Reb+Ast",
  // MLB
  k:                 "K's",
  so:                "K's",
  tb:                "Total Bases",
  hr:                "Home Runs",
  rbi:               "RBIs",
  h:                 "Hits",
  r:                 "Runs",
  // NHL
  g:                 "Goals",
  a:                 "Assists",
  sog:               "Shots",
  shots:             "Shots",
  // Soccer
  goals:             "Goals",
  shots_on_target:   "Shots on Target",
  // Tennis / generic
  aces:              "Aces",
  df:                "Double Faults",
  games:             "Games",
  sets:              "Sets",
  // UFC
  strikes:           "Strikes",
  td:                "Takedowns",
  // MLB compound (with batter_/pitcher_ prefix stripped)
  batter_bb:           "Walks",
  batter_hr:           "HR",
  batter_rbi:          "RBIs",
  batter_runs:         "Runs",
  batter_h:            "Hits",
  batter_hits:         "Hits",
  batter_tb:           "Total Bases",
  batter_k:            "K's",
  batter_sb:           "Stolen Bases",
  pitcher_k:           "Strikeouts",
  pitcher_er:          "Earned Runs",
  pitcher_h:           "Hits Allow.",
  pitcher_bb:          "BBs Allow.",
  pitcher_ip:          "Innings",
  // Prefixed compound: used as-is in STAT_LABELS lookup
  top_batter_hits:     "Hits (Top)",
  top_batter_rbi:      "RBIs (Top)",
  top_batter_tb:       "Total Bases (Top)",
  top_pitcher_k:       "Strikeouts (Top)",
  total_runs:          "Total Runs",
  total_goals:         "Total Goals",
  total_points:        "Total Points",


};

const PROJECTION_LABELS: Record<string, string> = {
  double_double:          "Dbl-Dbl",
  triple_double:          "Triple-Dbl",
  home_top_scorer_pts:    "Home Top Scorer",
  away_top_scorer_pts:    "Away Top Scorer",
  top_ast_reg:            "Top Assists",
  top_reb_reg:            "Top Reb",
  first_basket:           "First Basket",
  last_basket:            "Last Basket",
  first_td:               "First TD",
  anytime_td:             "Anytime TD",
  anytime_goal:           "Anytime Goal",
  first_goal:             "First Goal",
  batter_fantasy_score:   "Fantasy Score",
};

const PERIOD_LABELS: Record<string, string> = {
  "1st_half":     "1H",
  "2nd_half":     "2H",
  "1st_quarter":  "Q1",
  "2nd_quarter":  "Q2",
  "3rd_quarter":  "Q3",
  "4th_quarter":  "Q4",
  "1st_period":   "P1",
  "2nd_period":   "P2",
  "3rd_period":   "P3",
};

/**
 * Convert an internal prop_type key into a human-readable label.
 * Examples:
 *   "pts_over_20"        → "Points O20"
 *   "ast_over_6"         → "Assists O6"
 *   "pts_reb_ast_over_35"→ "Pts+Reb+Ast O35"
 *   "double_double"      → "Dbl-Dbl"
 *   "total_assists"      → "Assists"
 *   "assists_milestones" → "Assists"
 *   "shots_on_goal_milestones" → "Shots"
 */
export function formatPropType(raw: string): string {
  if (!raw) return raw;
  const key = raw.toLowerCase().trim();

  // Direct projection match
  if (key in PROJECTION_LABELS) return PROJECTION_LABELS[key]!;

  // Direct stat match for plain keys (e.g. "batter_hr", "pitcher_k")
  if (key in STAT_LABELS) return STAT_LABELS[key]!;

  // Period-specific prop: "1st_half_total" or "1st_quarter_spread"
  for (const [period, label] of Object.entries(PERIOD_LABELS)) {
    if (key.startsWith(period + "_")) {
      const rest = key.slice(period.length + 1);
      if (rest === "total" || rest === "total_goals" || rest === "total_runs") return `${label} Total`;
      if (rest === "spread") return `${label} Spread`;
      if (rest === "moneyline") return `${label} ML`;
      return `${label} ${formatPropType(rest)}`;
    }
  }

  // Pattern: {stat}_over_{line} or {stat}_under_{line}
  const overUnder = key.match(/^(.+?)_(over|under)_(\d+(?:\.\d+)?)$/);
  if (overUnder) {
    const [, statPart, direction, line] = overUnder;
    const statKey = statPart!.replace(/_/g, "_");
    const base = STAT_LABELS[statKey!] ?? titleCase(statPart!);
    const dir = direction === "over" ? "O" : "U";
    return `${base} ${dir}${line}`;
  }

  // Pattern: total_{stat} e.g. "total_assists", "total_points"
  const totalMatch = key.match(/^total_(.+)$/);
  if (totalMatch) {
    const statPart = totalMatch[1]!;
    return STAT_LABELS[statPart] ?? titleCase(statPart);
  }

  // Pattern: {stat}_milestones
  const milestoneMatch = key.match(/^(.+?)_milestones$/);
  if (milestoneMatch) {
    const statPart = milestoneMatch[1]!;
    return STAT_LABELS[statPart] ?? titleCase(statPart);
  }

  // Pattern: {stat}_prop e.g. "basketball_player_prop"
  if (key.endsWith("_prop") || key.endsWith("_props")) {
    return titleCase(key.replace(/_props?$/, "").replace(/_/g, " "));
  }

  // Pattern: {stat}_reg → regression projection e.g. "pitcher_k_reg", "top_batter_hits_reg"
  const regMatch = key.match(/^(.+?)_reg$/);
  if (regMatch) {
    const statPart = regMatch[1]!;
    const base = STAT_LABELS[statPart] ?? titleCase(statPart.replace(/_/g, " "));
    return `${base} (Proj)`;
  }

  // Fallback: title-case with underscores → spaces
  return titleCase(key.replace(/_/g, " "));
}
