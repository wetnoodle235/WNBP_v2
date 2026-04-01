export const dynamic = "force-dynamic";

import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { getPlayers, getTeams } from "@/lib/api";
import { SectionBand, Badge, StatCard, TeamBadge } from "@/components/ui";
import { getDisplayName } from "@/lib/sports-config";
import { notFound } from "next/navigation";
import Image from "next/image";
import Link from "next/link";

import type { Player } from "../page";
import { resolveServerApiBase } from "@/lib/api-base";

/* ------------------------------------------------------------------ */
/*  Types & helpers                                                    */
/* ------------------------------------------------------------------ */

interface PageProps {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ sport?: string }>;
}

const API_BASE = resolveServerApiBase();

async function getPlayerStats(sport: string, playerId: string) {
  try {
    const res = await fetch(
      `${API_BASE}/v1/${sport}/player-stats?player_id=${playerId}&aggregate=true`,
      { cache: "no-store" },
    );
    if (!res.ok) return null;
    const json = await res.json();
    return json?.data ?? json ?? null;
  } catch {
    return null;
  }
}

function getInitials(name: string): string {
  return name
    .split(" ")
    .map((p) => p[0])
    .join("")
    .toUpperCase()
    .slice(0, 2);
}

function formatStatus(status: string | null): string {
  if (!status) return "Unknown";
  return status.charAt(0).toUpperCase() + status.slice(1).toLowerCase();
}

function statusAccent(status: string | null): string {
  const s = (status ?? "").toLowerCase();
  if (s === "active") return "var(--color-win)";
  if (s === "injured" || s === "out") return "var(--color-loss)";
  return "var(--color-text-2)";
}

interface StatDef {
  label: string;
  key: string;
  format?: (v: unknown) => string;
}

const pct = (v: unknown) => (v != null ? `${(Number(v) * 100).toFixed(1)}%` : "—");
const dec = (d: number) => (v: unknown) => (v != null ? Number(v).toFixed(d) : "—");
const num = (v: unknown) => (v != null ? String(v) : "—");

const SOCCER_STATS: StatDef[] = [
  { label: "APP", key: "appearances", format: num },
  { label: "G", key: "goals", format: num },
  { label: "A", key: "assists", format: num },
  { label: "MINS", key: "minutes", format: num },
  { label: "YC", key: "yellow_cards", format: num },
  { label: "RC", key: "red_cards", format: num },
  { label: "CS", key: "clean_sheets", format: num },
  { label: "SV%", key: "save_pct", format: pct },
];

const STAT_DEFS: Record<string, StatDef[]> = {
  nba: [
    { label: "PTS", key: "pts", format: dec(1) },
    { label: "REB", key: "reb", format: dec(1) },
    { label: "AST", key: "ast", format: dec(1) },
    { label: "STL", key: "stl", format: dec(1) },
    { label: "BLK", key: "blk", format: dec(1) },
    { label: "FG%", key: "fg_pct", format: pct },
    { label: "FT%", key: "ft_pct", format: pct },
    { label: "3PT%", key: "fg3_pct", format: pct },
  ],
  wnba: [
    { label: "PTS", key: "pts", format: dec(1) },
    { label: "REB", key: "reb", format: dec(1) },
    { label: "AST", key: "ast", format: dec(1) },
    { label: "STL", key: "stl", format: dec(1) },
    { label: "BLK", key: "blk", format: dec(1) },
    { label: "FG%", key: "fg_pct", format: pct },
  ],
  nfl: [
    { label: "PASS YDS", key: "pass_yds", format: num },
    { label: "PASS TD", key: "pass_td", format: num },
    { label: "RUSH YDS", key: "rush_yds", format: num },
    { label: "RUSH TD", key: "rush_td", format: num },
    { label: "REC YDS", key: "rec_yds", format: num },
    { label: "SACKS", key: "sacks", format: num },
  ],
  mlb: [
    { label: "AVG", key: "avg", format: dec(3) },
    { label: "HR", key: "hr", format: num },
    { label: "RBI", key: "rbi", format: num },
    { label: "HITS", key: "hits", format: num },
    { label: "ERA", key: "era", format: dec(2) },
    { label: "K", key: "strikeouts", format: num },
  ],
  nhl: [
    { label: "G", key: "goals", format: num },
    { label: "A", key: "assists", format: num },
    { label: "PTS", key: "pts", format: num },
    { label: "+/−", key: "plus_minus", format: num },
    { label: "SOG", key: "sog", format: num },
    { label: "SV%", key: "sv_pct", format: pct },
  ],
  ncaab: [
    { label: "PTS", key: "pts", format: dec(1) },
    { label: "REB", key: "reb", format: dec(1) },
    { label: "AST", key: "ast", format: dec(1) },
    { label: "STL", key: "stl", format: dec(1) },
    { label: "BLK", key: "blk", format: dec(1) },
    { label: "FG%", key: "fg_pct", format: pct },
  ],
  ncaaw: [
    { label: "PTS", key: "pts", format: dec(1) },
    { label: "REB", key: "reb", format: dec(1) },
    { label: "AST", key: "ast", format: dec(1) },
    { label: "STL", key: "stl", format: dec(1) },
    { label: "BLK", key: "blk", format: dec(1) },
    { label: "FG%", key: "fg_pct", format: pct },
  ],
  ncaaf: [
    { label: "PASS YDS", key: "pass_yds", format: num },
    { label: "PASS TD", key: "pass_td", format: num },
    { label: "RUSH YDS", key: "rush_yds", format: num },
    { label: "RUSH TD", key: "rush_td", format: num },
    { label: "REC YDS", key: "rec_yds", format: num },
    { label: "SACKS", key: "sacks", format: num },
  ],
  epl: SOCCER_STATS,
  bundesliga: SOCCER_STATS,
  laliga: SOCCER_STATS,
  ligue1: SOCCER_STATS,
  seriea: SOCCER_STATS,
  mls: SOCCER_STATS,
  ucl: SOCCER_STATS,
  nwsl: SOCCER_STATS,
  ufc: [
    { label: "FIGHTS", key: "fights", format: num },
    { label: "W", key: "wins", format: num },
    { label: "L", key: "losses", format: num },
    { label: "KO", key: "ko_wins", format: num },
    { label: "SUB", key: "sub_wins", format: num },
    { label: "SIG STR", key: "sig_strikes", format: num },
    { label: "STR%", key: "sig_strike_pct", format: pct },
    { label: "TD", key: "takedowns", format: num },
    { label: "TD%", key: "takedown_pct", format: pct },
  ],
  f1: [
    { label: "RACES", key: "races", format: num },
    { label: "WINS", key: "wins", format: num },
    { label: "PODIUMS", key: "podiums", format: num },
    { label: "POLES", key: "poles", format: num },
    { label: "PTS", key: "points", format: dec(1) },
    { label: "FL", key: "fastest_laps", format: num },
    { label: "DNF", key: "dnf", format: num },
  ],
  atp: [
    { label: "MATCHES", key: "matches", format: num },
    { label: "W", key: "wins", format: num },
    { label: "L", key: "losses", format: num },
    { label: "ACES", key: "aces", format: num },
    { label: "DF", key: "double_faults", format: num },
    { label: "1ST%", key: "first_serve_pct", format: pct },
    { label: "BP%", key: "break_pts_pct", format: pct },
  ],
  wta: [
    { label: "MATCHES", key: "matches", format: num },
    { label: "W", key: "wins", format: num },
    { label: "L", key: "losses", format: num },
    { label: "ACES", key: "aces", format: num },
    { label: "DF", key: "double_faults", format: num },
    { label: "1ST%", key: "first_serve_pct", format: pct },
    { label: "BP%", key: "break_pts_pct", format: pct },
  ],
  lol: [
    { label: "GAMES", key: "games", format: num },
    { label: "K", key: "kills", format: dec(1) },
    { label: "D", key: "deaths", format: dec(1) },
    { label: "A", key: "assists", format: dec(1) },
    { label: "KDA", key: "kda", format: dec(2) },
    { label: "CS/M", key: "cs_per_min", format: dec(1) },
    { label: "G/M", key: "gold_per_min", format: num },
  ],
  csgo: [
    { label: "MAPS", key: "maps", format: num },
    { label: "K", key: "kills", format: num },
    { label: "D", key: "deaths", format: num },
    { label: "K/D", key: "kd_ratio", format: dec(2) },
    { label: "ADR", key: "adr", format: dec(1) },
    { label: "RATING", key: "rating", format: dec(2) },
  ],
  dota2: [
    { label: "GAMES", key: "games", format: num },
    { label: "K", key: "kills", format: dec(1) },
    { label: "D", key: "deaths", format: dec(1) },
    { label: "A", key: "assists", format: dec(1) },
    { label: "GPM", key: "gold_per_min", format: num },
    { label: "XPM", key: "xp_per_min", format: num },
  ],
  valorant: [
    { label: "MAPS", key: "maps", format: num },
    { label: "K", key: "kills", format: num },
    { label: "D", key: "deaths", format: num },
    { label: "A", key: "assists", format: num },
    { label: "K/D", key: "kd_ratio", format: dec(2) },
    { label: "ACS", key: "acs", format: dec(1) },
  ],
  golf: [
    { label: "EVENTS", key: "events", format: num },
    { label: "WINS", key: "wins", format: num },
    { label: "TOP 10", key: "top_10", format: num },
    { label: "CUTS", key: "cuts_made", format: num },
    { label: "AVG", key: "scoring_avg", format: dec(2) },
    { label: "FW%", key: "fairway_pct", format: pct },
    { label: "GIR%", key: "gir_pct", format: pct },
  ],
};

/* ------------------------------------------------------------------ */
/*  Shared inline styles                                               */
/* ------------------------------------------------------------------ */

const cardStyle: React.CSSProperties = {
  background: "var(--color-bg-2)",
  border: "1px solid var(--color-border)",
  borderRadius: "var(--radius-md)",
  padding: "var(--space-4)",
};

const metaText: React.CSSProperties = {
  fontSize: "var(--text-sm)",
  color: "var(--color-text-2)",
};

/* ------------------------------------------------------------------ */
/*  Metadata                                                           */
/* ------------------------------------------------------------------ */

export async function generateMetadata({
  params,
  searchParams,
}: PageProps): Promise<Metadata> {
  const { id } = await params;
  const { sport = "nba" } = await searchParams;

  const players = (await getPlayers(sport)) as Player[];
  const player = players.find((p) => p.id === id);

  if (!player) {
    return buildPageMetadata({
      title: "Player Not Found",
      description: "The requested player could not be found.",
      path: `/players/${id}`,
    });
  }

  const sportName = getDisplayName(sport);
  return buildPageMetadata({
    title: `${player.name} – ${sportName}`,
    description: `${sportName} profile and stats for ${player.name}${player.position ? ` (${player.position})` : ""}.`,
    path: `/players/${id}`,
  });
}

/* ------------------------------------------------------------------ */
/*  Page                                                               */
/* ------------------------------------------------------------------ */

export default async function PlayerDetailPage({
  params,
  searchParams,
}: PageProps) {
  const { id } = await params;
  const { sport = "nba" } = await searchParams;

  const [players, teams, rawStats] = await Promise.all([
    getPlayers(sport) as Promise<Player[]>,
    getTeams(sport),
    getPlayerStats(sport, id),
  ]);

  const player = players.find((p) => p.id === id);
  if (!player) notFound();

  const team = player.team_id
    ? teams.find((t) => t.id === player.team_id)
    : null;

  const sportName = getDisplayName(sport);

  // Flatten stats if array
  const stats: Record<string, unknown> | null =
    Array.isArray(rawStats) && rawStats.length > 0
      ? (rawStats[0] as Record<string, unknown>)
      : rawStats && typeof rawStats === "object"
        ? (rawStats as Record<string, unknown>)
        : null;

  const statDefs = STAT_DEFS[sport.toLowerCase()] ?? [];

  return (
    <main>
      {/* ── Breadcrumb ──────────────────────────────────── */}
      <nav aria-label="Breadcrumb" style={{ padding: "var(--space-3) var(--space-4)", fontSize: "var(--text-sm)" }}>
        <ol style={{ display: "flex", gap: "var(--space-2)", listStyle: "none", margin: 0, padding: 0, color: "var(--color-text-muted)" }}>
          <li><Link href="/" style={{ color: "var(--color-accent)" }}>Home</Link></li>
          <li aria-hidden="true">/</li>
          <li><Link href="/players" style={{ color: "var(--color-accent)" }}>Players</Link></li>
          <li aria-hidden="true">/</li>
          <li aria-current="page">{player.name}</li>
        </ol>
      </nav>

      {/* ── Player Header ─────────────────────────────────── */}
      <SectionBand title={player.name}>
        <div
          style={{
            ...cardStyle,
            display: "flex",
            gap: "var(--space-4)",
            flexWrap: "wrap",
            alignItems: "center",
          }}
        >
          {/* Headshot / initials avatar */}
          {player.headshot_url ? (
            <Image
              src={player.headshot_url}
              alt={player.name}
              width={96}
              height={96}
              unoptimized
              style={{
                borderRadius: "50%",
                objectFit: "cover",
                border: "2px solid var(--color-border)",
              }}
            />
          ) : (
            <div
              style={{
                width: 96,
                height: 96,
                borderRadius: "50%",
                background: "var(--color-accent, #6366f1)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: "2rem",
                fontWeight: 700,
                color: "#fff",
                flexShrink: 0,
              }}
            >
              {getInitials(player.name)}
            </div>
          )}

          {/* Bio info */}
          <div style={{ flex: 1, minWidth: 200 }}>
            <h2 style={{ margin: 0, fontSize: "1.5rem" }}>{player.name}</h2>

            {/* Team */}
            {team && (
              <div style={{ marginTop: "0.25rem" }}>
                <TeamBadge
                  teamId={team.id}
                  name={team.name}
                  abbrev={team.abbreviation ?? undefined}
                  sport={sport}
                  logoUrl={team.logo_url ?? undefined}
                  href={`/teams?sport=${sport}`}
                  size="sm"
                />
              </div>
            )}

            {/* Badges row */}
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: "0.5rem",
                marginTop: "0.5rem",
                alignItems: "center",
              }}
            >
              {player.position && (
                <Badge variant={sport}>{player.position}</Badge>
              )}
              {player.jersey_number != null && (
                <Badge>#{player.jersey_number}</Badge>
              )}
              {player.status && (
                <span
                  style={{
                    fontSize: "var(--text-sm)",
                    fontWeight: 600,
                    color: statusAccent(player.status),
                  }}
                >
                  {formatStatus(player.status)}
                </span>
              )}
            </div>

            {/* Detail row */}
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: "1rem",
                marginTop: "0.5rem",
                ...metaText,
              }}
            >
              {player.height && <span>📏 {player.height}</span>}
              {player.weight != null && <span>⚖️ {player.weight} lbs</span>}
              {player.nationality && <span>🌍 {player.nationality}</span>}
              {player.experience_years != null && (
                <span>
                  🏅 {player.experience_years}{" "}
                  {player.experience_years === 1 ? "year" : "years"} exp
                </span>
              )}
              {player.birth_date && <span>🎂 {player.birth_date}</span>}
            </div>
          </div>
        </div>
      </SectionBand>

      {/* ── Player Stats ──────────────────────────────────── */}
      <SectionBand title={`${sportName} Stats`}>
        {stats && statDefs.length > 0 ? (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))",
              gap: "var(--space-3, 0.75rem)",
            }}
          >
            {statDefs.map((def) => {
              const raw = stats[def.key];
              const display = def.format ? def.format(raw) : num(raw);
              return (
                <StatCard key={def.key} label={def.label} value={display} />
              );
            })}
          </div>
        ) : (
          <div style={cardStyle}>
            <p style={{ ...metaText, margin: 0 }}>
              Stats not yet available for this player.
            </p>
          </div>
        )}
      </SectionBand>

      {/* ── Team Info ─────────────────────────────────────── */}
      {team && (
        <SectionBand title="Team">
          <div
            style={{
              ...cardStyle,
              display: "flex",
              gap: "var(--space-4)",
              alignItems: "center",
              flexWrap: "wrap",
            }}
          >
            <TeamBadge
              teamId={team.id}
              name={team.name}
              abbrev={team.abbreviation ?? undefined}
              sport={sport}
              logoUrl={team.logo_url ?? undefined}
              href={`/teams?sport=${sport}`}
              size="lg"
            />

            <div style={{ flex: 1, minWidth: 160 }}>
              <h3 style={{ margin: 0 }}>
                {team.city ? `${team.city} ` : ""}
                {team.name}
              </h3>

              <div
                style={{
                  display: "flex",
                  flexWrap: "wrap",
                  gap: "0.75rem",
                  marginTop: "0.25rem",
                  ...metaText,
                }}
              >
                {team.conference && <span>Conference: {team.conference}</span>}
                {team.division && <span>Division: {team.division}</span>}
              </div>
            </div>

            <Link
              href={`/teams?sport=${sport}`}
              style={{
                fontSize: "var(--text-sm)",
                color: "var(--color-accent, #6366f1)",
              }}
            >
              View all {sportName} teams →
            </Link>
          </div>
        </SectionBand>
      )}
    </main>
  );
}
