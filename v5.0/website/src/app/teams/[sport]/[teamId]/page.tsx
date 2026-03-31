export const dynamic = "force-dynamic";

import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { SectionBand, Badge, TeamBadge } from "@/components/ui";
import { getDisplayName } from "@/lib/sports-config";
import { notFound } from "next/navigation";
import Link from "next/link";

interface PageProps {
  params: Promise<{ sport: string; teamId: string }>;
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function getTeamDetail(sport: string, teamId: string) {
  try {
    const res = await fetch(`${API_BASE}/v1/${sport}/teams/${teamId}`, {
      next: { revalidate: 60 },
    });
    if (!res.ok) return null;
    const json = await res.json();
    return json?.data ?? json ?? null;
  } catch {
    return null;
  }
}

async function getTeamRoster(sport: string, teamId: string) {
  try {
    const res = await fetch(
      `${API_BASE}/v1/${sport}/players?team_id=${teamId}&limit=100`,
      { next: { revalidate: 60 } },
    );
    if (!res.ok) return [];
    const json = await res.json();
    return json?.data ?? json ?? [];
  } catch {
    return [];
  }
}

async function getTeamGames(sport: string, teamId: string) {
  try {
    const res = await fetch(
      `${API_BASE}/v1/${sport}/games?team_id=${teamId}&limit=20`,
      { next: { revalidate: 60 } },
    );
    if (!res.ok) return [];
    const json = await res.json();
    return json?.data ?? json ?? [];
  } catch {
    return [];
  }
}

async function getTeamStats(sport: string, teamId: string) {
  try {
    const res = await fetch(
      `${API_BASE}/v1/${sport}/team-stats?team_id=${teamId}&aggregate=true&limit=1`,
      { next: { revalidate: 60 } },
    );
    if (!res.ok) return null;
    const json = await res.json();
    const data = json?.data ?? json ?? [];
    return Array.isArray(data) && data.length > 0 ? data[0] : null;
  } catch {
    return null;
  }
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { sport, teamId } = await params;
  const team = await getTeamDetail(sport, teamId);
  const name = team?.name ?? teamId;
  return buildPageMetadata({
    title: `${name} — ${getDisplayName(sport)}`,
    description: `Team profile and schedule for ${name} in ${getDisplayName(sport)}.`,
    path: `/teams/${sport}/${teamId}`,
  });
}

export default async function TeamDetailPage({ params }: PageProps) {
  const { sport, teamId } = await params;

  const [team, roster, games, teamStats] = await Promise.all([
    getTeamDetail(sport, teamId),
    getTeamRoster(sport, teamId),
    getTeamGames(sport, teamId),
    getTeamStats(sport, teamId),
  ]);

  const teamName = team?.name ?? teamId;
  const sportName = getDisplayName(sport);

  const cardStyle: React.CSSProperties = {
    background: "var(--color-bg-2)",
    border: "1px solid var(--color-border)",
    borderRadius: "var(--radius-md)",
    padding: "var(--space-4)",
  };

  return (
    <main>
      {/* Breadcrumb */}
      <nav aria-label="Breadcrumb" style={{ padding: "var(--space-2) var(--space-4)", fontSize: "var(--text-sm, 0.875rem)" }}>
        <ol className="breadcrumb" style={{ listStyle: "none", display: "flex", gap: "0.25rem", margin: 0, padding: 0, flexWrap: "wrap" }}>
          <li><Link href="/teams" style={{ color: "var(--color-accent, #6366f1)", textDecoration: "none" }}>Teams</Link></li>
          <li aria-hidden="true" style={{ color: "var(--color-text-muted)" }}>/</li>
          <li><Link href={`/teams?sport=${sport}`} style={{ color: "var(--color-accent, #6366f1)", textDecoration: "none" }}>{sportName}</Link></li>
          <li aria-hidden="true" style={{ color: "var(--color-text-muted)" }}>/</li>
          <li aria-current="page" style={{ color: "var(--color-text-muted)" }}>{teamName}</li>
        </ol>
      </nav>

      <SectionBand title={teamName}>
        <div style={{ ...cardStyle, display: "flex", gap: "var(--space-4)", flexWrap: "wrap", alignItems: "center" }}>
          <TeamBadge
            teamId={teamId}
            name={teamName}
            abbrev={team?.abbreviation ?? undefined}
            sport={sport}
            logoUrl={team?.logo_url ?? undefined}
            size="lg"
          />
          <div style={{ flex: 1, minWidth: 200 }}>
            <h2 style={{ margin: 0, fontSize: "1.5rem" }}>
              {team?.city ? `${team.city} ` : ""}{teamName}
            </h2>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", marginTop: "0.5rem" }}>
              <Badge variant={sport}>{sportName}</Badge>
              {team?.conference && <Badge>{team.conference}</Badge>}
              {team?.division && <Badge>{team.division}</Badge>}
            </div>
          </div>
        </div>
      </SectionBand>

      {/* Team Stats */}
      {teamStats && (
        <SectionBand title="Season Stats">
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))", gap: "var(--space-3)" }}>
            {Object.entries(teamStats as Record<string, unknown>)
              .filter(([k, v]) => v != null && !["team_id", "team_name", "player_id", "player_name", "season", "sport", "game_id", "date", "id"].includes(k) && typeof v === "number")
              .slice(0, 12)
              .map(([k, v]) => (
                <div key={k} style={{ ...cardStyle, textAlign: "center", padding: "var(--space-3)" }}>
                  <div style={{ fontSize: "1.25rem", fontWeight: 700, color: "var(--color-brand)" }}>
                    {typeof v === "number" ? (Number.isInteger(v) ? v : (v as number).toFixed(1)) : String(v)}
                  </div>
                  <div style={{ fontSize: "0.7rem", fontWeight: 600, textTransform: "uppercase", color: "var(--color-text-2)", marginTop: "0.25rem" }}>
                    {k.replace(/_/g, " ")}
                  </div>
                </div>
              ))}
          </div>
        </SectionBand>
      )}

      {/* Roster */}
      <SectionBand title="Roster">
        {Array.isArray(roster) && roster.length > 0 ? (
          <div className="card">
            <div className="responsive-table-wrap">
              <table className="responsive-table team-roster-table" style={{ borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "left", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>Name</th>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "center", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>Pos</th>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "center", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>#</th>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "left", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>Status</th>
                </tr>
              </thead>
              <tbody>
                {(roster as Record<string, unknown>[]).map((p, i) => (
                  <tr key={(p.id as string) ?? i} style={{ backgroundColor: i % 2 === 0 ? "transparent" : "rgba(128,128,128,0.04)" }}>
                    <td style={{ padding: "0.5rem", fontWeight: 600, borderBottom: "1px solid var(--border)" }}>
                      <Link href={`/players/${p.id}?sport=${sport}`} style={{ color: "var(--color-accent, #6366f1)", textDecoration: "none" }}>
                        {(p.name as string) ?? "Unknown"}
                      </Link>
                    </td>
                    <td style={{ padding: "0.5rem", textAlign: "center", borderBottom: "1px solid var(--border)", fontSize: "0.875rem" }}>{(p.position as string) ?? "—"}</td>
                    <td style={{ padding: "0.5rem", textAlign: "center", borderBottom: "1px solid var(--border)", fontSize: "0.875rem" }}>{p.jersey_number != null ? String(p.jersey_number) : "—"}</td>
                    <td style={{ padding: "0.5rem", borderBottom: "1px solid var(--border)", fontSize: "0.875rem" }}>{(p.status as string) ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="card" style={{ padding: "var(--space-6)", textAlign: "center", color: "var(--text-muted)" }}>
            No roster data available.
          </div>
        )}
      </SectionBand>

      {/* Recent Games */}
      <SectionBand title="Recent Games">
        {Array.isArray(games) && games.length > 0 ? (
          <div className="card">
            <div className="responsive-table-wrap">
              <table className="responsive-table team-games-table" style={{ borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "left", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>Date</th>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "left", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>Matchup</th>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "center", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>Score</th>
                  <th scope="col" style={{ padding: "0.5rem", textAlign: "center", borderBottom: "2px solid var(--border)", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase" }}>Status</th>
                </tr>
              </thead>
              <tbody>
                {(games as Record<string, unknown>[]).map((g, i) => (
                  <tr key={(g.id as string) ?? i} style={{ backgroundColor: i % 2 === 0 ? "transparent" : "rgba(128,128,128,0.04)" }}>
                    <td style={{ padding: "0.5rem", borderBottom: "1px solid var(--border)", fontSize: "0.875rem" }}>{(g.date as string) ?? "—"}</td>
                    <td style={{ padding: "0.5rem", fontWeight: 600, borderBottom: "1px solid var(--border)", fontSize: "0.875rem" }}>
                      <Link href={`/games/${sport}/${g.id}`} style={{ color: "var(--color-accent, #6366f1)", textDecoration: "none" }}>
                        {(g.away_team as string) ?? "Away"} @ {(g.home_team as string) ?? "Home"}
                      </Link>
                    </td>
                    <td style={{ padding: "0.5rem", textAlign: "center", borderBottom: "1px solid var(--border)", fontSize: "0.875rem", fontWeight: 700 }}>
                      {g.away_score != null && g.home_score != null ? `${g.away_score} - ${g.home_score}` : "—"}
                    </td>
                    <td style={{ padding: "0.5rem", textAlign: "center", borderBottom: "1px solid var(--border)", fontSize: "0.875rem" }}>{(g.status as string) ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="card" style={{ padding: "var(--space-6)", textAlign: "center", color: "var(--text-muted)" }}>
            No recent games available.
          </div>
        )}
      </SectionBand>
    </main>
  );
}
