import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { SectionBand } from "@/components/ui";
import { getInjuries, getTeams } from "@/lib/api";
import type { Injury, Team } from "@/lib/schemas";
import { getDisplayName } from "@/lib/sports-config";
import Link from "next/link";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export const metadata: Metadata = buildPageMetadata({
  title: "Injury Report",
  description:
    "Aggregate injury report across all supported sports — player status, body part, and expected return dates.",
  path: "/injuries",
  keywords: ["injury report", "player injuries", "injury status", "out", "questionable", "day-to-day"],
});

const INJURY_SPORTS = [
  "nba", "nfl", "mlb", "nhl", "wnba",
  "ncaab", "ncaaf", "epl", "mls", "ufc",
] as const;

const STATUS_PRIORITY: Record<string, number> = {
  out: 0,
  "game-time decision": 1,
  gtd: 1,
  questionable: 2,
  "day-to-day": 3,
  dtd: 3,
  doubtful: 4,
  ir: 5,
  probable: 6,
};

const PANEL_BG = "var(--color-bg-3)";
const CARD_BG = "var(--color-bg-2)";
const BORDER = "1px solid var(--color-border)";
const TEXT_MUTED = "var(--color-text-muted)";
const TEXT_SECONDARY = "var(--color-text-secondary)";

function statusPriority(status: string): number {
  const lo = status.toLowerCase();
  for (const [key, val] of Object.entries(STATUS_PRIORITY)) {
    if (lo.includes(key)) return val;
  }
  return 99;
}

function StatusPill({ status }: { status: string }) {
  const lo = status.toLowerCase();
  const style =
    lo.includes("out") || lo.includes("ir")
      ? { bg: "rgba(239,68,68,0.18)", color: "#ef4444" }
      : lo.includes("question") || lo.includes("gtd") || lo.includes("game-time")
      ? { bg: "rgba(245,158,11,0.18)", color: "#f59e0b" }
      : lo.includes("dtd") || lo.includes("day-to-day")
      ? { bg: "rgba(249,115,22,0.18)", color: "#f97316" }
      : lo.includes("doubtful")
      ? { bg: "rgba(239,68,68,0.10)", color: "#dc2626" }
      : { bg: PANEL_BG, color: TEXT_SECONDARY };
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 4,
        fontSize: "0.72rem",
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        background: style.bg,
        color: style.color,
        whiteSpace: "nowrap",
      }}
    >
      {status}
    </span>
  );
}

function readSearchParam(value: string | string[] | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

function matchesStatusFilter(status: string, filter: string): boolean {
  const lo = status.toLowerCase();
  if (!filter) return true;
  if (filter === "out") return lo.includes("out") || lo.includes("ir") || lo.includes("game-time") || lo.includes("gtd");
  if (filter === "questionable") return lo.includes("question");
  if (filter === "dtd") return lo.includes("day-to-day") || lo.includes("dtd");
  if (filter === "doubtful") return lo.includes("doubtful");
  if (filter === "probable") return lo.includes("probable");
  return lo.includes(filter);
}

interface InjuriesPageProps {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}

export default async function InjuriesPage({ searchParams }: InjuriesPageProps) {
  const filters = searchParams ? await searchParams : {};
  const selectedSport = readSearchParam(filters.sport).toLowerCase();
  const selectedStatus = readSearchParam(filters.status).toLowerCase();
  const query = readSearchParam(filters.q).toLowerCase();
  const [injuryBatches, teamBatches] = await Promise.all([
    Promise.allSettled(
    INJURY_SPORTS.map((sport) =>
      getInjuries(sport).then((rows) => rows.map((r) => ({ ...r, _sport: sport })))
    )
    ),
    Promise.allSettled(
      INJURY_SPORTS.map((sport) =>
        getTeams(sport).then((rows) => ({ sport, rows }))
      )
    ),
  ]);

  const all: (Injury & { _sport: string })[] = [];
  for (const result of injuryBatches) {
    if (result.status === "fulfilled") all.push(...result.value);
  }

  const teamLookup: Record<string, Record<string, Team>> = {};
  for (const result of teamBatches) {
    if (result.status !== "fulfilled") continue;
    teamLookup[result.value.sport] = Object.fromEntries(
      result.value.rows.map((team) => [team.id, team])
    );
  }

  const filtered = all.filter((inj) => {
    if (selectedSport && inj._sport.toLowerCase() !== selectedSport) return false;
    if (!matchesStatusFilter(inj.status ?? "", selectedStatus)) return false;
    if (!query) return true;
    const teamId = inj.team_id ?? "";
    const team = teamId ? teamLookup[inj._sport]?.[teamId] : null;
    const haystack = [
      inj.player_name,
      team?.name,
      team?.abbreviation,
      inj.description,
      inj.body_part,
      teamId,
      inj._sport,
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  });

  // Sort: status priority first, then by sport
  filtered.sort((a, b) => {
    const pa = statusPriority(a.status ?? "");
    const pb = statusPriority(b.status ?? "");
    if (pa !== pb) return pa - pb;
    return (a._sport ?? "").localeCompare(b._sport ?? "");
  });

  // Group by sport for the summary strip
  const outCount = filtered.filter((i) => statusPriority(i.status ?? "") <= 1).length;
  const questCount = filtered.filter((i) => {
    const p = statusPriority(i.status ?? "");
    return p >= 2 && p <= 3;
  }).length;

  const jsonLd = buildCollectionJsonLd({
    name: "Injury Report",
    path: "/injuries",
    description: "Aggregate injury report across all supported sports.",
  });

  const thStyle: React.CSSProperties = {
    textAlign: "left",
    padding: "8px 12px",
    color: TEXT_MUTED,
    fontWeight: 500,
    fontSize: "0.78rem",
    textTransform: "uppercase",
    letterSpacing: "0.05em",
    borderBottom: BORDER,
    whiteSpace: "nowrap",
  };

  const tdStyle: React.CSSProperties = {
    padding: "9px 12px",
    borderBottom: BORDER,
    fontSize: "0.875rem",
    verticalAlign: "middle",
  };

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />

      <SectionBand title="Injury Report">
        <p style={{ color: TEXT_SECONDARY, marginTop: 0, marginBottom: "1rem", fontSize: "0.9rem" }}>
          Aggregate player injury status across all monitored sports.
        </p>

        <form method="get" style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap", marginBottom: "1rem" }}>
          <select name="sport" defaultValue={selectedSport} style={{ padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }}>
            <option value="">All sports</option>
            {INJURY_SPORTS.map((sport) => (
              <option key={sport} value={sport}>{getDisplayName(sport)}</option>
            ))}
          </select>
          <select name="status" defaultValue={selectedStatus} style={{ padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }}>
            <option value="">All statuses</option>
            <option value="out">Out / IR / GTD</option>
            <option value="questionable">Questionable</option>
            <option value="dtd">Day-to-day</option>
            <option value="doubtful">Doubtful</option>
            <option value="probable">Probable</option>
          </select>
          <input name="q" defaultValue={query} placeholder="Search player, team, note" style={{ minWidth: 220, flex: "1 1 220px", padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }} />
          <button type="submit" style={{ padding: "0.65rem 0.95rem", borderRadius: 8, background: "var(--color-brand)", color: "#fff", border: 0, fontWeight: 700, boxShadow: "var(--shadow-sm)" }}>Filter</button>
          <Link href="/injuries" style={{ display: "inline-flex", alignItems: "center", padding: "0.65rem 0.2rem", color: TEXT_SECONDARY, textDecoration: "none", fontWeight: 600 }}>Reset</Link>
        </form>

        {filtered.length === 0 ? (
          <p style={{ color: TEXT_SECONDARY, padding: "1rem 0" }}>
            No injuries match the current filters.
          </p>
        ) : (
          <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
            {[
              { label: "Out / IR / GTD", count: outCount, color: "#ef4444" },
              { label: "Questionable / DTD", count: questCount, color: "#f59e0b" },
              { label: "Total", count: filtered.length, color: TEXT_SECONDARY },
            ].map((item) => (
              <div
                key={item.label}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.5rem",
                  padding: "6px 14px",
                  borderRadius: 8,
                  background: PANEL_BG,
                  border: BORDER,
                  fontSize: "0.875rem",
                }}
              >
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: item.color, flexShrink: 0 }} />
                <span style={{ color: TEXT_SECONDARY, fontWeight: 600 }}>{item.count}</span>
                <span style={{ color: TEXT_MUTED }}>{item.label}</span>
              </div>
            ))}
          </div>
        )}
      </SectionBand>

      {filtered.length > 0 && (
        <SectionBand title={`All Players (${filtered.length}${filtered.length !== all.length ? ` of ${all.length}` : ""})`}>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={thStyle}>Sport</th>
                  <th style={thStyle}>Player</th>
                  <th style={thStyle}>Team</th>
                  <th style={thStyle}>Status</th>
                  <th style={thStyle}>Body Part</th>
                  <th style={thStyle}>Return</th>
                  <th style={thStyle}>Notes</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((inj, i) => {
                  const teamId = inj.team_id;
                  const team = teamId ? teamLookup[inj._sport]?.[teamId] : null;
                  const returnDate = inj.return_date ? inj.return_date.slice(0, 10) : null;
                  return (
                    <tr key={i}>
                      <td style={tdStyle}>
                        <span
                          style={{
                            display: "inline-block",
                            padding: "2px 7px",
                            borderRadius: 4,
                            fontSize: "0.72rem",
                            fontWeight: 600,
                            background: PANEL_BG,
                            color: TEXT_SECONDARY,
                          }}
                        >
                          {getDisplayName(inj._sport)}
                        </span>
                      </td>
                      <td style={{ ...tdStyle, fontWeight: 600 }}>
                        {inj.player_id ? (
                          <Link
                            href={`/players/${inj.player_id}`}
                            style={{ color: "inherit", textDecoration: "none" }}
                          >
                            {inj.player_name ?? inj.player_id}
                          </Link>
                        ) : (
                          inj.player_name ?? "—"
                        )}
                      </td>
                      <td style={tdStyle}>
                        {teamId ? (
                          <Link
                            href={`/teams/${inj._sport}/${teamId}`}
                            style={{ color: "var(--color-brand)", textDecoration: "none", fontSize: "0.8rem", fontWeight: 700 }}
                          >
                            {team?.abbreviation ?? team?.name ?? teamId}
                          </Link>
                        ) : (
                          <span style={{ color: TEXT_MUTED }}>—</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        <StatusPill status={inj.status ?? "Unknown"} />
                      </td>
                      <td style={{ ...tdStyle, color: TEXT_SECONDARY, fontSize: "0.82rem" }}>
                        {inj.body_part ?? "—"}
                      </td>
                      <td style={{ ...tdStyle, color: TEXT_SECONDARY, fontSize: "0.82rem" }}>
                        {returnDate ?? "—"}
                      </td>
                      <td
                        style={{
                          ...tdStyle,
                          color: TEXT_SECONDARY,
                          fontSize: "0.8rem",
                          maxWidth: 260,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                        title={inj.description ?? undefined}
                      >
                        {inj.description ?? "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </SectionBand>
      )}
    </>
  );
}
