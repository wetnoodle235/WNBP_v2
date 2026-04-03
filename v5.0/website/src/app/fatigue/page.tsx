import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { PremiumTeaser, SectionBand } from "@/components/ui";
import { getScheduleFatigue, getTeams } from "@/lib/api";
import type { ScheduleFatigue, Team } from "@/lib/schemas";
import { getDisplayName } from "@/lib/sports-config";
import { getViewerTier, hasPremiumTier } from "@/lib/server-access";
import Link from "next/link";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export const metadata: Metadata = buildPageMetadata({
  title: "Fatigue Board",
  description:
    "Schedule fatigue analysis — identify teams playing on short rest or heavy travel schedules across all sports.",
  path: "/fatigue",
  keywords: ["schedule fatigue", "back to back", "rest advantage", "travel fatigue", "short rest"],
});

const FATIGUE_SPORTS = [
  "nba", "nfl", "mlb", "nhl", "wnba",
  "ncaab", "ncaaf", "epl", "mls",
] as const;

const FATIGUE_LEVEL_COLORS: Record<string, string> = {
  high: "var(--color-error, #ef4444)",
  medium: "var(--color-warning, #f59e0b)",
  low: "var(--color-success, #22c55e)",
};

const PANEL_BG = "var(--color-bg-3)";
const CARD_BG = "var(--color-bg-2)";
const BORDER = "1px solid var(--color-border)";
const TEXT_MUTED = "var(--color-text-muted)";
const TEXT_SECONDARY = "var(--color-text-secondary)";

function FatigueLevelBadge({ level }: { level: string | null | undefined }) {
  const label = level ?? "unknown";
  const color = FATIGUE_LEVEL_COLORS[label.toLowerCase()] ?? "var(--color-text-muted)";
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 4,
        fontSize: "0.75rem",
        fontWeight: 600,
        textTransform: "capitalize",
        color: "#fff",
        background: color,
      }}
    >
      {label}
    </span>
  );
}

function ScoreBar({ score }: { score: number | null | undefined }) {
  const pct = Math.min(100, Math.max(0, Math.round((score ?? 0) * 100)));
  const color =
    pct >= 70
      ? "var(--color-error, #ef4444)"
      : pct >= 40
      ? "var(--color-warning, #f59e0b)"
      : "var(--color-success, #22c55e)";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, minWidth: 120 }}>
      <div
        style={{
          flex: 1,
          height: 6,
          borderRadius: 3,
          background: PANEL_BG,
          overflow: "hidden",
        }}
      >
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 3 }} />
      </div>
      <span style={{ fontSize: "0.8rem", color: TEXT_SECONDARY, minWidth: 28 }}>
        {score != null ? score.toFixed(2) : "—"}
      </span>
    </div>
  );
}

function readSearchParam(value: string | string[] | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

interface FatigueBoardPageProps {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}

export default async function FatigueBoardPage({ searchParams }: FatigueBoardPageProps) {
  const tier = await getViewerTier();
  const hasPremium = hasPremiumTier(tier);
  const filters = searchParams ? await searchParams : {};
  const selectedSport = readSearchParam(filters.sport).toLowerCase();
  const selectedLevel = readSearchParam(filters.level).toLowerCase();
  const query = readSearchParam(filters.q).toLowerCase();
  const [fatigueBatches, teamBatches] = await Promise.all([
    Promise.allSettled(
      FATIGUE_SPORTS.map((sport) =>
        getScheduleFatigue(sport, { limit: "30" }).then((rows) =>
          rows.map((r) => ({ ...r, _sport: sport }))
        )
      )
    ),
    Promise.allSettled(
      FATIGUE_SPORTS.map((sport) =>
        getTeams(sport).then((rows) => ({ sport, rows }))
      )
    ),
  ]);

  const allFatigue: (ScheduleFatigue & { _sport: string })[] = [];
  for (const result of fatigueBatches) {
    if (result.status === "fulfilled") {
      allFatigue.push(...result.value);
    }
  }

  const teamLookup: Record<string, Record<string, Team>> = {};
  for (const result of teamBatches) {
    if (result.status !== "fulfilled") continue;
    teamLookup[result.value.sport] = Object.fromEntries(
      result.value.rows.map((team) => [team.id, team])
    );
  }

  const filteredFatigue = allFatigue.filter((row) => {
    if (selectedSport && row._sport.toLowerCase() !== selectedSport) return false;
    if (selectedLevel && (row.fatigue_level ?? "").toLowerCase() !== selectedLevel) return false;
    if (!query) return true;
    const teamId = row.team_id ?? "";
    const team = teamId ? teamLookup[row._sport]?.[teamId] : null;
    const haystack = [team?.name, team?.abbreviation, teamId, row._sport].join(" ").toLowerCase();
    return haystack.includes(query);
  });

  // Sort: high fatigue first (score desc), then medium, then low
  filteredFatigue.sort((a, b) => {
    const scoreB = b.fatigue_score ?? 0;
    const scoreA = a.fatigue_score ?? 0;
    return scoreB - scoreA;
  });

  // Compute summary counts
  const highCount = filteredFatigue.filter(
    (f) => (f.fatigue_level ?? "").toLowerCase() === "high"
  ).length;
  const medCount = filteredFatigue.filter(
    (f) => (f.fatigue_level ?? "").toLowerCase() === "medium"
  ).length;
  const lowCount = filteredFatigue.filter(
    (f) => (f.fatigue_level ?? "").toLowerCase() === "low"
  ).length;

  const jsonLd = buildCollectionJsonLd({
    name: "Fatigue Board",
    path: "/fatigue",
    description: "Schedule fatigue rankings across all supported sports.",
  });

  const thStyle: React.CSSProperties = {
    textAlign: "left",
    padding: "8px 12px",
    color: TEXT_MUTED,
    fontWeight: 500,
    fontSize: "0.8rem",
    textTransform: "uppercase",
    letterSpacing: "0.05em",
    borderBottom: BORDER,
    whiteSpace: "nowrap",
  };

  const tdStyle: React.CSSProperties = {
    padding: "10px 12px",
    borderBottom: BORDER,
    fontSize: "0.875rem",
    verticalAlign: "middle",
  };

  const statBarItems = [
    { label: "High Fatigue", count: highCount, color: FATIGUE_LEVEL_COLORS.high },
    { label: "Medium", count: medCount, color: FATIGUE_LEVEL_COLORS.medium },
    { label: "Low", count: lowCount, color: FATIGUE_LEVEL_COLORS.low },
  ];

  const visibleFatigue = hasPremium ? filteredFatigue : filteredFatigue.slice(0, 12);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />

      <SectionBand title="Fatigue Board">
        <p style={{ color: TEXT_SECONDARY, marginTop: 0, marginBottom: "1rem", fontSize: "0.9rem" }}>
          Teams ranked by schedule fatigue — back-to-back games, travel load, and rest disadvantage.
        </p>
        <form method="get" style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap", marginBottom: "1rem" }}>
          <select name="sport" defaultValue={selectedSport} style={{ padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }}>
            <option value="">All sports</option>
            {FATIGUE_SPORTS.map((sport) => (
              <option key={sport} value={sport}>{getDisplayName(sport)}</option>
            ))}
          </select>
          <select name="level" defaultValue={selectedLevel} style={{ padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }}>
            <option value="">All levels</option>
            <option value="high">High</option>
            <option value="medium">Medium</option>
            <option value="low">Low</option>
          </select>
          <input name="q" defaultValue={query} placeholder="Search team" style={{ minWidth: 220, flex: "1 1 220px", padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }} />
          <button type="submit" style={{ padding: "0.65rem 0.95rem", borderRadius: 8, background: "var(--color-brand)", color: "#fff", border: 0, fontWeight: 700, boxShadow: "var(--shadow-sm)" }}>Filter</button>
          <Link href="/fatigue" style={{ display: "inline-flex", alignItems: "center", padding: "0.65rem 0.2rem", color: TEXT_SECONDARY, textDecoration: "none", fontWeight: 600 }}>Reset</Link>
        </form>
        {filteredFatigue.length === 0 ? (
          <p style={{ color: TEXT_SECONDARY, padding: "1rem 0" }}>
            No fatigue rows match the current filters.
          </p>
        ) : (
          <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap" }}>
            {statBarItems.map((item) => (
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
                <span
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: "50%",
                    background: item.color,
                    flexShrink: 0,
                  }}
                />
                <span style={{ color: TEXT_SECONDARY, fontWeight: 600 }}>
                  {item.count}
                </span>
                <span style={{ color: TEXT_MUTED }}>{item.label}</span>
              </div>
            ))}
          </div>
        )}
      </SectionBand>

      {!hasPremium && (
        <PremiumTeaser
          icon="⚡"
          message="Upgrade to unlock the full fatigue board, including all teams, rest disadvantage rankings, and cross-sport schedule stress signals."
          ctaHref="/pricing"
        />
      )}

      {visibleFatigue.length > 0 && (
        <SectionBand title={`All Teams by Fatigue Score (${filteredFatigue.length}${filteredFatigue.length !== allFatigue.length ? ` of ${allFatigue.length}` : ""})`}>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={thStyle}>#</th>
                  <th style={thStyle}>Sport</th>
                  <th style={thStyle}>Team</th>
                  <th style={thStyle}>Level</th>
                  <th style={thStyle}>Fatigue Score</th>
                  <th style={thStyle}>Date</th>
                  <th style={thStyle}>Game</th>
                </tr>
              </thead>
              <tbody>
                {visibleFatigue.map((f, i) => {
                  const dateStr = f.date ? f.date.slice(0, 10) : "—";
                  const teamId = f.team_id;
                  const team = teamId ? teamLookup[f._sport]?.[teamId] : null;
                  return (
                    <tr key={i}>
                      <td style={{ ...tdStyle, color: TEXT_MUTED, width: 36 }}>
                        {i + 1}
                      </td>
                      <td style={tdStyle}>
                        <span
                          style={{
                            display: "inline-block",
                            padding: "2px 7px",
                            borderRadius: 4,
                            fontSize: "0.75rem",
                            fontWeight: 600,
                            background: PANEL_BG,
                            color: TEXT_SECONDARY,
                          }}
                        >
                          {getDisplayName(f._sport)}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        {teamId ? (
                          <Link
                            href={`/teams/${f._sport}/${teamId}`}
                            style={{ color: "var(--color-brand)", textDecoration: "none", fontWeight: 700 }}
                          >
                            {team?.abbreviation ?? team?.name ?? teamId}
                          </Link>
                        ) : (
                          <span style={{ color: TEXT_MUTED }}>—</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        <FatigueLevelBadge level={f.fatigue_level} />
                      </td>
                      <td style={tdStyle}>
                        <ScoreBar score={f.fatigue_score} />
                      </td>
                      <td style={{ ...tdStyle, color: TEXT_SECONDARY }}>
                        {dateStr}
                      </td>
                      <td style={tdStyle}>
                        <Link
                          href={`/games/${f._sport}/${f.game_id}`}
                          style={{ color: "var(--color-brand)", textDecoration: "none", fontSize: "0.8rem", fontWeight: 700 }}
                        >
                          View →
                        </Link>
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
