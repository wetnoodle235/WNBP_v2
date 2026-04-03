import Link from "next/link";
import { notFound } from "next/navigation";
import { Suspense } from "react";
import type { Metadata } from "next";
import { SPORTS, isSportKey, type SportKey } from "@/lib/sports";
import { buildPageMetadata } from "@/lib/seo";
import { getGames, getStandings, getPredictions, getOdds, getNews, getInjuries, getMarketSignals, getScheduleFatigue, getTeams } from "@/lib/api";
import {
  SectionBand,
  StoryCard,
  StatCard,
  SportBadge,
  TeamBadge,
} from "@/components/ui";
import { SkeletonCard, SkeletonTable } from "@/components/LoadingSkeleton";
import { ElitePicksGrid } from "@/components/ElitePicksGrid";
import type { ElitePick } from "@/components/ElitePickCard";
import { ConfidenceBar } from "@/components/ConfidenceBar";

export const dynamic = "force-dynamic";

interface PageProps {
  params: Promise<{ sport: string }>;
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { sport } = await params;
  const def = isSportKey(sport) ? SPORTS[sport] : null;
  if (!def) return { title: "Not Found" };
  return buildPageMetadata({
    title: def.label,
    description: `${def.label} daily games, predictions, and latest headlines.`,
    path: `/${sport}`,
  });
}

/* ── Injury status pill ── */
function InjuryStatusPill({ status }: { status: string }) {
  const lo = status.toLowerCase();
  const style =
    lo.includes("out") ? { bg: "rgba(239,68,68,0.15)", color: "var(--color-loss, #dc2626)" } :
    lo.includes("question") ? { bg: "rgba(245,158,11,0.15)", color: "var(--color-neutral, #b45309)" } :
    lo.includes("dtd") || lo.includes("day-to-day") ? { bg: "rgba(249,115,22,0.15)", color: "var(--color-neutral, #c2410c)" } :
    { bg: "var(--color-bg-3)", color: "var(--color-text-muted)" };
  return (
    <span style={{
      display: "inline-block", padding: "2px 7px",
      borderRadius: "var(--radius-sm)", background: style.bg, color: style.color,
      fontSize: "var(--text-xs)", fontWeight: 700,
      textTransform: "uppercase", letterSpacing: "0.05em", whiteSpace: "nowrap",
    }}>
      {status}
    </span>
  );
}

/* ── Data loaders ─────────────────────────────────────────────────────── */

async function GamesSection({ sport }: { sport: SportKey }) {
  const today = new Date().toISOString().slice(0, 10);
  const games = await getGames(sport, { date: today });

  if (games.length === 0) {
    return (
      <div className="responsive-table-wrap">
        <table className="data-table responsive-table sport-games-table">
          <thead><tr><th>Matchup</th><th>Status</th><th style={{ textAlign: "right" }}>Score</th></tr></thead>
          <tbody><tr><td colSpan={3} className="text-secondary">No game data available.</td></tr></tbody>
        </table>
      </div>
    );
  }

  return (
    <div className="responsive-table-wrap">
      <table className="data-table responsive-table sport-games-table">
        <thead>
          <tr>
            <th>Matchup</th>
            <th>Status</th>
            <th style={{ textAlign: "right" }}>Score</th>
          </tr>
        </thead>
        <tbody>
          {games.slice(0, 12).map((row) => (
            <tr key={row.id} style={{ cursor: "pointer" }}>
              <td>
                <Link
                  href={`/games/${sport}/${row.id}`}
                  style={{ textDecoration: "none", color: "inherit", display: "inline-flex", alignItems: "center", gap: "var(--space-3)" }}
                >
                  <TeamBadge name={row.away_team ?? "AWAY"} abbrev={row.away_team} sport={sport} size="sm" />
                  <span className="text-secondary">@</span>
                  <TeamBadge name={row.home_team ?? "HOME"} abbrev={row.home_team} sport={sport} size="sm" />
                </Link>
              </td>
              <td>
                <span style={{
                  fontSize: "var(--text-xs)", fontWeight: 600, textTransform: "capitalize",
                  color: row.status === "in_progress" ? "#10b981" : "var(--color-text-secondary)",
                }}>
                  {row.status ?? "—"}
                </span>
              </td>
              <td style={{ textAlign: "right" }}>
                {row.home_score != null && row.away_score != null
                  ? <span style={{ fontVariantNumeric: "tabular-nums" }}>{row.away_score} – {row.home_score}</span>
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

async function StandingsSection({ sport }: { sport: SportKey }) {
  const currentYear = new Date().getFullYear();
  const standings = await getStandings(sport, currentYear);

  if (standings.length === 0) {
    return (
      <p style={{ color: "var(--color-text-muted)", padding: "var(--space-4) 0" }}>
        No standings data available.
      </p>
    );
  }

  // Group by conference
  const conferences = new Map<string, typeof standings>();
  for (const s of standings) {
    const conf = s.conference ?? "";
    if (!conferences.has(conf)) conferences.set(conf, []);
    conferences.get(conf)!.push(s);
  }

  return (
    <div style={{ display: "grid", gridTemplateColumns: conferences.size > 1 ? "repeat(2, 1fr)" : "1fr", gap: "var(--space-6)" }}>
      {Array.from(conferences.entries()).map(([conf, teams]) => (
        <div key={conf || "all"}>
          {conf && (
            <div style={{
              fontSize: "var(--text-xs)", fontWeight: 800, textTransform: "uppercase",
              letterSpacing: "0.08em", color: "var(--color-accent)", marginBottom: "var(--space-3)",
            }}>
              {conf}ern Conference
            </div>
          )}
          <div className="responsive-table-wrap">
            <table className="responsive-table sport-standings-table" aria-label="Upcoming predictions" style={{ borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                  {["#", "Team", "W-L", "Win%"].map((h, j) => (
                    <th key={h} style={{
                      textAlign: j < 2 ? "left" : "right",
                      padding: "var(--space-1) var(--space-2)",
                      fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 600,
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {teams.sort((a, b) => (a.overall_rank ?? a.conference_rank ?? 99) - (b.overall_rank ?? b.conference_rank ?? 99)).slice(0, 6).map((st, idx) => {
                  const total = st.wins + st.losses;
                  const winPct = total > 0 ? (st.wins / total).toFixed(3) : ".000";
                  const abbr = (st.team_id ?? "").toUpperCase();
                  return (
                    <tr key={st.team_id ?? idx} style={{ borderBottom: "1px solid var(--color-border)" }}>
                      <td style={{ padding: "var(--space-2)", color: "var(--color-text-muted)", fontWeight: 600, fontSize: "var(--text-xs)" }}>
                        {idx + 1}
                      </td>
                      <td style={{ padding: "var(--space-2)" }}>
                        <TeamBadge name={abbr} abbrev={abbr} sport={sport} size="sm" />
                      </td>
                      <td style={{ textAlign: "right", padding: "var(--space-2)", fontVariantNumeric: "tabular-nums", fontSize: "var(--text-xs)", color: "var(--color-text-secondary)" }}>
                        {st.wins}–{st.losses}
                      </td>
                      <td style={{ textAlign: "right", padding: "var(--space-2)", fontVariantNumeric: "tabular-nums", fontSize: "var(--text-xs)", color: "var(--color-text-muted)" }}>
                        {winPct}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
}

async function PredictionsSection({ sport }: { sport: SportKey }) {
  const predictions = await getPredictions(sport);

  if (predictions.length === 0) {
    return (
      <p style={{ color: "var(--color-text-muted)", padding: "var(--space-4) 0" }}>
        No predictions available.
      </p>
    );
  }

  return (
    <div className="data-table-wrap responsive-table-wrap">
      <table className="data-table responsive-table sport-predictions-table">
        <thead>
          <tr>
            <th>Game</th>
            <th style={{ textAlign: "right" }}>Home Win</th>
            <th style={{ textAlign: "right" }}>Away Win</th>
            <th style={{ textAlign: "right" }}>Total</th>
            <th style={{ textAlign: "right" }}>Spread</th>
            <th style={{ textAlign: "center", minWidth: 100 }}>Conf</th>
          </tr>
        </thead>
        <tbody>
          {predictions.slice(0, 8).map((p, i) => {
            const homeWin = (p.home_win_prob ?? 0) > (p.away_win_prob ?? 0);
            return (
              <tr key={p.game_id ?? i}>
                <td style={{ fontWeight: 600 }}>{p.game_id}</td>
                <td style={{
                  textAlign: "right", fontVariantNumeric: "tabular-nums", fontSize: "0.82rem",
                  ...(homeWin ? { fontWeight: 700, color: "var(--color-win, #16a34a)" } : {}),
                }}>
                  {p.home_win_prob != null ? `${(p.home_win_prob * 100).toFixed(1)}%` : "—"}
                </td>
                <td style={{
                  textAlign: "right", fontVariantNumeric: "tabular-nums", fontSize: "0.82rem",
                  ...(!homeWin && p.away_win_prob != null ? { fontWeight: 700, color: "var(--color-win, #16a34a)" } : {}),
                }}>
                  {p.away_win_prob != null ? `${(p.away_win_prob * 100).toFixed(1)}%` : "—"}
                </td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontSize: "0.82rem" }}>
                  {p.predicted_total != null ? p.predicted_total.toFixed(1) : "—"}
                </td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontSize: "0.82rem" }}>
                  {p.predicted_spread != null ? (p.predicted_spread > 0 ? "+" : "") + p.predicted_spread.toFixed(1) : "—"}
                </td>
                <td style={{ textAlign: "center", padding: "0.25rem 0.4rem" }}>
                  {p.confidence != null ? (
                    <ConfidenceBar value={p.confidence} height={18} compact />
                  ) : "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

type InjuryRow = Awaited<ReturnType<typeof getInjuries>>[number];

function InjuriesSection({ injuries }: { injuries: InjuryRow[] }) {
  const filtered = injuries.slice(0, 8);

  if (filtered.length === 0) return null;

  return (
    <div className="responsive-table-wrap">
      <table className="data-table responsive-table sport-injuries-table">
        <thead>
          <tr>
            <th>Player</th>
            <th>Team</th>
            <th>Status</th>
            <th>Reason</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((inj, i) => (
            <tr key={i}>
              <td style={{ fontWeight: 600 }}>{inj.player_name}</td>
              <td>
                <span style={{
                  display: "inline-block", padding: "2px 7px",
                  borderRadius: "var(--radius-sm)", background: "var(--color-bg-3)",
                  fontSize: "var(--text-xs)", fontWeight: 700,
                  color: "var(--color-text-secondary)", textTransform: "uppercase",
                  letterSpacing: "0.05em",
                }}>
                  {(inj.team_id ?? "").split(" ").slice(-1)[0]}
                </span>
              </td>
              <td><InjuryStatusPill status={inj.status ?? "Unknown"} /></td>
              <td style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", maxWidth: 240 }}>
                {inj.description ?? ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

async function NewsSection({ sport }: { sport: SportKey }) {
  const articles = await getNews(sport, 3);

  if (articles.length === 0) {
    return (
      <p style={{ color: "var(--color-text-muted)", padding: "var(--space-4) 0" }}>
        No recent news.
      </p>
    );
  }

  return (
    <div className="grid-3">
      {articles.map((story, i) => (
        <StoryCard
          key={story.id ?? i}
          href={story.link ?? "#"}
          title={story.headline}
          excerpt={story.description ?? undefined}
          imageUrl={story.image_url ?? undefined}
          publishedAt={story.published ?? undefined}
          meta={<SportBadge sport={sport} />}
        />
      ))}
    </div>
  );
}

/* ── Page ──────────────────────────────────────────────────────────────── */

async function ElitePicksSection({ sport }: { sport: SportKey }) {
  const predictions = await getPredictions(sport);

  const elitePicks: ElitePick[] = predictions
    .filter((p) => p.confidence != null && p.confidence > 0.5 && p.home_win_prob != null && p.away_win_prob != null)
    .sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0))
    .slice(0, 6)
    .map((p) => {
      const homeWin = (p.home_win_prob ?? 0) >= (p.away_win_prob ?? 0);
      return {
        game_id: p.game_id,
        sport: p.sport || sport,
        home_team: p.home_team ?? "HOME",
        away_team: p.away_team ?? "AWAY",
        predicted_winner: homeWin ? (p.home_team ?? "HOME") : (p.away_team ?? "AWAY"),
        win_prob: homeWin ? (p.home_win_prob ?? 0) : (p.away_win_prob ?? 0),
        confidence: p.confidence ?? 0,
        predicted_spread: p.predicted_spread ?? null,
        predicted_total: p.predicted_total ?? null,
      };
    });

  if (elitePicks.length === 0) return null;

  return <ElitePicksGrid picks={elitePicks} />;
}

export default async function SportPage({ params }: PageProps) {
  const { sport } = await params;

  if (!isSportKey(sport)) {
    notFound();
  }

  const sportUpper = sport.toUpperCase();

  return (
    <>
      {/* ── A) Overview stats ── */}
      <SectionBand
        id={`${sport}-overview`}
        title={`${sportUpper} Today`}
        action={<SportBadge sport={sport} />}
      >
        <Suspense fallback={<div className="grid-4">{Array.from({ length: 4 }, (_, i) => <div key={i} className="stat-card"><div className="skeleton" style={{ height: 40 }} /></div>)}</div>}>
          <OverviewStats sport={sport} />
        </Suspense>
      </SectionBand>

      {/* ── A½) Elite Picks ── */}
      <SectionBand
        id={`${sport}-elite`}
        title="🔒 Elite Picks"
        accentColor="var(--color-bracket-elite)"
        action={<Link href="/predictions">All picks</Link>}
      >
        <Suspense fallback={<div className="elite-picks-grid">{Array.from({ length: 3 }, (_, i) => <div key={i} className="elite-pick-card"><div className="skeleton" style={{ height: 120 }} /></div>)}</div>}>
          <ElitePicksSection sport={sport} />
        </Suspense>
      </SectionBand>

      {/* ── B) Game Board ── */}
      <SectionBand
        id={`${sport}-games`}
        title="Game Board"
        action={<Link href="/predictions">All predictions</Link>}
      >
        <Suspense fallback={<SkeletonTable />}>
          <GamesSection sport={sport} />
        </Suspense>
      </SectionBand>

      {/* ── C) Today's Top Model Picks ── */}
      <SectionBand
        id={`${sport}-picks`}
        title="Today's Top Model Picks"
        action={<Link href="/predictions">View all picks</Link>}
      >
        <Suspense fallback={<SkeletonTable />}>
          <PredictionsSection sport={sport} />
        </Suspense>
      </SectionBand>

      {/* ── D) Key Injuries Today ── */}
      <Suspense fallback={null}>
        <InjuriesSectionWrapper sport={sport} />
      </Suspense>

      {/* ── E) Conference Standings ── */}
      <SectionBand id={`${sport}-standings`} title="Conference Standings">
        <Suspense fallback={<SkeletonTable />}>
          <StandingsSection sport={sport} />
        </Suspense>
      </SectionBand>

      {/* ── F) Market Signals ── */}
      <Suspense fallback={null}>
        <MarketSignalsSectionWrapper sport={sport} />
      </Suspense>

      {/* ── G) Schedule Fatigue ── */}
      <Suspense fallback={null}>
        <FatigueSectionWrapper sport={sport} />
      </Suspense>

      {/* ── H) Related News ── */}
      <SectionBand
        id={`${sport}-news`}
        title="Related News"
        action={<Link href="/news">News hub</Link>}
      >
        <Suspense fallback={<div className="grid-3">{Array.from({ length: 3 }, (_, i) => <SkeletonCard key={i} rows={2} />)}</div>}>
          <NewsSection sport={sport} />
        </Suspense>
      </SectionBand>
    </>
  );
}

async function OverviewStats({ sport }: { sport: SportKey }) {
  const today = new Date().toISOString().slice(0, 10);
  const [games, predictions, injuries, news] = await Promise.allSettled([
    getGames(sport, { date: today }),
    getPredictions(sport),
    getInjuries(sport),
    getNews(sport, 3),
  ]);

  const gamesCount = games.status === "fulfilled" ? games.value.length : 0;
  const predsCount = predictions.status === "fulfilled" ? predictions.value.length : 0;
  const injCount = injuries.status === "fulfilled" ? injuries.value.length : 0;
  const newsCount = news.status === "fulfilled" ? news.value.length : 0;

  return (
    <div className="grid-4">
      <StatCard label="Games Today" value={gamesCount} />
      <StatCard label="Top Picks" value={predsCount} />
      <StatCard label="Injuries" value={injCount} />
      <StatCard label="Stories" value={newsCount} />
    </div>
  );
}

async function InjuriesSectionWrapper({ sport }: { sport: SportKey }) {
  const injuries = await getInjuries(sport);
  const filtered = injuries.filter((inj) => {
    const lo = (inj.status ?? "").toLowerCase();
    return lo.includes("out") || lo.includes("question");
  });
  if (filtered.length === 0) return null;

  return (
    <SectionBand id={`${sport}-injuries`} title="Key Injuries Today">
      <InjuriesSection injuries={filtered} />
    </SectionBand>
  );
}

async function MarketSignalsSectionWrapper({ sport }: { sport: SportKey }) {
  const signals = await getMarketSignals(sport, { limit: "6" }).catch(() => []);
  if (signals.length === 0) return null;

  const REGIME_COLORS: Record<string, string> = {
    volatile: "#ef4444",
    moving: "#f59e0b",
    stable: "#22c55e",
  };

  return (
    <SectionBand
      id={`${sport}-market-signals`}
      title="Market Signals"
      action={<Link href="/market-intel">All signals →</Link>}
    >
      <div style={{ overflowX: "auto" }}>
        <table className="data-table responsive-table" style={{ borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Date</th>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Matchup</th>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Regime</th>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Bookmaker</th>
            </tr>
          </thead>
          <tbody>
            {signals.slice(0, 6).map((s, i) => {
              const regime = (s.market_regime ?? "").toLowerCase();
              const color = REGIME_COLORS[regime] ?? "var(--color-text-muted)";
              const matchup = s.home_team && s.away_team ? `${s.away_team} @ ${s.home_team}` : s.game_id;
              return (
                <tr key={i}>
                  <td style={{ padding: "7px 10px", fontSize: "0.82rem", color: "var(--color-text-muted)" }}>{s.date ? s.date.slice(0, 10) : "—"}</td>
                  <td style={{ padding: "7px 10px", fontSize: "0.82rem" }}>
                    <Link href={`/games/${sport}/${s.game_id}`} style={{ color: "inherit", textDecoration: "none" }}>{matchup}</Link>
                  </td>
                  <td style={{ padding: "7px 10px" }}>
                    <span style={{ display: "inline-block", padding: "2px 7px", borderRadius: 4, fontSize: "0.72rem", fontWeight: 700, textTransform: "capitalize", background: `${color}22`, color }}>{s.market_regime ?? "—"}</span>
                  </td>
                  <td style={{ padding: "7px 10px", fontSize: "0.82rem", color: "var(--color-text-muted)" }}>{s.bookmaker ?? "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </SectionBand>
  );
}

async function FatigueSectionWrapper({ sport }: { sport: SportKey }) {
  const rows = await getScheduleFatigue(sport, { limit: "6" }).catch(() => []);
  if (rows.length === 0) return null;

  const teams = await getTeams(sport).catch(() => []);
  const teamLookup = Object.fromEntries(teams.map((team) => [team.id, team]));

  const sorted = [...rows].sort((a, b) => (b.fatigue_score ?? 0) - (a.fatigue_score ?? 0)).slice(0, 6);

  const LEVEL_COLORS: Record<string, string> = {
    high: "#ef4444",
    medium: "#f59e0b",
    low: "#22c55e",
  };

  return (
    <SectionBand
      id={`${sport}-fatigue`}
      title="Schedule Fatigue"
      action={<Link href="/fatigue">Fatigue board →</Link>}
    >
      <div style={{ overflowX: "auto" }}>
        <table className="data-table responsive-table" style={{ borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Team</th>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Level</th>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Score</th>
              <th style={{ textAlign: "left", padding: "6px 10px", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", fontWeight: 500 }}>Date</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((f, i) => {
              const level = (f.fatigue_level ?? "").toLowerCase();
              const color = LEVEL_COLORS[level] ?? "var(--color-text-muted)";
              const team = f.team_id ? teamLookup[f.team_id] : null;
              return (
                <tr key={i}>
                  <td style={{ padding: "7px 10px", fontSize: "0.82rem", fontWeight: 600 }}>
                    {f.team_id ? (
                      <Link href={`/teams/${sport}/${f.team_id}`} style={{ color: "var(--color-brand, #60a5fa)", textDecoration: "none" }}>{team?.abbreviation ?? team?.name ?? f.team_id}</Link>
                    ) : "—"}
                  </td>
                  <td style={{ padding: "7px 10px" }}>
                    <span style={{ display: "inline-block", padding: "2px 7px", borderRadius: 4, fontSize: "0.72rem", fontWeight: 700, textTransform: "capitalize", background: `${color}22`, color }}>{f.fatigue_level ?? "—"}</span>
                  </td>
                  <td style={{ padding: "7px 10px", fontSize: "0.82rem", color: "var(--color-text-muted)", fontVariantNumeric: "tabular-nums" }}>{f.fatigue_score != null ? f.fatigue_score.toFixed(2) : "—"}</td>
                  <td style={{ padding: "7px 10px", fontSize: "0.82rem", color: "var(--color-text-muted)" }}>{f.date ? f.date.slice(0, 10) : "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </SectionBand>
  );
}
