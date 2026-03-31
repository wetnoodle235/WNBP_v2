import Link from "next/link";
import type { Metadata } from "next";
import { getGames, getPredictions, getNews, getTeams } from "@/lib/api";
import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import { formatRelativeTime, formatProbability } from "@/lib/formatters";
import {
  FREE_PREDICTION_PREVIEW_LIMIT,
  getViewerTier,
  hasEnterpriseDocsAccess,
  hasPremiumTier,
  limitPredictionPreview,
} from "@/lib/server-access";
import {
  SectionBand,
  StoryCard,
  StatCard,
  PremiumTeaser,
  SportBadge,
  Badge,
  TeamBadge,
} from "@/components/ui";
import { ElitePicksGrid } from "@/components/ElitePicksGrid";
import type { ElitePick } from "@/components/ElitePickCard";
import { ConfidenceBar } from "@/components/ConfidenceBar";
import { getSportIcon, SPORT_CATEGORIES, getDisplayName, getLeagueLogoUrl } from "@/lib/sports-config";
import { LeagueLogo } from "@/components/ui/LeagueLogo";

export const dynamic = "auto";

export const metadata: Metadata = buildPageMetadata({
  title: "Home",
  description:
    "Data-driven multi-sport predictions powered by normalized platform data feeds.",
  path: "/",
  keywords: ["sports predictions", "NBA picks", "MLB picks", "NFL predictions", "machine learning sports", "data analytics", "WNBP"],
});

type Story = {
  id: string;
  headline: string;
  description?: string;
  published?: string;
  image_url?: string;
  link?: string;
  sport: string;
};

type GameMatchup = {
  id: string;
  sport: string;
  homeName: string;
  awayName: string;
  homeLogo?: string;
  awayLogo?: string;
  homeScore?: number;
  awayScore?: number;
  status?: string;
  startTime?: string;
  timeLabel?: string;
  confidence?: string;
};

const HOME_SPORTS = Array.from(
  new Set(
    SPORT_CATEGORIES.flatMap((cat) => cat.sports.map((sport) => (sport === "cs2" ? "csgo" : sport))),
  ),
);

const CONF_VARIANTS: Record<string, "live" | "win" | "free"> = {
  LOCK: "live", STRONG: "live", ELITE: "live",
  GOOD: "win", VERY_HIGH: "win", HIGH: "win",
  MEDIUM: "free", WEAK: "free", LOW: "free",
};

type TeamRow = {
  id?: string;
  name?: string;
  abbreviation?: string;
  logo_url?: string;
};

function normalizeName(value?: string): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function isFinalStatus(status?: string): boolean {
  const s = (status ?? "").trim().toLowerCase();
  return s.includes("final") || s.includes("completed") || s === "closed" || s === "post";
}

function formatTimeToStart(startTime?: string | null): string | undefined {
  if (!startTime) return undefined;
  const start = new Date(startTime);
  if (Number.isNaN(start.getTime())) return undefined;
  const diffMs = start.getTime() - Date.now();
  const absMin = Math.round(Math.abs(diffMs) / 60000);
  if (diffMs > 0) {
    const h = Math.floor(absMin / 60);
    const m = absMin % 60;
    if (h > 0) return `Starts in ${h}h ${m}m`;
    return `Starts in ${m}m`;
  }
  if (absMin < 180) return `Started ${absMin}m ago`;
  return undefined;
}

async function getHomeData(hasPremium: boolean) {
  const todayDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const [gamesResults, newsResults, predictionsResults, teamsResults] = await Promise.all([
    Promise.allSettled(HOME_SPORTS.map((s) => getGames(s, { date: todayDate }))),
    Promise.allSettled(HOME_SPORTS.map((s) => getNews(s, 3))),
    Promise.allSettled(HOME_SPORTS.map((s) => getPredictions(s, { date: todayDate }))),
    Promise.allSettled(HOME_SPORTS.map((s) => getTeams(s))),
  ]);

  const teamMaps = teamsResults.map((r) => {
    const byId = new Map<string, string>();
    const byName = new Map<string, string>();
    if (r.status !== "fulfilled") return { byId, byName };
    for (const team of r.value as TeamRow[]) {
      const logo = team.logo_url;
      if (!logo) continue;
      if (team.id) byId.set(String(team.id), logo);
      if (team.name) byName.set(normalizeName(team.name), logo);
      if (team.abbreviation) byName.set(normalizeName(team.abbreviation), logo);
    }
    return { byId, byName };
  });

  const allMatchups: GameMatchup[] = gamesResults.flatMap((r, i) => {
    if (r.status !== "fulfilled") return [];
    const map = teamMaps[i];

    const logoFor = (teamId?: string | null, teamName?: string | null) => {
      if (!map) return undefined;
      if (teamId && map.byId.has(String(teamId))) return map.byId.get(String(teamId));
      const key = normalizeName(teamName ?? undefined);
      if (key && map.byName.has(key)) return map.byName.get(key);
      return undefined;
    };

    return r.value
      .filter((g) => !isFinalStatus(g.status ?? undefined))
      .slice(0, 8)
      .map((g) => ({
      id: g.id ?? `${HOME_SPORTS[i]}-${Math.random()}`,
      sport: g.sport || HOME_SPORTS[i],
      homeName: g.home_team ?? "HOME",
      awayName: g.away_team ?? "AWAY",
      homeLogo: logoFor(g.home_team_id ?? null, g.home_team ?? null),
      awayLogo: logoFor(g.away_team_id ?? null, g.away_team ?? null),
      homeScore: g.home_score ?? undefined,
      awayScore: g.away_score ?? undefined,
      status: g.status ?? undefined,
      startTime: g.start_time ?? undefined,
      timeLabel: formatTimeToStart(g.start_time ?? undefined),
    }));
  });

  const stories: Story[] = newsResults.flatMap((r, i) => {
    if (r.status !== "fulfilled") return [];
    return r.value.slice(0, 2).map((n, j) => ({
      id: n.id ?? `${HOME_SPORTS[i]}-${j}`,
      headline: n.headline,
      description: n.description ?? undefined,
      published: n.published ?? undefined,
      image_url: n.image_url ?? undefined,
      link: n.link ?? undefined,
      sport: n.sport || HOME_SPORTS[i],
    }));
  });

  // All predictions (raw set)
  const allPredictionsRaw = predictionsResults.flatMap((r, i) =>
    r.status === "fulfilled"
      ? r.value.map((p) => ({ ...p, sport: p.sport || HOME_SPORTS[i] }))
      : [],
  );

  const rankedPredictions = [...allPredictionsRaw].sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0));
  const predictions = hasPremium
    ? rankedPredictions
    : limitPredictionPreview(rankedPredictions, FREE_PREDICTION_PREVIEW_LIMIT);

  // Build elite picks: top 6 predictions by confidence
  const elitePicks: ElitePick[] = allPredictionsRaw
    .filter((p) => p.confidence != null && p.confidence > 0.5 && p.home_win_prob != null && p.away_win_prob != null)
    .sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0))
    .slice(0, 6)
    .map((p) => {
      const homeWin = (p.home_win_prob ?? 0) >= (p.away_win_prob ?? 0);
      return {
        game_id: p.game_id,
        sport: p.sport,
        home_team: p.home_team ?? "HOME",
        away_team: p.away_team ?? "AWAY",
        predicted_winner: homeWin ? (p.home_team ?? "HOME") : (p.away_team ?? "AWAY"),
        win_prob: homeWin ? (p.home_win_prob ?? 0) : (p.away_win_prob ?? 0),
        confidence: p.confidence ?? 0,
        predicted_spread: p.predicted_spread ?? null,
        predicted_total: p.predicted_total ?? null,
      };
    });

  // Compute dashboard stats
  const totalPredictions = hasPremium ? allPredictionsRaw.length : predictions.length;
  const highConfPredictions = hasPremium
    ? allPredictionsRaw.filter((p) => (p.confidence ?? 0) > 0.65).length
    : 0;
  const topConfidence = hasPremium
    ? allPredictionsRaw.reduce((max, p) => Math.max(max, p.confidence ?? 0), 0)
    : null;
  const highConfidenceRate = hasPremium && allPredictionsRaw.length > 0
    ? highConfPredictions / allPredictionsRaw.length
    : null;
  const liveGames = allMatchups.filter((g) => {
    const s = (g.status ?? "").toLowerCase();
    return s.includes("in progress") || s.includes("live") || s.includes("in_progress");
  }).length;

  return {
    allMatchups, stories, predictions, elitePicks,
    sportsCovered: HOME_SPORTS.length,
    totalPredictions, highConfPredictions, topConfidence, highConfidenceRate, liveGames,
  };
}

export default async function HomePage() {
  const tier = await getViewerTier();
  const hasPremium = hasPremiumTier(tier);
  const hasEnterpriseAccess = hasEnterpriseDocsAccess(tier);
  const homeData = await getHomeData(hasPremium);
  const {
    allMatchups, stories, predictions, elitePicks, sportsCovered,
    totalPredictions, highConfPredictions, topConfidence, highConfidenceRate, liveGames,
  } = homeData;
  const visiblePredictions = hasPremium ? predictions : predictions.slice(0, 3);
  const jsonLd = buildCollectionJsonLd({
    name: "WNBP Home",
    path: "/",
    description: "Editorial home for multi-sport predictions and stories.",
  });

  const gamesBySport = allMatchups.reduce<Record<string, number>>((acc, game) => {
    const s = game.sport.toLowerCase();
    acc[s] = (acc[s] ?? 0) + 1;
    return acc;
  }, {});
  const activeSports = Object.keys(gamesBySport);

  return (
    <main>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />

      {/* ── VALUE PROPOSITION BANNER (TOP OF PAGE) ── */}
      <section className="home-pitch-banner" aria-labelledby="home-pitch-heading">
        <div className="home-pitch-inner">
          <div className="home-pitch-text">
            <h1 id="home-pitch-heading" className="home-pitch-headline">Stop Guessing. Start Knowing.</h1>
            <p className="home-pitch-sub">
              Our machine learning models crunch thousands of data points every day — player trends,
              matchup history, pace, injury impact, and line movement — so you always have an edge
              before tip-off or first pitch.
            </p>
            <div className="home-pitch-features">
              <div className="home-pitch-feature">
                <span className="home-pitch-icon">🎯</span>
                <div>
                  <strong>Data-Driven Props</strong>
                  <span>Multi-sport player props with model-predicted values and confidence tiers</span>
                </div>
              </div>
              <div className="home-pitch-feature">
                <span className="home-pitch-icon">📊</span>
                <div>
                  <strong>Game Predictions</strong>
                  <span>Full matchup analysis — winner, spread, total, and projected final score</span>
                </div>
              </div>
              <div className="home-pitch-feature">
                <span className="home-pitch-icon">⚡</span>
                <div>
                  <strong>Daily Fresh Picks</strong>
                  <span>New predictions every morning. Free picks available, premium unlocks everything</span>
                </div>
              </div>
            </div>
            <div className="home-pitch-actions">
              <Link href="/predictions" className="btn btn-primary">View Today&apos;s Picks</Link>
              <Link href="/pricing" className="btn btn-secondary">See Plans</Link>
              {hasEnterpriseAccess && (
                <Link href="/api-guide" className="btn btn-ghost">API Guide</Link>
              )}
            </div>
          </div>
          <div className="home-pitch-stat-block">
            <div className="home-pitch-big-stat">
              <span className="home-pitch-big-num">{allMatchups.length > 0 ? allMatchups.length : "—"}</span>
              <span className="home-pitch-big-label">Games Today</span>
            </div>
            <div className="home-pitch-divider" />
            <div className="home-pitch-big-stat">
              <span className="home-pitch-big-num">{totalPredictions > 0 ? totalPredictions : "—"}</span>
              <span className="home-pitch-big-label">Active Predictions</span>
            </div>
            <div className="home-pitch-divider" />
            <div className="home-pitch-big-stat">
              <span className="home-pitch-big-num">
                {hasPremium && topConfidence != null && topConfidence > 0 ? formatProbability(topConfidence) : "🔒"}
              </span>
              <span className="home-pitch-big-label">Top Confidence</span>
            </div>
          </div>
          <div className="home-pitch-trust" aria-label="Supported sports">
            <span className="home-pitch-trust-label">Covering</span>
            <span className="home-pitch-trust-sports">
              {activeSports.slice(0, 8).map((s) => (
                <Link key={s} href={`/${s}`} className="home-pitch-trust-pill">
                  {getSportIcon(s)} {s.toUpperCase()}
                </Link>
              ))}
              {activeSports.length > 8 && (
                <span className="home-pitch-trust-pill">+{activeSports.length - 8} more</span>
              )}
            </span>
          </div>
        </div>
      </section>
      <SectionBand
        id="home-overview"
        title="📊 Dashboard"
        action={<Link href="/stats">Model stats →</Link>}
      >
        <div className="grid-4">
          <StatCard
            label="Total Games"
            value={allMatchups.length > 0 ? allMatchups.length : "—"}
            sub={activeSports.length > 0
              ? `across ${activeSports.map(s => s.toUpperCase()).join(", ")}`
              : "no active sports"}
          />
          <StatCard
            label="Active Predictions"
            value={totalPredictions > 0 ? totalPredictions : "—"}
            accent="win"
            sub={highConfPredictions > 0 ? `${highConfPredictions} high confidence` : undefined}
          />
          <StatCard
            label="Confidence Signal"
            value={hasPremium && highConfidenceRate != null ? `${(highConfidenceRate * 100).toFixed(1)}%` : "🔒"}
            accent="brand"
            sub={hasPremium ? "high-confidence pick rate" : "premium metric"}
          />
          <StatCard
            label="Live Now"
            value={liveGames > 0 ? liveGames : "—"}
            accent={liveGames > 0 ? "loss" : "neutral"}
            sub={liveGames > 0 ? "games in progress" : "no live games"}
          />
        </div>
      </SectionBand>

      {/* ── SPORTS HUB ── */}
      <SectionBand id="home-sports" title="🏟️ Sports Hub">
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
          gap: "var(--space-4)",
        }}>
          {SPORT_CATEGORIES.map((cat) => (
            <div key={cat.label} style={{
              background: "var(--color-bg-2)",
              border: "1px solid var(--color-border)",
              borderRadius: "var(--radius-md)",
              padding: "var(--space-4)",
            }}>
              <div style={{
                fontSize: "var(--text-sm)",
                fontWeight: 700,
                marginBottom: "var(--space-2)",
                color: "var(--color-text-muted)",
                textTransform: "uppercase",
                letterSpacing: "0.04em",
              }}>
                {cat.icon} {cat.label}
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--space-2)" }}>
                {cat.sports.map((sport) => (
                  <Link
                    key={sport}
                    href={`/${sport}`}
                    className="sport-hub-link"
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      gap: "0.25rem",
                      padding: "0.25rem 0.5rem",
                      borderRadius: "var(--radius)",
                      fontSize: "var(--text-xs)",
                      fontWeight: 600,
                      color: "var(--color-text)",
                      background: "var(--color-bg-3)",
                      textDecoration: "none",
                      transition: "background var(--transition-fast)",
                    }}
                  >
                    {getLeagueLogoUrl(sport) ? (
                      <LeagueLogo sport={sport} size={14} className="sports-hub-logo" />
                    ) : getSportIcon(sport)} {getDisplayName(sport)}
                    {gamesBySport[sport] != null && (
                      <span style={{
                        background: "var(--color-brand)",
                        color: "#fff",
                        borderRadius: "var(--radius-full)",
                        fontSize: 10,
                        padding: "0 0.35rem",
                        fontWeight: 700,
                        marginLeft: "2px",
                      }}>
                        {gamesBySport[sport]}
                      </span>
                    )}
                  </Link>
                ))}
              </div>
            </div>
          ))}
        </div>
      </SectionBand>

      {/* ── ELITE PICKS ── */}
      {hasPremium && elitePicks.length > 0 && (
        <SectionBand
          id="home-elite"
          title="🔒 ELITE PLAYS"
          accentColor="var(--color-bracket-elite)"
          action={<Link href="/predictions">All picks →</Link>}
        >
          <ElitePicksGrid picks={elitePicks} />
        </SectionBand>
      )}

      {/* ── TODAY'S GAME SLATE ── */}
      {allMatchups.length > 0 && (
        <SectionBand
          id="home-slate"
          title="Today's Slate"
          action={<Link href="/predictions">Full board →</Link>}
        >
          <div className="home-slate-grid">
            {allMatchups.map((game) => (
              <Link
                key={`${game.sport}_${game.id}`}
                href={`/games/${game.sport}/${game.id}`}
                className="game-matchup-card"
                aria-label={`${game.awayName} at ${game.homeName} — ${game.sport.toUpperCase()}`}
              >
                <div className="game-matchup-sport">
                  <span style={{ color: `var(--color-${game.sport.toLowerCase()})`, fontWeight: "var(--fw-bold)", fontSize: "var(--text-xs)" }}>
                    {game.sport.toUpperCase()}
                  </span>
                  {game.confidence && (
                    <span style={{ fontSize: "10px" }}>
                      <Badge variant={CONF_VARIANTS[game.confidence] ?? "free"}>
                        {game.confidence}
                      </Badge>
                    </span>
                  )}
                </div>
                <div className="game-matchup-teams">
                  <div className="game-matchup-team">
                    <TeamBadge
                      logoOnly
                      size="md"
                      sport={game.sport}
                      logoUrl={game.awayLogo}
                      name={game.awayName}
                      className="game-matchup-team-logo-wrap"
                    />
                    <span className="game-matchup-team-name">{game.awayName}</span>
                  </div>
                  <div className="game-matchup-vs">@</div>
                  <div className="game-matchup-team">
                    <TeamBadge
                      logoOnly
                      size="md"
                      sport={game.sport}
                      logoUrl={game.homeLogo}
                      name={game.homeName}
                      className="game-matchup-team-logo-wrap"
                    />
                    <span className="game-matchup-team-name">{game.homeName}</span>
                  </div>
                </div>
                {game.homeScore != null && game.awayScore != null && (
                  <div className="game-matchup-score">
                    <span className="text-secondary" style={{ fontSize: "var(--text-xs)" }}>Score</span>
                    <span style={{ fontWeight: "var(--fw-semibold)", fontSize: "var(--text-sm)" }}>
                      {game.awayScore}–{game.homeScore}
                    </span>
                  </div>
                )}
                {game.timeLabel && (
                  <div className="game-matchup-total">
                    <span className="text-secondary" style={{ fontSize: "var(--text-xs)" }}>Start</span>
                    <span style={{ fontWeight: "var(--fw-semibold)", fontSize: "var(--text-xs)" }}>
                      {game.timeLabel}
                    </span>
                  </div>
                )}
              </Link>
            ))}
          </div>
        </SectionBand>
      )}

      {/* ── TOP PREDICTIONS ── */}
      {predictions.length > 0 && (
        <SectionBand
          id="home-predictions"
          title={hasPremium ? "Today's Top Predictions" : "Today's Top Predictions (Preview)"}
          action={<Link href="/predictions">View full board →</Link>}
        >
          {!hasPremium && (
            <div className="stale-banner stale-banner-info" style={{ marginBottom: "var(--space-3)" }}>
              <span className="stale-banner-icon" aria-hidden="true">🔒</span>
              <span className="stale-banner-text">
                Free tier shows a limited preview. Upgrade to unlock full probabilities, confidence tiers, and all model outputs.
              </span>
            </div>
          )}
          <div className="data-table-wrap responsive-table-wrap">
            <table className="data-table responsive-table home-predictions-table" aria-label="Top predictions for today">
              <caption className="sr-only">Today&apos;s top game predictions sorted by confidence</caption>
              <thead>
                <tr>
                  <th scope="col" style={{ padding: "0.3rem 0.4rem" }}>Sport</th>
                  <th scope="col" style={{ padding: "0.3rem 0.4rem" }}>Game</th>
                  <th scope="col" style={{ textAlign: "right", padding: "0.3rem 0.4rem" }}>Home</th>
                  <th scope="col" style={{ textAlign: "right", padding: "0.3rem 0.4rem" }}>Away</th>
                  <th scope="col" style={{ textAlign: "right", padding: "0.3rem 0.4rem" }}>Total</th>
                  <th scope="col" style={{ textAlign: "center", padding: "0.3rem 0.4rem" }}>Conf</th>
                </tr>
              </thead>
              <tbody>
                {visiblePredictions.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="text-secondary">Predictions not yet published.</td>
                  </tr>
                ) : (
                  visiblePredictions.slice(0, 10).map((pred, i) => (
                    <tr key={`${pred.sport}_${pred.game_id}_${i}`}>
                      <td style={{ padding: "0.25rem 0.4rem" }}>
                        <span style={{
                          fontSize: "var(--text-xs)",
                          fontWeight: "var(--fw-bold)",
                          color: `var(--color-${pred.sport?.toLowerCase()})`,
                        }}>
                          {pred.sport?.toUpperCase() ?? "—"}
                        </span>
                      </td>
                      <td style={{ fontWeight: 600, whiteSpace: "nowrap", padding: "0.25rem 0.4rem" }}>
                        {pred.home_team && pred.away_team
                          ? `${pred.home_team} vs ${pred.away_team}`
                          : pred.game_id}
                      </td>
                      <td style={{
                        textAlign: "right",
                        fontVariantNumeric: "tabular-nums",
                        fontSize: "0.82rem",
                        padding: "0.25rem 0.4rem",
                        ...(pred.home_win_prob != null && pred.away_win_prob != null && pred.home_win_prob > pred.away_win_prob
                          ? { fontWeight: 700, color: "var(--color-win, #16a34a)" } : {}),
                      }}>
                        {hasPremium
                          ? (pred.home_win_prob != null ? `${(pred.home_win_prob * 100).toFixed(1)}%` : "—")
                          : "🔒"}
                      </td>
                      <td style={{
                        textAlign: "right",
                        fontVariantNumeric: "tabular-nums",
                        fontSize: "0.82rem",
                        padding: "0.25rem 0.4rem",
                        ...(pred.away_win_prob != null && pred.home_win_prob != null && pred.away_win_prob > pred.home_win_prob
                          ? { fontWeight: 700, color: "var(--color-win, #16a34a)" } : {}),
                      }}>
                        {hasPremium
                          ? (pred.away_win_prob != null ? `${(pred.away_win_prob * 100).toFixed(1)}%` : "—")
                          : "🔒"}
                      </td>
                      <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontSize: "0.82rem", padding: "0.25rem 0.4rem" }}>
                        {hasPremium
                          ? (pred.predicted_total != null ? pred.predicted_total.toFixed(1) : "—")
                          : "🔒"}
                      </td>
                      <td style={{ textAlign: "center", padding: "0.25rem 0.4rem", minWidth: 90 }}>
                        {hasPremium && pred.confidence != null ? (
                          <ConfidenceBar value={pred.confidence} height={18} compact />
                        ) : hasPremium ? "—" : "🔒"}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </SectionBand>
      )}

      {/* ── LATEST NEWS ── */}
      {stories.length > 0 && (
        <SectionBand
          id="home-news"
          title="Latest News"
          action={<Link href="/news">All headlines →</Link>}
        >
          <div className="home-news-split">
            {/* Featured story — left half */}
            {stories[0] && (
              <StoryCard
                key={`${stories[0].sport}_${stories[0].id}_featured`}
                href={stories[0].link ?? "/news"}
                title={
                  (stories[0].published &&
                    Date.now() - new Date(stories[0].published).getTime() < 6 * 60 * 60 * 1000
                    ? "🚨 BREAKING: "
                    : "") + stories[0].headline
                }
                excerpt={stories[0].description}
                imageUrl={stories[0].image_url}
                publishedAt={stories[0].published ? formatRelativeTime(stories[0].published) : undefined}
                meta={<SportBadge sport={stories[0].sport} />}
                size="featured"
              />
            )}
            {/* Grid — right half */}
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-4)" }}>
              {stories.slice(1, 4).map((story, si) => (
                <StoryCard
                  key={`${story.sport}_${story.id}_${si}`}
                  href={story.link ?? "/news"}
                  title={
                    (story.published &&
                      Date.now() - new Date(story.published).getTime() < 6 * 60 * 60 * 1000
                      ? "🚨 BREAKING: "
                      : "") + story.headline
                  }
                  excerpt={story.description}
                  imageUrl={story.image_url}
                  publishedAt={story.published ? formatRelativeTime(story.published) : undefined}
                  meta={<SportBadge sport={story.sport} />}
                />
              ))}
            </div>
          </div>
        </SectionBand>
      )}

      {!hasPremium && (
        <PremiumTeaser
          message="Upgrade for full pick cards, advanced model fields, and premium tools."
          ctaHref="/pricing"
        />
      )}
    </main>
  );
}
