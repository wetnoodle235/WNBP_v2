import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import { buildPageMetadata } from "@/lib/seo";
import { getGames, getPredictions, getNews } from "@/lib/api";
import { isSportKey, SPORTS, type SportKey } from "@/lib/sports";
import { getSportIcon, getSportFullName } from "@/lib/sports-config";
import { VenueVisual } from "@/components/venue";

export const dynamic = "force-dynamic";

interface PageProps {
  params: Promise<{ sport: string }>;
}

type HubTheme = {
  tone: "arena" | "pitch" | "diamond" | "rink" | "track" | "court" | "octagon" | "server";
  headline: string;
  subline: string;
};

const SPORT_THEME: Partial<Record<SportKey, HubTheme>> = {
  nba: { tone: "arena", headline: "Rim Pressure", subline: "Track pace, edges, and late steam." },
  wnba: { tone: "arena", headline: "Playmaker Map", subline: "Ride form streaks and matchup leverage." },
  ncaab: { tone: "arena", headline: "March Engine", subline: "Spot volatility before the market catches up." },
  ncaaw: { tone: "arena", headline: "Bracket Pulse", subline: "Read momentum shifts game by game." },
  nfl: { tone: "pitch", headline: "Drive Chart", subline: "Follow line moves, game scripts, and red-zone risk." },
  ncaaf: { tone: "pitch", headline: "Saturday Signal", subline: "Measure chaos, tempo, and upset pressure." },
  mlb: { tone: "diamond", headline: "Diamond Ledger", subline: "Pitching context, totals shape, and bullpen drag." },
  nhl: { tone: "rink", headline: "Rink Tempo", subline: "Monitor goalie edges and travel fatigue spots." },
  epl: { tone: "pitch", headline: "Fixture Lab", subline: "Find spots around congestion and xG drift." },
  laliga: { tone: "pitch", headline: "Control Grid", subline: "Exploit tactical mismatches and home splits." },
  bundesliga: { tone: "pitch", headline: "Pressing Index", subline: "High-event slates with totals opportunity." },
  seriea: { tone: "pitch", headline: "Tactical Board", subline: "Defensive structure meets market inefficiency." },
  ligue1: { tone: "pitch", headline: "Form Radar", subline: "Catch attack spikes and variance pockets." },
  mls: { tone: "pitch", headline: "Travel Meter", subline: "Altitude, turnaround, and roster rotation spots." },
  ucl: { tone: "pitch", headline: "Knockout Model", subline: "Second-leg leverage and matchup volatility." },
  nwsl: { tone: "pitch", headline: "Transition Watch", subline: "Identify pace shifts and tactical edges." },
  f1: { tone: "track", headline: "Race Wall", subline: "Grid context, weather angle, and long-run form." },
  indycar: { tone: "track", headline: "Oval Edge", subline: "Oval vs. road course pace and pit strategy." },
  atp: { tone: "court", headline: "Baseline Read", subline: "Surface, serve profile, and fatigue windows." },
  wta: { tone: "court", headline: "Breakpoint Lens", subline: "Form volatility and return pressure spots." },
  ufc: { tone: "octagon", headline: "Fight Matrix", subline: "Style clashes, pace, and finish probability." },
  lol: { tone: "server", headline: "Draft Board", subline: "Meta edges, map control, and objective tempo." },
  csgo: { tone: "server", headline: "Map Pool", subline: "Veto leverage and side-specific strength." },
  dota2: { tone: "server", headline: "Lane Priority", subline: "Tempo spikes and objective conversion." },
  valorant: { tone: "server", headline: "Utility Flow", subline: "Map tendencies and comp exploitation." },
  golf: { tone: "track", headline: "Course Signal", subline: "Field strength, weather, and profile fit." },
  lpga: { tone: "track", headline: "Tour Form", subline: "Course history, strokes gained, and field depth." },
};

function fallbackTheme(sport: SportKey): HubTheme {
  const byCategory: Record<string, HubTheme> = {
    basketball: { tone: "arena", headline: "Arena Board", subline: "See matchup pressure in one place." },
    football: { tone: "pitch", headline: "Field Board", subline: "Read game scripts and market movement." },
    baseball: { tone: "diamond", headline: "Diamond Board", subline: "Context first: starters, bullpen, and totals." },
    hockey: { tone: "rink", headline: "Rink Board", subline: "Spot pace and goalie-driven edges." },
    soccer: { tone: "pitch", headline: "Fixture Board", subline: "Track schedule load and xG shape." },
    motorsport: { tone: "track", headline: "Track Board", subline: "Model race-state and variance." },
    tennis: { tone: "court", headline: "Court Board", subline: "Surface and fatigue context, streamlined." },
    mma: { tone: "octagon", headline: "Octagon Board", subline: "Style matchup leverage at a glance." },
    esports: { tone: "server", headline: "Server Board", subline: "Meta and map context in one lane." },
    golf: { tone: "track", headline: "Course Board", subline: "Course fit and field pressure snapshots." },
  };
  return byCategory[SPORTS[sport].category] ?? byCategory.soccer;
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { sport } = await params;
  if (!isSportKey(sport)) return { title: "Not Found" };
  return buildPageMetadata({
    title: `${SPORTS[sport].label} Hub`,
    description: `${SPORTS[sport].label} sport hub with game center, model picks, and headlines.`,
    path: `/sports/${sport}`,
  });
}

function formatGameTime(iso: string | null | undefined): string {
  if (!iso) return "TBD";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "TBD";
  return d.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export default async function SportHubDetailPage({ params }: PageProps) {
  const { sport } = await params;
  if (!isSportKey(sport)) notFound();

  const [games, predictions, headlines] = await Promise.all([
    getGames(sport, { date: new Date().toISOString().slice(0, 10) }),
    getPredictions(sport),
    getNews(sport, 4),
  ]);

  const theme = SPORT_THEME[sport] ?? fallbackTheme(sport);
  const topPredictions = predictions
    .filter((p) => typeof p.confidence === "number")
    .sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0))
    .slice(0, 5);

  return (
    <main className="sport-detail-shell" data-tone={theme.tone}>
      <section className="sport-detail-hero">
        <div className="sport-detail-badge">{getSportIcon(sport)} {SPORTS[sport].label}</div>
        <h1>{theme.headline}</h1>
        <p>{theme.subline}</p>
        <div className="sport-detail-hero-links">
          <Link href={`/games/${sport}`} className="sport-detail-link-primary">Open {SPORTS[sport].label} Games</Link>
          <Link href={`/${sport}`} className="sport-detail-link-secondary">Legacy Hub</Link>
          <Link href="/sports" className="sport-detail-link-secondary">All Sports</Link>
        </div>
      </section>

      <section className="sport-detail-kpis" aria-label="Sport hub metrics">
        <article className="sport-detail-kpi-card">
          <span>Games Today</span>
          <strong>{games.length}</strong>
        </article>
        <article className="sport-detail-kpi-card">
          <span>Model Picks</span>
          <strong>{predictions.length}</strong>
        </article>
        <article className="sport-detail-kpi-card">
          <span>Headlines</span>
          <strong>{headlines.length}</strong>
        </article>
      </section>

      <section className="sport-detail-grid">
        <article className="sport-detail-panel">
          <header>
            <h2>Game Center</h2>
            <span>{getSportFullName(sport)}</span>
          </header>
          {games.length === 0 ? (
            <p className="sport-detail-empty">No games listed for today.</p>
          ) : (
            <ul className="sport-detail-list">
              {games.slice(0, 8).map((g) => (
                <li key={g.id}>
                  <Link href={`/games/${sport}/${g.id}`}>
                    <span>{g.away_team} at {g.home_team}</span>
                    <span>{formatGameTime(g.start_time ?? g.date)}</span>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </article>

        <article className="sport-detail-panel">
          <header>
            <h2>Top Model Confidence</h2>
            <span>Sorted high to low</span>
          </header>
          {topPredictions.length === 0 ? (
            <p className="sport-detail-empty">No prediction confidence data available.</p>
          ) : (
            <ul className="sport-detail-list">
              {topPredictions.map((pick) => (
                <li key={pick.game_id}>
                  <Link href={`/games/${sport}/${pick.game_id}`}>
                    <span>{pick.game_id}</span>
                    <span>{Math.round((pick.confidence ?? 0) * 100)}%</span>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </article>

        <article className="sport-detail-panel">
          <header>
            <h2>Latest Headlines</h2>
            <span>Live feed</span>
          </header>
          {headlines.length === 0 ? (
            <p className="sport-detail-empty">No recent headlines yet.</p>
          ) : (
            <ul className="sport-detail-list sport-detail-news-list">
              {headlines.map((news, idx) => (
                <li key={news.id ?? idx}>
                  {news.link ? (
                    <a href={news.link} target="_blank" rel="noreferrer">{news.headline}</a>
                  ) : (
                    <span>{news.headline}</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </article>
      </section>

      {/* ── VENUE VISUAL ─────────────────────────────────────────── */}
      {(() => {
        const noVisual = ["mma", "ufc", "boxing", "lol", "csgo", "dota2", "valorant", "esports"];
        if (noVisual.includes(sport.toLowerCase())) return null;
        // Get upcoming race venue for F1/IndyCar
        const nextRaceVenue = (sport === "f1" || sport === "indycar")
          ? games.find((g) => g.status !== "final")?.venue ?? games[0]?.venue ?? ""
          : "";
        return (
          <section className="sport-detail-venue" aria-label="Playing surface">
            <header style={{ padding: "var(--space-4) var(--space-6) var(--space-2)", borderBottom: "1px solid var(--color-border)" }}>
              <h2 style={{ margin: 0, fontSize: "var(--text-lg)", fontWeight: 700 }}>
                {sport === "f1" || sport === "indycar"
                  ? "🏎️ Race Circuit"
                  : sport === "golf" || sport === "lpga" || sport === "pga"
                  ? "⛳ Course Preview"
                  : sport === "atp" || sport === "wta"
                  ? "🎾 Court Layout"
                  : "🏟️ Playing Surface"}
              </h2>
            </header>
            <div style={{ padding: "var(--space-5)", display: "flex", justifyContent: "center" }}>
              <VenueVisual
                sport={sport}
                venueName={nextRaceVenue}
                animate={sport === "f1" || sport === "indycar"}
              />
            </div>
          </section>
        );
      })()}
    </main>
  );
}
