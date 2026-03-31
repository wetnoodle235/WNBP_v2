import Link from "next/link";
import type { Metadata } from "next";
import { buildPageMetadata } from "@/lib/seo";
import { ALL_SPORT_KEYS, SPORTS, type SportKey } from "@/lib/sports";
import { getSportIcon, getSportFullName } from "@/lib/sports-config";
import { getGames, getPredictions } from "@/lib/api";

export const dynamic = "force-dynamic";

export const metadata: Metadata = buildPageMetadata({
  title: "Sports Hub",
  description: "Unified sports hub with featured league snapshots, game center links, and category navigation.",
  path: "/sports",
});

const FEATURED_SPORTS: readonly SportKey[] = ["nba", "nfl", "mlb", "epl"] as const;

const CATEGORY_ORDER = [
  "basketball",
  "football",
  "baseball",
  "hockey",
  "soccer",
  "motorsport",
  "tennis",
  "mma",
  "esports",
  "golf",
] as const;

const CATEGORY_LABELS: Record<(typeof CATEGORY_ORDER)[number], string> = {
  basketball: "Basketball",
  football: "Football",
  baseball: "Baseball",
  hockey: "Hockey",
  soccer: "Soccer",
  motorsport: "Motorsport",
  tennis: "Tennis",
  mma: "Combat",
  esports: "Esports",
  golf: "Golf",
};

function sportsByCategory(): Array<{ category: string; sports: SportKey[] }> {
  const grouped = new Map<string, SportKey[]>();
  for (const key of ALL_SPORT_KEYS) {
    const category = SPORTS[key].category;
    const list = grouped.get(category) ?? [];
    list.push(key);
    grouped.set(category, list);
  }

  return CATEGORY_ORDER
    .filter((c) => grouped.has(c))
    .map((category) => ({
      category,
      sports: grouped.get(category) ?? [],
    }));
}

export default async function SportsHubPage() {
  const sections = sportsByCategory();

  const featuredStats = await Promise.all(
    FEATURED_SPORTS.map(async (sport) => {
      const [games, predictions] = await Promise.all([
        getGames(sport, { date: new Date().toISOString().slice(0, 10) }),
        getPredictions(sport),
      ]);
      return {
        sport,
        gamesToday: games.length,
        picks: predictions.length,
        topConfidence: predictions.reduce((best, p) => Math.max(best, p.confidence ?? 0), 0),
      };
    }),
  );

  return (
    <main className="sports-hub-shell">
      <section className="sports-hub-hero">
        <p className="sports-hub-kicker">Navigation</p>
        <h1>Sports Hub</h1>
        <p>
          The clean launchpad for every league. Start here, choose a sport,
          then move into the dedicated hub and game center flows.
        </p>
        <div className="sports-hub-hero-actions">
          <Link href="/predictions" className="sports-hub-hero-btn sports-hub-hero-btn-primary">Open Predictions</Link>
          <Link href="/live" className="sports-hub-hero-btn">Live Scoreboard</Link>
        </div>
      </section>

      <section className="sports-hub-featured" aria-label="Featured leagues">
        {featuredStats.map((item) => (
          <article key={item.sport} className="sports-hub-feature-card">
            <header>
              <h2>{getSportIcon(item.sport)} {SPORTS[item.sport].label}</h2>
              <span>{getSportFullName(item.sport)}</span>
            </header>
            <div className="sports-hub-feature-kpis">
              <div>
                <small>Games Today</small>
                <strong>{item.gamesToday}</strong>
              </div>
              <div>
                <small>Model Picks</small>
                <strong>{item.picks}</strong>
              </div>
              <div>
                <small>Top Confidence</small>
                <strong>{Math.round(item.topConfidence * 100)}%</strong>
              </div>
            </div>
            <div className="sports-hub-feature-actions">
              <Link href={`/sports/${item.sport}`}>Open Hub</Link>
              <Link href={`/games/${item.sport}`}>Games</Link>
            </div>
          </article>
        ))}
      </section>

      <section className="sports-hub-grid" aria-label="Sports by category">
        {sections.map(({ category, sports }) => (
          <article key={category} className="sports-hub-category-card">
            <header className="sports-hub-category-head">
              <h2>{CATEGORY_LABELS[category as (typeof CATEGORY_ORDER)[number]] ?? category}</h2>
              <span>{sports.length} leagues</span>
            </header>

            <div className="sports-hub-links">
              {sports.map((sport) => (
                <Link key={sport} href={`/sports/${sport}`} className="sports-hub-link">
                  <span className="sports-hub-link-left">
                    <span className="sports-hub-link-icon" aria-hidden="true">{getSportIcon(sport)}</span>
                    <span>{SPORTS[sport].label}</span>
                  </span>
                  <span className="sports-hub-link-pill">View</span>
                </Link>
              ))}
            </div>
          </article>
        ))}
      </section>
    </main>
  );
}
