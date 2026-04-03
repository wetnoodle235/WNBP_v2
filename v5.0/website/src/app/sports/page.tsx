import Link from "next/link";
import type { Metadata } from "next";
import { buildPageMetadata } from "@/lib/seo";
import { ALL_SPORT_KEYS, SPORTS, type SportKey } from "@/lib/sports";
import { getSportIcon, getSportFullName } from "@/lib/sports-config";
import { getAggregateGames, getAggregatePredictions, getSports } from "@/lib/api";
import { resolveMediaUrl } from "@/lib/media";

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
  const sportCatalog = (await getSports()) ?? [];
  const catalogEntries: Array<[string, string | null]> = sportCatalog.reduce<Array<[string, string | null]>>(
    (acc, entry) => {
      const key = String(entry.key ?? "");
      if (!key) return acc;
      acc.push([key, resolveMediaUrl((entry.image_url ?? null) as string | null)]);
      return acc;
    },
    [],
  );
  const catalogMap = new Map<string, string | null>(catalogEntries);

  const today = new Date().toISOString().slice(0, 10);
  const featuredSportsList = [...FEATURED_SPORTS];
  const [featuredGames, featuredPredictions] = await Promise.all([
    getAggregateGames(featuredSportsList, { date: today, limitPerSport: 500 }),
    getAggregatePredictions(featuredSportsList, { date: today, limitPerSport: 500 }),
  ]);

  const featuredStats = FEATURED_SPORTS.map((sport) => {
    const games = featuredGames.filter((g) => (g.sport ?? sport) === sport);
    const predictions = featuredPredictions.filter((p) => (p.sport ?? sport) === sport);
    return {
      sport,
      gamesToday: games.length,
      picks: predictions.length,
      topConfidence: predictions.reduce((best, p) => Math.max(best, p.confidence ?? 0), 0),
    };
  });

  const totalFeaturedGames = featuredStats.reduce((sum, item) => sum + item.gamesToday, 0);
  const totalFeaturedPicks = featuredStats.reduce((sum, item) => sum + item.picks, 0);

  const QUICK_WORKFLOWS = [
    { href: "/predictions", label: "Predictions", desc: "Model board by confidence and market context" },
    { href: "/live", label: "Live Scores", desc: "Track in-progress games and live state" },
    { href: "/odds", label: "Odds", desc: "Compare available books and prices" },
    { href: "/stats", label: "Stats", desc: "Player and team stats explorer" },
    { href: "/market-intel", label: "Market Intel", desc: "Volatile vs stable market regimes" },
    { href: "/fatigue", label: "Fatigue Board", desc: "Schedule stress and rest disadvantage" },
    { href: "/injuries", label: "Injury Report", desc: "Cross-sport player status monitor" },
  ];

  return (
    <main className="sports-hub-shell">
      <section className="sports-hub-hero">
        <p className="sports-hub-kicker">League Directory</p>
        <h1>Sports Hub</h1>
        <p>
          Your league launchboard. Jump into a sport, open workflow tools,
          and move from discovery to picks in one pass.
        </p>
        <div className="sports-hub-hero-actions">
          <Link href="/predictions" className="sports-hub-hero-btn sports-hub-hero-btn-primary">Open Predictions</Link>
          <Link href="/live" className="sports-hub-hero-btn">Live Scoreboard</Link>
          <Link href="/market-intel" className="sports-hub-hero-btn">Market Intel</Link>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "var(--space-2)", marginTop: "var(--space-3)" }}>
          <div style={{ border: "1px solid var(--color-border)", borderRadius: "var(--radius)", padding: "0.5rem 0.65rem", background: "var(--color-bg-2)" }}>
            <small style={{ color: "var(--color-text-muted)" }}>Tracked Leagues</small>
            <div style={{ fontWeight: 800, fontSize: "var(--text-md)" }}>{ALL_SPORT_KEYS.length}</div>
          </div>
          <div style={{ border: "1px solid var(--color-border)", borderRadius: "var(--radius)", padding: "0.5rem 0.65rem", background: "var(--color-bg-2)" }}>
            <small style={{ color: "var(--color-text-muted)" }}>Featured Games</small>
            <div style={{ fontWeight: 800, fontSize: "var(--text-md)" }}>{totalFeaturedGames}</div>
          </div>
          <div style={{ border: "1px solid var(--color-border)", borderRadius: "var(--radius)", padding: "0.5rem 0.65rem", background: "var(--color-bg-2)" }}>
            <small style={{ color: "var(--color-text-muted)" }}>Featured Picks</small>
            <div style={{ fontWeight: 800, fontSize: "var(--text-md)" }}>{totalFeaturedPicks}</div>
          </div>
        </div>
      </section>

      <section className="sports-hub-grid" aria-label="Quick workflows">
        {QUICK_WORKFLOWS.map((item) => (
          <article key={item.href} className="sports-hub-category-card">
            <header className="sports-hub-category-head">
              <h2>{item.label}</h2>
              <span>Tool</span>
            </header>
            <p style={{ color: "var(--color-text-secondary)", fontSize: "var(--text-sm)", lineHeight: 1.5 }}>
              {item.desc}
            </p>
            <div>
              <Link href={item.href} className="sports-hub-link">
                <span className="sports-hub-link-left">Open</span>
                <span className="sports-hub-link-pill">Go</span>
              </Link>
            </div>
          </article>
        ))}
      </section>

      <section className="sports-hub-featured" aria-label="Featured leagues">
        {featuredStats.map((item) => (
          <article key={item.sport} className="sports-hub-feature-card">
            <header>
              <h2 style={{ display: "flex", alignItems: "center", gap: "0.65rem" }}>
                {catalogMap.get(item.sport) ? (
                  <img
                    src={catalogMap.get(item.sport) ?? undefined}
                    alt={`${SPORTS[item.sport].label} logo`}
                    style={{ width: 30, height: 30, objectFit: "contain", borderRadius: 9999, background: "var(--color-bg-2)" }}
                  />
                ) : (
                  <span>{getSportIcon(item.sport)}</span>
                )}
                <span>{SPORTS[item.sport].label}</span>
              </h2>
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
                <strong>{item.picks > 0 ? `${Math.round(item.topConfidence * 100)}%` : "—"}</strong>
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
                    <span className="sports-hub-link-icon" aria-hidden="true">
                      {catalogMap.get(sport) ? (
                        <img
                          src={catalogMap.get(sport) ?? undefined}
                          alt=""
                            style={{ width: 18, height: 18, objectFit: "contain", borderRadius: 9999, background: "var(--color-bg-2)" }}
                        />
                      ) : getSportIcon(sport)}
                    </span>
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
