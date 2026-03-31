import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { OddsClient } from "./OddsClient";
import { getOdds } from "@/lib/api";

export const dynamic = "auto";
export const revalidate = 30;

const ODDS_SPORTS = ["nba", "mlb", "nfl", "nhl", "wnba", "epl", "ncaab", "ncaaf", "ncaaw", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl", "ufc", "atp", "wta", "csgo", "lol", "dota2", "valorant", "f1"] as const;

export const metadata: Metadata = buildPageMetadata({
  title: "Odds",
  description: "Compare live odds across sportsbooks for today's games.",
  path: "/odds",
  keywords: ["sports odds", "betting odds", "sportsbook comparison", "live odds", "moneyline", "spread"],
});

export default async function OddsPage() {
  const today = new Date().toISOString().slice(0, 10);
  const results = await Promise.allSettled(
    ODDS_SPORTS.map(async (sport) => {
      const data = await getOdds(sport, { date: today });
      return data.map((o) => ({ ...o, sport: (o as Record<string, unknown>).sport ?? sport }));
    }),
  );
  const initialOdds = results.flatMap((r) =>
    r.status === "fulfilled" ? r.value : [],
  );

  const jsonLd = buildCollectionJsonLd({ name: "Sports Odds", path: "/odds", description: "Compare live odds across sportsbooks for today's games." });

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />
      <OddsClient sports={[...ODDS_SPORTS]} initialOdds={initialOdds as never[]} />
    </>
  );
}
