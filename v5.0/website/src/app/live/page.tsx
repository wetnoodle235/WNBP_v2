import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { getAggregateGames } from "@/lib/api";
import dynamicComponent from "next/dynamic";

// Dynamically import LiveClient to enable code splitting
// This prevents the 47KB component from loading on other pages
const LiveClient = dynamicComponent(() => import("./LiveClient").then(m => ({ default: m.LiveClient })), {
  loading: () => <div style={{ padding: "2rem", textAlign: "center" }}>Loading live scores...</div>,
  ssr: true,
});

export const dynamic = "force-dynamic";
export const revalidate = 0;

const LIVE_SPORTS = ["nba", "mlb", "nfl", "nhl", "wnba", "epl", "ncaab", "ncaaf", "ncaaw", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl", "ufc", "csgo", "lol", "dota2", "valorant", "f1"] as const;

export const metadata: Metadata = buildPageMetadata({
  title: "Live Scores",
  description: "Real-time scores and updates for games in progress.",
  path: "/live",
  keywords: ["live scores", "real-time updates", "game scores", "sports scores", "in-progress games"],
});

export default async function LiveScoresPage() {
  const today = new Date().toISOString().slice(0, 10);
  const allGames = await getAggregateGames([...LIVE_SPORTS], {
    date: today,
    excludeFinal: false,
    limitPerSport: 300,
  });

  return <LiveClient games={allGames} sports={[...LIVE_SPORTS]} />;
}
