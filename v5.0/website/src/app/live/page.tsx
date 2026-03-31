import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { getGames } from "@/lib/api";
import { LiveClient } from "./LiveClient";

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

  const results = await Promise.allSettled(
    LIVE_SPORTS.map((sport) => getGames(sport, { date: today })),
  );

  const allGames = results.flatMap((r, i) =>
    r.status === "fulfilled"
      ? r.value.map((game) => ({
          ...game,
          sport: game.sport ?? LIVE_SPORTS[i],
        }))
      : [],
  );

  return <LiveClient games={allGames} sports={[...LIVE_SPORTS]} />;
}
