import type { Metadata } from "next";
import { buildPageMetadata } from "@/lib/seo";
import { LeaderboardClient } from "./LeaderboardClient";

export const dynamic = "auto";
export const revalidate = 60;

export const metadata: Metadata = buildPageMetadata({
  title: "Model Leaderboard",
  description: "Sports prediction accuracy rankings powered by machine learning. See which sports our models predict best.",
  path: "/leaderboard",
});

export default function LeaderboardPage() {
  return <LeaderboardClient />;
}
