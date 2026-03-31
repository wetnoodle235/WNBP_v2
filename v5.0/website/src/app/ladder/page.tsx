import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import LadderClient from "./LadderClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Ladder",
  description: "Community leaderboard and rankings.",
  path: "/ladder",
  keywords: ["leaderboard", "rankings", "community", "sports betting rankings", "top predictors"],
});

export default function LadderPage() {
  return <LadderClient />;
}
