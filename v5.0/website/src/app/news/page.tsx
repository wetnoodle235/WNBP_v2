import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { getAggregateNews } from "@/lib/api";
import { NewsClient } from "./NewsClient";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const NEWS_SPORTS = ["nba", "mlb", "nfl", "nhl", "epl", "ncaab", "ncaaf", "ncaaw", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl", "wnba", "ufc", "atp", "wta", "csgo", "lol", "dota2", "valorant", "f1"] as const;

export const metadata: Metadata = buildPageMetadata({
  title: "News",
  description: "Latest sports news and headlines across all leagues.",
  path: "/news",
  keywords: ["sports news", "headlines", "live updates", "NBA news", "NFL news", "MLB news"],
});

export default async function NewsPage() {
  const allNews = await getAggregateNews([...NEWS_SPORTS], 15);

  // Deduplicate by headline
  const seen = new Set<string>();
  const dedupedNews = allNews.filter((item) => {
    const key = item.headline.toLowerCase().trim();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Sort newest first
  dedupedNews.sort((a, b) => {
    if (!a.published) return 1;
    if (!b.published) return -1;
    return new Date(b.published).getTime() - new Date(a.published).getTime();
  });

  const jsonLd = buildCollectionJsonLd({ name: "Sports News", path: "/news", description: "Latest sports news and headlines across all leagues." });

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />
      <NewsClient news={dedupedNews} sports={[...NEWS_SPORTS]} />
    </>
  );
}
