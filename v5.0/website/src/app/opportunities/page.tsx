import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { getViewerTier, hasPremiumTier } from "@/lib/server-access";
import { OpportunitiesClient } from "./OpportunitiesClient";

export const dynamic = "auto";
export const revalidate = 60;

export const metadata: Metadata = buildPageMetadata({
  title: "Prop Opportunities",
  description: "Ranked player-prop market opportunities across all supported sports, filtered and sorted by model confidence.",
  path: "/opportunities",
  keywords: ["prop bets", "player props", "betting opportunities", "value bets", "model confidence"],
});

export default async function OpportunitiesPage() {
  const tier = await getViewerTier();
  const hasPremium = hasPremiumTier(tier);
  const jsonLd = buildCollectionJsonLd({ name: "Prop Opportunities", path: "/opportunities", description: "Ranked player-prop market opportunities across all supported sports." });

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />
      <OpportunitiesClient hasPremium={hasPremium} />
    </>
  );
}
