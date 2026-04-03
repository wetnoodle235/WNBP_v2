import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { getAggregatePredictions } from "@/lib/api";
import type { Prediction } from "@/lib/schemas";
import { AGGREGATE_SPORT_KEYS } from "@/lib/sports";
import { getViewerTier, hasPremiumTier, limitPredictionPreview } from "@/lib/server-access";
import { PredictionsClient } from "./PredictionsClient";

export const dynamic = "auto";
export const revalidate = 45;

export const metadata: Metadata = buildPageMetadata({
  title: "Predictions",
  description: "Data-driven predictions across all supported sports.",
  path: "/predictions",
  keywords: ["sports predictions", "game predictions", "AI predictions", "win probability", "betting predictions"],
});

export default async function PredictionsPage() {
  const sports = [...AGGREGATE_SPORT_KEYS];
  const today = new Date().toISOString().slice(0, 10);
  const tier = await getViewerTier();
  const hasPremium = hasPremiumTier(tier);

  const allPredictionsRaw = await getAggregatePredictions(sports, {
    date: today,
    limitPerSport: 250,
  });
  const normalized = allPredictionsRaw.map((p: Prediction) => ({ ...p, sport: p.sport ?? "unknown" }));
  const allPredictions = hasPremium ? normalized : limitPredictionPreview(normalized);
  const jsonLd = buildCollectionJsonLd({ name: "Sports Predictions", path: "/predictions", description: "Data-driven predictions across all supported sports." });
  const initialWarning = null;

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />
      <PredictionsClient
        predictions={allPredictions}
        sports={sports}
        today={today}
        hasPremium={hasPremium}
        initialWarning={initialWarning}
      />
    </>
  );
}
