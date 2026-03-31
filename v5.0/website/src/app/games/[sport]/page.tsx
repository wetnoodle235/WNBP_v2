import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { SectionBand } from "@/components/ui";
import { SportTabs } from "@/components/SportSelectors";
import { getDisplayName } from "@/lib/sports-config";
import GamesClient from "./GamesClient";

interface PageProps {
  params: Promise<{ sport: string }>;
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { sport } = await params;
  const name = getDisplayName(sport);
  return buildPageMetadata({
    title: `${name} Games`,
    description: `Browse upcoming and recent ${name} games, scores, and predictions.`,
    path: `/games/${sport}`,
    keywords: [`${name} games`, `${name} scores`, `${name} schedule`, "game predictions"],
  });
}

export default async function GamesListPage({ params }: PageProps) {
  const { sport } = await params;

  return (
    <main>
      <SportTabs currentSport={sport} baseUrl="/games" variant="pills" />
      <SectionBand title={`${getDisplayName(sport)} Games`}>
        <GamesClient sport={sport} />
      </SectionBand>
    </main>
  );
}
