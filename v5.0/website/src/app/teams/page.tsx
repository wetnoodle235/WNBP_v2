export const dynamic = "force-dynamic";

import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { getAggregateTeams } from "@/lib/api";
import { AGGREGATE_SPORT_KEYS } from "@/lib/sports";
import { TeamsClient } from "./TeamsClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Teams",
  description: "Browse teams across all supported sports.",
  path: "/teams",
});

export default async function TeamsPage() {
  const sports = [...AGGREGATE_SPORT_KEYS];
  const allTeams = await getAggregateTeams(sports, 1000);

  // Sort alphabetically by name
  allTeams.sort((a, b) => a.name.localeCompare(b.name));

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{
          __html: buildCollectionJsonLd({
            name: "Teams",
            path: "/teams",
            description: "Browse teams across all supported sports.",
          }),
        }}
      />
      <TeamsClient teams={allTeams} sports={sports} />
    </>
  );
}
