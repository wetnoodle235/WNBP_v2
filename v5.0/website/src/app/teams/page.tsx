export const dynamic = "force-dynamic";

import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { getTeams } from "@/lib/api";
import { ALL_SPORT_KEYS } from "@/lib/sports";
import { TeamsClient } from "./TeamsClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Teams",
  description: "Browse teams across all supported sports.",
  path: "/teams",
});

export default async function TeamsPage() {
  const sports = [...ALL_SPORT_KEYS];
  const results = await Promise.allSettled(
    sports.map((sport) => getTeams(sport)),
  );

  const allTeams = results.flatMap((r, i) =>
    r.status === "fulfilled"
      ? r.value.map((team) => ({
          ...team,
          sport: team.sport ?? sports[i],
        }))
      : [],
  );

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
