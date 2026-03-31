import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { SectionBand } from "@/components/ui";
import { getStandingsWithMeta, getTeams } from "@/lib/api";
import type { Standing } from "@/lib/schemas";
import { StandingsClient } from "./StandingsClient";

export const dynamic = "auto";
export const revalidate = 180;

export const metadata: Metadata = buildPageMetadata({
  title: "Standings",
  description: "Current league standings for all supported sports.",
  path: "/standings",
});

const STANDINGS_SPORTS = [
  "nba", "mlb", "nfl", "nhl", "wnba",
  "epl", "bundesliga", "laliga", "ligue1", "seriea", "mls", "ucl",
  "ncaab", "ncaaf", "ncaaw", "nwsl",
] as const;

export type TeamLookup = Record<string, { name: string; abbrev?: string }>;

export default async function StandingsPage() {
  const results = await Promise.all(
    STANDINGS_SPORTS.map(async (sport) => {
      const [standingsResult, teams] = await Promise.all([
        getStandingsWithMeta(sport),
        getTeams(sport),
      ]);
      const teamLookup: TeamLookup = {};
      for (const t of teams) {
        teamLookup[t.id] = {
          name: t.name,
          abbrev: t.abbreviation ?? undefined,
        };
      }
      return {
        sport,
        standings: standingsResult.data,
        teamLookup,
        seasonActive: standingsResult.seasonActive,
        seasonYear: standingsResult.seasonYear,
      };
    }),
  );

  const standingsBySport: Record<string, Standing[]> = {};
  const teamsBySport: Record<string, TeamLookup> = {};
  const seasonActive: Record<string, boolean> = {};
  const seasonYears: Record<string, string | null> = {};

  for (const { sport, standings, teamLookup, seasonActive: active, seasonYear } of results) {
    standingsBySport[sport] = standings;
    teamsBySport[sport] = teamLookup;
    seasonActive[sport] = active;
    seasonYears[sport] = seasonYear;
  }

  return (
    <main>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{
          __html: buildCollectionJsonLd({
            name: "Standings",
            path: "/standings",
            description: "Current league standings for all supported sports.",
          }),
        }}
      />
      <SectionBand title="Standings">
        <StandingsClient
          sports={STANDINGS_SPORTS as unknown as string[]}
          standingsBySport={standingsBySport}
          teamsBySport={teamsBySport}
          seasonActive={seasonActive}
          seasonYears={seasonYears}
        />
      </SectionBand>
    </main>
  );
}
