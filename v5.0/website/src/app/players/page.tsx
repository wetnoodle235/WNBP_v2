export const dynamic = "force-dynamic";

import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { getAggregatePlayers, getAggregateTeams } from "@/lib/api";
import type { Team } from "@/lib/schemas";
import { AGGREGATE_SPORT_KEYS } from "@/lib/sports";
import { PlayersClient } from "./PlayersClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Players",
  description: "Browse player profiles and stats across all sports.",
  path: "/players",
});

export interface Player {
  source: string;
  id: string;
  sport: string;
  name: string;
  team_id: string | null;
  position: string | null;
  jersey_number: number | null;
  height: string | null;
  weight: number | null;
  birth_date: string | null;
  nationality: string | null;
  experience_years: number | null;
  status: string | null;
  headshot_url: string | null;
  team_name?: string | null;
  team_abbreviation?: string | null;
}

export interface TeamInfo {
  id: string;
  sport: string;
  name: string;
  abbreviation: string;
  city: string;
  logo_url: string | null;
  color_primary: string | null;
}

export default async function PlayersPage() {
  const sports = [...AGGREGATE_SPORT_KEYS];
  const [allPlayersRaw, allTeamsRaw] = await Promise.all([
    getAggregatePlayers(sports, 5000),
    getAggregateTeams(sports, 1000),
  ]);

  const playersBySport: Record<string, Player[]> = {};
  const teamLookup: Record<string, TeamInfo> = {};

  for (const sport of sports) {
    playersBySport[sport] = (allPlayersRaw as Player[]).filter((p) => (p.sport ?? sport) === sport);
  }

  for (const t of allTeamsRaw as Team[]) {
    const sport = t.sport ?? "unknown";
    if (!sport) continue;
    teamLookup[`${sport}-${t.id}`] = {
      id: t.id,
      sport,
      name: t.name,
      abbreviation: t.abbreviation ?? "",
      city: t.city ?? "",
      logo_url: t.logo_url ?? null,
      color_primary: t.color_primary ?? null,
    };
  }

  for (const sport of sports) {
    if (!playersBySport[sport]) {
      playersBySport[sport] = [];
    }
  }

  return (
    <main>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{
          __html: buildCollectionJsonLd({
            name: "Players",
            path: "/players",
            description: "Browse player profiles and stats across all sports.",
          }),
        }}
      />
      <PlayersClient
        playersBySport={playersBySport}
        teamLookup={teamLookup}
        sports={sports}
      />
    </main>
  );
}
