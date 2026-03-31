export const dynamic = "force-dynamic";

import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { getPlayers, getTeams } from "@/lib/api";
import type { Team } from "@/lib/schemas";
import { ALL_SPORT_KEYS } from "@/lib/sports";
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
  const sports = [...ALL_SPORT_KEYS];
  const results = await Promise.allSettled(
    sports.map(async (sport) => {
      const [players, teams] = await Promise.all([
        getPlayers(sport, { limit: "1000", offset: "0" }),
        getTeams(sport),
      ]);
      let allPlayers = players as Player[];
      // Fetch additional pages for sports with many players (college, esports)
      let offset = 1000;
      while (allPlayers.length >= offset && offset < 5000) {
        const page = await getPlayers(sport, { limit: "1000", offset: String(offset) });
        const arr = page as Player[];
        if (arr.length === 0) break;
        allPlayers = [...allPlayers, ...arr];
        offset += 1000;
      }
      return { sport, players: allPlayers, teams: teams as Team[] };
    }),
  );

  const playersBySport: Record<string, Player[]> = {};
  const teamLookup: Record<string, TeamInfo> = {};

  for (let i = 0; i < results.length; i++) {
    const sport = sports[i];
    const result = results[i];
    if (result.status === "fulfilled") {
      playersBySport[sport] = result.value.players;
      for (const t of result.value.teams) {
        teamLookup[`${sport}-${t.id}`] = {
          id: t.id,
          sport: t.sport ?? sport,
          name: t.name,
          abbreviation: t.abbreviation ?? "",
          city: t.city ?? "",
          logo_url: t.logo_url ?? null,
          color_primary: t.color_primary ?? null,
        };
      }
    } else {
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
