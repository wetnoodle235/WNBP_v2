import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { AGGREGATE_SPORT_KEYS } from "@/lib/sports";
import { StatsClient } from "./StatsClient";
import { getAggregateStats, getAggregateTeams, getSports } from "@/lib/api";

export const dynamic = "force-dynamic";

export const metadata: Metadata = buildPageMetadata({
  title: "Stats",
  description: "Player and team statistics across NBA, NFL, NHL, MLB, WNBA, soccer leagues, ATP, WTA, UFC, F1, and esports.",
  path: "/stats",
});

export default async function StatsPage() {
  const advertisedSports = await getSports();
  const backendSports = new Set(
    (advertisedSports ?? [])
      .map((entry) => String((entry as Record<string, unknown>).key ?? "").toLowerCase())
      .filter(Boolean),
  );

  // Keep frontend defaults as fallback, but prefer backend-advertised sports to
  // avoid aggregate HTTP 422 when catalogs drift between deploys.
  const sports = (backendSports.size > 0
    ? AGGREGATE_SPORT_KEYS.filter((sport) => backendSports.has(sport))
    : [...AGGREGATE_SPORT_KEYS]
  );
  const [statsPayload, teamsPayload] = await Promise.all([
    getAggregateStats(sports, { playerLimitPerSport: 200, teamLimitPerSport: 200 }),
    getAggregateTeams(sports, 500),
  ]);

  const playerStatsBySport: Record<string, unknown[]> = {};
  const teamStatsBySport: Record<string, unknown[]> = {};
  const teamMapBySport: Record<string, Record<string, string>> = {};
  const degradedSports = new Set<string>();

  for (const sport of sports) {
    const statsForSport = statsPayload?.[sport];
    playerStatsBySport[sport] = statsForSport?.player_stats ?? [];
    teamStatsBySport[sport] = statsForSport?.team_stats ?? [];
    if (playerStatsBySport[sport].length === 0 && teamStatsBySport[sport].length === 0) {
      degradedSports.add(sport);
    }
    const tmap: Record<string, string> = {};
    for (const t of teamsPayload ?? []) {
      if (((t as any).sport ?? sport) !== sport) continue;
      const tid = String((t as any).team_id ?? (t as any).id ?? "");
      const name = (t as any).abbreviation ?? (t as any).short_name ?? (t as any).name ?? "";
      if (tid && name) tmap[tid] = name;
    }
    teamMapBySport[sport] = tmap;
  }

  const initialWarning = degradedSports.size > 0
    ? `Some stats feeds could not be loaded (${[...degradedSports].join(", ")}). Tables may be incomplete.`
    : null;

  return (
    <StatsClient
      playerStatsBySport={playerStatsBySport}
      teamStatsBySport={teamStatsBySport}
      teamMapBySport={teamMapBySport}
      sports={sports}
      initialWarning={initialWarning}
    />
  );
}
