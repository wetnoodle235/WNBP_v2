import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { ALL_SPORT_KEYS } from "@/lib/sports";
import { StatsClient } from "./StatsClient";

export const dynamic = "force-dynamic";

export const metadata: Metadata = buildPageMetadata({
  title: "Stats",
  description: "Player and team statistics across NBA, NFL, NHL, MLB, WNBA, soccer leagues, ATP, WTA, UFC, F1, and esports.",
  path: "/stats",
});

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

/** Return the current season year for a sport based on current date.
 *  Cross-year leagues use end-year labelling (e.g. NBA 2025-26 → "2026"). */
function getCurrentSeason(sport: string): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;

  // Cross-year sports — threshold month: if month >= threshold, season = year+1
  const crossYear: Record<string, number> = {
    nba: 10, nhl: 10,
    nfl: 9,
    ncaab: 8, ncaaf: 8, ncaaw: 8,
    epl: 8, laliga: 8, bundesliga: 8, seriea: 8, ligue1: 8, ucl: 8,
  };
  if (sport in crossYear) {
    return month >= crossYear[sport] ? String(year + 1) : String(year);
  }
  // Calendar-year sports: MLB, MLS, NWSL, WNBA, esports, tennis, F1, UFC
  return String(year);
}

async function fetchTeams(sport: string): Promise<unknown[]> {
  try {
    const res = await fetch(`${API_BASE}/v1/${sport}/teams?limit=100`, { next: { revalidate: 300 } });
    if (!res.ok) return [];
    const json = await res.json();
    return json.data ?? json ?? [];
  } catch {
    return [];
  }
}

async function fetchStats(sport: string, type: string): Promise<unknown[]> {
  try {
    const season = getCurrentSeason(sport);
    const aggregate = type === "player-stats" ? "&aggregate=true" : "";
    const sort = type === "player-stats" ? "&sort=points" : "";
    const url = `${API_BASE}/v1/${sport}/${type}?season=${season}${aggregate}${sort}&limit=200`;
    const res = await fetch(url, { next: { revalidate: 60 } });
    if (!res.ok) return [];
    const json = await res.json();
    const data = json.data ?? json ?? [];

    // If no data, try the most recent available season (e.g. ATP/WTA may lack current year)
    if (data.length === 0) {
      const available: string[] = json.meta?.available_seasons ?? [];
      const fallback = available.sort().reverse()[0];
      if (fallback && fallback !== season) {
        const fbUrl = `${API_BASE}/v1/${sport}/${type}?season=${fallback}${aggregate}${sort}&limit=200`;
        const fbRes = await fetch(fbUrl, { next: { revalidate: 60 } });
        if (fbRes.ok) {
          const fbJson = await fbRes.json();
          return fbJson.data ?? fbJson ?? [];
        }
      }
    }

    return data;
  } catch {
    return [];
  }
}

export default async function StatsPage() {
  const sports = [...ALL_SPORT_KEYS];
  const results = await Promise.allSettled(
    sports.map(async (sport) => {
      const [playerStats, teamStats, teams] = await Promise.all([
        fetchStats(sport, "player-stats"),
        fetchStats(sport, "team-stats"),
        fetchTeams(sport),
      ]);
      return { sport, playerStats, teamStats, teams };
    }),
  );

  const playerStatsBySport: Record<string, unknown[]> = {};
  const teamStatsBySport: Record<string, unknown[]> = {};
  const teamMapBySport: Record<string, Record<string, string>> = {};

  for (const r of results) {
    if (r.status === "fulfilled") {
      playerStatsBySport[r.value.sport] = r.value.playerStats;
      teamStatsBySport[r.value.sport] = r.value.teamStats;
      // Build team_id → display name map
      const tmap: Record<string, string> = {};
      for (const t of r.value.teams) {
        const tid = String((t as any).team_id ?? (t as any).id ?? "");
        const name = (t as any).abbreviation ?? (t as any).short_name ?? (t as any).name ?? "";
        if (tid && name) tmap[tid] = name;
      }
      teamMapBySport[r.value.sport] = tmap;
    }
  }

  return (
    <StatsClient
      playerStatsBySport={playerStatsBySport}
      teamStatsBySport={teamStatsBySport}
      teamMapBySport={teamMapBySport}
      sports={sports}
    />
  );
}
