import LeaderboardClient from "./LeaderboardClient";
import { resolveServerApiBase } from "@/lib/api-base";

const API = resolveServerApiBase();

export const dynamic = "force-dynamic";

export default async function LeaderboardPage() {
  let leaders: any[] = [];
  try {
    const res = await fetch(`${API}/v1/paper/leaderboard?limit=50`, {
      cache: "no-store",
    });
    if (res.ok) {
      const json = await res.json();
      leaders = json.data ?? [];
    }
  } catch {}

  return <LeaderboardClient initialLeaders={leaders} />;
}
