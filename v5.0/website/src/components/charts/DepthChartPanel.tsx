"use client";

import { useEffect, useState } from "react";
import { ChartPanel } from "@/components/ui";

interface DepthEntry {
  team_id: string;
  team_name: string;
  team_abbr: string;
  position: string;
  position_key: string;
  rank: number;
  athlete_id: string;
  athlete_name: string;
}

interface Props {
  sport: string;
  teamId: string;
  season?: string;
}

const API_BASE =
  typeof window !== "undefined"
    ? ""
    : (process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000");

// Group positions into meaningful display groups per sport
const POSITION_ORDER: Record<string, string[]> = {
  nba: ["Point Guard", "Shooting Guard", "Small Forward", "Power Forward", "Center"],
  wnba: ["Point Guard", "Shooting Guard", "Small Forward", "Power Forward", "Center"],
  nfl: ["Quarterback", "Running Back", "Wide Receiver", "Tight End", "Left Tackle", "Left Guard", "Center", "Right Guard", "Right Tackle",
        "Defensive End", "Defensive Tackle", "Outside Linebacker", "Inside Linebacker", "Cornerback", "Free Safety", "Strong Safety",
        "Kicker", "Punter", "Long Snapper"],
  mlb: ["Starting Pitcher", "Relief Pitcher", "Catcher", "First Base", "Second Base", "Shortstop", "Third Base", "Left Field", "Center Field", "Right Field"],
  nhl: ["Goalie", "Left Wing", "Center", "Right Wing", "Left Defense", "Right Defense"],
};

export function DepthChartPanel({ sport, teamId, season }: Props) {
  const [data, setData] = useState<DepthEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!sport || !teamId) return;
    setLoading(true);
    setError(null);
    const params = new URLSearchParams({ team_id: teamId, limit: "200" });
    if (season) params.set("season", season);
    fetch(`${API_BASE}/v1/${sport}/depth_charts?${params}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
      .then((json) => {
        setData(Array.isArray(json?.data) ? json.data : []);
        setLoading(false);
      })
      .catch(() => {
        setError("Depth chart not available");
        setLoading(false);
      });
  }, [sport, teamId, season]);

  if (loading) {
    return (
      <ChartPanel title="Depth Chart">
        <div style={{ color: "var(--color-text-muted)", fontSize: "0.875rem" }}>Loading depth chart…</div>
      </ChartPanel>
    );
  }
  if (error || data.length === 0) {
    return (
      <ChartPanel title="Depth Chart">
        <p style={{ color: "var(--color-text-muted)", fontSize: "0.875rem", margin: 0 }}>
          {error ?? "No depth chart data available."}
        </p>
      </ChartPanel>
    );
  }

  // Group by position, sorted by rank
  const byPosition = new Map<string, DepthEntry[]>();
  for (const entry of data) {
    const pos = entry.position || "Other";
    if (!byPosition.has(pos)) byPosition.set(pos, []);
    byPosition.get(pos)!.push(entry);
  }
  for (const entries of byPosition.values()) {
    entries.sort((a, b) => a.rank - b.rank);
  }

  // Sort positions by sport-specific order
  const positionOrder = POSITION_ORDER[sport] ?? [];
  const sortedPositions = [...byPosition.keys()].sort((a, b) => {
    const ai = positionOrder.indexOf(a);
    const bi = positionOrder.indexOf(b);
    if (ai === -1 && bi === -1) return a.localeCompare(b);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });

  const RANK_COLORS = ["#16a34a", "#2563eb", "#6366f1", "#9ca3af", "#6b7280"];

  return (
    <ChartPanel title="Depth Chart">
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))",
          gap: "var(--space-3, 0.75rem)",
        }}
      >
        {sortedPositions.map((pos) => {
          const entries = byPosition.get(pos) ?? [];
          return (
            <div
              key={pos}
              style={{
                background: "var(--color-bg-2, #18181b)",
                border: "1px solid var(--color-border, #27272a)",
                borderRadius: "var(--radius, 8px)",
                padding: "var(--space-3, 0.75rem)",
                minWidth: 0,
              }}
            >
              <div
                style={{
                  fontSize: "0.7rem",
                  fontWeight: 700,
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  color: "var(--color-text-muted, #71717a)",
                  marginBottom: "0.5rem",
                }}
              >
                {pos}
              </div>
              {entries.map((e, idx) => (
                <div
                  key={e.athlete_id || `${pos}-${idx}`}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "0.4rem",
                    padding: "0.2rem 0",
                    fontSize: "0.8125rem",
                    color: idx === 0
                      ? "var(--color-text, #f4f4f5)"
                      : "var(--color-text-secondary, #a1a1aa)",
                  }}
                >
                  <span
                    style={{
                      width: 18,
                      height: 18,
                      borderRadius: "50%",
                      background: RANK_COLORS[idx] ?? "#6b7280",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontSize: "0.65rem",
                      fontWeight: 700,
                      color: "#fff",
                      flexShrink: 0,
                    }}
                  >
                    {e.rank}
                  </span>
                  <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontWeight: idx === 0 ? 600 : 400 }}>
                    {e.athlete_name || "—"}
                  </span>
                </div>
              ))}
            </div>
          );
        })}
      </div>
    </ChartPanel>
  );
}
