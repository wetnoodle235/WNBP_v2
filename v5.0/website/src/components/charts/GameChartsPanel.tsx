"use client";

import { useState } from "react";
import { WinProbChart } from "@/components/charts/WinProbChart";
import { LineMovementChart } from "@/components/charts/LineMovementChart";

interface Props {
  sport: string;
  gameId: string;
  homeTeam: string;
  awayTeam: string;
  /** Only show for completed/live games */
  isLiveOrFinal: boolean;
}

type LineKey = "h2h_home" | "h2h_away" | "spread_home" | "total_line";

const LINE_TABS: { key: LineKey; label: string }[] = [
  { key: "h2h_home", label: "ML Home" },
  { key: "h2h_away", label: "ML Away" },
  { key: "spread_home", label: "Spread" },
  { key: "total_line", label: "Total" },
];

export function GameChartsPanel({ sport, gameId, homeTeam, awayTeam, isLiveOrFinal }: Props) {
  const [activeLine, setActiveLine] = useState<LineKey>("h2h_home");

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-5)" }}>
      {/* Win probability only for live/final games */}
      {isLiveOrFinal && (
        <WinProbChart
          sport={sport}
          gameId={gameId}
          homeTeam={homeTeam}
          awayTeam={awayTeam}
        />
      )}

      {/* Line movement — always shown */}
      <div>
        {/* Tab selector */}
        <div
          style={{
            display: "flex",
            gap: "var(--space-2)",
            marginBottom: "var(--space-3)",
            flexWrap: "wrap",
          }}
        >
          {LINE_TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveLine(tab.key)}
              style={{
                padding: "var(--space-1) var(--space-3)",
                fontSize: "var(--text-xs)",
                fontWeight: 600,
                borderRadius: "var(--radius)",
                border: "1px solid var(--color-border)",
                background: activeLine === tab.key ? "var(--color-primary)" : "transparent",
                color: activeLine === tab.key ? "#fff" : "var(--color-text-muted)",
                cursor: "pointer",
                transition: "all 0.15s",
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>
        <LineMovementChart
          sport={sport}
          gameId={gameId}
          lineType={activeLine}
        />
      </div>
    </div>
  );
}
