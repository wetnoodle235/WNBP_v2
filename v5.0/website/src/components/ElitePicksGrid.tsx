"use client";

import { ElitePickCard, type ElitePick } from "@/components/ElitePickCard";

interface ElitePicksGridProps {
  picks: ElitePick[];
}

export function ElitePicksGrid({ picks }: ElitePicksGridProps) {
  if (picks.length === 0) {
    return (
      <p style={{ color: "var(--color-text-muted)", padding: "var(--space-4) 0" }}>
        No elite picks available right now — check back closer to game time.
      </p>
    );
  }

  return (
    <div className="elite-picks-grid">
      {picks.map((pick) => (
        <ElitePickCard key={pick.game_id} pick={pick} />
      ))}
    </div>
  );
}

export default ElitePicksGrid;
