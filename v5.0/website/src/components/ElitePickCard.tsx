"use client";

import { Badge, SportBadge } from "@/components/ui";
import { ConfidenceBar } from "@/components/ConfidenceBar";

export interface ElitePick {
  game_id: string;
  sport: string;
  home_team: string;
  away_team: string;
  predicted_winner: string;
  win_prob: number;
  confidence: number;
  predicted_spread?: number | null;
  predicted_total?: number | null;
}

const CONF_TIER: Record<string, { label: string; variant: "live" | "win" | "free" }> = {
  elite: { label: "🔒 LOCK", variant: "live" },
  high:  { label: "🔥 STRONG", variant: "win" },
  good:  { label: "SOLID", variant: "free" },
};

function getTier(conf: number) {
  if (conf >= 0.75) return CONF_TIER.elite;
  if (conf >= 0.62) return CONF_TIER.high;
  return CONF_TIER.good;
}

export function ElitePickCard({ pick }: { pick: ElitePick }) {
  const tier = getTier(pick.confidence);
  const isHome = pick.predicted_winner === pick.home_team;
  const winPct = pick.win_prob * 100;

  return (
    <article className="elite-pick-card" aria-label={`Elite pick: ${pick.away_team} at ${pick.home_team}, predicted winner ${pick.predicted_winner}`}>
      {/* Header: sport badge + confidence tier */}
      <div className="elite-pick-header">
        <SportBadge sport={pick.sport} />
        <Badge variant={tier.variant}>{tier.label}</Badge>
      </div>

      {/* Matchup */}
      <div className="elite-pick-body">
        <div className="elite-pick-prop">
          <span style={{ fontWeight: 700, fontSize: "var(--text-sm)", color: "var(--color-text)" }}>
            {pick.away_team}
          </span>
          <span style={{ color: "var(--color-text-muted)", fontSize: "var(--text-xs)" }}>@</span>
          <span style={{ fontWeight: 700, fontSize: "var(--text-sm)", color: "var(--color-text)" }}>
            {pick.home_team}
          </span>
        </div>

        {/* Predicted winner callout */}
        <div className="elite-pick-direction">
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 4,
            padding: "2px 8px", borderRadius: "var(--radius-sm)",
            background: isHome ? "rgba(22,163,74,0.12)" : "rgba(99,102,241,0.12)",
            color: isHome ? "#16a34a" : "#6366f1",
            fontWeight: 700, fontSize: "var(--text-xs)", letterSpacing: "0.02em",
          }}>
            <span aria-hidden="true">▸</span> {pick.predicted_winner} {isHome ? "(Home)" : "(Away)"}
          </span>
        </div>

        {/* Confidence bar */}
        <ConfidenceBar value={pick.win_prob} height={20} compact />
      </div>

      {/* Footer stats */}
      <div className="elite-pick-footer">
        <div className="elite-pick-meta">
          <span style={{ color: "var(--color-text-muted)", fontSize: "var(--text-xs)" }}>
            Win Prob
          </span>
          <span style={{
            fontWeight: 700, fontSize: "var(--text-xs)",
            fontVariantNumeric: "tabular-nums",
            color: winPct >= 65 ? "var(--color-win, #16a34a)" : "var(--color-text)",
          }}>
            {winPct.toFixed(1)}%
          </span>
          {pick.predicted_spread != null && (
            <>
              <span style={{ color: "var(--color-border)" }} aria-hidden="true">|</span>
              <span style={{ color: "var(--color-text-muted)", fontSize: "var(--text-xs)" }}>Spread</span>
              <span style={{
                fontWeight: 600, fontSize: "var(--text-xs)",
                fontVariantNumeric: "tabular-nums",
                color: "var(--color-text-secondary)",
              }}>
                {pick.predicted_spread > 0 ? "+" : ""}{pick.predicted_spread.toFixed(1)}
              </span>
            </>
          )}
          {pick.predicted_total != null && (
            <>
              <span style={{ color: "var(--color-border)" }} aria-hidden="true">|</span>
              <span style={{ color: "var(--color-text-muted)", fontSize: "var(--text-xs)" }}>O/U</span>
              <span style={{
                fontWeight: 600, fontSize: "var(--text-xs)",
                fontVariantNumeric: "tabular-nums",
                color: "var(--color-text-secondary)",
              }}>
                {pick.predicted_total.toFixed(1)}
              </span>
            </>
          )}
        </div>
      </div>
    </article>
  );
}

export default ElitePickCard;
