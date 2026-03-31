"use client";

import { useState, useEffect } from "react";

// ── Types ──────────────────────────────────────────────────────────────────────

export interface InjuredPlayer {
  player_id: string;
  player_name: string;
  team_abbr: string;
  status: string;
  reason?: string;
  injury_start?: string;
  injury_end?: string;
}

export interface GameLogEntry {
  game_date?: string;
  pts?: number;
  reb?: number;
  ast?: number;
  stl?: number;
  blk?: number;
  min?: string;
  plus_minus?: number;
  // team stats (for team view)
  [key: string]: unknown;
}

interface SplitStats {
  games: number;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  plusMinus: number;
}

function avgSplit(games: GameLogEntry[]): SplitStats {
  if (games.length === 0) return { games: 0, pts: 0, reb: 0, ast: 0, stl: 0, blk: 0, plusMinus: 0 };
  type Acc = { pts: number; reb: number; ast: number; stl: number; blk: number; plusMinus: number };
  const sum = games.reduce<Acc>(
    (acc, g) => ({
      pts: acc.pts + (g.pts ?? 0),
      reb: acc.reb + (g.reb ?? 0),
      ast: acc.ast + (g.ast ?? 0),
      stl: acc.stl + (g.stl ?? 0),
      blk: acc.blk + (g.blk ?? 0),
      plusMinus: acc.plusMinus + (g.plus_minus ?? 0),
    }),
    { pts: 0, reb: 0, ast: 0, stl: 0, blk: 0, plusMinus: 0 }
  );
  const n = games.length;
  return {
    games: n,
    pts: sum.pts / n,
    reb: sum.reb / n,
    ast: sum.ast / n,
    stl: sum.stl / n,
    blk: sum.blk / n,
    plusMinus: sum.plusMinus / n,
  };
}

function fmt(v: number, decimals = 1): string {
  return v.toFixed(decimals);
}

function DeltaBadge({ delta, label }: { delta: number; label: string }) {
  if (Math.abs(delta) < 0.05) return <span style={{ color: "var(--color-text-tertiary)", fontSize: "var(--text-xs)" }}>±0</span>;
  const up = delta > 0;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 2,
        color: up ? "var(--color-win)" : "var(--color-loss)",
        fontWeight: 600,
        fontSize: "var(--text-xs)",
      }}
    >
      {up ? "▲" : "▼"} {Math.abs(delta).toFixed(1)} {label}
    </span>
  );
}

interface InjuryImpactPanelProps {
  sport: string;
  teamAbbr: string;
  /** Player-view: pass the game log to enable split analysis */
  gameLog?: GameLogEntry[];
  /** Context label, e.g. "Celtics" or "Jayson Tatum" */
  contextLabel?: string;
}

export default function InjuryImpactPanel({ sport, teamAbbr, gameLog, contextLabel }: InjuryImpactPanelProps) {
  const [injuries, setInjuries] = useState<InjuredPlayer[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    if (!sport || !teamAbbr) return;
    fetch(`/api/injuries/${sport}`)
      .then(r => r.json())
      .then(d => {
        const raw: InjuredPlayer[] = Array.isArray(d?.injuries) ? d.injuries : [];
        // Filter to this team only
        const abbr = teamAbbr.toUpperCase();
        setInjuries(raw.filter(i => i.team_abbr?.toUpperCase() === abbr));
      })
      .catch(() => setFetchError(true))
      .finally(() => setLoading(false));
  }, [sport, teamAbbr]);

  if (loading) {
    return (
      <div style={{ padding: "var(--space-4)", background: "var(--color-bg-2)", borderRadius: "var(--radius)", border: "1px solid var(--color-border)" }}>
        <div style={{ height: 16, width: 160, background: "var(--color-bg-3)", borderRadius: 4, marginBottom: 8 }} />
        <div style={{ height: 12, width: "80%", background: "var(--color-bg-3)", borderRadius: 4 }} />
      </div>
    );
  }

  if (fetchError) {
    return (
      <div style={{ padding: "var(--space-4)", background: "var(--color-bg-2)", borderRadius: "var(--radius)", border: "1px solid var(--color-border)", color: "var(--text-muted, var(--color-text-secondary))", fontSize: "var(--text-sm)" }}>
        Unable to load injury data.
      </div>
    );
  }

  if (injuries.length === 0) {
    return (
      <div style={{ padding: "var(--space-4)", background: "var(--color-bg-2)", borderRadius: "var(--radius)", border: "1px solid var(--color-border)", color: "var(--color-text-secondary)", fontSize: "var(--text-sm)" }}>
        ✅ No current injuries reported for this team.
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-3)" }}>
      <p style={{ fontSize: "var(--text-sm)", color: "var(--color-text-secondary)", margin: 0 }}>
        {injuries.length} player{injuries.length !== 1 ? "s" : ""} currently injured.
        {gameLog && gameLog.length > 0 && " Split stats show performance with vs. without each player."}
      </p>

      {injuries.map(inj => {
        const injuryStart = inj.injury_start ? new Date(inj.injury_start) : null;
        const isOut = inj.status.toLowerCase().includes("out");

        // Compute split if we have a game log
        let withPlayer: SplitStats | null = null;
        let withoutPlayer: SplitStats | null = null;
        let returnImpact: { stat: string; delta: number; direction: "up" | "down" }[] = [];

        if (gameLog && gameLog.length > 0 && injuryStart) {
          const gamesWithPlayer = gameLog.filter(g => {
            if (!g.game_date) return false;
            return new Date(g.game_date) < injuryStart;
          });
          const gamesWithoutPlayer = gameLog.filter(g => {
            if (!g.game_date) return false;
            return new Date(g.game_date) >= injuryStart;
          });

          if (gamesWithPlayer.length >= 3 && gamesWithoutPlayer.length >= 3) {
            withPlayer = avgSplit(gamesWithPlayer);
            withoutPlayer = avgSplit(gamesWithoutPlayer);

            // Estimate return impact: reverting toward "with" averages
            returnImpact = (
              [
                { stat: "PTS", delta: withPlayer.pts - withoutPlayer.pts, direction: (withPlayer.pts > withoutPlayer.pts ? "up" : "down") as "up" | "down" },
                { stat: "REB", delta: withPlayer.reb - withoutPlayer.reb, direction: (withPlayer.reb > withoutPlayer.reb ? "up" : "down") as "up" | "down" },
                { stat: "AST", delta: withPlayer.ast - withoutPlayer.ast, direction: (withPlayer.ast > withoutPlayer.ast ? "up" : "down") as "up" | "down" },
              ] as { stat: string; delta: number; direction: "up" | "down" }[]
            ).filter(x => Math.abs(x.delta) >= 0.3);
          }
        }

        const isExpanded = expanded === inj.player_id;

        return (
          <div
            key={inj.player_id}
            style={{
              background: "var(--color-bg-2)",
              border: `1px solid ${isOut ? "var(--color-loss)" : "var(--color-warning, #f59e0b)"}22`,
              borderLeft: `3px solid ${isOut ? "var(--color-loss)" : "var(--color-warning, #f59e0b)"}`,
              borderRadius: "var(--radius)",
              overflow: "hidden",
            }}
          >
            {/* Header row */}
            <button
              onClick={() => setExpanded(isExpanded ? null : inj.player_id)}
              style={{
                width: "100%",
                display: "flex",
                alignItems: "center",
                gap: "var(--space-3)",
                padding: "var(--space-3) var(--space-4)",
                background: "none",
                border: "none",
                cursor: "pointer",
                textAlign: "left",
              }}
            >
              {/* Status badge */}
              <span
                style={{
                  display: "inline-block",
                  padding: "2px 8px",
                  borderRadius: 999,
                  fontSize: "var(--text-xs)",
                  fontWeight: 700,
                  background: isOut ? "var(--color-loss)" : "var(--color-warning, #f59e0b)",
                  color: "#fff",
                  whiteSpace: "nowrap",
                  flexShrink: 0,
                }}
              >
                {inj.status}
              </span>

              <div style={{ flex: 1, minWidth: 0 }}>
                <span style={{ fontWeight: 600, fontSize: "var(--text-sm)", color: "var(--color-text-primary)" }}>
                  {inj.player_name}
                </span>
                {inj.injury_end && (
                  <span style={{ marginLeft: 8, fontSize: "var(--text-xs)", fontWeight: 600, color: "var(--color-text-secondary)", background: "var(--color-bg-3)", padding: "1px 6px", borderRadius: 999 }}>
                    Est. return {new Date(inj.injury_end).toLocaleDateString("en-US", { month: "short", day: "numeric" })}
                  </span>
                )}
              </div>

              {/* Quick return impact pills */}
              {returnImpact.length > 0 && (
                <span style={{ display: "flex", gap: 6, flexShrink: 0 }}>
                  {returnImpact.slice(0, 2).map(ri => (
                    <DeltaBadge key={ri.stat} delta={ri.delta} label={ri.stat} />
                  ))}
                </span>
              )}

              {(withPlayer || inj.reason) && (
                <span style={{ fontSize: "var(--text-xs)", color: "var(--color-text-tertiary)", flexShrink: 0 }}>
                  {isExpanded ? "▲" : "▼"}
                </span>
              )}
            </button>

            {/* Expanded details */}
            {isExpanded && (
              <div style={{ padding: "0 var(--space-4) var(--space-4)", display: "flex", flexDirection: "column", gap: "var(--space-3)" }}>

                {/* Injury reason */}
                {inj.reason && (
                  <p style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", margin: 0, fontStyle: "italic", borderTop: "1px solid var(--color-border)", paddingTop: "var(--space-2)" }}>
                    {inj.reason.length > 200 ? inj.reason.slice(0, 200) + "…" : inj.reason}
                  </p>
                )}

                {/* Date range */}
                {injuryStart && (
                  <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-tertiary)" }}>
                    Out since {injuryStart.toLocaleDateString()}
                  </div>
                )}

                {/* Split stats table */}
                {withPlayer && withoutPlayer && (
                  <div style={{ background: "var(--color-bg-3)", borderRadius: "var(--radius)", padding: "var(--space-3)", overflow: "hidden" }}>
                    <p style={{ fontSize: "var(--text-xs)", fontWeight: 700, color: "var(--color-text-secondary)", margin: "0 0 var(--space-2)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                      {contextLabel ?? "Player"} splits · {withPlayer.games}G with / {withoutPlayer.games}G without
                    </p>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "var(--text-xs)" }}>
                      <thead>
                        <tr style={{ color: "var(--color-text-tertiary)" }}>
                          <th style={{ textAlign: "left", padding: "2px 6px", fontWeight: 600 }}>Stat</th>
                          <th style={{ textAlign: "right", padding: "2px 6px", fontWeight: 600 }}>With</th>
                          <th style={{ textAlign: "right", padding: "2px 6px", fontWeight: 600 }}>Without</th>
                          <th style={{ textAlign: "right", padding: "2px 6px", fontWeight: 600 }}>Return Δ</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(
                          [
                            { label: "PTS", with: withPlayer.pts, without: withoutPlayer.pts },
                            { label: "REB", with: withPlayer.reb, without: withoutPlayer.reb },
                            { label: "AST", with: withPlayer.ast, without: withoutPlayer.ast },
                            { label: "STL", with: withPlayer.stl, without: withoutPlayer.stl },
                            { label: "BLK", with: withPlayer.blk, without: withoutPlayer.blk },
                            { label: "+/-", with: withPlayer.plusMinus, without: withoutPlayer.plusMinus },
                          ] as { label: string; with: number; without: number }[]
                        ).map(row => {
                          const delta = row.with - row.without;
                          const color = Math.abs(delta) < 0.1 ? "inherit" : delta > 0 ? "var(--color-win)" : "var(--color-loss)";
                          return (
                            <tr key={row.label} style={{ borderTop: "1px solid var(--color-border)" }}>
                              <td style={{ padding: "3px 6px", fontWeight: 600, color: "var(--color-text-secondary)" }}>{row.label}</td>
                              <td style={{ textAlign: "right", padding: "3px 6px" }}>{fmt(row.with)}</td>
                              <td style={{ textAlign: "right", padding: "3px 6px" }}>{fmt(row.without)}</td>
                              <td style={{ textAlign: "right", padding: "3px 6px", color, fontWeight: Math.abs(delta) >= 0.5 ? 700 : 400 }}>
                                {delta > 0 ? "+" : ""}{fmt(delta)}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                    <p style={{ fontSize: "var(--text-xs)", color: "var(--color-text-tertiary)", margin: "var(--space-2) 0 0", fontStyle: "italic" }}>
                      Return Δ = projected change if {inj.player_name.split(" ").pop()} returns (positive = improvement).
                      Based on {withPlayer.games + withoutPlayer.games} recent games.
                    </p>
                  </div>
                )}

                {/* No split data fallback — show estimated impact from season averages */}
                {!withPlayer && (
                  <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", borderTop: "1px solid var(--color-border)", paddingTop: "var(--space-2)" }}>
                    {gameLog && gameLog.length > 0
                      ? `Not enough game log overlap to compute splits (need ≥3 games each side of ${injuryStart?.toLocaleDateString() ?? "injury date"}).`
                      : "Add a game log to enable split analysis."}
                  </div>
                )}
              </div>
            )}
          </div>
        );
      })}

      <p style={{ fontSize: "var(--text-xs)", color: "var(--color-text-tertiary)", margin: 0 }}>
        * Projections based on recent game performance splits before/after injury dates. Past performance is not a guarantee of future results.
      </p>
    </div>
  );
}
