"use client";

import Link from "next/link";
import { SectionBand } from "@/components/ui";

interface Leader {
  rank: number;
  display_name: string;
  balance: number;
  pnl: number;
  total_bets: number;
  wins: number;
  losses: number;
  win_rate: number;
}

export default function LeaderboardClient({ initialLeaders }: { initialLeaders: Leader[] }) {
  const leaders = initialLeaders;

  const thStyle: React.CSSProperties = {
    padding: "var(--space-2) var(--space-4)",
    textAlign: "left",
    fontSize: "0.75rem",
    fontWeight: 600,
    textTransform: "uppercase",
    color: "var(--color-text-muted)",
    borderBottom: "2px solid var(--color-border)",
    whiteSpace: "nowrap",
  };
  const tdStyle: React.CSSProperties = {
    padding: "var(--space-2) var(--space-4)",
    fontSize: "0.875rem",
    borderBottom: "1px solid var(--color-border)",
    whiteSpace: "nowrap",
  };

  const medalColors: Record<number, string> = { 1: "#FFD700", 2: "#C0C0C0", 3: "#CD7F32" };

  return (
    <main>
      <SectionBand title="Paper Trading Leaderboard">
        <div style={{ display: "flex", gap: "var(--space-3)", marginBottom: "var(--space-6)" }}>
          <Link
            href="/paper"
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-md, 6px)",
              border: "1px solid var(--color-border)",
              backgroundColor: "transparent",
              color: "inherit",
              fontSize: "0.875rem",
              fontWeight: 600,
              textDecoration: "none",
            }}
          >
            ← My Portfolio
          </Link>
        </div>

        {leaders.length === 0 ? (
          <div
            className="card"
            style={{ padding: "var(--space-8)", textAlign: "center", color: "var(--color-text-muted)" }}
          >
            <p style={{ fontSize: "1.5rem", marginBottom: "var(--space-2)" }}>🏆</p>
            <p>No traders on the leaderboard yet. Be the first to place a bet!</p>
            <Link href="/paper" style={{ color: "var(--color-accent, #2563eb)", textDecoration: "underline" }}>
              Start Trading
            </Link>
          </div>
        ) : (
          <div className="card">
            <div className="responsive-table-wrap">
              <table className="responsive-table paper-leaderboard-table" aria-label="Paper trading leaderboard" style={{ borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th scope="col" style={thStyle}>Rank</th>
                  <th scope="col" style={thStyle}>Trader</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>Balance</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>P&L</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>Bets</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>W-L</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>Win %</th>
                </tr>
              </thead>
              <tbody>
                {leaders.map((l) => {
                  const pnlColor = l.pnl > 0 ? "var(--color-win, #22c55e)" : l.pnl < 0 ? "var(--color-loss, #ef4444)" : "inherit";
                  return (
                    <tr key={l.rank}>
                      <td style={tdStyle}>
                        <span style={{ color: medalColors[l.rank] ?? "inherit", fontWeight: l.rank <= 3 ? 700 : 400 }}>
                          {l.rank <= 3 ? ["🥇", "🥈", "🥉"][l.rank - 1] : `#${l.rank}`}
                        </span>
                      </td>
                      <td style={{ ...tdStyle, fontWeight: 600 }}>{l.display_name}</td>
                      <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>
                        ${l.balance.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </td>
                      <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace", color: pnlColor, fontWeight: 600 }}>
                        {l.pnl >= 0 ? "+" : ""}${l.pnl.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </td>
                      <td style={{ ...tdStyle, textAlign: "right" }}>{l.total_bets}</td>
                      <td style={{ ...tdStyle, textAlign: "right" }}>{l.wins}-{l.losses}</td>
                      <td style={{ ...tdStyle, textAlign: "right" }}>{l.win_rate}%</td>
                    </tr>
                  );
                })}
              </tbody>
              </table>
            </div>
          </div>
        )}
      </SectionBand>
    </main>
  );
}
