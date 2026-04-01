"use client";

import { useEffect, useState } from "react";
import { SectionBand } from "@/components/ui";

const API = "/api/proxy";

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

function medal(rank: number) {
  if (rank === 1) return "🥇";
  if (rank === 2) return "🥈";
  if (rank === 3) return "🥉";
  return `#${rank}`;
}

function pnlColor(pnl: number) {
  if (pnl > 0) return "var(--accent-green)";
  if (pnl < 0) return "var(--accent-red)";
  return "var(--text-secondary)";
}

export default function LadderClient() {
  const [leaders, setLeaders] = useState<Leader[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    fetch(`${API}/v1/paper/leaderboard?limit=50`, { signal: ctrl.signal })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => {
        if (d.success) setLeaders(d.data);
        else setError("Failed to load leaderboard");
      })
      .catch((e) => {
        if (e.name !== "AbortError") setError("Unable to connect to the server");
      })
      .finally(() => setLoading(false));
    return () => ctrl.abort();
  }, []);

  return (
    <main>
      <SectionBand title="Leaderboard">
        {loading ? (
          <div className="card">
            <div className="card-body" role="status" aria-live="polite" style={{ textAlign: "center", padding: "2rem" }}>
              Loading leaderboard…
            </div>
          </div>
        ) : error ? (
          <div className="card" role="alert">
            <div className="card-body" style={{ textAlign: "center", padding: "2rem" }}>
              <p style={{ fontSize: "1.1rem", marginBottom: "0.5rem", color: "var(--accent-red)" }}>⚠️ {error}</p>
              <button className="btn btn-sm btn-primary" onClick={() => window.location.reload()}>Retry</button>
            </div>
          </div>
        ) : leaders.length === 0 ? (
          <div className="card">
            <div className="card-body" style={{ textAlign: "center", padding: "2rem" }}>
              <p style={{ fontSize: "1.1rem", marginBottom: "0.5rem" }}>No traders yet</p>
              <p style={{ color: "var(--text-secondary)", fontSize: "0.9rem" }}>
                Start paper trading to appear on the leaderboard. Everyone begins with $10,000.
              </p>
            </div>
          </div>
        ) : (
          <div className="card responsive-table-wrap ladder-table-wrap">
            <table className="responsive-table ladder-table" aria-label="Paper trading leaderboard" style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
              <caption className="sr-only">Paper trading leaderboard — top 50 traders</caption>
              <thead>
                <tr
                  style={{
                    borderBottom: "2px solid var(--border)",
                    textAlign: "left",
                  }}
                >
                  <th scope="col" style={{ padding: "0.75rem 1rem" }}>Rank</th>
                  <th scope="col" style={{ padding: "0.75rem 0.5rem" }}>Trader</th>
                  <th scope="col" style={{ padding: "0.75rem 0.5rem", textAlign: "right" }}>Balance</th>
                  <th scope="col" style={{ padding: "0.75rem 0.5rem", textAlign: "right" }}>P&amp;L</th>
                  <th scope="col" style={{ padding: "0.75rem 0.5rem", textAlign: "center" }}>Record</th>
                  <th scope="col" style={{ padding: "0.75rem 0.5rem", textAlign: "right" }}>Win %</th>
                  <th scope="col" style={{ padding: "0.75rem 1rem", textAlign: "right" }}>Bets</th>
                </tr>
              </thead>
              <tbody>
                {leaders.map((l) => (
                  <tr
                    key={l.rank}
                    style={{
                      borderBottom: "1px solid var(--border)",
                      background: l.rank <= 3 ? "rgba(255,215,0,0.04)" : undefined,
                    }}
                  >
                    <td
                      style={{
                        padding: "0.6rem 1rem",
                        fontWeight: l.rank <= 3 ? 700 : 400,
                        fontSize: l.rank <= 3 ? "1.1rem" : "0.9rem",
                      }}
                    >
                      {medal(l.rank)}
                    </td>
                    <td style={{ padding: "0.6rem 0.5rem", fontWeight: 500 }}>
                      {l.display_name}
                    </td>
                    <td
                      style={{
                        padding: "0.6rem 0.5rem",
                        textAlign: "right",
                        fontVariantNumeric: "tabular-nums",
                      }}
                    >
                      ${l.balance.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                    </td>
                    <td
                      style={{
                        padding: "0.6rem 0.5rem",
                        textAlign: "right",
                        color: pnlColor(l.pnl),
                        fontWeight: 600,
                        fontVariantNumeric: "tabular-nums",
                      }}
                    >
                      {l.pnl >= 0 ? "+" : ""}
                      ${l.pnl.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                    </td>
                    <td
                      style={{
                        padding: "0.6rem 0.5rem",
                        textAlign: "center",
                        fontVariantNumeric: "tabular-nums",
                      }}
                    >
                      <span style={{ color: "var(--accent-green)" }} aria-label={`${l.wins} wins`}>{l.wins}W</span>
                      {" - "}
                      <span style={{ color: "var(--accent-red)" }} aria-label={`${l.losses} losses`}>{l.losses}L</span>
                    </td>
                    <td
                      style={{
                        padding: "0.6rem 0.5rem",
                        textAlign: "right",
                        fontVariantNumeric: "tabular-nums",
                      }}
                    >
                      {l.win_rate}%
                    </td>
                    <td
                      style={{
                        padding: "0.6rem 1rem",
                        textAlign: "right",
                        fontVariantNumeric: "tabular-nums",
                      }}
                    >
                      {l.total_bets}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </SectionBand>
    </main>
  );
}
