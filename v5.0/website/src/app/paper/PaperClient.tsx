"use client";

import React, { useState, useEffect, useMemo, useCallback, useRef } from "react";
import Link from "next/link";
import { SectionBand, Pagination } from "@/components/ui";
import { formatOdds } from "@/lib/formatters";
import { getStoredToken, authHeaders } from "@/lib/api";

const STORAGE_KEY = "wnbp_paper_trades";
const STARTING_BALANCE = 10_000;
const PER_PAGE = 15;
const API_BASE =
  typeof window === "undefined"
    ? (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000")
    : "/api/proxy";

interface PaperBet {
  id: string;
  date: string;
  sport: string;
  game_id?: string;
  matchup: string;
  betType: string;
  pick: string;
  odds: number;
  stake: number;
  result: "win" | "loss" | "push" | "pending";
  pnl: number;
}

interface Portfolio {
  balance: number;
  bets: PaperBet[];
}

function loadPortfolio(): Portfolio {
  if (typeof window === "undefined") {
    return { balance: STARTING_BALANCE, bets: [] };
  }
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as Portfolio;
      return parsed;
    }
  } catch { /* ignore */ }
  return { balance: STARTING_BALANCE, bets: [] };
}

function savePortfolio(portfolio: Portfolio) {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(portfolio));
  } catch {
    // localStorage quota exceeded — silently degrade
  }
}

function buildHeaders(): Record<string, string> {
  return { "Content-Type": "application/json", ...authHeaders() };
}

/** Fetch portfolio from backend with localStorage fallback */
async function fetchBackendPortfolio(): Promise<{ portfolio: Portfolio | null; error?: string }> {
  try {
    const token = getStoredToken();
    if (!token) return { portfolio: null };
    const res = await fetch(`${API_BASE}/v1/paper/portfolio`, {
      headers: buildHeaders(),
    });
    if (!res.ok) return { portfolio: null, error: `Server returned ${res.status} fetching portfolio.` };
    const json = await res.json();
    const data = json?.data ?? json;
    if (data && typeof data.balance === "number") {
      return { portfolio: data as Portfolio };
    }
    return { portfolio: null };
  } catch {
    return { portfolio: null, error: "Could not reach server to load portfolio." };
  }
}

/** Fetch bet history from backend */
async function fetchBetHistory(): Promise<{ bets: PaperBet[] | null; error?: string }> {
  try {
    const token = getStoredToken();
    if (!token) return { bets: null };
    const res = await fetch(`${API_BASE}/v1/paper/history`, {
      headers: buildHeaders(),
    });
    if (!res.ok) return { bets: null, error: `Server returned ${res.status} fetching history.` };
    const json = await res.json();
    const data = json?.data ?? json;
    return { bets: Array.isArray(data) ? data : null };
  } catch {
    return { bets: null, error: "Could not reach server for bet history." };
  }
}

/** Cancel a pending bet on the backend */
async function cancelBetOnBackend(betId: string): Promise<{ ok: boolean; error?: string }> {
  try {
    const token = getStoredToken();
    if (!token) return { ok: false, error: "Not logged in." };
    const res = await fetch(`${API_BASE}/v1/paper/bet/${betId}`, {
      method: "DELETE",
      headers: buildHeaders(),
    });
    if (!res.ok) return { ok: false, error: `Server returned ${res.status} canceling bet.` };
    return { ok: true };
  } catch {
    return { ok: false, error: "Could not reach server to cancel bet." };
  }
}

/** Place a bet via the backend */
export async function placeBetOnBackend(bet: {
  sport: string;
  game_id: string;
  matchup: string;
  bet_type: string;
  selection: string;
  odds: number;
  stake: number;
}): Promise<{ ok: boolean; error?: string }> {
  // Client-side validation
  if (!Number.isFinite(bet.stake) || bet.stake <= 0) {
    return { ok: false, error: "Stake must be a positive number." };
  }
  if (!Number.isFinite(bet.odds)) {
    return { ok: false, error: "Invalid odds value." };
  }
  if (!bet.game_id || !bet.sport || !bet.selection) {
    return { ok: false, error: "Missing required bet details." };
  }
  try {
    const token = getStoredToken();
    if (!token) return { ok: false, error: "Not logged in." };
    const res = await fetch(`${API_BASE}/v1/paper/bet`, {
      method: "POST",
      headers: buildHeaders(),
      body: JSON.stringify(bet),
    });
    if (!res.ok) {
      const errJson = await res.json().catch(() => null);
      const msg = errJson?.detail ?? errJson?.error ?? `Server returned ${res.status}`;
      return { ok: false, error: msg };
    }
    return { ok: true };
  } catch {
    return { ok: false, error: "Could not reach server to place bet." };
  }
}

/* ── Shared styles (outside component to avoid re-creation) ── */
const statCardStyle: React.CSSProperties = {
  flex: "1 1 200px",
  padding: "var(--space-4)",
  textAlign: "center",
};
const statValueStyle: React.CSSProperties = {
  fontSize: "1.5rem",
  fontWeight: 700,
  fontVariantNumeric: "tabular-nums",
};
const statLabelStyle: React.CSSProperties = {
  fontSize: "0.75rem",
  color: "var(--text-muted)",
  textTransform: "uppercase",
  fontWeight: 600,
  marginTop: "4px",
};
const thStyle: React.CSSProperties = {
  padding: "var(--space-2) var(--space-4)",
  textAlign: "left",
  fontSize: "0.75rem",
  fontWeight: 600,
  textTransform: "uppercase",
  color: "var(--text-muted)",
  borderBottom: "2px solid var(--border)",
  whiteSpace: "nowrap",
};
const tdStyle: React.CSSProperties = {
  padding: "var(--space-2) var(--space-4)",
  fontSize: "0.875rem",
  borderBottom: "1px solid var(--border)",
  whiteSpace: "nowrap",
};

export function PaperClient() {
  const [portfolio, setPortfolio] = useState<Portfolio>({ balance: STARTING_BALANCE, bets: [] });
  const [page, setPage] = useState(1);
  const [mounted, setMounted] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const errorTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const showError = useCallback((msg: string) => {
    setErrorMsg(msg);
    if (errorTimer.current) clearTimeout(errorTimer.current);
    errorTimer.current = setTimeout(() => setErrorMsg(null), 5000);
  }, []);

  const refreshPortfolio = useCallback(async () => {
    const { portfolio: backendPortfolio, error } = await fetchBackendPortfolio();
    if (error) showError(error);
    if (backendPortfolio) {
      setPortfolio(backendPortfolio);
      savePortfolio(backendPortfolio);
    } else {
      setPortfolio(loadPortfolio());
    }
  }, [showError]);

  useEffect(() => {
    let cancelled = false;
    async function init() {
      await refreshPortfolio();
      if (!cancelled) setMounted(true);
    }
    init();
    return () => { cancelled = true; };
  }, [refreshPortfolio]);

  // Listen for bet-placed events from other pages so we refetch portfolio
  useEffect(() => {
    function onBetPlaced() {
      refreshPortfolio();
    }
    window.addEventListener("paper-bet-placed", onBetPlaced);
    return () => window.removeEventListener("paper-bet-placed", onBetPlaced);
  }, [refreshPortfolio]);

  const activeBets = useMemo(
    () => portfolio.bets.filter((b) => b.result === "pending"),
    [portfolio.bets],
  );

  const settledBets = useMemo(
    () => portfolio.bets.filter((b) => b.result !== "pending"),
    [portfolio.bets],
  );

  const totalPnl = settledBets.reduce((sum, b) => sum + b.pnl, 0);
  const wins = settledBets.filter((b) => b.result === "win").length;
  const losses = settledBets.filter((b) => b.result === "loss").length;
  const winRate = wins + losses > 0 ? ((wins / (wins + losses)) * 100).toFixed(1) : "0.0";

  const totalPages = Math.max(1, Math.ceil(settledBets.length / PER_PAGE));
  const safeP = Math.min(page, totalPages);
  const pageSlice = settledBets.slice((safeP - 1) * PER_PAGE, safeP * PER_PAGE);

  async function handleCancelBet(betId: string) {
    const { ok, error } = await cancelBetOnBackend(betId);
    if (ok) {
      await refreshPortfolio();
    } else if (error) {
      // Fallback: remove from local state
      showError(error);
      setPortfolio((prev) => {
        const cancelledBet = prev.bets.find((b) => b.id === betId);
        const updated: Portfolio = {
          balance: prev.balance + (cancelledBet?.stake ?? 0),
          bets: prev.bets.filter((b) => b.id !== betId),
        };
        savePortfolio(updated);
        return updated;
      });
    }
  }

  async function handleReset() {
    const fresh: Portfolio = { balance: STARTING_BALANCE, bets: [] };
    setPortfolio(fresh);
    savePortfolio(fresh);
    setPage(1);

    try {
      const token = getStoredToken();
      if (token) {
        const res = await fetch(`${API_BASE}/v1/paper/reset`, {
          method: "POST",
          headers: buildHeaders(),
        });
        if (!res.ok) {
          showError("Failed to reset portfolio on server. Local data was cleared.");
        }
      }
    } catch {
      showError("Could not reach server to reset portfolio. Local data was cleared.");
    }
  }

  if (!mounted) {
    return (
      <main>
        <SectionBand title="Paper Trading">
          <div
            className="card"
            style={{ padding: "var(--space-8)", textAlign: "center", color: "var(--text-muted)" }}
          >
            Loading portfolio…
          </div>
        </SectionBand>
      </main>
    );
  }

  return (
    <main>
      <SectionBand title="Paper Trading">
        {/* Leaderboard link */}
        <div style={{ marginBottom: "var(--space-4)" }}>
          <Link
            href="/paper/leaderboard"
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-md, 6px)",
              border: "1px solid var(--border)",
              backgroundColor: "transparent",
              color: "inherit",
              fontSize: "0.875rem",
              fontWeight: 600,
              textDecoration: "none",
            }}
          >
            🏆 Leaderboard
          </Link>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 240px), 1fr))",
            gap: "var(--space-4)",
            marginBottom: "var(--space-6)",
          }}
        >
          <Link
            href="/ladder"
            className="card"
            style={{ padding: "var(--space-4)", textDecoration: "none" }}
          >
            <div style={{ fontSize: "1.05rem", fontWeight: 700, marginBottom: "0.35rem" }}>📈 Ladder</div>
            <div style={{ color: "var(--text-muted)", fontSize: "0.86rem" }}>
              Build progressive step-up bet sequences and track every rung.
            </div>
          </Link>
          <Link
            href="/autobets"
            className="card"
            style={{ padding: "var(--space-4)", textDecoration: "none" }}
          >
            <div style={{ fontSize: "1.05rem", fontWeight: 700, marginBottom: "0.35rem" }}>🎟️ Lotto</div>
            <div style={{ color: "var(--text-muted)", fontSize: "0.86rem" }}>
              Review high-volatility lotto strategy tickets in AutoBets history.
            </div>
          </Link>
        </div>

        {/* Error toast */}
        {errorMsg && (
          <div
            role="alert"
            style={{
              padding: "0.75rem 1rem",
              marginBottom: "var(--space-4)",
              borderRadius: "var(--radius-md, 6px)",
              background: "#fef2f2",
              border: "1px solid #fecaca",
              color: "#991b1b",
              fontSize: "0.875rem",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <span>{errorMsg}</span>
            <button
              onClick={() => setErrorMsg(null)}
              style={{
                background: "none",
                border: "none",
                color: "#991b1b",
                cursor: "pointer",
                fontWeight: 700,
                fontSize: "1rem",
                lineHeight: 1,
              }}
              aria-label="Dismiss"
            >
              ×
            </button>
          </div>
        )}
        {/* Portfolio Summary */}
        <div
          style={{
            display: "flex",
            gap: "var(--space-4)",
            flexWrap: "wrap",
            marginBottom: "var(--space-6)",
          }}
        >
          <div className="card" style={statCardStyle}>
            <div style={statValueStyle}>
              ${STARTING_BALANCE.toLocaleString()}
            </div>
            <div style={statLabelStyle}>Starting Balance</div>
          </div>
          <div className="card" style={statCardStyle}>
            <div style={statValueStyle}>
              ${portfolio.balance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </div>
            <div style={statLabelStyle}>Current Balance</div>
          </div>
          <div className="card" style={statCardStyle}>
            <div
              style={{
                ...statValueStyle,
                color: totalPnl > 0 ? "#16a34a" : totalPnl < 0 ? "#dc2626" : "inherit",
              }}
            >
              {totalPnl >= 0 ? "+" : ""}${totalPnl.toFixed(2)}
            </div>
            <div style={statLabelStyle}>Total P&L</div>
          </div>
          <div className="card" style={statCardStyle}>
            <div style={statValueStyle}>{winRate}%</div>
            <div style={statLabelStyle}>
              Win Rate ({wins}W – {losses}L)
            </div>
          </div>
        </div>

        {/* Active Bets */}
        <h3
          style={{
            fontSize: "1.1rem",
            fontWeight: 700,
            marginBottom: "var(--space-4)",
          }}
        >
          Active Bets
        </h3>
        {activeBets.length === 0 ? (
          <div
            className="card"
            style={{
              padding: "var(--space-6)",
              textAlign: "center",
              color: "var(--text-muted)",
              marginBottom: "var(--space-6)",
            }}
          >
            <div style={{ fontSize: "1.5rem", marginBottom: "var(--space-2)" }}>📋</div>
            No active bets. Place a bet from the Games or Predictions page.
          </div>
        ) : (
          <div
            className="card responsive-table-wrap"
            style={{ marginBottom: "var(--space-6)" }}
          >
            <table aria-label="Open positions" className="responsive-table paper-active-table" style={{ borderCollapse: "collapse" }}>
              <caption className="sr-only">Pending bets</caption>
              <thead>
                <tr>
                  <th scope="col" style={thStyle}>Sport</th>
                  <th scope="col" style={thStyle}>Matchup</th>
                  <th scope="col" style={thStyle}>Type</th>
                  <th scope="col" style={thStyle}>Pick</th>
                  <th scope="col" style={thStyle}>Odds</th>
                  <th scope="col" style={thStyle}>Stake</th>
                  <th scope="col" style={thStyle}></th>
                </tr>
              </thead>
              <tbody>
                {activeBets.map((bet) => (
                  <tr key={bet.id}>
                    <td style={tdStyle}>{bet.sport.toUpperCase()}</td>
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{bet.matchup}</td>
                    <td style={tdStyle}>{bet.betType}</td>
                    <td style={tdStyle}>{bet.pick}</td>
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{formatOdds(bet.odds)}</td>
                    <td style={tdStyle}>${bet.stake.toFixed(2)}</td>
                    <td style={tdStyle}>
                      <button
                        onClick={() => handleCancelBet(bet.id)}
                        title="Cancel bet"
                        style={{
                          padding: "2px 8px",
                          borderRadius: "var(--radius-md, 6px)",
                          border: "1px solid #dc2626",
                          backgroundColor: "transparent",
                          color: "#dc2626",
                          cursor: "pointer",
                          fontSize: "0.75rem",
                          fontWeight: 600,
                        }}
                      >
                        Cancel
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Bet History */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: "var(--space-4)",
          }}
        >
          <h3 style={{ fontSize: "1.1rem", fontWeight: 700 }}>Bet History</h3>
          <button
            onClick={handleReset}
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-md, 6px)",
              border: "1px solid #dc2626",
              backgroundColor: "transparent",
              color: "#dc2626",
              cursor: "pointer",
              fontSize: "0.8rem",
              fontWeight: 600,
            }}
          >
            Reset Portfolio
          </button>
        </div>

        {settledBets.length === 0 ? (
          <div
            className="card"
            style={{
              padding: "var(--space-6)",
              textAlign: "center",
              color: "var(--text-muted)",
            }}
          >
            <div style={{ fontSize: "1.5rem", marginBottom: "var(--space-2)" }}>📊</div>
            No settled bets yet. Your history will appear here once bets are resolved.
          </div>
        ) : (
          <>
            <div className="card responsive-table-wrap">
              <table aria-label="Bet history" className="responsive-table paper-history-table" style={{ borderCollapse: "collapse" }}>
                <caption className="sr-only">Bet history</caption>
                <thead>
                  <tr>
                    <th scope="col" style={thStyle}>Date</th>
                    <th scope="col" style={thStyle}>Sport</th>
                    <th scope="col" style={thStyle}>Matchup</th>
                    <th scope="col" style={thStyle}>Bet Type</th>
                    <th scope="col" style={thStyle}>Pick</th>
                    <th scope="col" style={thStyle}>Odds</th>
                    <th scope="col" style={thStyle}>Stake</th>
                    <th scope="col" style={thStyle}>Result</th>
                    <th scope="col" style={thStyle}>P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {pageSlice.map((bet, i) => {
                    const resultColor =
                      bet.result === "win"
                        ? "#16a34a"
                        : bet.result === "loss"
                        ? "#dc2626"
                        : "var(--text-muted)";
                    return (
                      <tr
                        key={bet.id}
                        style={{
                          backgroundColor: i % 2 === 0 ? "transparent" : "rgba(128,128,128,0.04)",
                        }}
                      >
                        <td style={tdStyle}>{bet.date}</td>
                        <td style={tdStyle}>{bet.sport.toUpperCase()}</td>
                        <td style={{ ...tdStyle, fontWeight: 600 }}>{bet.matchup}</td>
                        <td style={tdStyle}>{bet.betType}</td>
                        <td style={tdStyle}>{bet.pick}</td>
                        <td style={{ ...tdStyle, fontWeight: 600 }}>{formatOdds(bet.odds)}</td>
                        <td style={tdStyle}>${bet.stake.toFixed(2)}</td>
                        <td style={{ ...tdStyle, fontWeight: 700, color: resultColor, textTransform: "uppercase" }}>
                          {bet.result}
                        </td>
                        <td
                          style={{
                            ...tdStyle,
                            fontWeight: 700,
                            color: bet.pnl > 0 ? "#16a34a" : bet.pnl < 0 ? "#dc2626" : "inherit",
                          }}
                        >
                          {bet.pnl >= 0 ? "+" : ""}${bet.pnl.toFixed(2)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <Pagination page={safeP} totalPages={totalPages} onPageChange={setPage} />
          </>
        )}
      </SectionBand>
    </main>
  );
}
