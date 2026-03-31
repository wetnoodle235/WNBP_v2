"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { SectionBand, StatCard, Badge, SportBadge, Pagination } from "@/components/ui";
import { fetchAPI } from "@/lib/api";

// ── Types ────────────────────────────────────────────────────────────

interface BetLeg {
  game_id: string;
  sport: string;
  bet_type: string;
  selection: string;
  line: number | null;
  odds_american: number;
  odds_decimal: number;
  bookmaker: string;
  model_confidence: number;
  model_edge: number;
  home_team: string;
  away_team: string;
  result: string | null;
}

interface AutoBet {
  id: string;
  placed_at: string;
  sport: string;
  bet_type: string;
  leg_count: number;
  legs: BetLeg[];
  stake_units: number;
  model_confidence: number;
  model_edge: number;
  implied_odds: number;
  status: string;
  result_at: string | null;
  pnl_units: number;
  rationale: string;
  strategy: string;
}

interface BotStatus {
  bot: {
    enabled: boolean;
    sports: string[];
    betting_cycle_seconds: number;
    grading_cycle_seconds: number;
  };
  bankroll: {
    starting: number;
    current: number;
    pending_exposure: number;
  };
  today: {
    total: number;
    pending: number;
    won: number;
    lost: number;
    push: number;
    pnl: number;
  };
  stats_30d: {
    total_graded: number;
    won: number;
    lost: number;
    push: number;
    win_rate: number;
    total_pnl: number;
    total_wagered: number;
    roi: number;
    by_sport: Record<string, { won: number; lost: number; push: number; pnl: number; wagered: number }>;
    by_bet_type: Record<string, { won: number; lost: number; push: number; pnl: number; wagered: number }>;
  };
  active_bets: AutoBet[];
}

// ── Helpers ──────────────────────────────────────────────────────────

function formatOdds(american: number): string {
  return american > 0 ? `+${american}` : `${american}`;
}

function formatPnl(pnl: number): string {
  return pnl >= 0 ? `+${pnl.toFixed(2)}u` : `${pnl.toFixed(2)}u`;
}

function formatPct(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function strategyLabel(strategy: string): string {
  switch (strategy) {
    case "core":
      return "Core";
    case "lotto_daily":
      return "Lotto";
    case "ladder_daily":
      return "Ladder";
    default:
      return strategy;
  }
}

function betTypeLabel(bt: string): string {
  return bt.charAt(0).toUpperCase() + bt.slice(1).replace(/_/g, " ");
}

function statusVariant(status: string): string {
  switch (status) {
    case "won":
      return "win";
    case "lost":
      return "loss";
    case "push":
      return "push";
    default:
      return "free";
  }
}

// ── Styles ───────────────────────────────────────────────────────────

const cardStyle: React.CSSProperties = {
  background: "var(--color-bg-2)",
  border: "1px solid var(--color-border)",
  borderRadius: "var(--radius-md, 8px)",
  padding: "var(--space-4)",
};

const thStyle: React.CSSProperties = {
  padding: "var(--space-2) var(--space-4)",
  textAlign: "left",
  fontSize: "var(--text-sm)",
  fontWeight: 600,
  textTransform: "uppercase",
  color: "var(--color-text-muted)",
  borderBottom: "2px solid var(--color-border)",
};

const tdStyle: React.CSSProperties = {
  padding: "var(--space-2) var(--space-4)",
  fontSize: "var(--text-base)",
  borderBottom: "1px solid var(--color-border)",
};

const REFRESH_MS = 30_000;
const HISTORY_PER_PAGE = 12;

// ── Component ────────────────────────────────────────────────────────

export default function AutoBetsClient() {
  const [status, setStatus] = useState<BotStatus | null>(null);
  const [history, setHistory] = useState<AutoBet[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [histPage, setHistPage] = useState(1);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const abortRef = useRef<AbortController | null>(null);

  const fetchData = useCallback(async (signal?: AbortSignal) => {
    const [statusRes, histRes] = await Promise.all([
      fetchAPI<{ success: boolean; data: BotStatus }>("/v1/autobet/status", { signal }),
      fetchAPI<{ success: boolean; data: AutoBet[]; meta: { total: number } }>("/v1/autobet/history?limit=200", { signal }),
    ]);

    if (signal?.aborted) return;

    const gotStatus = statusRes.ok && statusRes.data?.data;
    const gotHistory = histRes.ok && histRes.data?.data;

    if (gotStatus) setStatus(statusRes.data!.data);
    if (gotHistory) setHistory(histRes.data!.data);

    if (!gotStatus && !gotHistory) {
      setError((prev) => prev ?? "Unable to connect to AutoBet service");
    } else {
      setError(null);
      setLastUpdated(new Date());
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    abortRef.current = controller;
    fetchData(controller.signal);
    intervalRef.current = setInterval(() => {
      // Abort any in-flight request before starting a new one
      abortRef.current?.abort();
      const c = new AbortController();
      abortRef.current = c;
      fetchData(c.signal);
    }, REFRESH_MS);
    return () => {
      controller.abort();
      abortRef.current?.abort();
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [fetchData]);

  // ── Derived ──────────────────────────────────────────────

  const activeBets = status?.active_bets ?? [];
  const today = status?.today ?? { total: 0, pending: 0, won: 0, lost: 0, push: 0, pnl: 0 };
  const stats = status?.stats_30d;
  const bankroll = status?.bankroll;

  const histTotal = Math.max(1, Math.ceil(history.length / HISTORY_PER_PAGE));
  const histSlice = history.slice((histPage - 1) * HISTORY_PER_PAGE, histPage * HISTORY_PER_PAGE);

  // ── Loading / Error states ─────────────────────────────

  if (loading) {
    return (
      <SectionBand title="Auto Bets">
        <div style={{ ...cardStyle, textAlign: "center", padding: "var(--space-8)" }}>
          <p style={{ color: "var(--color-text-muted)" }}>Loading AutoBet data…</p>
        </div>
      </SectionBand>
    );
  }

  if (error && !status) {
    return (
      <SectionBand title="Auto Bets">
        <div style={{ ...cardStyle, textAlign: "center", padding: "var(--space-8)" }}>
          <p role="alert" style={{ color: "var(--color-text-muted)", marginBottom: "var(--space-2)" }}>
            {error}
          </p>
          <p style={{ color: "var(--color-text-muted)", fontSize: "var(--text-sm)" }}>
            The AutoBet bot may not be running yet. Start it with{" "}
            <code style={{ background: "var(--color-bg-3)", padding: "2px 6px", borderRadius: "var(--radius-sm)" }}>
              python -m autobet.scheduler
            </code>
          </p>
        </div>
      </SectionBand>
    );
  }

  return (
    <>
      {/* ── Bot Status & Summary Stats ──────────────────────────── */}
      <SectionBand
        title="Auto Bets"
        action={
          <span style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>
            {lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()} · ` : ""}Auto-refreshes every 30s
          </span>
        }
      >
        {/* Status indicator */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "var(--space-3)",
            marginBottom: "var(--space-4)",
          }}
        >
          <span
            role="status"
            aria-label={status?.bot.enabled ? "Bot is enabled" : "Bot is disabled"}
            style={{
              display: "inline-block",
              width: 10,
              height: 10,
              borderRadius: "50%",
              background: status?.bot.enabled ? "var(--color-win)" : "var(--color-loss)",
            }}
          />
          <span style={{ fontWeight: 600, fontSize: "var(--text-md)" }}>
            Bot {status?.bot.enabled ? "Enabled" : "Disabled"}
          </span>
          {status?.bot.sports.map((s) => (
            <SportBadge key={s} sport={s} />
          ))}
        </div>

        {/* Summary stat cards */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))",
            gap: "var(--space-4)",
            marginBottom: "var(--space-6)",
          }}
        >
          <StatCard
            label="Bankroll"
            value={`$${bankroll?.current.toFixed(2) ?? "—"}`}
            sub={`Starting: $${bankroll?.starting.toFixed(2) ?? "—"}`}
            accent={
              bankroll && bankroll.current >= bankroll.starting ? "win" : "loss"
            }
          />
          <StatCard
            label="Today P/L"
            value={formatPnl(today.pnl)}
            sub={`${today.won}W – ${today.lost}L – ${today.push}P`}
            accent={today.pnl > 0 ? "win" : today.pnl < 0 ? "loss" : "neutral"}
          />
          <StatCard
            label="Active Bets"
            value={activeBets.length}
            sub={`Exposure: ${bankroll?.pending_exposure.toFixed(2) ?? 0}u`}
            accent="blue"
          />
          <StatCard
            label="Win Rate (30d)"
            value={stats ? formatPct(stats.win_rate) : "—"}
            sub={stats ? `${stats.won}W – ${stats.lost}L` : undefined}
            accent={stats && stats.win_rate >= 0.55 ? "win" : "neutral"}
          />
          <StatCard
            label="ROI (30d)"
            value={stats ? formatPct(stats.roi) : "—"}
            sub={stats ? `P/L: ${formatPnl(stats.total_pnl)}` : undefined}
            accent={stats && stats.roi > 0 ? "win" : stats && stats.roi < 0 ? "loss" : "neutral"}
          />
          <StatCard
            label="Total Bets (30d)"
            value={stats?.total_graded ?? 0}
            sub={stats ? `Wagered: ${stats.total_wagered.toFixed(1)}u` : undefined}
          />
        </div>
      </SectionBand>

      {/* ── Active Bets ─────────────────────────────────────────── */}
      <SectionBand title="Active Bets">
        {activeBets.length === 0 ? (
          <div style={{ ...cardStyle, textAlign: "center" }}>
            <p style={{ color: "var(--color-text-muted)" }}>No active bets right now.</p>
          </div>
        ) : (
          <div className="responsive-table-wrap">
            <table className="responsive-table autobets-table" aria-label="Auto bet rules" style={{ borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th scope="col" style={thStyle}>Sport</th>
                  <th scope="col" style={thStyle}>Type</th>
                  <th scope="col" style={thStyle}>Pick</th>
                  <th scope="col" style={thStyle}>Odds</th>
                  <th scope="col" style={thStyle}>Stake</th>
                  <th scope="col" style={thStyle}>Confidence</th>
                  <th scope="col" style={thStyle}>Edge</th>
                  <th scope="col" style={thStyle}>Strategy</th>
                  <th scope="col" style={thStyle}>Placed</th>
                </tr>
              </thead>
              <tbody>
                {activeBets.map((bet) => (
                  <tr key={bet.id}>
                    <td style={tdStyle}>
                      <SportBadge sport={bet.sport} />
                    </td>
                    <td style={tdStyle}>{betTypeLabel(bet.bet_type)}</td>
                    <td style={tdStyle}>
                      <BetPickCell bet={bet} />
                    </td>
                    <td style={tdStyle}>
                      {bet.legs.length === 1
                        ? formatOdds(bet.legs[0].odds_american)
                        : `${bet.implied_odds.toFixed(2)}x`}
                    </td>
                    <td style={tdStyle}>{bet.stake_units.toFixed(2)}u</td>
                    <td style={tdStyle}>{formatPct(bet.model_confidence)}</td>
                    <td style={tdStyle}>{formatPct(bet.model_edge)}</td>
                    <td style={tdStyle}>
                      <Badge variant="free">{strategyLabel(bet.strategy)}</Badge>
                    </td>
                    <td style={{ ...tdStyle, fontSize: "var(--text-sm)", color: "var(--color-text-secondary)" }}>
                      {formatTime(bet.placed_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </SectionBand>

      {/* ── Bet History ──────────────────────────────────────────── */}
      <SectionBand title="Bet History">
        {history.length === 0 ? (
          <div style={{ ...cardStyle, textAlign: "center" }}>
            <p style={{ color: "var(--color-text-muted)" }}>No completed bets yet.</p>
          </div>
        ) : (
          <>
            <div className="responsive-table-wrap">
              <table className="responsive-table autobets-table" aria-label="Recent auto bet results" style={{ borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th scope="col" style={thStyle}>Result</th>
                    <th scope="col" style={thStyle}>Sport</th>
                    <th scope="col" style={thStyle}>Type</th>
                    <th scope="col" style={thStyle}>Pick</th>
                    <th scope="col" style={thStyle}>Odds</th>
                    <th scope="col" style={thStyle}>Stake</th>
                    <th scope="col" style={thStyle}>P/L</th>
                    <th scope="col" style={thStyle}>Strategy</th>
                    <th scope="col" style={thStyle}>Settled</th>
                  </tr>
                </thead>
                <tbody>
                  {histSlice.map((bet) => (
                    <tr key={bet.id}>
                      <td style={tdStyle}>
                        <Badge variant={statusVariant(bet.status)}>
                          {bet.status.charAt(0).toUpperCase() + bet.status.slice(1)}
                        </Badge>
                      </td>
                      <td style={tdStyle}>
                        <SportBadge sport={bet.sport} />
                      </td>
                      <td style={tdStyle}>{betTypeLabel(bet.bet_type)}</td>
                      <td style={tdStyle}>
                        <BetPickCell bet={bet} />
                      </td>
                      <td style={tdStyle}>
                        {bet.legs.length === 1
                          ? formatOdds(bet.legs[0].odds_american)
                          : `${bet.implied_odds.toFixed(2)}x`}
                      </td>
                      <td style={tdStyle}>{bet.stake_units.toFixed(2)}u</td>
                      <td
                        style={{
                          ...tdStyle,
                          fontWeight: 600,
                          color:
                            bet.pnl_units > 0
                              ? "var(--color-win)"
                              : bet.pnl_units < 0
                              ? "var(--color-loss)"
                              : "inherit",
                        }}
                      >
                        {formatPnl(bet.pnl_units)}
                      </td>
                      <td style={tdStyle}>
                        <Badge variant="free">{strategyLabel(bet.strategy)}</Badge>
                      </td>
                      <td style={{ ...tdStyle, fontSize: "var(--text-sm)", color: "var(--color-text-secondary)" }}>
                        {bet.result_at ? formatTime(bet.result_at) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div style={{ marginTop: "var(--space-4)" }}>
              <Pagination page={histPage} totalPages={histTotal} onPageChange={setHistPage} />
            </div>
          </>
        )}
      </SectionBand>

      {/* ── 30-Day Breakdown by Sport ────────────────────────────── */}
      {stats && Object.keys(stats.by_sport).length > 0 && (
        <SectionBand title="30-Day Breakdown by Sport">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
              gap: "var(--space-4)",
            }}
          >
            {Object.entries(stats.by_sport).map(([sport, data]) => {
              const total = data.won + data.lost + data.push;
              const wr = total > 0 ? data.won / total : 0;
              return (
                <div key={sport} style={cardStyle}>
                  <div style={{ display: "flex", alignItems: "center", gap: "var(--space-2)", marginBottom: "var(--space-2)" }}>
                    <SportBadge sport={sport} />
                  </div>
                  <div style={{ fontSize: "var(--text-lg)", fontWeight: 700 }}>
                    {formatPct(wr)}
                  </div>
                  <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-secondary)" }}>
                    {data.won}W – {data.lost}L – {data.push}P
                  </div>
                  <div
                    style={{
                      fontSize: "var(--text-sm)",
                      fontWeight: 600,
                      color: data.pnl > 0 ? "var(--color-win)" : data.pnl < 0 ? "var(--color-loss)" : "inherit",
                    }}
                  >
                    {formatPnl(data.pnl)}
                  </div>
                </div>
              );
            })}
          </div>
        </SectionBand>
      )}
    </>
  );
}

// ── Sub-components ────────────────────────────────────────────────────

function BetPickCell({ bet }: { bet: AutoBet }) {
  if (bet.legs.length === 1) {
    const leg = bet.legs[0];
    const matchup = leg.home_team && leg.away_team ? `${leg.away_team} @ ${leg.home_team}` : "";
    const lineStr = leg.line != null ? ` (${leg.line > 0 ? "+" : ""}${leg.line})` : "";
    return (
      <div>
        <div style={{ fontWeight: 600 }}>{leg.selection}{lineStr}</div>
        {matchup && (
          <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)" }}>
            {matchup}
          </div>
        )}
      </div>
    );
  }

  return (
    <div>
      <div style={{ fontWeight: 600 }}>
        {bet.leg_count}-Leg {betTypeLabel(bet.bet_type)}
      </div>
      <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)" }}>
        {bet.legs
          .slice(0, 3)
          .map((l) => l.selection)
          .join(", ")}
        {bet.legs.length > 3 && ` +${bet.legs.length - 3} more`}
      </div>
    </div>
  );
}
