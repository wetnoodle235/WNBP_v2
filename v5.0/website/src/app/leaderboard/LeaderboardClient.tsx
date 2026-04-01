"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { resolveServerApiBase } from "@/lib/api-base";

const API_BASE = typeof window === "undefined"
  ? resolveServerApiBase()
  : "/api/proxy";

interface LeaderboardEntry {
  rank: number;
  sport: string;
  display_name: string;
  total_predictions: number;
  evaluated: number;
  correct: number;
  accuracy: number;
  has_props_model: boolean;
}

interface LeaderboardMeta {
  count: number;
  date_start: string | null;
  date_end: string | null;
  min_evaluated: number;
  generated_at: string;
}

interface LeaderboardPayload {
  success: boolean;
  data: LeaderboardEntry[];
  meta: LeaderboardMeta;
}

const BAR_MAX_WIDTH = 220; // px

function AccuracyBar({ accuracy }: { accuracy: number }) {
  const pct = Math.round(accuracy * 100);
  const tier = accuracy >= 0.65 ? "high" : accuracy >= 0.55 ? "mid" : "low";
  return (
    <div className="lb-accuracy-bar">
      <div className="lb-accuracy-track" aria-hidden="true">
        <div className={`lb-accuracy-fill lb-accuracy-fill--${tier}`} style={{ width: `${pct}%` }} />
      </div>
      <span className={`lb-accuracy-label lb-accuracy-label--${tier}`}>
        {pct}%
      </span>
    </div>
  );
}

function RankMedal({ rank }: { rank: number }) {
  if (rank === 1) return <span className="lb-medal" aria-label="1st place">🥇</span>;
  if (rank === 2) return <span className="lb-medal" aria-label="2nd place">🥈</span>;
  if (rank === 3) return <span className="lb-medal" aria-label="3rd place">🥉</span>;
  return <span className="lb-rank-num">{rank}</span>;
}

export function LeaderboardClient() {
  const [payload, setPayload] = useState<LeaderboardPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [minEvaluated, setMinEvaluated] = useState(1);
  const [dateStart, setDateStart] = useState("");
  const [dateEnd, setDateEnd] = useState("");
  const [sportFilter, setSportFilter] = useState("all");

  const fetchLeaderboard = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("min_evaluated", String(minEvaluated));
      if (dateStart) params.set("date_start", dateStart);
      if (dateEnd) params.set("date_end", dateEnd);
      const res = await fetch(
        `${API_BASE}/v1/predictions/leaderboard?${params.toString()}`,
        { cache: "default" },
      );
      if (!res.ok) {
        const bodyText = await res.text().catch(() => "");
        throw new Error(bodyText || `HTTP ${res.status}`);
      }
      const data: LeaderboardPayload = await res.json();
      if (!data?.success) throw new Error("Leaderboard request was not successful");
      setPayload(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load leaderboard");
    } finally {
      setLoading(false);
    }
  }, [minEvaluated, dateStart, dateEnd]);

  useEffect(() => { fetchLeaderboard(); }, [fetchLeaderboard]);

  const visibleRows = payload?.data.filter((entry) => sportFilter === "all" || entry.sport === sportFilter) ?? [];
  const availableSports = payload?.data.map((entry) => entry.sport).sort((a, b) => a.localeCompare(b)) ?? [];

  return (
    <main className="lb-shell" style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 900, margin: "0 auto" }}>
      {/* Header */}
      <div style={{ marginBottom: "var(--space-6)" }}>
        <h1 style={{ fontSize: "var(--text-2xl)", fontWeight: "var(--fw-bold)", marginBottom: "var(--space-2)" }}>
          Model Leaderboard
        </h1>
        <p style={{ color: "var(--color-text-secondary)", fontSize: "var(--text-sm)" }}>
          Sports ranked by prediction accuracy across all historical evaluated games.
        </p>
      </div>

      {/* Filters */}
      <div
        className="card lb-filters-card"
        style={{ marginBottom: "var(--space-5)", padding: "var(--space-4)" }}
      >
        <div
          className="lb-filters-row"
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: "var(--space-4)",
            alignItems: "flex-end",
          }}
        >
          <div className="lb-filter-group" style={{ display: "flex", flexDirection: "column", gap: "var(--space-1)" }}>
            <label htmlFor="lb-date-start" style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", fontWeight: 600 }}>
              Date start
            </label>
            <input
              id="lb-date-start"
              type="date"
              value={dateStart}
              onChange={(e) => setDateStart(e.target.value)}
              style={{
                background: "var(--color-surface-2)",
                border: "1px solid var(--color-border)",
                borderRadius: "var(--radius-sm)",
                padding: "0.3rem 0.6rem",
                color: "var(--color-text)",
                fontSize: "var(--text-sm)",
              }}
            />
          </div>
          <div className="lb-filter-group" style={{ display: "flex", flexDirection: "column", gap: "var(--space-1)" }}>
            <label htmlFor="lb-date-end" style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", fontWeight: 600 }}>
              Date end
            </label>
            <input
              id="lb-date-end"
              type="date"
              value={dateEnd}
              onChange={(e) => setDateEnd(e.target.value)}
              style={{
                background: "var(--color-surface-2)",
                border: "1px solid var(--color-border)",
                borderRadius: "var(--radius-sm)",
                padding: "0.3rem 0.6rem",
                color: "var(--color-text)",
                fontSize: "var(--text-sm)",
              }}
            />
          </div>
          <div className="lb-filter-group" style={{ display: "flex", flexDirection: "column", gap: "var(--space-1)" }}>
            <label htmlFor="lb-min-eval" style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", fontWeight: 600 }}>
              Min evaluated games
            </label>
            <input
              id="lb-min-eval"
              type="number"
              value={minEvaluated}
              min={1}
              onChange={(e) => setMinEvaluated(Math.max(1, Number(e.target.value)))}
              style={{
                background: "var(--color-surface-2)",
                border: "1px solid var(--color-border)",
                borderRadius: "var(--radius-sm)",
                padding: "0.3rem 0.6rem",
                color: "var(--color-text)",
                fontSize: "var(--text-sm)",
                width: 90,
              }}
            />
          </div>
          <div className="lb-filter-group" style={{ display: "flex", flexDirection: "column", gap: "var(--space-1)" }}>
            <label htmlFor="lb-sport" style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", fontWeight: 600 }}>
              Sport
            </label>
            <select
              id="lb-sport"
              value={sportFilter}
              onChange={(e) => setSportFilter(e.target.value)}
              style={{
                background: "var(--color-surface-2)",
                border: "1px solid var(--color-border)",
                borderRadius: "var(--radius-sm)",
                padding: "0.3rem 0.6rem",
                color: "var(--color-text)",
                fontSize: "var(--text-sm)",
                minWidth: 130,
              }}
            >
              <option value="all">All sports</option>
              {availableSports.map((sport) => (
                <option key={sport} value={sport}>
                  {sport.toUpperCase()}
                </option>
              ))}
            </select>
          </div>
          <button
            onClick={fetchLeaderboard}
            disabled={loading}
            className="btn btn-primary"
            style={{ height: 34, padding: "0 1rem" }}
          >
            {loading ? "Loading…" : "Apply"}
          </button>
          {(dateStart || dateEnd) && (
            <button
              onClick={() => { setDateStart(""); setDateEnd(""); }}
              className="btn btn-secondary"
              style={{ height: 34, padding: "0 0.75rem", fontSize: "var(--text-xs)" }}
            >
              Clear dates
            </button>
          )}
        </div>
      </div>

      {/* Content */}
      {error && (
        <div role="alert" className="stale-banner stale-banner-error" style={{ marginBottom: "var(--space-4)" }}>
          {error}
        </div>
      )}

      {loading && !payload && (
        <div role="status" aria-live="polite" style={{ textAlign: "center", padding: "var(--space-8)", color: "var(--color-text-secondary)" }}>
          Loading leaderboard…
        </div>
      )}

      {payload && (
        <>
          {/* Summary row */}
          <div
            style={{
              display: "flex",
              gap: "var(--space-3)",
              flexWrap: "wrap",
              marginBottom: "var(--space-4)",
              fontSize: "var(--text-sm)",
              color: "var(--color-text-secondary)",
            }}
          >
            <span>
              <strong style={{ color: "var(--color-text)" }}>{visibleRows.length}</strong> sports ranked
            </span>
            {payload.meta.date_start && (
              <span>
                Window:{" "}
                <strong style={{ color: "var(--color-text)" }}>
                  {payload.meta.date_start} → {payload.meta.date_end ?? "today"}
                </strong>
              </span>
            )}
            <span>
              Min evaluated:{" "}
              <strong style={{ color: "var(--color-text)" }}>{payload.meta.min_evaluated}</strong>
            </span>
            <span style={{ marginLeft: "auto", opacity: 0.6, fontSize: "var(--text-xs)" }}>
              Updated {new Date(payload.meta.generated_at).toLocaleTimeString()}
            </span>
          </div>

          {visibleRows.length === 0 ? (
            <div className="card">
              <div className="card-body">
                <p style={{ color: "var(--color-text-secondary)" }}>
                  No sports meet the current filter criteria. Try lowering &quot;Min evaluated games&quot; or widening the date range.
                </p>
              </div>
            </div>
          ) : (
            <div className="card">
              <div className="card-body responsive-table-wrap leaderboard-table-wrap" style={{ padding: 0 }}>
                <table className="data-table responsive-table leaderboard-table" aria-label="Sport accuracy leaderboard" style={{ width: "100%" }}>
                  <thead>
                    <tr>
                      <th scope="col" style={{ width: 50, textAlign: "center", padding: "0.5rem 0.75rem" }}>Rank</th>
                      <th scope="col" style={{ padding: "0.5rem 0.75rem" }}>Sport</th>
                      <th scope="col" style={{ padding: "0.5rem 0.75rem", minWidth: 280 }}>Accuracy</th>
                      <th scope="col" style={{ textAlign: "right", padding: "0.5rem 0.75rem" }}>Evaluated</th>
                      <th scope="col" style={{ textAlign: "right", padding: "0.5rem 0.75rem" }}>Correct</th>
                      <th scope="col" style={{ textAlign: "center", padding: "0.5rem 0.75rem" }}>Props</th>
                      <th scope="col" style={{ textAlign: "center", padding: "0.5rem 0.75rem" }}>Explore</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleRows.map((entry) => {
                      const color = getSportColor(entry.sport);
                      return (
                        <tr key={entry.sport}>
                          <td style={{ textAlign: "center", padding: "0.5rem 0.75rem" }}>
                            <RankMedal rank={entry.rank} />
                          </td>
                          <td style={{ padding: "0.5rem 0.75rem" }}>
                            <span
                              style={{
                                display: "inline-block",
                                background: `${color}22`,
                                color,
                                border: `1px solid ${color}44`,
                                borderRadius: "var(--radius-sm)",
                                padding: "2px 8px",
                                fontSize: "var(--text-xs)",
                                fontWeight: 700,
                                fontFamily: "var(--font-mono)",
                                letterSpacing: "0.04em",
                              }}
                            >
                              {entry.sport.toUpperCase()}
                            </span>
                            <span
                              style={{
                                marginLeft: "0.5rem",
                                color: "var(--color-text-secondary)",
                                fontSize: "var(--text-xs)",
                              }}
                            >
                              {entry.display_name || getDisplayName(entry.sport)}
                            </span>
                          </td>
                          <td style={{ padding: "0.5rem 0.75rem" }}>
                            <AccuracyBar accuracy={entry.accuracy} />
                          </td>
                          <td
                            style={{
                              textAlign: "right",
                              fontVariantNumeric: "tabular-nums",
                              padding: "0.5rem 0.75rem",
                              fontSize: "var(--text-sm)",
                            }}
                          >
                            {entry.evaluated.toLocaleString()}
                          </td>
                          <td
                            style={{
                              textAlign: "right",
                              fontVariantNumeric: "tabular-nums",
                              padding: "0.5rem 0.75rem",
                              fontSize: "var(--text-sm)",
                            }}
                          >
                            {entry.correct.toLocaleString()}
                          </td>
                          <td style={{ textAlign: "center", padding: "0.5rem 0.75rem" }}>
                            {entry.has_props_model ? (
                              <span
                                style={{
                                  background: "rgba(34,197,94,0.12)",
                                  color: "var(--color-win)",
                                  border: "1px solid rgba(34,197,94,0.2)",
                                  borderRadius: "var(--radius-sm)",
                                  padding: "2px 7px",
                                  fontSize: "var(--text-xs)",
                                  fontWeight: 600,
                                }}
                              >
                                Active
                              </span>
                            ) : (
                              <span style={{ color: "var(--color-text-secondary)", fontSize: "var(--text-xs)" }}>—</span>
                            )}
                          </td>
                          <td style={{ textAlign: "center", padding: "0.5rem 0.75rem" }}>
                            <Link
                              href={`/predictions/${entry.sport}`}
                              style={{
                                color: "var(--color-accent, #cc0000)",
                                textDecoration: "none",
                                fontSize: "var(--text-xs)",
                                fontWeight: 600,
                              }}
                            >
                              Predictions
                            </Link>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {!loading && !error && !payload && (
        <div className="card">
          <div className="card-body">
            <p style={{ color: "var(--color-text-secondary)" }}>
              No leaderboard data returned. Try refreshing or adjusting filters.
            </p>
          </div>
        </div>
      )}
    </main>
  );
}
