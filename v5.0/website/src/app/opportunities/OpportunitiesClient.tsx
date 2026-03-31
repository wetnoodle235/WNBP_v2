"use client";

import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import Link from "next/link";
import { SectionBand, Pagination } from "@/components/ui";
import { getDisplayName, getSportColor, getSportIcon } from "@/lib/sports-config";
import { formatPropType } from "@/lib/formatters";

const API_BASE =
  typeof window === "undefined"
    ? (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000")
    : "/api/proxy";

const TIER_ORDER: Record<string, number> = { high: 0, medium: 1, low: 2 };
const PER_PAGE = 25;
const AUTO_REFRESH_MS = 90_000;

interface Market {
  prop_type: string;
  line?: number | null;
  market_type?: string;
}

interface OpportunityRow {
  sport: string;
  game_id: string;
  date?: string | null;
  status?: string | null;
  home_team?: string | null;
  away_team?: string | null;
  start_time?: string | null;
  recommendation_score: number;
  recommendation_tier: "high" | "medium" | "low";
  available_markets?: Market[];
  live_context?: {
    live_home_wp?: number | null;
    live_away_wp?: number | null;
    momentum?: string | null;
    momentum_score?: number | null;
    time_remaining?: string | null;
  };
}

interface PropOpportunity {
  sport: string;
  game_id: string;
  status?: string | null;
  home_team?: string | null;
  away_team?: string | null;
  start_time?: string | null;
  recommendation_score: number;
  recommendation_tier: "high" | "medium" | "low";
  prop_type: string;
  line?: number | null;
  market_type?: string;
  live_context?: OpportunityRow["live_context"];
}

type SortKey = "recommendation_score" | "sport" | "recommendation_tier" | "prop_type";
type SortDir = "asc" | "desc";
const FREE_PREVIEW_LIMIT = 3;

function getFriendlyProp(raw: string): { label: string; projectedText: string; playerLabel: string } {
  const overUnder = raw.match(/^(.*)_(over|under)_(-?\d+(?:\.\d+)?)$/i);
  const line = overUnder ? Number(overUnder[3]) : null;
  const projectedText = line !== null && Number.isFinite(line)
    ? line.toFixed(Number.isInteger(line) ? 0 : 1)
    : "—";
  const base = overUnder ? overUnder[1]!.toLowerCase() : "";
  return {
    label: formatPropType(raw),
    projectedText,
    playerLabel: base.startsWith("pitchr_") ? "Starting Pitcher (TBD)" : "Player (Not Provided)",
  };
}

async function fetchAllOpportunities(
  minScore: number,
  sportFilter: string | null,
  signal?: AbortSignal,
): Promise<{ rows: OpportunityRow[]; trainedSports: string[] }> {
  try {
    const params = new URLSearchParams({ limit: "500", min_score: String(minScore) });
    if (sportFilter) params.set("sports", sportFilter);
    const res = await fetch(`${API_BASE}/v1/predictions/opportunities?${params}`, {
      cache: "default",
      signal,
    });
    if (!res.ok) return { rows: [], trainedSports: [] };
    const json = await res.json();
    return {
      rows: (json?.data ?? []) as OpportunityRow[],
      trainedSports: (json?.meta?.trained_sports ?? []) as string[],
    };
  } catch {
    return { rows: [], trainedSports: [] };
  }
}

function TierBadge({ tier }: { tier: string }) {
  const colors: Record<string, string> = {
    high: "var(--color-win, #16a34a)",
    medium: "var(--color-accent, #d97706)",
    low: "var(--color-text-muted, #6b7280)",
  };
  return (
    <span
      style={{
        fontSize: "0.7rem",
        fontWeight: 700,
        textTransform: "uppercase",
        padding: "0.15rem 0.45rem",
        borderRadius: "4px",
        border: `1px solid ${colors[tier] ?? "#ccc"}`,
        color: colors[tier] ?? "var(--color-text-muted)",
        whiteSpace: "nowrap",
      }}
    >
      {tier}
    </span>
  );
}

function ScoreBar({ score }: { score: number }) {
  const pct = Math.round(score * 100);
  const color =
    score >= 0.75 ? "var(--color-win, #16a34a)" : score >= 0.6 ? "#d97706" : "var(--color-text-muted)";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
      <div
        style={{
          flex: 1,
          maxWidth: "80px",
          height: "6px",
          borderRadius: "3px",
          background: "var(--color-border, #e5e7eb)",
          overflow: "hidden",
        }}
      >
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: "3px" }} />
      </div>
      <span style={{ fontSize: "0.8rem", fontVariantNumeric: "tabular-nums", color, fontWeight: 600, minWidth: "2.8rem" }}>
        {pct}%
      </span>
    </div>
  );
}

export function OpportunitiesClient({ hasPremium }: { hasPremium: boolean }) {
  const [rows, setRows] = useState<OpportunityRow[]>([]);
  const [trainedSports, setTrainedSports] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [activeSport, setActiveSport] = useState<string | null>(null);
  const [tierFilter, setTierFilter] = useState<string>("all");
  const [propFilter, setPropFilter] = useState<string>("all");
  const [minScore, setMinScore] = useState(0);
  const [sortKey, setSortKey] = useState<SortKey>("recommendation_score");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [page, setPage] = useState(1);

  const abortRef = useRef<AbortController | null>(null);

  const loadData = useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setLoading(true);
    try {
      const { rows: fetched, trainedSports: ts } = await fetchAllOpportunities(0, activeSport, ac.signal);
      if (ac.signal.aborted) return;
      setRows(fetched);
      setTrainedSports(ts);
      setLastUpdated(new Date());
    } finally {
      if (!ac.signal.aborted) setLoading(false);
    }
  }, [activeSport]);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, AUTO_REFRESH_MS);
    return () => {
      clearInterval(interval);
      abortRef.current?.abort();
    };
  }, [loadData]);

  const propRows = useMemo<PropOpportunity[]>(() => {
    const expanded: PropOpportunity[] = [];
    rows.forEach((row) => {
      const markets = row.available_markets?.length
        ? row.available_markets
        : [{ prop_type: "market_unavailable", line: null, market_type: "unknown" }];
      markets.forEach((market) => {
        expanded.push({
          sport: row.sport,
          game_id: row.game_id,
          status: row.status,
          home_team: row.home_team,
          away_team: row.away_team,
          start_time: row.start_time,
          recommendation_score: row.recommendation_score,
          recommendation_tier: row.recommendation_tier,
          prop_type: market.prop_type,
          line: market.line,
          market_type: market.market_type,
          live_context: row.live_context,
        });
      });
    });
    return expanded;
  }, [rows]);

  const propTypes = useMemo(() => {
    return [...new Set(propRows.map((r) => r.prop_type))]
      .filter((v) => v && v !== "market_unavailable")
      .sort((a, b) => a.localeCompare(b));
  }, [propRows]);

  const filtered = useMemo(() => {
    let r = propRows;
    if (minScore > 0) r = r.filter((x) => x.recommendation_score >= minScore / 100);
    if (tierFilter !== "all") r = r.filter((x) => x.recommendation_tier === tierFilter);
    if (propFilter !== "all") r = r.filter((x) => x.prop_type === propFilter);
    return r;
  }, [propRows, minScore, tierFilter, propFilter]);

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      if (sortKey === "recommendation_score") {
        cmp = (a.recommendation_score ?? 0) - (b.recommendation_score ?? 0);
      } else if (sortKey === "sport") {
        cmp = a.sport.localeCompare(b.sport);
      } else if (sortKey === "recommendation_tier") {
        cmp = (TIER_ORDER[a.recommendation_tier] ?? 3) - (TIER_ORDER[b.recommendation_tier] ?? 3);
      } else if (sortKey === "prop_type") {
        cmp = a.prop_type.localeCompare(b.prop_type);
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [filtered, sortKey, sortDir]);

  const gatedRows = useMemo(() => {
    return hasPremium ? sorted : sorted.slice(0, FREE_PREVIEW_LIMIT);
  }, [hasPremium, sorted]);

  const totalPages = Math.max(1, Math.ceil(gatedRows.length / PER_PAGE));
  const safePage = Math.min(page, totalPages);
  const pageRows = gatedRows.slice((safePage - 1) * PER_PAGE, safePage * PER_PAGE);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
    setPage(1);
  }

  function SortHeader({ k, children }: { k: SortKey; children: React.ReactNode }) {
    const active = sortKey === k;
    const arrow = active ? (sortDir === "desc" ? " ▼" : " ▲") : "";
    return (
      <th
        scope="col"
        role="columnheader"
        aria-sort={active ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
        tabIndex={0}
        style={{
          padding: "0.45rem 0.5rem",
          textAlign: "left",
          cursor: "pointer",
          userSelect: "none",
          whiteSpace: "nowrap",
          fontWeight: active ? 700 : 500,
          color: active ? "var(--color-accent, #cc0000)" : undefined,
        }}
        onClick={() => {
          toggleSort(k);
          setPage(1);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            toggleSort(k);
            setPage(1);
          }
        }}
      >
        {children}
        {arrow}
      </th>
    );
  }

  return (
    <main>
      <SectionBand title="Player Prediction Opportunities">
                {!hasPremium && (
                  <div className="stale-banner stale-banner-info" style={{ marginBottom: "var(--space-3)" }}>
                    <span className="stale-banner-icon" aria-hidden="true">🔒</span>
                    <span className="stale-banner-text">
                      Free tier shows {FREE_PREVIEW_LIMIT} opportunity rows. Upgrade for full market list and confidence details.
                    </span>
                  </div>
                )}

                <div style={{ fontSize: "0.75rem", color: "var(--color-text-muted)", marginBottom: "0.75rem" }}>
                  Current feed is market-level and does not yet include individual player names.
                </div>

        <div style={{ marginBottom: "1rem" }}>
          <p style={{ fontSize: "0.9rem", color: "var(--color-text-muted)", margin: 0 }}>
            Player-prop recommendation rows sorted by confidence score and live momentum context.
          </p>
        </div>

        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: "0.75rem",
            alignItems: "center",
            marginBottom: "1.25rem",
          }}
        >
          <div style={{ display: "flex", gap: "0.35rem", flexWrap: "wrap" }}>
            <button
              className={`sport-tab${activeSport === null ? " sport-tab--active" : ""}`}
              onClick={() => {
                setActiveSport(null);
                setPage(1);
              }}
            >
              All
            </button>
            {trainedSports.map((s) => (
              <button
                key={s}
                className={`sport-tab${activeSport === s ? " sport-tab--active" : ""}`}
                style={activeSport === s ? { borderColor: getSportColor(s), color: getSportColor(s) } : {}}
                onClick={() => {
                  setActiveSport(s);
                  setPage(1);
                }}
              >
                {getSportIcon(s)} {getDisplayName(s)}
              </button>
            ))}
          </div>

          <select
            value={tierFilter}
            onChange={(e) => {
              setTierFilter(e.target.value);
              setPage(1);
            }}
            style={{
              fontSize: "0.82rem",
              padding: "0.3rem 0.6rem",
              borderRadius: "6px",
              border: "1px solid var(--color-border)",
              background: "var(--color-surface, #fff)",
              cursor: "pointer",
            }}
            aria-label="Filter by tier"
          >
            <option value="all">All tiers</option>
            <option value="high">High</option>
            <option value="medium">Medium</option>
            <option value="low">Low</option>
          </select>

          <select
            value={propFilter}
            onChange={(e) => {
              setPropFilter(e.target.value);
              setPage(1);
            }}
            style={{
              fontSize: "0.82rem",
              padding: "0.3rem 0.6rem",
              borderRadius: "6px",
              border: "1px solid var(--color-border)",
              background: "var(--color-surface, #fff)",
              cursor: "pointer",
              minWidth: 210,
            }}
            aria-label="Filter by prop type"
          >
            <option value="all">All prop markets</option>
            {propTypes.map((prop) => (
              <option key={prop} value={prop}>
                {prop}
              </option>
            ))}
          </select>

          <label style={{ display: "flex", alignItems: "center", gap: "0.4rem", fontSize: "0.82rem" }}>
            Min score:
            <input
              type="range"
              min={0}
              max={90}
              step={5}
              value={minScore}
              onChange={(e) => {
                setMinScore(Number(e.target.value));
                setPage(1);
              }}
              aria-valuetext={`${minScore}%`}
              style={{ width: "90px" }}
            />
            <span style={{ fontVariantNumeric: "tabular-nums", minWidth: "2rem" }}>{minScore}%</span>
          </label>

          <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: "0.5rem" }}>
            {lastUpdated && !loading && (
              <span style={{ fontSize: "0.72rem", color: "var(--color-text-muted)" }}>
                Updated {lastUpdated.toLocaleTimeString()}
              </span>
            )}
            <button
              onClick={() => loadData()}
              disabled={loading}
              style={{
                fontSize: "0.82rem",
                padding: "0.3rem 0.7rem",
                borderRadius: "6px",
                border: "1px solid var(--color-border)",
                background: "transparent",
                cursor: "pointer",
                opacity: loading ? 0.6 : 1,
              }}
              aria-label="Refresh"
            >
              {loading ? "Loading..." : "Refresh"}
            </button>
          </div>
        </div>

        <div style={{ fontSize: "0.8rem", color: "var(--color-text-muted)", marginBottom: "0.75rem" }}>
          {loading
            ? "Fetching player prop opportunities..."
            : `${gatedRows.length} prop opportunit${gatedRows.length === 1 ? "y" : "ies"} shown`}
        </div>

        {!loading && gatedRows.length === 0 ? (
          <div
            style={{
              padding: "2.5rem 1rem",
              textAlign: "center",
              color: "var(--color-text-muted)",
              border: "1px dashed var(--color-border)",
              borderRadius: "8px",
            }}
          >
            <div style={{ fontSize: "2rem", marginBottom: "0.5rem" }}>🔍</div>
            <p style={{ margin: 0 }}>
              No player prop opportunities match the current filters.
              {rows.length === 0
                ? " No trained models found for the selected sport(s)."
                : " Try widening the filters."}
            </p>
          </div>
        ) : (
          <>
            <div className="responsive-table-wrap opportunities-table-wrap">
              <table
                className="responsive-table opportunities-table"
                aria-label="Player prop opportunities and recommendations"
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: "0.85rem",
                }}
              >
                <thead>
                  <tr style={{ borderBottom: "2px solid var(--color-border)" }}>
                    <SortHeader k="sport">Sport</SortHeader>
                    <th style={{ padding: "0.45rem 0.5rem" }}>Matchup</th>
                    <th style={{ padding: "0.45rem 0.5rem" }}>Player</th>
                    <th style={{ padding: "0.45rem 0.5rem" }}>Status</th>
                    <SortHeader k="recommendation_score">Score</SortHeader>
                    <SortHeader k="recommendation_tier">Tier</SortHeader>
                    <SortHeader k="prop_type">Prop Market</SortHeader>
                    <th style={{ padding: "0.45rem 0.5rem" }}>Projected</th>
                    <th style={{ padding: "0.45rem 0.5rem" }}>Live</th>
                    <th style={{ padding: "0.45rem 0.5rem" }}>Detail</th>
                  </tr>
                </thead>
                <tbody>
                  {pageRows.map((row, i) => {
                    const friendly = getFriendlyProp(row.prop_type);
                    const matchup = row.home_team && row.away_team ? `${row.away_team} @ ${row.home_team}` : row.game_id;
                    const liveCtx = row.live_context;
                    const hasLive = liveCtx && (liveCtx.live_home_wp != null || liveCtx.time_remaining);
                    return (
                      <tr
                        key={`${row.sport}-${row.game_id}-${row.prop_type}-${i}`}
                        style={{ borderBottom: "1px solid var(--color-border)", transition: "background 0.1s" }}
                        onMouseEnter={(e) =>
                          ((e.currentTarget as HTMLTableRowElement).style.background =
                            "var(--color-surface-hover, rgba(0,0,0,0.03))")
                        }
                        onMouseLeave={(e) => ((e.currentTarget as HTMLTableRowElement).style.background = "")}
                      >
                        <td style={{ padding: "0.5rem 0.5rem", whiteSpace: "nowrap" }}>
                          <span
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: "0.3rem",
                              fontSize: "0.78rem",
                              fontWeight: 600,
                              color: getSportColor(row.sport),
                            }}
                          >
                            {getSportIcon(row.sport)} {getDisplayName(row.sport)}
                          </span>
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem" }}>
                          <span style={{ fontWeight: 500 }}>{matchup}</span>
                          {row.start_time && (
                            <span
                              style={{
                                display: "block",
                                fontSize: "0.72rem",
                                color: "var(--color-text-muted)",
                              }}
                            >
                              {row.start_time}
                            </span>
                          )}
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem", fontSize: "0.76rem", color: "var(--color-text-muted)" }}>
                          {friendly.playerLabel}
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem", fontSize: "0.78rem", color: "var(--color-text-muted)" }}>
                          {row.status ?? "-"}
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem" }}>
                          {hasPremium ? <ScoreBar score={row.recommendation_score} /> : <span>🔒</span>}
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem" }}>
                          {hasPremium ? <TierBadge tier={row.recommendation_tier} /> : <span>🔒</span>}
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem" }}>
                          <div style={{ fontWeight: 600, fontSize: "0.8rem" }}>{friendly.label}</div>
                          <div style={{ fontSize: "0.72rem", color: "var(--color-text-muted)" }}>
                            {row.line != null ? `Line ${row.line}` : "No line"}
                            {row.market_type ? ` · ${row.market_type}` : ""}
                          </div>
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem", fontWeight: 600, fontSize: "0.78rem" }}>
                          {friendly.projectedText}
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem", fontSize: "0.78rem" }}>
                          {hasLive ? (
                            <span style={{ color: "var(--color-win, #16a34a)", fontWeight: 600 }}>
                              {liveCtx?.momentum
                                ? liveCtx.momentum.charAt(0).toUpperCase() + liveCtx.momentum.slice(1)
                                : "Live"}
                              {liveCtx?.time_remaining ? ` · ${liveCtx.time_remaining}` : ""}
                            </span>
                          ) : (
                            <span style={{ color: "var(--color-text-muted)" }}>-</span>
                          )}
                        </td>

                        <td style={{ padding: "0.5rem 0.5rem" }}>
                          <Link
                            href={`/games/${row.sport}/${row.game_id}`}
                            style={{
                              fontSize: "0.78rem",
                              color: "var(--color-accent, #cc0000)",
                              textDecoration: "none",
                              fontWeight: 500,
                            }}
                          >
                            View →
                          </Link>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {totalPages > 1 && (
              <div style={{ marginTop: "1rem" }}>
                <Pagination page={safePage} totalPages={totalPages} onPageChange={setPage} />
              </div>
            )}
          </>
        )}

        {!loading && rows.length > 0 && (
          <p style={{ fontSize: "0.75rem", color: "var(--color-text-muted)", marginTop: "1.25rem" }}>
            Prop models currently trained for{" "}
            <strong>
              {(trainedSports.length ? trainedSports : [...new Set(rows.map((r) => r.sport))].sort())
                .map(getDisplayName)
                .join(", ")}
            </strong>
            .
          </p>
        )}
      </SectionBand>
    </main>
  );
}
