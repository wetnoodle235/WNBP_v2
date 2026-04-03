"use client";

import { useState, useMemo, useEffect, useCallback, useRef } from "react";
import { SectionBand, Pagination } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { formatOdds, formatGameDateTime } from "@/lib/formatters";
import { resolveServerApiBase } from "@/lib/api-base";
import { LineMovementChart } from "@/components/charts";

const API_BASE = typeof window === "undefined"
  ? resolveServerApiBase()
  : "/api/proxy";

type OddsConnectionState = "connecting" | "connected" | "fallback";

interface OddsItem {
  game_id: string;
  sport: string;
  bookmaker: string;
  home_team?: string;
  away_team?: string;
  h2h_home?: number | null;
  h2h_away?: number | null;
  h2h_draw?: number | null;
  spread_home?: number | null;
  spread_away?: number | null;
  spread_home_line?: number | null;
  spread_away_line?: number | null;
  total_over?: number | null;
  total_under?: number | null;
  total_line?: number | null;
  date?: string | null;
  timestamp?: string | null;
  is_live?: boolean;
  consensus_score?: number | null;
  consensus_warning?: boolean;
}

interface Props {
  initialOdds?: OddsItem[];
  sports: string[];
}

interface GroupedGame {
  game_id: string;
  sport: string;
  home_team: string;
  away_team: string;
  date: string | null;
  bookmakers: OddsItem[];
}

const PER_PAGE = 10;

export function OddsClient({ initialOdds, sports }: Props) {
  const [odds, setOdds] = useState<OddsItem[]>(initialOdds ?? []);
  const [loading, setLoading] = useState(!initialOdds?.length);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [activeSport, setActiveSport] = useState<string | null>(null);
  const [activeBookmaker, setActiveBookmaker] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [connectionState, setConnectionState] = useState<OddsConnectionState>("connecting");
  const streamsRef = useRef<EventSource[]>([]);
  const abortRef = useRef<AbortController | null>(null);

  const fetchOdds = useCallback(async () => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      setFetchError(null);
      const today = new Date().toISOString().slice(0, 10);
      const results = await Promise.allSettled(
        sports.map(async (sport) => {
          const r = await fetch(`${API_BASE}/v1/${sport}/odds?date=${today}`, {
            cache: "no-store",
            signal: controller.signal,
          });
          if (!r.ok) return [];
          const json = await r.json();
          const items = json?.data ?? json ?? [];
          return (items as OddsItem[]).map((o) => ({
            ...o,
            sport: o.sport ?? sport,
            home_team: o.home_team || "",
            away_team: o.away_team || "",
          }));
        }),
      );
      if (controller.signal.aborted) return;
      const all = results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
      const failedCount = results.filter((r) => r.status === "rejected").length;
      setOdds(all);
      if (all.length === 0 && failedCount > 0) {
        setFetchError("Some odds sources are unavailable. Showing cached data.");
      }
    } catch (err) {
      if (err instanceof DOMException && err.name === "AbortError") return;
      setFetchError("Failed to fetch odds. Will retry automatically.");
    } finally {
      setLoading(false);
    }
  }, [sports]);

  const streamedSports = useMemo(() => {
    if (activeSport) {
      return [activeSport];
    }
    return Array.from(new Set(odds.map((item) => item.sport)));
  }, [activeSport, odds]);

  const upsertOdds = useCallback((incoming: OddsItem) => {
    setOdds((current) => {
      const index = current.findIndex(
        (item) => item.sport === incoming.sport
          && String(item.game_id) === String(incoming.game_id)
          && item.bookmaker === incoming.bookmaker,
      );

      if (index === -1) {
        return [incoming, ...current];
      }

      const next = [...current];
      next[index] = {
        ...next[index],
        ...incoming,
        sport: incoming.sport ?? next[index].sport,
      };
      return next;
    });
  }, []);

  useEffect(() => {
    fetchOdds();
    return () => { abortRef.current?.abort(); };
  }, [fetchOdds]);

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(fetchOdds, 30000);
    return () => clearInterval(id);
  }, [autoRefresh, fetchOdds]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    streamsRef.current.forEach((stream) => stream.close());
    streamsRef.current = [];

    if (streamedSports.length === 0) {
      setConnectionState("fallback");
      return;
    }

    setConnectionState("connecting");
    let disposed = false;
    let connectedAny = false;

    const streams = streamedSports.map((sport) => {
      const source = new EventSource(`${API_BASE}/v1/sse/${sport}/odds`);

      source.addEventListener("system", () => {
        connectedAny = true;
        if (!disposed) {
          setConnectionState("connected");
        }
      });

      source.addEventListener("odds_update", (event) => {
        try {
          const payload = JSON.parse((event as MessageEvent<string>).data) as OddsItem;
          upsertOdds({ ...payload, sport: payload.sport ?? sport });
          if (!disposed) {
            setConnectionState("connected");
          }
        } catch {
          // Ignore malformed events and let polling backfill state.
        }
      });

      source.onerror = () => {
        if (!disposed && !connectedAny) {
          setConnectionState("fallback");
        }
      };

      return source;
    });

    streamsRef.current = streams;

    return () => {
      disposed = true;
      streams.forEach((stream) => stream.close());
      streamsRef.current = [];
    };
  }, [streamedSports, upsertOdds]);

  const allBookmakers = useMemo(() => {
    const set = new Set(odds.map((o) => o.bookmaker));
    return Array.from(set).sort();
  }, [odds]);

  const grouped = useMemo(() => {
    let filtered = activeSport
      ? odds.filter((o) => o.sport === activeSport)
      : odds;
    if (activeBookmaker) {
      filtered = filtered.filter((o) => o.bookmaker === activeBookmaker);
    }

    const map = new Map<string, GroupedGame>();
    for (const o of filtered) {
      const key = `${o.sport}-${o.game_id}`;
      if (!map.has(key)) {
        map.set(key, {
          game_id: o.game_id,
          sport: o.sport,
          home_team: o.home_team || "",
          away_team: o.away_team || "",
          date: o.date ?? o.timestamp ?? null,
          bookmakers: [],
        });
      }
      map.get(key)!.bookmakers.push(o);
    }
    return Array.from(map.values());
  }, [odds, activeSport, activeBookmaker]);

  const totalPages = Math.max(1, Math.ceil(grouped.length / PER_PAGE));
  const safeP = Math.min(page, totalPages);
  const pageSlice = grouped.slice((safeP - 1) * PER_PAGE, safeP * PER_PAGE);

  function handleSportChange(sport: string | null) {
    setActiveSport(sport);
    setPage(1);
  }

  function handleBookmakerChange(bk: string | null) {
    setActiveBookmaker(bk);
    setPage(1);
  }

  function oddsColor(val: number | null | undefined): string {
    if (val == null) return "var(--text-muted)";
    return val < 0 ? "var(--color-win, #16a34a)" : "var(--color-loss, #dc2626)";
  }

  function renderOddsValue(value: number | null | undefined): string {
    if (value == null) return "—";
    return formatOdds(value);
  }

  function renderSpread(line: number | null | undefined, val: number | null | undefined): string {
    if (line == null && val == null) return "—";
    const l = line != null ? (line > 0 ? `+${line}` : String(line)) : "";
    const o = val != null ? ` (${formatOdds(val)})` : "";
    return `${l}${o}`;
  }

  function gameTitle(game: GroupedGame): string {
    if (game.home_team && game.away_team) {
      return `${game.away_team} @ ${game.home_team}`;
    }
    // Fallback when team names unavailable
    return `Game ${game.game_id.slice(0, 8)}…`;
  }

  const thStyle: React.CSSProperties = {
    padding: "var(--space-2) var(--space-4)",
    textAlign: "left",
    fontSize: "0.75rem",
    fontWeight: 600,
    textTransform: "uppercase",
    color: "var(--text-muted)",
    borderBottom: "1px solid var(--border)",
  };

  const tdStyle: React.CSSProperties = {
    padding: "var(--space-2) var(--space-4)",
    fontSize: "0.875rem",
    borderBottom: "1px solid var(--border)",
  };

  const pillBase: React.CSSProperties = {
    padding: "var(--space-2) var(--space-4)",
    borderRadius: "var(--radius-full, 9999px)",
    border: "1px solid var(--border)",
    cursor: "pointer",
    fontSize: "var(--text-sm)",
    fontWeight: 600,
  };

  return (
    <main>
      <SectionBand title="Odds">
        {/* Sport filter tabs + refresh controls */}
        <div
          className="odds-sport-tabs"
          role="tablist"
          aria-label="Filter odds by sport"
          style={{
            display: "flex",
            gap: "var(--space-2)",
            flexWrap: "wrap",
            marginBottom: "var(--space-4)",
            alignItems: "center",
          }}
        >
          <button
            role="tab"
            aria-selected={!activeSport}
            onClick={() => handleSportChange(null)}
            style={{
              ...pillBase,
              backgroundColor: !activeSport ? "var(--color-accent, #2563eb)" : "transparent",
              color: !activeSport ? "#fff" : "inherit",
            }}
          >
            All Sports
          </button>
          {sports.map((sport) => {
            const isActive = activeSport === sport;
            const color = getSportColor(sport);
            return (
              <button
                key={sport}
                role="tab"
                aria-selected={isActive}
                onClick={() => handleSportChange(sport)}
                style={{
                  ...pillBase,
                  border: `1px solid ${color}`,
                  backgroundColor: isActive ? color : "transparent",
                  color: isActive ? "#fff" : "inherit",
                }}
              >
                {getDisplayName(sport)}
              </button>
            );
          })}
        </div>

        {/* Refresh controls */}
        <div style={{ display: "flex", gap: "var(--space-2)", marginBottom: "var(--space-4)" }}>
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            style={{ ...pillBase, fontSize: "0.75rem", padding: "4px 12px" }}
          >
            Auto-refresh: {autoRefresh ? "On" : "Off"}
          </button>
          <button
            onClick={() => { setLoading(true); fetchOdds(); }}
            disabled={loading}
            style={{ ...pillBase, fontSize: "0.75rem", padding: "4px 12px" }}
          >
            {loading ? "Loading…" : "↻ Refresh"}
          </button>
        </div>

        <div
          role="status"
          aria-live="polite"
          style={{
            marginBottom: "var(--space-4)",
            fontSize: "0.75rem",
            color: "var(--text-muted)",
            fontStyle: "italic",
          }}
        >
          {connectionState === "connected"
            ? `Live odds stream active${streamedSports.length ? ` for ${streamedSports.length} sport${streamedSports.length === 1 ? "" : "s"}` : ""}`
            : connectionState === "connecting"
              ? "Connecting odds stream..."
              : "Polling odds every 30s"}
        </div>

        {/* Error banner */}
        {fetchError && (
          <div
            role="alert"
            aria-live="polite"
            style={{
              padding: "var(--space-3) var(--space-4)",
              marginBottom: "var(--space-4)",
              borderRadius: "var(--radius-md, 8px)",
              backgroundColor: "rgba(245, 158, 11, 0.1)",
              border: "1px solid rgba(245, 158, 11, 0.3)",
              color: "var(--text-secondary)",
              fontSize: "0.8125rem",
              display: "flex",
              alignItems: "center",
              gap: "var(--space-2)",
            }}
          >
            <span aria-hidden="true">⚠️</span> {fetchError}
          </div>
        )}

        {/* Bookmaker filter tabs */}
        {allBookmakers.length > 1 && (
          <div
            className="odds-bookmaker-tabs"
            role="tablist"
            aria-label="Filter by bookmaker"
            style={{
              display: "flex",
              gap: "var(--space-2)",
              flexWrap: "wrap",
              marginBottom: "var(--space-4)",
            }}
          >
            <button
              role="tab"
              aria-selected={!activeBookmaker}
              onClick={() => handleBookmakerChange(null)}
              style={{
                ...pillBase,
                fontSize: "0.75rem",
                padding: "4px 12px",
                backgroundColor: !activeBookmaker ? "var(--surface-elevated)" : "transparent",
                fontWeight: !activeBookmaker ? 700 : 500,
              }}
            >
              All Books
            </button>
            {allBookmakers.map((bk) => (
              <button
                key={bk}
                role="tab"
                aria-selected={activeBookmaker === bk}
                onClick={() => handleBookmakerChange(bk)}
                style={{
                  ...pillBase,
                  fontSize: "0.75rem",
                  padding: "4px 12px",
                  backgroundColor: activeBookmaker === bk ? "var(--surface-elevated)" : "transparent",
                  fontWeight: activeBookmaker === bk ? 700 : 500,
                }}
              >
                {bk}
              </button>
            ))}
          </div>
        )}

        {loading ? (
          <div role="status" aria-live="polite" className="card" style={{ padding: "var(--space-8)", textAlign: "center", color: "var(--text-muted)" }}>
            Loading odds from sportsbooks…
          </div>
        ) : (
        <>
        {/* Count */}
        <div
          aria-live="polite"
          style={{
            fontSize: "0.875rem",
            color: "var(--text-muted)",
            marginBottom: "var(--space-4)",
          }}
        >
          {grouped.length} game{grouped.length !== 1 ? "s" : ""} with odds
        </div>

        {grouped.length === 0 ? (
          <div
            className="card"
            style={{
              padding: "var(--space-8)",
              textAlign: "center",
              color: "var(--text-muted)",
            }}
          >
            No odds available{activeSport ? ` for ${getDisplayName(activeSport)}` : ""}.
          </div>
        ) : (
          <>
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-4)" }}>
              {pageSlice.map((game) => {
                const sportColor = getSportColor(game.sport);
                return (
                  <div key={`${game.sport}-${game.game_id}`} className="card">
                    <div className="card-body" style={{ padding: "var(--space-4)" }}>
                      {/* Game header */}
                      <div
                        style={{
                          display: "flex",
                          justifyContent: "space-between",
                          alignItems: "center",
                          marginBottom: "var(--space-4)",
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", gap: "var(--space-2)" }}>
                          <span
                            style={{
                              display: "inline-block",
                              padding: "2px 8px",
                              borderRadius: "var(--radius-full, 9999px)",
                              backgroundColor: sportColor,
                              color: "#fff",
                              fontSize: "0.7rem",
                              fontWeight: 700,
                              textTransform: "uppercase",
                            }}
                          >
                            {getDisplayName(game.sport)}
                          </span>
                          <span style={{ fontWeight: 700, fontSize: "1rem" }}>
                            {gameTitle(game)}
                          </span>
                          {game.bookmakers.some((b) => b.is_live) && (
                            <span
                              style={{
                                background: "var(--color-loss, #dc2626)",
                                color: "#fff",
                                fontSize: "0.6rem",
                                fontWeight: 700,
                                padding: "1px 6px",
                                borderRadius: "var(--radius-full, 9999px)",
                              }}
                            >
                              LIVE
                            </span>
                          )}
                          {(() => {
                            const cs = game.bookmakers.find((b) => b.consensus_score != null)?.consensus_score;
                            const warn = game.bookmakers.some((b) => b.consensus_warning);
                            if (cs == null) return null;
                            return (
                              <span
                                title={`Bookmaker consensus: ${cs}/100${warn ? " — books disagree significantly" : ""}`}
                                style={{
                                  background: warn ? "rgba(217,119,6,0.15)" : "rgba(99,102,241,0.15)",
                                  color: warn ? "#d97706" : "#6366f1",
                                  border: `1px solid ${warn ? "#d97706" : "#6366f1"}`,
                                  fontSize: "0.6rem",
                                  fontWeight: 700,
                                  padding: "1px 6px",
                                  borderRadius: "var(--radius-full, 9999px)",
                                  cursor: "help",
                                }}
                              >
                                {warn ? "⚠" : "✓"} Consensus {cs}
                              </span>
                            );
                          })()}
                        </div>
                        {game.date && (
                          <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>
                            {formatGameDateTime(game.date)}
                          </span>
                        )}
                      </div>

                      {/* Odds table */}
                      <div className="responsive-table-wrap">
                        <table className="responsive-table odds-table" aria-label="Sportsbook odds comparison" style={{ borderCollapse: "collapse" }}>
                          <caption className="sr-only">{`Odds for ${game.home_team} vs ${game.away_team}`}</caption>
                          <thead>
                            <tr>
                              <th scope="col" style={thStyle}>Bookmaker</th>
                              <th scope="col" style={thStyle}>ML Home</th>
                              <th scope="col" style={thStyle}>ML Away</th>
                              <th scope="col" style={thStyle}>Spread Home</th>
                              <th scope="col" style={thStyle}>Spread Away</th>
                              <th scope="col" style={thStyle}>Total</th>
                            </tr>
                          </thead>
                          <tbody>
                            {game.bookmakers.map((bk, i) => (
                              <tr key={`${bk.bookmaker}-${i}`}>
                                <td style={{ ...tdStyle, fontWeight: 600 }}>{bk.bookmaker}</td>
                                <td style={{ ...tdStyle, color: oddsColor(bk.h2h_home), fontWeight: 600 }}>
                                  {renderOddsValue(bk.h2h_home)}
                                </td>
                                <td style={{ ...tdStyle, color: oddsColor(bk.h2h_away), fontWeight: 600 }}>
                                  {renderOddsValue(bk.h2h_away)}
                                </td>
                                <td style={{ ...tdStyle, fontSize: "0.8rem" }}>
                                  {renderSpread(bk.spread_home_line, bk.spread_home)}
                                </td>
                                <td style={{ ...tdStyle, fontSize: "0.8rem" }}>
                                  {renderSpread(bk.spread_away_line, bk.spread_away)}
                                </td>
                                <td style={{ ...tdStyle, fontSize: "0.8rem" }}>
                                  {bk.total_line != null ? (
                                    <span>
                                      <span style={{ fontWeight: 600 }}>{bk.total_line}</span>{" "}
                                      <span style={{ color: "var(--color-win, #16a34a)" }}>
                                        O{bk.total_over != null ? ` ${formatOdds(bk.total_over)}` : ""}
                                      </span>{" "}
                                      /{" "}
                                      <span style={{ color: "var(--color-loss, #dc2626)" }}>
                                        U{bk.total_under != null ? ` ${formatOdds(bk.total_under)}` : ""}
                                      </span>
                                    </span>
                                  ) : (
                                    "—"
                                  )}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                    {/* Line movement chart for this game */}
                    <div style={{ padding: "0 var(--space-4) var(--space-4)" }}>
                      <LineMovementChart
                        sport={game.sport}
                        gameId={game.game_id}
                        lineType="h2h_home"
                        height={180}
                      />
                    </div>
                  </div>
                );
              })}
            </div>

            <Pagination page={safeP} totalPages={totalPages} onPageChange={setPage} />
          </>
        )}
        </>
        )}
      </SectionBand>
    </main>
  );
}
