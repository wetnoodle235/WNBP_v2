"use client";

import { useState, useMemo, useEffect, useCallback, useRef } from "react";
import { SectionBand, Pagination } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { formatGameTime, formatOdds, formatPropType, formatSpread, formatTotal } from "@/lib/formatters";
import { resolveServerApiBase } from "@/lib/api-base";

const API_BASE = typeof window === "undefined"
  ? resolveServerApiBase()
  : "/api/proxy";
const REFRESH_INTERVAL_MS = 30_000;

type LiveConnectionState = "connecting" | "connected" | "fallback";

interface GameItem {
  id: string;
  sport: string;
  home_team: string;
  away_team: string;
  home_team_id?: string | null;
  away_team_id?: string | null;
  home_score?: number | null;
  away_score?: number | null;
  status: string;
  date: string;
  start_time?: string | null;
  period?: string | null;
  venue?: string | null;
  broadcast?: string | null;
  broadcast_url?: string | null;
}

interface LivePredictionItem {
  game_id: string;
  home_team?: string;
  away_team?: string;
  home_score?: number | null;
  away_score?: number | null;
  period?: string | null;
  time_remaining?: string | null;
  pre_game_home_wp?: number | null;
  live_home_wp?: number | null;
  live_away_wp?: number | null;
  predicted_final_home?: number | null;
  predicted_final_away?: number | null;
  momentum?: string | null;
  momentum_score?: number | null;
  key_factors?: string[];
}

interface PropOpportunityItem {
  sport: string;
  game_id: string;
  home_team?: string;
  away_team?: string;
  status?: string;
  recommendation_score: number;
  recommendation_tier: "high" | "medium" | "low";
  available_markets?: Array<{ prop_type: string; line?: number | null; market_type?: string }>;
}

interface LiveOddsItem {
  game_id: string;
  sport: string;
  bookmaker?: string | null;
  home_team?: string | null;
  away_team?: string | null;
  h2h_home?: number | null;
  h2h_away?: number | null;
  spread_home?: number | null;
  spread_away?: number | null;
  spread_home_line?: number | null;
  spread_away_line?: number | null;
  total_line?: number | null;
  total_over?: number | null;
  total_under?: number | null;
  is_live?: boolean;
}

interface Props {
  games: GameItem[];
  sports: string[];
}

const PER_PAGE = 12;

const BROADCAST_LINKS: Record<string, string> = {
  "abc": "https://abc.com/watch-live",
  "apple tv": "https://tv.apple.com/",
  "cbs": "https://www.paramountplus.com/live-tv/",
  "cbs sports": "https://www.cbssports.com/live/",
  "cbs sports network": "https://www.cbssports.com/live/",
  "espn": "https://www.espn.com/watch/",
  "espn+": "https://www.espn.com/watch/",
  "fanatiz": "https://www.fanatiz.com/",
  "fox": "https://www.foxsports.com/live",
  "fox sports": "https://www.foxsports.com/live",
  "fubo": "https://www.fubo.tv/",
  "fubotv": "https://www.fubo.tv/",
  "golf channel": "https://www.nbcsports.com/golf",
  "golf chnl": "https://www.nbcsports.com/golf",
  "max": "https://www.max.com/sports",
  "mlb.tv": "https://www.mlb.com/live-stream-games",
  "nba league pass": "https://www.nba.com/watch/league-pass-stream",
  "nbc": "https://www.peacocktv.com/sports",
  "nbc sports": "https://www.peacocktv.com/sports",
  "paramount+": "https://www.paramountplus.com/",
  "peacock": "https://www.peacocktv.com/sports",
  "prime video": "https://www.primevideo.com/",
  "tnt": "https://www.max.com/sports",
  "tnt sports": "https://www.max.com/sports",
};

function splitBroadcasts(raw: string | null | undefined): string[] {
  if (!raw) return [];
  return raw
    .split(/[,/|]+/)
    .map((token) => token.trim())
    .filter(Boolean);
}

function normalizeBroadcastToken(token: string): string {
  return token.toLowerCase().replace(/\s+/g, " ").trim();
}

function providerLink(token: string): string {
  const normalized = normalizeBroadcastToken(token);
  if (BROADCAST_LINKS[normalized]) {
    return BROADCAST_LINKS[normalized];
  }
  return `https://www.google.com/search?q=${encodeURIComponent(`${token} live stream`)}`;
}

function normalizeWatchUrl(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
    return trimmed;
  }
  return `https://${trimmed}`;
}

function isLive(status: string): boolean {
  const s = status.toLowerCase();
  return s.includes("in") || s.includes("live") || s.includes("progress");
}

function getTeamLogoUrl(sport: string, teamId: string | null | undefined): string | null {
  if (!teamId) return null;
  const sportMap: Record<string, string> = {
    nba: "nba",
    mlb: "mlb",
    nfl: "nfl",
    nhl: "nhl",
    wnba: "wnba",
    ncaab: "ncaa",
    ncaaf: "ncaa",
    ncaaw: "ncaa",
    epl: "soccer",
    laliga: "soccer",
    bundesliga: "soccer",
    seriea: "soccer",
    ligue1: "soccer",
    mls: "soccer",
    ucl: "soccer",
    nwsl: "soccer",
    liga_mx: "soccer",
  };
  const espnSport = sportMap[sport];
  if (!espnSport) return null;
  return `https://a.espncdn.com/i/teamlogos/${espnSport}/500/${teamId}.png`;
}

async function fetchLiveGames(sportsList: string[], signal?: AbortSignal): Promise<GameItem[]> {
  const today = new Date().toISOString().slice(0, 10);
  const results = await Promise.allSettled(
    sportsList.map(async (sport) => {
      const res = await fetch(`${API_BASE}/v1/${sport}/games?date=${today}&limit=100`, { signal });
      if (!res.ok) return [];
      const json = await res.json();
      return (json?.data ?? []).map((g: GameItem) => ({
        ...g,
        sport: g.sport ?? sport,
      }));
    }),
  );
  return results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
}

async function fetchLivePredictionSnapshots(sportsList: string[], signal?: AbortSignal): Promise<Record<string, LivePredictionItem>> {
  const results = await Promise.allSettled(
    sportsList.map(async (sport) => {
      const res = await fetch(`${API_BASE}/v1/${sport}/live-predictions`, { cache: "no-store", signal });
      if (!res.ok) return [] as LivePredictionItem[];
      const json = await res.json();
      return (json?.data?.games ?? []) as LivePredictionItem[];
    }),
  );

  const entries = results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
  const merged: Record<string, LivePredictionItem> = {};
  for (const prediction of entries) {
    merged[String(prediction.game_id)] = prediction;
  }
  return merged;
}

async function fetchPropOpportunities(sportsList: string[], activeSport: string | null, signal?: AbortSignal): Promise<PropOpportunityItem[]> {
  const targetSports = activeSport ? [activeSport] : sportsList;
  const results = await Promise.allSettled(
    targetSports.map(async (sport) => {
      const res = await fetch(`${API_BASE}/v1/predictions/${sport}/player-props/opportunities?limit=6&min_score=0.6`, {
        cache: "no-store",
        signal,
      });
      if (!res.ok) return [] as PropOpportunityItem[];
      const json = await res.json();
      return (json?.data ?? []) as PropOpportunityItem[];
    }),
  );

  const merged = results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
  return merged
    .sort((a, b) => (b.recommendation_score ?? 0) - (a.recommendation_score ?? 0))
    .slice(0, 8);
}

function lineCompletenessScore(odds: LiveOddsItem): number {
  let score = 0;
  if (odds.h2h_home != null) score += 1;
  if (odds.h2h_away != null) score += 1;
  if (odds.spread_home != null || odds.spread_away != null) score += 1;
  if (odds.total_line != null) score += 1;
  if (odds.is_live) score += 1;
  return score;
}

async function fetchLiveOddsSnapshots(
  sportsList: string[],
  activeSport: string | null,
  signal?: AbortSignal,
): Promise<Record<string, LiveOddsItem>> {
  const today = new Date().toISOString().slice(0, 10);
  const targetSports = activeSport ? [activeSport] : sportsList;

  const results = await Promise.allSettled(
    targetSports.map(async (sport) => {
      const res = await fetch(`${API_BASE}/v1/${sport}/odds?date=${today}&limit=200`, {
        cache: "no-store",
        signal,
      });
      if (!res.ok) return [] as LiveOddsItem[];
      const json = await res.json();
      return (json?.data ?? []) as LiveOddsItem[];
    }),
  );

  const allOdds = results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
  const bestByGame: Record<string, LiveOddsItem> = {};
  for (const odds of allOdds) {
    const key = `${odds.sport}:${String(odds.game_id)}`;
    const prev = bestByGame[key];
    if (!prev || lineCompletenessScore(odds) > lineCompletenessScore(prev)) {
      bestByGame[key] = odds;
    }
  }
  return bestByGame;
}

export function LiveClient({ games: initialGames, sports }: Props) {
  const [allGames, setAllGames] = useState<GameItem[]>(initialGames);
  const [livePredictions, setLivePredictions] = useState<Record<string, LivePredictionItem>>({});
  const [liveOdds, setLiveOdds] = useState<Record<string, LiveOddsItem>>({});
  const [propOpportunities, setPropOpportunities] = useState<PropOpportunityItem[]>([]);
  const [activeSport, setActiveSport] = useState<string | null>(null);
  const [showLiveOnly, setShowLiveOnly] = useState(false);
  const [page, setPage] = useState(1);
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date());
  const [secondsAgo, setSecondsAgo] = useState(0);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [connectionState, setConnectionState] = useState<LiveConnectionState>("connecting");
  const [logoErrors, setLogoErrors] = useState<Set<string>>(new Set());

  const handleLogoError = useCallback((key: string) => {
    setLogoErrors((prev) => { const next = new Set(prev); next.add(key); return next; });
  }, []);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const streamsRef = useRef<EventSource[]>([]);

  const abortRef = useRef<AbortController | null>(null);

  const refreshGames = useCallback(async () => {
    // Cancel any in-flight refresh
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    setIsRefreshing(true);
    try {
      const [fresh, snapshots, oddsSnapshots, opportunities] = await Promise.all([
        fetchLiveGames(sports, ac.signal),
        fetchLivePredictionSnapshots(sports, ac.signal),
        fetchLiveOddsSnapshots(sports, activeSport, ac.signal),
        fetchPropOpportunities(sports, activeSport, ac.signal),
      ]);
      if (ac.signal.aborted) return;
      setAllGames(fresh);
      if (Object.keys(snapshots).length > 0) {
        setLivePredictions((current) => ({
          ...current,
          ...snapshots,
        }));
      }
      if (Object.keys(oddsSnapshots).length > 0) {
        setLiveOdds((current) => ({
          ...current,
          ...oddsSnapshots,
        }));
      }
      setPropOpportunities(opportunities);
      setLastUpdated(new Date());
      setSecondsAgo(0);
    } catch {
      /* silent fail — keep stale data */
    } finally {
      if (!ac.signal.aborted) {
        setIsRefreshing(false);
      }
    }
  }, [sports, activeSport]);

  const oddsByTeam = useMemo(() => {
    const map: Record<string, LiveOddsItem> = {};
    for (const odds of Object.values(liveOdds)) {
      const away = (odds.away_team ?? "").toLowerCase().trim();
      const home = (odds.home_team ?? "").toLowerCase().trim();
      if (!away || !home) continue;
      map[`${odds.sport}:${away}::${home}`] = odds;
    }
    return map;
  }, [liveOdds]);

  const games = allGames;

  const streamedSports = useMemo(() => {
    if (activeSport) {
      return [activeSport];
    }
    return Array.from(
      new Set(allGames.filter((game) => isLive(game.status)).map((game) => game.sport)),
    );
  }, [activeSport, allGames]);

  const upsertGame = useCallback((incoming: GameItem) => {
    setAllGames((current) => {
      const index = current.findIndex(
        (game) => game.sport === incoming.sport && String(game.id) === String(incoming.id),
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
    setLastUpdated(new Date());
    setSecondsAgo(0);
  }, []);

  const upsertPrediction = useCallback((incoming: LivePredictionItem) => {
    const key = String(incoming.game_id);
    setLivePredictions((current) => ({
      ...current,
      [key]: {
        ...current[key],
        ...incoming,
      },
    }));
    setLastUpdated(new Date());
    setSecondsAgo(0);
  }, []);

  // Auto-refresh every 30s, pause when tab not visible
  useEffect(() => {
    const start = () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
      intervalRef.current = setInterval(refreshGames, REFRESH_INTERVAL_MS);
    };
    const stop = () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
    const onVisibility = () => {
      if (document.hidden) { stop(); } else { refreshGames(); start(); }
    };
    start();
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      stop();
      abortRef.current?.abort();
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [refreshGames]);

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
      const source = new EventSource(`${API_BASE}/v1/sse/${sport}/live`);

      source.addEventListener("system", () => {
        connectedAny = true;
        if (!disposed) {
          setConnectionState("connected");
        }
      });

      source.addEventListener("game_update", (event) => {
        try {
          const payload = JSON.parse((event as MessageEvent<string>).data) as GameItem;
          upsertGame({ ...payload, sport: payload.sport ?? sport });
          if (!disposed) {
            setConnectionState("connected");
          }
        } catch {
          // Ignore malformed events and let polling backfill state.
        }
      });

      source.addEventListener("prediction_update", (event) => {
        try {
          const payload = JSON.parse((event as MessageEvent<string>).data) as LivePredictionItem;
          upsertPrediction(payload);
          if (!disposed) {
            setConnectionState("connected");
          }
        } catch {
          // Ignore malformed events and let the score stream continue.
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
  }, [streamedSports, upsertGame, upsertPrediction]);

  // Tick the "seconds ago" counter every second
  useEffect(() => {
    tickRef.current = setInterval(() => {
      setSecondsAgo(Math.floor((Date.now() - lastUpdated.getTime()) / 1000));
    }, 1000);
    return () => {
      if (tickRef.current) clearInterval(tickRef.current);
    };
  }, [lastUpdated]);

  const filtered = useMemo(() => {
    let result = activeSport
      ? games.filter((g) => g.sport === activeSport)
      : games;
    if (showLiveOnly) {
      result = result.filter((g) => isLive(g.status));
    }
    // Sort: live games first, then by date
    return result.sort((a, b) => {
      const aLive = isLive(a.status) ? 0 : 1;
      const bLive = isLive(b.status) ? 0 : 1;
      if (aLive !== bLive) return aLive - bLive;
      return new Date(a.date).getTime() - new Date(b.date).getTime();
    });
  }, [games, activeSport, showLiveOnly]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER_PAGE));
  const safeP = Math.min(page, totalPages);
  const pageSlice = filtered.slice((safeP - 1) * PER_PAGE, safeP * PER_PAGE);

  const liveCount = games.filter((g) => isLive(g.status)).length;

  function handleSportChange(sport: string | null) {
    setActiveSport(sport);
    setPage(1);
  }

  function formatWinProbability(value: number | null | undefined): string {
    if (value == null) return "--";
    return `${Math.round(value * 100)}%`;
  }

  function momentumLabel(prediction: LivePredictionItem): string {
    if (!prediction.momentum || prediction.momentum === "neutral") {
      return "Momentum neutral";
    }
    const team = prediction.momentum === "home" ? "Home" : "Away";
    return `${team} momentum${prediction.momentum_score != null ? ` (${Math.round(prediction.momentum_score * 100)}%)` : ""}`;
  }

  return (
    <main>
      <SectionBand title="Live Scores">
        {propOpportunities.length > 0 && (
          <div
            style={{
              marginBottom: "var(--space-4)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-lg, 10px)",
              padding: "var(--space-3)",
              background: "var(--surface-elevated)",
            }}
          >
            <div style={{ fontSize: "0.78rem", fontWeight: 700, textTransform: "uppercase", marginBottom: "var(--space-2)" }}>
              Top Prop Opportunities
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 220px), 1fr))", gap: "var(--space-2)" }}>
              {propOpportunities.slice(0, 4).map((opp) => {
                const color = getSportColor(opp.sport);
                const scorePct = Math.round((opp.recommendation_score ?? 0) * 100);
                const markets = opp.available_markets?.slice(0, 2).map((m) => formatPropType(m.prop_type)).join(" • ") ?? "No markets";
                return (
                  <div
                    key={`${opp.sport}-${opp.game_id}`}
                    style={{
                      border: `1px solid ${color}55`,
                      borderRadius: "var(--radius-md, 6px)",
                      padding: "var(--space-2)",
                      background: "var(--surface)",
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: "var(--space-2)", marginBottom: 4 }}>
                      <span style={{ fontSize: "0.72rem", fontWeight: 700, color }}>{getDisplayName(opp.sport)}</span>
                      <span style={{ fontSize: "0.72rem", fontWeight: 700 }}>{scorePct}%</span>
                    </div>
                    <div style={{ fontSize: "0.8rem", fontWeight: 600, marginBottom: 4 }}>
                      {opp.away_team} at {opp.home_team}
                    </div>
                    <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginBottom: 4 }}>
                      Tier: {opp.recommendation_tier}
                    </div>
                    <div style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>{markets}</div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Sport filter tabs */}
        <div
          style={{
            display: "flex",
            gap: "var(--space-2)",
            flexWrap: "wrap",
            marginBottom: "var(--space-4)",
          }}
        >
          <button
            onClick={() => handleSportChange(null)}
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-full, 9999px)",
              border: "1px solid var(--border)",
              backgroundColor: !activeSport
                ? "var(--color-accent, #2563eb)"
                : "transparent",
              color: !activeSport ? "#fff" : "inherit",
              cursor: "pointer",
              fontSize: "var(--text-sm)",
              fontWeight: "var(--fw-semibold, 600)",
            }}
          >
            All
          </button>
          {sports.map((sport) => {
            const isActive = activeSport === sport;
            const color = getSportColor(sport);
            return (
              <button
                key={sport}
                onClick={() => handleSportChange(sport)}
                style={{
                  padding: "var(--space-2) var(--space-4)",
                  borderRadius: "var(--radius-full, 9999px)",
                  border: `1px solid ${color}`,
                  backgroundColor: isActive ? color : "transparent",
                  color: isActive ? "#fff" : "inherit",
                  cursor: "pointer",
                  fontSize: "var(--text-sm)",
                  fontWeight: "var(--fw-semibold, 600)",
                }}
              >
                {getDisplayName(sport)}
              </button>
            );
          })}
        </div>

        {/* Controls row */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: "var(--space-6)",
            flexWrap: "wrap",
            gap: "var(--space-2)",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "var(--space-4)" }}>
            <label
              style={{
                display: "flex",
                alignItems: "center",
                gap: "var(--space-2)",
                cursor: "pointer",
                fontSize: "0.875rem",
              }}
            >
              <input
                id="live-only-filter"
                type="checkbox"
                checked={showLiveOnly}
                onChange={(e) => {
                  setShowLiveOnly(e.target.checked);
                  setPage(1);
                }}
              />
              Live only
              {liveCount > 0 && (
                <span
                  style={{
                    background: "#dc2626",
                    color: "#fff",
                    fontSize: "0.7rem",
                    fontWeight: 700,
                    padding: "1px 6px",
                    borderRadius: "var(--radius-full, 9999px)",
                  }}
                >
                  {liveCount}
                </span>
              )}
            </label>

            <button
              onClick={refreshGames}
              disabled={isRefreshing}
              aria-label="Refresh games"
              aria-busy={isRefreshing}
              style={{
                padding: "var(--space-1, 4px) var(--space-3, 12px)",
                borderRadius: "var(--radius-md, 6px)",
                border: "1px solid var(--border)",
                backgroundColor: "transparent",
                color: "inherit",
                cursor: isRefreshing ? "not-allowed" : "pointer",
                fontSize: "0.8rem",
                fontWeight: 600,
                opacity: isRefreshing ? 0.6 : 1,
                display: "flex",
                alignItems: "center",
                gap: "4px",
              }}
            >
              <span style={isRefreshing ? { animation: "spin 1s linear infinite", display: "inline-block" } : undefined}>↻</span>
              {isRefreshing ? "Refreshing…" : "Refresh"}
            </button>
          </div>

          <span
            role="status"
            aria-live="polite"
            style={{
              fontSize: "0.75rem",
              color: "var(--text-muted)",
              fontStyle: "italic",
              display: "flex",
              alignItems: "center",
              gap: "var(--space-2)",
            }}
          >
            <span>
              Last updated: {secondsAgo < 5 ? "just now" : `${secondsAgo}s ago`}
            </span>
            <span>•</span>
            <span>
              {connectionState === "connected"
                ? `Live stream active${streamedSports.length ? ` for ${streamedSports.length} sport${streamedSports.length === 1 ? "" : "s"}` : ""}`
                : connectionState === "connecting"
                  ? "Connecting live stream..."
                  : "Polling every 30s"}
            </span>
            <span>•</span>
            <span>{filtered.length} game{filtered.length !== 1 ? "s" : ""} today</span>
          </span>
        </div>

        {filtered.length === 0 ? (
          <div className="empty-state">
            <div className="empty-state-icon" aria-hidden="true">
              {showLiveOnly ? "📺" : "🏟️"}
            </div>
            <h3 className="empty-state-title">
              {showLiveOnly ? "No live games right now" : "No games today"}
            </h3>
            <p className="empty-state-desc">
              {showLiveOnly
                ? "Check back soon — games can start at any time!"
                : `No games scheduled for today${activeSport ? ` in ${getDisplayName(activeSport)}` : ""}. Try a different sport or check back later.`}
            </p>
          </div>
        ) : (
          <>
            <div
              aria-live="polite"
              aria-label="Live game scores"
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 320px), 1fr))",
                gap: "var(--space-4)",
              }}
            >
              {pageSlice.map((game) => {
                const live = isLive(game.status);
                const sportColor = getSportColor(game.sport);
                const broadcasts = splitBroadcasts(game.broadcast);
                const directBroadcastUrl = normalizeWatchUrl(game.broadcast_url);
                const homeLogoUrl = getTeamLogoUrl(game.sport, game.home_team_id);
                const awayLogoUrl = getTeamLogoUrl(game.sport, game.away_team_id);
                const prediction = livePredictions[String(game.id)];
                const odds =
                  liveOdds[`${game.sport}:${String(game.id)}`]
                  ?? oddsByTeam[`${game.sport}:${game.away_team.toLowerCase().trim()}::${game.home_team.toLowerCase().trim()}`];
                const gameKey = `${game.sport}-${game.id}`;
                const awayLogoOk = awayLogoUrl && !logoErrors.has(`${gameKey}-away`);
                const homeLogoOk = homeLogoUrl && !logoErrors.has(`${gameKey}-home`);

                return (
                  <div
                    key={`${game.sport}-${game.id}`}
                    className="card"
                    style={{
                      borderLeft: live ? `3px solid #dc2626` : undefined,
                    }}
                  >
                    <div className="card-body" style={{ padding: "var(--space-4)" }}>
                      {/* Header */}
                      <div
                        style={{
                          display: "flex",
                          justifyContent: "space-between",
                          alignItems: "center",
                          marginBottom: "var(--space-4)",
                        }}
                      >
                        <span
                          style={{
                            display: "inline-block",
                            padding: "2px 8px",
                            borderRadius: "var(--radius-full, 9999px)",
                            backgroundColor: sportColor,
                            color: "#fff",
                            fontSize: "0.65rem",
                            fontWeight: 700,
                            textTransform: "uppercase",
                          }}
                        >
                          {getDisplayName(game.sport)}
                        </span>
                        <div style={{ display: "flex", alignItems: "center", gap: "var(--space-2)" }}>
                          {live && (
                            <span
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                gap: "4px",
                                background: "#dc2626",
                                color: "#fff",
                                fontSize: "0.65rem",
                                fontWeight: 700,
                                padding: "2px 8px",
                                borderRadius: "var(--radius-full, 9999px)",
                                animation: "pulse 2s infinite",
                              }}
                            >
                              <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#fff" }} />
                              LIVE
                            </span>
                          )}
                          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
                            {game.status}
                          </span>
                        </div>
                      </div>

                      {/* Teams & Score */}
                      <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-2)" }}>
                        {/* Away team */}
                        <div
                          style={{
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                          }}
                        >
                          <div style={{ display: "flex", alignItems: "center", gap: "var(--space-2)" }}>
                            {awayLogoOk ? (
                              <img
                                src={awayLogoUrl}
                                alt={game.away_team}
                                style={{ width: 28, height: 28, objectFit: "contain" }}
                                onError={() => handleLogoError(`${gameKey}-away`)}
                              />
                            ) : (
                              <div
                                style={{
                                  width: 28,
                                  height: 28,
                                  borderRadius: "50%",
                                  background: "var(--border)",
                                  display: "flex",
                                  alignItems: "center",
                                  justifyContent: "center",
                                  fontSize: "0.6rem",
                                  fontWeight: 700,
                                }}
                              >
                                {game.away_team.slice(0, 3).toUpperCase()}
                              </div>
                            )}
                            <span style={{ fontWeight: 600, fontSize: "0.9rem" }}>{game.away_team}</span>
                          </div>
                          <span
                            style={{
                              fontWeight: 700,
                              fontSize: "1.2rem",
                              fontVariantNumeric: "tabular-nums",
                            }}
                          >
                            {game.away_score ?? "—"}
                          </span>
                        </div>

                        {/* Home team */}
                        <div
                          style={{
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                          }}
                        >
                          <div style={{ display: "flex", alignItems: "center", gap: "var(--space-2)" }}>
                            {homeLogoOk ? (
                              <img
                                src={homeLogoUrl}
                                alt={game.home_team}
                                style={{ width: 28, height: 28, objectFit: "contain" }}
                                onError={() => handleLogoError(`${gameKey}-home`)}
                              />
                            ) : (
                              <div
                                style={{
                                  width: 28,
                                  height: 28,
                                  borderRadius: "50%",
                                  background: "var(--border)",
                                  display: "flex",
                                  alignItems: "center",
                                  justifyContent: "center",
                                  fontSize: "0.6rem",
                                  fontWeight: 700,
                                }}
                              >
                                {game.home_team.slice(0, 3).toUpperCase()}
                              </div>
                            )}
                            <span style={{ fontWeight: 600, fontSize: "0.9rem" }}>{game.home_team}</span>
                          </div>
                          <span
                            style={{
                              fontWeight: 700,
                              fontSize: "1.2rem",
                              fontVariantNumeric: "tabular-nums",
                            }}
                          >
                            {game.home_score ?? "—"}
                          </span>
                        </div>
                      </div>

                      {prediction && (
                        <div
                          style={{
                            marginTop: "var(--space-4)",
                            padding: "var(--space-3)",
                            borderRadius: "var(--radius-md, 6px)",
                            background: "var(--surface-elevated)",
                            border: "1px solid var(--border)",
                          }}
                        >
                          <div
                            style={{
                              display: "flex",
                              justifyContent: "space-between",
                              alignItems: "center",
                              marginBottom: "var(--space-2)",
                              gap: "var(--space-2)",
                              flexWrap: "wrap",
                            }}
                          >
                            <span style={{ fontSize: "0.72rem", fontWeight: 700, textTransform: "uppercase" }}>
                              Live model
                            </span>
                            <span style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
                              {prediction.period ?? game.period ?? "In progress"}
                              {prediction.time_remaining ? ` • ${prediction.time_remaining}` : ""}
                            </span>
                          </div>

                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "var(--space-3)" }}>
                            <div>
                              <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginBottom: 4 }}>
                                Home win probability
                              </div>
                              <div style={{ fontSize: "1rem", fontWeight: 700 }}>
                                {formatWinProbability(prediction.live_home_wp)}
                              </div>
                              <div style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
                                Pregame {formatWinProbability(prediction.pre_game_home_wp)}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginBottom: 4 }}>
                                Projected final
                              </div>
                              <div style={{ fontSize: "1rem", fontWeight: 700 }}>
                                {prediction.predicted_final_away ?? "--"} - {prediction.predicted_final_home ?? "--"}
                              </div>
                              <div style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
                                {game.away_team} at {game.home_team}
                              </div>
                            </div>
                          </div>

                          <div style={{ marginTop: "var(--space-3)" }}>
                            <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginBottom: 6 }}>
                              {momentumLabel(prediction)}
                            </div>
                            <div
                              role="progressbar"
                              aria-valuenow={Math.round((prediction.live_home_wp ?? 0.5) * 100)}
                              aria-valuemin={0}
                              aria-valuemax={100}
                              aria-label="Home team win probability"
                              style={{
                                width: "100%",
                                height: 8,
                                borderRadius: 999,
                                background: "var(--border)",
                                overflow: "hidden",
                              }}
                            >
                              <div
                                style={{
                                  width: `${Math.max(0, Math.min(100, (prediction.live_home_wp ?? 0.5) * 100))}%`,
                                  height: "100%",
                                  background: sportColor,
                                }}
                              />
                            </div>
                          </div>

                          {prediction.key_factors && prediction.key_factors.length > 0 && (
                            <div style={{ marginTop: "var(--space-3)", fontSize: "0.72rem", color: "var(--text-muted)" }}>
                              {prediction.key_factors.slice(0, 2).join(" • ")}
                            </div>
                          )}
                        </div>
                      )}

                      {odds && (
                        <div
                          style={{
                            marginTop: "var(--space-3)",
                            padding: "var(--space-3)",
                            borderRadius: "var(--radius-md, 6px)",
                            border: "1px solid var(--border)",
                            background: "var(--surface)",
                          }}
                        >
                          <div
                            style={{
                              display: "flex",
                              justifyContent: "space-between",
                              alignItems: "center",
                              marginBottom: "var(--space-2)",
                              gap: "var(--space-2)",
                              flexWrap: "wrap",
                            }}
                          >
                            <span style={{ fontSize: "0.72rem", fontWeight: 700, textTransform: "uppercase" }}>
                              Market Odds
                            </span>
                            <span style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
                              {odds.bookmaker ?? "Best available"}
                            </span>
                          </div>

                          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "var(--space-2)" }}>
                            <div>
                              <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", textTransform: "uppercase" }}>ML</div>
                              <div style={{ fontSize: "0.8rem", fontWeight: 600 }}>
                                {odds.h2h_away != null ? formatOdds(odds.h2h_away) : "--"}
                                <span style={{ color: "var(--text-muted)", margin: "0 4px" }}>/</span>
                                {odds.h2h_home != null ? formatOdds(odds.h2h_home) : "--"}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", textTransform: "uppercase" }}>Spread</div>
                              <div style={{ fontSize: "0.8rem", fontWeight: 600 }}>
                                {odds.spread_away != null ? formatSpread(odds.spread_away) : "--"}
                                <span style={{ color: "var(--text-muted)", margin: "0 4px" }}>/</span>
                                {odds.spread_home != null ? formatSpread(odds.spread_home) : "--"}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", textTransform: "uppercase" }}>Total</div>
                              <div style={{ fontSize: "0.8rem", fontWeight: 600 }}>
                                {odds.total_line != null ? formatTotal(odds.total_line) : "--"}
                              </div>
                            </div>
                          </div>
                        </div>
                      )}

                      {/* Footer */}
                      <div
                        style={{
                          marginTop: "var(--space-4)",
                          paddingTop: "var(--space-2)",
                          borderTop: "1px solid var(--border)",
                          display: "flex",
                          justifyContent: "space-between",
                          fontSize: "0.75rem",
                          color: "var(--text-muted)",
                        }}
                      >
                        <span>{game.venue ?? ""}</span>
                        <span>
                          {game.start_time
                            ? formatGameTime(game.start_time)
                            : formatGameTime(game.date)}
                        </span>
                      </div>

                      {(broadcasts.length > 0 || directBroadcastUrl || game.id) && (
                        <div
                          style={{
                            marginTop: "var(--space-2)",
                            display: "flex",
                            flexWrap: "wrap",
                            gap: "var(--space-2)",
                            alignItems: "center",
                          }}
                        >
                          {directBroadcastUrl && (
                            <a
                              href={directBroadcastUrl}
                              target="_blank"
                              rel="noreferrer"
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                border: "1px solid var(--border)",
                                borderRadius: "9999px",
                                padding: "2px 8px",
                                fontSize: "0.68rem",
                                fontWeight: 700,
                                color: "inherit",
                                textDecoration: "none",
                                background: "var(--surface)",
                              }}
                              title="Open official live stream"
                            >
                              Watch live
                            </a>
                          )}

                          {broadcasts.slice(0, 3).map((network) => (
                            <a
                              key={`${game.sport}-${game.id}-${network}`}
                              href={providerLink(network)}
                              target="_blank"
                              rel="noreferrer"
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                border: "1px solid var(--border)",
                                borderRadius: "9999px",
                                padding: "2px 8px",
                                fontSize: "0.68rem",
                                fontWeight: 600,
                                color: "inherit",
                                textDecoration: "none",
                              }}
                              title={`Watch on ${network}`}
                            >
                              Watch {network}
                            </a>
                          ))}

                          <a
                            href={`/games/${game.sport}/${game.id}`}
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              border: "1px solid var(--border)",
                              borderRadius: "9999px",
                              padding: "2px 8px",
                              fontSize: "0.68rem",
                              fontWeight: 600,
                              color: "inherit",
                              textDecoration: "none",
                              background: "var(--surface)",
                            }}
                            title="Open game detail"
                          >
                            Live view
                          </a>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>

            <Pagination page={safeP} totalPages={totalPages} onPageChange={setPage} />
          </>
        )}
      </SectionBand>
    </main>
  );
}
