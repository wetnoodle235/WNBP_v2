"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import Link from "next/link";
import { Badge, LiveBadge } from "@/components/ui";
import { TeamBadge } from "@/components/ui";
import { formatGameTime, formatProbability } from "@/lib/formatters";

/* ------------------------------------------------------------------ */
/*  Types (mirrored from schemas — kept slim for the client bundle)   */
/* ------------------------------------------------------------------ */

interface Game {
  id: string;
  sport: string;
  season: string;
  date: string;
  status: string;
  home_team: string;
  away_team: string;
  home_score?: number | null;
  away_score?: number | null;
  home_team_id?: string | null;
  away_team_id?: string | null;
  venue?: string | null;
  attendance?: number | null;
  start_time?: string | null;
  period?: string | null;
  home_linescores?: number[] | null;
  away_linescores?: number[] | null;
  // Quarter fields (basketball, football, hockey)
  home_q1?: number | null; home_q2?: number | null; home_q3?: number | null; home_q4?: number | null; home_ot?: number | null;
  away_q1?: number | null; away_q2?: number | null; away_q3?: number | null; away_q4?: number | null; away_ot?: number | null;
  // Inning fields (MLB)
  home_i1?: number | null; home_i2?: number | null; home_i3?: number | null; home_i4?: number | null; home_i5?: number | null;
  home_i6?: number | null; home_i7?: number | null; home_i8?: number | null; home_i9?: number | null; home_extras?: number | null;
  away_i1?: number | null; away_i2?: number | null; away_i3?: number | null; away_i4?: number | null; away_i5?: number | null;
  away_i6?: number | null; away_i7?: number | null; away_i8?: number | null; away_i9?: number | null; away_extras?: number | null;
  [key: string]: unknown;
}

interface Prediction {
  game_id: string;
  confidence?: number | null;
  home_win_prob?: number | null;
  away_win_prob?: number | null;
  predicted_home_score?: number | null;
  predicted_away_score?: number | null;
  predicted_spread?: number | null;
  predicted_total?: number | null;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

/** Build linescores array from individual period/inning fields. */
function buildLinescores(game: Game, prefix: "home" | "away", sport: string): number[] | null {
  if (sport === "mlb") {
    const scores: number[] = [];
    for (let i = 1; i <= 9; i++) {
      const v = game[`${prefix}_i${i}`] as number | null | undefined;
      if (v == null && i > scores.length + 1) break;
      scores.push(v ?? 0);
    }
    const extras = game[`${prefix}_extras`] as number | null | undefined;
    if (extras != null && extras > 0) scores.push(extras);
    return scores.length > 0 ? scores : null;
  }
  // Quarters (basketball, football, hockey)
  const scores: number[] = [];
  for (let i = 1; i <= 4; i++) {
    const v = game[`${prefix}_q${i}`] as number | null | undefined;
    if (v != null) scores.push(v);
  }
  const ot = game[`${prefix}_ot`] as number | null | undefined;
  if (ot != null && ot > 0) scores.push(ot);
  return scores.length > 0 ? scores : null;
}

/** Get linescores for a game — use pre-built array or build from fields. */
function getLinescores(game: Game, sport: string): { home: number[] | null; away: number[] | null } {
  return {
    home: game.home_linescores ?? buildLinescores(game, "home", sport),
    away: game.away_linescores ?? buildLinescores(game, "away", sport),
  };
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

function toDateStr(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function shiftDate(iso: string, days: number): string {
  const d = new Date(iso + "T12:00:00");
  d.setDate(d.getDate() + days);
  return toDateStr(d);
}

function formatDisplayDate(iso: string): string {
  const d = new Date(iso + "T12:00:00");
  return d.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function isLive(status: string | null | undefined): boolean {
  if (!status) return false;
  const s = status.toLowerCase();
  return s.includes("in progress") || s.includes("live") || s.includes("in_progress");
}

function isFinal(status: string | null | undefined): boolean {
  if (!status) return false;
  return status.toLowerCase() === "final";
}

function isScheduled(status: string | null | undefined): boolean {
  if (!status) return false;
  return status.toLowerCase() === "scheduled";
}

/* ------------------------------------------------------------------ */
/*  Styles                                                            */
/* ------------------------------------------------------------------ */

const gridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 340px), 1fr))",
  gap: "1rem",
};

const cardStyle: React.CSSProperties = {
  background: "var(--color-bg-2)",
  border: "1px solid var(--color-border)",
  borderRadius: "var(--radius-md)",
  overflow: "hidden",
  textDecoration: "none",
  color: "var(--color-text)",
  transition: "box-shadow var(--transition-base), border-color var(--transition-base)",
  display: "block",
};

const cardHoverProps = {
  boxShadow: "var(--shadow-md)",
  borderColor: "var(--color-border-active)",
};

const headerStyle: React.CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  padding: "0.625rem 1rem",
  borderBottom: "1px solid var(--color-border)",
  fontSize: "var(--text-xs)",
};

const teamRowStyle: React.CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  padding: "0.5rem 1rem",
};

const scoreStyle: React.CSSProperties = {
  fontWeight: "var(--fw-bold)" as unknown as number,
  fontSize: "var(--text-md)",
  fontVariantNumeric: "tabular-nums",
  minWidth: "2rem",
  textAlign: "right",
};

const footerStyle: React.CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  padding: "0.5rem 1rem",
  borderTop: "1px solid var(--color-border)",
  fontSize: "var(--text-xs)",
  color: "var(--color-text-muted)",
};

const navStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  gap: "0.75rem",
  marginBottom: "1.25rem",
  flexWrap: "wrap",
};

const navBtnStyle: React.CSSProperties = {
  background: "var(--color-bg-3)",
  border: "1px solid var(--color-border)",
  borderRadius: "var(--radius)",
  color: "var(--color-text)",
  padding: "0.375rem 0.75rem",
  cursor: "pointer",
  fontSize: "var(--text-sm)",
  fontWeight: "var(--fw-medium)" as unknown as number,
  transition: "background var(--transition-fast)",
};

const todayBtnStyle: React.CSSProperties = {
  ...navBtnStyle,
  background: "var(--color-brand)",
  color: "var(--color-text-inverse)",
  border: "1px solid var(--color-brand)",
};

/* ------------------------------------------------------------------ */
/*  Accent strip on top of card based on game status                  */
/* ------------------------------------------------------------------ */

function accentColor(status: string): string {
  if (isFinal(status)) return "var(--color-win)";
  if (isLive(status)) return "var(--color-loss)";
  return "transparent";
}

/* ------------------------------------------------------------------ */
/*  Countdown helper                                                  */
/* ------------------------------------------------------------------ */

function formatCountdown(startTime: string): string | null {
  const start = new Date(startTime).getTime();
  const now = Date.now();
  const diff = start - now;
  if (diff <= 0) return null;
  const hours = Math.floor(diff / (1000 * 60 * 60));
  const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
  if (hours > 24) return null;
  if (hours > 0) return `Starts in ${hours}h ${mins}m`;
  return `Starts in ${mins}m`;
}

/* ------------------------------------------------------------------ */
/*  Linescore helpers                                                 */
/* ------------------------------------------------------------------ */

function getPeriodLabels(sport: string, count: number): string[] {
  const s = sport.toLowerCase();
  if (["nba", "wnba", "ncaab", "ncaaw"].includes(s)) {
    const base = ["Q1", "Q2", "Q3", "Q4"];
    for (let i = base.length; i < count; i++) {
      const otNum = i - 3;
      base.push(otNum === 1 ? "OT" : `${otNum}OT`);
    }
    return base.slice(0, count);
  }
  if (s === "nhl") {
    const base = ["P1", "P2", "P3"];
    if (count > 3) base.push("OT");
    if (count > 4) base.push("SO");
    for (let i = base.length; i < count; i++) base.push(`${i - 2}OT`);
    return base.slice(0, count);
  }
  if (["nfl", "ncaaf"].includes(s)) {
    const base = ["Q1", "Q2", "Q3", "Q4"];
    for (let i = base.length; i < count; i++) {
      const otNum = i - 3;
      base.push(otNum === 1 ? "OT" : `${otNum}OT`);
    }
    return base.slice(0, count);
  }
  if (s === "mlb") {
    return Array.from({ length: count }, (_, i) => String(i + 1));
  }
  return Array.from({ length: count }, (_, i) => String(i + 1));
}

const breakdownWrapperStyle: React.CSSProperties = {
  background: "var(--color-bg-3)",
  padding: "0.25rem 0.75rem",
};

const breakdownTableStyle: React.CSSProperties = {
  width: "100%",
  fontSize: "var(--text-xs)",
  fontVariantNumeric: "tabular-nums",
  borderCollapse: "collapse",
  textAlign: "center",
};

const breakdownThStyle: React.CSSProperties = {
  padding: "0.125rem 0.25rem",
  fontWeight: "var(--fw-medium)" as unknown as number,
  color: "var(--color-text-muted)",
  whiteSpace: "nowrap",
};

const breakdownTdStyle: React.CSSProperties = {
  padding: "0.125rem 0.25rem",
  whiteSpace: "nowrap",
};

const breakdownTotalStyle: React.CSSProperties = {
  ...breakdownTdStyle,
  fontWeight: "var(--fw-bold)" as unknown as number,
};

function ScoreBreakdown({
  game,
  sport,
}: {
  game: Game;
  sport: string;
}) {
  const ls = getLinescores(game, sport);
  const away = ls.away;
  const home = ls.home;
  if (!away || !home || away.length === 0 || home.length === 0) return null;

  const maxLen = Math.max(away.length, home.length);
  const labels = getPeriodLabels(sport, maxLen);

  const awayTotal = game.away_score ?? away.reduce((a, b) => a + b, 0);
  const homeTotal = game.home_score ?? home.reduce((a, b) => a + b, 0);

  const awayWins = awayTotal > homeTotal;
  const homeWins = homeTotal > awayTotal;

  const pad = (arr: number[], len: number) => {
    const result = [...arr];
    while (result.length < len) result.push(0);
    return result;
  };
  const awayPadded = pad(away, maxLen);
  const homePadded = pad(home, maxLen);

  return (
    <div className="responsive-table-wrap" style={breakdownWrapperStyle}>
      <table className="responsive-table game-breakdown-table" aria-label="Game breakdown" style={breakdownTableStyle}>
        <caption className="sr-only">{`${game.away_team} vs ${game.home_team} score breakdown`}</caption>
        <thead>
          <tr>
            <th scope="col" style={{ ...breakdownThStyle, textAlign: "left", minWidth: "2rem" }}></th>
            {labels.map((l) => (
              <th scope="col" key={l} style={breakdownThStyle}>{l}</th>
            ))}
            <th scope="col" style={{ ...breakdownThStyle, fontWeight: "var(--fw-bold)" as unknown as number }}>T</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <th scope="row" style={{ ...breakdownTdStyle, textAlign: "left", color: "var(--color-text-muted)" }}>
              {game.away_team.length > 5 ? game.away_team.slice(0, 4) + "…" : game.away_team}
            </th>
            {awayPadded.map((s, i) => (
              <td key={i} style={breakdownTdStyle}>{s}</td>
            ))}
            <td style={{
              ...breakdownTotalStyle,
              color: awayWins ? "var(--color-win)" : undefined,
            }}>
              {awayTotal}
            </td>
          </tr>
          <tr>
            <th scope="row" style={{ ...breakdownTdStyle, textAlign: "left", color: "var(--color-text-muted)" }}>
              {game.home_team.length > 5 ? game.home_team.slice(0, 4) + "…" : game.home_team}
            </th>
            {homePadded.map((s, i) => (
              <td key={i} style={breakdownTdStyle}>{s}</td>
            ))}
            <td style={{
              ...breakdownTotalStyle,
              color: homeWins ? "var(--color-win)" : undefined,
            }}>
              {homeTotal}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Game Card                                                         */
/* ------------------------------------------------------------------ */

function GameCard({
  game,
  prediction,
  sport,
}: {
  game: Game;
  prediction?: Prediction;
  sport: string;
}) {
  const [hovered, setHovered] = useState(false);
  const live = isLive(game.status);
  const final = isFinal(game.status);
  const scheduled = isScheduled(game.status);

  const countdown = scheduled && game.start_time ? formatCountdown(game.start_time) : null;

  const mergedCard: React.CSSProperties = {
    ...cardStyle,
    borderTopWidth: "3px",
    borderTopColor: accentColor(game.status),
    ...(live ? { animation: "pulse 2s ease-in-out infinite" } : {}),
    ...(hovered ? cardHoverProps : {}),
  };

  const hasPredScore =
    prediction?.predicted_home_score != null &&
    prediction?.predicted_away_score != null;

  return (
    <Link
      href={`/games/${sport}/${game.id}`}
      style={mergedCard}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      {/* Header — status */}
      <div style={headerStyle}>
        <span>
          {live && <LiveBadge />}
          {final && <Badge variant="win">FINAL</Badge>}
          {scheduled && countdown && (
            <span className="countdown">🕐 {countdown}</span>
          )}
          {scheduled && !countdown && game.start_time && (
            <span style={{ color: "var(--color-text-secondary)" }}>
              {formatGameTime(game.start_time)}
            </span>
          )}
          {!live && !final && !scheduled && (
            <Badge>{game.status.toUpperCase()}</Badge>
          )}
        </span>
        {game.period && live && (
          <span style={{ color: "var(--color-text-secondary)" }}>{game.period}</span>
        )}
      </div>

      {/* Away team row */}
      <div style={teamRowStyle}>
        <TeamBadge
          teamId={game.away_team_id ?? undefined}
          name={game.away_team}
          sport={sport}
          size="sm"
        />
        {(final || live) && (
          <span style={scoreStyle}>{game.away_score ?? "–"}</span>
        )}
      </div>

      {/* Home team row */}
      <div style={teamRowStyle}>
        <TeamBadge
          teamId={game.home_team_id ?? undefined}
          name={game.home_team}
          sport={sport}
          size="sm"
        />
        {(final || live) && (
          <span style={scoreStyle}>{game.home_score ?? "–"}</span>
        )}
      </div>

      {/* Score breakdown (linescore mini box score) */}
      {(final || live) && (
        <ScoreBreakdown game={game} sport={sport} />
      )}

      {/* Prediction details */}
      {prediction && (hasPredScore || prediction.predicted_spread != null || prediction.predicted_total != null) && (
        <div className="game-card-prediction">
          {hasPredScore && (
            <span>
              <strong>Predicted:</strong>{" "}
              {game.home_team} {prediction.predicted_home_score} – {game.away_team} {prediction.predicted_away_score}
            </span>
          )}
          {prediction.predicted_spread != null && (
            <span>
              <strong>Spread:</strong>{" "}
              {game.home_team} {prediction.predicted_spread > 0 ? "+" : ""}
              {prediction.predicted_spread.toFixed(1)}
            </span>
          )}
          {prediction.predicted_total != null && (
            <span>
              <strong>O/U:</strong> {prediction.predicted_total.toFixed(1)}
            </span>
          )}
        </div>
      )}

      {/* Footer — venue & prediction confidence */}
      <div style={footerStyle}>
        <span>
          {game.venue
            ? game.venue
            : scheduled
              ? "Upcoming matchup"
              : ""}
        </span>
        {prediction?.confidence != null && prediction.confidence > 0 && (
          <Badge variant="premium">
            {formatProbability(prediction.confidence)} conf
          </Badge>
        )}
      </div>
    </Link>
  );
}

/* ------------------------------------------------------------------ */
/*  Main client component                                             */
/* ------------------------------------------------------------------ */

interface GamesClientProps {
  sport: string;
}

export default function GamesClient({ sport }: GamesClientProps) {
  const [date, setDate] = useState(() => toDateStr(new Date()));
  const [games, setGames] = useState<Game[]>([]);
  const [predictions, setPredictions] = useState<Record<string, Prediction>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const fetchGames = useCallback(
    async (d: string) => {
      abortRef.current?.abort();
      const ac = new AbortController();
      abortRef.current = ac;

      setLoading(true);
      setError(null);

      try {
        const [gamesRes, predsRes] = await Promise.all([
          fetch(`${API_BASE}/v1/${sport}/games?date=${d}`, { signal: ac.signal }),
          fetch(`${API_BASE}/v1/${sport}/predictions?date=${d}`, { signal: ac.signal }),
        ]);

        if (ac.signal.aborted) return;
        if (!gamesRes.ok) throw new Error(`Games: HTTP ${gamesRes.status}`);
        const gamesJson = await gamesRes.json();
        const gamesData: Game[] = gamesJson?.data ?? gamesJson ?? [];

        let predsMap: Record<string, Prediction> = {};
        if (predsRes.ok) {
          const predsJson = await predsRes.json();
          const predsData: Prediction[] = predsJson?.data ?? predsJson ?? [];
          predsMap = Object.fromEntries(predsData.map((p) => [p.game_id, p]));
        }

        if (ac.signal.aborted) return;
        setGames(Array.isArray(gamesData) ? gamesData : []);
        setPredictions(predsMap);
      } catch (err) {
        if (ac.signal.aborted) return;
        setError(err instanceof Error ? err.message : "Failed to load games");
        setGames([]);
        setPredictions({});
      } finally {
        if (!ac.signal.aborted) setLoading(false);
      }
    },
    [sport],
  );

  useEffect(() => {
    fetchGames(date);
    return () => abortRef.current?.abort();
  }, [date, fetchGames]);

  const today = toDateStr(new Date());
  const isToday = date === today;

  return (
    <div>
      {/* Date navigation */}
      <nav style={navStyle} aria-label="Date navigation">
        <button
          style={navBtnStyle}
          onClick={() => setDate((d) => shiftDate(d, -1))}
          aria-label="Previous day"
        >
          ◀ Prev
        </button>
        <span
          style={{
            fontWeight: "var(--fw-semibold)" as unknown as number,
            fontSize: "var(--text-base)",
            minWidth: "10rem",
            textAlign: "center",
          }}
        >
          {formatDisplayDate(date)}
        </span>
        <button
          style={navBtnStyle}
          onClick={() => setDate((d) => shiftDate(d, 1))}
          aria-label="Next day"
        >
          Next ▶
        </button>
        {!isToday && (
          <button
            style={todayBtnStyle}
            onClick={() => setDate(today)}
          >
            Today
          </button>
        )}
      </nav>

      {/* Loading state */}
      {loading && (
        <div role="status" aria-live="polite" style={{ textAlign: "center", padding: "3rem 1rem", color: "var(--color-text-muted)" }}>
          Loading games…
        </div>
      )}

      {/* Error state */}
      {!loading && error && (
        <div
          role="alert"
          style={{
            textAlign: "center",
            padding: "2rem 1rem",
            color: "var(--color-loss)",
            background: "var(--color-bg-3)",
            borderRadius: "var(--radius-md)",
          }}
        >
          {error}
        </div>
      )}

      {/* Empty state */}
      {!loading && !error && games.length === 0 && (
        <div
          style={{
            textAlign: "center",
            padding: "3rem 1rem",
            color: "var(--color-text-muted)",
          }}
        >
          No games scheduled for {formatDisplayDate(date)}.
        </div>
      )}

      {/* Games grid */}
      {!loading && !error && games.length > 0 && (
        <div style={gridStyle}>
          {games.map((game) => (
            <GameCard
              key={game.id}
              game={game}
              prediction={predictions[game.id]}
              sport={sport}
            />
          ))}
        </div>
      )}
    </div>
  );
}
