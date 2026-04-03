"use client";

import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { SectionBand, Badge, Pagination } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { formatProbability } from "@/lib/formatters";

interface Pred {
  game_id: string;
  sport?: string | null;
  date?: string | null;
  home_team?: string | null;
  away_team?: string | null;
  home_win_prob?: number | null;
  away_win_prob?: number | null;
  predicted_spread?: number | null;
  predicted_total?: number | null;
  confidence?: number | null;
}

interface Props {
  predictions: Pred[];
  sports: string[];
  today: string;
  hasPremium: boolean;
  initialWarning?: string | null;
}

const PER_PAGE = 25;

const numCell: React.CSSProperties = {
  textAlign: "right",
  fontVariantNumeric: "tabular-nums",
  fontSize: "0.82rem",
  padding: "0.25rem 0.4rem",
};

const centerCell: React.CSSProperties = {
  textAlign: "center",
  fontVariantNumeric: "tabular-nums",
  fontSize: "0.82rem",
  padding: "0.25rem 0.4rem",
};

const thPad: React.CSSProperties = { padding: "0.3rem 0.4rem" };

function winProbStyle(
  prob: number | null | undefined,
  otherProb: number | null | undefined,
): React.CSSProperties {
  if (prob != null && otherProb != null && prob > otherProb) {
    return { ...numCell, fontWeight: 700, color: "var(--color-win, #16a34a)" };
  }
  return numCell;
}

export function PredictionsClient({ predictions, sports, today, hasPremium, initialWarning = null }: Props) {
  const router = useRouter();
  const [activeSport, setActiveSport] = useState<string | null>(null);
  const [minConfidence, setMinConfidence] = useState(0);
  const [page, setPage] = useState(1);
  const [selectedDate, setSelectedDate] = useState(today);
  const [loadedPredictions, setLoadedPredictions] = useState<Pred[]>(predictions);
  const [isLoading, setIsLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  // Abort in-flight requests on unmount
  useEffect(() => () => { abortRef.current?.abort(); }, []);

  const prevDay = useCallback((d: string) => {
    const dt = new Date(d + "T00:00:00");
    dt.setDate(dt.getDate() - 1);
    return dt.toISOString().slice(0, 10);
  }, []);

  const nextDay = useCallback((d: string) => {
    const dt = new Date(d + "T00:00:00");
    dt.setDate(dt.getDate() + 1);
    return dt.toISOString().slice(0, 10);
  }, []);

  const handleDateChange = useCallback(async (newDate: string) => {
    setSelectedDate(newDate);
    setPage(1);
    setFetchError(null);
    if (newDate === today) {
      setLoadedPredictions(predictions);
      return;
    }
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setIsLoading(true);
    try {
      const results = await Promise.all(
        sports.map(async (sport) => {
          try {
            const res = await fetch(`/api/predictions/${sport}?date=${newDate}`, { signal: ac.signal });
            if (!res.ok) return [] as Pred[];
            const json = await res.json() as { data?: Pred[] };
            return (json.data ?? []).map((p) => ({ ...p, sport: p.sport ?? sport }));
          } catch { return [] as Pred[]; }
        }),
      );
      if (!ac.signal.aborted) setLoadedPredictions(results.flat());
    } catch (err) {
      if (err instanceof DOMException && err.name === "AbortError") return;
      setFetchError("Failed to load predictions. Please try again.");
    } finally {
      if (!ac.signal.aborted) setIsLoading(false);
    }
  }, [today, predictions, sports]);

  // Filter by sport
  const sportFiltered = useMemo(
    () =>
      activeSport
        ? loadedPredictions.filter((p) => p.sport?.toLowerCase() === activeSport)
        : loadedPredictions,
    [loadedPredictions, activeSport],
  );

  // Filter by confidence threshold
  const confidenceFiltered = useMemo(
    () =>
      minConfidence > 0
        ? sportFiltered.filter((p) => (p.confidence ?? 0) >= minConfidence / 100)
        : sportFiltered,
    [sportFiltered, minConfidence],
  );

  // Sort by confidence (highest first)
  const sorted = useMemo(
    () => [...confidenceFiltered].sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0)),
    [confidenceFiltered],
  );

  const totalPages = Math.max(1, Math.ceil(sorted.length / PER_PAGE));
  const safeP = Math.min(page, totalPages);
  const pageSlice = sorted.slice((safeP - 1) * PER_PAGE, safeP * PER_PAGE);

  const handleSportChange = useCallback((sport: string | null) => {
    setActiveSport(sport);
    setPage(1);
  }, []);

  const displayDate = new Date(selectedDate + "T00:00:00").toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
  const isToday = selectedDate === today;

  return (
    <main>
      <SectionBand title={isToday ? "Today's Predictions" : `Predictions: ${displayDate}`}>
        {/* Date navigation */}
        <div
          role="group"
          aria-label="Date navigation"
          style={{
            display: "flex",
            alignItems: "center",
            gap: "var(--space-2)",
            marginBottom: "var(--space-4)",
          }}
        >
          <button
            onClick={() => handleDateChange(prevDay(selectedDate))}
            disabled={isLoading}
            className="btn-ghost"
            style={{ padding: "0.25rem 0.6rem", fontSize: "var(--text-sm)" }}
            aria-label="Previous day"
          >
            ‹ Prev
          </button>
          <input
            type="date"
            value={selectedDate}
            max={today}
            onChange={(e) => { if (e.target.value) handleDateChange(e.target.value); }}
            disabled={isLoading}
            style={{
              fontSize: "var(--text-sm)",
              padding: "0.2rem 0.5rem",
              borderRadius: "var(--radius-sm, 4px)",
              border: "1px solid var(--border-color, #e2e8f0)",
              background: "var(--bg-surface, #fff)",
              color: "var(--text-primary)",
            }}
          />
          <button
            onClick={() => handleDateChange(nextDay(selectedDate))}
            disabled={isLoading || selectedDate >= today}
            className="btn-ghost"
            style={{ padding: "0.25rem 0.6rem", fontSize: "var(--text-sm)" }}
            aria-label="Next day"
          >
            Next ›
          </button>
          {!isToday && (
            <button
              onClick={() => handleDateChange(today)}
              disabled={isLoading}
              className="btn-ghost"
              style={{ padding: "0.25rem 0.6rem", fontSize: "var(--text-sm)", marginLeft: "var(--space-2)" }}
            >
              Today
            </button>
          )}
          {isLoading && (
            <span style={{ fontSize: "var(--text-sm)", color: "var(--text-muted)" }}>
              Loading…
            </span>
          )}
        </div>

        {/* Sport filter tabs */}
        <div className="predictions-tabs" role="tablist" aria-label="Filter predictions by sport" style={{ marginBottom: "var(--space-4)" }}>
          <button
            role="tab"
            aria-selected={!activeSport}
            className={`predictions-tab${!activeSport ? " active" : ""}`}
            onClick={() => handleSportChange(null)}
          >
            All <span className="tab-count">{sportFiltered.length}</span>
          </button>
          {sports.map((sport) => {
            const count = loadedPredictions.filter(
              (p) => p.sport?.toLowerCase() === sport,
            ).length;
            if (count === 0) return null;
            const color = getSportColor(sport);
            return (
              <button
                key={sport}
                role="tab"
                aria-selected={activeSport === sport}
                className={`predictions-tab${activeSport === sport ? " active" : ""}`}
                onClick={() => handleSportChange(sport)}
                style={
                  activeSport === sport
                    ? { borderBottomColor: color, color }
                    : undefined
                }
              >
                {getDisplayName(sport)}{" "}
                <span className="tab-count">{count}</span>
              </button>
            );
          })}
        </div>

        {!hasPremium && (
          <div className="stale-banner stale-banner-info" style={{ marginBottom: "var(--space-4)" }}>
            <span className="stale-banner-icon" aria-hidden="true">🔒</span>
            <span className="stale-banner-text">
              Free tier includes a limited preview of each slate. Upgrade to unlock full probabilities, spread, total, confidence, and the complete board.
            </span>
          </div>
        )}

        {hasPremium && (
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "var(--space-3)",
              marginBottom: "var(--space-4)",
              fontSize: "var(--text-sm)",
            }}
          >
            <label htmlFor="confidence-filter" style={{ color: "var(--text-muted)", whiteSpace: "nowrap" }}>
              Min confidence:
            </label>
            <input
              id="confidence-filter"
              type="range"
              min={0}
              max={90}
              step={5}
              value={minConfidence}
              onChange={(e) => { setMinConfidence(Number(e.target.value)); setPage(1); }}
              aria-label="Minimum confidence threshold"
              aria-valuenow={minConfidence}
              aria-valuetext={minConfidence > 0 ? `${minConfidence} percent` : "All predictions"}
              style={{ flex: 1, maxWidth: 200, accentColor: "var(--color-accent, #2563eb)" }}
            />
            <span style={{ fontWeight: 700, fontVariantNumeric: "tabular-nums", minWidth: "3ch" }}>
              {minConfidence > 0 ? `${minConfidence}%` : "All"}
            </span>
          </div>
        )}

        <div
          aria-live="polite"
          style={{
            fontSize: "var(--text-sm)",
            color: "var(--color-text-secondary)",
            marginBottom: "var(--space-4)",
          }}
        >
          {sorted.length} prediction{sorted.length !== 1 ? "s" : ""} for{" "}
          <strong>{displayDate}</strong>
          {!hasPremium ? " (preview)" : ""}
        </div>

        {fetchError && (
          <div role="alert" style={{
            padding: "var(--space-3) var(--space-4)",
            background: "var(--color-loss-bg, #fef2f2)",
            color: "var(--color-loss, #dc2626)",
            borderRadius: "var(--radius-md)",
            marginBottom: "var(--space-4)",
            fontSize: "var(--text-sm)",
          }}>
            {fetchError}
          </div>
        )}

        {initialWarning && (
          <div role="status" style={{
            padding: "var(--space-3) var(--space-4)",
            background: "var(--color-warning-bg, #fffbeb)",
            color: "var(--color-warning, #b45309)",
            borderRadius: "var(--radius-md)",
            marginBottom: "var(--space-4)",
            fontSize: "var(--text-sm)",
          }}>
            {initialWarning}
          </div>
        )}

        {sorted.length === 0 ? (
          <div className="card">
            <div className="card-body">
              <p>No predictions available{isToday ? " for today" : ` for ${displayDate}`}.</p>
            </div>
          </div>
        ) : (
          <div className="card">
            <div
              className="card-body responsive-table-wrap predictions-table-wrap"
              style={{
                padding: 0,
              }}
            >
              <table
                className="data-table table-striped sticky-header responsive-table predictions-table"
                style={{ width: "100%" }}
                aria-label={`${sorted.length} predictions for ${displayDate}`}
              >
                <thead>
                  <tr>
                    <th scope="col" style={thPad}>Sport</th>
                    <th scope="col" style={thPad}>Matchup</th>
                    <th scope="col" style={{ ...thPad, textAlign: "right" }}>Home</th>
                    <th scope="col" style={{ ...thPad, textAlign: "right" }}>Away</th>
                    <th scope="col" style={{ ...thPad, textAlign: "right" }}>Spread</th>
                    <th scope="col" style={{ ...thPad, textAlign: "right" }}>Total</th>
                    <th scope="col" style={{ ...thPad, textAlign: "center" }}>Conf</th>
                  </tr>
                </thead>
                <tbody>
                  {pageSlice.map((pred, i) => {
                    const matchup =
                      pred.home_team && pred.away_team
                        ? `${pred.home_team} vs ${pred.away_team}`
                        : pred.game_id;
                    const sport = pred.sport?.toLowerCase() ?? "nba";
                    const href = `/games/${sport}/${pred.game_id}`;
                    return (
                      <tr
                        key={`${pred.sport}-${pred.game_id}-${i}`}
                        onClick={() => router.push(href)}
                        onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); router.push(href); } }}
                        tabIndex={0}
                        role="link"
                        aria-label={`View ${matchup} prediction — ${Math.round((pred.confidence ?? 0) * 100)}% confidence`}
                        className="clickable-row"
                        style={{ cursor: "pointer" }}
                      >
                        <td style={{ padding: "0.25rem 0.4rem" }}>
                          <Badge variant="free">
                            {pred.sport?.toUpperCase()}
                          </Badge>
                        </td>
                        <td
                          style={{
                            fontWeight: 600,
                            whiteSpace: "nowrap",
                            padding: "0.25rem 0.4rem",
                          }}
                        >
                          <Link
                            href={href}
                            style={{
                              color: "var(--color-accent, #6366f1)",
                              textDecoration: "none",
                            }}
                          >
                            {matchup} →
                          </Link>
                        </td>
                        <td
                          style={winProbStyle(
                            pred.home_win_prob,
                            pred.away_win_prob,
                          )}
                        >
                          {hasPremium
                            ? (pred.home_win_prob != null
                              ? `${(pred.home_win_prob * 100).toFixed(1)}%`
                              : "—")
                            : "🔒"}
                        </td>
                        <td
                          style={winProbStyle(
                            pred.away_win_prob,
                            pred.home_win_prob,
                          )}
                        >
                          {hasPremium
                            ? (pred.away_win_prob != null
                              ? `${(pred.away_win_prob * 100).toFixed(1)}%`
                              : "—")
                            : "🔒"}
                        </td>
                        <td style={numCell}>
                          {hasPremium
                            ? (pred.predicted_spread != null
                              ? pred.predicted_spread.toFixed(1)
                              : "—")
                            : "🔒"}
                        </td>
                        <td style={numCell}>
                          {hasPremium
                            ? (pred.predicted_total != null
                              ? pred.predicted_total.toFixed(1)
                              : "—")
                            : "🔒"}
                        </td>
                        <td style={centerCell}>
                          {hasPremium && pred.confidence != null ? (
                            <Badge
                              variant={
                                pred.confidence >= 0.7
                                  ? "win"
                                  : pred.confidence >= 0.55
                                    ? "free"
                                    : "loss"
                              }
                            >
                              {formatProbability(pred.confidence)}
                            </Badge>
                          ) : (
                            hasPremium ? "—" : "🔒"
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <Pagination
              page={safeP}
              totalPages={totalPages}
              onPageChange={setPage}
            />
          </div>
        )}
      </SectionBand>
    </main>
  );
}
