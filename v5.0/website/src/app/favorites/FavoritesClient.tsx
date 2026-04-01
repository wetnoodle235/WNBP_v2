"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import Link from "next/link";
import { SectionBand, EmptyState, FavoriteButton } from "@/components/ui";
import { useFavorites } from "@/lib/hooks";
import { resolveServerApiBase } from "@/lib/api-base";

interface FavoriteGameInfo {
  game_id: string;
  sport?: string;
  home_team?: string;
  away_team?: string;
  date?: string;
  status?: string;
}

const API_BASE =
  typeof window === "undefined"
    ? resolveServerApiBase()
    : "/api/proxy";

export function FavoritesClient() {
  const { favorites, toggle, count } = useFavorites();
  const [gameDetails, setGameDetails] = useState<Map<string, FavoriteGameInfo>>(new Map());
  const [loading, setLoading] = useState(true);
  const abortRef = useRef<AbortController | null>(null);

  const fetchGameDetails = useCallback(async (ids: string[], signal?: AbortSignal) => {
    if (ids.length === 0) {
      setLoading(false);
      return;
    }
    setLoading(true);
    const details = new Map<string, FavoriteGameInfo>();

    // Try to fetch details for each game
    await Promise.allSettled(
      ids.map(async (id) => {
        // Game IDs are formatted as "sport-gameid" or just "gameid"
        const parts = id.split("-");
        const sport = parts.length > 1 ? parts[0] : null;
        if (!sport) {
          details.set(id, { game_id: id });
          return;
        }
        try {
          const res = await fetch(`${API_BASE}/v1/${sport}/games/${parts.slice(1).join("-")}`, {
            cache: "no-store",
            signal,
          });
          if (res.ok) {
            const json = await res.json();
            const data = json?.data ?? json;
            details.set(id, {
              game_id: id,
              sport,
              home_team: data?.home_team ?? data?.home_team_name,
              away_team: data?.away_team ?? data?.away_team_name,
              date: data?.date ?? data?.game_date,
              status: data?.status,
            });
          } else {
            details.set(id, { game_id: id, sport });
          }
        } catch {
          details.set(id, { game_id: id, sport });
        }
      }),
    );

    if (!signal?.aborted) {
      setGameDetails(details);
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    fetchGameDetails([...favorites], ac.signal);
    return () => ac.abort();
  }, [favorites, fetchGameDetails]);

  if (loading) {
    return (
      <SectionBand title="Favorites">
        <div style={{ textAlign: "center", padding: "3rem 0", color: "var(--color-text-muted)" }}>
          Loading your favorites…
        </div>
      </SectionBand>
    );
  }

  if (count === 0) {
    return (
      <SectionBand title="Favorites">
        <EmptyState
          icon="⭐"
          title="No Favorites Yet"
          description="Click the heart icon on any game card to bookmark it here."
          action={
            <Link href="/live" className="btn btn-primary" style={{ marginTop: "1rem" }}>
              Browse Live Games
            </Link>
          }
        />
      </SectionBand>
    );
  }

  const sortedFavorites = [...favorites].sort((a, b) => {
    const ga = gameDetails.get(a);
    const gb = gameDetails.get(b);
    // Sort by date descending, then by sport
    if (ga?.date && gb?.date) return gb.date.localeCompare(ga.date);
    return a.localeCompare(b);
  });

  return (
    <SectionBand title={`Favorites (${count})`}>
      <p style={{ color: "var(--color-text-muted)", marginBottom: "1.5rem", fontSize: "var(--text-sm)" }}>
        Your bookmarked games. Favorites are stored locally in your browser.
      </p>
      <div className="favorites-grid">
        {sortedFavorites.map((id) => {
          const info = gameDetails.get(id);
          const sport = info?.sport ?? "unknown";
          return (
            <div key={id} className="favorite-card">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div>
                  {info?.sport && (
                    <span
                      style={{
                        fontSize: "0.7rem",
                        fontWeight: 700,
                        textTransform: "uppercase",
                        color: "var(--color-primary)",
                        letterSpacing: "0.05em",
                      }}
                    >
                      {info.sport}
                    </span>
                  )}
                  <div style={{ fontWeight: 600, marginTop: 4 }}>
                    {info?.away_team && info?.home_team
                      ? `${info.away_team} @ ${info.home_team}`
                      : `Game ${id}`}
                  </div>
                  {info?.date && (
                    <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)", marginTop: 2 }}>
                      {info.date}
                    </div>
                  )}
                  {info?.status && (
                    <div
                      style={{
                        fontSize: "0.75rem",
                        fontWeight: 600,
                        marginTop: 4,
                        color:
                          info.status === "live"
                            ? "var(--color-loss, #dc2626)"
                            : info.status === "final"
                            ? "var(--color-text-muted)"
                            : "var(--color-win, #16a34a)",
                      }}
                    >
                      {info.status.toUpperCase()}
                    </div>
                  )}
                </div>
                <FavoriteButton id={id} />
              </div>
              <Link
                href={`/games/${sport}`}
                className="btn btn-sm"
                style={{ marginTop: 12, alignSelf: "flex-start" }}
              >
                View Games →
              </Link>
            </div>
          );
        })}
      </div>
    </SectionBand>
  );
}
