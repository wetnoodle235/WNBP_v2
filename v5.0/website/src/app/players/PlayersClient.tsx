"use client";

import { useState, useMemo, useCallback } from "react";
import Image from "next/image";
import Link from "next/link";
import { SectionBand, Badge } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { useDebounce } from "@/lib/hooks";
import type { Player, TeamInfo } from "./page";

const PAGE_SIZE = 100;

interface PlayersClientProps {
  playersBySport: Record<string, Player[]>;
  teamLookup: Record<string, TeamInfo>;
  sports: string[];
}

export function PlayersClient({
  playersBySport,
  teamLookup,
  sports,
}: PlayersClientProps) {
  const [activeSport, setActiveSport] = useState(sports[0] ?? "nba");
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, 250);
  const [positionFilter, setPositionFilter] = useState("");
  const [teamFilter, setTeamFilter] = useState("");
  const [page, setPage] = useState(0);

  const players = playersBySport[activeSport] ?? [];

  const positions = useMemo(() => {
    const set = new Set<string>();
    for (const p of players) {
      if (p.position) set.add(p.position);
    }
    return Array.from(set).sort();
  }, [players]);

  const teams = useMemo(() => {
    const map = new Map<string, string>();
    for (const p of players) {
      const tName = p.team_name;
      if (tName) {
        map.set(p.team_id || tName, tName);
      } else if (p.team_id) {
        const t = teamLookup[`${activeSport}-${p.team_id}`];
        if (t) map.set(p.team_id, t.name);
      }
    }
    return Array.from(map.entries())
      .sort((a, b) => a[1].localeCompare(b[1]))
      .map(([id, name]) => ({ id, name }));
  }, [players, teamLookup, activeSport]);

  const filtered = useMemo(() => {
    const q = debouncedSearch.toLowerCase();
    return players.filter((p) => {
      if (q && !p.name.toLowerCase().includes(q)) return false;
      if (positionFilter && p.position !== positionFilter) return false;
      if (teamFilter && p.team_id !== teamFilter) return false;
      return true;
    });
  }, [players, debouncedSearch, positionFilter, teamFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages - 1);
  const paged = filtered.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  const handleSportChange = useCallback((sport: string) => {
    setActiveSport(sport);
    setSearch("");
    setPositionFilter("");
    setTeamFilter("");
    setPage(0);
  }, []);

  function getTeamName(player: Player): string | null {
    // Prefer direct team_name from normalized data
    if (player.team_name) return player.team_name;
    if (!player.team_id) return null;
    return teamLookup[`${activeSport}-${player.team_id}`]?.name ?? null;
  }

  function getInitials(name: string): string {
    return name
      .split(" ")
      .map((w) => w[0])
      .join("")
      .toUpperCase()
      .slice(0, 2);
  }

  // Styles
  const selectStyle: React.CSSProperties = {
    background: "var(--color-bg-2)",
    border: "1px solid var(--color-border)",
    color: "var(--color-text)",
    borderRadius: 6,
    padding: "0.45rem 0.65rem",
    fontSize: "var(--text-sm)",
    minWidth: 140,
  };

  return (
    <SectionBand title="Players">
      {/* Sport pills */}
      <div className="players-sport-tabs" role="tablist" aria-label="Filter players by sport" style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginBottom: "1rem" }}>
        {sports.map((sport) => (
          <button
            key={sport}
            role="tab"
            aria-selected={activeSport === sport}
            onClick={() => handleSportChange(sport)}
            style={{
              padding: "0.4rem 1rem",
              borderRadius: 999,
              border: activeSport === sport ? "2px solid" : "1px solid var(--color-border)",
              borderColor: activeSport === sport ? getSportColor(sport) : "var(--color-border)",
              background: activeSport === sport ? getSportColor(sport) : "var(--color-bg-2)",
              color: activeSport === sport ? "#fff" : "var(--color-text)",
              cursor: "pointer",
              fontWeight: activeSport === sport ? 700 : 500,
              fontSize: "var(--text-sm)",
              transition: "all 0.15s ease",
            }}
          >
            {getDisplayName(sport)}
          </button>
        ))}
      </div>

      {/* Filters row */}
      <div
        className="players-filters-row"
        style={{
          display: "flex",
          gap: "0.75rem",
          flexWrap: "wrap",
          alignItems: "center",
          marginBottom: "1rem",
        }}
      >
        <label className="sr-only" htmlFor="players-search">Search players</label>
        <input
          id="players-search"
          type="text"
          placeholder="Search players…"
          value={search}
          onChange={(e) => {
            setSearch(e.target.value);
            setPage(0);
          }}
          style={{
            background: "var(--color-bg-2)",
            border: "1px solid var(--color-border)",
            color: "var(--color-text)",
            borderRadius: 6,
            padding: "0.45rem 0.75rem",
            fontSize: "var(--text-sm)",
            flex: "1 1 200px",
            minWidth: 180,
          }}
        />

        <label className="sr-only" htmlFor="players-position">Filter by position</label>
        <select
          id="players-position"
          value={positionFilter}
          onChange={(e) => {
            setPositionFilter(e.target.value);
            setPage(0);
          }}
          style={selectStyle}
        >
          <option value="">All Positions</option>
          {positions.map((pos) => (
            <option key={pos} value={pos}>
              {pos}
            </option>
          ))}
        </select>

        <label className="sr-only" htmlFor="players-team">Filter by team</label>
        <select
          id="players-team"
          value={teamFilter}
          onChange={(e) => {
            setTeamFilter(e.target.value);
            setPage(0);
          }}
          style={selectStyle}
        >
          <option value="">All Teams</option>
          {teams.map((t) => (
            <option key={t.id} value={t.id}>
              {t.name}
            </option>
          ))}
        </select>

        <span
          style={{
            fontSize: "var(--text-sm)",
            color: "var(--color-text-muted)",
            marginLeft: "auto",
          }}
        >
          {filtered.length} player{filtered.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Player grid */}
      {paged.length === 0 ? (
        <div className="card">
          <div className="card-body">
            <p style={{ color: "var(--color-text-muted)" }}>
              No players found{search ? ` matching "${search}"` : ""}.
            </p>
          </div>
        </div>
      ) : (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 280px), 1fr))",
            gap: "1rem",
          }}
        >
          {paged.map((player) => {
            const teamName = getTeamName(player);
            return (
              <Link
                key={`${player.sport}-${player.id}`}
                href={`/players/${player.id}?sport=${activeSport}`}
                style={{ textDecoration: "none", color: "inherit" }}
              >
                <div
                  className="card"
                  style={{
                    padding: "1rem",
                    display: "flex",
                    gap: "0.75rem",
                    alignItems: "center",
                    transition: "border-color 0.15s ease",
                    cursor: "pointer",
                  }}
                >
                  {/* Headshot */}
                  {player.headshot_url ? (
                    <div
                      style={{
                        width: 56,
                        height: 56,
                        borderRadius: "50%",
                        overflow: "hidden",
                        flexShrink: 0,
                        background: "var(--color-bg-3)",
                        position: "relative",
                      }}
                    >
                      <Image
                        src={player.headshot_url}
                        alt={player.name}
                        width={56}
                        height={56}
                        style={{ objectFit: "cover" }}
                        unoptimized
                      />
                    </div>
                  ) : (
                    <div
                      style={{
                        width: 56,
                        height: 56,
                        borderRadius: "50%",
                        background: "var(--color-brand-subtle)",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        fontSize: 18,
                        fontWeight: 700,
                        color: "var(--color-brand)",
                        flexShrink: 0,
                      }}
                    >
                      {getInitials(player.name)}
                    </div>
                  )}

                  {/* Info */}
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div
                      style={{
                        fontWeight: 600,
                        fontSize: "var(--text-sm)",
                        color: "var(--color-text)",
                        whiteSpace: "nowrap",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                      }}
                    >
                      {player.name}
                    </div>

                    {teamName && (
                      <div
                        style={{
                          fontSize: "var(--text-xs)",
                          color: "var(--color-text-muted)",
                          marginTop: 2,
                        }}
                      >
                        {teamName}
                      </div>
                    )}

                    <div
                      style={{
                        display: "flex",
                        gap: "0.4rem",
                        flexWrap: "wrap",
                        alignItems: "center",
                        marginTop: 6,
                      }}
                    >
                      {player.position && (
                        <Badge variant={activeSport}>{player.position}</Badge>
                      )}
                      {player.jersey_number != null && (
                        <span
                          style={{
                            fontSize: "var(--text-xs)",
                            color: "var(--color-text-muted)",
                            fontWeight: 600,
                          }}
                        >
                          #{Math.floor(player.jersey_number)}
                        </span>
                      )}
                      {(player.height || player.weight) && (
                        <span
                          style={{
                            fontSize: "var(--text-xs)",
                            color: "var(--color-text-muted)",
                          }}
                        >
                          {[player.height, player.weight ? `${Math.floor(player.weight)} lbs` : null]
                            .filter(Boolean)
                            .join(" · ")}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              </Link>
            );
          })}
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            gap: "0.5rem",
            marginTop: "1.5rem",
          }}
        >
          <button
            disabled={safePage === 0}
            onClick={() => setPage((p) => Math.max(0, p - 1))}
            style={{
              padding: "0.4rem 0.8rem",
              borderRadius: 6,
              border: "1px solid var(--color-border)",
              background: "var(--color-bg-2)",
              color: safePage === 0 ? "var(--color-text-muted)" : "var(--color-text)",
              cursor: safePage === 0 ? "not-allowed" : "pointer",
              fontSize: "var(--text-sm)",
              opacity: safePage === 0 ? 0.5 : 1,
            }}
          >
            ← Prev
          </button>

          {generatePageNumbers(safePage, totalPages).map((num, i) =>
            num === -1 ? (
              <span
                key={`ellipsis-${i}`}
                style={{ color: "var(--color-text-muted)", padding: "0 0.25rem" }}
              >
                …
              </span>
            ) : (
              <button
                key={num}
                onClick={() => setPage(num)}
                aria-current={safePage === num ? "page" : undefined}
                style={{
                  padding: "0.35rem 0.65rem",
                  borderRadius: 6,
                  border: safePage === num ? "2px solid var(--color-brand)" : "1px solid var(--color-border)",
                  background: safePage === num ? "var(--color-brand)" : "var(--color-bg-2)",
                  color: safePage === num ? "#fff" : "var(--color-text)",
                  cursor: "pointer",
                  fontWeight: safePage === num ? 700 : 400,
                  fontSize: "var(--text-sm)",
                  minWidth: 36,
                }}
              >
                {num + 1}
              </button>
            ),
          )}

          <button
            disabled={safePage >= totalPages - 1}
            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
            style={{
              padding: "0.4rem 0.8rem",
              borderRadius: 6,
              border: "1px solid var(--color-border)",
              background: "var(--color-bg-2)",
              color: safePage >= totalPages - 1 ? "var(--color-text-muted)" : "var(--color-text)",
              cursor: safePage >= totalPages - 1 ? "not-allowed" : "pointer",
              fontSize: "var(--text-sm)",
              opacity: safePage >= totalPages - 1 ? 0.5 : 1,
            }}
          >
            Next →
          </button>
        </div>
      )}
    </SectionBand>
  );
}

/** Generate page number array with ellipsis (-1) for large page counts. */
function generatePageNumbers(current: number, total: number): number[] {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i);

  const pages: number[] = [0];
  const start = Math.max(1, current - 1);
  const end = Math.min(total - 2, current + 1);

  if (start > 1) pages.push(-1);
  for (let i = start; i <= end; i++) pages.push(i);
  if (end < total - 2) pages.push(-1);
  pages.push(total - 1);

  return pages;
}
