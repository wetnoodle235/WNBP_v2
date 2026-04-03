"use client";

import { useState, useMemo, useCallback } from "react";
import Link from "next/link";
import { SectionBand, Pagination } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { useDebounce } from "@/lib/hooks";
import { StatTrendChart, LeadersBarChart } from "@/components/charts";
import {
  type SortDir,
  type ColumnDef,
  PLAYER_COLUMNS,
  MLB_PITCHER_COLUMNS,
  TEAM_COLUMNS,
  DEFAULT_SORT,
} from "@/lib/stats-columns";

/* eslint-disable @typescript-eslint/no-explicit-any */

function resolveTeamName(teamId: any, teamMap?: Record<string, string>): string {
  if (!teamId) return "—";
  const id = String(teamId);
  if (teamMap && teamMap[id]) return teamMap[id];
  if (id.length <= 5 && /^[A-Z]/.test(id)) return id;
  if (id.length <= 5) return id.toUpperCase();
  return id;
}

interface Props {
  playerStatsBySport: Record<string, any[]>;
  teamStatsBySport: Record<string, any[]>;
  teamMapBySport?: Record<string, Record<string, string>>;
  sports: string[];
  initialWarning?: string | null;
}

const PER_PAGE = 20;

export function StatsClient({ playerStatsBySport, teamStatsBySport, teamMapBySport, sports, initialWarning = null }: Props) {
  // Show all sports as tabs — sports without data get an empty-state message
  const hasSportData = (s: string) =>
    (playerStatsBySport[s]?.length ?? 0) > 0 || (teamStatsBySport[s]?.length ?? 0) > 0;
  const [activeSport, setActiveSport] = useState(
    sports.find(hasSportData) ?? sports[0] ?? "nba",
  );
  const [tab, setTab] = useState<"player" | "team">("player");
  const [sortKey, setSortKey] = useState<string | null>(DEFAULT_SORT[sports[0] ?? "nba"] ?? null);
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [page, setPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState("");
  const debouncedSearch = useDebounce(searchQuery, 250);
  const [mlbView, setMlbView] = useState<"batters" | "pitchers">("batters");

  const columns = useMemo(() => {
    if (tab === "team") return TEAM_COLUMNS;
    if (activeSport === "mlb" && mlbView === "pitchers") return MLB_PITCHER_COLUMNS;
    return PLAYER_COLUMNS[activeSport] ?? PLAYER_COLUMNS.nba;
  }, [tab, activeSport, mlbView]);

  // Data is pre-aggregated from backend — just resolve team names
  const currentTeamMap = teamMapBySport?.[activeSport];

  const processedPlayers = useMemo(() => {
    const raw = playerStatsBySport[activeSport] ?? [];
    let players = raw.map((r: any) => ({
      ...r,
      player_name: r.player_name ?? r.player_id ?? "—",
      team: resolveTeamName(r.team_id, currentTeamMap),
    }));
    // MLB: filter to batters or pitchers based on toggle
    if (activeSport === "mlb") {
      if (mlbView === "batters") {
        players = players.filter((p: any) => (p.ab ?? 0) > 0);
      } else {
        players = players.filter((p: any) => (p.innings_pitched ?? p.ip ?? 0) > 0);
      }
    }
    // Apply search filter
    if (debouncedSearch.trim()) {
      const q = debouncedSearch.toLowerCase();
      players = players.filter((p: any) =>
        (p.player_name ?? "").toLowerCase().includes(q) ||
        (p.team ?? "").toLowerCase().includes(q)
      );
    }
    return players;
  }, [activeSport, playerStatsBySport, currentTeamMap, mlbView, debouncedSearch]);

  const processedTeams = useMemo(() => {
    const raw = teamStatsBySport[activeSport] ?? [];
    return raw.map((t: any) => ({
      ...t,
      name: t.name ?? t.team ?? t.team_name ?? "—",
      wins: t.wins ?? t.w,
      losses: t.losses ?? t.l,
      points_for: t.points_for ?? t.ppg,
      points_against: t.points_against ?? t.oppg,
      diff: t.diff ?? (((t.points_for ?? 0) - (t.points_against ?? 0)) || null),
    }));
  }, [activeSport, teamStatsBySport]);

  const rawData = useMemo(() => {
    if (tab === "team") return processedTeams;
    return processedPlayers;
  }, [tab, processedTeams, processedPlayers]);

  const sorted = useMemo(() => {
    if (!sortKey) return rawData;
    return [...rawData].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = typeof av === "string" ? av.localeCompare(bv) : Number(av) - Number(bv);
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [rawData, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / PER_PAGE));
  const safeP = Math.min(page, totalPages);
  const pageSlice = sorted.slice((safeP - 1) * PER_PAGE, safeP * PER_PAGE);

  const handleSportChange = useCallback((sport: string) => {
    setActiveSport(sport);
    setSortKey(DEFAULT_SORT[sport] ?? null);
    setSortDir("desc");
    setPage(1);
    setSearchQuery("");
    setMlbView("batters");
  }, []);

  const handleSort = useCallback((key: string) => {
    setSortKey((prev) => {
      if (prev === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prev;
      }
      setSortDir("desc");
      return key;
    });
    setPage(1);
  }, []);

  const thStyle: React.CSSProperties = {
    padding: "var(--space-2) var(--space-4)",
    textAlign: "left",
    fontSize: "0.75rem",
    fontWeight: 600,
    textTransform: "uppercase",
    color: "var(--color-text-muted)",
    borderBottom: "2px solid var(--color-border)",
    cursor: "pointer",
    userSelect: "none",
    whiteSpace: "nowrap",
  };

  const tdStyle: React.CSSProperties = {
    padding: "var(--space-2) var(--space-4)",
    fontSize: "0.875rem",
    borderBottom: "1px solid var(--color-border)",
    whiteSpace: "nowrap",
  };

  return (
    <main>
      <SectionBand title="Stats">
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

        {/* Sport filter tabs */}
        <div
          className="stats-sport-tabs"
          role="tablist"
          aria-label="Filter stats by sport"
          style={{
            display: "flex",
            gap: "var(--space-2)",
            flexWrap: "wrap",
            marginBottom: "var(--space-4)",
          }}
        >
          {sports.map((sport) => {
            const isActive = activeSport === sport;
            const color = getSportColor(sport);
            const hasData = hasSportData(sport);
            return (
              <button
                key={sport}
                role="tab"
                aria-selected={isActive}
                onClick={() => handleSportChange(sport)}
                style={{
                  padding: "var(--space-2) var(--space-4)",
                  borderRadius: "var(--radius-full, 9999px)",
                  border: `1px solid ${isActive ? color : hasData ? color : "var(--color-border)"}`,
                  backgroundColor: isActive ? color : "transparent",
                  color: isActive ? "#fff" : hasData ? "inherit" : "var(--color-text-muted)",
                  cursor: "pointer",
                  fontSize: "var(--text-sm)",
                  fontWeight: "var(--fw-semibold, 600)",
                  opacity: hasData ? 1 : 0.55,
                }}
              >
                {getDisplayName(sport)}
              </button>
            );
          })}
        </div>

        {/* Player / Team toggle */}
        <div
          className="stats-mode-toggle"
          style={{
            display: "flex",
            gap: "var(--space-2)",
            marginBottom: "var(--space-6)",
          }}
        >
          <button
            aria-pressed={tab === "player"}
            onClick={() => { setTab("player"); setSortKey(DEFAULT_SORT[activeSport] ?? "pts"); setPage(1); }}
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-md, 6px)",
              border: "1px solid var(--color-border)",
              backgroundColor: tab === "player" ? "var(--color-accent, #2563eb)" : "transparent",
              color: tab === "player" ? "#fff" : "inherit",
              cursor: "pointer",
              fontSize: "0.875rem",
              fontWeight: 600,
            }}
          >
            Player Stats
          </button>
          <button
            aria-pressed={tab === "team"}
            onClick={() => { setTab("team"); setSortKey("wins"); setPage(1); }}
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-md, 6px)",
              border: "1px solid var(--color-border)",
              backgroundColor: tab === "team" ? "var(--color-accent, #2563eb)" : "transparent",
              color: tab === "team" ? "#fff" : "inherit",
              cursor: "pointer",
              fontSize: "0.875rem",
              fontWeight: 600,
            }}
          >
            Team Stats
          </button>
          <span
            style={{
              marginLeft: "auto",
              fontSize: "0.875rem",
              color: "var(--color-text-muted)",
              alignSelf: "center",
            }}
          >
            {sorted.length} {tab === "player" ? "player" : "team"}
            {sorted.length !== 1 ? "s" : ""}
          </span>
        </div>

        {/* Search + MLB pitcher/batter toggle */}
        <div className="stats-search-row" style={{ display: "flex", gap: "var(--space-3)", marginBottom: "var(--space-4)", flexWrap: "wrap", alignItems: "center" }}>
          <input
            type="text"
            placeholder={tab === "player" ? "Search players..." : "Search teams..."}
            value={searchQuery}
            onChange={(e) => { setSearchQuery(e.target.value); setPage(1); }}
            aria-label={tab === "player" ? "Search players by name" : "Search teams by name"}
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-md, 6px)",
              border: "1px solid var(--color-border)",
              backgroundColor: "var(--bg-surface, #1a1a2e)",
              color: "inherit",
              fontSize: "0.875rem",
              width: "240px",
            }}
          />
          {activeSport === "mlb" && tab === "player" && (
            <div role="group" aria-label="MLB player type" style={{ display: "flex", gap: "var(--space-2)" }}>
              <button
                onClick={() => { setMlbView("batters"); setSortKey("avg"); setPage(1); }}
                aria-pressed={mlbView === "batters"}
                style={{
                  padding: "var(--space-1) var(--space-3)",
                  borderRadius: "var(--radius-md, 6px)",
                  border: "1px solid var(--color-border)",
                  backgroundColor: mlbView === "batters" ? "var(--color-accent, #2563eb)" : "transparent",
                  color: mlbView === "batters" ? "#fff" : "inherit",
                  cursor: "pointer",
                  fontSize: "0.8rem",
                  fontWeight: 600,
                }}
              >
                Batters
              </button>
              <button
                onClick={() => { setMlbView("pitchers"); setSortKey("era"); setPage(1); }}
                aria-pressed={mlbView === "pitchers"}
                style={{
                  padding: "var(--space-1) var(--space-3)",
                  borderRadius: "var(--radius-md, 6px)",
                  border: "1px solid var(--color-border)",
                  backgroundColor: mlbView === "pitchers" ? "var(--color-accent, #2563eb)" : "transparent",
                  color: mlbView === "pitchers" ? "#fff" : "inherit",
                  cursor: "pointer",
                  fontSize: "0.8rem",
                  fontWeight: 600,
                }}
              >
                Pitchers
              </button>
            </div>
          )}
        </div>

        {sorted.length === 0 ? (
          <div
            className="card"
            style={{
              padding: "var(--space-8)",
              textAlign: "center",
              color: "var(--color-text-muted)",
            }}
          >
            No {tab} stats available for {getDisplayName(activeSport)}.
          </div>
        ) : (
          <>
            <div className="card">
              <div className="responsive-table-wrap">
                <table className="responsive-table stats-table" style={{ borderCollapse: "collapse" }} aria-label={`${getDisplayName(activeSport)} ${tab} statistics`}>
                <thead>
                  <tr>
                    {columns.map((col) => (
                      <th
                        key={col.key}
                        scope="col"
                        role="columnheader"
                        aria-sort={sortKey === col.key ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
                        tabIndex={0}
                        style={thStyle}
                        onClick={() => handleSort(col.key)}
                        onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort(col.key); } }}
                      >
                        {col.label}
                        {sortKey === col.key && (
                          <span style={{ marginLeft: 4 }} aria-hidden="true">
                            {sortDir === "asc" ? "↑" : "↓"}
                          </span>
                        )}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pageSlice.map((row: any, i: number) => (
                    <tr
                      key={row.player_id ?? row.name ?? i}
                      style={{
                        backgroundColor: i % 2 === 0 ? "transparent" : "rgba(128,128,128,0.04)",
                      }}
                    >
                      {columns.map((col) => (
                        <td
                          key={col.key}
                          style={{
                            ...tdStyle,
                            fontWeight: col.key === "player_name" || col.key === "name" ? 600 : 400,
                          }}
                        >
                          {col.key === "player_name" && row.player_id ? (
                            <Link
                              href={`/players/${row.player_id}?sport=${activeSport}`}
                              style={{ color: "var(--color-brand)", textDecoration: "none" }}
                            >
                              {col.format ? col.format(row[col.key]) : (row[col.key] ?? "—")}
                            </Link>
                          ) : (
                            col.format ? col.format(row[col.key]) : (row[col.key] ?? "—")
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
                </table>
              </div>
            </div>

            <Pagination page={safeP} totalPages={totalPages} onPageChange={setPage} />

          {/* Inline chart panels below the table */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: "var(--space-5)", marginTop: "var(--space-6)" }}>
            <LeadersBarChart
              sport={activeSport}
              stat={sortKey ?? "pts"}
              limit={10}
              title={`${sortKey ?? "pts"} Leaders`}
            />
            <StatTrendChart
              sport={activeSport}
              stat={sortKey ?? "pts"}
              limit={20}
              title={`${sortKey ?? "pts"} — Recent Trend`}
            />
          </div>
          </>
        )}
      </SectionBand>
    </main>
  );
}
