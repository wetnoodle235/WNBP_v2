"use client";

import { useState, useMemo, useCallback } from "react";
import { SectionBand, Pagination } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { useDebounce } from "@/lib/hooks";

interface TeamItem {
  id: string;
  sport: string;
  name: string;
  abbreviation?: string | null;
  city?: string | null;
  conference?: string | null;
  division?: string | null;
  logo_url?: string | null;
  color_primary?: string | null;
}

interface Props {
  teams: TeamItem[];
  sports: string[];
}

const PER_PAGE = 24;

function getTeamLogoUrl(sport: string, abbreviation: string | null | undefined): string | null {
  if (!abbreviation) return null;
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
  return `https://a.espncdn.com/i/teamlogos/${espnSport}/500/${abbreviation.toLowerCase()}.png`;
}

export function TeamsClient({ teams, sports }: Props) {
  const [activeSport, setActiveSport] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, 250);
  const [page, setPage] = useState(1);
  const [logoErrors, setLogoErrors] = useState<Set<string>>(new Set());

  const handleLogoError = useCallback((teamKey: string) => {
    setLogoErrors((prev) => {
      const next = new Set(prev);
      next.add(teamKey);
      return next;
    });
  }, []);

  const filtered = useMemo(() => {
    const q = debouncedSearch.toLowerCase();
    return teams.filter((t) => {
      if (activeSport && t.sport !== activeSport) return false;
      if (q) {
        const searchable = `${t.name} ${t.city ?? ""} ${t.abbreviation ?? ""}`.toLowerCase();
        if (!searchable.includes(q)) return false;
      }
      return true;
    });
  }, [teams, activeSport, debouncedSearch]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER_PAGE));
  const safeP = Math.min(page, totalPages);
  const pageSlice = filtered.slice((safeP - 1) * PER_PAGE, safeP * PER_PAGE);

  const handleSportChange = useCallback((sport: string | null) => {
    setActiveSport(sport);
    setSearch("");
    setPage(1);
  }, []);

  return (
    <main>
      <SectionBand title="Teams">
        {/* Sport filter tabs */}
        <div
          className="teams-sport-tabs"
          role="tablist"
          aria-label="Filter teams by sport"
          style={{
            display: "flex",
            gap: "var(--space-2)",
            flexWrap: "wrap",
            marginBottom: "var(--space-4)",
          }}
        >
          <button
            role="tab"
            aria-selected={!activeSport}
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
                role="tab"
                aria-selected={isActive}
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

        {/* Search + count */}
        <div
          className="teams-search-row"
          style={{
            display: "flex",
            gap: "var(--space-4)",
            alignItems: "center",
            marginBottom: "var(--space-6)",
            flexWrap: "wrap",
          }}
        >
          <input
            type="search"
            placeholder="Search teams…"
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(1);
            }}
            aria-label="Search teams by name, city, or abbreviation"
            style={{
              background: "var(--color-bg-2, transparent)",
              border: "1px solid var(--border)",
              color: "inherit",
              borderRadius: "var(--radius-md, 6px)",
              padding: "var(--space-2) var(--space-4)",
              fontSize: "0.875rem",
              flex: "1 1 200px",
              minWidth: 180,
            }}
          />
          <span style={{ fontSize: "0.875rem", color: "var(--text-muted)" }}>
            {filtered.length} team{filtered.length !== 1 ? "s" : ""}
          </span>
        </div>

        {filtered.length === 0 ? (
          <div
            className="card"
            style={{
              padding: "var(--space-8)",
              textAlign: "center",
              color: "var(--text-muted)",
            }}
          >
            No teams found{search ? ` matching "${search}"` : ""}.
          </div>
        ) : (
          <>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 260px), 1fr))",
                gap: "var(--space-4)",
              }}
            >
              {pageSlice.map((team) => {
                const sportColor = getSportColor(team.sport);
                const logoUrl = team.logo_url ?? getTeamLogoUrl(team.sport, team.abbreviation);
                const teamKey = `${team.sport}-${team.id}`;
                const showLogo = logoUrl && !logoErrors.has(teamKey);
                return (
                  <a
                    key={teamKey}
                    href={`/teams/${team.sport}/${team.id}`}
                    style={{ textDecoration: "none", color: "inherit" }}
                  >
                    <div
                      className="card"
                      style={{
                        padding: "var(--space-4)",
                        display: "flex",
                        gap: "var(--space-4)",
                        alignItems: "center",
                        cursor: "pointer",
                        transition: "border-color 0.15s ease",
                        borderLeft: `3px solid ${team.color_primary ?? sportColor}`,
                      }}
                    >
                      {/* Logo */}
                      {showLogo ? (
                        <img
                          src={logoUrl}
                          alt={team.name}
                          style={{ width: 48, height: 48, objectFit: "contain", flexShrink: 0 }}
                          onError={() => handleLogoError(teamKey)}
                        />
                      ) : (
                        <div
                          style={{
                            width: 48,
                            height: 48,
                            borderRadius: "var(--radius-md, 6px)",
                            background: team.color_primary ?? sportColor,
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            fontSize: "0.85rem",
                            fontWeight: 700,
                            color: "#fff",
                            flexShrink: 0,
                          }}
                        >
                          {(team.abbreviation ?? team.name.slice(0, 3)).toUpperCase()}
                        </div>
                      )}

                      {/* Info */}
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontWeight: 700, fontSize: "0.9rem" }}>{team.name}</div>
                        {team.city && (
                          <div style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>
                            {team.city}
                          </div>
                        )}
                        <div
                          style={{
                            display: "flex",
                            gap: "var(--space-2)",
                            flexWrap: "wrap",
                            marginTop: "4px",
                          }}
                        >
                          {team.abbreviation && (
                            <span
                              style={{
                                fontSize: "0.7rem",
                                fontWeight: 600,
                                padding: "1px 6px",
                                borderRadius: "var(--radius-full, 9999px)",
                                background: "rgba(128,128,128,0.12)",
                              }}
                            >
                              {team.abbreviation}
                            </span>
                          )}
                          <span
                            style={{
                              fontSize: "0.65rem",
                              fontWeight: 600,
                              padding: "1px 6px",
                              borderRadius: "var(--radius-full, 9999px)",
                              background: sportColor,
                              color: "#fff",
                            }}
                          >
                            {getDisplayName(team.sport)}
                          </span>
                          {team.conference && (
                            <span
                              style={{
                                fontSize: "0.7rem",
                                color: "var(--text-muted)",
                              }}
                            >
                              {team.conference}
                              {team.division ? ` · ${team.division}` : ""}
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  </a>
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
