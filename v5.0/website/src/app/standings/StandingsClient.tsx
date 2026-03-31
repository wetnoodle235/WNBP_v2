"use client";

import { useState, useMemo } from "react";
import type { Standing } from "@/lib/schemas";
import type { TeamLookup } from "./page";
import { DataTable, TeamBadge } from "@/components/ui";
import type { TableColumn } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { formatWinPct } from "@/lib/formatters";

type StandingRow = Standing & { teamName: string; teamAbbrev?: string };

interface StandingsClientProps {
  sports: string[];
  standingsBySport: Record<string, Standing[]>;
  teamsBySport: Record<string, TeamLookup>;
  seasonActive?: Record<string, boolean>;
  seasonYears?: Record<string, string | null>;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function enrichStandings(
  standings: Standing[],
  teamLookup: TeamLookup,
): StandingRow[] {
  return standings.map((s) => {
    const team = teamLookup[s.team_id];
    const isNumericId = /^\d+$/.test(s.team_id);
    return {
      ...s,
      teamName: s.team_name ?? team?.name ?? (isNumericId ? `Team #${s.team_id}` : s.team_id),
      teamAbbrev: team?.abbrev,
    };
  });
}

function groupRows(
  rows: StandingRow[],
  sport: string,
): { label: string; rows: StandingRow[] }[] {
  // Prefer `group` field as section header when available
  const groups = [
    ...new Set(rows.map((r) => r.group).filter(Boolean)),
  ] as string[];

  if (groups.length > 1) {
    return groups.map((g) => ({
      label: g,
      rows: sortRows(rows.filter((r) => r.group === g), sport),
    }));
  }

  const conferences = [
    ...new Set(rows.map((r) => r.conference).filter(Boolean)),
  ] as string[];

  if (conferences.length <= 1) {
    return [{ label: "", rows: sortRows(rows, sport) }];
  }

  return conferences.map((conf) => ({
    label: conf,
    rows: sortRows(rows.filter((r) => r.conference === conf), sport),
  }));
}

function sortRows(rows: StandingRow[], sport: string): StandingRow[] {
  const category = getSportCategory(sport);
  return [...rows].sort((a, b) => {
    if (a.rank != null && b.rank != null) return a.rank - b.rank;
    if (a.overall_rank != null && b.overall_rank != null) {
      return a.overall_rank - b.overall_rank;
    }
    // Points-based sports: higher points first
    if (category === "soccer" || category === "hockey") {
      if (a.points != null && b.points != null && a.points !== b.points) {
        return b.points - a.points;
      }
    }
    return (b.pct ?? 0) - (a.pct ?? 0);
  });
}

/** Check if any row has a non-null value for a field. */
function hasData(rows: StandingRow[], key: keyof StandingRow): boolean {
  return rows.some((r) => r[key] != null && r[key] !== "");
}

/* ------------------------------------------------------------------ */
/*  Playoff spots by sport                                             */
/* ------------------------------------------------------------------ */

const PLAYOFF_SPOTS: Record<string, number> = {
  nba: 8, nhl: 8, mlb: 6, nfl: 7, wnba: 8,
  ncaab: 16, ncaaf: 12,
  epl: 4, bundesliga: 4, laliga: 4, ligue1: 4, seriea: 4, ucl: 8,
  mls: 9,
};

const BOTTOM_THRESHOLD = 3; // Bottom N teams get red treatment

/* ------------------------------------------------------------------ */
/*  Sport categories                                                   */
/* ------------------------------------------------------------------ */

const BASKETBALL_SPORTS = new Set(["nba", "wnba", "ncaab", "ncaaw"]);
const FOOTBALL_SPORTS = new Set(["nfl", "ncaaf"]);
const HOCKEY_SPORTS = new Set(["nhl"]);
const BASEBALL_SPORTS = new Set(["mlb"]);

type SportCategory = "basketball" | "football" | "hockey" | "baseball" | "soccer";

function getSportCategory(sport: string): SportCategory {
  if (BASKETBALL_SPORTS.has(sport)) return "basketball";
  if (FOOTBALL_SPORTS.has(sport)) return "football";
  if (HOCKEY_SPORTS.has(sport)) return "hockey";
  if (BASEBALL_SPORTS.has(sport)) return "baseball";
  return "soccer";
}

/* ------------------------------------------------------------------ */
/*  Columns                                                            */
/* ------------------------------------------------------------------ */

function buildColumns(allRows: StandingRow[], sport: string): TableColumn<StandingRow>[] {
  const category = getSportCategory(sport);
  const showSeed = category === "basketball" || category === "hockey";
  const showClinch = showSeed;

  const cols: TableColumn<StandingRow>[] = [];

  /* # column – shows playoff seed for basketball / hockey */
  cols.push({
    key: "rank",
    header: "#",
    align: "center",
    width: "40px",
    render: (row, idx) => (
      <span
        style={{
          color: "var(--color-text-muted)",
          fontWeight: 600,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {showSeed && row.rank != null ? row.rank : idx + 1}
      </span>
    ),
    sortFn: (a, b) =>
      (a.rank ?? a.overall_rank ?? 999) - (b.rank ?? b.overall_rank ?? 999),
  });

  /* Team column – with optional clinch-status badge */
  cols.push({
    key: "team",
    header: "Team",
    sticky: true,
    render: (row) => (
      <span style={{ display: "inline-flex", alignItems: "center", gap: "0.35rem" }}>
        <TeamBadge
          teamId={row.team_id}
          name={row.teamName}
          abbrev={row.teamAbbrev}
          sport={sport}
          size="sm"
        />
        {showClinch && row.clinch_status && (
          <span
            aria-label={`${row.teamName}: ${row.clinch_status}`}
            title={row.clinch_status}
            style={{
              fontSize: "0.65rem",
              fontWeight: 700,
              padding: "0.05rem 0.3rem",
              borderRadius: "3px",
              background: "var(--color-bg-3, #333)",
              color: "var(--color-text-muted)",
              lineHeight: 1.4,
              whiteSpace: "nowrap",
            }}
          >
            {row.clinch_status}
          </span>
        )}
      </span>
    ),
  });

  /* ---------- reusable column definitions ---------- */

  const w: TableColumn<StandingRow> = {
    key: "wins", header: "W", align: "right", width: "44px",
    render: (row) => <NumCell>{row.wins}</NumCell>,
    sortFn: (a, b) => a.wins - b.wins,
  };

  const l: TableColumn<StandingRow> = {
    key: "losses", header: "L", align: "right", width: "44px",
    render: (row) => <NumCell>{row.losses}</NumCell>,
    sortFn: (a, b) => a.losses - b.losses,
  };

  const pctCol: TableColumn<StandingRow> = {
    key: "pct", header: "PCT", align: "right", width: "56px",
    render: (row) => (
      <NumCell bold>
        {row.pct != null ? row.pct.toFixed(3) : formatWinPct(row.wins, row.losses)}
      </NumCell>
    ),
    sortFn: (a, b) => (a.pct ?? 0) - (b.pct ?? 0),
  };

  const tiesCol = (header = "T"): TableColumn<StandingRow> => ({
    key: "ties", header, align: "right", width: "44px",
    render: (row) => <NumCell>{row.ties ?? "—"}</NumCell>,
    sortFn: (a, b) => (a.ties ?? 0) - (b.ties ?? 0),
  });

  const otlCol: TableColumn<StandingRow> = {
    key: "otl", header: "OTL", align: "right", width: "50px",
    render: (row) => <NumCell>{row.otl ?? "—"}</NumCell>,
    sortFn: (a, b) => (a.otl ?? 0) - (b.otl ?? 0),
  };

  const ptsCol: TableColumn<StandingRow> = {
    key: "points", header: "PTS", align: "right", width: "50px",
    render: (row) => <NumCell bold>{row.points ?? "—"}</NumCell>,
    sortFn: (a, b) => (a.points ?? 0) - (b.points ?? 0),
  };

  const gpCol: TableColumn<StandingRow> = {
    key: "games_played", header: "GP", align: "right", width: "44px",
    render: (row) => <NumCell>{row.games_played ?? "—"}</NumCell>,
    sortFn: (a, b) => (a.games_played ?? 0) - (b.games_played ?? 0),
  };

  const pfCol = (header = "PF"): TableColumn<StandingRow> => ({
    key: "points_for", header, align: "right",
    render: (row) => <NumCell>{row.points_for ?? "—"}</NumCell>,
    sortFn: (a, b) => (a.points_for ?? 0) - (b.points_for ?? 0),
  });

  const paCol = (header = "PA"): TableColumn<StandingRow> => ({
    key: "points_against", header, align: "right",
    render: (row) => <NumCell>{row.points_against ?? "—"}</NumCell>,
    sortFn: (a, b) => (a.points_against ?? 0) - (b.points_against ?? 0),
  });

  const streakCol: TableColumn<StandingRow> = {
    key: "streak", header: "Strk", align: "center",
    render: (row) =>
      row.streak ? (
        <span
          style={{
            fontVariantNumeric: "tabular-nums",
            fontSize: "0.82rem",
            color: row.streak.startsWith("W")
              ? "var(--color-win)"
              : row.streak.startsWith("L")
                ? "var(--color-loss)"
                : undefined,
          }}
        >
          {row.streak}
        </span>
      ) : (
        <NumCell>—</NumCell>
      ),
  };

  const l10Col: TableColumn<StandingRow> = {
    key: "last_ten", header: "L10", align: "center",
    render: (row) => <NumCell>{row.last_ten ?? "—"}</NumCell>,
  };

  /* ---------- sport-specific column sets ---------- */

  switch (category) {
    case "basketball":
      cols.push(w, l, pctCol);
      if (hasData(allRows, "streak")) cols.push(streakCol);
      if (hasData(allRows, "last_ten")) cols.push(l10Col);
      break;

    case "football":
      cols.push(w, l, tiesCol(), pctCol, pfCol(), paCol());
      if (hasData(allRows, "streak")) cols.push(streakCol);
      break;

    case "hockey":
      cols.push(w, l, otlCol, ptsCol, gpCol);
      if (hasData(allRows, "streak")) cols.push(streakCol);
      if (hasData(allRows, "last_ten")) cols.push(l10Col);
      break;

    case "baseball":
      cols.push(w, l, pctCol, gpCol, pfCol("RS"), paCol("RA"));
      if (hasData(allRows, "streak")) cols.push(streakCol);
      if (hasData(allRows, "last_ten")) cols.push(l10Col);
      break;

    case "soccer":
      cols.push(gpCol, w, tiesCol("D"), l, ptsCol, pfCol("GF"), paCol("GA"));
      if (hasData(allRows, "streak")) cols.push(streakCol);
      break;
  }

  return cols;
}

/* ------------------------------------------------------------------ */
/*  Tiny helpers                                                       */
/* ------------------------------------------------------------------ */

function NumCell({ children, bold }: { children: React.ReactNode; bold?: boolean }) {
  return (
    <span
      style={{
        fontVariantNumeric: "tabular-nums",
        fontSize: "0.82rem",
        fontWeight: bold ? 600 : undefined,
      }}
    >
      {children}
    </span>
  );
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export function StandingsClient({
  sports,
  standingsBySport,
  teamsBySport,
  seasonActive,
  seasonYears,
}: StandingsClientProps) {
  const [showOffSeason, setShowOffSeason] = useState(false);

  // Separate in-season and off-season sports
  const activeSports = useMemo(
    () => sports.filter((s) =>
      (standingsBySport[s]?.length ?? 0) > 0 || seasonActive?.[s] !== undefined
    ),
    [sports, standingsBySport, seasonActive],
  );

  const inSeasonSports = useMemo(
    () => activeSports.filter((s) => seasonActive?.[s] !== false),
    [activeSports, seasonActive],
  );

  const offSeasonSports = useMemo(
    () => activeSports.filter((s) => seasonActive?.[s] === false),
    [activeSports, seasonActive],
  );

  const visibleSports = showOffSeason ? activeSports : inSeasonSports;
  const [activeSport, setActiveSport] = useState(inSeasonSports[0] ?? activeSports[0] ?? sports[0]);

  const enrichedBySport = useMemo(() => {
    const map: Record<string, StandingRow[]> = {};
    for (const sport of sports) {
      map[sport] = enrichStandings(
        standingsBySport[sport] ?? [],
        teamsBySport[sport] ?? {},
      );
    }
    return map;
  }, [sports, standingsBySport, teamsBySport]);

  const allRows = enrichedBySport[activeSport] ?? [];

  // Detect seasons that haven't started (all teams at 0-0)
  const seasonNotStarted =
    allRows.length > 0 &&
    allRows.every((r) => r.wins === 0 && r.losses === 0 && (r.ties ?? 0) === 0);

  const groups = useMemo(() => groupRows(allRows, activeSport), [allRows, activeSport]);
  const columns = useMemo(
    () => buildColumns(allRows, activeSport),
    [allRows, activeSport],
  );

  const sportColor = getSportColor(activeSport);

  return (
    <div>
      {/* Sport selector pills */}
      <div
        className="standings-sport-tabs"
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "var(--space-2)",
          marginBottom: "var(--space-4, 1rem)",
        }}
        role="tablist"
        aria-label="Select sport"
      >
        {visibleSports.map((sport) => {
          const isActive = sport === activeSport;
          const color = getSportColor(sport);
          return (
            <button
              key={sport}
              className={`standings-sport-pill${isActive ? " standings-sport-pill--active" : ""}`}
              role="tab"
              aria-selected={isActive}
              onClick={() => setActiveSport(sport)}
              style={{
                padding: "0.4rem 0.9rem",
                borderRadius: "999px",
                border: isActive ? "none" : "1px solid var(--color-bg-3, #333)",
                background: isActive ? color : "transparent",
                color: isActive ? "#fff" : "var(--color-text-secondary)",
                fontWeight: isActive ? 700 : 500,
                fontSize: "var(--text-sm, 0.85rem)",
                cursor: "pointer",
                transition: "all 0.15s ease",
                lineHeight: 1.2,
              }}
            >
              {getDisplayName(sport)}
            </button>
          );
        })}
        {offSeasonSports.length > 0 && (
          <button
            className="standings-sport-pill standings-sport-pill--toggle"
            onClick={() => setShowOffSeason(!showOffSeason)}
            style={{
              padding: "0.4rem 0.9rem",
              borderRadius: "999px",
              border: "1px dashed var(--color-bg-3, #333)",
              background: "transparent",
              color: "var(--color-text-muted)",
              fontSize: "var(--text-sm, 0.85rem)",
              cursor: "pointer",
              transition: "all 0.15s ease",
              lineHeight: 1.2,
            }}
          >
            {showOffSeason ? "Hide Off-Season" : `+ ${offSeasonSports.length} Off-Season`}
          </button>
        )}
      </div>

      {/* Standings tables grouped by conference */}
      {seasonActive?.[activeSport] === false ? (
        <div
          className="card"
          style={{
            textAlign: "center",
            padding: "var(--space-8, 2rem) var(--space-4, 1rem)",
            color: "var(--color-text-muted)",
          }}
        >
          <div style={{ fontSize: "2rem", marginBottom: "var(--space-3, 0.75rem)" }}>🏁</div>
          <div style={{ fontWeight: 600, fontSize: "var(--text-base, 1rem)", marginBottom: "var(--space-2, 0.5rem)", color: "var(--color-text-secondary)" }}>
            {seasonYears?.[activeSport]
              ? `${seasonYears[activeSport]} ${getDisplayName(activeSport)} Season Completed`
              : `${getDisplayName(activeSport)} — Off-Season`}
          </div>
          <div style={{ fontSize: "var(--text-sm, 0.85rem)" }}>
            The {getDisplayName(activeSport)} season has ended. Standings will update when the next season begins.
          </div>
        </div>
      ) : allRows.length === 0 || seasonNotStarted ? (
        <div
          className="card"
          style={{
            textAlign: "center",
            padding: "var(--space-8, 2rem)",
            color: "var(--color-text-muted)",
          }}
        >
          {seasonNotStarted
            ? `${getDisplayName(activeSport)} season has not yet started.`
            : `No standings data available for ${getDisplayName(activeSport)}.`}
        </div>
      ) : (
        groups.map((group) => (
          <div key={group.label || "all"} style={{ marginBottom: "var(--space-4, 1rem)" }}>
            {group.label && (
              <h3
                style={{
                  fontSize: "var(--text-sm)",
                  fontWeight: 700,
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  color: sportColor,
                  padding: "var(--space-3, 0.75rem) 0 var(--space-1, 0.25rem)",
                  borderBottom: `2px solid ${sportColor}`,
                  marginBottom: "var(--space-2, 0.5rem)",
                }}
              >
                {group.label}
              </h3>
            )}
            <DataTable
              columns={columns}
              rows={group.rows}
              getRowKey={(row) => row.team_id}
              caption={group.label ? `${group.label} Standings` : `${getDisplayName(activeSport)} Standings`}
              defaultSortKey="rank"
              defaultSortDir="asc"
              className="standings-data-table"
              rowClassName={(row, idx) => {
                const playoffSpots = PLAYOFF_SPOTS[activeSport] ?? 8;
                const totalInGroup = group.rows.length;
                const rank = idx + 1;
                if (rank <= playoffSpots) return "standings-playoff-row";
                if (rank > totalInGroup - BOTTOM_THRESHOLD) return "standings-bottom-row";
                return undefined;
              }}
            />
          </div>
        ))
      )}
    </div>
  );
}
