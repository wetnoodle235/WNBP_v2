"use client";

import { useState, useCallback, useMemo } from "react";
import type { SportCategory } from "@/lib/sports";

// Column definitions per sport category
interface Column {
  key: string;
  header: string;
  align?: "left" | "right" | "center";
  sortable?: boolean;
}

const CATEGORY_COLUMNS: Record<SportCategory, Column[]> = {
  basketball: [
    { key: "name", header: "Player", align: "left" },
    { key: "pts", header: "PTS", align: "right", sortable: true },
    { key: "reb", header: "REB", align: "right", sortable: true },
    { key: "ast", header: "AST", align: "right", sortable: true },
    { key: "stl", header: "STL", align: "right", sortable: true },
    { key: "blk", header: "BLK", align: "right", sortable: true },
    { key: "fg_pct", header: "FG%", align: "right", sortable: true },
    { key: "min", header: "MIN", align: "right", sortable: true },
  ],
  football: [
    { key: "name", header: "Player", align: "left" },
    { key: "pass_yds", header: "PASS YDS", align: "right", sortable: true },
    { key: "pass_td", header: "PASS TD", align: "right", sortable: true },
    { key: "rush_yds", header: "RUSH YDS", align: "right", sortable: true },
    { key: "rush_td", header: "RUSH TD", align: "right", sortable: true },
    { key: "rec_yds", header: "REC YDS", align: "right", sortable: true },
    { key: "receptions", header: "REC", align: "right", sortable: true },
    { key: "tackles", header: "TKL", align: "right", sortable: true },
  ],
  baseball: [
    { key: "name", header: "Player", align: "left" },
    { key: "avg", header: "AVG", align: "right", sortable: true },
    { key: "hr", header: "HR", align: "right", sortable: true },
    { key: "rbi", header: "RBI", align: "right", sortable: true },
    { key: "hits", header: "H", align: "right", sortable: true },
    { key: "runs", header: "R", align: "right", sortable: true },
    { key: "obp", header: "OBP", align: "right", sortable: true },
    { key: "ops", header: "OPS", align: "right", sortable: true },
  ],
  hockey: [
    { key: "name", header: "Player", align: "left" },
    { key: "goals", header: "G", align: "right", sortable: true },
    { key: "assists", header: "A", align: "right", sortable: true },
    { key: "points", header: "PTS", align: "right", sortable: true },
    { key: "shots", header: "SOG", align: "right", sortable: true },
    { key: "plus_minus", header: "+/-", align: "right", sortable: true },
    { key: "pim", header: "PIM", align: "right", sortable: true },
    { key: "toi", header: "TOI", align: "right" },
  ],
  soccer: [
    { key: "name", header: "Player", align: "left" },
    { key: "goals", header: "G", align: "right", sortable: true },
    { key: "assists", header: "A", align: "right", sortable: true },
    { key: "shots", header: "SH", align: "right", sortable: true },
    { key: "xg", header: "xG", align: "right", sortable: true },
    { key: "xa", header: "xA", align: "right", sortable: true },
    { key: "pass_pct", header: "PASS%", align: "right", sortable: true },
    { key: "tackles", header: "TKL", align: "right", sortable: true },
  ],
  tennis: [
    { key: "name", header: "Player", align: "left" },
    { key: "aces", header: "ACE", align: "right", sortable: true },
    { key: "double_faults", header: "DF", align: "right", sortable: true },
    { key: "first_serve_pct", header: "1ST%", align: "right", sortable: true },
    { key: "winners", header: "WIN", align: "right", sortable: true },
    { key: "unforced_errors", header: "UE", align: "right", sortable: true },
    { key: "break_points_won", header: "BPW", align: "right", sortable: true },
  ],
  mma: [
    { key: "name", header: "Fighter", align: "left" },
    { key: "sig_strikes", header: "SIG STR", align: "right", sortable: true },
    { key: "takedowns", header: "TD", align: "right", sortable: true },
    { key: "sub_attempts", header: "SUB", align: "right", sortable: true },
    { key: "knockdowns", header: "KD", align: "right", sortable: true },
    { key: "result", header: "Result", align: "center" },
    { key: "method", header: "Method", align: "center" },
  ],
  motorsport: [
    { key: "name", header: "Driver", align: "left" },
    { key: "grid_position", header: "GRID", align: "right", sortable: true },
    { key: "finish_position", header: "FIN", align: "right", sortable: true },
    { key: "points", header: "PTS", align: "right", sortable: true },
    { key: "laps", header: "LAPS", align: "right" },
    { key: "fastest_lap", header: "FAST LAP", align: "right" },
    { key: "constructor", header: "Team", align: "left" },
  ],
  esports: [
    { key: "name", header: "Player", align: "left" },
    { key: "kills", header: "K", align: "right", sortable: true },
    { key: "deaths", header: "D", align: "right", sortable: true },
    { key: "assists", header: "A", align: "right", sortable: true },
    { key: "kda", header: "KDA", align: "right", sortable: true },
    { key: "damage", header: "DMG", align: "right", sortable: true },
    { key: "rating", header: "RTG", align: "right", sortable: true },
  ],
  golf: [
    { key: "name", header: "Player", align: "left" },
    { key: "position", header: "POS", align: "right", sortable: true },
    { key: "score_to_par", header: "TO PAR", align: "right", sortable: true },
    { key: "score", header: "SCORE", align: "right", sortable: true },
    { key: "birdies", header: "BIR", align: "right", sortable: true },
    { key: "bogeys", header: "BOG", align: "right", sortable: true },
    { key: "fairway_pct", header: "FW%", align: "right", sortable: true },
    { key: "gir_pct", header: "GIR%", align: "right", sortable: true },
  ],
};

interface PlayerStatsTableProps {
  rows: Record<string, unknown>[];
  category: SportCategory;
  className?: string;
}

export function PlayerStatsTable({ rows, category, className }: PlayerStatsTableProps) {
  const columns = CATEGORY_COLUMNS[category] ?? CATEGORY_COLUMNS.basketball;
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [search, setSearch] = useState("");

  const handleSort = useCallback((key: string) => {
    setSortKey((prev) => {
      if (prev === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prev;
      }
      setSortDir("desc");
      return key;
    });
  }, []);

  const filtered = useMemo(() => {
    if (!search) return rows;
    const q = search.toLowerCase();
    return rows.filter((r) =>
      String(r.name ?? "").toLowerCase().includes(q),
    );
  }, [rows, search]);

  const sorted = useMemo(() => {
    if (!sortKey) return filtered;
    return [...filtered].sort((a, b) => {
      const av = Number(a[sortKey] ?? 0);
      const bv = Number(b[sortKey] ?? 0);
      return sortDir === "asc" ? av - bv : bv - av;
    });
  }, [filtered, sortKey, sortDir]);

  return (
    <div className={`data-table-wrap${className ? ` ${className}` : ""}`}>
      <div style={{ padding: "var(--space-3)", display: "flex", gap: "var(--space-3)" }}>
        <input
          type="search"
          placeholder="Search players…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            padding: "var(--space-2) var(--space-3)",
            border: "1px solid var(--color-border)",
            borderRadius: "var(--radius)",
            background: "var(--color-surface-alt)",
            color: "var(--color-text)",
            fontSize: "var(--text-sm)",
            flex: 1,
            maxWidth: 300,
          }}
          aria-label="Search players"
        />
      </div>
      <table className="data-table" aria-label="Player statistics">
        <thead>
          <tr>
            {columns.map((col) => (
              <th
                key={col.key}
                style={{
                  textAlign: col.align ?? "left",
                  cursor: col.sortable ? "pointer" : undefined,
                  userSelect: col.sortable ? "none" : undefined,
                }}
                onClick={col.sortable ? () => handleSort(col.key) : undefined}
                aria-sort={
                  sortKey === col.key
                    ? sortDir === "asc" ? "ascending" : "descending"
                    : col.sortable ? "none" : undefined
                }
              >
                {col.header}
                {col.sortable && (
                  <span
                    aria-hidden="true"
                    style={{ marginLeft: "0.3rem", opacity: sortKey === col.key ? 1 : 0.35, fontSize: "0.7em" }}
                  >
                    {sortKey === col.key && sortDir === "asc" ? "▲" : "▼"}
                  </span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.length === 0 ? (
            <tr>
              <td colSpan={columns.length} style={{ textAlign: "center", color: "var(--color-text-muted)", padding: "var(--space-8)" }}>
                {search ? "No matching players" : "No stats available"}
              </td>
            </tr>
          ) : (
            sorted.map((row, idx) => (
              <tr key={String(row.id ?? idx)}>
                {columns.map((col) => {
                  const val = row[col.key];
                  let display: string;
                  if (val == null) display = "—";
                  else if (typeof val === "number" && col.key.includes("pct")) display = (val * 100).toFixed(1) + "%";
                  else if (typeof val === "number") display = Number.isInteger(val) ? String(val) : val.toFixed(1);
                  else display = String(val);
                  return (
                    <td key={col.key} style={{ textAlign: col.align ?? "left" }}>
                      {display}
                    </td>
                  );
                })}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
