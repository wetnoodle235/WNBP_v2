"use client";

import { useState, useCallback } from "react";
import type { Standing } from "@/lib/schemas";

interface StandingsTableProps {
  standings: Standing[];
  sport: string;
  className?: string;
}

type SortKey = "wins" | "losses" | "pct" | "streak" | "overall_rank";
type SortDir = "asc" | "desc";

export function StandingsTable({ standings, sport, className }: StandingsTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("overall_rank");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const handleSort = useCallback((key: SortKey) => {
    setSortKey((prev) => {
      if (prev === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prev;
      }
      setSortDir(key === "pct" || key === "wins" ? "desc" : "asc");
      return key;
    });
  }, []);

  const sorted = [...standings].sort((a, b) => {
    let av: number, bv: number;
    switch (sortKey) {
      case "wins": av = a.wins; bv = b.wins; break;
      case "losses": av = a.losses; bv = b.losses; break;
      case "pct": av = a.pct ?? 0; bv = b.pct ?? 0; break;
      case "overall_rank": av = a.overall_rank ?? 999; bv = b.overall_rank ?? 999; break;
      default: av = 0; bv = 0;
    }
    return sortDir === "asc" ? av - bv : bv - av;
  });

  // Group by conference if available
  const conferences = [...new Set(standings.map((s) => s.conference).filter(Boolean))];
  const grouped = conferences.length > 1
    ? conferences.map((conf) => ({
        label: conf!,
        rows: sorted.filter((s) => s.conference === conf),
      }))
    : [{ label: sport.toUpperCase(), rows: sorted }];

  const SortHeader = ({ col, label }: { col: SortKey; label: string }) => (
    <th
      scope="col"
      style={{ cursor: "pointer", userSelect: "none", textAlign: col === "wins" || col === "losses" || col === "pct" ? "right" : "left" }}
      onClick={() => handleSort(col)}
      aria-sort={sortKey === col ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
    >
      {label}
      <span
        aria-hidden="true"
        style={{ marginLeft: "0.3rem", opacity: sortKey === col ? 1 : 0.35, fontSize: "0.7em" }}
      >
        {sortKey === col && sortDir === "asc" ? "▲" : "▼"}
      </span>
    </th>
  );

  return (
    <div className={`data-table-wrap${className ? ` ${className}` : ""}`} role="region" aria-label={`${sport} standings`}>
      {grouped.map(({ label, rows }) => (
        <div key={label}>
          {conferences.length > 1 && (
            <h3 style={{ fontSize: "var(--text-sm)", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--color-text-muted)", padding: "var(--space-3) 0 var(--space-1)" }}>
              {label}
            </h3>
          )}
          <table className="data-table">
            <caption className="sr-only">{`${label} standings`}</caption>
            <thead>
              <tr>
                <th scope="col" style={{ width: "2rem", textAlign: "center" }}>#</th>
                <th scope="col">Team</th>
                <SortHeader col="wins" label="W" />
                <SortHeader col="losses" label="L" />
                <SortHeader col="pct" label="PCT" />
                <th scope="col" style={{ textAlign: "center" }}>Streak</th>
                <th scope="col" style={{ textAlign: "center" }}>L10</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={7} style={{ textAlign: "center", color: "var(--color-text-muted)", padding: "var(--space-8)" }}>
                    No standings data available
                  </td>
                </tr>
              ) : (
                rows.map((s, idx) => (
                  <tr key={s.team_id}>
                    <td style={{ textAlign: "center", color: "var(--color-text-muted)" }}>{s.overall_rank ?? idx + 1}</td>
                    <th scope="row" style={{ fontWeight: 600 }}>{s.team_id}</th>
                    <td style={{ textAlign: "right" }}>{s.wins}</td>
                    <td style={{ textAlign: "right" }}>{s.losses}</td>
                    <td style={{ textAlign: "right", fontWeight: 600 }}>
                      {s.pct != null ? s.pct.toFixed(3) : "—"}
                    </td>
                    <td style={{ textAlign: "center" }}>
                      {s.streak ? (
                        <span style={{ color: s.streak.startsWith("W") ? "var(--color-win)" : s.streak.startsWith("L") ? "var(--color-loss)" : undefined }}>
                          {s.streak}
                        </span>
                      ) : "—"}
                    </td>
                    <td style={{ textAlign: "center" }}>{s.last_ten ?? "—"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}
