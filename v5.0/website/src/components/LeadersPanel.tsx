"use client";

import { useState, useEffect } from "react";
import Link from "next/link";

type Leader = {
  rank: number;
  player_id?: number | null;
  player_name?: string;
  team?: string;
  games_played?: number;
  stat_value?: string | number;
  value?: number;
};

type StatTab = { key: string; label: string };

const NBA_TABS: StatTab[] = [
  { key: "pts", label: "PTS" },
  { key: "reb", label: "REB" },
  { key: "ast", label: "AST" },
  { key: "stl", label: "STL" },
  { key: "blk", label: "BLK" },
  { key: "fg3m", label: "3PM" },
];

const MLB_TABS: StatTab[] = [
  { key: "hr", label: "HR" },
  { key: "avg", label: "AVG" },
  { key: "rbi", label: "RBI" },
  { key: "ops", label: "OPS" },
  { key: "obp", label: "OBP" },
  { key: "sb", label: "SB" },
  { key: "era", label: "ERA" },
  { key: "strikeouts", label: "SO" },
];

type Props = {
  sport: string;
  initialStat: string;
  initialLeaders: Leader[];
  initialSeason?: number;
};

export default function LeadersPanel({ sport, initialStat, initialLeaders, initialSeason }: Props) {
  const tabs = sport === "mlb" ? MLB_TABS : NBA_TABS;
  const [activeStat, setActiveStat] = useState(initialStat);
  const [leaders, setLeaders] = useState<Leader[]>(initialLeaders);
  const [season, setSeason] = useState<number | undefined>(initialSeason);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (activeStat === initialStat) {
      setLeaders(initialLeaders);
      setSeason(initialSeason);
      setLoading(false);
      setError(null);
      return;
    }
    setLoading(true);
    setError(null);
    const controller = new AbortController();
    fetch(`/api/leaders/${sport}?stat=${activeStat}&limit=10`, { signal: controller.signal })
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then(d => {
        if (d?.leaders) {
          setLeaders(d.leaders);
          setSeason(d.season);
        }
        setLoading(false);
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError("Failed to load leaders");
        setLoading(false);
      });
    return () => controller.abort();
  }, [activeStat, sport, initialStat, initialLeaders, initialSeason]);

  return (
    <div>
      {/* Stat tabs */}
      <div style={{ display: "flex", gap: "var(--space-1)", flexWrap: "wrap", marginBottom: "var(--space-4)" }}>
        {tabs.map(tab => (
          <button
            key={tab.key}
            onClick={() => setActiveStat(tab.key)}
            style={{
              padding: "var(--space-1) var(--space-3)",
              fontSize: "var(--text-xs)",
              fontWeight: activeStat === tab.key ? "var(--fw-bold)" : "var(--fw-normal)",
              background: activeStat === tab.key ? "var(--color-accent-blue)" : "var(--color-bg-3)",
              color: activeStat === tab.key ? "#fff" : "var(--color-text-secondary)",
              border: activeStat === tab.key ? "1px solid var(--color-accent-blue)" : "1px solid var(--color-border)",
              borderRadius: "var(--radius-sm)",
              cursor: "pointer",
              transition: "all 0.1s",
            }}
          >
            {tab.label}
          </button>
        ))}
        {season && (
          <span style={{ marginLeft: "auto", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", alignSelf: "center" }}>
            {season}
          </span>
        )}
      </div>

      {/* Leaders table */}
      {error ? (
        <p role="alert" style={{ color: "var(--color-loss)", fontSize: "var(--text-sm)", padding: "var(--space-4)" }}>{error}</p>
      ) : (
      <div style={{ overflowX: "auto", opacity: loading ? 0.5 : 1, transition: "opacity 0.15s" }}>
        <table aria-label="Leaders summary" style={{ width: "100%", borderCollapse: "collapse" }}>
          <caption className="sr-only">{`${activeStat.toUpperCase()} leaders`}</caption>
          <thead>
            <tr style={{ borderBottom: "1px solid var(--color-border)", color: "var(--color-text-muted)" }}>
              {["#", "Player", "Team", "GP", activeStat.toUpperCase()].map((h, i) => (
                <th key={h} scope="col" style={{ textAlign: i < 2 ? "left" : "right", padding: "var(--space-1) var(--space-3)", fontSize: "var(--text-xs)", fontWeight: 700 }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {leaders.map(leader => (
              <tr key={leader.player_id ?? leader.rank} style={{ borderBottom: "1px solid var(--color-border)" }}>
                <td style={{ padding: "var(--space-2) var(--space-3)", color: "var(--color-text-muted)", fontSize: "var(--text-xs)", fontWeight: 700 }}>{leader.rank}</td>
                <td style={{ padding: "var(--space-2) var(--space-3)", fontWeight: 700, fontSize: "var(--text-sm)" }}>
                  {leader.player_id ? (
                    <Link href={`/players/${leader.player_id}?sport=${encodeURIComponent(sport)}`} style={{ color: "var(--color-text)", textDecoration: "none" }}>
                      {leader.player_name}
                    </Link>
                  ) : leader.player_name}
                </td>
                <td style={{ padding: "var(--space-2) var(--space-3)", textAlign: "right", fontSize: "var(--text-xs)", color: "var(--color-text-muted)" }}>{leader.team}</td>
                <td style={{ padding: "var(--space-2) var(--space-3)", textAlign: "right", fontSize: "var(--text-xs)", color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>{leader.games_played}</td>
                <td style={{ padding: "var(--space-2) var(--space-3)", textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums", color: "var(--color-accent)" }}>
                  {leader.stat_value}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      )}
    </div>
  );
}
