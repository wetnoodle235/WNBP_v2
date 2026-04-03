"use client";

import { useState, useEffect, useMemo, useCallback } from "react";
import { SectionBand, PremiumTeaser } from "@/components/ui";
import { useAuth } from "@/lib/auth";
import { getDisplayName, getSportColor, getSportIcon } from "@/lib/sports-config";
import { SkeletonCard, SkeletonTable } from "@/components/LoadingSkeleton";
import { resolveServerApiBase } from "@/lib/api-base";

/* ── Constants ─────────────────────────────────────────────────── */

const API =
  typeof window === "undefined"
    ? resolveServerApiBase()
    : "/api/proxy";

const SPORTS = [
  "nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "mls", "epl", "wnba", "f1",
] as const;
type Sport = (typeof SPORTS)[number];

const PREMIUM_TIERS = new Set([
  "trial", "monthly", "yearly", "premium", "dev",
  "starter", "pro", "enterprise",
]);

/* ── Types ─────────────────────────────────────────────────────── */

interface TeamProb {
  name: string;
  probability: number;
  seed?: number;
  logo_url?: string;
}

interface PlayoffOdds extends TeamProb {
  current_wins?: number;
  current_losses?: number;
  projected_wins?: number;
  division?: string;
  conference?: string;
}

interface DivisionWinner extends TeamProb {
  division?: string;
}

interface AwardCandidate {
  name: string;
  probability: number;
  team?: string;
  position?: string;
}

interface RoundOdds {
  name: string;
  rounds: Record<string, number>;
}

interface ProjectedWins {
  mean: number;
  median: number;
  std: number;
  p10: number;
  p90: number;
  current_wins: number;
  current_losses: number;
}

interface TeamStrength {
  name: string;
  rating: number;
  wins: number;
  losses: number;
  conference?: string;
  win_pct?: number;
  recent_form?: string;
}

interface DraftLotteryTeam {
  name: string;
  probability: number;
  current_wins?: number;
  current_losses?: number;
}

interface RelegationTeam {
  name: string;
  probability: number;
}

interface BracketMatchup {
  team1: string;
  team2: string;
  team1_prob: number;
  team2_prob: number;
}

interface SimulationData {
  sport: string;
  simulations: number;
  season: number;
  season_completion_pct: number;
  championship_probabilities: TeamProb[];
  playoff_odds: PlayoffOdds[];
  division_winner_odds: DivisionWinner[];
  awards: Record<string, AwardCandidate[]>;
  round_by_round_odds: RoundOdds[];
  projected_wins: Record<string, ProjectedWins>;
  team_strengths: TeamStrength[];
  draft_lottery_odds: DraftLotteryTeam[];
  relegation_odds?: RelegationTeam[];
  cfp_bracket?: Record<string, BracketMatchup[]>;
  march_madness?: Record<string, BracketMatchup[]>;
}

interface SimulatorPrediction {
  game_id: string;
  confidence?: number | null;
  home_win_prob?: number | null;
  away_win_prob?: number | null;
  predicted_total?: number | null;
  predicted_spread?: number | null;
}

interface RankingEntry {
  rank?: number;
  school?: string;
  team?: string;
  name?: string;
  poll?: string;
  week?: number;
  points?: number;
  first_place_votes?: number;
  wins?: number;
  losses?: number;
}

interface FuturesEntry {
  team?: string;
  name?: string;
  market?: string;
  odds?: number;
  american_odds?: number;
  implied_probability?: number;
  book?: string;
  description?: string;
}

/* ── Helpers ───────────────────────────────────────────────────── */

function probColor(pct: number): string {
  if (pct >= 40) return "var(--color-win)";
  if (pct >= 20) return "var(--color-green)";
  if (pct >= 10) return "var(--color-blue)";
  if (pct >= 5) return "var(--color-neutral)";
  return "var(--color-loss)";
}

function probBarGradient(pct: number): string {
  if (pct >= 40) return "linear-gradient(90deg, #16a34a, #22c55e)";
  if (pct >= 20) return "linear-gradient(90deg, #15803d, #16a34a)";
  if (pct >= 10) return "linear-gradient(90deg, #2563eb, #3b82f6)";
  if (pct >= 5) return "linear-gradient(90deg, #d97706, #f59e0b)";
  return "linear-gradient(90deg, #dc2626, #ef4444)";
}

const AWARD_LABELS: Record<string, string> = {
  mvp: "Most Valuable Player",
  roty: "Rookie of the Year",
  dpoy: "Defensive Player of the Year",
  smoy: "Sixth Man of the Year",
  mip: "Most Improved Player",
  coty: "Coach of the Year",
  finals_mvp: "Finals MVP",
  scoring_title: "Scoring Champion",
  cy_young: "Cy Young Award",
  batting_title: "Batting Champion",
  golden_boot: "Golden Boot",
  golden_glove: "Golden Glove",
  wdc: "World Drivers' Championship",
  wcc: "World Constructors' Championship",
  heisman: "Heisman Trophy",
  biletnikoff: "Biletnikoff Award",
  hart: "Hart Trophy",
  vezina: "Vezina Trophy",
  norris: "Norris Trophy",
  calder: "Calder Trophy",
  selke: "Selke Trophy",
};

function awardLabel(key: string): string {
  return AWARD_LABELS[key] ?? key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/* ── Probability bar (inline) ──────────────────────────────────── */

function ProbBar({ pct, height = 20 }: { pct: number; height?: number }) {
  const clamped = Math.min(Math.max(pct, 0), 100);
  return (
    <div
      className="sim-prob-bar"
      role="meter"
      aria-valuenow={clamped}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={`${clamped.toFixed(1)}%`}
      style={{ height }}
    >
      <div
        className="sim-prob-bar-fill"
        style={{
          width: `${clamped}%`,
          background: probBarGradient(clamped),
        }}
      />
      {clamped >= 3 && (
        <span className="sim-prob-bar-label">{clamped.toFixed(1)}%</span>
      )}
    </div>
  );
}

/* ── Section: Championship Odds ────────────────────────────────── */

function ChampionshipOdds({ data }: { data: TeamProb[] }) {
  if (!data?.length) return null;
  const top = data.slice(0, 20);
  return (
    <SectionBand title="🏆 Championship Odds" id="championship">
      <div className="card">
        <div className="card-body" style={{ padding: 0 }}>
          <div className="sim-champ-list">
            {top.map((t, i) => (
              <div key={t.name} className="sim-champ-row">
                <span className="sim-champ-rank">{i + 1}</span>
                <span className="sim-champ-name">{t.name}</span>
                <div className="sim-champ-bar-wrap">
                  <ProbBar pct={t.probability} />
                </div>
                <span className="sim-champ-pct" style={{ color: probColor(t.probability) }}>
                  {t.probability.toFixed(1)}%
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </SectionBand>
  );
}

/* ── Section: Playoff Odds ─────────────────────────────────────── */

function PlayoffOddsTable({
  playoffOdds,
  projectedWins,
  divisionWinners,
}: {
  playoffOdds: PlayoffOdds[];
  projectedWins: Record<string, ProjectedWins>;
  divisionWinners: DivisionWinner[];
}) {
  const [sortKey, setSortKey] = useState<string>("probability");
  const [sortAsc, setSortAsc] = useState(false);

  const divMap = useMemo(() => {
    const m: Record<string, number> = {};
    divisionWinners?.forEach((d) => {
      m[d.name] = d.probability;
    });
    return m;
  }, [divisionWinners]);

  const rows = useMemo(() => {
    type Row = PlayoffOdds & { proj_wins?: number; div_pct?: number };
    const mapped: Row[] = (playoffOdds ?? []).map((t) => {
      const pw = projectedWins?.[t.name];
      return {
        ...t,
        current_wins: t.current_wins ?? pw?.current_wins ?? 0,
        current_losses: t.current_losses ?? pw?.current_losses ?? 0,
        proj_wins: pw?.mean ?? t.projected_wins,
        div_pct: divMap[t.name] ?? 0,
      };
    });
    mapped.sort((a, b) => {
      let va: number, vb: number;
      switch (sortKey) {
        case "name":
          return sortAsc ? a.name.localeCompare(b.name) : b.name.localeCompare(a.name);
        case "record":
          va = (a.current_wins ?? 0);
          vb = (b.current_wins ?? 0);
          break;
        case "proj_wins":
          va = a.proj_wins ?? 0;
          vb = b.proj_wins ?? 0;
          break;
        case "div_pct":
          va = a.div_pct ?? 0;
          vb = b.div_pct ?? 0;
          break;
        default:
          va = a.probability;
          vb = b.probability;
      }
      return sortAsc ? va - vb : vb - va;
    });
    return mapped;
  }, [playoffOdds, projectedWins, divMap, sortKey, sortAsc]);

  function handleSort(key: string) {
    if (key === sortKey) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(false); }
  }

  const arrow = (key: string) =>
    sortKey === key ? (sortAsc ? " ▲" : " ▼") : "";

  if (!playoffOdds?.length) return null;

  return (
    <SectionBand title="📊 Playoff Odds" id="playoffs">
      <div className="card">
        <div className="card-body responsive-table-wrap season-table-wrap" style={{ padding: 0 }}>
          <table className="data-table table-striped sticky-header responsive-table season-table" style={{ width: "100%" }}>
            <caption className="sr-only">Playoff odds by team</caption>
            <thead>
              <tr>
                <th scope="col" style={{ cursor: "pointer" }} onClick={() => handleSort("name")} aria-sort={sortKey === "name" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("name"); } }}>
                  Team{arrow("name")}
                </th>
                <th scope="col" style={{ textAlign: "center", cursor: "pointer" }} onClick={() => handleSort("record")} aria-sort={sortKey === "record" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("record"); } }}>
                  Record{arrow("record")}
                </th>
                <th scope="col" style={{ textAlign: "right", cursor: "pointer" }} onClick={() => handleSort("proj_wins")} aria-sort={sortKey === "proj_wins" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("proj_wins"); } }}>
                  Proj. Wins{arrow("proj_wins")}
                </th>
                <th scope="col" style={{ textAlign: "right", cursor: "pointer" }} onClick={() => handleSort("probability")} aria-sort={sortKey === "probability" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("probability"); } }}>
                  Playoff %{arrow("probability")}
                </th>
                <th scope="col" style={{ textAlign: "right", cursor: "pointer" }} onClick={() => handleSort("div_pct")} aria-sort={sortKey === "div_pct" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("div_pct"); } }}>
                  Div Winner %{arrow("div_pct")}
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((t) => (
                <tr key={t.name}>
                  <td style={{ fontWeight: 600 }}>{t.name}</td>
                  <td style={{ textAlign: "center", fontVariantNumeric: "tabular-nums" }}>
                    {t.current_wins ?? 0}-{t.current_losses ?? 0}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {t.proj_wins != null ? t.proj_wins.toFixed(1) : "—"}
                  </td>
                  <td style={{ textAlign: "right" }}>
                    <span style={{ color: probColor(t.probability), fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>
                      {t.probability.toFixed(1)}%
                    </span>
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {(t as unknown as Record<string, number>).div_pct != null
                      ? ((t as unknown as Record<string, number>).div_pct).toFixed(1) + "%"
                      : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </SectionBand>
  );
}

/* ── Section: Award Predictions ────────────────────────────────── */

function AwardPredictions({ awards }: { awards: Record<string, AwardCandidate[]> }) {
  if (!awards || Object.keys(awards).length === 0) return null;
  return (
    <SectionBand title="🏅 Award Predictions" id="awards">
      <div className="sim-awards-grid">
        {Object.entries(awards).map(([key, candidates]) => {
          if (!candidates?.length) return null;
          const top5 = candidates.slice(0, 5);
          return (
            <div key={key} className="card sim-award-card">
              <div className="card-header">
                <h3 className="card-title">{awardLabel(key)}</h3>
              </div>
              <div className="card-body" style={{ padding: 0 }}>
                {top5.map((c, i) => (
                  <div key={c.name} className="sim-award-row">
                    <span className="sim-award-rank">{i + 1}</span>
                    <div className="sim-award-info">
                      <span className="sim-award-name">{c.name}</span>
                      {c.team && (
                        <span className="sim-award-team">{c.team}</span>
                      )}
                    </div>
                    <div className="sim-award-bar-wrap">
                      <ProbBar pct={c.probability} height={16} />
                    </div>
                    <span className="sim-award-pct" style={{ color: probColor(c.probability) }}>
                      {c.probability.toFixed(1)}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </SectionBand>
  );
}

/* ── Section: Round-by-Round Odds ──────────────────────────────── */

function RoundByRound({ data }: { data: RoundOdds[] }) {
  if (!data?.length) return null;
  const safeRows = data.filter(
    (team): team is RoundOdds =>
      !!team &&
      typeof team.name === "string" &&
      !!team.name &&
      !!team.rounds &&
      typeof team.rounds === "object" &&
      !Array.isArray(team.rounds),
  );
  if (!safeRows.length) return null;
  const allRounds = Array.from(
    new Set(safeRows.flatMap((team) => Object.keys(team.rounds ?? {}))),
  );
  if (!allRounds.length) return null;
  return (
    <SectionBand title="🔄 Round-by-Round Odds" id="rounds">
      <div className="card">
        <div className="card-body responsive-table-wrap season-table-wrap" style={{ padding: 0 }}>
          <table className="data-table table-striped sticky-header responsive-table season-table" style={{ width: "100%" }}>
            <caption className="sr-only">Round-by-round projected wins</caption>
            <thead>
              <tr>
                <th scope="col">Team</th>
                {allRounds.map((r) => (
                  <th scope="col" key={r} style={{ textAlign: "right" }}>{r}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {safeRows.map((t) => (
                <tr key={t.name}>
                  <td style={{ fontWeight: 600 }}>{t.name}</td>
                  {allRounds.map((r) => {
                    const val = t.rounds?.[r];
                    return (
                      <td key={r} style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                        {val != null ? (
                          <span style={{ color: probColor(val), fontWeight: 600 }}>
                            {val.toFixed(1)}%
                          </span>
                        ) : "—"}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </SectionBand>
  );
}

/* ── Section: Team Strength Rankings ───────────────────────────── */

function TeamStrengthTable({ data }: { data: TeamStrength[] }) {
  const [sortKey, setSortKey] = useState("rating");
  const [sortAsc, setSortAsc] = useState(false);

  const sorted = useMemo(() => {
    const rows = data.map((t) => ({
      ...t,
      win_pct: t.win_pct ?? (t.wins + t.losses > 0 ? t.wins / (t.wins + t.losses) : 0),
    }));
    rows.sort((a, b) => {
      let va: number | string, vb: number | string;
      switch (sortKey) {
        case "name":
          return sortAsc ? a.name.localeCompare(b.name) : b.name.localeCompare(a.name);
        case "wins":
          va = a.wins; vb = b.wins; break;
        case "win_pct":
          va = a.win_pct; vb = b.win_pct; break;
        case "form":
          va = a.recent_form ?? ""; vb = b.recent_form ?? "";
          return sortAsc ? (va as string).localeCompare(vb as string) : (vb as string).localeCompare(va as string);
        default:
          va = a.rating; vb = b.rating;
      }
      return sortAsc ? (va as number) - (vb as number) : (vb as number) - (va as number);
    });
    return rows;
  }, [data, sortKey, sortAsc]);

  function handleSort(key: string) {
    if (key === sortKey) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(false); }
  }

  const arrow = (key: string) =>
    sortKey === key ? (sortAsc ? " ▲" : " ▼") : "";

  if (!data?.length) return null;

  return (
    <SectionBand title="💪 Team Strength Rankings" id="strength">
      <div className="card">
        <div className="card-body responsive-table-wrap season-table-wrap" style={{ padding: 0 }}>
          <table className="data-table table-striped sticky-header responsive-table season-table" style={{ width: "100%" }}>
            <caption className="sr-only">Power rankings by team</caption>
            <thead>
              <tr>
                <th scope="col" style={{ textAlign: "center" }}>Rank</th>
                <th scope="col" style={{ cursor: "pointer" }} onClick={() => handleSort("name")} aria-sort={sortKey === "name" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("name"); } }}>
                  Team{arrow("name")}
                </th>
                <th scope="col" style={{ textAlign: "right", cursor: "pointer" }} onClick={() => handleSort("rating")} aria-sort={sortKey === "rating" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("rating"); } }}>
                  Rating{arrow("rating")}
                </th>
                <th scope="col" style={{ textAlign: "center", cursor: "pointer" }} onClick={() => handleSort("wins")} aria-sort={sortKey === "wins" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("wins"); } }}>
                  W-L{arrow("wins")}
                </th>
                <th scope="col" style={{ textAlign: "right", cursor: "pointer" }} onClick={() => handleSort("win_pct")} aria-sort={sortKey === "win_pct" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("win_pct"); } }}>
                  Win%{arrow("win_pct")}
                </th>
                <th scope="col" style={{ textAlign: "center", cursor: "pointer" }} onClick={() => handleSort("form")} aria-sort={sortKey === "form" ? (sortAsc ? "ascending" : "descending") : "none"} tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleSort("form"); } }}>
                  Recent Form{arrow("form")}
                </th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((t, i) => (
                <tr key={t.name}>
                  <td style={{ textAlign: "center", fontWeight: 700, color: "var(--color-text-muted)" }}>
                    {i + 1}
                  </td>
                  <td style={{ fontWeight: 600 }}>{t.name}</td>
                  <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                    {t.rating.toFixed(1)}
                  </td>
                  <td style={{ textAlign: "center", fontVariantNumeric: "tabular-nums" }}>
                    {t.wins}-{t.losses}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {(t.win_pct * 100).toFixed(1)}%
                  </td>
                  <td style={{ textAlign: "center", letterSpacing: "0.1em", fontSize: "var(--text-xs)" }}>
                    {t.recent_form ?? "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </SectionBand>
  );
}

/* ── Section: Draft Lottery (NBA/NHL) ──────────────────────────── */

function DraftLottery({ data, sport }: { data: DraftLotteryTeam[]; sport: string }) {
  if (!data?.length) return null;
  if (!["nba", "nhl"].includes(sport)) return null;

  const normalized = data.map((team) => {
    const raw = Number(team.probability);
    const probability = Number.isFinite(raw) ? raw : 0;
    return { ...team, probability };
  });

  return (
    <SectionBand title="🎰 Draft Lottery Odds" id="draft">
      <div className="card">
        <div className="card-body responsive-table-wrap season-table-wrap" style={{ padding: 0 }}>
          <table className="data-table table-striped sticky-header responsive-table season-table" style={{ width: "100%" }}>
            <thead>
              <tr>
                <th>Team</th>
                <th style={{ textAlign: "center" }}>Record</th>
                <th style={{ textAlign: "right" }}>#1 Pick %</th>
                <th style={{ width: "35%" }}>Probability</th>
              </tr>
            </thead>
            <tbody>
              {normalized.map((t) => (
                <tr key={t.name}>
                  <td style={{ fontWeight: 600 }}>{t.name}</td>
                  <td style={{ textAlign: "center", fontVariantNumeric: "tabular-nums" }}>
                    {t.current_wins ?? 0}-{t.current_losses ?? 0}
                  </td>
                  <td style={{ textAlign: "right", fontWeight: 600, fontVariantNumeric: "tabular-nums", color: probColor(t.probability) }}>
                    {t.probability.toFixed(1)}%
                  </td>
                  <td>
                    <ProbBar pct={t.probability} height={16} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </SectionBand>
  );
}

/* ── Section: Relegation Odds (EPL) ────────────────────────────── */

function RelegationOdds({ data, sport }: { data?: RelegationTeam[]; sport: string }) {
  if (!data?.length || sport !== "epl") return null;
  return (
    <SectionBand title="⬇️ Relegation Odds" id="relegation">
      <div className="card">
        <div className="card-body" style={{ padding: 0 }}>
          <div className="sim-champ-list">
            {data.map((t, i) => (
              <div key={t.name} className="sim-champ-row">
                <span className="sim-champ-rank">{i + 1}</span>
                <span className="sim-champ-name">{t.name}</span>
                <div className="sim-champ-bar-wrap">
                  <ProbBar pct={t.probability} />
                </div>
                <span className="sim-champ-pct" style={{ color: probColor(t.probability) }}>
                  {t.probability.toFixed(1)}%
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </SectionBand>
  );
}

/* ── Section: Bracket Visualization ────────────────────────────── */

function BracketSection({
  data,
  title,
}: {
  data?: Record<string, BracketMatchup[]>;
  title: string;
}) {
  if (!data || Object.keys(data).length === 0) return null;
  return (
    <SectionBand title={title} id="bracket">
      <div className="sim-bracket-grid">
        {Object.entries(data).map(([roundName, matchups]) => (
          <div key={roundName} className="card sim-bracket-round">
            <div className="card-header">
              <h3 className="card-title">{roundName}</h3>
            </div>
            <div className="card-body" style={{ padding: 0 }}>
              {matchups.map((m, i) => (
                <div key={i} className="sim-bracket-matchup">
                  <div className={`sim-bracket-team${m.team1_prob >= m.team2_prob ? " sim-bracket-fav" : ""}`}>
                    <span>{m.team1}</span>
                    <span className="sim-bracket-prob" style={{ color: probColor(m.team1_prob) }}>
                      {m.team1_prob.toFixed(1)}%
                    </span>
                  </div>
                  <div className="sim-bracket-vs">vs</div>
                  <div className={`sim-bracket-team${m.team2_prob > m.team1_prob ? " sim-bracket-fav" : ""}`}>
                    <span>{m.team2}</span>
                    <span className="sim-bracket-prob" style={{ color: probColor(m.team2_prob) }}>
                      {m.team2_prob.toFixed(1)}%
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </SectionBand>
  );
}

/* ── Section: Simulator Predictions ───────────────────────────── */

function SimulatorPredictions({ data }: { data: SimulatorPrediction[] }) {
  if (!data.length) return null;
  const top = [...data]
    .sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0))
    .slice(0, 12);

  return (
    <SectionBand title="📈 Simulator Predictions" id="predictions">
      <div className="card">
        <div className="card-body responsive-table-wrap season-table-wrap" style={{ padding: 0 }}>
          <table className="data-table table-striped sticky-header responsive-table season-table" style={{ width: "100%" }}>
            <thead>
              <tr>
                <th>Game</th>
                <th style={{ textAlign: "right" }}>Home Win</th>
                <th style={{ textAlign: "right" }}>Away Win</th>
                <th style={{ textAlign: "right" }}>Spread</th>
                <th style={{ textAlign: "right" }}>Total</th>
                <th style={{ textAlign: "right" }}>Confidence</th>
              </tr>
            </thead>
            <tbody>
              {top.map((p) => (
                <tr key={p.game_id}>
                  <td style={{ fontWeight: 600 }}>{p.game_id}</td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {p.home_win_prob != null ? `${(p.home_win_prob * 100).toFixed(1)}%` : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {p.away_win_prob != null ? `${(p.away_win_prob * 100).toFixed(1)}%` : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {p.predicted_spread != null ? `${p.predicted_spread > 0 ? "+" : ""}${p.predicted_spread.toFixed(1)}` : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {p.predicted_total != null ? p.predicted_total.toFixed(1) : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontWeight: 700 }}>
                    {p.confidence != null ? `${Math.round(p.confidence * 100)}%` : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </SectionBand>
  );
}

/* ── Loading skeleton ──────────────────────────────────────────── */

function SimSkeleton() {
  return (
    <>
      <SkeletonCard rows={6} />
      <SkeletonTable rows={10} cols={5} />
      <SkeletonCard rows={4} />
    </>
  );
}

/* ── Coming Soon message ───────────────────────────────────────── */

function ComingSoonMessage({
  sport,
  onRetry,
}: {
  sport: string;
  onRetry: () => void;
}) {
  return (
    <div className="card">
      <div
        className="card-body"
        style={{
          textAlign: "center",
          padding: "var(--space-8) var(--space-6)",
        }}
      >
        <p style={{ fontSize: "2.5rem", marginBottom: "var(--space-3)" }}>🔮</p>
        <h3
          style={{
            fontSize: "var(--text-lg)",
            marginBottom: "var(--space-2)",
          }}
        >
          Simulation Running Soon
        </h3>
        <p
          style={{
            color: "var(--color-text-secondary)",
            maxWidth: 460,
            margin: "0 auto var(--space-4)",
            lineHeight: 1.6,
          }}
        >
          Season simulations for{" "}
          <strong>{getDisplayName(sport)}</strong> are generated every{" "}
          <strong>Tuesday at 8:00 AM EST</strong>. Check back after the next
          scheduled run for championship odds, playoff projections, award
          predictions, and more.
        </p>
        <div
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "var(--space-2)",
            padding: "var(--space-2) var(--space-4)",
            background: "var(--color-bg-3)",
            borderRadius: 8,
            fontSize: "var(--text-sm)",
            color: "var(--color-text-muted)",
            marginBottom: "var(--space-4)",
          }}
        >
          📅 Next update: Tuesday 8:00 AM EST
        </div>
        <br />
        <button
          className="btn btn-secondary btn-sm"
          style={{ marginTop: "var(--space-2)" }}
          onClick={onRetry}
        >
          Refresh
        </button>
      </div>
    </div>
  );
}

/* ── Free Preview Teaser (when API blocks free users) ──────────── */

function FreePreviewTeaser({ sport }: { sport: string }) {
  const features = [
    { icon: "🏆", label: "Championship Odds", desc: "Win probabilities for every team" },
    { icon: "📊", label: "Playoff Projections", desc: "Seed probabilities and matchup odds" },
    { icon: "🏅", label: "Award Predictions", desc: "MVP, ROTY, DPOY, and more" },
    { icon: "💪", label: "Team Rankings", desc: "Power ratings with recent form" },
    { icon: "🔄", label: "Round-by-Round Odds", desc: "Playoff advancement probabilities" },
    { icon: "🎰", label: "Draft Lottery", desc: "#1 pick odds for lottery teams" },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-6)" }}>
      <div className="card">
        <div
          className="card-body"
          style={{
            textAlign: "center",
            padding: "var(--space-8) var(--space-6)",
          }}
        >
          <p style={{ fontSize: "2.5rem", marginBottom: "var(--space-3)" }}>
            🔮
          </p>
          <h3
            style={{
              fontSize: "var(--text-lg)",
              marginBottom: "var(--space-2)",
            }}
          >
            Unlock {getDisplayName(sport)} Season Projections
          </h3>
          <p
            style={{
              color: "var(--color-text-secondary)",
              maxWidth: 480,
              margin: "0 auto var(--space-6)",
              lineHeight: 1.6,
            }}
          >
            Our Monte Carlo engine runs 10,000 simulations to project the
            entire {getDisplayName(sport)} season. Premium members get full
            access to:
          </p>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 220px), 1fr))",
              gap: "var(--space-3)",
              textAlign: "left",
              maxWidth: 640,
              margin: "0 auto var(--space-6)",
            }}
          >
            {features.map((f) => (
              <div
                key={f.label}
                style={{
                  display: "flex",
                  gap: "var(--space-3)",
                  alignItems: "flex-start",
                  padding: "var(--space-3)",
                  background: "var(--color-bg-3)",
                  borderRadius: 8,
                }}
              >
                <span style={{ fontSize: "1.25rem", flexShrink: 0 }}>
                  {f.icon}
                </span>
                <div>
                  <div style={{ fontWeight: 600, fontSize: "var(--text-sm)" }}>
                    {f.label}
                  </div>
                  <div
                    style={{
                      color: "var(--color-text-muted)",
                      fontSize: "var(--text-xs)",
                    }}
                  >
                    {f.desc}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
      <PremiumTeaser
        icon="⚡"
        message="Upgrade to Premium to access the full Season Simulator with all projections and predictions."
        ctaLabel="Upgrade Now"
        ctaHref="/pricing"
      />
    </div>
  );
}

/* ── Free Preview (limited data for non-premium users) ─────────── */

function FreePreview({ data, sport }: { data: SimulationData; sport: string }) {
  const champTop = data.championship_probabilities?.slice(0, 5) ?? [];
  const mvpTop = data.awards?.mvp?.slice(0, 3) ?? [];
  const totalTeams = data.championship_probabilities?.length ?? 0;

  if (champTop.length === 0 && mvpTop.length === 0) {
    return <FreePreviewTeaser sport={sport} />;
  }

  return (
    <>
      {/* Championship Odds Preview */}
      {champTop.length > 0 && (
        <SectionBand title="🏆 Championship Odds — Preview" id="championship-preview">
          <div className="card">
            <div className="card-body" style={{ padding: 0 }}>
              <div className="sim-champ-list">
                {champTop.map((t, i) => (
                  <div key={t.name} className="sim-champ-row">
                    <span className="sim-champ-rank">{i + 1}</span>
                    <span className="sim-champ-name">{t.name}</span>
                    <div className="sim-champ-bar-wrap">
                      <ProbBar pct={t.probability} />
                    </div>
                    <span
                      className="sim-champ-pct"
                      style={{ color: probColor(t.probability) }}
                    >
                      {t.probability.toFixed(1)}%
                    </span>
                  </div>
                ))}
              </div>
              {totalTeams > 5 && (
                <div
                  style={{
                    textAlign: "center",
                    padding: "var(--space-3)",
                    color: "var(--color-text-muted)",
                    fontSize: "var(--text-sm)",
                    borderTop: "1px solid var(--color-border, #e5e7eb)",
                  }}
                >
                  Showing top 5 of {totalTeams} teams
                </div>
              )}
            </div>
          </div>
        </SectionBand>
      )}

      {/* MVP Race Preview */}
      {mvpTop.length > 0 && (
        <SectionBand title="🏅 MVP Race — Top 3" id="mvp-preview">
          <div className="card">
            <div className="card-body" style={{ padding: 0 }}>
              {mvpTop.map((c, i) => (
                <div key={c.name} className="sim-award-row">
                  <span className="sim-award-rank">{i + 1}</span>
                  <div className="sim-award-info">
                    <span className="sim-award-name">{c.name}</span>
                    {c.team && (
                      <span className="sim-award-team">{c.team}</span>
                    )}
                  </div>
                  <div className="sim-award-bar-wrap">
                    <ProbBar pct={c.probability} height={16} />
                  </div>
                  <span
                    className="sim-award-pct"
                    style={{ color: probColor(c.probability) }}
                  >
                    {c.probability.toFixed(1)}%
                  </span>
                </div>
              ))}
            </div>
          </div>
        </SectionBand>
      )}

      {/* Upgrade Gate */}
      <div
        style={{
          position: "relative",
          marginTop: "var(--space-4)",
          padding: "var(--space-6)",
          background:
            "linear-gradient(180deg, var(--color-bg-2) 0%, var(--color-bg-3) 100%)",
          borderRadius: 12,
          border: "1px solid var(--color-border, #e5e7eb)",
          textAlign: "center",
        }}
      >
        <p
          style={{
            fontSize: "var(--text-lg)",
            fontWeight: 700,
            marginBottom: "var(--space-2)",
          }}
        >
          🔓 Unlock the Full Simulation
        </p>
        <p
          style={{
            color: "var(--color-text-secondary)",
            maxWidth: 500,
            margin: "0 auto var(--space-4)",
            lineHeight: 1.6,
          }}
        >
          Premium members get playoff odds, team rankings, draft lottery
          projections, round-by-round odds, and award predictions across all
          categories.
        </p>
        <PremiumTeaser
          icon="⚡"
          message="Upgrade to Premium for the complete Season Simulator experience."
          ctaLabel="Upgrade Now"
          ctaHref="/pricing"
        />
      </div>
    </>
  );
}

/* ── Main Component ────────────────────────────────────────────── */

export function SeasonClient() {
  const { user, isLoading: authLoading } = useAuth();
  const [sport, setSport] = useState<Sport>("nba");
  const [data, setData] = useState<SimulationData | null>(null);
  const [simPredictions, setSimPredictions] = useState<SimulatorPrediction[]>([]);
  const [rankings, setRankings] = useState<RankingEntry[]>([]);
  const [futures, setFutures] = useState<FuturesEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const tier = user?.tier ?? "free";
  const hasPremium = PREMIUM_TIERS.has(tier);

  const fetchData = useCallback(async (s: Sport, signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    setData(null);
    setSimPredictions([]);
    setRankings([]);
    setFutures([]);
    try {
      const headers: Record<string, string> = {};
      if (typeof window !== "undefined") {
        const token = localStorage.getItem("wnbp_token");
        const apiKey = localStorage.getItem("wnbp_api_key");
        if (token) headers["Authorization"] = `Bearer ${token}`;
        if (apiKey) headers["X-API-Key"] = apiKey;
      }
      const res = await fetch(`${API}/v1/${s}/simulation`, { headers, signal });
      if (signal?.aborted) return;
      if (!res.ok) {
        if (res.status === 404) {
          setError("no_data");
        } else if (res.status === 401 || res.status === 403) {
          setError("tier_restricted");
        } else {
          setError(`Failed to load simulation data (${res.status})`);
        }
        return;
      }
      const json = await res.json();
      const payload = json?.data && json?.success ? json.data : json;
      setData(payload as SimulationData);

      const predRes = await fetch(`${API}/v1/predictions/${s}?limit=30`, { headers, signal });
      if (!signal?.aborted && predRes.ok) {
        const predJson = await predRes.json();
        const predPayload = predJson?.data && predJson?.success ? predJson.data : predJson;
        if (Array.isArray(predPayload)) {
          setSimPredictions(predPayload as SimulatorPrediction[]);
        }
      }

      // Rankings (AP/Coaches Poll — primarily ncaaf, ncaab)
      const rankRes = await fetch(`${API}/v1/${s}/rankings?week=current&limit=25`, { headers, signal });
      if (!signal?.aborted && rankRes.ok) {
        const rankJson = await rankRes.json();
        const rankPayload = rankJson?.data && rankJson?.success ? rankJson.data : rankJson;
        if (Array.isArray(rankPayload)) setRankings(rankPayload as RankingEntry[]);
      }

      // Futures / outright odds
      const futRes = await fetch(`${API}/v1/${s}/futures?limit=25`, { headers, signal });
      if (!signal?.aborted && futRes.ok) {
        const futJson = await futRes.json();
        const futPayload = futJson?.data && futJson?.success ? futJson.data : futJson;
        if (Array.isArray(futPayload)) setFutures(futPayload as FuturesEntry[]);
      }
    } catch (err) {
      if ((err as Error).name === "AbortError") return;
      setError("Unable to connect to the simulation server.");
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    fetchData(sport, controller.signal);
    return () => controller.abort();
  }, [sport, fetchData]);

  function handleSportChange(s: Sport) {
    if (s === sport) return;
    setSport(s);
  }

  /* ── Determine content state ── */
  const isReady = !loading && !authLoading;
  const showNoData = isReady && error === "no_data";
  const showTierRestricted =
    isReady && error === "tier_restricted" && !hasPremium;
  const showGenericError =
    isReady &&
    error != null &&
    error !== "no_data" &&
    error !== "tier_restricted";
  const showFreePreview = isReady && data != null && !hasPremium;
  const showFullView = isReady && data != null && hasPremium;

  const keyStats = useMemo(() => {
    if (!data) return [] as Array<{ label: string; value: string; tone?: "good" | "warn" }>;
    const topChamp = data.championship_probabilities?.[0];
    const completion = data.season_completion_pct ?? 0;
    return [
      { label: "Simulations", value: (data.simulations ?? 10000).toLocaleString() },
      { label: "Season Progress", value: `${completion.toFixed(0)}%`, tone: completion > 65 ? "warn" : "good" },
      { label: "Teams Tracked", value: String(data.championship_probabilities?.length ?? 0) },
      { label: "Front Runner", value: topChamp ? `${topChamp.name} ${topChamp.probability.toFixed(1)}%` : "—" },
    ];
  }, [data]);

  const sectionLinks = useMemo(() => {
    const links: Array<{ id: string; label: string }> = [];
    if (!showFullView || !data) return links;
    if (data.championship_probabilities?.length) links.push({ id: "championship", label: "Championship" });
    if (simPredictions.length) links.push({ id: "predictions", label: "Predictions" });
    if (rankings.length) links.push({ id: "rankings", label: "Rankings" });
    if (futures.length) links.push({ id: "futures", label: "Futures" });
    if (data.playoff_odds?.length) links.push({ id: "playoffs", label: "Playoffs" });
    if (data.awards && Object.keys(data.awards).length > 0) links.push({ id: "awards", label: "Awards" });
    if (data.round_by_round_odds?.length) links.push({ id: "rounds", label: "Rounds" });
    if (data.team_strengths?.length) links.push({ id: "strength", label: "Power" });
    if (["nba", "nhl"].includes(sport) && data.draft_lottery_odds?.length) links.push({ id: "draft", label: "Lottery" });
    if (sport === "epl" && data.relegation_odds?.length) links.push({ id: "relegation", label: "Relegation" });
    if (sport === "ncaab" && data.march_madness) links.push({ id: "bracket", label: "Bracket" });
    if (sport === "ncaaf" && data.cfp_bracket) links.push({ id: "bracket", label: "Playoff" });
    if (sport === "f1" && data.awards?.wdc) links.push({ id: "wdc", label: "WDC" });
    if (sport === "f1" && data.awards?.wcc) links.push({ id: "wcc", label: "WCC" });
    return links;
  }, [data, showFullView, sport, simPredictions.length, rankings.length, futures.length]);

  return (
    <main className="season-shell">
      <section className="sim-hero sim-hero-overhaul">
        <div className="sim-hero-inner sim-hero-inner-overhaul">
          <div className="sim-kicker">Simulation Lab</div>
          <h1 className="sim-hero-title">Season Simulator</h1>
          <p className="sim-hero-subtitle">
            Scenario engine for title races, playoff cut-lines, and award volatility.
          </p>

          <div className="sim-sport-tabs sim-sport-tabs-overhaul" role="tablist" aria-label="Select sport">
            {SPORTS.map((s) => {
              const isActive = s === sport;
              const color = getSportColor(s);
              return (
                <button
                  key={s}
                  role="tab"
                  aria-selected={isActive}
                  className={`sim-sport-tab sim-sport-tab-overhaul${isActive ? " active" : ""}`}
                  onClick={() => handleSportChange(s)}
                  style={isActive ? { borderColor: color, boxShadow: `0 0 0 2px ${color}30` } : undefined}
                >
                  <span className="sim-sport-icon">{getSportIcon(s)}</span>
                  <span className="sim-sport-label">{getDisplayName(s)}</span>
                </button>
              );
            })}
          </div>

          {data && !loading && (
            <div className="season-kpi-grid">
              {keyStats.map((item) => (
                <article key={item.label} className={`season-kpi-card${item.tone ? ` tone-${item.tone}` : ""}`}>
                  <span className="season-kpi-label">{item.label}</span>
                  <strong className="season-kpi-value">{item.value}</strong>
                </article>
              ))}
            </div>
          )}
        </div>
      </section>

      <div className="sim-content sim-content-overhaul">
        {loading && <SimSkeleton />}

        {showNoData && (
          <ComingSoonMessage sport={sport} onRetry={() => fetchData(sport)} />
        )}

        {showTierRestricted && <FreePreviewTeaser sport={sport} />}

        {showGenericError && (
          <div className="card">
            <div className="card-body" style={{ textAlign: "center", padding: "var(--space-8)" }}>
              <p style={{ fontSize: "var(--text-lg)", marginBottom: "var(--space-2)" }}>📭</p>
              <p role="alert" style={{ color: "var(--color-text-secondary)" }}>{error}</p>
              <button
                className="btn btn-secondary btn-sm"
                style={{ marginTop: "var(--space-4)" }}
                onClick={() => fetchData(sport)}
              >
                Try Again
              </button>
            </div>
          </div>
        )}

        {showFreePreview && data && (
          <div className="season-stream-layout">
            <aside className="season-quick-rail">
              <div className="season-rail-card">
                <h3>Preview Mode</h3>
                <p>Top outcomes are visible. Full ladders and deep playoff paths require Premium.</p>
              </div>
            </aside>
            <div>
              <FreePreview data={data} sport={sport} />
            </div>
          </div>
        )}

        {showFullView && data && (
          <div className="season-stream-layout">
            <aside className="season-quick-rail">
              <div className="season-rail-card">
                <h3>Control Rail</h3>
                <p>
                  <strong>{data.sport?.toUpperCase()}</strong> {data.season} with {(data.simulations ?? 10000).toLocaleString()} simulations.
                </p>
              </div>
              {sectionLinks.length > 0 && (
                <nav className="season-rail-card" aria-label="Jump to section">
                  <h3>Jump To</h3>
                  <div className="season-rail-links">
                    {sectionLinks.map((item) => (
                      <a key={item.id} href={`#${item.id}`} className="season-rail-link">
                        {item.label}
                      </a>
                    ))}
                  </div>
                </nav>
              )}
            </aside>

            <div>
              <SimulatorPredictions data={simPredictions} />

              <ChampionshipOdds data={data.championship_probabilities} />

              <PlayoffOddsTable
                playoffOdds={data.playoff_odds}
                projectedWins={data.projected_wins}
                divisionWinners={data.division_winner_odds}
              />

              <AwardPredictions awards={data.awards} />

              <RoundByRound data={data.round_by_round_odds} />

              <TeamStrengthTable data={data.team_strengths} />

              {["nba", "nhl"].includes(sport) && (
                <DraftLottery data={data.draft_lottery_odds} sport={sport} />
              )}

              {sport === "epl" && (
                <RelegationOdds data={data.relegation_odds} sport={sport} />
              )}

              {sport === "ncaab" && data.march_madness && (
                <BracketSection
                  data={data.march_madness}
                  title="🏀 March Madness Bracket"
                />
              )}

              {sport === "ncaaf" && data.cfp_bracket && (
                <BracketSection
                  data={data.cfp_bracket}
                  title="🏈 College Football Playoff"
                />
              )}

              {sport === "f1" && data.awards && (
                <>
                  {data.awards.wdc && (
                    <SectionBand title="🏎️ WDC Projections" id="wdc">
                      <div className="card">
                        <div className="card-body" style={{ padding: 0 }}>
                          <div className="sim-champ-list">
                            {data.awards.wdc.slice(0, 20).map((c, i) => (
                              <div key={c.name} className="sim-champ-row">
                                <span className="sim-champ-rank">{i + 1}</span>
                                <span className="sim-champ-name">
                                  {c.name}
                                  {c.team && (
                                    <span
                                      style={{
                                        color: "var(--color-text-muted)",
                                        fontSize: "var(--text-xs)",
                                        marginLeft: 6,
                                      }}
                                    >
                                      {c.team}
                                    </span>
                                  )}
                                </span>
                                <div className="sim-champ-bar-wrap">
                                  <ProbBar pct={c.probability} />
                                </div>
                                <span
                                  className="sim-champ-pct"
                                  style={{ color: probColor(c.probability) }}
                                >
                                  {c.probability.toFixed(1)}%
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>
                    </SectionBand>
                  )}
                  {data.awards.wcc && (
                    <SectionBand title="🏗️ WCC Projections" id="wcc">
                      <div className="card">
                        <div className="card-body" style={{ padding: 0 }}>
                          <div className="sim-champ-list">
                            {data.awards.wcc.slice(0, 10).map((c, i) => (
                              <div key={c.name} className="sim-champ-row">
                                <span className="sim-champ-rank">{i + 1}</span>
                                <span className="sim-champ-name">{c.name}</span>
                                <div className="sim-champ-bar-wrap">
                                  <ProbBar pct={c.probability} />
                                </div>
                                <span
                                  className="sim-champ-pct"
                                  style={{ color: probColor(c.probability) }}
                                >
                                  {c.probability.toFixed(1)}%
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>
                    </SectionBand>
                  )}
                </>
              )}

              {/* AP/Coaches Poll Rankings — primarily ncaaf/ncaab */}
              {rankings.length > 0 && (
                <SectionBand title="📊 Poll Rankings" id="rankings">
                  <div className="card">
                    <div className="card-body" style={{ padding: 0, overflowX: "auto" }}>
                      <table className="stats-table" style={{ width: "100%" }}>
                        <thead>
                          <tr>
                            <th>Rank</th>
                            <th>Team</th>
                            {rankings[0]?.poll && <th>Poll</th>}
                            {rankings[0]?.points !== undefined && <th>Points</th>}
                            {rankings[0]?.first_place_votes !== undefined && <th>1st Votes</th>}
                            {rankings[0]?.wins !== undefined && <th>W-L</th>}
                          </tr>
                        </thead>
                        <tbody>
                          {rankings.slice(0, 25).map((r, i) => (
                            <tr key={i}>
                              <td style={{ fontWeight: 700, color: "var(--color-text-accent)" }}>
                                #{r.rank ?? i + 1}
                              </td>
                              <td>{r.school ?? r.team ?? r.name ?? "—"}</td>
                              {r.poll && <td style={{ color: "var(--color-text-muted)", fontSize: "var(--text-sm)" }}>{r.poll}</td>}
                              {r.points !== undefined && <td>{r.points}</td>}
                              {r.first_place_votes !== undefined && <td>{r.first_place_votes}</td>}
                              {r.wins !== undefined && <td>{r.wins}-{r.losses ?? 0}</td>}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </SectionBand>
              )}

              {/* Futures / Outright Odds */}
              {futures.length > 0 && (
                <SectionBand title="📈 Futures & Outright Odds" id="futures">
                  <div className="card">
                    <div className="card-body" style={{ padding: 0, overflowX: "auto" }}>
                      <table className="stats-table" style={{ width: "100%" }}>
                        <thead>
                          <tr>
                            <th>Team / Player</th>
                            {futures[0]?.market && <th>Market</th>}
                            {futures[0]?.american_odds !== undefined && <th>Odds</th>}
                            {futures[0]?.implied_probability !== undefined && <th>Impl. Prob</th>}
                            {futures[0]?.book && <th>Book</th>}
                          </tr>
                        </thead>
                        <tbody>
                          {futures.slice(0, 25).map((f, i) => (
                            <tr key={i}>
                              <td style={{ fontWeight: 600 }}>{f.team ?? f.name ?? f.description ?? "—"}</td>
                              {f.market && <td style={{ color: "var(--color-text-muted)", fontSize: "var(--text-sm)", textTransform: "capitalize" }}>{f.market.replace(/_/g, " ")}</td>}
                              {f.american_odds !== undefined && (
                                <td style={{ color: (f.american_odds ?? 0) > 0 ? "var(--color-win)" : "var(--color-text-secondary)", fontWeight: 700 }}>
                                  {(f.american_odds ?? 0) > 0 ? `+${f.american_odds}` : f.american_odds}
                                </td>
                              )}
                              {f.implied_probability !== undefined && (
                                <td>{((f.implied_probability ?? 0) * 100).toFixed(1)}%</td>
                              )}
                              {f.book && <td style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>{f.book}</td>}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </SectionBand>
              )}
            </div>
          </div>
        )}
      </div>
    </main>
  );
}

export default SeasonClient;
