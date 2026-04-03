"use client";

import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import Link from "next/link";
import { useAuth } from "@/lib/auth";
import { CopyButton } from "@/components/ui";

/* ── Constants ────────────────────────────────────────────────────── */

const API_URL =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
const DOCS_BASE_URL = API_URL.replace(/\/$/, "");

const SPORT_CATEGORIES = [
  { label: "Basketball", sports: ["NBA", "WNBA", "NCAAB"] },
  { label: "Football", sports: ["NFL", "NCAAF"] },
  { label: "Hockey", sports: ["NHL"] },
  { label: "Baseball", sports: ["MLB"] },
  { label: "Soccer", sports: ["MLS", "EPL", "La Liga", "Bundesliga", "Serie A", "Ligue 1"] },
  { label: "Tennis", sports: ["ATP", "WTA"] },
  { label: "Combat", sports: ["UFC"] },
  { label: "Racing", sports: ["F1"] },
  { label: "Esports", sports: ["LoL", "CS2", "Dota2", "Valorant"] },
] as const;

const SPORT_SLUG: Record<string, string> = {
  NBA: "nba",
  NFL: "nfl",
  NHL: "nhl",
  MLB: "mlb",
  WNBA: "wnba",
  NCAAB: "ncaab",
  NCAAF: "ncaaf",
  MLS: "mls",
  ATP: "atp",
  WTA: "wta",
  UFC: "ufc",
  F1: "f1",
  LoL: "lol",
  CS2: "cs2",
  Dota2: "dota2",
  Valorant: "valorant",
  EPL: "epl",
  "La Liga": "laliga",
  Bundesliga: "bundesliga",
  "Serie A": "seriea",
  "Ligue 1": "ligue1",
};

interface EndpointDef {
  id: string;
  path: string;
  method?: string;
  title: string;
  description: string;
  params: ParamDef[];
  exampleQuery: string;
  exampleResponse: string;
}

interface ParamDef {
  name: string;
  type: string;
  required: boolean;
  default?: string;
  description: string;
}

/* ── Shared parameter definitions ─────────────────────────────────── */

const COMMON_PARAMS: Record<string, ParamDef> = {
  season: { name: "season", type: "int", required: false, default: "current", description: "Season year (e.g. 2025)" },
  date: { name: "date", type: "string", required: false, description: "Date filter in YYYY-MM-DD format" },
  team: { name: "team", type: "string", required: false, description: "Filter by team name" },
  player: { name: "player", type: "string", required: false, description: "Filter by player name" },
  limit: { name: "limit", type: "int", required: false, default: "50", description: "Maximum number of results to return" },
  offset: { name: "offset", type: "int", required: false, default: "0", description: "Pagination offset" },
  aggregate: { name: "aggregate", type: "bool", required: false, default: "false", description: "Aggregate player stats across games" },
  sort: { name: "sort", type: "string", required: false, description: "Field to sort results by" },
  order: { name: "order", type: "string", required: false, default: "desc", description: "Sort order: asc or desc" },
};

function buildEndpoints(sport: string, slug: string): EndpointDef[] {
  return [
    {
      id: `${slug}-games`,
      path: `/v1/${slug}/games`,
      title: "Games",
      description: `Retrieve ${sport} game schedules and results. Filter by season, date, or team.`,
      params: [COMMON_PARAMS.season, COMMON_PARAMS.date, COMMON_PARAMS.team, COMMON_PARAMS.limit, COMMON_PARAMS.offset, COMMON_PARAMS.sort, COMMON_PARAMS.order],
      exampleQuery: `season=2025&date=2025-03-26`,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          {
            id: "game_001",
            home_team: "Lakers",
            away_team: "Celtics",
            date: "2025-03-26",
            home_score: 112,
            away_score: 108,
            status: "final",
          },
        ],
        meta: { total: 1, limit: 50, offset: 0 },
      }, null, 2),
    },
    {
      id: `${slug}-teams`,
      path: `/v1/${slug}/teams`,
      title: "Teams",
      description: `List all ${sport} teams for a given season.`,
      params: [COMMON_PARAMS.season, COMMON_PARAMS.limit, COMMON_PARAMS.offset],
      exampleQuery: `season=2025`,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          { id: "team_001", name: "Lakers", abbreviation: "LAL", conference: "Western", division: "Pacific" },
        ],
        meta: { total: 30, limit: 50, offset: 0 },
      }, null, 2),
    },
    {
      id: `${slug}-standings`,
      path: `/v1/${slug}/standings`,
      title: "Standings",
      description: `Get current ${sport} standings for a season.`,
      params: [COMMON_PARAMS.season, COMMON_PARAMS.sort, COMMON_PARAMS.order],
      exampleQuery: `season=2025`,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          { rank: 1, team: "Celtics", wins: 52, losses: 18, pct: 0.743, gb: "-" },
          { rank: 2, team: "Thunder", wins: 50, losses: 20, pct: 0.714, gb: "2.0" },
        ],
      }, null, 2),
    },
    {
      id: `${slug}-player-stats`,
      path: `/v1/${slug}/player-stats`,
      title: "Player Stats",
      description: `Retrieve ${sport} player statistics. Use aggregate=true for season totals.`,
      params: [COMMON_PARAMS.season, COMMON_PARAMS.player, COMMON_PARAMS.team, COMMON_PARAMS.aggregate, COMMON_PARAMS.date, COMMON_PARAMS.limit, COMMON_PARAMS.offset, COMMON_PARAMS.sort, COMMON_PARAMS.order],
      exampleQuery: `season=2025&player=LeBron&aggregate=true`,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          { player: "LeBron James", team: "Lakers", games: 65, ppg: 25.4, rpg: 7.1, apg: 8.2 },
        ],
        meta: { total: 1, limit: 50, offset: 0 },
      }, null, 2),
    },
    {
      id: `${slug}-odds`,
      path: `/v1/${slug}/odds`,
      title: "Odds",
      description: `Get betting odds for ${sport} games. Includes moneyline, spread, and totals.`,
      params: [COMMON_PARAMS.date, COMMON_PARAMS.team, COMMON_PARAMS.limit, COMMON_PARAMS.offset],
      exampleQuery: `date=2025-03-26`,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          {
            game_id: "game_001",
            home_team: "Lakers",
            away_team: "Celtics",
            moneyline: { home: -150, away: +130 },
            spread: { home: -3.5, away: +3.5 },
            total: { over: 220.5, under: 220.5 },
          },
        ],
      }, null, 2),
    },
    {
      id: `${slug}-injuries`,
      path: `/v1/${slug}/injuries`,
      title: "Injuries",
      description: `Get current ${sport} injury reports.`,
      params: [COMMON_PARAMS.team, COMMON_PARAMS.limit, COMMON_PARAMS.offset],
      exampleQuery: ``,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          { player: "Anthony Davis", team: "Lakers", status: "Questionable", injury: "Knee soreness", updated: "2025-03-25" },
        ],
      }, null, 2),
    },
    {
      id: `${slug}-news`,
      path: `/v1/${slug}/news`,
      title: "News",
      description: `Get latest ${sport} news articles.`,
      params: [COMMON_PARAMS.team, { name: "limit", type: "int", required: false, default: "20", description: "Maximum articles to return" }, COMMON_PARAMS.offset],
      exampleQuery: `limit=20`,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          { id: "news_001", title: "Trade Deadline Recap", source: "ESPN", published: "2025-03-25T18:00:00Z", url: "https://example.com/article" },
        ],
        meta: { total: 142, limit: 20, offset: 0 },
      }, null, 2),
    },
    {
      id: `${slug}-predictions`,
      path: `/v1/predictions/${slug}`,
      title: "Predictions",
      description: `Get AI-powered ${sport} game predictions with confidence scores.`,
      params: [COMMON_PARAMS.date, COMMON_PARAMS.team, COMMON_PARAMS.limit, COMMON_PARAMS.offset],
      exampleQuery: `date=2025-03-26`,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          {
            game_id: "game_001",
            home_team: "Lakers",
            away_team: "Celtics",
            predicted_winner: "Lakers",
            confidence: 0.68,
            predicted_spread: -3.2,
            predicted_total: 219.5,
          },
        ],
      }, null, 2),
    },
    {
      id: `${slug}-player-props`,
      path: `/v1/predictions/${slug}/player-props`,
      title: "Player Props Models",
      description: `Get trained player prop model metadata for ${sport}. Returns available prop types, inferred lines, ensemble counts, and feature information.`,
      params: [
        COMMON_PARAMS.date,
        { name: "prop_type", type: "string", required: false, description: "Filter by specific prop type (e.g., pts_over_20, double_double)" },
        COMMON_PARAMS.limit,
        COMMON_PARAMS.offset,
      ],
      exampleQuery: `date=2025-03-26`,
      exampleResponse: JSON.stringify({
        success: true,
        data: [
          {
            sport: "nba",
            prop_type: "pts_over_20",
            line: 20.0,
            market_type: "over_under",
            n_classifiers: 12,
            n_regressors: 12,
            model: "ensemble_voter",
            trained_at: "2026-03-30T20:06:44.554752",
          },
          {
            sport: "nba",
            prop_type: "double_double",
            line: null,
            market_type: "projection",
            n_classifiers: 12,
            n_regressors: 12,
            model: "ensemble_voter",
            trained_at: "2026-03-30T20:06:44.554752",
          },
        ],
        meta: {
          sport: "nba",
          date: "2026-03-26",
          count: 2,
          total: 4,
          limit: 50,
          offset: 0,
          model_available: true,
          supported_props: ["double_double", "pts_over_20", "pts_reb_ast_over_35"],
          feature_count: 140,
          trained_at: "2026-03-30T20:06:44.554752",
          seasons: [2023, 2024, 2025],
          cached_at: "2026-03-30T21:00:00Z",
        },
      }, null, 2),
    },
    {
      id: `${slug}-player-props-opportunities`,
      path: `/v1/predictions/${slug}/player-props/opportunities`,
      title: "Player Props Opportunities",
      description: `Get ranked player-prop opportunities for open ${sport} games using model availability, schedule context, and live momentum signals.`,
      params: [
        COMMON_PARAMS.date,
        { name: "prop_type", type: "string", required: false, description: "Restrict opportunities to one trained prop market" },
        { name: "min_score", type: "float", required: false, default: "0.0", description: "Minimum recommendation score (0-1)" },
        { name: "tier", type: "string", required: false, description: "Opportunity tier filter: high | medium | low" },
        COMMON_PARAMS.limit,
        COMMON_PARAMS.offset,
      ],
      exampleQuery: `date=2026-03-30&prop_type=pts_over_20&min_score=0.6&tier=high&limit=10`,
      exampleResponse: JSON.stringify({
        success: true,
        data: [
          {
            sport: "nba",
            game_id: "401810919",
            home_team: "Lakers",
            away_team: "Celtics",
            status: "in_progress",
            recommendation_score: 0.78,
            recommendation_tier: "high",
            available_markets: [
              { prop_type: "pts_over_20", line: 20.0, market_type: "over_under" },
              { prop_type: "double_double", line: null, market_type: "projection" },
            ],
            model_context: {
              trained_at: "2026-03-30T20:06:44.554752",
              feature_count: 140,
              supported_props: ["double_double", "pts_over_20"],
            },
            live_context: {
              live_home_wp: 0.66,
              live_away_wp: 0.34,
              momentum: "home",
              momentum_score: 0.12,
              time_remaining: "8:42",
            },
          },
        ],
        meta: {
          sport: "nba",
          date: "2026-03-30",
          count: 1,
          total: 1,
          limit: 10,
          offset: 0,
          model_available: true,
          open_games_considered: 3,
          supported_props: ["double_double", "pts_over_20"],
          prop_type: "pts_over_20",
          min_score: 0.6,
          tier: "high",
          cached_at: "2026-03-30T23:35:00Z",
        },
      }, null, 2),
    },
    {
      id: `${slug}-model-health`,
      path: `/v1/predictions/${slug}/history`,
      title: "Prediction History",
      description: `Historical prediction results for ${sport}, including evaluated counts and aggregate accuracy in the response metadata.`,
      params: [],
      exampleQuery: ``,
      exampleResponse: JSON.stringify({
        success: true,
        data: [
          {
            game_id: "401710800",
            sport: "nba",
            model: "catboost_v5.2",
            home_win_prob: 0.68,
            away_win_prob: 0.32,
            predicted_spread: -5.5,
            home_score: 115,
            away_score: 102,
          },
        ],
        meta: {
          sport: "nba",
          total_predictions: 1048,
          count: 1,
          limit: 50,
          offset: 0,
          evaluated: 986,
          correct: 612,
          accuracy: 0.6207,
          cached_at: "2026-03-30T23:20:00Z",
        },
      }, null, 2),
    },
    {
      id: `${slug}-live`,
      path: `/v1/sse/${slug}/live`,
      title: "Live",
      description: `Server-sent event stream for live ${sport} updates.`,
      params: [],
      exampleQuery: ``,
      exampleResponse: JSON.stringify({
        status: "success",
        data: [
          {
            game_id: "game_002",
            home_team: "Warriors",
            away_team: "Nuggets",
            home_score: 54,
            away_score: 48,
            period: "3rd",
            clock: "8:42",
            status: "in_progress",
          },
        ],
      }, null, 2),
    },
  ];
}

/* ── TryIt component ──────────────────────────────────────────────── */

function TryItPanel({ endpoint, apiKey }: { endpoint: EndpointDef; apiKey: string }) {
  const [params, setParams] = useState<Record<string, string>>({});
  const [response, setResponse] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<number | null>(null);

  const handleRun = useCallback(async () => {
    setLoading(true);
    setResponse(null);
    setStatus(null);
    const qs = Object.entries(params)
      .filter(([, v]) => v.trim() !== "")
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join("&");
    const url = `${API_URL}${endpoint.path}${qs ? `?${qs}` : ""}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    try {
      const res = await fetch(url, {
        headers: { "X-API-Key": apiKey },
        signal: controller.signal,
      });
      clearTimeout(timeout);
      setStatus(res.status);
      const text = await res.text();
      try {
        setResponse(JSON.stringify(JSON.parse(text), null, 2));
      } catch {
        setResponse(text);
      }
    } catch (err) {
      clearTimeout(timeout);
      const isAbort = err instanceof DOMException && err.name === "AbortError";
      setResponse(isAbort ? "Request timed out after 15 seconds." : `Error: ${err instanceof Error ? err.message : "Request failed"}`);
    } finally {
      setLoading(false);
    }
  }, [params, endpoint.path, apiKey]);

  return (
    <div className="apidocs-tryit">
      <div className="apidocs-tryit-header">
        <span className="apidocs-tryit-title">Try It</span>
      </div>
      <div className="apidocs-tryit-params">
        {endpoint.params.map((p) => (
          <div key={p.name} className="apidocs-tryit-field">
            <label htmlFor={`${endpoint.id}-${p.name}`}>{p.name}</label>
            <input
              id={`${endpoint.id}-${p.name}`}
              type="text"
              placeholder={p.default ?? ""}
              value={params[p.name] ?? ""}
              onChange={(e) => setParams((prev) => ({ ...prev, [p.name]: e.target.value }))}
            />
          </div>
        ))}
      </div>
      <button className="btn btn-primary btn-sm apidocs-tryit-run" onClick={handleRun} disabled={loading} aria-busy={loading}>
        {loading ? (
          <span style={{ display: "inline-flex", alignItems: "center", gap: "0.4rem" }}>
            <span className="btn-spinner" aria-hidden="true" /> Sending…
          </span>
        ) : "Send Request"}
      </button>
      {response !== null && (
        <div className="apidocs-tryit-response" role="region" aria-label="API response" aria-live="polite">
          <div className="apidocs-tryit-status">
            <span
              className={`apidocs-status-badge ${status && status < 300 ? "success" : "error"}`}
              role="status"
              aria-label={`Response status: ${status ?? "Error"}`}
            >
              {status ?? "ERR"}
            </span>
            Response
          </div>
          <pre className="apidocs-code"><code>{response}</code></pre>
        </div>
      )}
    </div>
  );
}

/* ── Collapsible section ──────────────────────────────────────────── */

function Collapsible({ title, children, defaultOpen = false, idBase }: { title: string; children: React.ReactNode; defaultOpen?: boolean; idBase?: string }) {
  const [open, setOpen] = useState(defaultOpen);
  const id = (idBase ?? title.toLowerCase().replace(/\s+/g, "-")) + "-panel";
  return (
    <div className={`apidocs-collapsible ${open ? "open" : ""}`}>
      <button
        className="apidocs-collapsible-trigger"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        aria-controls={id}
      >
        <span className="apidocs-collapsible-icon" aria-hidden="true">{open ? "▾" : "▸"}</span>
        {title}
      </button>
      {open && <div id={id} className="apidocs-collapsible-body" role="region" aria-label={title}>{children}</div>}
    </div>
  );
}

/* ── Global (cross-sport) endpoint definitions ────────────────────── */

const GLOBAL_ENDPOINTS: EndpointDef[] = [
  {
    id: "global-opportunities",
    path: "/v1/predictions/opportunities",
    title: "Aggregate Prop Opportunities",
    description:
      "Fetch ranked player-prop opportunities across ALL trained sports in one call. Useful for scanning every available market without querying each sport individually.",
    params: [
      COMMON_PARAMS.date,
      { name: "sports", type: "string", required: false, description: "Comma-separated sport slugs to include (e.g. nba,nfl). Defaults to all trained sports." },
      { name: "prop_type", type: "string", required: false, description: "Restrict to one prop market (e.g. pts_over_20)" },
      { name: "min_score", type: "float", required: false, default: "0.0", description: "Minimum recommendation score (0–1)" },
      { name: "tier", type: "string", required: false, description: "Opportunity tier: high | medium | low" },
      COMMON_PARAMS.limit,
      COMMON_PARAMS.offset,
    ],
    exampleQuery: "date=2026-03-30&tier=high&limit=20",
    exampleResponse: JSON.stringify({
      success: true,
      data: [
        {
          sport: "nba",
          game_id: "401810919",
          home_team: "Lakers",
          away_team: "Celtics",
          status: "in_progress",
          recommendation_score: 0.82,
          recommendation_tier: "high",
          available_markets: [
            { prop_type: "pts_over_20", line: 20.0, market_type: "over_under" },
          ],
        },
        {
          sport: "nfl",
          game_id: "401720041",
          home_team: "Chiefs",
          away_team: "Bills",
          status: "scheduled",
          recommendation_score: 0.74,
          recommendation_tier: "high",
          available_markets: [
            { prop_type: "pass_yds_over_250", line: 250.5, market_type: "over_under" },
          ],
        },
      ],
      meta: {
        date: "2026-03-30",
        count: 2,
        total: 8,
        limit: 20,
        offset: 0,
        tier: "high",
        trained_sports: ["nba", "nfl"],
        prop_type_counts: { pts_over_20: 1, pass_yds_over_250: 1 },
        tier_counts: { high: 2, medium: 4, low: 2 },
      },
    }, null, 2),
  },
  {
    id: "global-leaderboard",
    path: "/v1/predictions/leaderboard",
    title: "Prediction Leaderboard",
    description:
      "Rank all sports by historical prediction accuracy. Compare model performance across leagues using win-rate, calibration (Brier score), and evaluation sample size.",
    params: [
      { name: "date_start", type: "string", required: false, description: "Start of evaluation window (YYYY-MM-DD)" },
      { name: "date_end", type: "string", required: false, description: "End of evaluation window (YYYY-MM-DD)" },
      { name: "min_evaluated", type: "int", required: false, default: "1", description: "Minimum number of evaluated games required to appear in rankings" },
    ],
    exampleQuery: "date_start=2026-01-01&date_end=2026-03-30&min_evaluated=10",
    exampleResponse: JSON.stringify({
      success: true,
      data: [
        { rank: 1, sport: "nba", accuracy: 0.71, brier_score: 0.21, evaluated: 124 },
        { rank: 2, sport: "nfl", accuracy: 0.66, brier_score: 0.24, evaluated: 88 },
        { rank: 3, sport: "nhl", accuracy: 0.61, brier_score: 0.27, evaluated: 56 },
      ],
      meta: {
        date_start: "2026-01-01",
        date_end: "2026-03-30",
        min_evaluated: 10,
        total_sports: 3,
      },
    }, null, 2),
  },
  {
    id: "global-trained-sports",
    path: "/v1/predictions/trained-sports",
    title: "Trained Sports",
    description:
      "List every sport that has a trained player-props model available. Returns model file size and last training timestamp. Useful for checking readiness before querying sport-specific endpoints.",
    params: [],
    exampleQuery: "",
    exampleResponse: JSON.stringify({
      success: true,
      data: [
        { sport: "nba", model_size_bytes: 2457600, modified_at: "2026-03-30T20:06:44Z" },
        { sport: "nfl", model_size_bytes: 1884160, modified_at: "2026-03-29T18:30:00Z" },
      ],
      meta: { count: 2 },
    }, null, 2),
  },
  {
    id: "global-cache",
    path: "/v1/predictions/cache",
    method: "DELETE",
    title: "Invalidate Bundle Cache",
    description:
      "Flush the in-memory player-props bundle cache. All cached sport models are evicted and will be reloaded from disk on the next request. Useful after deploying a newly trained model.",
    params: [],
    exampleQuery: "",
    exampleResponse: JSON.stringify({
      success: true,
      evicted: ["nba", "nfl"],
      message: "Bundle cache cleared. 2 entries evicted.",
    }, null, 2),
  },
];

/* ── Endpoint section ─────────────────────────────────────────────── */

function EndpointSection({ endpoint, apiKey }: { endpoint: EndpointDef; apiKey: string }) {
  const curlKey = apiKey || "your-api-key-here";
  const method = endpoint.method ?? "GET";
  const curlUrl = `${DOCS_BASE_URL}${endpoint.path}${endpoint.exampleQuery ? `?${endpoint.exampleQuery}` : ""}`;
  const paramsSummary = endpoint.params.length === 0
    ? "No query parameters"
    : `${endpoint.params.length} parameter${endpoint.params.length === 1 ? "" : "s"}: ${endpoint.params.slice(0, 4).map((p) => p.name).join(", ")}${endpoint.params.length > 4 ? ", ..." : ""}`;
  const curlCmd = method === "DELETE"
    ? `curl -X DELETE -H "X-API-Key: ${curlKey}" \\\n  "${curlUrl}"`
    : `curl -H "X-API-Key: ${curlKey}" \\\n  "${curlUrl}"`;
  return (
    <section id={endpoint.id} className="apidocs-endpoint">
      <div className="apidocs-endpoint-header">
        <span className={`apidocs-method${method === "DELETE" ? " apidocs-method--delete" : ""}`}>{method}</span>
        <code className="apidocs-path">{endpoint.path}</code>
      </div>
      <p className="apidocs-desc">{endpoint.description}</p>
      <p className="apidocs-endpoint-meta">{paramsSummary}</p>

      <Collapsible title="Schema, Examples, and Try It" defaultOpen={false} idBase={`${endpoint.id}-details`}>
        {endpoint.params.length > 0 && (
          <div className="apidocs-params-wrap">
            <h4>Parameters</h4>
            <div className="apidocs-table-wrap responsive-table-wrap">
              <table className="apidocs-params-table apidocs-endpoint-params-table">
                <thead>
                  <tr>
                    <th scope="col">Name</th>
                    <th scope="col">Type</th>
                    <th scope="col">Required</th>
                    <th scope="col">Default</th>
                    <th scope="col">Description</th>
                  </tr>
                </thead>
                <tbody>
                  {endpoint.params.map((p) => (
                    <tr key={p.name}>
                      <td><code>{p.name}</code></td>
                      <td>{p.type}</td>
                      <td>{p.required ? "Yes" : "No"}</td>
                      <td>{p.default ?? "—"}</td>
                      <td>{p.description}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        <h4 className="apidocs-subsection-title">Example Request</h4>
        <div style={{ position: "relative" }}>
          <CopyButton text={curlCmd} label="Copy" className="apidocs-copy-btn" />
          <pre className="apidocs-code"><code>{curlCmd}</code></pre>
        </div>

        <h4 className="apidocs-subsection-title">Example Response</h4>
        <div style={{ position: "relative" }}>
          <CopyButton text={endpoint.exampleResponse} label="Copy" className="apidocs-copy-btn" />
          <pre className="apidocs-code"><code>{endpoint.exampleResponse}</code></pre>
        </div>

        <TryItPanel endpoint={endpoint} apiKey={apiKey} />
      </Collapsible>
    </section>
  );
}

/* ── Gate component ───────────────────────────────────────────────── */

function UpgradeGate() {
  return (
    <div className="apidocs-gate">
      <div className="apidocs-gate-card">
        <div className="apidocs-gate-icon">🔒</div>
        <h1>Enterprise API Access</h1>
        <p>
          The interactive API documentation is available exclusively for
          Enterprise-tier subscribers. Upgrade your plan to unlock full API
          access with your personal API key.
        </p>
        <div className="apidocs-gate-actions">
          <Link href="/pricing" className="btn btn-primary">
            Upgrade to Enterprise
          </Link>
          <Link href="/account" className="btn btn-secondary">
            My Account
          </Link>
        </div>
      </div>
    </div>
  );
}

/* ── Main component ───────────────────────────────────────────────── */

export default function ApiDocsClient() {
  const { user, isLoading } = useAuth();
  const [activeSection, setActiveSection] = useState("overview");
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [visibleSportSlug, setVisibleSportSlug] = useState("nba");
  const [basicsOpen, setBasicsOpen] = useState(false);
  const mainRef = useRef<HTMLDivElement>(null);

  const endpointMatchesQuery = useCallback((endpoint: EndpointDef, q: string) => {
    return (
      endpoint.id.toLowerCase().includes(q)
      || endpoint.title.toLowerCase().includes(q)
      || endpoint.path.toLowerCase().includes(q)
      || endpoint.description.toLowerCase().includes(q)
      || endpoint.params.some((param) =>
        param.name.toLowerCase().includes(q)
        || param.description.toLowerCase().includes(q),
      )
    );
  }, []);

  const filteredSportGroups = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();

    return SPORT_CATEGORIES.flatMap((cat) =>
      cat.sports.flatMap((sport) => {
        const slug = SPORT_SLUG[sport];
        const endpoints = buildEndpoints(sport, slug);

        if (!q) {
          return [{ category: cat.label, sport, slug, endpoints }];
        }

        const sportMatch = sport.toLowerCase().includes(q) || cat.label.toLowerCase().includes(q);
        const matchedEndpoints = sportMatch
          ? endpoints
          : endpoints.filter((endpoint) => endpointMatchesQuery(endpoint, q));

        if (matchedEndpoints.length === 0) {
          return [];
        }

        return [{ category: cat.label, sport, slug, endpoints: matchedEndpoints }];
      }),
    );
  }, [searchQuery, endpointMatchesQuery]);

  const filteredCategories = useMemo(() => {
    if (!searchQuery.trim()) return SPORT_CATEGORIES;

    const sportsByCategory = filteredSportGroups.reduce<Record<string, string[]>>((acc, group) => {
      if (!acc[group.category]) {
        acc[group.category] = [];
      }
      acc[group.category].push(group.sport);
      return acc;
    }, {});

    return SPORT_CATEGORIES.map((cat) => ({
      ...cat,
      sports: cat.sports.filter((sport) => (sportsByCategory[cat.label] ?? []).includes(sport)),
    })).filter((cat) => cat.sports.length > 0);
  }, [searchQuery, filteredSportGroups]);

  const filteredGlobalEndpoints = useMemo(() => {
    if (!searchQuery.trim()) return GLOBAL_ENDPOINTS;
    const q = searchQuery.toLowerCase();
    return GLOBAL_ENDPOINTS.filter(
      (ep) => endpointMatchesQuery(ep, q),
    );
  }, [searchQuery, endpointMatchesQuery]);

  const endpointCount = useMemo(() => {
    const sportEndpointCount = filteredSportGroups.reduce((sum, group) => sum + group.endpoints.length, 0);
    return filteredGlobalEndpoints.length + sportEndpointCount;
  }, [filteredGlobalEndpoints, filteredSportGroups]);

  const renderedSportGroups = useMemo(() => {
    if (searchQuery.trim()) {
      return filteredSportGroups;
    }
    return filteredSportGroups.filter((group) => group.slug === visibleSportSlug);
  }, [searchQuery, filteredSportGroups, visibleSportSlug]);

  const isEnterprise = user?.tier === "enterprise" || user?.tier === "dev";
  const apiKey = user?.api_key ?? "";

  /* scroll-spy: track which section is in view */
  useEffect(() => {
    if (!isEnterprise) return;
    const container = mainRef.current;
    if (!container) return;
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            setActiveSection(entry.target.id);
          }
        }
      },
      { root: container, rootMargin: "-20% 0px -70% 0px", threshold: 0 },
    );
    const sections = container.querySelectorAll("section[id]");
    sections.forEach((s) => observer.observe(s));
    return () => observer.disconnect();
  }, [isEnterprise]);

  if (isLoading) {
    return (
      <div className="apidocs-loading">
        <div className="apidocs-spinner" />
        <p>Loading…</p>
      </div>
    );
  }

  if (!user || !isEnterprise) {
    return <UpgradeGate />;
  }

  const scrollTo = (id: string) => {
    if (id === "overview" || id === "authentication" || id === "rate-limits") {
      setBasicsOpen(true);
      requestAnimationFrame(() => {
        const target = document.getElementById(id);
        if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    }
    setActiveSection(id);
    setSidebarOpen(false);
    if (id !== "overview" && id !== "authentication" && id !== "rate-limits") {
      const el = document.getElementById(id);
      if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  return (
    <div className="apidocs-layout">
      {/* Mobile sidebar toggle */}
      <button
        className={`apidocs-sidebar-toggle ${sidebarOpen ? "open" : ""}`}
        onClick={() => setSidebarOpen((v) => !v)}
        aria-label={`${sidebarOpen ? "Close" : "Open"} API sidebar`}
        aria-expanded={sidebarOpen}
        aria-controls="apidocs-sidebar"
      >
        ☰ API Reference
      </button>

      {/* Sidebar */}
      <aside id="apidocs-sidebar" className={`apidocs-sidebar ${sidebarOpen ? "open" : ""}`}>
        <div className="apidocs-sidebar-header">
          <span>API Reference</span>
          <span className="apidocs-version">v1</span>
        </div>

        {/* Search box */}
        <div className="apidocs-sidebar-search">
          <input
            type="search"
            placeholder="Search endpoints…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            aria-label="Search API endpoints"
            className="apidocs-sidebar-search-input"
          />
        </div>

        <nav className="apidocs-sidebar-nav">
          <button
            className={`apidocs-nav-item ${activeSection === "overview" ? "active" : ""}`}
            onClick={() => scrollTo("overview")}
          >
            Overview
          </button>
          <button
            className={`apidocs-nav-item ${activeSection === "authentication" ? "active" : ""}`}
            onClick={() => scrollTo("authentication")}
          >
            Authentication
          </button>
          <button
            className={`apidocs-nav-item ${activeSection === "rate-limits" ? "active" : ""}`}
            onClick={() => scrollTo("rate-limits")}
          >
            Rate Limits
          </button>

          <div className="apidocs-nav-divider" />

          {/* Global / cross-sport endpoints */}
          {filteredGlobalEndpoints.length > 0 && (
          <div className="apidocs-nav-group">
            <div className="apidocs-nav-group-label">Predictions</div>
            {filteredGlobalEndpoints.map((ep) => (
              <button
                key={ep.id}
                className={`apidocs-nav-item apidocs-nav-sport ${activeSection === ep.id ? "active" : ""}`}
                onClick={() => scrollTo(ep.id)}
              >
                {ep.title}
              </button>
            ))}
          </div>
          )}

          <div className="apidocs-nav-divider" />

          {filteredCategories.map((cat) => (
            <div key={cat.label} className="apidocs-nav-group">
              <div className="apidocs-nav-group-label">{cat.label}</div>
              {cat.sports.map((sport) => {
                const slug = SPORT_SLUG[sport];
                const isVisibleSport = !searchQuery.trim() && visibleSportSlug === slug;
                return (
                  <button
                    key={slug}
                    className={`apidocs-nav-item apidocs-nav-sport ${activeSection.startsWith(slug) || isVisibleSport ? "active" : ""}`}
                    onClick={() => {
                      setVisibleSportSlug(slug);
                      scrollTo(`${slug}-games`);
                    }}
                  >
                    {sport}
                  </button>
                );
              })}
            </div>
          ))}
        </nav>
      </aside>

      {sidebarOpen && (
        <div className="apidocs-sidebar-overlay" onClick={() => setSidebarOpen(false)} />
      )}

      {/* Main content */}
      <div className="apidocs-main" ref={mainRef}>
        {searchQuery.trim() && (
          <div className="apidocs-results-bar" role="status" aria-live="polite">
            Showing {endpointCount} matching endpoint{endpointCount === 1 ? "" : "s"}
          </div>
        )}

        <section className="apidocs-section">
          <h1>WNBP API Documentation</h1>
          <p>
            Real-time scores, standings, player statistics, odds, and prediction endpoints across 20+ sports.
          </p>
          <div className="apidocs-info-box">
            <strong>Base URL</strong>
            <code>{DOCS_BASE_URL}</code>
          </div>
          <div className="apidocs-info-box">
            <strong>Your API Key</strong>
            <code className="apidocs-api-key">{apiKey || "Not available — check your account page"}</code>
          </div>

          <div className={`apidocs-collapsible ${basicsOpen ? "open" : ""}`}>
            <button
              className="apidocs-collapsible-trigger"
              onClick={() => setBasicsOpen((v) => !v)}
              aria-expanded={basicsOpen}
              aria-controls="apidocs-basics-panel"
            >
              <span className="apidocs-collapsible-icon" aria-hidden="true">{basicsOpen ? "▾" : "▸"}</span>
              Basics: Response Format, Authentication, and Rate Limits
            </button>

            {basicsOpen && (
              <div id="apidocs-basics-panel" className="apidocs-collapsible-body" role="region" aria-label="API basics">
                <section id="overview" className="apidocs-section">
                  <h3>Response Format</h3>
                  <p>All responses are JSON. Successful responses have the shape:</p>
                  <pre className="apidocs-code"><code>{`{
  "status": "success",
  "data": [ ... ],
  "meta": {
    "total": 100,
    "limit": 50,
    "offset": 0
  }
}`}</code></pre>
                  <p>Error responses include a message:</p>
                  <pre className="apidocs-code"><code>{`{
  "status": "error",
  "message": "Invalid API key",
  "code": 401
}`}</code></pre>
                </section>

                <section id="authentication" className="apidocs-section">
                  <h3>Authentication</h3>
                  <p>
                    All API requests require an API key passed via the <code>X-API-Key</code> header.
                    You can find your API key on your{" "}
                    <Link href="/account">account page</Link>.
                  </p>
                  <pre className="apidocs-code"><code>{`curl -H "X-API-Key: ${apiKey || "your-api-key-here"}" \\
  "${DOCS_BASE_URL}/v1/nba/games?season=2025"`}</code></pre>
                  <div className="apidocs-warning-box">
                    <strong>⚠ Keep your key secret.</strong> Do not expose it in client-side code
                    or public repositories. Rotate your key from the account page if compromised.
                  </div>
                </section>

                <section id="rate-limits" className="apidocs-section">
                  <h3>Rate Limits</h3>
                  <p>Rate limits vary by subscription tier:</p>
                  <div className="apidocs-table-wrap responsive-table-wrap">
                    <table className="apidocs-params-table apidocs-rate-limits-table">
                      <thead>
                        <tr><th>Tier</th><th>Requests / min</th><th>Requests / day</th></tr>
                      </thead>
                      <tbody>
                        <tr><td>Starter</td><td>30</td><td>1,000</td></tr>
                        <tr><td>Pro</td><td>120</td><td>10,000</td></tr>
                        <tr><td>Enterprise</td><td>600</td><td>100,000</td></tr>
                      </tbody>
                    </table>
                  </div>
                  <p>
                    Rate limit headers are included in every response:
                  </p>
                  <pre className="apidocs-code"><code>{`X-RateLimit-Limit: 600
X-RateLimit-Remaining: 594
X-RateLimit-Reset: 1711459260`}</code></pre>
                </section>
              </div>
            )}
          </div>
        </section>

        <div className="apidocs-divider" />

        {/* Cross-sport / global prediction endpoints */}
        {filteredGlobalEndpoints.length > 0 && (
          <>
            <div className="apidocs-sport-group">
              <h2 className="apidocs-sport-title" id="global-opportunities" style={{ scrollMarginTop: "80px" }}>
                Cross-Sport Predictions
              </h2>
              <p className="apidocs-desc" style={{ marginBottom: "1.5rem" }}>
                These endpoints operate across all trained sports simultaneously and do not require a sport slug in the path.
              </p>
              {filteredGlobalEndpoints.map((ep) => (
                <div key={ep.id} id={ep.id} style={{ scrollMarginTop: "80px" }}>
                  <EndpointSection endpoint={ep} apiKey={apiKey} />
                </div>
              ))}
            </div>

            <div className="apidocs-divider" />
          </>
        )}

        {/* Sport endpoints */}
        {!searchQuery.trim() && (
          <div className="apidocs-results-bar" role="status" aria-live="polite">
            Focus mode: showing {renderedSportGroups[0]?.sport ?? "selected"} endpoints
          </div>
        )}

        {renderedSportGroups.map(({ sport, slug, endpoints }) => (
          <div key={slug} className="apidocs-sport-group">
            <h2 className="apidocs-sport-title" id={`${slug}-games`} style={{ scrollMarginTop: "80px" }}>
              {sport}
            </h2>
            {endpoints.map((ep) => (
              <div key={ep.id} id={ep.id === `${slug}-games` ? undefined : ep.id} style={{ scrollMarginTop: "80px" }}>
                <EndpointSection endpoint={ep} apiKey={apiKey} />
              </div>
            ))}
          </div>
        ))}

        {searchQuery.trim() && endpointCount === 0 && (
          <div className="apidocs-empty-state" role="status" aria-live="polite">
            No endpoints matched "{searchQuery.trim()}". Try a sport, path segment, or parameter name.
          </div>
        )}
      </div>
    </div>
  );
}
