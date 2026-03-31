import Link from "next/link";
import { notFound } from "next/navigation";
import { getViewerTier, hasEnterpriseDocsAccess } from "@/lib/server-access";
import styles from "./page.module.css";

export const metadata = {
  title: "Enterprise API Guide",
  description: "Sellable endpoint guide for enterprise and development members.",
};

type GuideParameter = {
  name: string;
  in: "path" | "query";
  required: boolean;
  example: string;
  notes: string;
};

type GuideEndpoint = {
  title: string;
  method: "GET";
  path: string;
  fullUrl: string;
  description: string;
  bestFor: string;
  parameters: GuideParameter[];
  exampleResponse: string;
};

const API_BASE = (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000").replace(/\/$/, "");
const SELLABLE_OPENAPI_URL = `${API_BASE}/openapi-sellable.json`;

const GUIDE_ENDPOINTS: GuideEndpoint[] = [
  {
    title: "Overview",
    method: "GET",
    path: "/v1/{sport}/overview",
    fullUrl: `${API_BASE}/v1/nfl/overview?season=2025`,
    description: "Single-call sport summary for landing pages and lightweight app shells.",
    bestFor: "Sport hubs, dashboard cards, and league landing pages.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nfl", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Current or historical season context." },
    ],
    exampleResponse: `{
  "success": true,
  "data": {
    "recent_games": [{ "id": "401547001", "date": "2025-03-30", "home_team": "Chiefs", "away_team": "Bills", "status": "final" }],
    "standings": [{ "team_id": "14", "wins": 12, "losses": 4, "conference_rank": 1 }],
    "top_news": [{ "headline": "Kansas City locks up home field", "published_at": "2025-03-30T18:20:00Z" }],
    "injury_count": 19,
    "team_count": 32,
    "game_count": 272
  },
  "meta": {
    "sport": "nfl",
    "season": "2025"
  }
}`,
  },
  {
    title: "Games",
    method: "GET",
    path: "/v1/{sport}/games",
    fullUrl: `${API_BASE}/v1/nba/games?season=2025&date=2025-03-31&status=final&sort=-date&limit=25&offset=0`,
    description: "Primary normalized game feed for schedules, scoreboards, and archives.",
    bestFor: "Daily slates, scoreboards, archives, and team-specific game views.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season year. Use `all` for all seasons." },
      { name: "date", in: "query", required: false, example: "2025-03-31", notes: "Exact day filter for one slate." },
      { name: "date_start", in: "query", required: false, example: "2025-03-01", notes: "Start of date range (inclusive)." },
      { name: "date_end", in: "query", required: false, example: "2025-03-31", notes: "End of date range (inclusive)." },
      { name: "team", in: "query", required: false, example: "Lakers", notes: "Filter by team name or team ID." },
      { name: "status", in: "query", required: false, example: "final", notes: "Typical values: scheduled, in_progress, final." },
      { name: "sort", in: "query", required: false, example: "-date", notes: "Sort field, prefix with `-` for descending." },
      { name: "limit", in: "query", required: false, example: "25", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "id": "401710904",
      "sport": "nba",
      "season": "2025",
      "date": "2025-03-31",
      "home_team": "Los Angeles Lakers",
      "away_team": "Phoenix Suns",
      "home_score": 118,
      "away_score": 112,
      "status": "final",
      "venue": "Crypto.com Arena"
    }
  ],
  "meta": {
    "sport": "nba",
    "season": "2025",
    "count": 1,
    "limit": 25,
    "offset": 0
  }
}`,
  },
  {
    title: "Game Detail",
    method: "GET",
    path: "/v1/{sport}/games/{game_id}",
    fullUrl: `${API_BASE}/v1/wta/games/173741`,
    description: "Single game endpoint with sport-specific stat fields and event metadata.",
    bestFor: "Game detail pages, sport-specific stat panels, and event drill-downs.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "wta", notes: "League or tour key in the path." },
      { name: "game_id", in: "path", required: true, example: "173741", notes: "Backend game identifier." },
    ],
    exampleResponse: `{
  "success": true,
  "data": {
    "id": "173741",
    "sport": "wta",
    "date": "2026-03-31",
    "home_team": "Iga Swiatek",
    "away_team": "Aryna Sabalenka",
    "status": "in_progress",
    "total_sets": 3,
    "home_sets_won": 1,
    "away_sets_won": 1,
    "home_aces": 6,
    "away_aces": 8
  },
  "meta": {
    "sport": "wta",
    "season": "2026"
  }
}`,
  },
  {
    title: "Teams",
    method: "GET",
    path: "/v1/{sport}/teams",
    fullUrl: `${API_BASE}/v1/mlb/teams?season=2025&limit=50&offset=0`,
    description: "Normalized team directory with branding and league metadata.",
    bestFor: "Team pickers, navigation, logos, and reference joins.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "mlb", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season context for teams." },
      { name: "limit", in: "query", required: false, example: "50", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "id": "147",
      "name": "New York Yankees",
      "abbreviation": "NYY",
      "city": "New York",
      "league": "AL",
      "division": "East",
      "venue_name": "Yankee Stadium"
    }
  ],
  "meta": {
    "sport": "mlb",
    "season": "2025",
    "count": 1
  }
}`,
  },
  {
    title: "Team Detail",
    method: "GET",
    path: "/v1/{sport}/teams/{team_id}",
    fullUrl: `${API_BASE}/v1/nba/teams/14?season=2025`,
    description: "Single team profile with roster attachment when available.",
    bestFor: "Team profile pages, roster modules, and team-level context panels.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "team_id", in: "path", required: true, example: "14", notes: "Team ID or abbreviation." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season context for roster data." },
    ],
    exampleResponse: `{
  "success": true,
  "data": {
    "id": "14",
    "name": "Golden State Warriors",
    "conference": "Western",
    "division": "Pacific",
    "roster": [
      { "id": "201939", "name": "Stephen Curry", "position": "G" },
      { "id": "1626172", "name": "Draymond Green", "position": "F" }
    ]
  },
  "meta": {
    "sport": "nba",
    "season": "2025"
  }
}`,
  },
  {
    title: "Players",
    method: "GET",
    path: "/v1/{sport}/players",
    fullUrl: `${API_BASE}/v1/nba/players?season=2025&team_id=14&search=Stephen&limit=20&offset=0`,
    description: "Normalized player directory with roster and status context.",
    bestFor: "Search, roster views, selectors, and player profile routing.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season roster context." },
      { name: "team_id", in: "query", required: false, example: "14", notes: "Optional team filter." },
      { name: "search", in: "query", required: false, example: "Stephen", notes: "Case-insensitive player search." },
      { name: "limit", in: "query", required: false, example: "20", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "id": "201939",
      "name": "Stephen Curry",
      "team_id": "14",
      "position": "G",
      "status": "active",
      "headshot_url": "https://cdn.example.com/players/201939.png"
    }
  ],
  "meta": {
    "sport": "nba",
    "season": "2025",
    "count": 1,
    "limit": 20
  }
}`,
  },
  {
    title: "Player Stats",
    method: "GET",
    path: "/v1/{sport}/player-stats",
    fullUrl: `${API_BASE}/v1/nba/player-stats?season=2025&player_id=201939&aggregate=true&limit=10&offset=0`,
    description: "Player game-level or aggregated season stat lines with sport-specific categories.",
    bestFor: "Player dashboards, prop model features, and trend analytics.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season context for stats." },
      { name: "player_id", in: "query", required: false, example: "201939", notes: "Filter to a single player." },
      { name: "aggregate", in: "query", required: false, example: "true", notes: "Return season averages instead of per-game rows." },
      { name: "limit", in: "query", required: false, example: "10", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "player_id": "201939",
      "player_name": "Stephen Curry",
      "season": "2025",
      "category": "basketball",
      "pts": 28.4,
      "ast": 6.1,
      "three_pct": 41.3
    }
  ],
  "meta": {
    "sport": "nba",
    "season": "2025",
    "count": 1
  }
}`,
  },
  {
    title: "Standings",
    method: "GET",
    path: "/v1/{sport}/standings",
    fullUrl: `${API_BASE}/v1/nhl/standings?season=2025&conference=Eastern&limit=50&offset=0`,
    description: "Normalized standings for conference, division, and overall ranking tables.",
    bestFor: "Tables, playoff race views, and season progress surfaces.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nhl", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season table to load." },
      { name: "conference", in: "query", required: false, example: "Eastern", notes: "Conference filter when supported." },
      { name: "limit", in: "query", required: false, example: "50", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "team_id": "10",
      "team": "Boston Bruins",
      "wins": 49,
      "losses": 19,
      "pct": 0.721,
      "conference": "Eastern",
      "conference_rank": 1
    }
  ],
  "meta": {
    "sport": "nhl",
    "season": "2025",
    "count": 1
  }
}`,
  },
  {
    title: "Odds",
    method: "GET",
    path: "/v1/{sport}/odds",
    fullUrl: `${API_BASE}/v1/nba/odds?season=2025&date=2025-03-31&game_id=401710904&limit=20&offset=0`,
    description: "Normalized bookmaker pricing for game markets across supported sports.",
    bestFor: "Odds boards, market comparisons, and pricing ingestion.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season context." },
      { name: "date", in: "query", required: false, example: "2025-03-31", notes: "Date filter." },
      { name: "game_id", in: "query", required: false, example: "401710904", notes: "Optional single-game focus." },
      { name: "limit", in: "query", required: false, example: "20", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "game_id": "401710904",
      "bookmaker": "DraftKings",
      "market": "h2h",
      "home_team": "Lakers",
      "away_team": "Suns",
      "home_price": -135,
      "away_price": 114,
      "last_update": "2025-03-31T17:40:00Z"
    }
  ],
  "meta": {
    "sport": "nba",
    "count": 1,
    "limit": 20
  }
}`,
  },
  {
    title: "Market Signals",
    method: "GET",
    path: "/v1/{sport}/market-signals",
    fullUrl: `${API_BASE}/v1/nba/market-signals?season=2025&bookmaker=DraftKings&regime=moving&limit=25&offset=0`,
    description: "Line-movement enrichment derived from odds snapshots, including movement/regime context.",
    bestFor: "Market movement monitors, volatility filters, and pre-trade signal pipelines.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season context." },
      { name: "game_id", in: "query", required: false, example: "401710904", notes: "Optional single-game filter." },
      { name: "bookmaker", in: "query", required: false, example: "DraftKings", notes: "Case-insensitive bookmaker filter." },
      { name: "regime", in: "query", required: false, example: "moving", notes: "Market regime filter: stable/moving/volatile." },
      { name: "limit", in: "query", required: false, example: "25", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "game_id": "401710904",
      "sport": "nba",
      "bookmaker": "DraftKings",
      "market_regime": "moving",
      "home_team": "Lakers",
      "away_team": "Suns",
      "spread_open": -3.5,
      "spread_close": -4.5,
      "spread_abs_move": 1.0
    }
  ],
  "meta": {
    "sport": "nba",
    "season": "2025",
    "count": 1,
    "limit": 25,
    "offset": 0
  }
}`,
  },
  {
    title: "Schedule Fatigue",
    method: "GET",
    path: "/v1/{sport}/schedule-fatigue",
    fullUrl: `${API_BASE}/v1/nba/schedule-fatigue?season=2025&fatigue_level=high&limit=25&offset=0`,
    description: "Team schedule congestion enrichment with rest windows, back-to-back flags, and fatigue scores.",
    bestFor: "Rest-disadvantage models, spot analysis, and game-context augmentation.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "season", in: "query", required: false, example: "2025", notes: "Season context." },
      { name: "game_id", in: "query", required: false, example: "401710904", notes: "Optional single-game filter." },
      { name: "team_id", in: "query", required: false, example: "14", notes: "Optional team filter." },
      { name: "fatigue_level", in: "query", required: false, example: "high", notes: "Filter by fatigue bucket: low/medium/high." },
      { name: "limit", in: "query", required: false, example: "25", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "game_id": "401710904",
      "sport": "nba",
      "team_id": "14",
      "fatigue_level": "high",
      "fatigue_score": 0.78,
      "is_back_to_back": true,
      "games_last_7d": 4
    }
  ],
  "meta": {
    "sport": "nba",
    "season": "2025",
    "count": 1,
    "limit": 25,
    "offset": 0
  }
}`,
  },
  {
    title: "Predictions",
    method: "GET",
    path: "/v1/predictions/{sport}",
    fullUrl: `${API_BASE}/v1/predictions/nba?date=2026-03-31&limit=50&offset=0`,
    description: "Model win-probability and spread/total predictions for one sport.",
    bestFor: "Prediction boards, game-detail model cards, and automation workflows.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "date", in: "query", required: false, example: "2026-03-31", notes: "Prediction date. Defaults to today." },
      { name: "limit", in: "query", required: false, example: "50", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "game_id": "401710904",
      "sport": "nba",
      "home_win_prob": 0.61,
      "away_win_prob": 0.39,
      "predicted_spread": -4.0,
      "predicted_total": 227.5,
      "confidence": 0.74
    }
  ],
  "meta": {
    "sport": "nba",
    "date": "2026-03-31",
    "count": 1,
    "limit": 50,
    "offset": 0
  }
}`,
  },
  {
    title: "Player Prop Markets",
    method: "GET",
    path: "/v1/predictions/{sport}/player-props",
    fullUrl: `${API_BASE}/v1/predictions/nba/player-props?date=2026-03-31&prop_type=pts_over_20&limit=50&offset=0`,
    description: "Lists trained player-prop model markets for a sport (metadata only, not per-player picks).",
    bestFor: "Model capability checks, supported prop pickers, and preflight endpoint selection.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "date", in: "query", required: false, example: "2026-03-31", notes: "Date context. Defaults to today." },
      { name: "prop_type", in: "query", required: false, example: "pts_over_20", notes: "Filter to a single trained market." },
      { name: "limit", in: "query", required: false, example: "50", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "sport": "nba",
      "prop_type": "pts_over_20",
      "line": 20,
      "market_type": "over_under",
      "n_classifiers": 12,
      "n_regressors": 12,
      "trained_at": "2026-03-30T20:06:44Z"
    }
  ],
  "meta": {
    "sport": "nba",
    "count": 1,
    "total": 4,
    "model_available": true
  }
}`,
  },
  {
    title: "Player Prop Opportunities (Sport)",
    method: "GET",
    path: "/v1/predictions/{sport}/player-props/opportunities",
    fullUrl: `${API_BASE}/v1/predictions/nba/player-props/opportunities?date=2026-03-31&prop_type=pts_over_20&min_score=0.6&tier=high&limit=50&offset=0`,
    description: "Sport-specific ranked player-prop opportunities for open games.",
    bestFor: "Single-sport prop boards and sport-scoped recommendation workflows.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "date", in: "query", required: false, example: "2026-03-31", notes: "Target date. Defaults to today." },
      { name: "prop_type", in: "query", required: false, example: "pts_over_20", notes: "Filter to a specific prop market." },
      { name: "min_score", in: "query", required: false, example: "0.6", notes: "Minimum recommendation score (0..1)." },
      { name: "tier", in: "query", required: false, example: "high", notes: "Filter by recommendation tier: high/medium/low." },
      { name: "limit", in: "query", required: false, example: "50", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "sport": "nba",
      "game_id": "401710904",
      "recommendation_score": 0.82,
      "recommendation_tier": "high",
      "available_markets": [
        { "prop_type": "pts_over_24", "line": 24, "market_type": "over_under" }
      ]
    }
  ],
  "meta": {
    "sport": "nba",
    "count": 1,
    "total": 1,
    "open_games_considered": 7
  }
}`,
  },
  {
    title: "Trained Sports",
    method: "GET",
    path: "/v1/predictions/trained-sports",
    fullUrl: `${API_BASE}/v1/predictions/trained-sports`,
    description: "Returns sports that currently have trained player-prop model bundles.",
    bestFor: "Feature flags, sport availability checks, and startup health checks.",
    parameters: [],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "sport": "nba",
      "display_name": "NBA",
      "size_bytes": 12499388,
      "modified_at": "2026-03-30T20:07:11Z"
    }
  ],
  "meta": {
    "count": 1,
    "scanned_at": "2026-03-31T00:03:20Z"
  }
}`,
  },
  {
    title: "Opportunities",
    method: "GET",
    path: "/v1/predictions/opportunities",
    fullUrl: `${API_BASE}/v1/predictions/opportunities?date=2026-03-31&sports=nba,wta&min_score=0.6&tier=high&prop_type=points_over_20&limit=50&offset=0`,
    description: "Cross-sport ranked player-prop opportunities generated from trained model bundles.",
    bestFor: "Player prop boards, filtered recommendation feeds, and value scanning tools.",
    parameters: [
      { name: "date", in: "query", required: false, example: "2026-03-31", notes: "Target date. Defaults to today." },
      { name: "sports", in: "query", required: false, example: "nba,wta", notes: "Comma-separated sport filter list." },
      { name: "prop_type", in: "query", required: false, example: "points_over_20", notes: "Filter to a specific prop market." },
      { name: "min_score", in: "query", required: false, example: "0.6", notes: "Minimum recommendation score (0..1)." },
      { name: "tier", in: "query", required: false, example: "high", notes: "Filter by recommendation tier: high/medium/low." },
      { name: "limit", in: "query", required: false, example: "50", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "sport": "nba",
      "game_id": "401710904",
      "recommendation_score": 0.82,
      "recommendation_tier": "high",
      "available_markets": [
        { "prop_type": "points_over_24", "line": 24, "market_type": "over_under" },
        { "prop_type": "assists_over_6", "line": 6, "market_type": "over_under" }
      ]
    }
  ],
  "meta": {
    "date": "2026-03-31",
    "count": 1,
    "total": 1,
    "trained_sports": ["nba", "wta"]
  }
}`,
  },
  {
    title: "Leaderboard",
    method: "GET",
    path: "/v1/predictions/leaderboard",
    fullUrl: `${API_BASE}/v1/predictions/leaderboard?date_start=2026-01-01&date_end=2026-03-31&min_evaluated=1`,
    description: "Historical model accuracy ranking by sport with optional evaluation-window filters.",
    bestFor: "Model monitoring pages, leaderboard blocks, and performance reporting.",
    parameters: [
      { name: "date_start", in: "query", required: false, example: "2026-01-01", notes: "Inclusive start date for evaluation window." },
      { name: "date_end", in: "query", required: false, example: "2026-03-31", notes: "Inclusive end date for evaluation window." },
      { name: "min_evaluated", in: "query", required: false, example: "1", notes: "Minimum evaluated predictions to include a sport." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "rank": 1,
      "sport": "nba",
      "accuracy": 0.641,
      "evaluated": 402,
      "correct": 258,
      "has_props_model": true
    }
  ],
  "meta": {
    "count": 1,
    "date_start": "2026-01-01",
    "date_end": "2026-03-31",
    "min_evaluated": 1
  }
}`,
  },
  {
    title: "Injuries",
    method: "GET",
    path: "/v1/{sport}/injuries",
    fullUrl: `${API_BASE}/v1/nba/injuries?limit=100&offset=0`,
    description: "Normalized active injury feed with player, team, and availability status mapping.",
    bestFor: "Availability tracking, alerts, slate context, and team status cards.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "limit", in: "query", required: false, example: "100", notes: "Page size." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "player_name": "Anthony Davis",
      "team": "Los Angeles Lakers",
      "status": "questionable",
      "injury": "knee soreness",
      "expected_return": "2025-04-02"
    }
  ],
  "meta": {
    "sport": "nba",
    "count": 1,
    "limit": 100,
    "offset": 0
  }
}`,
  },
  {
    title: "News",
    method: "GET",
    path: "/v1/{sport}/news",
    fullUrl: `${API_BASE}/v1/nba/news?limit=20&offset=0`,
    description: "Latest sport-specific news feed sorted by publication date.",
    bestFor: "News modules, context panels, and narrative overlays for model outputs.",
    parameters: [
      { name: "sport", in: "path", required: true, example: "nba", notes: "League key in the path." },
      { name: "limit", in: "query", required: false, example: "20", notes: "Number of stories to return." },
      { name: "offset", in: "query", required: false, example: "0", notes: "Pagination offset." },
    ],
    exampleResponse: `{
  "success": true,
  "data": [
    {
      "id": "41085432",
      "sport": "nba",
      "headline": "Cavaliers clinch top seed",
      "summary": "Cleveland secures home-court advantage.",
      "url": "https://www.espn.com/nba/story/_/id/41085432",
      "author": "Reporter",
      "published_at": "2026-03-31T21:15:00Z"
    }
  ],
  "meta": {
    "sport": "nba",
    "count": 1,
    "limit": 20,
    "offset": 0
  }
}`,
  },
];

const AI_WORKFLOW_STEPS = [
  "Provide the sellable schema URL to the agent before any prompt that asks it to call the API.",
  "Have the agent choose one of the canonical endpoints on this page and include required path parameters first, then optional query filters.",
  "Use the full request address in generated examples so clients do not guess hostnames or route names.",
  "Prefer tight filters and compact limits for faster responses and better AI application latency.",
];

export default async function ApiGuidePage() {
  const tier = await getViewerTier();

  if (!hasEnterpriseDocsAccess(tier)) {
    notFound();
  }

  return (
    <main className={styles.guidePage}>
      <section className={styles.hero}>
        <p className={styles.kicker}>Enterprise Access</p>
        <h1>Normalized Data Product Guide</h1>
        <p>
          This page is the customer-facing API surface. It uses canonical endpoints, shows exact request addresses,
          and includes representative normalized response examples.
        </p>
        <div className={styles.heroActions}>
          <Link href="/dashboard" className={styles.primaryCta}>
            Open Dashboard
          </Link>
          <a href={SELLABLE_OPENAPI_URL} className={styles.secondaryCta} target="_blank" rel="noopener noreferrer">
            Open Sellable OpenAPI JSON
          </a>
        </div>
        <div className={styles.heroMeta}>
          <div>
            <span>Base URL</span>
            <strong>{API_BASE}</strong>
          </div>
          <div>
            <span>Auth Header</span>
            <strong>X-API-Key: YOUR_KEY</strong>
          </div>
          <div>
            <span>Schema URL</span>
            <strong>{SELLABLE_OPENAPI_URL}</strong>
          </div>
        </div>
      </section>

      <section className={styles.aiSection}>
        <div className={styles.aiCopy}>
          <p className={styles.sectionEyebrow}>AI Integration</p>
          <h2>Use one sellable schema for faster AI applications</h2>
          <p>
            AI tools perform best when they only see the customer-safe schema. Start them with <strong>{SELLABLE_OPENAPI_URL}</strong>
            so they generate requests against the sellable surface.
          </p>
          <ul>
            {AI_WORKFLOW_STEPS.map((step) => (
              <li key={step}>{step}</li>
            ))}
          </ul>
        </div>
        <div className={styles.aiCard}>
          <h3>Recommended Prompt Pattern</h3>
          <pre>{`Use ${SELLABLE_OPENAPI_URL} as the only API schema.
Choose from the canonical sellable endpoints on the API guide page.
Include every required path parameter, then add query filters.
Build final requests with the full URL and X-API-Key header.`}</pre>
        </div>
      </section>

      <section className={styles.catalogSection}>
        {GUIDE_ENDPOINTS.map((endpoint) => (
          <article className={styles.endpointSection} key={endpoint.title}>
            <header className={styles.endpointHeader}>
              <div>
                <p className={styles.sectionEyebrow}>{endpoint.title}</p>
                <h2>{endpoint.path}</h2>
              </div>
              <span className={styles.methodBadge}>{endpoint.method}</span>
            </header>

            <p className={styles.endpointDescription}>{endpoint.description}</p>
            <p className={styles.endpointBestFor}><strong>Best for:</strong> {endpoint.bestFor}</p>

            <div className={styles.endpointGrid}>
              <div>
                <div className={styles.requestBlock}>
                  <span>Full request address</span>
                  <code>{endpoint.fullUrl}</code>
                </div>

                <div className={styles.parameterTable}>
                  <div className={styles.parameterHead}>Parameter</div>
                  <div className={styles.parameterHead}>In</div>
                  <div className={styles.parameterHead}>Required</div>
                  <div className={styles.parameterHead}>Example</div>
                  <div className={styles.parameterHead}>How to use it</div>
                  {endpoint.parameters.map((parameter) => (
                    <div className={styles.parameterRow} key={`${endpoint.title}-${parameter.name}`}>
                      <div className={styles.parameterCell}>{parameter.name}</div>
                      <div className={styles.parameterCell}>{parameter.in}</div>
                      <div className={styles.parameterCell}>{parameter.required ? "yes" : "no"}</div>
                      <div className={styles.parameterCell}><code>{parameter.example}</code></div>
                      <div className={styles.parameterCell}>{parameter.notes}</div>
                    </div>
                  ))}
                </div>
              </div>

              <div className={styles.exampleBlock}>
                <div className={styles.exampleHeader}>
                  <h3>Example Response</h3>
                  <span>Representative normalized payload</span>
                </div>
                <pre>{endpoint.exampleResponse}</pre>
              </div>
            </div>
          </article>
        ))}
      </section>
    </main>
  );
}
