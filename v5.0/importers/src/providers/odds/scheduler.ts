// ──────────────────────────────────────────────────────────
// V5.0 Odds Collection System — Scheduler
// ──────────────────────────────────────────────────────────
// Handles opening/closing/snapshot collection strategy and
// ESPN odds extraction.

import fs from "node:fs";
import path from "node:path";
import type { Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, readJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import {
  ODDS_CONFIG,
  ODDSAPI_SPORT_KEYS,
  type OddsRecord,
  type OddsCollectionType,
} from "./config.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "odds";
const ODDSAPI_BASE = "https://api.the-odds-api.com";

// Conservative: 1 request per hour to preserve API quota
const ODDSAPI_RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 3_600_000 };

// ── OddsAPI response types ──────────────────────────────────

interface OddsOutcome {
  name: string;
  price: number;
  point?: number;
}

interface OddsMarket {
  key: string;
  outcomes: OddsOutcome[];
}

interface OddsBookmaker {
  key: string;
  title: string;
  markets: OddsMarket[];
}

interface OddsEvent {
  id: string;
  sport_key: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers: OddsBookmaker[];
}

// ── ESPN odds file shape ────────────────────────────────────

interface EspnOddsProvider {
  name: string;
}

interface EspnTeamOdds {
  moneyLine?: number;
  spreadOdds?: number;
}

interface EspnOddsItem {
  provider: EspnOddsProvider;
  homeTeamOdds: EspnTeamOdds;
  awayTeamOdds: EspnTeamOdds;
  spread?: number;
  overUnder?: number;
  overOdds?: number;
  underOdds?: number;
}

interface EspnOddsFile {
  eventId?: string;
  odds?: EspnOddsItem[];
}

interface EspnPropPage {
  count?: number;
  pageIndex?: number;
  pageSize?: number;
  pageCount?: number;
  items?: any[];
}

interface EspnPlayerPropRecord {
  game_id: string;
  player_id: string;
  player_name: string | null;
  team_id: string | null;
  market: string;
  line: number;
  over_price: number | null;
  under_price: number | null;
  bookmaker: string;
  timestamp: string;
}

// ── Helpers ─────────────────────────────────────────────────

function todayISO(): string {
  return new Date().toISOString().slice(0, 10);
}

function normalizeBookmaker(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/\s*-\s*live\s*odds$/i, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseRefId(ref: string | undefined | null, segment: string): string | null {
  if (!ref) return null;
  const match = ref.match(new RegExp(`/${segment}/([^/?]+)`));
  return match?.[1] ?? null;
}

function toFloat(value: unknown): number | null {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const parsed = parseFloat(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function toInt(value: unknown): number | null {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? Math.trunc(value) : null;
  const parsed = parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeMarket(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseBookmakerOdds(
  event: OddsEvent,
  bookmaker: OddsBookmaker,
  sport: Sport,
  date: string,
  type: OddsCollectionType,
): OddsRecord {
  const record: OddsRecord = {
    game_id: event.id,
    sport,
    date,
    timestamp: new Date().toISOString(),
    type,
    sportsbook: bookmaker.key,
    home_team: event.home_team,
    away_team: event.away_team,
    home_spread: null,
    away_spread: null,
    home_spread_odds: null,
    away_spread_odds: null,
    home_ml: null,
    away_ml: null,
    total: null,
    over_odds: null,
    under_odds: null,
  };

  for (const market of bookmaker.markets) {
    if (market.key === "h2h") {
      for (const o of market.outcomes) {
        if (o.name === event.home_team) record.home_ml = o.price;
        else if (o.name === event.away_team) record.away_ml = o.price;
      }
    } else if (market.key === "spreads") {
      for (const o of market.outcomes) {
        if (o.name === event.home_team) {
          record.home_spread = o.point ?? null;
          record.home_spread_odds = o.price;
        } else if (o.name === event.away_team) {
          record.away_spread = o.point ?? null;
          record.away_spread_odds = o.price;
        }
      }
    } else if (market.key === "totals") {
      for (const o of market.outcomes) {
        if (o.name === "Over") {
          record.total = o.point ?? null;
          record.over_odds = o.price;
        } else if (o.name === "Under") {
          record.under_odds = o.price;
        }
      }
    }
  }

  return record;
}

// ── Scheduler class ─────────────────────────────────────────

export class OddsScheduler {
  private readonly dataDir: string;
  private readonly fetched = new Set<string>();

  constructor(dataDir: string) {
    this.dataDir = dataDir;
  }

  // ── OddsAPI fetch ───────────────────────────────────────

  private async fetchOddsAPI(
    sport: Sport,
    date: string,
    type: OddsCollectionType,
  ): Promise<OddsRecord[]> {
    const cacheKey = `${sport}_${date}`;

    // Avoid duplicate API calls in the same session for opening/snapshot
    if (type !== "closing" && this.fetched.has(cacheKey)) {
      logger.debug(`Already fetched ${cacheKey} this session — skipping`, NAME);
      return [];
    }

    const sportKey = ODDSAPI_SPORT_KEYS[sport];
    if (!sportKey) {
      logger.warn(`No OddsAPI sport key for ${sport}`, NAME);
      return [];
    }

    if (!ODDS_CONFIG.oddsapi_key) {
      logger.warn("ODDSAPI_KEY not set — skipping OddsAPI fetch", NAME);
      return [];
    }

    const url =
      `${ODDSAPI_BASE}/v4/sports/${sportKey}/odds` +
      `?regions=us&markets=h2h,spreads,totals&oddsFormat=american` +
      `&apiKey=${ODDS_CONFIG.oddsapi_key}`;

    logger.info(`Fetching ${type} odds for ${sport} from OddsAPI`, NAME);

    const events = await fetchJSON<OddsEvent[]>(url, NAME, ODDSAPI_RATE_LIMIT);

    if (!events || events.length === 0) {
      logger.info(`No odds events returned for ${sport}`, NAME);
      return [];
    }

    if (type !== "closing") {
      this.fetched.add(cacheKey);
    }

    const enabledBooks = new Set(ODDS_CONFIG.enabled_sportsbooks);
    const records: OddsRecord[] = [];

    for (const event of events) {
      for (const bookmaker of event.bookmakers) {
        if (!enabledBooks.has(bookmaker.key)) continue;
        records.push(parseBookmakerOdds(event, bookmaker, sport, date, type));
      }
    }

    logger.info(
      `Parsed ${records.length} odds records from ${events.length} events`,
      NAME,
    );

    return records;
  }

  // ── Opening collection ──────────────────────────────────

  async collectOpening(sport: Sport, date: string): Promise<number> {
    const records = await this.fetchOddsAPI(sport, date, "opening");
    if (records.length === 0) return 0;

    const outPath = path.join(
      this.dataDir, "raw", "odds", sport, date, "opening.json",
    );

    writeJSON(outPath, {
      sport,
      date,
      type: "opening" as const,
      recordCount: records.length,
      records,
      fetchedAt: new Date().toISOString(),
    });

    logger.progress(NAME, sport, "opening", `Saved ${records.length} opening odds records`);
    return 1;
  }

  // ── Closing collection ──────────────────────────────────

  async collectClosing(
    sport: Sport,
    gameId: string,
    gameStartTime: Date,
  ): Promise<number> {
    const now = new Date();
    const minutesUntilStart =
      (gameStartTime.getTime() - now.getTime()) / 60_000;

    if (minutesUntilStart > ODDS_CONFIG.closing_minutes_before) {
      logger.debug(
        `Game ${gameId} starts in ${minutesUntilStart.toFixed(0)}min — too early for closing odds`,
        NAME,
      );
      return 0;
    }

    const date = gameStartTime.toISOString().slice(0, 10);
    const records = await this.fetchOddsAPI(sport, date, "closing");

    // Filter to the specific game
    const gameRecords = records.filter((r) => r.game_id === gameId);
    if (gameRecords.length === 0) return 0;

    const outPath = path.join(
      this.dataDir, "raw", "odds", sport, date, `closing_${gameId}.json`,
    );

    writeJSON(outPath, {
      sport,
      date,
      type: "closing" as const,
      gameId,
      gameStartTime: gameStartTime.toISOString(),
      recordCount: gameRecords.length,
      records: gameRecords,
      fetchedAt: new Date().toISOString(),
    });

    logger.progress(NAME, sport, "closing", `Saved ${gameRecords.length} closing odds for ${gameId}`);
    return 1;
  }

  // ── Snapshot collection ─────────────────────────────────

  async collectSnapshot(
    sport: Sport,
    date: string,
    hour: number,
  ): Promise<number> {
    const records = await this.fetchOddsAPI(sport, date, "snapshot");
    if (records.length === 0) return 0;

    const outPath = path.join(
      this.dataDir, "raw", "odds", sport, date, "snapshots", `${hour}.json`,
    );

    writeJSON(outPath, {
      sport,
      date,
      type: "snapshot" as const,
      hour,
      recordCount: records.length,
      records,
      fetchedAt: new Date().toISOString(),
    });

    logger.progress(NAME, sport, "snapshot", `Saved hour-${hour} snapshot (${records.length} records)`);
    return 1;
  }

  // ── ESPN odds extraction ────────────────────────────────

  async extractEspnOdds(
    sport: Sport,
    season: number,
    dataDir: string,
  ): Promise<number> {
    const espnOddsDir = path.join(
      dataDir, "raw", "espn", sport, String(season), "odds",
    );
    const espnGamesDir = path.join(
      dataDir, "raw", "espn", sport, String(season), "games",
    );

    // Build event_id → { home_team, away_team } lookup from ESPN game files
    const teamLookup = new Map<string, { home: string; away: string }>();
    if (fs.existsSync(espnGamesDir)) {
      for (const gf of fs.readdirSync(espnGamesDir).filter((f) => f.endsWith(".json"))) {
        const gd = readJSON<any>(path.join(espnGamesDir, gf));
        const eid = String(gd?.eventId ?? gf.replace(".json", ""));
        const competitors: any[] =
          gd?.summary?.header?.competitions?.[0]?.competitors ?? [];
        let home = "", away = "";
        for (const c of competitors) {
          const name: string = c?.team?.displayName ?? "";
          if (c?.homeAway === "home") home = name;
          else if (c?.homeAway === "away") away = name;
        }
        if (home || away) teamLookup.set(eid, { home, away });
      }
    }

    if (!fs.existsSync(espnOddsDir)) {
      logger.debug(`No ESPN odds directory: ${espnOddsDir}`, NAME);
      return 0;
    }

    const files = fs.readdirSync(espnOddsDir).filter(
      (f) => f.endsWith(".json") && f !== "all_odds.json",
    );

    if (files.length === 0) {
      logger.debug(`No ESPN odds files in ${espnOddsDir}`, NAME);
      return 0;
    }

    logger.info(`Extracting ESPN odds from ${files.length} files`, NAME);

    const allRecords: OddsRecord[] = [];
    const dateSet = new Set<string>();

    for (const file of files) {
      const filePath = path.join(espnOddsDir, file);
      const data = readJSON<EspnOddsFile>(filePath);
      if (!data?.odds || data.odds.length === 0) continue;

      const eventId = data.eventId ?? file.replace(".json", "");

      for (const item of data.odds) {
        if (!item.provider?.name) continue;

        const record: OddsRecord = {
          game_id: eventId,
          sport,
          date: todayISO(),
          timestamp: new Date().toISOString(),
          type: "snapshot",
          sportsbook: normalizeBookmaker(item.provider.name),
          home_team: teamLookup.get(eventId)?.home ?? "",
          away_team: teamLookup.get(eventId)?.away ?? "",
          home_spread: item.spread ?? null,
          away_spread: item.spread != null ? -item.spread : null,
          home_spread_odds: item.homeTeamOdds?.spreadOdds ?? null,
          away_spread_odds: item.awayTeamOdds?.spreadOdds ?? null,
          home_ml: item.homeTeamOdds?.moneyLine ?? null,
          away_ml: item.awayTeamOdds?.moneyLine ?? null,
          total: item.overUnder ?? null,
          over_odds: item.overOdds ?? null,
          under_odds: item.underOdds ?? null,
        };

        allRecords.push(record);
        dateSet.add(record.date);
      }
    }

    if (allRecords.length === 0) {
      logger.info("No ESPN odds records extracted", NAME);
      return 0;
    }

    // Write one file per date
    let filesWritten = 0;
    for (const date of dateSet) {
      const dateRecords = allRecords.filter((r) => r.date === date);
      const outPath = path.join(
        dataDir, "raw", "odds", sport, date, "espn_baseline.json",
      );

      writeJSON(outPath, {
        sport,
        date,
        source: "espn",
        season,
        recordCount: dateRecords.length,
        records: dateRecords,
        extractedAt: new Date().toISOString(),
      });

      filesWritten++;
    }

    logger.progress(
      NAME, sport, "espn_extract",
      `Extracted ${allRecords.length} ESPN odds records into ${filesWritten} files`,
    );

    return filesWritten;
  }

  // ── ESPN player props extraction ─────────────────────────

  async extractEspnPlayerProps(
    sport: Sport,
    season: number,
    dataDir: string,
  ): Promise<number> {
    const espnOddsDir = path.join(
      dataDir, "raw", "espn", sport, String(season), "odds",
    );

    if (!fs.existsSync(espnOddsDir)) {
      logger.debug(`No ESPN odds directory: ${espnOddsDir}`, NAME);
      return 0;
    }

    const files = fs.readdirSync(espnOddsDir).filter(
      (f) => f.endsWith(".json") && f !== "all_odds.json",
    );
    if (files.length === 0) return 0;

    const propsByDate = new Map<string, EspnPlayerPropRecord[]>();
    let refsProcessed = 0;

    for (const file of files) {
      const filePath = path.join(espnOddsDir, file);
      const data = readJSON<EspnOddsFile>(filePath);
      if (!data?.odds?.length) continue;

      const eventId = data.eventId ?? file.replace(".json", "");
      const eventDate = todayISO();

      for (const oddItem of data.odds) {
        const providerName = oddItem.provider?.name || "unknown";
        const bookmaker = normalizeBookmaker(providerName);
        const propRef = (oddItem as any)?.propBets?.$ref as string | undefined;
        if (!propRef) continue;

        refsProcessed += 1;
        const baseUrl = propRef.replace("http://", "https://");

        const firstPage = await fetchJSON<EspnPropPage>(
          `${baseUrl}${baseUrl.includes("?") ? "&" : "?"}limit=1000`,
          NAME,
          { requests: 4, perMs: 1_000 },
          { retries: 1 },
        ).catch(() => null);

        if (!firstPage?.items?.length) continue;

        const allItems: any[] = [...firstPage.items];
        const pageCount = Math.max(1, firstPage.pageCount ?? 1);

        for (let page = 2; page <= pageCount; page++) {
          const pageUrl = `${baseUrl}${baseUrl.includes("?") ? "&" : "?"}limit=1000&page=${page}`;
          const pageData = await fetchJSON<EspnPropPage>(
            pageUrl,
            NAME,
            { requests: 4, perMs: 1_000 },
            { retries: 1 },
          ).catch(() => null);
          if (pageData?.items?.length) {
            allItems.push(...pageData.items);
          }
        }

        const out = propsByDate.get(eventDate) ?? [];

        for (const item of allItems) {
          const playerId = parseRefId(item?.athlete?.$ref, "athletes") ?? "unknown";
          const marketName = item?.type?.name ? String(item.type.name) : "unknown";
          const market = normalizeMarket(marketName);
          const line =
            toFloat(item?.current?.target?.value) ??
            toFloat(item?.open?.target?.value) ??
            toFloat(item?.odds?.total?.value);
          if (line == null) continue;

          const overPrice =
            toInt(item?.current?.over?.american) ??
            toInt(item?.odds?.american?.value);
          const underPrice = toInt(item?.current?.under?.american);

          out.push({
            game_id: eventId,
            player_id: playerId,
            player_name: null,
            team_id: null,
            market,
            line,
            over_price: overPrice,
            under_price: underPrice,
            bookmaker,
            timestamp: item?.lastUpdated ?? new Date().toISOString(),
          });
        }

        propsByDate.set(eventDate, out);
      }
    }

    let filesWritten = 0;
    for (const [date, records] of propsByDate) {
      if (!records.length) continue;

      const outPath = path.join(
        dataDir,
        "raw",
        "odds",
        sport,
        date,
        "player_props",
        "espn_player_props.json",
      );

      writeJSON(outPath, {
        sport,
        season,
        date,
        source: "espn",
        recordCount: records.length,
        records,
        extractedAt: new Date().toISOString(),
      });
      filesWritten += 1;
    }

    logger.progress(
      NAME,
      sport,
      "espn_props",
      `Extracted ${Array.from(propsByDate.values()).reduce((n, r) => n + r.length, 0)} player props from ${refsProcessed} prop feeds`,
    );

    return filesWritten;
  }
}
