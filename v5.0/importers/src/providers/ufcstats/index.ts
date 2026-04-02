// ──────────────────────────────────────────────────────────
// V5.0 UFC Stats Provider
// ──────────────────────────────────────────────────────────
// Scrapes fight data from ufcstats.com — events, fights, and
// round-by-round fighter statistics plus fighter profile pages.
// Uses node-html-parser for HTML parsing.
// No API key required.

import { parse as parseHTML } from "node-html-parser";
import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { writeJSON, readJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "ufcstats";
const EVENTS_URL = "http://ufcstats.com/statistics/events/completed?page=all";
const EVENT_DETAIL_URL = "http://ufcstats.com/event-details";
const FIGHT_DETAIL_URL = "http://ufcstats.com/fight-details";
const FIGHTER_DETAIL_URL = "http://ufcstats.com/fighter-details";
const FIGHTERS_LIST_URL = "http://ufcstats.com/statistics/fighters?char=";

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };

const SUPPORTED_SPORTS: Sport[] = ["ufc"];

const ALL_ENDPOINTS = [
  "events",
  "fights",
  "fighter_stats",
  "fighter_profiles",
] as const;

const DEFAULT_ENDPOINTS: Endpoint[] = [
  "events",
  "fights",
  "fighter_stats",
];

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Types ───────────────────────────────────────────────────

interface UFCEvent {
  id: string;
  name: string;
  date: string;
  location: string;
  url: string;
}

interface UFCFight {
  id: string;
  eventId: string;
  fighters: string[];
  fighterRefs?: UFCFighterRef[];
  result: string;
  method: string;
  round: string;
  time: string;
  weightClass: string;
  url: string;
}

interface UFCFighterRef {
  id: string;
  name: string;
  url: string;
}

interface UFCFightDetail {
  id: string;
  fighters: { id?: string; url?: string; name: string; nickname: string; result: string }[];
  method: string;
  round: string;
  time: string;
  weightClass: string;
  totals: Record<string, string>[];
  rounds: { round: number; fighters: Record<string, string>[] }[];
  significantStrikes: Record<string, string>[];
}

interface UFCFighterProfile {
  id: string;
  url: string;
  name: string;
  nickname: string;
  record: string;
  height: string;
  weight: string;
  reach: string;
  stance: string;
  dob: string;
  slpm: string;
  strAcc: string;
  sapm: string;
  strDef: string;
  tdAvg: string;
  tdAcc: string;
  tdDef: string;
  subAvg: string;
}

// ── Endpoint context ────────────────────────────────────────

interface EndpointContext {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

// ── HTML Parsing helpers ────────────────────────────────────

function cleanText(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

function parseEventDateISO(raw: string): string {
  const match = raw.match(/([A-Za-z]+\s+\d{1,2},\s+\d{4})/);
  if (!match) return "unknown-date";
  const dt = new Date(match[1]);
  if (Number.isNaN(dt.getTime())) return "unknown-date";
  return dt.toISOString().slice(0, 10);
}

function weekDirFromISO(dateISO: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateISO)) return "week_00";
  const date = new Date(`${dateISO}T00:00:00Z`);
  const dayNum = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((date.getTime() - yearStart.getTime()) / 86_400_000) + 1) / 7);
  return `week_${String(weekNo).padStart(2, "0")}`;
}

function structuredEventDir(ctx: EndpointContext, event: UFCEvent): string {
  const dateISO = parseEventDateISO(event.date);
  return rawPath(
    ctx.dataDir,
    NAME,
    ctx.sport,
    ctx.season,
    "season_types",
    "regular",
    "weeks",
    weekDirFromISO(dateISO),
    "dates",
    dateISO,
    "events",
    event.id,
  );
}

function structuredFightsPath(ctx: EndpointContext, event: UFCEvent): string {
  return `${structuredEventDir(ctx, event)}/fights.json`;
}

function structuredStatsPath(ctx: EndpointContext, event: UFCEvent, fightId: string): string {
  return `${structuredEventDir(ctx, event)}/fighter_stats/${fightId}.json`;
}

function eventsIndexPath(ctx: EndpointContext): string {
  return rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "reference", "events.json");
}

function fighterProfilesIndexPath(ctx: EndpointContext): string {
  return rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "reference", "fighter_profiles_index.json");
}

function fighterProfilePath(ctx: EndpointContext, fighterId: string): string {
  return rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "reference", "fighters", `${fighterId}.json`);
}

function legacyEventsPath(ctx: EndpointContext): string {
  return rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "events.json");
}

function legacyFightPath(ctx: EndpointContext, eventId: string): string {
  return rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "fights", `${eventId}.json`);
}

function legacyStatPath(ctx: EndpointContext, fightId: string): string {
  return rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "fighter_stats", `${fightId}.json`);
}

function writeIfMissing(filePath: string, payload: unknown): boolean {
  if (fileExists(filePath)) return false;
  writeJSON(filePath, payload);
  return true;
}

function parseEventsPage(html: string): UFCEvent[] {
  const root = parseHTML(html);
  const events: UFCEvent[] = [];

  const rows = root.querySelectorAll("tr.b-statistics__table-row");

  for (const row of rows) {
    const link = row.querySelector("a.b-link");
    if (!link) continue;

    const href = link.getAttribute("href") ?? "";
    const id = href.split("/").pop() ?? "";
    if (!id) continue;

    const name = cleanText(link.textContent);

    const cells = row.querySelectorAll("td");
    const dateText = cells[0] ? cleanText(cells[0].textContent) : "";
    const location = cells[1] ? cleanText(cells[1].textContent) : "";

    events.push({ id, name, date: dateText, location, url: href });
  }

  return events;
}

function parseEventDetailPage(html: string, eventId: string): UFCFight[] {
  const root = parseHTML(html);
  const fights: UFCFight[] = [];

  const rows = root.querySelectorAll("tr.b-fight-details__table-row");

  for (const row of rows) {
    // Skip header rows
    if (row.querySelector("th")) continue;

    const link = row.querySelector("a.b-flag");
    const href = link?.getAttribute("href") ?? row.getAttribute("data-link") ?? "";
    const id = href.split("/").pop() ?? "";
    if (!id) continue;

    const cells = row.querySelectorAll("td");
    if (cells.length < 10) continue;

    // Fighters are in the first column
    const fighterLinks = cells[1]?.querySelectorAll("a") ?? [];
    const fighterRefs: UFCFighterRef[] = fighterLinks
      .map((a) => {
        const fighterUrl = a.getAttribute("href") ?? "";
        const fighterId = fighterUrl.split("/").pop() ?? "";
        const name = cleanText(a.textContent);
        if (!fighterId || !name) return null;
        return { id: fighterId, name, url: fighterUrl };
      })
      .filter((v): v is UFCFighterRef => v !== null);
    const fighters = fighterRefs.map((f) => f.name);

    const result = cells[0] ? cleanText(cells[0].textContent) : "";
    const weightClass = cells[6] ? cleanText(cells[6].textContent) : "";
    const method = cells[7] ? cleanText(cells[7].textContent) : "";
    const round = cells[8] ? cleanText(cells[8].textContent) : "";
    const time = cells[9] ? cleanText(cells[9].textContent) : "";

    fights.push({
      id,
      eventId,
      fighters,
      fighterRefs,
      result,
      method,
      round,
      time,
      weightClass,
      url: href,
    });
  }

  return fights;
}

function parseFightDetailPage(html: string, fightId: string): UFCFightDetail {
  const root = parseHTML(html);

  // Fighter names and results
  const fighterNodes = root.querySelectorAll("div.b-fight-details__person");
  const fighters = fighterNodes.map((node) => {
    const nameEl = node.querySelector("a.b-fight-details__person-link, h3.b-fight-details__person-name a");
    const href = nameEl?.getAttribute("href") ?? "";
    const fighterId = href.split("/").pop() ?? "";
    const statusEl = node.querySelector("i.b-fight-details__person-status");
    const nicknameEl = node.querySelector("p.b-fight-details__person-title");
    return {
      id: fighterId || undefined,
      url: href || undefined,
      name: nameEl ? cleanText(nameEl.textContent) : "",
      nickname: nicknameEl ? cleanText(nicknameEl.textContent) : "",
      result: statusEl ? cleanText(statusEl.textContent) : "",
    };
  });

  // Method, round, time from the detail box
  const detailItems = root.querySelectorAll("i.b-fight-details__text-item, p.b-fight-details__text");
  let method = "";
  let round = "";
  let time = "";
  let weightClass = "";

  const titleEl = root.querySelector("i.b-fight-details__fight-title");
  if (titleEl) {
    weightClass = cleanText(titleEl.textContent).replace(/\s+Bout$/i, "").trim();
  }

  for (const item of detailItems) {
    const line = cleanText(item.textContent);
    if (!line) continue;
    if (!method && line.toLowerCase().startsWith("method:")) {
      method = line.replace(/^method:\s*/i, "").trim();
      continue;
    }
    if (!round && line.toLowerCase().startsWith("round:")) {
      round = line.replace(/^round:\s*/i, "").trim();
      continue;
    }
    if (!time && line.toLowerCase().startsWith("time:")) {
      time = line.replace(/^time:\s*/i, "").trim();
      continue;
    }
    if (!weightClass && line.toLowerCase().startsWith("weight class:")) {
      weightClass = line.replace(/^weight class:\s*/i, "").trim();
      continue;
    }
  }

  if (!method) {
    const methodEl = root.querySelector("i.b-fight-details__text-item_first i[style*='font-style: normal']");
    if (methodEl) method = cleanText(methodEl.textContent);
  }

  // Parse stat tables — totals and per-round
  const tables = root.querySelectorAll("table.b-fight-details__table");
  const totals: Record<string, string>[] = [];
  const rounds: { round: number; fighters: Record<string, string>[] }[] = [];
  const significantStrikes: Record<string, string>[] = [];

  for (const table of tables) {
    const headers = table.querySelectorAll("th").map((th) => cleanText(th.textContent));
    const bodyRows = table.querySelectorAll("tbody tr");

    for (const row of bodyRows) {
      const cells = row.querySelectorAll("td");
      const rowData: Record<string, string>[] = [];

      // Each cell may contain two values (one per fighter), separated by <br> or <p>
      for (let i = 0; i < cells.length; i++) {
        const paragraphs = cells[i]!.querySelectorAll("p");
        if (paragraphs.length >= 2) {
          // Two-fighter cell
          if (!rowData[0]) rowData[0] = {};
          if (!rowData[1]) rowData[1] = {};
          rowData[0][headers[i] ?? `col_${i}`] = cleanText(paragraphs[0]!.textContent);
          rowData[1][headers[i] ?? `col_${i}`] = cleanText(paragraphs[1]!.textContent);
        } else {
          if (!rowData[0]) rowData[0] = {};
          rowData[0][headers[i] ?? `col_${i}`] = cleanText(cells[i]!.textContent);
        }
      }

      // Determine if this is a totals row or a round row based on section context
      const section = table.closest("section");
      const sectionTitle = section
        ? cleanText(section.querySelector("p.b-fight-details__collapse-link_bot, a.b-fight-details__collapse-link_rnd")?.textContent ?? "")
        : "";

      if (sectionTitle.toLowerCase().includes("significant strikes")) {
        significantStrikes.push(...rowData);
      } else if (sectionTitle.toLowerCase().includes("round")) {
        // We'll consolidate rounds after parsing
        for (const rd of rowData) {
          if (rd) totals.push(rd);
        }
      } else {
        totals.push(...rowData);
      }
    }
  }

  // Parse per-round sections
  const roundSections = root.querySelectorAll("section.b-fight-details__section");
  let roundNum = 0;
  for (const section of roundSections) {
    const heading = section.querySelector("p.b-fight-details__collapse-link_rnd");
    if (!heading) continue;
    roundNum++;

    const roundTable = section.querySelector("table");
    if (!roundTable) continue;

    const headers = roundTable.querySelectorAll("th").map((th) => cleanText(th.textContent));
    const roundFighters: Record<string, string>[] = [];

    const bodyRows = roundTable.querySelectorAll("tbody tr");
    for (const row of bodyRows) {
      const cells = row.querySelectorAll("td");
      for (let i = 0; i < cells.length; i++) {
        const paragraphs = cells[i]!.querySelectorAll("p");
        if (paragraphs.length >= 2) {
          if (!roundFighters[0]) roundFighters[0] = {};
          if (!roundFighters[1]) roundFighters[1] = {};
          roundFighters[0][headers[i] ?? `col_${i}`] = cleanText(paragraphs[0]!.textContent);
          roundFighters[1][headers[i] ?? `col_${i}`] = cleanText(paragraphs[1]!.textContent);
        }
      }
    }

    if (roundFighters.length > 0) {
      rounds.push({ round: roundNum, fighters: roundFighters });
    }
  }

  return {
    id: fightId,
    fighters,
    method,
    round,
    time,
    weightClass,
    totals,
    rounds,
    significantStrikes,
  };
}

function parseFighterProfilePage(html: string, fighterId: string, fighterUrl: string, fallbackName: string): UFCFighterProfile {
  const root = parseHTML(html);

  const name = cleanText(root.querySelector(".b-content__title-highlight")?.textContent ?? fallbackName);
  const recordRaw = cleanText(root.querySelector(".b-content__title-record")?.textContent ?? "");
  const record = recordRaw.replace(/^Record:\s*/i, "").trim();
  const nickname = cleanText(root.querySelector("p.b-content__Nickname")?.textContent ?? "");

  const fields: Record<string, string> = {};
  const items = root.querySelectorAll("li.b-list__box-list-item");
  for (const item of items) {
    const labelRaw = cleanText(item.querySelector("i.b-list__box-item-title")?.textContent ?? "");
    const label = labelRaw.replace(/:\s*$/, "").toLowerCase();
    if (!label) continue;
    const value = cleanText(item.textContent).replace(labelRaw, "").trim();
    if (value) fields[label] = value;
  }

  return {
    id: fighterId,
    url: fighterUrl,
    name,
    nickname,
    record,
    height: fields["height"] ?? "",
    weight: fields["weight"] ?? "",
    reach: fields["reach"] ?? "",
    stance: fields["stance"] ?? "",
    dob: fields["dob"] ?? "",
    slpm: fields["slpm"] ?? "",
    strAcc: fields["str. acc."] ?? "",
    sapm: fields["sapm"] ?? "",
    strDef: fields["str. def"] ?? "",
    tdAvg: fields["td avg."] ?? "",
    tdAcc: fields["td acc."] ?? "",
    tdDef: fields["td def."] ?? "",
    subAvg: fields["sub. avg."] ?? "",
  };
}

function parseFighterDirectoryPage(html: string): UFCFighterRef[] {
  const root = parseHTML(html);
  const rows = root.querySelectorAll("tr.b-statistics__table-row");
  const fighters: UFCFighterRef[] = [];

  for (const row of rows) {
    const link = row.querySelector("a.b-link.b-link_style_black") ?? row.querySelector("a.b-link");
    if (!link) continue;
    const href = link.getAttribute("href") ?? "";
    const id = href.split("/").pop() ?? "";
    if (!id) continue;

    const cells = row.querySelectorAll("td");
    const firstName = cells[0] ? cleanText(cells[0].textContent) : "";
    const lastName = cells[1] ? cleanText(cells[1].textContent) : "";
    const name = cleanText(`${firstName} ${lastName}`);
    if (!name) continue;
    fighters.push({ id, name, url: href });
  }

  return fighters;
}

/** Extract the year from a UFC event date string like "March 14, 2023" */
function extractYear(dateStr: string): number | null {
  const match = dateStr.match(/\b(20\d{2}|19\d{2})\b/);
  return match ? parseInt(match[1], 10) : null;
}

// ── Endpoint implementations ────────────────────────────────

async function importEvents(ctx: EndpointContext): Promise<{ result: EndpointResult; events: UFCEvent[] }> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const refEventsPath = eventsIndexPath(ctx);
  const oldEventsPath = legacyEventsPath(ctx);

  if (fileExists(refEventsPath)) {
    logger.progress(NAME, sport, "events", `Skipping ${season} — already exists`);
    const cached = readJSON<UFCEvent[]>(refEventsPath);
    if (cached && Array.isArray(cached)) {
      for (const ev of cached) {
        if (writeIfMissing(`${structuredEventDir(ctx, ev)}/event.json`, ev)) filesWritten++;
      }
      return { result: { filesWritten, errors }, events: cached };
    }
  }

  if (fileExists(oldEventsPath)) {
    const cachedLegacy = readJSON<UFCEvent[]>(oldEventsPath);
    if (cachedLegacy && Array.isArray(cachedLegacy)) {
      if (writeIfMissing(refEventsPath, cachedLegacy)) filesWritten++;
      for (const ev of cachedLegacy) {
        if (writeIfMissing(`${structuredEventDir(ctx, ev)}/event.json`, ev)) filesWritten++;
      }
      logger.progress(NAME, sport, "events", `Loaded ${cachedLegacy.length} cached events from legacy layout`);
      return { result: { filesWritten, errors }, events: cachedLegacy };
    }
  }

  logger.progress(NAME, sport, "events", `Fetching all completed events`);

  if (dryRun) return { result: { filesWritten: 0, errors: [] }, events: [] };

  try {
    const html = await fetchText(EVENTS_URL, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
    const allEvents = parseEventsPage(html);

    // Filter to the requested season year
    const seasonEvents = allEvents.filter((e) => extractYear(e.date) === season);

    logger.progress(NAME, sport, "events", `Found ${seasonEvents.length} events for ${season} (${allEvents.length} total)`);

    writeJSON(refEventsPath, seasonEvents);
    filesWritten++;
    if (writeIfMissing(oldEventsPath, seasonEvents)) filesWritten++;
    for (const ev of seasonEvents) {
      if (writeIfMissing(`${structuredEventDir(ctx, ev)}/event.json`, ev)) filesWritten++;
    }

    return { result: { filesWritten, errors }, events: seasonEvents };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`events ${season}: ${msg}`, NAME);
    errors.push(`events/${season}: ${msg}`);
    return { result: { filesWritten, errors }, events: [] };
  }
}

async function importFights(ctx: EndpointContext, events: UFCEvent[]): Promise<EndpointResult> {
  const { sport, season, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (events.length === 0) return { filesWritten, errors };

  logger.progress(NAME, sport, "fights", `Fetching fights for ${events.length} events`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  for (const event of events) {
    const legacyOutFile = legacyFightPath(ctx, event.id);
    const structuredOutFile = structuredFightsPath(ctx, event);

    if (fileExists(structuredOutFile)) continue;

    if (fileExists(legacyOutFile)) {
      const cachedLegacy = readJSON<{ event: UFCEvent; fights: UFCFight[] }>(legacyOutFile);
      if (cachedLegacy) {
        if (writeIfMissing(`${structuredEventDir(ctx, event)}/event.json`, cachedLegacy.event ?? event)) filesWritten++;
        if (writeIfMissing(structuredOutFile, cachedLegacy)) filesWritten++;
        continue;
      }
    }

    try {
      const url = `${EVENT_DETAIL_URL}/${event.id}`;
      const html = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
      const fights = parseEventDetailPage(html, event.id);

      const payload = { event, fights };
      writeJSON(structuredOutFile, payload);
      filesWritten++;
      if (writeIfMissing(legacyOutFile, payload)) filesWritten++;
      if (writeIfMissing(`${structuredEventDir(ctx, event)}/event.json`, event)) filesWritten++;
      logger.progress(NAME, sport, "fights", `${event.name}: ${fights.length} fights`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(`fights event ${event.id} (${event.name}): ${msg}`, NAME);
      errors.push(`fights/${season}/${event.id}: ${msg}`);
    }
  }

  return { filesWritten, errors };
}

async function importFighterStats(ctx: EndpointContext, events: UFCEvent[]): Promise<EndpointResult> {
  const { sport, season, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (events.length === 0) return { filesWritten, errors };

  if (dryRun) return { filesWritten: 0, errors: [] };

  // Collect all fight IDs from saved fight files
  const allFightIds: { fightId: string; event: UFCEvent }[] = [];

  for (const event of events) {
    const structuredFights = readJSON<{ fights: UFCFight[] }>(structuredFightsPath(ctx, event));
    const legacyFights = readJSON<{ fights: UFCFight[] }>(legacyFightPath(ctx, event.id));
    const data = structuredFights ?? legacyFights;
    if (!data || !Array.isArray(data.fights)) continue;
    for (const fight of data.fights) {
      allFightIds.push({ fightId: fight.id, event });
    }
  }

  logger.progress(NAME, sport, "fighter_stats", `Fetching details for ${allFightIds.length} fights`);

  for (const { fightId, event } of allFightIds) {
    const outFile = structuredStatsPath(ctx, event, fightId);
    const legacyOutFile = legacyStatPath(ctx, fightId);

    if (fileExists(outFile)) continue;

    if (fileExists(legacyOutFile)) {
      const cachedLegacy = readJSON<Record<string, unknown>>(legacyOutFile);
      if (cachedLegacy) {
        if (writeIfMissing(outFile, cachedLegacy)) filesWritten++;
        continue;
      }
    }


    try {
      const url = `${FIGHT_DETAIL_URL}/${fightId}`;
      const html = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
      const detail = parseFightDetailPage(html, fightId);

      const payload = { eventId: event.id, ...detail };
      writeJSON(outFile, payload);
      filesWritten++;
      if (writeIfMissing(legacyOutFile, payload)) filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(`fighter_stats fight ${fightId}: ${msg}`, NAME);
      errors.push(`fighter_stats/${season}/${event.id}/${fightId}: ${msg}`);
    }
  }

  return { filesWritten, errors };
}

async function fetchFighterDirectory(): Promise<Map<string, UFCFighterRef>> {
  const map = new Map<string, UFCFighterRef>();
  const alphabet = "abcdefghijklmnopqrstuvwxyz";

  for (const char of alphabet) {
    const url = `${FIGHTERS_LIST_URL}${char}&page=all`;
    try {
      const html = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
      const fighters = parseFighterDirectoryPage(html);
      for (const fighter of fighters) {
        map.set(fighter.name.toLowerCase(), fighter);
      }
    } catch (err) {
      logger.warn(`fighter directory ${char}: ${err instanceof Error ? err.message : String(err)}`, NAME);
    }
  }

  return map;
}

async function importFighterProfiles(ctx: EndpointContext, events: UFCEvent[]): Promise<EndpointResult> {
  const { dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (events.length === 0) return { filesWritten, errors };
  if (dryRun) return { filesWritten: 0, errors: [] };

  const refsByName = new Map<string, UFCFighterRef>();
  const unresolvedNames = new Set<string>();

  for (const event of events) {
    const fightBundle = readJSON<{ fights: UFCFight[] }>(structuredFightsPath(ctx, event))
      ?? readJSON<{ fights: UFCFight[] }>(legacyFightPath(ctx, event.id));
    if (fightBundle?.fights) {
      for (const fight of fightBundle.fights) {
        for (const ref of fight.fighterRefs ?? []) {
          refsByName.set(ref.name.toLowerCase(), ref);
        }
        for (const fighterName of fight.fighters ?? []) {
          const key = fighterName.trim().toLowerCase();
          if (key.length > 0 && !refsByName.has(key)) unresolvedNames.add(key);
        }
      }
    }

    const fightIds = (fightBundle?.fights ?? []).map((f) => f.id);
    for (const fightId of fightIds) {
      const statData = readJSON<{ fighters?: Array<{ id?: string; url?: string; name?: string }> }>(structuredStatsPath(ctx, event, fightId))
        ?? readJSON<{ fighters?: Array<{ id?: string; url?: string; name?: string }> }>(legacyStatPath(ctx, fightId));
      for (const fighter of statData?.fighters ?? []) {
        const name = (fighter.name ?? "").trim();
        const fighterId = (fighter.id ?? "").trim();
        if (!name) continue;
        if (!fighterId) {
          unresolvedNames.add(name.toLowerCase());
          continue;
        }
        const key = name.toLowerCase();
        refsByName.set(key, {
          id: fighterId,
          name,
          url: fighter.url && fighter.url.length > 0 ? fighter.url : `${FIGHTER_DETAIL_URL}/${fighterId}`,
        });
        unresolvedNames.delete(key);
      }
    }
  }

  if (unresolvedNames.size > 0) {
    const directory = await fetchFighterDirectory();
    for (const name of unresolvedNames) {
      const ref = directory.get(name);
      if (ref && !refsByName.has(name)) {
        refsByName.set(name, ref);
      }
    }
  }

  const fighters = [...refsByName.values()];
  logger.progress(NAME, ctx.sport, "fighter_profiles", `Fetching profiles for ${fighters.length} fighters`);

  for (const fighter of fighters) {
    const outFile = fighterProfilePath(ctx, fighter.id);
    if (fileExists(outFile)) continue;

    try {
      const url = fighter.url && fighter.url.length > 0
        ? fighter.url
        : `${FIGHTER_DETAIL_URL}/${fighter.id}`;
      const html = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
      const profile = parseFighterProfilePage(html, fighter.id, url, fighter.name);
      writeJSON(outFile, profile);
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`fighter_profiles/${ctx.season}/${fighter.id}: ${msg}`);
    }
  }

  const indexOut = fighterProfilesIndexPath(ctx);
  if (!fileExists(indexOut)) {
    writeJSON(indexOut, {
      season: ctx.season,
      count: fighters.length,
      fighters: fighters.map((f) => ({ id: f.id, name: f.name, url: f.url })),
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
  }

  return { filesWritten, errors };
}

// ── Provider implementation ─────────────────────────────────

const ufcstats: Provider = {
  name: NAME,
  label: "UFC Stats",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: [...ALL_ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const sports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...DEFAULT_ENDPOINTS];

    logger.info(
      `Starting import — ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── UFC ${season} ──`, NAME);

        // Step 1: Always fetch events first (fights/fighter_stats depend on them)
        let events: UFCEvent[] = [];
        if (endpoints.includes("events") || endpoints.includes("fights") || endpoints.includes("fighter_stats") || endpoints.includes("fighter_profiles")) {
          const ctx: EndpointContext = {
            sport,
            season,
            dataDir: opts.dataDir,
            dryRun: opts.dryRun,
          };
          const { result, events: fetchedEvents } = await importEvents(ctx);
          totalFiles += result.filesWritten;
          allErrors.push(...result.errors);
          events = fetchedEvents;
        }

        // Step 2: Fetch fight lists per event
        if (endpoints.includes("fights") || endpoints.includes("fighter_stats")) {
          try {
            const ctx: EndpointContext = {
              sport,
              season,
              dataDir: opts.dataDir,
              dryRun: opts.dryRun,
            };
            const result = await importFights(ctx, events);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${season}/fights: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}/fights: ${msg}`);
          }
        }

        // Step 3: Fetch per-fight round-by-round stats
        if (endpoints.includes("fighter_stats") || endpoints.includes("fighter_profiles")) {
          try {
            const ctx: EndpointContext = {
              sport,
              season,
              dataDir: opts.dataDir,
              dryRun: opts.dryRun,
            };
            const result = await importFighterStats(ctx, events);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${season}/fighter_stats: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}/fighter_stats: ${msg}`);
          }
        }

        // Step 4: Fetch fighter profile pages (equivalent coverage to shanktt/ufcstats)
        if (endpoints.includes("fighter_profiles")) {
          try {
            const ctx: EndpointContext = {
              sport,
              season,
              dataDir: opts.dataDir,
              dryRun: opts.dryRun,
            };
            const result = await importFighterProfiles(ctx, events);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${season}/fighter_profiles: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}/fighter_profiles: ${msg}`);
          }
        }
      }
    }

    const durationMs = Date.now() - start;
    logger.summary(NAME, totalFiles, allErrors.length, durationMs);

    return {
      provider: NAME,
      sport: "ufc",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs,
    };
  },
};

export default ufcstats;
