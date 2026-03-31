// ──────────────────────────────────────────────────────────
// V5.0 UFC Stats Provider
// ──────────────────────────────────────────────────────────
// Scrapes fight data from ufcstats.com — events, fights, and
// round-by-round fighter statistics.
// Uses node-html-parser for HTML parsing.
// No API key required.

import { parse as parseHTML } from "node-html-parser";
import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "ufcstats";
const EVENTS_URL = "http://ufcstats.com/statistics/events/completed?page=all";
const EVENT_DETAIL_URL = "http://ufcstats.com/event-details";
const FIGHT_DETAIL_URL = "http://ufcstats.com/fight-details";

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };

const SUPPORTED_SPORTS: Sport[] = ["ufc"];

const ALL_ENDPOINTS = [
  "events",
  "fights",
  "fighter_stats",
] as const;

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
  result: string;
  method: string;
  round: string;
  time: string;
  weightClass: string;
  url: string;
}

interface UFCFightDetail {
  id: string;
  fighters: { name: string; nickname: string; result: string }[];
  method: string;
  round: string;
  time: string;
  weightClass: string;
  totals: Record<string, string>[];
  rounds: { round: number; fighters: Record<string, string>[] }[];
  significantStrikes: Record<string, string>[];
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
    if (cells.length < 8) continue;

    // Fighters are in the first column
    const fighterLinks = cells[1]?.querySelectorAll("a") ?? [];
    const fighters = fighterLinks.map((a) => cleanText(a.textContent));

    const result = cells[0] ? cleanText(cells[0].textContent) : "";
    const weightClass = cells[6] ? cleanText(cells[6].textContent) : "";
    const method = cells[7] ? cleanText(cells[7].textContent) : "";
    const round = cells[8] ? cleanText(cells[8].textContent) : "";
    const time = cells[9] ? cleanText(cells[9].textContent) : "";

    fights.push({
      id,
      eventId,
      fighters,
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
    const statusEl = node.querySelector("i.b-fight-details__person-status");
    const nicknameEl = node.querySelector("p.b-fight-details__person-title");
    return {
      name: nameEl ? cleanText(nameEl.textContent) : "",
      nickname: nicknameEl ? cleanText(nicknameEl.textContent) : "",
      result: statusEl ? cleanText(statusEl.textContent) : "",
    };
  });

  // Method, round, time from the detail box
  const detailItems = root.querySelectorAll("i.b-fight-details__text-item");
  let method = "";
  let round = "";
  let time = "";
  let weightClass = "";

  for (const item of detailItems) {
    const label = item.querySelector("i.b-fight-details__label");
    if (!label) continue;
    const labelText = cleanText(label.textContent).replace(":", "");
    const value = cleanText(item.textContent).replace(cleanText(label.textContent), "").trim();

    switch (labelText.toLowerCase()) {
      case "method":
        method = value;
        break;
      case "round":
        round = value;
        break;
      case "time":
        time = value;
        break;
      case "weight class":
        weightClass = value;
        break;
    }
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

  const outFile = rawPath(dataDir, NAME, sport, season, "events.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "events", `Skipping ${season} — already exists`);
    try {
      const fs = await import("node:fs");
      const cached = JSON.parse(fs.readFileSync(outFile, "utf-8")) as UFCEvent[];
      return { result: { filesWritten, errors }, events: cached };
    } catch {
      // Fall through to re-fetch
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

    writeJSON(outFile, seasonEvents);
    filesWritten++;

    return { result: { filesWritten, errors }, events: seasonEvents };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`events ${season}: ${msg}`, NAME);
    errors.push(`events/${season}: ${msg}`);
    return { result: { filesWritten, errors }, events: [] };
  }
}

async function importFights(ctx: EndpointContext, events: UFCEvent[]): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (events.length === 0) return { filesWritten, errors };

  logger.progress(NAME, sport, "fights", `Fetching fights for ${events.length} events`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  for (const event of events) {
    const outFile = rawPath(dataDir, NAME, sport, season, "fights", `${event.id}.json`);

    if (fileExists(outFile)) continue;

    try {
      const url = `${EVENT_DETAIL_URL}/${event.id}`;
      const html = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
      const fights = parseEventDetailPage(html, event.id);

      writeJSON(outFile, { event, fights });
      filesWritten++;
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
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (events.length === 0) return { filesWritten, errors };

  if (dryRun) return { filesWritten: 0, errors: [] };

  // Collect all fight IDs from saved fight files
  const fs = await import("node:fs");
  const allFightIds: { fightId: string; eventId: string }[] = [];

  for (const event of events) {
    const fightsFile = rawPath(dataDir, NAME, sport, season, "fights", `${event.id}.json`);
    try {
      if (!fs.existsSync(fightsFile)) continue;
      const data = JSON.parse(fs.readFileSync(fightsFile, "utf-8")) as { fights: UFCFight[] };
      for (const fight of data.fights) {
        allFightIds.push({ fightId: fight.id, eventId: event.id });
      }
    } catch {
      continue;
    }
  }

  logger.progress(NAME, sport, "fighter_stats", `Fetching details for ${allFightIds.length} fights`);

  for (const { fightId, eventId } of allFightIds) {
    const outFile = rawPath(dataDir, NAME, sport, season, "fighter_stats", `${fightId}.json`);

    if (fileExists(outFile)) continue;

    try {
      const url = `${FIGHT_DETAIL_URL}/${fightId}`;
      const html = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
      const detail = parseFightDetailPage(html, fightId);

      writeJSON(outFile, { eventId, ...detail });
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(`fighter_stats fight ${fightId}: ${msg}`, NAME);
      errors.push(`fighter_stats/${season}/${eventId}/${fightId}: ${msg}`);
    }
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
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting import — ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── UFC ${season} ──`, NAME);

        // Step 1: Always fetch events first (fights/fighter_stats depend on them)
        let events: UFCEvent[] = [];
        if (endpoints.includes("events") || endpoints.includes("fights") || endpoints.includes("fighter_stats")) {
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
        if (endpoints.includes("fighter_stats")) {
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
