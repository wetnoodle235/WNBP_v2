// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Cross-Provider ID Mapper
// ──────────────────────────────────────────────────────────
// Maps provider-specific IDs (ESPN, OddsAPI, etc.) to a
// canonical internal ID so different data sources can be
// joined on the same entity.
// Ported from v4.0/util/mapping/id-mapper.ts, adapted for v5.0.

import fs from "node:fs";
import path from "node:path";

import type { Sport } from "./types.js";

export type EntityType = "game" | "team" | "player";

export interface ProviderIdEntry {
  provider: string;
  externalId: string | number;
  canonicalId: string;
}

interface MappingFile {
  sport: string;
  type: EntityType;
  maps: ProviderIdEntry[];
}

/**
 * Persistent ID mapping store backed by JSON files.
 *
 * Directory layout:
 *   `{baseDir}/mappings/{sport}/{type}.json`
 */
export class IdMapper {
  private baseDir: string;
  /** In-memory cache keyed by "{sport}/{type}" */
  private cache = new Map<string, MappingFile>();

  constructor(baseDir: string) {
    this.baseDir = baseDir;
  }

  // ── paths ──────────────────────────────────────────────────────────

  private filePath(sport: string, type: EntityType): string {
    return path.join(this.baseDir, "mappings", sport.toLowerCase(), `${type}.json`);
  }

  private cacheKey(sport: string, type: EntityType): string {
    return `${sport.toLowerCase()}/${type}`;
  }

  // ── persistence ────────────────────────────────────────────────────

  private load(sport: string, type: EntityType): MappingFile {
    const key = this.cacheKey(sport, type);
    const cached = this.cache.get(key);
    if (cached) return cached;

    const fp = this.filePath(sport, type);
    let data: MappingFile;

    if (fs.existsSync(fp)) {
      try {
        data = JSON.parse(fs.readFileSync(fp, "utf-8")) as MappingFile;
      } catch {
        data = { sport, type, maps: [] };
      }
    } else {
      data = { sport, type, maps: [] };
    }

    this.cache.set(key, data);
    return data;
  }

  private save(data: MappingFile): void {
    const fp = this.filePath(data.sport, data.type);
    fs.mkdirSync(path.dirname(fp), { recursive: true });
    fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf-8");
    this.cache.set(this.cacheKey(data.sport, data.type), data);
  }

  // ── public API ─────────────────────────────────────────────────────

  /**
   * Link a provider-specific external ID to a canonical internal ID.
   * Upserts — if the same provider+externalId already exists, it is updated.
   */
  link(
    sport: Sport | string,
    type: EntityType,
    provider: string,
    externalId: string | number,
    canonicalId: string,
  ): void {
    const data = this.load(sport, type);
    const idx = data.maps.findIndex(
      (m) => m.provider === provider && String(m.externalId) === String(externalId),
    );
    const entry: ProviderIdEntry = { provider, externalId, canonicalId };

    if (idx >= 0) {
      data.maps[idx] = entry;
    } else {
      data.maps.push(entry);
    }
    this.save(data);
  }

  /**
   * Resolve an external provider ID to the canonical internal ID.
   * Returns `undefined` if no mapping exists.
   */
  resolve(
    sport: Sport | string,
    type: EntityType,
    provider: string,
    externalId: string | number,
  ): string | undefined {
    const data = this.load(sport, type);
    return data.maps.find(
      (m) => m.provider === provider && String(m.externalId) === String(externalId),
    )?.canonicalId;
  }

  /**
   * Reverse-lookup: find the external provider ID for a canonical ID.
   */
  resolveExternal(
    sport: Sport | string,
    type: EntityType,
    provider: string,
    canonicalId: string,
  ): string | number | undefined {
    const data = this.load(sport, type);
    return data.maps.find(
      (m) => m.provider === provider && m.canonicalId === canonicalId,
    )?.externalId;
  }

  // ── convenience helpers ────────────────────────────────────────────

  linkGame(sport: Sport | string, provider: string, externalId: string | number, canonicalId: string): void {
    this.link(sport, "game", provider, externalId, canonicalId);
  }

  linkTeam(sport: Sport | string, provider: string, externalId: string | number, canonicalId: string): void {
    this.link(sport, "team", provider, externalId, canonicalId);
  }

  linkPlayer(sport: Sport | string, provider: string, externalId: string | number, canonicalId: string): void {
    this.link(sport, "player", provider, externalId, canonicalId);
  }

  resolveGame(sport: Sport | string, provider: string, externalId: string | number): string | undefined {
    return this.resolve(sport, "game", provider, externalId);
  }

  resolveTeam(sport: Sport | string, provider: string, externalId: string | number): string | undefined {
    return this.resolve(sport, "team", provider, externalId);
  }

  resolvePlayer(sport: Sport | string, provider: string, externalId: string | number): string | undefined {
    return this.resolve(sport, "player", provider, externalId);
  }
}
