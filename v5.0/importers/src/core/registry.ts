// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Provider Registry
// ──────────────────────────────────────────────────────────
// Central registry that discovers, loads, and runs providers.
// New providers are added by: 1) creating src/providers/{name}/index.ts
// that default-exports a Provider, then 2) adding the name to PROVIDER_MODULES.

import type { ImportOptions, ImportResult, Provider, Sport } from "./types.js";
import { logger } from "./logger.js";

/**
 * All known provider module names.
 * To add a new source, add its folder name here.
 * To disable a source, set provider.enabled = false in its module.
 */
export const PROVIDER_MODULES = [
  "espn",
  "oddsapi",
  "odds",
  "sgo",
  "nbastats",
  "nflfastr",
  "apisports",
  "cfbdata",
  "statsbomb",
  "understat",
  "openf1",
  "ergast",
  "tennisabstract",
  "ufcstats",
  "lahman",
  "nhl",
  "clearsports",
  "pandascore",
  "opendota",
  "weather",
  "oracleselixir",
  "fivethirtyeight",
  "footballdata",
  "mlbstats",
] as const;

export type ProviderName = (typeof PROVIDER_MODULES)[number];

class Registry {
  private providers = new Map<string, Provider>();

  register(provider: Provider): void {
    if (this.providers.has(provider.name)) {
      logger.warn(`Provider "${provider.name}" already registered — overwriting`);
    }
    this.providers.set(provider.name, provider);
    logger.debug(`Registered provider: ${provider.name} (${provider.sports.join(", ")})`, "registry");
  }

  get(name: string): Provider | undefined {
    return this.providers.get(name);
  }

  getAll(): Provider[] {
    return Array.from(this.providers.values());
  }

  getEnabled(): Provider[] {
    return this.getAll().filter((p) => p.enabled);
  }

  /** Get all enabled providers that cover a given sport */
  forSport(sport: Sport): Provider[] {
    return this.getEnabled().filter((p) => p.sports.includes(sport));
  }

  /** Enable or disable a provider at runtime */
  setEnabled(name: string, enabled: boolean): boolean {
    const p = this.providers.get(name);
    if (!p) return false;
    p.enabled = enabled;
    logger.info(`${enabled ? "Enabled" : "Disabled"} provider: ${name}`, "registry");
    return true;
  }

  /** Dynamically load all provider modules */
  async loadAll(): Promise<void> {
    await this.loadOnly([...PROVIDER_MODULES]);
  }

  /** Load only the specified provider modules (faster startup) */
  async loadOnly(names: readonly string[]): Promise<void> {
    const toLoad = names.filter((n) => PROVIDER_MODULES.includes(n as any));
    await Promise.all(
      toLoad.map(async (name) => {
        try {
          const mod = await import(`../providers/${name}/index.js`);
          const provider: Provider = mod.default ?? mod.provider;
          if (!provider) {
            logger.warn(`Provider module "${name}" has no default export`, "registry");
            return;
          }
          this.register(provider);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn(`Failed to load provider "${name}": ${msg}`, "registry");
        }
      }),
    );
  }

  /**
   * Run imports for selected providers.
   * Providers run concurrently (respecting per-provider rate limits).
   * Returns results for each provider run.
   */
  async run(
    providerNames: string[] | "all",
    opts: ImportOptions,
  ): Promise<ImportResult[]> {
    const targets =
      providerNames === "all"
        ? this.getEnabled()
        : providerNames
            .map((n) => this.get(n))
            .filter((p): p is Provider => {
              if (!p) return false;
              if (!p.enabled) {
                logger.warn(`Provider "${p.name}" is disabled — skipping`);
                return false;
              }
              return true;
            });

    if (targets.length === 0) {
      logger.warn("No providers to run", "registry");
      return [];
    }

    logger.info(`Running ${targets.length} provider(s) in parallel: ${targets.map((p) => p.name).join(", ")}`, "registry");

    const tasks = targets.map(async (provider) => {
      // Filter sports to only those this provider supports
      const sportFilter =
        opts.sports.length > 0
          ? opts.sports.filter((s) => provider.sports.includes(s))
          : [...provider.sports];

      if (sportFilter.length === 0) {
        logger.info(`Skipping ${provider.name} — no matching sports`, "registry");
        return null;
      }

      const providerOpts: ImportOptions = { ...opts, sports: sportFilter as Sport[] };

      try {
        const result = await provider.import(providerOpts);
        logger.summary(result.provider, result.filesWritten, result.errors.length, result.durationMs);
        return result;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error(`Provider "${provider.name}" crashed: ${msg}`, "registry");
        return {
          provider: provider.name,
          sport: "multi" as const,
          filesWritten: 0,
          errors: [msg],
          durationMs: 0,
        } satisfies ImportResult;
      }
    });

    const settled = await Promise.allSettled(tasks);
    const results: ImportResult[] = [];
    for (const s of settled) {
      if (s.status === "fulfilled" && s.value) {
        results.push(s.value);
      } else if (s.status === "rejected") {
        results.push({
          provider: "unknown",
          sport: "multi",
          filesWritten: 0,
          errors: [String(s.reason)],
          durationMs: 0,
        });
      }
    }

    return results;
  }
}

/** Singleton registry instance */
export const registry = new Registry();
