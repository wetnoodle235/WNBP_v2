// Oracle's Elixir LoL match data — DISABLED
// The S3 download links are no longer public.
// Use the "lolesports" provider instead, which uses the
// official Riot Games esports API with better coverage.

import type { Provider } from "../../core/types.js";

const oracleselixir: Provider = {
  name: "oracleselixir",
  label: "Oracle's Elixir LoL (disabled — use lolesports)",
  sports: ["lol"],
  requiresKey: false,
  rateLimit: { requests: 1, perMs: 1_000 },
  endpoints: [],
  enabled: false,
  async import() {
    return { provider: "oracleselixir", sport: "lol", filesWritten: 0,
      errors: ["Provider disabled: use lolesports provider instead"], durationMs: 0 };
  },
};

export default oracleselixir;
