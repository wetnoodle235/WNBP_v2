// ClearSports API — DISABLED (service defunct)
// ClearSports shut down. No replacement needed as coverage is
// handled by espn, actionnetwork, and draftkings providers.

import type { Provider } from "../../core/types.js";

const clearsports: Provider = {
  name: "clearsports",
  label: "ClearSports (disabled — service defunct)",
  sports: [],
  requiresKey: false,
  rateLimit: { requests: 1, perMs: 1_000 },
  endpoints: [],
  enabled: false,
  async import() {
    return { provider: "clearsports", sport: "multi", filesWritten: 0,
      errors: ["Provider disabled: service is defunct"], durationMs: 0 };
  },
};

export default clearsports;
