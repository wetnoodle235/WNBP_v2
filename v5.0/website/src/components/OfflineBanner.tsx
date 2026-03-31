"use client";

import { useOnlineStatus } from "@/lib/hooks";

export function OfflineBanner() {
  const online = useOnlineStatus();

  if (online) return null;

  return (
    <div
      className="offline-banner"
      role="alert"
      aria-live="assertive"
    >
      <span aria-hidden="true">📡</span>
      <span>You&apos;re offline. Some data may be stale.</span>
    </div>
  );
}
