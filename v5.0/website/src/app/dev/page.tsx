import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { getViewerTier } from "@/lib/server-access";
import { resolveServerApiBase } from "@/lib/api-base";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Dev Dashboard | WNBP",
  description: "Local development dashboard for backend and media diagnostics.",
  robots: { index: false, follow: false },
};

type HealthResponse = {
  success?: boolean;
  data?: {
    status?: string;
    uptime_seconds?: number;
    cache?: {
      hits?: number;
      misses?: number;
      size?: number;
    };
    media_mirror?: {
      total_assets?: number;
      stale_assets?: number;
      error_assets?: number;
    };
  };
};

async function getHealthSummary(): Promise<HealthResponse | null> {
  try {
    const res = await fetch(`${resolveServerApiBase()}/v1/health`, {
      cache: "no-store",
    });
    if (!res.ok) {
      return null;
    }
    return (await res.json()) as HealthResponse;
  } catch {
    return null;
  }
}

function formatUptime(seconds?: number): string {
  if (typeof seconds !== "number" || !Number.isFinite(seconds)) {
    return "unknown";
  }

  const total = Math.max(0, Math.round(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

export default async function DevDashboardPage() {
  const tier = await getViewerTier();
  if (tier !== "dev") {
    notFound();
  }

  const health = await getHealthSummary();
  const healthData = health?.data;

  return (
    <main className="container" style={{ paddingTop: "var(--space-8)", paddingBottom: "var(--space-10)" }}>
      <section className="card" style={{ marginBottom: "var(--space-6)" }}>
        <div className="card-header">
          <h1 className="card-title">Dev Dashboard</h1>
          <p className="card-subtitle">Local-only operational tools and diagnostics.</p>
        </div>
        <div className="card-body" style={{ display: "grid", gap: "var(--space-4)" }}>
          <div className="module-grid">
            <div className="module-card">
              <div style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>Backend</div>
              <div style={{ fontSize: "var(--text-xl)", fontWeight: 700, marginBottom: 6 }}>
                {healthData?.status ?? "unknown"}
              </div>
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>
                Uptime: {formatUptime(healthData?.uptime_seconds)}
              </div>
            </div>

            <div className="module-card">
              <div style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>Cache</div>
              <div style={{ fontSize: "var(--text-xl)", fontWeight: 700, marginBottom: 6 }}>
                {healthData?.cache?.hits ?? 0} hits
              </div>
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>
                Misses: {healthData?.cache?.misses ?? 0}
              </div>
            </div>

            <div className="module-card">
              <div style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>Media Mirror</div>
              <div style={{ fontSize: "var(--text-xl)", fontWeight: 700, marginBottom: 6 }}>
                {healthData?.media_mirror?.total_assets ?? 0} assets
              </div>
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>
                Stale: {healthData?.media_mirror?.stale_assets ?? 0} · Errors: {healthData?.media_mirror?.error_assets ?? 0}
              </div>
            </div>
          </div>

          <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--space-3)" }}>
            <Link href="/dev/media" className="btn btn-primary">Open Media Admin</Link>
            <Link href="/api-docs" className="btn btn-outline">Tech Docs</Link>
            <Link href="/model-health" className="btn btn-outline">Model Health</Link>
          </div>
        </div>
      </section>
    </main>
  );
}