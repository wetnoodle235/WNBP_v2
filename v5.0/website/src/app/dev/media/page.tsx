import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { resolveServerApiBase } from "@/lib/api-base";
import { getViewerTier } from "@/lib/server-access";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Media Admin | WNBP",
  description: "Local media mirror diagnostics and freshness status.",
  robots: { index: false, follow: false },
};

type MediaStatusResponse = {
  success?: boolean;
  data?: {
    catalog_ready?: boolean;
    catalog_path?: string;
    media_dir?: string;
    media_dir_exists?: boolean;
    stale_thresholds?: {
      warning_hours?: number;
      error_hours?: number;
    };
    total_assets?: number;
    synced_assets?: number;
    error_assets?: number;
    stale_assets?: number;
    latest_fetched_at?: string | null;
    by_status?: Record<string, number>;
    by_staleness?: Record<string, number>;
    by_entity_type?: Record<string, number>;
    by_sport?: Record<string, number>;
    stale_by_sport?: Record<string, number>;
  };
  meta?: {
    cached_at?: string;
  };
};

async function getMediaStatus(): Promise<MediaStatusResponse | null> {
  try {
    const res = await fetch(`${resolveServerApiBase()}/v1/media/status`, {
      cache: "no-store",
    });
    if (!res.ok) {
      return null;
    }
    return (await res.json()) as MediaStatusResponse;
  } catch {
    return null;
  }
}

function formatTimestamp(value?: string | null): string {
  if (!value) {
    return "never";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function renderRows(source: Record<string, number> | undefined, emptyLabel: string) {
  const rows = Object.entries(source ?? {});
  if (rows.length === 0) {
    return (
      <tr>
        <td colSpan={2} style={{ color: "var(--color-text-muted)" }}>{emptyLabel}</td>
      </tr>
    );
  }
  return rows.map(([key, value]) => (
    <tr key={key}>
      <td>{key}</td>
      <td>{value}</td>
    </tr>
  ));
}

export default async function DevMediaPage() {
  const tier = await getViewerTier();
  if (tier !== "dev") {
    notFound();
  }

  const status = await getMediaStatus();
  const data = status?.data;

  return (
    <main className="container" style={{ paddingTop: "var(--space-8)", paddingBottom: "var(--space-10)" }}>
      <section className="card" style={{ marginBottom: "var(--space-6)" }}>
        <div className="card-header" style={{ display: "flex", justifyContent: "space-between", gap: "var(--space-4)", alignItems: "flex-start", flexWrap: "wrap" }}>
          <div>
            <h1 className="card-title">Media Admin</h1>
            <p className="card-subtitle">Mirror coverage, freshness, and stale-asset diagnostics.</p>
          </div>
          <div style={{ display: "flex", gap: "var(--space-3)", flexWrap: "wrap" }}>
            <Link href="/dev" className="btn btn-outline">Back to Dev</Link>
            <Link href="/api-docs" className="btn btn-outline">Tech Docs</Link>
          </div>
        </div>

        <div className="card-body" style={{ display: "grid", gap: "var(--space-5)" }}>
          <div className="module-grid">
            <div className="module-card">
              <div style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>Catalog</div>
              <div style={{ fontSize: "var(--text-xl)", fontWeight: 700 }}>{data?.catalog_ready ? "ready" : "offline"}</div>
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)", marginTop: 6 }}>
                Updated: {formatTimestamp(status?.meta?.cached_at)}
              </div>
            </div>
            <div className="module-card">
              <div style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>Assets</div>
              <div style={{ fontSize: "var(--text-xl)", fontWeight: 700 }}>{data?.total_assets ?? 0}</div>
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)", marginTop: 6 }}>
                Synced: {data?.synced_assets ?? 0} · Errors: {data?.error_assets ?? 0}
              </div>
            </div>
            <div className="module-card">
              <div style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>Freshness</div>
              <div style={{ fontSize: "var(--text-xl)", fontWeight: 700 }}>{data?.stale_assets ?? 0} stale</div>
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)", marginTop: 6 }}>
                Latest sync: {formatTimestamp(data?.latest_fetched_at)}
              </div>
            </div>
            <div className="module-card">
              <div style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>Thresholds</div>
              <div style={{ fontSize: "var(--text-xl)", fontWeight: 700 }}>
                {data?.stale_thresholds?.warning_hours ?? 0}h / {data?.stale_thresholds?.error_hours ?? 0}h
              </div>
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)", marginTop: 6 }}>
                warning / critical
              </div>
            </div>
          </div>

          <div className="module-card">
            <div style={{ fontSize: "var(--text-sm)", fontWeight: 600, marginBottom: 8 }}>Storage</div>
            <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)", display: "grid", gap: 6 }}>
              <div>Media dir: {data?.media_dir ?? "unknown"}</div>
              <div>Catalog path: {data?.catalog_path ?? "unknown"}</div>
              <div>Media dir exists: {data?.media_dir_exists ? "yes" : "no"}</div>
            </div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: "var(--space-4)" }}>
            <div className="module-card">
              <div style={{ fontSize: "var(--text-sm)", fontWeight: 600, marginBottom: 10 }}>By Staleness</div>
              <div className="data-table-wrap responsive-table-wrap">
                <table className="data-table responsive-table">
                  <thead>
                    <tr><th>Bucket</th><th>Count</th></tr>
                  </thead>
                  <tbody>{renderRows(data?.by_staleness, "No freshness data")}</tbody>
                </table>
              </div>
            </div>

            <div className="module-card">
              <div style={{ fontSize: "var(--text-sm)", fontWeight: 600, marginBottom: 10 }}>By Status</div>
              <div className="data-table-wrap responsive-table-wrap">
                <table className="data-table responsive-table">
                  <thead>
                    <tr><th>Status</th><th>Count</th></tr>
                  </thead>
                  <tbody>{renderRows(data?.by_status, "No status data")}</tbody>
                </table>
              </div>
            </div>

            <div className="module-card">
              <div style={{ fontSize: "var(--text-sm)", fontWeight: 600, marginBottom: 10 }}>By Entity Type</div>
              <div className="data-table-wrap responsive-table-wrap">
                <table className="data-table responsive-table">
                  <thead>
                    <tr><th>Entity</th><th>Count</th></tr>
                  </thead>
                  <tbody>{renderRows(data?.by_entity_type, "No entity data")}</tbody>
                </table>
              </div>
            </div>

            <div className="module-card">
              <div style={{ fontSize: "var(--text-sm)", fontWeight: 600, marginBottom: 10 }}>Stale By Sport</div>
              <div className="data-table-wrap responsive-table-wrap">
                <table className="data-table responsive-table">
                  <thead>
                    <tr><th>Sport</th><th>Stale</th></tr>
                  </thead>
                  <tbody>{renderRows(data?.stale_by_sport, "No stale assets")}</tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="module-card">
            <div style={{ fontSize: "var(--text-sm)", fontWeight: 600, marginBottom: 10 }}>Coverage By Sport</div>
            <div className="data-table-wrap responsive-table-wrap">
              <table className="data-table responsive-table">
                <thead>
                  <tr><th>Sport</th><th>Assets</th></tr>
                </thead>
                <tbody>{renderRows(data?.by_sport, "No sport coverage yet")}</tbody>
              </table>
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}