"use client";

import { useEffect, useMemo, useState, useCallback } from "react";
import { ALL_SPORT_KEYS } from "@/lib/sports";

const API_BASE = typeof window === "undefined"
  ? (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000")
  : "/api/proxy";

type CalibrationBin = {
  range: string;
  samples: number;
  accuracy: number | null;
  avg_confidence: number | null;
  gap: number | null;
  stable: boolean;
};

type TrendBucket = {
  period_start: string;
  period_end: string;
  sample_size: number;
  accuracy: number | null;
  brier_score: number | null;
  log_loss: number | null;
  ece: number | null;
};

type TrendPayload = {
  success: boolean;
  data?: {
    sport: string;
    window_days: number;
    bucket_days: number;
    buckets: TrendBucket[];
  };
};

type CalibrationPayload = {
  success: boolean;
  data?: {
    sport: string;
    window_days: number;
    sample_size: number;
    overall_accuracy?: number;
    average_confidence?: number;
    brier_score?: number;
    log_loss?: number;
    expected_calibration_error?: number;
    calibration_gap?: number;
    confidence_bins?: CalibrationBin[];
    message?: string;
  };
};

type BundleCacheEntry = {
  sport: string;
  loaded: boolean;
  age_seconds: number;
  ttl_seconds: number;
  expires_in_seconds: number;
};

type BundleCacheInfo = {
  entries: BundleCacheEntry[];
  hits: number;
  misses: number;
  hit_rate: number | null;
  ttl_seconds: number;
};

type HealthPayload = {
  success: boolean;
  data?: Record<string, { sport: string; model_type: string; status: string; cached_at: string; warnings: string[] }>;
  meta?: {
    sport: string;
    health_summary: string;
    models_count: number;
    alert_count: number;
    bundle_cache?: BundleCacheInfo;
  };
};

export function ModelHealthClient() {
  const [sport, setSport] = useState<string>("nba");
  const [days, setDays] = useState<number>(180);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<CalibrationPayload | null>(null);
  
  const [isTrendLoading, setIsTrendLoading] = useState<boolean>(false);
  const [trendPayload, setTrendPayload] = useState<TrendPayload | null>(null);
  const [healthPayload, setHealthPayload] = useState<HealthPayload | null>(null);

  const loadHealth = useCallback(async (s: string) => {
    try {
      const res = await fetch(`${API_BASE}/v1/predictions/${s}/health`, { cache: "no-store" });
      if (res.ok) setHealthPayload((await res.json()) as HealthPayload);
      else setHealthPayload(null);
    } catch {
      setHealthPayload(null);
    }
  }, []);

  useEffect(() => {
    const ac = new AbortController();

    async function load() {
      setIsLoading(true);
      setError(null);
      try {
        const url = `${API_BASE}/v1/predictions/${sport}/metrics/calibration?days=${days}&bins=10`;
        const res = await fetch(url, { cache: "no-store", signal: ac.signal });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const json = (await res.json()) as CalibrationPayload;
        setPayload(json);
      } catch (err) {
        if (!ac.signal.aborted) {
          setPayload(null);
          setError(err instanceof Error ? err.message : "Failed to load");
        }
      } finally {
        if (!ac.signal.aborted) {
          setIsLoading(false);
        }
      }
    }

    load();
    return () => ac.abort();
  }, [sport, days]);

  useEffect(() => {
    loadHealth(sport);
  }, [sport, loadHealth]);

  useEffect(() => {
    const ac = new AbortController();

    async function loadTrend() {
      setIsTrendLoading(true);
      try {
        const url = `${API_BASE}/v1/predictions/${sport}/metrics/calibration/trend?window_days=180&bucket_days=30`;
        const res = await fetch(url, { cache: "no-store", signal: ac.signal });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const json = (await res.json()) as TrendPayload;
        setTrendPayload(json);
      } catch (err) {
        if (!ac.signal.aborted) {
          setTrendPayload(null);
        }
      } finally {
        if (!ac.signal.aborted) {
          setIsTrendLoading(false);
        }
      }
    }

    loadTrend();
    return () => ac.abort();
  }, [sport]);

  const bins = useMemo(() => payload?.data?.confidence_bins ?? [], [payload]);

  return (
    <main className="container" style={{ paddingTop: "var(--space-8)", paddingBottom: "var(--space-10)" }}>
      <section className="card" style={{ marginBottom: "var(--space-6)" }}>
        <div className="card-header">
          <h1 className="card-title">Model Health</h1>
          <p className="card-subtitle">Calibration and confidence-quality metrics by sport.</p>
        </div>
        <div className="card-body" style={{ display: "grid", gap: "var(--space-4)" }}>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--space-3)" }}>
            <label htmlFor="mh-sport" style={{ display: "grid", gap: 6 }}>
              <span>Sport</span>
              <select id="mh-sport" className="auth-input" value={sport} onChange={(e) => setSport(e.target.value)}>
                {ALL_SPORT_KEYS.map((s) => (
                  <option key={s} value={s}>{s.toUpperCase()}</option>
                ))}
              </select>
            </label>
            <label htmlFor="mh-window" style={{ display: "grid", gap: 6 }}>
              <span>Window</span>
              <select id="mh-window" className="auth-input" value={days} onChange={(e) => setDays(Number(e.target.value))}>
                <option value={30}>30 days</option>
                <option value={90}>90 days</option>
                <option value={180}>180 days</option>
                <option value={365}>365 days</option>
              </select>
            </label>
          </div>

          {isLoading ? <p role="status" aria-live="polite">Loading calibration metrics...</p> : null}
          {error ? <p role="alert" style={{ color: "var(--color-danger)" }}>Failed to load: {error}</p> : null}
          {!isLoading && !error && payload?.data?.message ? <p>{payload.data.message}</p> : null}

          {!isLoading && !error && payload?.data && !payload.data.message ? (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: "var(--space-3)" }}>
                <MetricCard label="Sample Size" value={String(payload.data.sample_size ?? 0)} />
                <MetricCard label="Accuracy" value={fmtPct(payload.data.overall_accuracy)} />
                <MetricCard label="Avg Confidence" value={fmtPct(payload.data.average_confidence)} />
                <MetricCard label="Calibration Gap" value={fmtPct(payload.data.calibration_gap)} />
                <MetricCard label="Brier Score" value={fmtNum(payload.data.brier_score)} />
                <MetricCard label="Log Loss" value={fmtNum(payload.data.log_loss)} />
                <MetricCard label="ECE" value={fmtNum(payload.data.expected_calibration_error)} />
              </div>

              <div className="data-table-wrap responsive-table-wrap">
                <table className="data-table responsive-table model-health-bins-table">
                  <thead>
                    <tr>
                      <th>Confidence Bin</th>
                      <th>Samples</th>
                      <th>Accuracy</th>
                      <th>Avg Confidence</th>
                      <th>Gap</th>
                      <th>Stable</th>
                    </tr>
                  </thead>
                  <tbody>
                    {bins.map((b) => (
                      <tr key={b.range}>
                        <td>{b.range}</td>
                        <td>{b.samples}</td>
                        <td>{fmtPct(b.accuracy)}</td>
                        <td>{fmtPct(b.avg_confidence)}</td>
                        <td>{fmtPct(b.gap)}</td>
                        <td>{b.stable ? "yes" : "no"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <hr style={{ margin: "var(--space-6) 0" }} />

              <h3 style={{ marginBottom: "var(--space-4)" }}>Performance Trend (180 days)</h3>
              {isTrendLoading ? (
                <p style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>Loading trend data...</p>
              ) : trendPayload?.data?.buckets && trendPayload.data.buckets.length > 0 ? (
                <>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "var(--space-3)", marginBottom: "var(--space-4)" }}>
                    <SparkCard
                      label="Accuracy"
                      values={trendPayload.data.buckets.map((b) => b.accuracy)}
                      color="#16a34a"
                      fmt={fmtPct}
                    />
                    <SparkCard
                      label="Brier Score"
                      values={trendPayload.data.buckets.map((b) => b.brier_score)}
                      color="#d97706"
                      fmt={fmtNum}
                    />
                    <SparkCard
                      label="ECE"
                      values={trendPayload.data.buckets.map((b) => b.ece)}
                      color="#6366f1"
                      fmt={fmtNum}
                    />
                  </div>
                <div className="data-table-wrap responsive-table-wrap">
                  <table className="data-table responsive-table model-health-trend-table" style={{ fontSize: "var(--text-sm)" }}>
                    <thead>
                      <tr>
                        <th>Period</th>
                        <th>Samples</th>
                        <th>Accuracy</th>
                        <th>Brier</th>
                        <th>Log Loss</th>
                        <th>ECE</th>
                      </tr>
                    </thead>
                    <tbody>
                      {trendPayload.data.buckets.map((b) => (
                        <tr key={`${b.period_start}-${b.period_end}`}>
                          <td style={{ fontSize: "var(--text-xs) " }}>
                            {new Date(b.period_start).toLocaleDateString()} to {new Date(b.period_end).toLocaleDateString()}
                          </td>
                          <td>{b.sample_size}</td>
                          <td>{fmtPct(b.accuracy)}</td>
                          <td>{fmtNum(b.brier_score)}</td>
                          <td>{fmtNum(b.log_loss)}</td>
                          <td>{fmtNum(b.ece)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                </>
              ) : null}
            </>
          ) : null}
        </div>
      </section>

      {/* Model Diagnostics & Bundle Cache section */}
      {healthPayload?.meta && (
    <section className="card" style={{ marginTop: "var(--space-6)" }}>
      <div className="card-header">
        <h2 className="card-title" style={{ fontSize: "var(--text-lg)" }}>Model Diagnostics</h2>
        <p className="card-subtitle">
          Bundle cache state and live model health for <strong>{sport.toUpperCase()}</strong>.
        </p>
      </div>
      <div className="card-body" style={{ display: "grid", gap: "var(--space-4)" }}>
        {/* Overall status */}
        <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--space-3)", alignItems: "center" }}>
          <StatusPill status={healthPayload.meta.health_summary} />
          <span style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>
            {healthPayload.meta.models_count} model{healthPayload.meta.models_count !== 1 ? "s" : ""}
            {" · "}
            {healthPayload.meta.alert_count} alert{healthPayload.meta.alert_count !== 1 ? "s" : ""}
          </span>
        </div>

        {/* Individual model status */}
        {healthPayload.data && Object.keys(healthPayload.data).length > 0 && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: "var(--space-3)" }}>
            {Object.entries(healthPayload.data).map(([modelType, m]) => (
              <div key={modelType} className="card" style={{ margin: 0 }}>
                <div className="card-body" style={{ padding: "var(--space-3) var(--space-4)" }}>
                  <div style={{ fontWeight: 600, fontSize: "var(--text-sm)", marginBottom: "0.25rem" }}>
                    {modelType.replace(/_/g, " ")}
                  </div>
                  <StatusPill status={m.status} />
                  {m.warnings.length > 0 && (
                    <ul style={{ margin: "0.4rem 0 0", paddingLeft: "1rem", fontSize: "var(--text-xs)", color: "var(--color-text-muted)" }}>
                      {m.warnings.map((w, i) => <li key={i}>{w}</li>)}
                    </ul>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Bundle cache stats */}
        {healthPayload.meta.bundle_cache && (
          <>
            <h3 style={{ margin: "0.5rem 0 0", fontSize: "var(--text-base)" }}>Bundle Cache</h3>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "var(--space-3)" }}>
              <MetricCard
                label="Hit Rate"
                value={healthPayload.meta.bundle_cache.hit_rate != null
                  ? `${(healthPayload.meta.bundle_cache.hit_rate * 100).toFixed(1)}%`
                  : "—"}
              />
              <MetricCard label="Cache Hits" value={String(healthPayload.meta.bundle_cache.hits)} />
              <MetricCard label="Cache Misses" value={String(healthPayload.meta.bundle_cache.misses)} />
              <MetricCard label="TTL (s)" value={String(healthPayload.meta.bundle_cache.ttl_seconds)} />
            </div>
            {healthPayload.meta.bundle_cache.entries.length > 0 && (
              <div className="data-table-wrap responsive-table-wrap">
                <table className="data-table responsive-table model-health-bundle-table" style={{ fontSize: "var(--text-sm)" }}>
                  <thead>
                    <tr>
                      <th>Sport</th>
                      <th>Loaded</th>
                      <th>Age (s)</th>
                      <th>Expires In (s)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {healthPayload.meta.bundle_cache.entries.map((e) => (
                      <tr key={e.sport}>
                        <td>{e.sport.toUpperCase()}</td>
                        <td>{e.loaded ? "✓" : "✗"}</td>
                        <td>{e.age_seconds}</td>
                        <td>{e.expires_in_seconds}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </div>
    </section>
  )}

    </main>
  );
}

function StatusPill({ status }: { status: string }) {
  const color =
    status === "healthy" ? "#16a34a"
    : status === "degraded" ? "#d97706"
    : status === "unhealthy" ? "#dc2626"
    : "var(--color-text-muted)";
  return (
    <span
      style={{
        display: "inline-block",
        padding: "0.15rem 0.55rem",
        borderRadius: "999px",
        border: `1px solid ${color}`,
        color,
        fontSize: "var(--text-xs)",
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
      }}
    >
      {status}
    </span>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="card" style={{ margin: 0 }}>
      <div className="card-body" style={{ padding: "var(--space-3) var(--space-4)" }}>
        <div style={{ color: "var(--color-text-muted)", fontSize: "var(--text-sm)" }}>{label}</div>
        <div style={{ fontSize: "var(--text-xl)", fontWeight: 700 }}>{value}</div>
      </div>
    </div>
  );
}

function TrendSparkline({ values, color = "#6366f1" }: { values: (number | null)[]; color?: string }) {
  const nums = values.filter((v): v is number => v !== null && !Number.isNaN(v));
  if (nums.length < 2) {
    return <div style={{ height: 40, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--color-text-muted)", fontSize: "var(--text-xs)" }}>—</div>;
  }
  const W = 160;
  const H = 40;
  const pad = 3;
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const range = max - min || 1;
  const points = nums.map((v, i) => {
    const x = pad + (i / (nums.length - 1)) * (W - pad * 2);
    const y = H - pad - ((v - min) / range) * (H - pad * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const last = nums[nums.length - 1];
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", maxWidth: W, height: H, display: "block" }} role="img" aria-label="Trend sparkline chart">
      <polyline
        points={points.join(" ")}
        fill="none"
        stroke={color}
        strokeWidth={1.8}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      <circle cx={points[points.length - 1].split(",")[0]} cy={points[points.length - 1].split(",")[1]} r={2.5} fill={color} />
      <text x={W - pad} y={H - pad} textAnchor="end" fontSize={9} fill={color} fontFamily="monospace">
        {last.toFixed(3)}
      </text>
    </svg>
  );
}

function SparkCard({ label, values, color, fmt }: {
  label: string;
  values: (number | null)[];
  color: string;
  fmt: (v: number | null | undefined) => string;
}) {
  const nums = values.filter((v): v is number => v !== null);
  const latest = nums.length > 0 ? nums[nums.length - 1] : null;
  const prev = nums.length > 1 ? nums[nums.length - 2] : null;
  const delta = (latest !== null && prev !== null) ? latest - prev : null;
  return (
    <div className="card" style={{ margin: 0 }}>
      <div className="card-body" style={{ padding: "var(--space-3) var(--space-4)" }}>
        <div style={{ color: "var(--color-text-muted)", fontSize: "var(--text-sm)", marginBottom: "0.25rem" }}>{label}</div>
        <div style={{ display: "flex", alignItems: "baseline", gap: "0.4rem", marginBottom: "0.4rem" }}>
          <span style={{ fontSize: "var(--text-lg)", fontWeight: 700 }}>{fmt(latest)}</span>
          {delta !== null && (
            <span style={{ fontSize: "var(--text-xs)", color: delta < 0 ? "#16a34a" : "#dc2626", fontWeight: 600 }}>
              {delta > 0 ? "+" : ""}{fmt(delta)}
            </span>
          )}
        </div>
        <TrendSparkline values={values} color={color} />
      </div>
    </div>
  );
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return `${(v * 100).toFixed(2)}%`;
}

function fmtNum(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return v.toFixed(4);
}
