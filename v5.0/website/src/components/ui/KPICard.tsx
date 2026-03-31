"use client";

interface Props {
  label: string;
  value: string | number;
  /** Previous value for trend calculation */
  previousValue?: number;
  /** Override trend direction */
  trend?: "up" | "down" | "flat";
  /** Suffix like "%" or "pts" */
  suffix?: string;
  icon?: React.ReactNode;
  className?: string;
}

/**
 * KPI stat card with optional trend indicator arrow.
 * Shows green/red trend based on value change.
 */
export function KPICard({ label, value, previousValue, trend, suffix = "", icon, className = "" }: Props) {
  let resolvedTrend = trend;
  let changePercent: number | null = null;

  if (!resolvedTrend && previousValue != null && typeof value === "number") {
    const diff = value - previousValue;
    resolvedTrend = diff > 0 ? "up" : diff < 0 ? "down" : "flat";
    if (previousValue !== 0) {
      changePercent = Math.round((diff / Math.abs(previousValue)) * 100);
    }
  }

  const trendColor =
    resolvedTrend === "up" ? "var(--color-win, #16a34a)" :
    resolvedTrend === "down" ? "var(--color-loss, #dc2626)" :
    "var(--color-text-muted)";

  const trendArrow =
    resolvedTrend === "up" ? "↑" :
    resolvedTrend === "down" ? "↓" :
    "→";

  return (
    <div className={`kpi-card ${className}`}>
      <div className="kpi-header">
        {icon && <span className="kpi-icon" aria-hidden="true">{icon}</span>}
        <span className="kpi-label">{label}</span>
      </div>
      <div className="kpi-value">
        {value}{suffix}
      </div>
      {resolvedTrend && (
        <div className="kpi-trend" style={{ color: trendColor }}>
          <span aria-hidden="true">{trendArrow}</span>
          {changePercent != null && (
            <span className="kpi-change">{changePercent > 0 ? "+" : ""}{changePercent}%</span>
          )}
          <span className="sr-only">
            {resolvedTrend === "up" ? "Trending up" : resolvedTrend === "down" ? "Trending down" : "No change"}
          </span>
        </div>
      )}
    </div>
  );
}
