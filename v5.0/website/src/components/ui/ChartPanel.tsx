import type { ReactNode } from "react";

interface ChartPanelProps {
  title?: string;
  /** Optional subtitle or description */
  description?: string;
  /** Trailing header actions (buttons, time-range selectors) */
  actions?: ReactNode;
  children: ReactNode;
  /** Height of the chart area */
  height?: string | number;
  /** Shows stale data banner when truthy — pass the stale-since timestamp */
  staleAt?: Date | string | null;
  loading?: boolean;
  className?: string;
}

/**
 * Wrapper panel for all chart/visualization content.
 * Provides consistent heading, action slot, and stale-data indicator.
 * Does NOT import any charting library — renders children as-is, so the
 * caller is responsible for the chart implementation (recharts, visx, etc.)
 */
export function ChartPanel({
  title,
  description,
  actions,
  children,
  height = 240,
  staleAt,
  loading,
  className,
}: ChartPanelProps) {
  return (
    <div className={`card${className ? ` ${className}` : ""}`}>
      {(title || actions) && (
        <div className="card-header">
          <div>
            {title && <div className="card-title">{title}</div>}
            {description && (
              <p style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)", marginTop: 2 }}>
                {description}
              </p>
            )}
          </div>
          {actions && <div style={{ display: "flex", gap: "var(--space-2)", flexShrink: 0 }}>{actions}</div>}
        </div>
      )}

      {staleAt && (
        <div className="stale-banner" role="status" aria-live="polite">
          <span aria-hidden="true">⚠</span>
          Data may be stale — last updated{" "}
          {typeof staleAt === "string" ? staleAt : staleAt.toLocaleString()}
        </div>
      )}

      <div
        className="card-body"
        style={{
          height,
          position: "relative",
          padding: title || actions ? undefined : "var(--space-5)",
        }}
        aria-busy={loading}
      >
        {loading ? (
          <>
            <div
              className="skeleton"
              style={{ position: "absolute", inset: "var(--space-5)", borderRadius: "var(--radius)" }}
              aria-hidden="true"
            />
            <span className="sr-only">Loading chart…</span>
          </>
        ) : (
          children
        )}
      </div>
    </div>
  );
}
