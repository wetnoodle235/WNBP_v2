"use client";

interface TrendIndicatorProps {
  value: number;
  previousValue: number;
  format?: (v: number) => string;
  showDelta?: boolean;
  size?: "sm" | "md" | "lg";
}

export function TrendIndicator({
  value,
  previousValue,
  format = (v) => v.toFixed(1),
  showDelta = true,
  size = "md",
}: TrendIndicatorProps) {
  const delta = value - previousValue;
  const pct = previousValue !== 0 ? (delta / Math.abs(previousValue)) * 100 : 0;
  const direction = delta > 0 ? "up" : delta < 0 ? "down" : "flat";

  const colors: Record<string, string> = {
    up: "var(--color-win, #16a34a)",
    down: "var(--color-loss, #dc2626)",
    flat: "var(--color-text-muted, #6b7280)",
  };

  const arrows: Record<string, string> = { up: "↑", down: "↓", flat: "→" };

  const fontSizes: Record<string, string> = {
    sm: "var(--text-xs)",
    md: "var(--text-sm)",
    lg: "var(--text-base)",
  };

  return (
    <span
      className="trend-indicator"
      style={{ color: colors[direction], fontSize: fontSizes[size], fontWeight: 600 }}
      aria-label={`${direction === "up" ? "Increased" : direction === "down" ? "Decreased" : "Unchanged"} by ${Math.abs(pct).toFixed(1)}%`}
    >
      <span aria-hidden="true">{arrows[direction]}</span>
      {showDelta && (
        <span style={{ marginLeft: "0.2em" }}>
          {delta > 0 ? "+" : ""}
          {format(delta)}
          {pct !== 0 && (
            <span style={{ opacity: 0.7, marginLeft: "0.25em" }}>
              ({pct > 0 ? "+" : ""}{pct.toFixed(1)}%)
            </span>
          )}
        </span>
      )}
    </span>
  );
}
