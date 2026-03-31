"use client";

interface Props {
  /** Value between 0 and 1 */
  value: number;
  /** Optional label shown inside the bar */
  label?: string;
  /** Color of the filled portion */
  color?: string;
  /** Height in px */
  height?: number;
  className?: string;
}

/**
 * Horizontal percentage bar with animation.
 * Use for win probability, accuracy, etc.
 */
export function PercentBar({
  value,
  label,
  color = "var(--color-accent, #3b82f6)",
  height = 24,
  className = "",
}: Props) {
  const pct = Math.max(0, Math.min(100, value * 100));

  return (
    <div
      className={`percent-bar ${className}`}
      role="meter"
      aria-valuenow={Math.round(pct)}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={label ? `${label}: ${Math.round(pct)}%` : `${Math.round(pct)}%`}
      style={{ height, borderRadius: height / 2 }}
    >
      <div
        className="percent-bar-fill"
        style={{
          width: `${pct}%`,
          background: color,
          height: "100%",
          borderRadius: "inherit",
          transition: "width 0.6s ease-out",
          display: "flex",
          alignItems: "center",
          justifyContent: "flex-end",
          paddingRight: pct > 15 ? 8 : 0,
        }}
      >
        {pct > 15 && label && (
          <span style={{ fontSize: "0.7rem", fontWeight: 700, color: "#fff", whiteSpace: "nowrap" }}>
            {label}
          </span>
        )}
      </div>
      {pct <= 15 && label && (
        <span style={{
          position: "absolute",
          left: `calc(${pct}% + 6px)`,
          top: "50%",
          transform: "translateY(-50%)",
          fontSize: "0.7rem",
          fontWeight: 700,
          color: "var(--color-text-muted)",
          whiteSpace: "nowrap",
        }}>
          {label}
        </span>
      )}
    </div>
  );
}
