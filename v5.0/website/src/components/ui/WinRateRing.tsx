"use client";

interface WinRateRingProps {
  /** Win rate from 0 to 100 */
  rate: number;
  /** Optional label below the ring */
  label?: string;
  /** Trend indicator */
  trend?: "up" | "down" | "neutral";
  /** Trend delta text (e.g., "+2.3%") */
  trendText?: string;
  /** Ring size in px */
  size?: number;
  /** Ring color — defaults to accent */
  color?: string;
}

export function WinRateRing({
  rate,
  label,
  trend,
  trendText,
  size = 80,
  color,
}: WinRateRingProps) {
  const clampedRate = Math.max(0, Math.min(100, rate));
  const radius = (size - 12) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (clampedRate / 100) * circumference;

  const ringColor =
    color ??
    (clampedRate >= 65
      ? "var(--color-win, #16a34a)"
      : clampedRate >= 50
      ? "var(--color-accent, #3b82f6)"
      : "var(--color-loss, #dc2626)");

  const trendArrow = trend === "up" ? "↑" : trend === "down" ? "↓" : "→";

  return (
    <div
      className="win-rate-ring"
      role="meter"
      aria-valuenow={clampedRate}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={`Win rate: ${clampedRate.toFixed(1)}%${label ? ` — ${label}` : ""}`}
    >
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <circle
          className="win-rate-ring-track"
          cx={size / 2}
          cy={size / 2}
          r={radius}
        />
        <circle
          className="win-rate-ring-fill"
          cx={size / 2}
          cy={size / 2}
          r={radius}
          stroke={ringColor}
          strokeDasharray={circumference}
          strokeDashoffset={offset}
        />
        <text
          className="win-rate-ring-text"
          x={size / 2}
          y={size / 2}
        >
          {Math.round(clampedRate)}%
        </text>
      </svg>
      {label && <span className="win-rate-ring-label">{label}</span>}
      {trend && trendText && (
        <span className="win-rate-ring-trend" data-trend={trend}>
          {trendArrow} {trendText}
        </span>
      )}
    </div>
  );
}
