"use client";

interface Props {
  /** Value between 0 and 100 */
  value: number;
  size?: number;
  strokeWidth?: number;
  color?: string;
  trackColor?: string;
  label?: string;
  className?: string;
}

export function ProgressRing({
  value,
  size = 64,
  strokeWidth = 5,
  color = "var(--accent-blue)",
  trackColor = "var(--border)",
  label,
  className,
}: Props) {
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (Math.min(100, Math.max(0, value)) / 100) * circumference;

  return (
    <div
      className={className}
      role="progressbar"
      aria-valuenow={Math.round(value)}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={label ?? `${Math.round(value)}%`}
      style={{ width: size, height: size, position: "relative", display: "inline-flex" }}
    >
      <svg width={size} height={size} style={{ transform: "rotate(-90deg)" }}>
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={trackColor}
          strokeWidth={strokeWidth}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={color}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          style={{ transition: "stroke-dashoffset 0.5s ease" }}
        />
      </svg>
      <span
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: `${size * 0.22}px`,
          fontWeight: 700,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {Math.round(value)}%
      </span>
    </div>
  );
}
