"use client";

interface ConfidenceGaugeProps {
  value: number; // 0-100
  size?: number;
  label?: string;
}

export function ConfidenceGauge({ value, size = 80, label }: ConfidenceGaugeProps) {
  const clamped = Math.min(100, Math.max(0, value));
  const radius = (size - 8) / 2;
  const circumference = 2 * Math.PI * radius;
  const dashOffset = circumference * (1 - clamped / 100);

  const color =
    clamped >= 75 ? "var(--color-win, #16a34a)" :
    clamped >= 55 ? "var(--color-accent, #f59e0b)" :
    "var(--color-loss, #dc2626)";

  return (
    <div
      className="confidence-gauge"
      style={{ width: size, height: size, position: "relative", display: "inline-flex", alignItems: "center", justifyContent: "center" }}
    >
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} aria-hidden="true">
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="var(--color-border, #e5e7eb)"
          strokeWidth={6}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={color}
          strokeWidth={6}
          strokeDasharray={circumference}
          strokeDashoffset={dashOffset}
          strokeLinecap="round"
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
          style={{ transition: "stroke-dashoffset 0.6s ease" }}
        />
      </svg>
      <div
        style={{
          position: "absolute",
          textAlign: "center",
          lineHeight: 1.2,
        }}
      >
        <div style={{ fontSize: size * 0.22, fontWeight: 700, color }}>{clamped.toFixed(0)}%</div>
        {label && (
          <div style={{ fontSize: size * 0.12, color: "var(--color-text-muted)", fontWeight: 500 }}>
            {label}
          </div>
        )}
      </div>
      <span className="sr-only">{label ?? "Confidence"}: {clamped.toFixed(0)}%</span>
    </div>
  );
}
