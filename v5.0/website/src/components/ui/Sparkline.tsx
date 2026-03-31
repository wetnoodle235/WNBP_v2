"use client";

interface SparklineProps {
  /** Array of numeric data points */
  data: number[];
  /** SVG width */
  width?: number;
  /** SVG height */
  height?: number;
  /** Stroke color (CSS value) */
  color?: string;
  /** Show a filled area beneath the line */
  filled?: boolean;
  /** Highlight the last data point with a dot */
  showDot?: boolean;
  /** Accessible label */
  label?: string;
}

export function Sparkline({
  data,
  width = 100,
  height = 28,
  color = "var(--color-accent)",
  filled = false,
  showDot = true,
  label = "Trend",
}: SparklineProps) {
  if (data.length < 2) return null;

  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const padding = 2;

  const points = data.map((val, i) => {
    const x = padding + (i / (data.length - 1)) * (width - padding * 2);
    const y = padding + (1 - (val - min) / range) * (height - padding * 2);
    return { x, y };
  });

  const pathD = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`)
    .join(" ");

  const areaD = pathD
    + ` L ${points[points.length - 1].x.toFixed(1)} ${height - padding}`
    + ` L ${points[0].x.toFixed(1)} ${height - padding} Z`;

  const last = points[points.length - 1];
  const trend = data[data.length - 1] >= data[0] ? "up" : "down";

  return (
    <svg
      className="sparkline"
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`${label}: trending ${trend}`}
    >
      {filled && (
        <path
          d={areaD}
          fill={color}
          opacity={0.15}
        />
      )}
      <path
        d={pathD}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {showDot && (
        <circle
          cx={last.x}
          cy={last.y}
          r={2.5}
          fill={color}
        />
      )}
    </svg>
  );
}
