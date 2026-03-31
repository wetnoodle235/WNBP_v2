"use client";

interface StatComparisonProps {
  /** Left side label (e.g. team name) */
  leftLabel: string;
  /** Right side label */
  rightLabel: string;
  /** Array of stat rows to compare */
  stats: {
    label: string;
    left: number | string;
    right: number | string;
    /** Optional: "higher" means higher number wins, "lower" means lower wins */
    direction?: "higher" | "lower";
  }[];
  /** Optional left color */
  leftColor?: string;
  /** Optional right color */
  rightColor?: string;
}

export function StatComparison({
  leftLabel,
  rightLabel,
  stats,
  leftColor = "var(--color-brand, #3b82f6)",
  rightColor = "var(--color-accent-alt, #8b5cf6)",
}: StatComparisonProps) {
  return (
    <div className="stat-comparison" role="table" aria-label={`${leftLabel} vs ${rightLabel} comparison`}>
      <div className="stat-comparison-header" role="row">
        <span role="columnheader" style={{ color: leftColor, fontWeight: 700, fontSize: "var(--text-sm)" }}>
          {leftLabel}
        </span>
        <span role="columnheader" style={{ color: "var(--color-text-secondary)", fontSize: "var(--text-xs)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
          Stat
        </span>
        <span role="columnheader" style={{ color: rightColor, fontWeight: 700, fontSize: "var(--text-sm)", textAlign: "right" }}>
          {rightLabel}
        </span>
      </div>
      {stats.map((stat, i) => {
        const leftNum = typeof stat.left === "number" ? stat.left : parseFloat(String(stat.left));
        const rightNum = typeof stat.right === "number" ? stat.right : parseFloat(String(stat.right));
        const isHigherBetter = stat.direction !== "lower";
        const leftWins = isHigherBetter ? leftNum > rightNum : leftNum < rightNum;
        const rightWins = isHigherBetter ? rightNum > leftNum : rightNum < leftNum;
        const tie = leftNum === rightNum || isNaN(leftNum) || isNaN(rightNum);

        return (
          <div key={i} className="stat-comparison-row" role="row">
            <span
              role="cell"
              className="stat-comparison-value"
              style={{
                fontWeight: leftWins && !tie ? 700 : 400,
                color: leftWins && !tie ? leftColor : "var(--color-text)",
              }}
            >
              {stat.left}
            </span>
            <span role="cell" className="stat-comparison-label">
              {stat.label}
            </span>
            <span
              role="cell"
              className="stat-comparison-value"
              style={{
                textAlign: "right",
                fontWeight: rightWins && !tie ? 700 : 400,
                color: rightWins && !tie ? rightColor : "var(--color-text)",
              }}
            >
              {stat.right}
            </span>
          </div>
        );
      })}
    </div>
  );
}

export default StatComparison;
