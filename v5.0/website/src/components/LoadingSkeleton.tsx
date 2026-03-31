import type { CSSProperties } from "react";

interface SkeletonProps {
  width?: string | number;
  height?: string | number;
  borderRadius?: string;
  className?: string;
  style?: CSSProperties;
}

export function Skeleton({ width, height, borderRadius, className, style }: SkeletonProps) {
  return (
    <div
      className={`skeleton${className ? ` ${className}` : ""}`}
      style={{ width, height, borderRadius, ...style }}
      aria-hidden="true"
    />
  );
}

export function SkeletonText({ lines = 3 }: { lines?: number }) {
  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--space-2)" }}
      aria-hidden="true"
    >
      {Array.from({ length: lines }, (_, i) => (
        <Skeleton key={i} height={14} width={i === lines - 1 ? "60%" : "100%"} />
      ))}
    </div>
  );
}

export function SkeletonCard({ rows = 4 }: { rows?: number }) {
  return (
    <div className="card" aria-hidden="true">
      <div className="card-header">
        <Skeleton width="40%" height={14} />
      </div>
      <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: "var(--space-3)" }}>
        {Array.from({ length: rows }, (_, i) => (
          <Skeleton key={i} height={36} />
        ))}
      </div>
    </div>
  );
}

export function SkeletonGameCard() {
  return (
    <div className="card game-card" aria-hidden="true">
      <div className="card-header">
        <Skeleton width={60} height={20} borderRadius="var(--radius-sm)" />
        <Skeleton width={50} height={14} />
      </div>
      <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: "var(--space-3)" }}>
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <Skeleton width="55%" height={18} />
          <Skeleton width={30} height={18} />
        </div>
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <Skeleton width="55%" height={18} />
          <Skeleton width={30} height={18} />
        </div>
        <Skeleton height={6} borderRadius="3px" />
      </div>
    </div>
  );
}

export function SkeletonTable({ rows = 8, cols = 5 }: { rows?: number; cols?: number }) {
  return (
    <div className="data-table-wrap" aria-hidden="true">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} style={{ display: "flex", gap: "var(--space-3)", padding: "var(--space-2) 0" }}>
          {Array.from({ length: cols }, (_, j) => (
            <Skeleton key={j} height={20} style={{ flex: j === 0 ? 2 : 1 }} />
          ))}
        </div>
      ))}
    </div>
  );
}
