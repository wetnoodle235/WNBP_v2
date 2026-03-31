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

/** A full row of skeleton text lines */
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

/** Skeleton card placeholder with optional header */
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

/** Pre-composed skeleton for a game card */
export function GameCardSkeleton() {
  return (
    <div className="card" aria-hidden="true">
      <div className="card-header" style={{ display: "flex", justifyContent: "space-between" }}>
        <Skeleton width={60} height={20} />
        <Skeleton width={50} height={14} />
      </div>
      <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: "var(--space-3)" }}>
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <Skeleton width="60%" height={18} />
          <Skeleton width={30} height={18} />
        </div>
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <Skeleton width="60%" height={18} />
          <Skeleton width={30} height={18} />
        </div>
        <Skeleton width="100%" height={8} borderRadius="4px" />
      </div>
    </div>
  );
}

/** Pre-composed skeleton for stat cards */
export function StatCardSkeleton() {
  return (
    <div className="stat-card" aria-hidden="true" style={{ display: "flex", flexDirection: "column", gap: "var(--space-2)" }}>
      <Skeleton width={80} height={14} />
      <Skeleton width={60} height={28} />
      <Skeleton width="100%" height={10} />
    </div>
  );
}
