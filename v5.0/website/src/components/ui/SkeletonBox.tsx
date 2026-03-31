"use client";

interface SkeletonBoxProps {
  width?: string | number;
  height?: string | number;
  borderRadius?: string;
  className?: string;
  style?: React.CSSProperties;
}

/**
 * Reusable shimmer skeleton placeholder for loading states.
 */
export function SkeletonBox({
  width = "100%",
  height = "1rem",
  borderRadius = "var(--radius-md, 6px)",
  className = "",
  style,
}: SkeletonBoxProps) {
  return (
    <div
      className={`skeleton-box ${className}`}
      aria-hidden="true"
      style={{
        width,
        height,
        borderRadius,
        background: "var(--color-bg-3, #1e293b)",
        ...style,
      }}
    />
  );
}
