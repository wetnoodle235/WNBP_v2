"use client";

import { useEffect, useRef, useState } from "react";

/* ── Color thresholds ── */
function barColor(pct: number): string {
  if (pct < 50) return "var(--cb-red, #ef4444)";
  if (pct < 65) return "var(--cb-yellow, #eab308)";
  if (pct < 80) return "var(--cb-gold, #f59e0b)";
  return "var(--cb-green, #22c55e)";
}

function barGradient(pct: number): string {
  if (pct < 50) return "linear-gradient(90deg, #dc2626, #ef4444)";
  if (pct < 65) return "linear-gradient(90deg, #ca8a04, #eab308)";
  if (pct < 80) return "linear-gradient(90deg, #d97706, #fbbf24)";
  return "linear-gradient(90deg, #16a34a, #22c55e)";
}

export interface ConfidenceBarProps {
  /** Probability 0–1 (e.g. 0.72) */
  value: number;
  /** Optional 68% CI bounds [lower, upper] as 0–1 */
  ci68?: [number, number];
  /** Optional 95% CI bounds [lower, upper] as 0–1 */
  ci95?: [number, number];
  /** Bar height in px */
  height?: number;
  /** Show label inside bar */
  showLabel?: boolean;
  /** Compact mode — smaller text, no label padding */
  compact?: boolean;
}

export function ConfidenceBar({
  value,
  ci68,
  ci95,
  height = 22,
  showLabel = true,
  compact = false,
}: ConfidenceBarProps) {
  const pct = Math.min(Math.max(value * 100, 0), 100);
  const [animated, setAnimated] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setAnimated(true); obs.disconnect(); } },
      { threshold: 0.2 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  const fontSize = compact ? "10px" : "11px";

  return (
    <div
      ref={ref}
      className="confidence-bar"
      role="meter"
      aria-valuenow={pct}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={`Confidence ${pct.toFixed(0)}%${ci68 ? `, 68% CI ${(ci68[0]*100).toFixed(0)}–${(ci68[1]*100).toFixed(0)}%` : ""}${ci95 ? `, 95% CI ${(ci95[0]*100).toFixed(0)}–${(ci95[1]*100).toFixed(0)}%` : ""}`}
      style={{ height, position: "relative", borderRadius: 999, overflow: "hidden", background: "var(--color-surface-2, #1e1e2e)" }}
    >
      {/* 95% CI band */}
      {ci95 && (
        <div
          className="confidence-bar-ci95"
          aria-hidden="true"
          title={`95% confidence interval: ${(ci95[0] * 100).toFixed(0)}%–${(ci95[1] * 100).toFixed(0)}%`}
          style={{
            position: "absolute", top: 0, bottom: 0,
            left: `${ci95[0] * 100}%`,
            width: `${(ci95[1] - ci95[0]) * 100}%`,
            background: "rgba(99,102,241,0.12)",
            borderRadius: 999,
            transition: "opacity 0.4s",
            opacity: animated ? 1 : 0,
          }}
        />
      )}

      {/* 68% CI band */}
      {ci68 && (
        <div
          className="confidence-bar-ci68"
          aria-hidden="true"
          title={`68% confidence interval: ${(ci68[0] * 100).toFixed(0)}%–${(ci68[1] * 100).toFixed(0)}%`}
          style={{
            position: "absolute", top: 0, bottom: 0,
            left: `${ci68[0] * 100}%`,
            width: `${(ci68[1] - ci68[0]) * 100}%`,
            background: "rgba(99,102,241,0.22)",
            borderRadius: 999,
            transition: "opacity 0.4s 0.1s",
            opacity: animated ? 1 : 0,
          }}
        />
      )}

      {/* Main fill bar */}
      <div
        className="confidence-bar-fill"
        style={{
          position: "absolute", top: 0, bottom: 0, left: 0,
          width: animated ? `${pct}%` : "0%",
          background: barGradient(pct),
          borderRadius: 999,
          transition: "width 0.7s cubic-bezier(.4,0,.2,1)",
          boxShadow: `0 0 8px ${barColor(pct)}44`,
        }}
      />

      {/* CI tick marks */}
      {ci68 && (
        <>
          <CITick position={ci68[0]} height={height} animated={animated} opacity={0.6} delay="0.5s" />
          <CITick position={ci68[1]} height={height} animated={animated} opacity={0.6} delay="0.5s" />
        </>
      )}
      {ci95 && (
        <>
          <CITick position={ci95[0]} height={height} animated={animated} opacity={0.35} delay="0.6s" />
          <CITick position={ci95[1]} height={height} animated={animated} opacity={0.35} delay="0.6s" />
        </>
      )}

      {/* Percentage label */}
      {showLabel && (
        <span
          style={{
            position: "relative", zIndex: 2,
            display: "flex", alignItems: "center", height: "100%",
            paddingLeft: compact ? 6 : 8, paddingRight: compact ? 6 : 8,
            fontSize,
            fontWeight: 700,
            color: "#fff",
            textShadow: "0 1px 2px rgba(0,0,0,0.5)",
            fontVariantNumeric: "tabular-nums",
            whiteSpace: "nowrap",
          }}
        >
          {pct.toFixed(0)}%
        </span>
      )}
    </div>
  );
}

/* Small tick mark for CI boundaries */
function CITick({
  position,
  height,
  animated,
  opacity,
  delay,
}: {
  position: number;
  height: number;
  animated: boolean;
  opacity: number;
  delay: string;
}) {
  return (
    <div
      style={{
        position: "absolute",
        left: `${position * 100}%`,
        top: 2,
        width: 1.5,
        height: height - 4,
        background: `rgba(255,255,255,${opacity})`,
        borderRadius: 1,
        transition: `opacity 0.3s ${delay}`,
        opacity: animated ? 1 : 0,
        zIndex: 3,
      }}
    />
  );
}

export default ConfidenceBar;
