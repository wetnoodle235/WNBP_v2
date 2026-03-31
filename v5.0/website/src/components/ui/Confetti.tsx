"use client";

import { useEffect, useRef, useCallback } from "react";

interface ConfettiProps {
  /** Whether to fire confetti */
  active: boolean;
  /** Number of confetti pieces */
  count?: number;
  /** Duration in ms */
  duration?: number;
}

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4"];

/**
 * Lightweight CSS-only confetti burst. Zero dependencies.
 * Fires once when `active` transitions to true.
 * Respects prefers-reduced-motion.
 */
export function Confetti({ active, count = 50, duration = 2500 }: ConfettiProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const prevActive = useRef(false);

  const fire = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

    container.innerHTML = "";
    for (let i = 0; i < count; i++) {
      const piece = document.createElement("span");
      piece.className = "confetti-piece";
      const color = COLORS[Math.floor(Math.random() * COLORS.length)];
      const x = (Math.random() - 0.5) * 400;
      const y = -(Math.random() * 300 + 100);
      const rot = Math.random() * 720 - 360;
      const size = Math.random() * 6 + 4;
      const delay = Math.random() * 300;

      piece.style.cssText = `
        position: absolute;
        width: ${size}px;
        height: ${size * 0.6}px;
        background: ${color};
        border-radius: ${Math.random() > 0.5 ? "50%" : "1px"};
        left: 50%;
        top: 50%;
        pointer-events: none;
        animation: confetti-fall ${duration}ms ease-out ${delay}ms forwards;
        --confetti-x: ${x}px;
        --confetti-y: ${y}px;
        --confetti-rot: ${rot}deg;
      `;
      container.appendChild(piece);
    }

    setTimeout(() => {
      if (container) container.innerHTML = "";
    }, duration + 400);
  }, [count, duration]);

  useEffect(() => {
    if (active && !prevActive.current) {
      fire();
    }
    prevActive.current = active;
  }, [active, fire]);

  return (
    <div
      ref={containerRef}
      aria-hidden="true"
      style={{
        position: "fixed",
        top: "40%",
        left: "50%",
        transform: "translate(-50%, -50%)",
        pointerEvents: "none",
        zIndex: 9999,
      }}
    />
  );
}
