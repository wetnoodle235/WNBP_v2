import type { ReactNode } from "react";

interface HeroRailProps {
  /** Left/wide primary column content */
  primary: ReactNode;
  /** Right/narrow secondary column content */
  secondary: ReactNode;
  className?: string;
}

/**
 * Two-column hero layout: primary (wider) + secondary (narrower).
 * Collapses to single-column on mobile.
 */
export function HeroRail({ primary, secondary, className }: HeroRailProps) {
  return (
    <div className={`hero-rail${className ? ` ${className}` : ""}`}>
      <div>{primary}</div>
      <div>{secondary}</div>
    </div>
  );
}
