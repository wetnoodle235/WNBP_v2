import Link from "next/link";
import type { ReactNode } from "react";

interface PremiumTeaserProps {
  /** Short copy describing what's locked */
  message?: string;
  /** CTA button label */
  ctaLabel?: string;
  /** CTA destination — defaults to /pricing */
  ctaHref?: string;
  /** Optional icon/emoji to prefix the message */
  icon?: ReactNode;
  className?: string;
}

export function PremiumTeaser({
  message = "Upgrade to Premium to see full predictions, confidence brackets, and advanced model data.",
  ctaLabel = "Get Premium",
  ctaHref = "/pricing",
  icon,
  className,
}: PremiumTeaserProps) {
  return (
    <aside
      className={`premium-teaser${className ? ` ${className}` : ""}`}
      aria-label="Premium feature"
    >
      <p className="premium-teaser-text">
        {icon && <span aria-hidden="true">{icon} </span>}
        {message}
      </p>
      <Link href={ctaHref} className="premium-teaser-cta">
        {ctaLabel}
      </Link>
    </aside>
  );
}

/** Inline text teaser — minimal version for inside tables/cards */
export function PremiumLock({ className }: { className?: string }) {
  return (
    <span
      className={`badge badge-premium${className ? ` ${className}` : ""}`}
      title="Premium feature — upgrade to unlock"
      aria-label="Premium — locked"
    >
      ★ Premium
    </span>
  );
}
