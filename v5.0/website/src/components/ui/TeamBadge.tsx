"use client";

import { useState, useMemo } from "react";
import Image from "next/image";
import Link from "next/link";
import { toTeamSlug } from "@/lib/team-slugs";

function espnTeamLogoUrl(teamId: string | number, sport: string): string {
  return `https://a.espncdn.com/i/teamlogos/${sport}/500/${teamId}.png`;
}

function espnTeamLogoFromName(name: string, abbrev: string | undefined, sport: string): string {
  const slug = toTeamSlug(name, sport, abbrev);
  return `https://a.espncdn.com/i/teamlogos/${sport}/500/${slug}.png`;
}

interface TeamBadgeProps {
  teamId?: string | number;
  name: string;
  abbrev?: string;
  sport?: string;
  logoUrl?: string;
  href?: string;
  size?: "sm" | "md" | "lg";
  /** Show only the logo, no name text */
  logoOnly?: boolean;
  className?: string;
}

const LOGO_SIZE: Record<"sm" | "md" | "lg", number> = { sm: 20, md: 28, lg: 40 };

export function TeamBadge({
  teamId,
  name,
  abbrev,
  sport,
  logoUrl,
  href,
  size = "md",
  logoOnly = false,
  className,
}: TeamBadgeProps) {
  const px = LOGO_SIZE[size];
  const [imgError, setImgError] = useState(false);

  const imgSrc = useMemo(() => {
    if (logoUrl) return logoUrl;
    if (teamId && sport) return espnTeamLogoUrl(teamId, sport);
    if (sport) return espnTeamLogoFromName(name, abbrev, sport);
    return undefined;
  }, [logoUrl, teamId, sport, name, abbrev]);

  const showImg = imgSrc && !imgError;

  const inner = (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: "var(--space-2)",
        lineHeight: 1,
      }}
    >
      {showImg ? (
        <span
          style={{
            width: px,
            height: px,
            flexShrink: 0,
            position: "relative",
            display: "inline-flex",
          }}
        >
          <Image
            src={imgSrc}
            alt={name}
            width={px}
            height={px}
            style={{ objectFit: "contain" }}
            unoptimized
            onError={() => setImgError(true)}
          />
        </span>
      ) : (
        <span
          aria-hidden="true"
          style={{
            width: px,
            height: px,
            borderRadius: "var(--radius-sm)",
            background: "var(--color-bg-3)",
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: px * 0.38,
            fontWeight: "var(--fw-bold)",
            color: "var(--color-text-secondary)",
            flexShrink: 0,
          }}
        >
          {(abbrev ?? name).slice(0, 3).toUpperCase()}
        </span>
      )}
      {!logoOnly && (
        <span
          style={{
            fontSize: size === "sm" ? "var(--text-xs)" : "var(--text-sm)",
            fontWeight: "var(--fw-semibold)",
            color: "var(--color-text)",
          }}
        >
          {abbrev ?? name}
        </span>
      )}
    </span>
  );

  if (href) {
    return (
      <Link href={href} className={className} style={{ textDecoration: "none" }}>
        {inner}
      </Link>
    );
  }

  return <span className={className}>{inner}</span>;
}
