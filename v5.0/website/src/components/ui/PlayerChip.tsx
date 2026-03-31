"use client";

import { useState, useMemo } from "react";
import Image from "next/image";
import Link from "next/link";
import type { ReactNode } from "react";

interface PlayerChipProps {
  playerId?: string | number;
  name: string;
  sport?: string;
  /** Override the generated headshot URL — pass a known-good ESPN URL to skip the lookup */
  headshotUrl?: string;
  team?: string;
  /** Optional trailing element (e.g. a stat badge) */
  trailing?: ReactNode;
  href?: string;
  size?: "sm" | "md" | "lg";
  className?: string;
}

const HEADSHOT_SIZE: Record<"sm" | "md" | "lg", number> = { sm: 24, md: 32, lg: 48 };

export function PlayerChip({
  playerId,
  name,
  sport,
  headshotUrl,
  team,
  trailing,
  href,
  size = "md",
  className,
}: PlayerChipProps) {
  const px = HEADSHOT_SIZE[size];
  const [imgError, setImgError] = useState(false);

  const resolvedUrl = useMemo(() => {
    if (headshotUrl) return headshotUrl;
    if (playerId && sport)
      return `https://a.espncdn.com/i/headshots/${sport}/players/full/${playerId}.png`;
    return null;
  }, [headshotUrl, playerId, sport]);

  const showImg = resolvedUrl && !imgError;


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
            borderRadius: "50%",
            overflow: "hidden",
            flexShrink: 0,
            background: "var(--color-bg-3)",
            position: "relative",
            display: "inline-block",
          }}
        >
          <Image
            src={resolvedUrl!}
            alt={name}
            width={px}
            height={px}
            style={{ objectFit: "cover" }}
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
            borderRadius: "50%",
            background: "var(--color-brand-subtle)",
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: px * 0.4,
            fontWeight: "var(--fw-bold)",
            color: "var(--color-brand)",
            flexShrink: 0,
          }}
        >
          {name[0]?.toUpperCase()}
        </span>
      )}
      <span style={{ display: "flex", flexDirection: "column", gap: 1 }}>
        <span
          style={{
            fontSize: size === "sm" ? "var(--text-xs)" : "var(--text-sm)",
            fontWeight: "var(--fw-semibold)",
            color: "var(--color-text)",
          }}
        >
          {name}
        </span>
        {team && (
          <span
            style={{
              fontSize: "var(--text-xs)",
              color: "var(--color-text-muted)",
            }}
          >
            {team}
          </span>
        )}
      </span>
      {trailing && (
        <span style={{ marginLeft: "auto", flexShrink: 0 }}>{trailing}</span>
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
