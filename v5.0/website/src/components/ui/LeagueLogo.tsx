"use client";

import { useMemo, useState } from "react";
import Image from "next/image";
import { getDisplayName, getLeagueLogoUrl, getSportIcon } from "@/lib/sports-config";
import { leagueMediaProxyUrl } from "@/lib/media";

interface LeagueLogoProps {
  sport: string;
  size?: number;
  className?: string;
}

export function LeagueLogo({ sport, size = 14, className }: LeagueLogoProps) {
  const [attempt, setAttempt] = useState(0);
  const sources = useMemo(() => {
    const fallback = getLeagueLogoUrl(sport);
    return [leagueMediaProxyUrl(sport), fallback].filter(Boolean) as string[];
  }, [sport]);
  const src = sources[attempt];

  if (!src) {
    return (
      <span
        aria-label={`${getDisplayName(sport)} icon`}
        className={className}
        style={{
          width: size,
          height: size,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: Math.max(10, Math.floor(size * 0.85)),
          lineHeight: 1,
        }}
      >
        {getSportIcon(sport)}
      </span>
    );
  }

  return (
    <Image
      src={src}
      alt={`${getDisplayName(sport)} league logo`}
      width={size}
      height={size}
      className={className}
      unoptimized
      onError={() => setAttempt((current) => current + 1)}
      style={{ borderRadius: 9999, objectFit: "contain", background: "#fff" }}
    />
  );
}