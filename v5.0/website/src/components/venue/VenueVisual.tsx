"use client";

import Image from "next/image";
import F1Circuit from "./F1Circuit";
import IndyCarTrack from "./IndyCarTrack";
import GolfHole from "./GolfHole";

export interface VenueVisualProps {
  sport: string;
  width?: number;
  height?: number;
  homeColor?: string;
  awayColor?: string;
  /** Venue name — used to pick correct circuit/track */
  venueName?: string;
  /** For tennis: surface type */
  surface?: "hard" | "clay" | "grass";
  /** For football: league variant */
  league?: "nfl" | "ncaaf";
  /** Whether to animate (racing circuits) */
  animate?: boolean;
  className?: string;
}

const SPORT_MAP: Record<string, string> = {
  nba: "basketball",
  ncaab: "basketball",
  ncaaw: "basketball",
  wnba: "basketball",
  nfl: "football",
  ncaaf: "football",
  mlb: "baseball",
  nhl: "hockey",
  mls: "soccer",
  nwsl: "soccer",
  epl: "soccer",
  bundesliga: "soccer",
  laliga: "soccer",
  seriea: "soccer",
  ligue1: "soccer",
  ligamx: "soccer",
  uefachampionsleague: "soccer",
  ucl: "soccer",
  atp: "tennis",
  wta: "tennis",
  f1: "f1",
  formula1: "f1",
  indycar: "indycar",
  pga: "golf",
  lpga: "golf",
  golf: "golf",
  // Esports
  csgo: "cs2",
  cs2: "cs2",
  lol: "esports",
  leagueoflegends: "esports",
  dota2: "esports",
  valorant: "esports",
  overwatch: "esports",
  esports: "esports",
};

/** Map normalized sport type → sportypy/awpy/fastf1-generated PNG in /public/venues/ */
function sportToImage(
  type: string,
  opts: {
    league?: string;
    surface?: "hard" | "clay" | "grass";
    sport?: string;
    venueName?: string;
  }
): string | null {
  switch (type) {
    case "basketball": {
      const s = (opts.sport ?? "").toLowerCase();
      if (s === "wnba") return "/venues/wnba_court.png";
      if (s === "ncaab" || s === "ncaaw") return "/venues/ncaa_basketball_court.png";
      return "/venues/nba_court.png";
    }
    case "football": {
      const variant = opts.league ?? opts.sport?.toLowerCase() ?? "nfl";
      return variant === "ncaaf" ? "/venues/ncaa_football_field.png" : "/venues/nfl_field.png";
    }
    case "baseball":
      return "/venues/mlb_field.png";
    case "hockey":
      return "/venues/nhl_rink.png";
    case "soccer":
      return "/venues/soccer_pitch.png";
    case "tennis":
      return `/venues/tennis_${opts.surface ?? "hard"}.png`;
    case "f1": {
      // fastf1-generated track maps (speed-colored telemetry)
      const venue = (opts.venueName ?? "").toLowerCase().replace(/\s+/g, "_");
      const F1_TRACKS: Record<string, string> = {
        bahrain: "/venues/f1_bahrain.png",
        sakhir: "/venues/f1_bahrain.png",
      };
      return F1_TRACKS[venue] ?? null; // falls back to SVG component if no image
    }
    case "cs2": {
      // awpy-generated CS2 map images
      const venue = (opts.venueName ?? "de_dust2").toLowerCase().replace(/\s+/g, "_");
      const CS2_MAPS: Record<string, string> = {
        dust2: "/venues/cs2_de_dust2.png",
        de_dust2: "/venues/cs2_de_dust2.png",
        mirage: "/venues/cs2_de_mirage.png",
        de_mirage: "/venues/cs2_de_mirage.png",
        inferno: "/venues/cs2_de_inferno.png",
        de_inferno: "/venues/cs2_de_inferno.png",
        nuke: "/venues/cs2_de_nuke.png",
        de_nuke: "/venues/cs2_de_nuke.png",
        ancient: "/venues/cs2_de_ancient.png",
        de_ancient: "/venues/cs2_de_ancient.png",
        anubis: "/venues/cs2_de_anubis.png",
        de_anubis: "/venues/cs2_de_anubis.png",
        vertigo: "/venues/cs2_de_vertigo.png",
        de_vertigo: "/venues/cs2_de_vertigo.png",
        overpass: "/venues/cs2_de_overpass.png",
        de_overpass: "/venues/cs2_de_overpass.png",
        train: "/venues/cs2_de_train.png",
        de_train: "/venues/cs2_de_train.png",
        italy: "/venues/cs2_cs_italy.png",
        cs_italy: "/venues/cs2_cs_italy.png",
      };
      return CS2_MAPS[venue] ?? "/venues/cs2_de_dust2.png";
    }
    default:
      return null;
  }
}

export function normalizeSport(sport: string): string {
  const s = sport.toLowerCase().replace(/[^a-z0-9]/g, "");
  return SPORT_MAP[s] ?? s;
}

/** Sportypy-generated venue image with optional team-color gradient overlays */
function VenueImage({
  src,
  alt,
  width,
  height,
  homeColor,
  awayColor,
  className,
}: {
  src: string;
  alt: string;
  width: number;
  height: number;
  homeColor?: string;
  awayColor?: string;
  className?: string;
}) {
  return (
    <div
      className={className}
      style={{
        position: "relative",
        width,
        height,
        borderRadius: "var(--radius-md, 8px)",
        overflow: "hidden",
        display: "block",
      }}
    >
      <Image
        src={src}
        alt={alt}
        fill
        style={{ objectFit: "cover" }}
        sizes={`${width}px`}
        priority={false}
      />
      {/* Away (left) team color accent */}
      {awayColor && (
        <div
          aria-hidden="true"
          style={{
            position: "absolute",
            inset: 0,
            background: `linear-gradient(to right, ${awayColor}55 0%, transparent 35%)`,
            pointerEvents: "none",
          }}
        />
      )}
      {/* Home (right) team color accent */}
      {homeColor && (
        <div
          aria-hidden="true"
          style={{
            position: "absolute",
            inset: 0,
            background: `linear-gradient(to left, ${homeColor}55 0%, transparent 35%)`,
            pointerEvents: "none",
          }}
        />
      )}
    </div>
  );
}

export default function VenueVisual({
  sport,
  width,
  height,
  homeColor,
  awayColor,
  venueName = "",
  surface,
  league,
  animate = false,
  className,
}: VenueVisualProps) {
  const type = normalizeSport(sport);
  const imgSrc = sportToImage(type, { league, surface, sport, venueName });

  if (imgSrc) {
    const defaultSizes: Record<string, { w: number; h: number }> = {
      basketball: { w: 560, h: 320 },
      football:   { w: 600, h: 300 },
      baseball:   { w: 440, h: 420 },
      hockey:     { w: 560, h: 280 },
      soccer:     { w: 560, h: 360 },
      tennis:     { w: 420, h: 260 },
      f1:         { w: 560, h: 420 },
      cs2:        { w: 420, h: 420 },
      esports:    { w: 420, h: 420 },
    };
    const def = defaultSizes[type] ?? { w: 560, h: 320 };
    return (
      <VenueImage
        src={imgSrc}
        alt={`${type} venue diagram`}
        width={width ?? def.w}
        height={height ?? def.h}
        homeColor={homeColor}
        awayColor={awayColor}
        className={className}
      />
    );
  }

  switch (type) {
    case "f1":
      return (
        <F1Circuit
          className={className}
          width={width ?? 300}
          height={height ?? 240}
          circuitKey={venueName || "bahrain"}
          animate={animate}
        />
      );
    case "indycar":
      return (
        <IndyCarTrack
          className={className}
          width={width ?? 280}
          height={height ?? 220}
          trackName={venueName}
        />
      );
    case "golf":
      return (
        <GolfHole
          className={className}
          width={width ?? 180}
          height={height ?? 300}
          par={4}
        />
      );
    default:
      return null;
  }
}
