"use client";

import BasketballCourt from "./BasketballCourt";
import FootballField from "./FootballField";
import BaseballDiamond from "./BaseballDiamond";
import HockeyRink from "./HockeyRink";
import SoccerPitch from "./SoccerPitch";
import TennisCourt from "./TennisCourt";
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
};

function normalizeSport(sport: string): string {
  const s = sport.toLowerCase().replace(/[^a-z0-9]/g, "");
  return SPORT_MAP[s] ?? s;
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

  switch (type) {
    case "basketball":
      return (
        <BasketballCourt
          className={className}
          width={width ?? 560}
          height={height ?? 360}
          homeColor={homeColor}
          awayColor={awayColor}
        />
      );

    case "football":
      return (
        <FootballField
          className={className}
          width={width ?? 600}
          height={height ?? 300}
          homeColor={homeColor}
          awayColor={awayColor}
          variant={(league as "nfl" | "ncaaf") ?? (sport.toLowerCase() === "ncaaf" ? "ncaaf" : "nfl")}
        />
      );

    case "baseball":
      return (
        <BaseballDiamond
          className={className}
          width={width ?? 420}
          height={height ?? 400}
          homeColor={homeColor}
          awayColor={awayColor}
        />
      );

    case "hockey":
      return (
        <HockeyRink
          className={className}
          width={width ?? 560}
          height={height ?? 280}
          homeColor={homeColor}
          awayColor={awayColor}
        />
      );

    case "soccer":
      return (
        <SoccerPitch
          className={className}
          width={width ?? 560}
          height={height ?? 340}
          homeColor={homeColor}
          awayColor={awayColor}
        />
      );

    case "tennis":
      return (
        <TennisCourt
          className={className}
          width={width ?? 320}
          height={height ?? 480}
          variant={surface ?? "hard"}
        />
      );

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

export { normalizeSport };
