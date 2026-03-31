"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { SPORTS, ALL_SPORT_KEYS, type SportKey } from "@/lib/sports";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import "./SportTabs.css";

type Props = {
  currentSport?: string;
  onSelect?: (sport: string) => void;
  baseUrl?: string;
  variant?: "tabs" | "pills";
};

export function SportTabs({
  currentSport,
  onSelect,
  baseUrl = "/games",
  variant = "tabs",
}: Props) {
  const pathname = usePathname();

  return (
    <div className={`sport-tabs ${variant}`}>
      {ALL_SPORT_KEYS.map((sport) => {
        const isActive = currentSport === sport || (pathname.includes(sport) && !currentSport);
        const href = `${baseUrl}/${sport}`;
        const displayName = getDisplayName(sport);
        const color = getSportColor(sport);

        return (
          <Link
            key={sport}
            href={href}
            className={`sport-tab ${isActive ? "active" : ""}`}
            style={
              isActive && variant === "tabs"
                ? { borderBottomColor: color, color }
                : variant === "pills"
                  ? {
                      backgroundColor: isActive ? color : "transparent",
                      color: isActive ? "var(--color-on-brand, #fff)" : "inherit",
                      borderColor: color,
                    }
                  : undefined
            }
            onClick={() => onSelect?.(sport)}
          >
            {displayName}
          </Link>
        );
      })}
    </div>
  );
}
