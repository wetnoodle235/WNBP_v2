"use client";

import { useEffect } from "react";
import { usePathname } from "next/navigation";
import { useRecentlyViewed } from "@/lib/hooks";

const ROUTE_TITLES: Record<string, string> = {
  "/": "Home",
  "/predictions": "Predictions",
  "/opportunities": "Props",
  "/odds": "Odds",
  "/live": "Live Games",
  "/news": "News",
  "/standings": "Standings",
  "/stats": "Stats",
  "/players": "Players",
  "/teams": "Teams",
  "/season": "Season Sim",
  "/paper": "Paper Trading",
  "/autobets": "AutoBets",
  "/model-health": "Model Health",
  "/dashboard": "Dashboard",
  "/account": "Account",
  "/login": "Sign In",
  "/favorites": "Favorites",
  "/api-docs": "API Docs",
  "/pricing": "Pricing",
};

/** Silently records the current page for "recently viewed" */
export function PageTracker() {
  const pathname = usePathname();
  const { addPage } = useRecentlyViewed();

  useEffect(() => {
    if (!pathname) return;
    const title = ROUTE_TITLES[pathname] ?? pathname.split("/").pop()?.replace(/-/g, " ") ?? pathname;
    addPage(pathname, title.charAt(0).toUpperCase() + title.slice(1));
  }, [pathname, addPage]);

  return null;
}
