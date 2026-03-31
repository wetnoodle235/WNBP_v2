"use client";

import { useEffect } from "react";
import { usePathname } from "next/navigation";

/**
 * Restores scroll position to URL hash fragment on route change.
 * Next.js doesn't handle this natively for client transitions.
 */
export function ScrollToHash() {
  const pathname = usePathname();

  useEffect(() => {
    const hash = window.location.hash;
    if (!hash) return;

    // Small delay to let the page render
    const timer = setTimeout(() => {
      const el = document.querySelector(hash);
      if (el) {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }, 100);

    return () => clearTimeout(timer);
  }, [pathname]);

  return null;
}
