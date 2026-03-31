"use client";

import { useEffect, useState } from "react";
import { usePathname, useSearchParams } from "next/navigation";

/**
 * Thin NProgress-style loading bar for Next.js route transitions.
 * Listens to pathname + searchParams changes to animate.
 */
export function LoadingBar() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    setLoading(true);
    setProgress(30);

    const t1 = setTimeout(() => setProgress(60), 100);
    const t2 = setTimeout(() => setProgress(90), 250);
    const t3 = setTimeout(() => {
      setProgress(100);
      setTimeout(() => {
        setLoading(false);
        setProgress(0);
      }, 200);
    }, 350);

    return () => {
      clearTimeout(t1);
      clearTimeout(t2);
      clearTimeout(t3);
    };
  }, [pathname, searchParams]);

  if (!loading && progress === 0) return null;

  return (
    <div
      className="loading-bar"
      role="progressbar"
      aria-valuenow={progress}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label="Page loading"
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        width: `${progress}%`,
        height: 3,
        background: "var(--color-brand, #cc0000)",
        zIndex: 9999,
        transition: loading
          ? "width 0.3s ease"
          : "width 0.15s ease, opacity 0.3s ease",
        opacity: loading ? 1 : 0,
        boxShadow: "0 0 8px var(--color-brand, #cc0000)",
      }}
    />
  );
}
