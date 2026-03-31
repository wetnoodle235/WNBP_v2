"use client";

import { useEffect } from "react";

export default function StandingsError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    if (process.env.NODE_ENV === "development") console.error(error);
  }, [error]);

  return (
    <main
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "50vh",
        textAlign: "center",
        gap: "var(--space-4)",
        padding: "var(--space-8)",
      }}
    >
      <div aria-hidden="true" style={{ fontSize: "3rem" }}>🏅</div>
      <h1 style={{ fontSize: "var(--text-2xl)", fontWeight: 700 }}>
        Failed to load standings
      </h1>
      <p style={{ color: "var(--color-text-muted)", maxWidth: "40ch" }}>
        We couldn&apos;t load this page. This is usually temporary.
      </p>
      <button type="button" className="btn btn-primary" onClick={reset}>
        Try Again
      </button>
    </main>
  );
}
