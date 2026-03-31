"use client";

import { useEffect, useState } from "react";

const MAX_RETRIES = 3;

interface ErrorProps {
  error: Error & { digest?: string };
  reset: () => void;
}

export default function ErrorPage({ error, reset }: ErrorProps) {
  const [showDetails, setShowDetails] = useState(false);
  const [retryCount, setRetryCount] = useState(0);
  const exhausted = retryCount >= MAX_RETRIES;

  useEffect(() => {
    if (process.env.NODE_ENV === "development") {
      console.error(error);
    }
  }, [error]);

  const handleRetry = () => {
    if (exhausted) return;
    setRetryCount((c) => c + 1);
    reset();
  };

  return (
    <main
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "60vh",
        textAlign: "center",
        gap: "var(--space-4)",
        padding: "var(--space-8)",
      }}
    >
      <div aria-hidden="true" style={{ fontSize: "4rem", lineHeight: 1 }}>⚠️</div>
      <h1 style={{ fontSize: "var(--text-3xl)", fontWeight: 700 }}>Something went wrong</h1>
      <p style={{ fontSize: "var(--text-md)", opacity: 0.7, maxWidth: "40ch" }}>
        {exhausted
          ? "We\u2019ve tried several times but the error persists. Please refresh the page or try again later."
          : "An unexpected error occurred. This has been logged and we\u2019re working on it."}
      </p>

      {error.digest && (
        <p style={{
          fontSize: "var(--text-xs)",
          color: "var(--color-text-muted)",
          fontFamily: "monospace",
          background: "var(--color-bg-3)",
          padding: "var(--space-2) var(--space-4)",
          borderRadius: "var(--radius-md)",
        }}>
          Error ID: {error.digest}
        </p>
      )}

      <div style={{ display: "flex", gap: "var(--space-3)", marginTop: "var(--space-2)" }}>
        {exhausted ? (
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="btn btn-primary"
          >
            Refresh Page
          </button>
        ) : (
          <button
            type="button"
            onClick={handleRetry}
            className="btn btn-primary"
            aria-label={`Try again (attempt ${retryCount + 1} of ${MAX_RETRIES})`}
          >
            Try Again{retryCount > 0 ? ` (${retryCount}/${MAX_RETRIES})` : ""}
          </button>
        )}
        <button
          type="button"
          onClick={() => setShowDetails((v) => !v)}
          className="btn btn-ghost"
          aria-expanded={showDetails}
        >
          {showDetails ? "Hide Details" : "Show Details"}
        </button>
      </div>

      {showDetails && (
        <div style={{
          marginTop: "var(--space-4)",
          padding: "var(--space-4)",
          background: "var(--color-bg-3)",
          borderRadius: "var(--radius-md)",
          textAlign: "left",
          fontSize: "var(--text-xs)",
          fontFamily: "monospace",
          maxWidth: "600px",
          width: "100%",
          overflow: "auto",
          maxHeight: "200px",
          color: "var(--color-loss)",
        }}>
          <p style={{ fontWeight: 700, marginBottom: "var(--space-2)" }}>{error.name}: {error.message}</p>
          {error.stack && (
            <pre style={{ whiteSpace: "pre-wrap", wordBreak: "break-all", opacity: 0.7 }}>
              {error.stack.split("\n").slice(1, 8).join("\n")}
            </pre>
          )}
        </div>
      )}
    </main>
  );
}
