"use client";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <html lang="en">
      <body>
        <div
          style={{
            minHeight: "100vh",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            padding: "2rem",
            textAlign: "center",
            fontFamily: "system-ui, -apple-system, sans-serif",
            background: "#0f172a",
            color: "#e2e8f0",
          }}
        >
          <div style={{ fontSize: "4rem", marginBottom: "1rem" }} aria-hidden="true">🔥</div>
          <h1 style={{ fontSize: "1.75rem", fontWeight: 700, marginBottom: "0.5rem" }}>
            Application Error
          </h1>
          <p style={{ opacity: 0.7, maxWidth: "40ch", marginBottom: "1.5rem" }}>
            A critical error occurred. Please try again or contact support if the issue persists.
          </p>
          {error.digest && (
            <p style={{ fontSize: "0.75rem", opacity: 0.5, fontFamily: "monospace", marginBottom: "1rem" }}>
              Error ID: {error.digest}
            </p>
          )}
          <div style={{ display: "flex", gap: "0.75rem" }}>
            <button
              onClick={reset}
              style={{
                padding: "0.625rem 1.5rem",
                background: "#3b82f6",
                color: "#fff",
                border: "none",
                borderRadius: "0.5rem",
                fontWeight: 600,
                cursor: "pointer",
                fontSize: "0.9rem",
              }}
            >
              Try Again
            </button>
            <button
              onClick={() => (window.location.href = "/")}
              style={{
                padding: "0.625rem 1.5rem",
                background: "transparent",
                color: "#94a3b8",
                border: "1px solid #334155",
                borderRadius: "0.5rem",
                fontWeight: 600,
                cursor: "pointer",
                fontSize: "0.9rem",
              }}
            >
              Go Home
            </button>
          </div>
        </div>
      </body>
    </html>
  );
}
