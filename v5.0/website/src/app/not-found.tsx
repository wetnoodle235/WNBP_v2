import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Page Not Found — WNBP",
  robots: { index: false, follow: false },
};

const SPORT_SUGGESTIONS = [
  { href: "/nba", label: "NBA Predictions", icon: "🏀" },
  { href: "/mlb", label: "MLB Predictions", icon: "⚾" },
  { href: "/nfl", label: "NFL Predictions", icon: "🏈" },
  { href: "/nhl", label: "NHL Predictions", icon: "🏒" },
  { href: "/epl", label: "EPL Predictions", icon: "⚽" },
  { href: "/predictions", label: "All Predictions", icon: "📊" },
];

export default function NotFound() {
  return (
    <main
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "60vh",
        textAlign: "center",
        gap: "var(--space-6)",
        padding: "var(--space-8)",
      }}
    >
      <div className="not-found-animation" aria-hidden="true">
        <span style={{ fontSize: "6rem", lineHeight: 1, display: "block" }}>🏈</span>
      </div>
      <h1 style={{ fontSize: "var(--text-5xl, 3rem)", fontWeight: 800, letterSpacing: "-0.02em" }}>
        <span style={{ color: "var(--color-brand)" }}>4</span>
        <span>0</span>
        <span style={{ color: "var(--color-brand)" }}>4</span>
      </h1>
      <p style={{ fontSize: "var(--text-lg)", opacity: 0.7, maxWidth: "32ch" }}>
        Looks like this page fumbled. The content you&apos;re looking for doesn&apos;t exist or has moved.
      </p>

      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
        gap: "var(--space-3)",
        width: "100%",
        maxWidth: "600px",
        marginTop: "var(--space-4)",
      }}>
        {SPORT_SUGGESTIONS.map(({ href, label, icon }) => (
          <Link
            key={href}
            href={href}
            className="card"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "var(--space-2)",
              padding: "var(--space-3) var(--space-4)",
              fontWeight: 600,
              fontSize: "var(--text-sm)",
              transition: "transform 0.15s, box-shadow 0.15s",
            }}
          >
            <span aria-hidden="true">{icon}</span>
            {label}
          </Link>
        ))}
      </div>

      <Link
        href="/"
        className="btn btn-primary"
        style={{ marginTop: "var(--space-4)" }}
      >
        ← Back to Home
      </Link>
    </main>
  );
}
