import type { Metadata, Viewport } from "next";
import Link from "next/link";
import { Plus_Jakarta_Sans } from "next/font/google";
import { Suspense } from "react";
import "./globals.css";
import { Providers } from "@/components/Providers";
import { ClientLayout } from "@/components/ClientLayout";
import { buildSiteJsonLd } from "@/lib/seo";
import { SPORT_CATEGORIES, getDisplayName } from "@/lib/sports-config";

const plusJakarta = Plus_Jakarta_Sans({
  subsets: ["latin"],
  variable: "--font-plus-jakarta",
  weight: ["400", "500", "600", "700", "800"],
  display: "swap",
});

export const metadata: Metadata = {
  title: {
    default: "WNBP – WetNoodlesBestPicks",
    template: "%s | WNBP",
  },
  description:
    "Data-driven sports predictions covering NBA, MLB, WNBA, NFL, NHL, NCAAB, NCAAF, MLS, EPL, ATP, WTA, PGA, MMA, F1, CS2 and more. Premium picks, live odds, and performance analytics.",
  robots: { index: true, follow: true },
  icons: {
    icon: "/favicon.svg",
    apple: "/apple-touch-icon.png",
  },
  manifest: "/manifest.json",
  openGraph: {
    type: "website",
    siteName: "WNBP – WetNoodlesBestPicks",
    title: "WNBP – WetNoodlesBestPicks",
    description: "Data-driven sports predictions: NBA, MLB, WNBA, NFL, NHL, soccer, tennis, golf, MMA, F1, and esports.",
    url: "https://wetnoodlesbestpicks.com",
    images: [{
      url: "https://wetnoodlesbestpicks.com/og-image.png",
      width: 1200,
      height: 630,
      alt: "WNBP – WetNoodlesBestPicks",
    }],
    locale: "en_US",
  },
  twitter: {
    card: "summary_large_image",
    title: "WNBP – WetNoodlesBestPicks",
    description: "Data-driven sports predictions covering 20+ leagues. Premium picks, live odds, and analytics.",
    images: ["https://wetnoodlesbestpicks.com/og-image.png"],
  },
  metadataBase: new URL(process.env.NEXT_PUBLIC_SITE_URL ?? "https://wetnoodlesbestpicks.com"),
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#3b82f6" },
    { media: "(prefers-color-scheme: dark)", color: "#0f172a" },
  ],
};

const ALL_SPORTS = SPORT_CATEGORIES.flatMap((cat) => cat.sports);

const FOOTER_NAV = [
  {
    title: "Sports",
    links: ALL_SPORTS.map((sport) => ({ href: `/${sport}`, label: getDisplayName(sport) })),
  },
  {
    title: "Tools",
    links: [
      { href: "/predictions", label: "Predictions" },
      { href: "/odds", label: "Odds" },
      { href: "/live", label: "Live Scores" },
      { href: "/stats", label: "Statistics" },
      { href: "/season", label: "Season Sim" },
      { href: "/opportunities", label: "Prop Opportunities" },
      { href: "/leaderboard", label: "Model Leaderboard" },
    ],
  },
  {
    title: "Community",
    links: [
      { href: "/news", label: "News" },
      { href: "/standings", label: "Standings" },
      { href: "/players", label: "Players" },
      { href: "/teams", label: "Teams" },
      { href: "/ladder", label: "Leaderboard" },
    ],
  },
  {
    title: "Account",
    links: [
      { href: "/pricing", label: "Pricing" },
      { href: "/dashboard", label: "My Account" },
      { href: "/favorites", label: "Favorites" },
      { href: "/paper", label: "Paper Trading" },
      { href: "/autobets", label: "AutoBets" },
      { href: "/login", label: "Sign In" },
    ],
  },
  {
    title: "Resources",
    links: [
      { href: "/model-health", label: "Model Status" },
      { href: "/season", label: "Season Simulator" },
      { href: "/dashboard", label: "Dashboard" },
    ],
  },
];

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const currentYear = new Date().getFullYear();

  return (
    <html lang="en" className={plusJakarta.variable} suppressHydrationWarning data-scroll-behavior="smooth">
      <head>
        {/* Performance: DNS prefetch & preconnect for API domain */}
        <link rel="dns-prefetch" href="//wetnoodlesbestpicks.com" />
        <link rel="preconnect" href="https://wetnoodlesbestpicks.com" crossOrigin="anonymous" />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: buildSiteJsonLd() }}
        />
      </head>
      <body>
        <Providers>
          {/* Skip to content — a11y */}
          <a href="#main-content" className="skip-to-content">
            Skip to main content
          </a>

          <div className="app-shell">
            <ClientLayout />
            <div className="page-area">
              <main id="main-content" className="main-content">
                <Suspense>{children}</Suspense>
              </main>

              <footer className="site-footer" aria-label="Site footer">
                <div className="footer-inner">
                  {/* Brand */}
                  <div className="footer-brand-section">
                    <span className="footer-logo">
                      <span style={{ color: "var(--color-brand)" }}>WNBP</span>
                      <span style={{ opacity: 0.5, fontWeight: 400 }}> · WetNoodlesBestPicks</span>
                    </span>
                    <p className="footer-tagline">
                      Data-driven sports predictions powered by machine learning models.
                      Covering NBA, MLB, NFL, NHL, soccer, tennis, and more.
                    </p>
                  </div>

                  {/* Navigation columns */}
                  <nav className="footer-nav-grid" aria-label="Footer navigation">
                    {FOOTER_NAV.map((section) => (
                      <div key={section.title} className="footer-nav-col">
                        <h3 className="footer-nav-heading">{section.title}</h3>
                        <ul className="footer-nav-list">
                          {section.links.map((link) => (
                            <li key={link.href}>
                              <Link href={link.href} className="footer-nav-link">
                                {link.label}
                              </Link>
                            </li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </nav>
                  <div className="footer-bottom">
                    <p className="footer-copyright">
                      © {currentYear} WNBP – WetNoodlesBestPicks. All rights reserved.
                    </p>
                    <div className="footer-bottom-links">
                      <Link href="/predictions" className="footer-bottom-link">Picks</Link>
                      <span className="footer-dot">·</span>
                      <Link href="/model-health" className="footer-bottom-link">Status</Link>
                      <span className="footer-dot">·</span>
                      <Link href="/pricing" className="footer-bottom-link">Pricing</Link>
                    </div>
                    <p className="footer-disclaimer">
                      For entertainment purposes only. No purchase necessary to access free picks.
                      Premium subscription required for full access. Predictions are not guaranteed.
                      Please gamble responsibly.
                    </p>
                  </div>
                </div>
              </footer>
            </div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
