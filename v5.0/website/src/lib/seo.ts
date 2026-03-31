import type { Metadata } from "next";

const SITE_NAME = "WNBP – WetNoodlesBestPicks";
const BASE_URL = process.env.NEXT_PUBLIC_SITE_URL ?? "https://wetnoodlesbestpicks.com";
const DEFAULT_OG_IMAGE = `${BASE_URL}/og-image.png`;

interface PageMetaInput {
  title: string;
  description: string;
  path: string;
  /** Override the default OG image */
  ogImage?: string;
  /** Optional SEO keywords */
  keywords?: string[];
}

export function buildPageMetadata({ title, description, path, ogImage, keywords }: PageMetaInput): Metadata {
  const url = `${BASE_URL}${path}`;
  const image = ogImage ?? DEFAULT_OG_IMAGE;

  return {
    title,
    description,
    ...(keywords ? { keywords: keywords.join(", ") } : {}),
    alternates: {
      canonical: url,
    },
    openGraph: {
      type: "website",
      siteName: SITE_NAME,
      title: `${title} | WNBP`,
      description,
      url,
      locale: "en_US",
      images: [{ url: image, width: 1200, height: 630, alt: `${title} | WNBP` }],
    },
    twitter: {
      card: "summary_large_image",
      title: `${title} | WNBP`,
      description,
      images: [image],
    },
    robots: { index: true, follow: true },
  };
}

interface CollectionJsonLdInput {
  name: string;
  path: string;
  description: string;
}

export function buildCollectionJsonLd({ name, path, description }: CollectionJsonLdInput): string {
  return JSON.stringify({
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    name,
    description,
    url: `${BASE_URL}${path}`,
    isPartOf: { "@type": "WebSite", name: SITE_NAME, url: BASE_URL },
  });
}

/** JSON-LD for the website / organization — place in root layout */
export function buildSiteJsonLd(): string {
  return JSON.stringify({
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: SITE_NAME,
    url: BASE_URL,
    description: "Data-driven sports predictions and analytics across 20+ leagues.",
    potentialAction: {
      "@type": "SearchAction",
      target: `${BASE_URL}/teams?q={search_term_string}`,
      "query-input": "required name=search_term_string",
    },
  });
}

/** JSON-LD for a sports event / game detail page */
export function buildSportsEventJsonLd(event: {
  homeTeam: string;
  awayTeam: string;
  sport: string;
  startTime?: string;
  status?: string;
  path: string;
}): string {
  return JSON.stringify({
    "@context": "https://schema.org",
    "@type": "SportsEvent",
    name: `${event.awayTeam} vs ${event.homeTeam}`,
    sport: event.sport,
    url: `${BASE_URL}${event.path}`,
    ...(event.startTime ? { startDate: event.startTime } : {}),
    homeTeam: { "@type": "SportsTeam", name: event.homeTeam },
    awayTeam: { "@type": "SportsTeam", name: event.awayTeam },
    ...(event.status ? { eventStatus: event.status === "Final" ? "https://schema.org/EventCompleted" : "https://schema.org/EventScheduled" } : {}),
  });
}
