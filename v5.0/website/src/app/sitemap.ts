import type { MetadataRoute } from "next";

const BASE_URL = process.env.NEXT_PUBLIC_SITE_URL ?? "https://wetnoodlesbestpicks.com";

const SPORTS = [
  "nba", "mlb", "nfl", "nhl", "wnba", "epl", "ncaab", "ncaaf", "ncaaw",
  "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl",
  "ufc", "atp", "wta", "csgo", "lol", "dota2", "valorant", "f1",
];

export default function sitemap(): MetadataRoute.Sitemap {
  const now = new Date().toISOString();

  const staticPages: MetadataRoute.Sitemap = [
    { url: BASE_URL, lastModified: now, changeFrequency: "daily", priority: 1.0 },
    { url: `${BASE_URL}/predictions`, lastModified: now, changeFrequency: "daily", priority: 0.9 },
    { url: `${BASE_URL}/live`, lastModified: now, changeFrequency: "always", priority: 0.9 },
    { url: `${BASE_URL}/odds`, lastModified: now, changeFrequency: "daily", priority: 0.8 },
    { url: `${BASE_URL}/news`, lastModified: now, changeFrequency: "daily", priority: 0.8 },
    { url: `${BASE_URL}/standings`, lastModified: now, changeFrequency: "daily", priority: 0.7 },
    { url: `${BASE_URL}/stats`, lastModified: now, changeFrequency: "weekly", priority: 0.6 },
    { url: `${BASE_URL}/players`, lastModified: now, changeFrequency: "daily", priority: 0.7 },
    { url: `${BASE_URL}/teams`, lastModified: now, changeFrequency: "daily", priority: 0.7 },
    { url: `${BASE_URL}/opportunities`, lastModified: now, changeFrequency: "daily", priority: 0.8 },
    { url: `${BASE_URL}/season`, lastModified: now, changeFrequency: "weekly", priority: 0.6 },
    { url: `${BASE_URL}/paper`, lastModified: now, changeFrequency: "weekly", priority: 0.5 },
    { url: `${BASE_URL}/model-health`, lastModified: now, changeFrequency: "daily", priority: 0.5 },
    { url: `${BASE_URL}/pricing`, lastModified: now, changeFrequency: "monthly", priority: 0.6 },
  ];

  const sportPages: MetadataRoute.Sitemap = SPORTS.map((sport) => ({
    url: `${BASE_URL}/games/${sport}`,
    lastModified: now,
    changeFrequency: "daily" as const,
    priority: 0.7,
  }));

  return [...staticPages, ...sportPages];
}
