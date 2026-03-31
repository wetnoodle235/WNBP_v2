import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  compress: true,
  distDir: process.env.NODE_ENV === "development" ? ".next-dev" : ".next",

  compiler: {
    removeConsole: process.env.NODE_ENV === "production" ? { exclude: ["error", "warn"] } : false,
  },

  experimental: {
    optimizePackageImports: ["@/components/ui"],
  },

  images: {
    remotePatterns: [
      { protocol: "https", hostname: "a.espncdn.com" },
      { protocol: "https", hostname: "s.espncdn.com" },
      { protocol: "https", hostname: "cdn.nba.com" },
      { protocol: "https", hostname: "securea.mlb.com" },
      { protocol: "https", hostname: "img.mlbstatic.com" },
    ],
    formats: ["image/avif", "image/webp"],
  },

  async headers() {
    const isDev = process.env.NODE_ENV !== "production";
    return isDev
      ? []
      : [
          {
            source: "/(.*)",
            headers: [
              { key: "X-Content-Type-Options", value: "nosniff" },
              { key: "X-Frame-Options", value: "DENY" },
              { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
              {
                key: "Permissions-Policy",
                value: "camera=(), microphone=(), geolocation=()",
              },
              {
                key: "Content-Security-Policy",
                value: [
                  "default-src 'self'",
                  "script-src 'self' 'unsafe-inline'",
                  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
                  "font-src 'self' https://fonts.gstatic.com",
                  "img-src 'self' data: blob: https://a.espncdn.com https://s.espncdn.com https://cdn.nba.com https://securea.mlb.com https://img.mlbstatic.com",
                  "connect-src 'self' http://localhost:8000 http://127.0.0.1:8000 https://*.workers.dev",
                  "frame-ancestors 'none'",
                  "base-uri 'self'",
                  "form-action 'self'",
                ].join("; "),
              },
              {
                key: "Strict-Transport-Security",
                value: "max-age=31536000; includeSubDomains",
              },
            ],
          },
        ];
  },

  async rewrites() {
    const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000";
    return [
      {
        source: "/api/proxy/:path*",
        destination: `${apiUrl}/:path*`,
      },
    ];
  },

  async redirects() {
    return [
      {
        source: "/home",
        destination: "/",
        permanent: true,
      },
    ];
  },

  webpack: (config, { dev }) => {
    if (dev) {
      config.cache = false;
    }
    return config;
  },
};

export default nextConfig;
