"use client";

import Link from "next/link";
import Image from "next/image";
import type { ReactNode } from "react";
import { useState } from "react";
import { getSportColor, getSportIcon } from "@/lib/sports-config";

interface StoryCardProps {
  href: string;
  title: string;
  excerpt?: string;
  tag?: string;
  tagColor?: string;
  imageUrl?: string;
  imageAlt?: string;
  author?: string;
  publishedAt?: string;
  sport?: string;
  /** Trailing badge or meta element */
  meta?: ReactNode;
  size?: "default" | "featured";
  className?: string;
  /** When true, skip the image area entirely if there's no valid image (no fallback). */
  hideImageOnFail?: boolean;
}

function isValidImageUrl(url?: string): boolean {
  if (!url) return false;
  if (url.startsWith("http://") || url.startsWith("https://")) return true;
  if (url.startsWith("/")) return true;
  return false;
}

function isOptimizableUrl(url: string): boolean {
  try {
    const hostname = new URL(url).hostname;
    const OPTIMIZABLE = ["a.espncdn.com", "s.espncdn.com", "cdn.nba.com", "securea.mlb.com", "img.mlbstatic.com"];
    return OPTIMIZABLE.includes(hostname) || url.startsWith("/");
  } catch {
    return url.startsWith("/");
  }
}

function SportFallback({ sport, size }: { sport?: string; size: "default" | "featured" }) {
  const color = sport ? getSportColor(sport) : "var(--color-text-muted)";
  const icon = sport ? getSportIcon(sport) : "🏅";
  return (
    <div
      className="story-card-image"
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: `linear-gradient(135deg, ${color}22, ${color}44)`,
        minHeight: size === "featured" ? 80 : 60,
      }}
    >
      <span style={{ fontSize: size === "featured" ? "3rem" : "2rem", opacity: 0.7 }}>
        {icon}
      </span>
    </div>
  );
}

export function StoryCard({
  href,
  title,
  excerpt,
  tag,
  tagColor,
  imageUrl,
  imageAlt,
  author,
  publishedAt,
  sport,
  meta,
  size = "default",
  className,
  hideImageOnFail,
}: StoryCardProps) {
  const isFeatured = size === "featured";
  const [imgError, setImgError] = useState(false);
  const hasImageUrl = isValidImageUrl(imageUrl);
  const showImage = hasImageUrl && !imgError;
  const showFallback = !showImage && !hideImageOnFail;
  const useNextImage = showImage && imageUrl && isOptimizableUrl(imageUrl);

  const isExternal = href.startsWith("http://") || href.startsWith("https://");
  const linkProps = isExternal ? { target: "_blank" as const, rel: "noopener noreferrer" } : {};

  return (
    <article
      className={`story-card${isFeatured ? " story-card--featured" : ""}${className ? ` ${className}` : ""}`}
    >
      {showImage ? (
        <Link href={href} tabIndex={-1} aria-hidden="true" {...linkProps}>
          <div className="story-card-image">
            {useNextImage ? (
              <Image
                src={imageUrl!}
                alt={imageAlt ?? title}
                fill
                sizes={isFeatured ? "(max-width: 768px) 100vw, 600px" : "(max-width: 768px) 100vw, 400px"}
                style={{ objectFit: "cover" }}
                onError={() => setImgError(true)}
              />
            ) : (
              // External images not in remotePatterns — fall back to <img>
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={imageUrl}
                alt={imageAlt ?? title}
                style={{ width: "100%", height: "100%", objectFit: "cover" }}
                onError={() => setImgError(true)}
                loading="lazy"
              />
            )}
          </div>
        </Link>
      ) : showFallback ? (
        <Link href={href} tabIndex={-1} aria-hidden="true" {...linkProps}>
          <SportFallback sport={sport} size={size ?? "default"} />
        </Link>
      ) : null}
      <div className="story-card-body">
        {(tag || sport) && (
          <div style={{ display: "flex", gap: "var(--space-2)", marginBottom: "var(--space-2)" }}>
            {tag && (
              <span
                className="story-card-tag"
                style={tagColor ? { color: tagColor } : undefined}
              >
                {tag}
              </span>
            )}
            {sport && (
              <span
                style={{
                  fontSize: "var(--text-xs)",
                  fontWeight: "var(--fw-semibold)",
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  color: getSportColor(sport),
                }}
              >
                {sport.toUpperCase()}
              </span>
            )}
          </div>
        )}
        <h3
          className={`story-card-title${isFeatured ? " text-xl" : ""}`}
          style={isFeatured ? { fontSize: "var(--text-xl)" } : undefined}
        >
          <Link href={href} className="story-card-title-link" {...linkProps}>
            {title}
          </Link>
        </h3>
        {excerpt && <p className="story-card-excerpt">{excerpt}</p>}
        <div className="story-card-meta">
          {author && <span>{author}</span>}
          {publishedAt && <span>{publishedAt}</span>}
          {meta}
        </div>
      </div>
    </article>
  );
}
