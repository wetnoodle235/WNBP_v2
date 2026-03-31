"use client";

import { useFavorites } from "@/lib/hooks";

interface FavoriteButtonProps {
  id: string;
  storageKey?: string;
  size?: "sm" | "md";
  className?: string;
}

/** Heart toggle button to bookmark games or predictions */
export function FavoriteButton({
  id,
  storageKey = "wnbp_favorites",
  size = "sm",
  className,
}: FavoriteButtonProps) {
  const { toggle, isFavorite } = useFavorites(storageKey);
  const active = isFavorite(id);

  return (
    <button
      type="button"
      className={`favorite-btn${active ? " favorite-btn--active" : ""}${className ? ` ${className}` : ""}`}
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        toggle(id);
      }}
      aria-label={active ? "Remove from favorites" : "Add to favorites"}
      aria-pressed={active}
      style={{
        fontSize: size === "sm" ? "1rem" : "1.25rem",
        lineHeight: 1,
        padding: "0.25rem",
        background: "transparent",
        border: "none",
        cursor: "pointer",
        transition: "transform 0.15s ease",
      }}
    >
      <span aria-hidden="true" style={{ display: "inline-block", transform: active ? "scale(1.15)" : "scale(1)", transition: "transform 0.15s ease" }}>
        {active ? "❤️" : "🤍"}
      </span>
    </button>
  );
}
