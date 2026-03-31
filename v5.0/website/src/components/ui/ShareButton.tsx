"use client";

import { useState, useCallback } from "react";

interface ShareButtonProps {
  title: string;
  text?: string;
  url?: string;
  className?: string;
}

/** Share button using Web Share API with clipboard fallback */
export function ShareButton({ title, text, url, className }: ShareButtonProps) {
  const [copied, setCopied] = useState(false);

  const handleShare = useCallback(async () => {
    const shareUrl = url ?? (typeof window !== "undefined" ? window.location.href : "");

    if (typeof navigator !== "undefined" && navigator.share) {
      try {
        await navigator.share({ title, text: text ?? title, url: shareUrl });
        return;
      } catch {
        // User cancelled or share failed — fall through to clipboard
      }
    }

    // Fallback: copy URL to clipboard
    try {
      await navigator.clipboard.writeText(shareUrl);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Final fallback for older browsers
      const textarea = document.createElement("textarea");
      textarea.value = shareUrl;
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, [title, text, url]);

  return (
    <button
      type="button"
      className={`share-btn${className ? ` ${className}` : ""}`}
      onClick={handleShare}
      aria-label={copied ? "Link copied!" : `Share ${title}`}
      title={copied ? "Copied!" : "Share"}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: "0.25rem",
        padding: "0.25rem 0.5rem",
        border: "1px solid var(--color-border, #e5e7eb)",
        borderRadius: "var(--radius-sm, 4px)",
        background: copied ? "var(--color-win, #22c55e)" : "transparent",
        color: copied ? "#fff" : "var(--color-text-muted, #6b7280)",
        cursor: "pointer",
        fontSize: "var(--text-sm, 0.875rem)",
        transition: "all 0.2s ease",
      }}
    >
      <span aria-hidden="true">{copied ? "✓" : "🔗"}</span>
      {copied ? "Copied" : "Share"}
    </button>
  );
}
