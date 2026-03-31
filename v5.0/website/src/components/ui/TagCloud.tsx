"use client";

import Link from "next/link";

interface Tag {
  label: string;
  href?: string;
  count?: number;
}

interface Props {
  tags: Tag[];
  className?: string;
}

/**
 * Horizontal tag cloud — e.g. for sport filters or category labels.
 */
export function TagCloud({ tags, className = "" }: Props) {
  if (!tags.length) return null;

  return (
    <div className={`tag-cloud ${className}`} role="list" aria-label="Tags">
      {tags.map((tag) => {
        const content = (
          <>
            {tag.label}
            {tag.count != null && <span className="tag-count">{tag.count}</span>}
          </>
        );

        return tag.href ? (
          <Link key={tag.label} href={tag.href} className="tag-chip" role="listitem">
            {content}
          </Link>
        ) : (
          <span key={tag.label} className="tag-chip" role="listitem">
            {content}
          </span>
        );
      })}
    </div>
  );
}
