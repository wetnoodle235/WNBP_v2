"use client";

import { useState, useEffect, useRef } from "react";

interface TocItem {
  id: string;
  text: string;
  level: number;
}

interface Props {
  /** CSS selector for the container to scan for headings */
  containerSelector?: string;
  /** Which heading levels to include */
  levels?: number[];
  className?: string;
}

/**
 * Auto-generated table of contents that highlights the active heading
 * based on scroll position. Scans headings in the given container.
 */
export function TableOfContents({
  containerSelector = "main",
  levels = [2, 3],
  className = "",
}: Props) {
  const [items, setItems] = useState<TocItem[]>([]);
  const [activeId, setActiveId] = useState("");
  const observerRef = useRef<IntersectionObserver | null>(null);

  useEffect(() => {
    const container = document.querySelector(containerSelector);
    if (!container) return;

    const selector = levels.map((l) => `h${l}[id]`).join(", ");
    const headings = Array.from(container.querySelectorAll(selector)) as HTMLHeadingElement[];
    const tocItems = headings.map((h) => ({
      id: h.id,
      text: h.textContent?.trim() ?? "",
      level: parseInt(h.tagName[1], 10),
    }));
    setItems(tocItems);

    // Observe headings for intersection
    observerRef.current = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            setActiveId(entry.target.id);
            break;
          }
        }
      },
      { rootMargin: "-80px 0px -60% 0px", threshold: 0 },
    );

    headings.forEach((h) => observerRef.current?.observe(h));
    return () => observerRef.current?.disconnect();
  }, [containerSelector, levels]);

  if (items.length < 2) return null;

  return (
    <nav className={`toc ${className}`} aria-label="Table of contents">
      <p className="toc-title">On this page</p>
      <ul className="toc-list">
        {items.map((item) => (
          <li
            key={item.id}
            className={`toc-item ${activeId === item.id ? "toc-active" : ""}`}
            style={{ paddingLeft: `${(item.level - 2) * 12}px` }}
          >
            <a href={`#${item.id}`}>{item.text}</a>
          </li>
        ))}
      </ul>
    </nav>
  );
}
