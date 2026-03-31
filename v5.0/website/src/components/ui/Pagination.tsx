"use client";

import { useCallback, useRef } from "react";

interface PaginationProps {
  page: number;
  totalPages: number;
  onPageChange: (page: number) => void;
}

function getPageNumbers(current: number, total: number): (number | "...")[] {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);

  const pages: (number | "...")[] = [1];

  if (current > 3) pages.push("...");

  const start = Math.max(2, current - 1);
  const end = Math.min(total - 1, current + 1);

  for (let i = start; i <= end; i++) pages.push(i);

  if (current < total - 2) pages.push("...");

  pages.push(total);
  return pages;
}

export function Pagination({ page, totalPages, onPageChange }: PaginationProps) {
  const navRef = useRef<HTMLElement>(null);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "ArrowLeft" && page > 1) {
        e.preventDefault();
        onPageChange(page - 1);
      } else if (e.key === "ArrowRight" && page < totalPages) {
        e.preventDefault();
        onPageChange(page + 1);
      } else if (e.key === "Home") {
        e.preventDefault();
        onPageChange(1);
      } else if (e.key === "End") {
        e.preventDefault();
        onPageChange(totalPages);
      }
    },
    [page, totalPages, onPageChange],
  );

  if (totalPages <= 1) return null;

  const pages = getPageNumbers(page, totalPages);

  return (
    <nav
      ref={navRef}
      className="pagination"
      aria-label="Pagination"
      onKeyDown={handleKeyDown}
    >
      <button
        className="pagination-btn"
        onClick={() => onPageChange(page - 1)}
        disabled={page <= 1}
        aria-label="Previous page"
      >
        « Prev
      </button>

      {pages.map((p, i) =>
        p === "..." ? (
          <span key={`ellipsis-${i}`} className="pagination-ellipsis" aria-hidden="true">
            …
          </span>
        ) : (
          <button
            key={p}
            className={`pagination-btn${p === page ? " pagination-btn--active" : ""}`}
            onClick={() => onPageChange(p)}
            aria-current={p === page ? "page" : undefined}
            aria-label={`Page ${p}`}
          >
            {p}
          </button>
        ),
      )}

      <button
        className="pagination-btn"
        onClick={() => onPageChange(page + 1)}
        disabled={page >= totalPages}
        aria-label="Next page"
      >
        Next »
      </button>

      <span className="pagination-info" aria-live="polite">
        Page {page} of {totalPages}
      </span>
    </nav>
  );
}
