"use client";

import { useState, useCallback, useMemo, useEffect } from "react";
import type { ReactNode, CSSProperties } from "react";
import { Skeleton } from "./Skeleton";

export interface TableColumn<T> {
  key: string;
  header: string;
  /** Custom render fn. Falls back to `String(row[key])`. */
  render?: (row: T, idx: number) => ReactNode;
  /** Comparator for client-side sorting. Receives two rows; return negative/0/positive. */
  sortFn?: (a: T, b: T) => number;
  align?: "left" | "right" | "center";
  width?: string;
  /** If true, this column is always visible (not hidden on mobile) */
  sticky?: boolean;
  /** Include this column in text search filtering */
  searchable?: boolean;
}

interface DataTableProps<T> {
  columns: TableColumn<T>[];
  rows: T[];
  getRowKey: (row: T, idx: number) => string | number;
  caption?: string;
  loading?: boolean;
  empty?: ReactNode;
  defaultSortKey?: string;
  defaultSortDir?: "asc" | "desc";
  className?: string;
  /** Called when a row is clicked */
  onRowClick?: (row: T) => void;
  /** Row-level style override (e.g. highlight conditional rows) */
  rowStyle?: (row: T) => CSSProperties | undefined;
  /** Row-level className (e.g. playoff/bottom row coloring) */
  rowClassName?: (row: T, idx: number) => string | undefined;
  /** Show a search/filter input above the table */
  searchable?: boolean;
  /** Placeholder text for the search input */
  searchPlaceholder?: string;
}

export function DataTable<T>({
  columns,
  rows,
  getRowKey,
  caption,
  loading,
  empty,
  defaultSortKey,
  defaultSortDir = "desc",
  className,
  onRowClick,
  rowStyle,
  rowClassName,
  searchable = false,
  searchPlaceholder = "Filter rows…",
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(defaultSortKey ?? null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">(defaultSortDir);
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");

  // Debounce search to avoid janky filtering on large datasets
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(searchQuery), 200);
    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Reset sort key if column no longer exists
  useEffect(() => {
    if (sortKey && !columns.find((c) => c.key === sortKey)) {
      setSortKey(null);
    }
  }, [columns, sortKey]);

  const handleSort = useCallback(
    (key: string) => {
      setSortKey((prev) => {
        if (prev === key) {
          setSortDir((d) => (d === "asc" ? "desc" : "asc"));
          return prev;
        }
        setSortDir("desc");
        return key;
      });
    },
    [],
  );

  const searchableCols = useMemo(
    () => columns.filter((c) => c.searchable !== false),
    [columns],
  );

  const filteredRows = useMemo(() => {
    if (!debouncedSearch.trim()) return rows;
    const q = debouncedSearch.toLowerCase();
    return rows.filter((row) => {
      const record = row as Record<string, unknown>;
      return searchableCols.some((col) => {
        const val = record[col.key];
        return val != null && String(val).toLowerCase().includes(q);
      });
    });
  }, [rows, debouncedSearch, searchableCols]);

  const sortedRows: T[] = useMemo(() => {
    if (!sortKey) return filteredRows;
    const col = columns.find((c) => c.key === sortKey);
    if (!col?.sortFn) return filteredRows;
    const fn = col.sortFn;
    return [...filteredRows].sort((a, b) => {
      const d = fn(a, b);
      return sortDir === "asc" ? d : -d;
    });
  }, [filteredRows, sortKey, sortDir, columns]);

  if (loading) {
    return (
      <div className={`data-table-wrap${className ? ` ${className}` : ""}`} role="region" aria-busy="true">
        {Array.from({ length: 5 }, (_, i) => (
          <Skeleton key={i} height={40} style={{ marginBottom: 2 }} />
        ))}
        <span className="sr-only">Loading table data…</span>
      </div>
    );
  }

  return (
    <div
      className={`data-table-wrap${className ? ` ${className}` : ""}`}
      role="region"
      aria-label={caption}
    >
      {searchable && (
        <div className="data-table-search">
          <input
            type="search"
            className="data-table-search-input"
            placeholder={searchPlaceholder}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            aria-label="Filter table"
          />
          {searchQuery && (
            <span className="data-table-search-count" aria-live="polite">
              {sortedRows.length} of {rows.length}
            </span>
          )}
        </div>
      )}
      <table className="data-table">
        {caption && <caption className="sr-only">{caption}</caption>}
        <thead>
          <tr>
            {columns.map((col) => {
              const isSorted = sortKey === col.key;
              return (
                <th
                  key={col.key}
                  style={{
                    textAlign: col.align ?? "left",
                    width: col.width,
                    cursor: col.sortFn ? "pointer" : undefined,
                    userSelect: col.sortFn ? "none" : undefined,
                  }}
                  onClick={col.sortFn ? () => handleSort(col.key) : undefined}
                  onKeyDown={col.sortFn ? (e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      handleSort(col.key);
                    }
                  } : undefined}
                  tabIndex={col.sortFn ? 0 : undefined}
                  role={col.sortFn ? "columnheader button" : "columnheader"}
                  aria-sort={
                    isSorted
                      ? sortDir === "asc"
                        ? "ascending"
                        : "descending"
                      : col.sortFn
                      ? "none"
                      : undefined
                  }
                  scope="col"
                >
                  {col.header}
                  {col.sortFn && (
                    <span
                      aria-hidden="true"
                      style={{
                        marginLeft: "0.3rem",
                        opacity: isSorted ? 1 : 0.35,
                        fontSize: "0.7em",
                      }}
                    >
                      {isSorted && sortDir === "asc" ? "▲" : "▼"}
                    </span>
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sortedRows.length === 0 ? (
            <tr>
              <td
                colSpan={columns.length}
                style={{ textAlign: "center", color: "var(--color-text-muted)", padding: "var(--space-8)" }}
              >
                {debouncedSearch ? `No results for "${debouncedSearch}"` : (empty ?? "No data available")}
              </td>
            </tr>
          ) : (
            sortedRows.map((row, idx) => (
              <tr
                key={getRowKey(row, idx)}
                className={rowClassName ? rowClassName(row, idx) : undefined}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                style={{
                  cursor: onRowClick ? "pointer" : undefined,
                  ...(rowStyle ? rowStyle(row) : {}),
                }}
                tabIndex={onRowClick ? 0 : undefined}
                onKeyDown={
                  onRowClick
                    ? (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onRowClick(row); } }
                    : undefined
                }
              >
                {columns.map((col) => (
                  <td
                    key={col.key}
                    style={{ textAlign: col.align ?? "left" }}
                  >
                    {col.render
                      ? col.render(row, idx)
                      : String((row as Record<string, unknown>)[col.key] ?? "")}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
