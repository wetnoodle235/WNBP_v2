import { Skeleton, SkeletonText } from "@/components/ui";

export default function LeaderboardLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1000, margin: "0 auto" }}>
      <Skeleton style={{ width: 200, height: 32, marginBottom: 24 }} />

      {/* Podium top 3 */}
      <div style={{ display: "flex", justifyContent: "center", gap: 16, marginBottom: 32 }}>
        {Array.from({ length: 3 }).map((_, i) => (
          <Skeleton key={i} style={{ width: 120, height: 100, borderRadius: 12 }} />
        ))}
      </div>

      {/* Table rows */}
      <div style={{ display: "grid", gap: 8 }}>
        {Array.from({ length: 15 }).map((_, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 16px", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <Skeleton style={{ width: 28, height: 28, borderRadius: "50%" }} />
            <SkeletonText lines={1} />
            <Skeleton style={{ width: 60, height: 20, marginLeft: "auto" }} />
          </div>
        ))}
      </div>
    </main>
  );
}
