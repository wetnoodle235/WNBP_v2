import { Skeleton, SkeletonText } from "@/components/ui";

export default function TeamDetailLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      {/* Breadcrumb */}
      <Skeleton style={{ width: 250, height: 16, marginBottom: 24 }} />

      {/* Team header */}
      <div style={{ display: "flex", gap: 20, marginBottom: 32, alignItems: "center" }}>
        <Skeleton style={{ width: 80, height: 80, borderRadius: 12, flexShrink: 0 }} />
        <div>
          <Skeleton style={{ width: 180, height: 28, marginBottom: 8 }} />
          <Skeleton style={{ width: 120, height: 20 }} />
        </div>
      </div>

      {/* Record cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))", gap: 12, marginBottom: 32 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} style={{ height: 72, borderRadius: 12 }} />
        ))}
      </div>

      {/* Roster table */}
      <Skeleton style={{ width: 100, height: 24, marginBottom: 12 }} />
      <div style={{ display: "grid", gap: 8, marginBottom: 32 }}>
        {Array.from({ length: 10 }).map((_, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: "8px 0", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <Skeleton style={{ width: 32, height: 32, borderRadius: "50%" }} />
            <SkeletonText lines={1} />
          </div>
        ))}
      </div>

      {/* Recent games */}
      <Skeleton style={{ width: 140, height: 24, marginBottom: 12 }} />
      <div style={{ display: "grid", gap: 8 }}>
        {Array.from({ length: 5 }).map((_, i) => (
          <SkeletonText key={i} lines={1} />
        ))}
      </div>
    </main>
  );
}
