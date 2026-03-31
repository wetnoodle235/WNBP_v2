import { Skeleton, SkeletonText } from "@/components/ui";

export default function PlayerDetailLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      {/* Breadcrumb */}
      <Skeleton style={{ width: 220, height: 16, marginBottom: 24 }} />

      {/* Player header */}
      <div style={{ display: "flex", gap: 24, marginBottom: 32 }}>
        <Skeleton style={{ width: 96, height: 96, borderRadius: "50%", flexShrink: 0 }} />
        <div style={{ flex: 1 }}>
          <Skeleton style={{ width: 200, height: 28, marginBottom: 8 }} />
          <Skeleton style={{ width: 140, height: 20, marginBottom: 8 }} />
          <div style={{ display: "flex", gap: 8 }}>
            <Skeleton style={{ width: 60, height: 24, borderRadius: 12 }} />
            <Skeleton style={{ width: 60, height: 24, borderRadius: 12 }} />
          </div>
        </div>
      </div>

      {/* Stat cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(150px, 1fr))", gap: 16, marginBottom: 32 }}>
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} style={{ height: 80, borderRadius: 12 }} />
        ))}
      </div>

      {/* Stats table */}
      <Skeleton style={{ width: 160, height: 24, marginBottom: 16 }} />
      <div style={{ display: "grid", gap: 8 }}>
        {Array.from({ length: 10 }).map((_, i) => (
          <SkeletonText key={i} lines={1} />
        ))}
      </div>
    </main>
  );
}
