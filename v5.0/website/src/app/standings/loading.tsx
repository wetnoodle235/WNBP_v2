import { Skeleton, SkeletonText } from "@/components/ui";

export default function StandingsLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 180, height: 32, marginBottom: 16 }} />
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} style={{ width: 70, height: 36, borderRadius: 18 }} />
        ))}
      </div>
      <div style={{ borderRadius: 8, overflow: "hidden", border: "1px solid var(--color-border, #e5e7eb)" }}>
        <Skeleton style={{ height: 44, width: "100%" }} />
        {Array.from({ length: 10 }).map((_, i) => (
          <div key={i} style={{ padding: "10px 16px", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <SkeletonText lines={1} />
          </div>
        ))}
      </div>
    </main>
  );
}
