import { Skeleton, SkeletonText } from "@/components/ui";

export default function StatsLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 200, height: 32, marginBottom: 16 }} />
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} style={{ width: 80, height: 36, borderRadius: 18 }} />
        ))}
      </div>
      <Skeleton style={{ width: 260, height: 36, marginBottom: 20, borderRadius: 6 }} />
      <div style={{ borderRadius: 8, overflow: "hidden", border: "1px solid var(--color-border, #e5e7eb)" }}>
        <Skeleton style={{ height: 44, width: "100%" }} />
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} style={{ padding: "10px 16px", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <SkeletonText lines={1} />
          </div>
        ))}
      </div>
    </main>
  );
}
