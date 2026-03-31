import { Skeleton, SkeletonText } from "@/components/ui";

export default function SeasonLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 220, height: 32, marginBottom: 8 }} />
      <Skeleton style={{ width: 300, height: 18, marginBottom: 24 }} />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 16, marginBottom: 32 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} style={{ height: 90, borderRadius: 8 }} />
        ))}
      </div>
      <div style={{ borderRadius: 8, overflow: "hidden", border: "1px solid var(--color-border, #e5e7eb)" }}>
        <Skeleton style={{ height: 44, width: "100%" }} />
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} style={{ padding: "10px 16px", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <SkeletonText lines={1} />
          </div>
        ))}
      </div>
    </main>
  );
}
