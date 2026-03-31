import { Skeleton, SkeletonText } from "@/components/ui";

export default function ApiDocsLoading() {
  return (
    <main style={{ display: "flex", gap: "var(--space-6)", padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <aside style={{ width: 240, flexShrink: 0 }}>
        <Skeleton style={{ width: "100%", height: 36, marginBottom: 16 }} />
        {Array.from({ length: 8 }).map((_, i) => (
          <Skeleton key={i} style={{ width: "90%", height: 28, marginBottom: 8 }} />
        ))}
      </aside>
      <div style={{ flex: 1 }}>
        <Skeleton style={{ width: 260, height: 32, marginBottom: 16 }} />
        <SkeletonText lines={3} />
        <div style={{ marginTop: 24 }}>
          <Skeleton style={{ width: "100%", height: 200, borderRadius: 8 }} />
        </div>
        <div style={{ marginTop: 24 }}>
          <Skeleton style={{ width: "100%", height: 200, borderRadius: 8 }} />
        </div>
      </div>
    </main>
  );
}
