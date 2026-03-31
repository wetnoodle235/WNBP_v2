import { Skeleton, SkeletonText } from "@/components/ui";

export default function NewsLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 120, height: 32, marginBottom: 16 }} />
      <Skeleton style={{ width: 300, height: 40, marginBottom: 24, borderRadius: 8 }} />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 16 }}>
        {Array.from({ length: 9 }).map((_, i) => (
          <div key={i} style={{ borderRadius: 12, overflow: "hidden" }}>
            <Skeleton style={{ height: 160, width: "100%" }} />
            <div style={{ padding: 12 }}>
              <SkeletonText lines={2} />
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
