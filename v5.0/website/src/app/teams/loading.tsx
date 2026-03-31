import { Skeleton, SkeletonText } from "@/components/ui";

export default function TeamsLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 140, height: 32, marginBottom: 16 }} />
      <Skeleton style={{ width: 300, height: 40, marginBottom: 24, borderRadius: 8 }} />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12 }}>
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} style={{ padding: 16, borderRadius: 8, border: "1px solid var(--color-border, #e5e7eb)" }}>
            <Skeleton style={{ width: 48, height: 48, borderRadius: "50%", marginBottom: 8 }} />
            <SkeletonText lines={2} />
          </div>
        ))}
      </div>
    </main>
  );
}
