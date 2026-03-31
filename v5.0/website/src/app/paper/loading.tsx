import { Skeleton, SkeletonText } from "@/components/ui";

export default function PaperLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1000, margin: "0 auto" }}>
      <Skeleton style={{ width: 220, height: 32, marginBottom: 16 }} />
      <Skeleton style={{ width: 300, height: 20, marginBottom: 24 }} />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 16, marginBottom: 24 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="card" style={{ padding: 16, textAlign: "center" }}>
            <Skeleton style={{ width: 60, height: 32, margin: "0 auto 8px" }} />
            <Skeleton style={{ width: 80, height: 14, margin: "0 auto" }} />
          </div>
        ))}
      </div>
      <div style={{ borderRadius: 8, overflow: "hidden", border: "1px solid var(--color-border, #e5e7eb)" }}>
        <Skeleton style={{ height: 44, width: "100%" }} />
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} style={{ padding: "10px 16px", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <SkeletonText lines={1} />
          </div>
        ))}
      </div>
    </main>
  );
}
