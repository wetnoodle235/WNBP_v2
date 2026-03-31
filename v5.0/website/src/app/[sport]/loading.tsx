import { Skeleton, SkeletonText } from "@/components/ui";

export default function SportLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 160, height: 36, marginBottom: 16 }} />
      <Skeleton style={{ width: 280, height: 18, marginBottom: 24 }} />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 16 }}>
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="card" style={{ padding: 20 }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 12 }}>
              <Skeleton style={{ width: 100, height: 16 }} />
              <Skeleton style={{ width: 50, height: 16 }} />
            </div>
            <SkeletonText lines={2} />
            <Skeleton style={{ width: "100%", height: 8, borderRadius: 4, marginTop: 12 }} />
          </div>
        ))}
      </div>
    </main>
  );
}
