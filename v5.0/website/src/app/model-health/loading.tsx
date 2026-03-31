import { Skeleton, SkeletonText } from "@/components/ui";

export default function ModelHealthLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1000, margin: "0 auto" }}>
      <Skeleton style={{ width: 220, height: 32, marginBottom: 16 }} />
      <Skeleton style={{ width: 180, height: 36, borderRadius: 6, marginBottom: 24 }} />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 16 }}>
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="card" style={{ padding: 20 }}>
            <Skeleton style={{ width: 120, height: 20, marginBottom: 12 }} />
            <SkeletonText lines={3} />
          </div>
        ))}
      </div>
    </main>
  );
}
