import { Skeleton, SkeletonText } from "@/components/ui";

export default function AccountLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 800, margin: "0 auto" }}>
      <Skeleton style={{ width: 180, height: 32, marginBottom: 24 }} />
      <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="card" style={{ padding: 20 }}>
            <Skeleton style={{ width: 140, height: 20, marginBottom: 12 }} />
            <SkeletonText lines={2} />
          </div>
        ))}
      </div>
    </main>
  );
}
