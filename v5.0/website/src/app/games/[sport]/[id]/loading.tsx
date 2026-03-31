import { Skeleton, SkeletonText } from "@/components/ui";

export default function GameDetailLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      {/* Breadcrumb */}
      <Skeleton style={{ width: 200, height: 16, marginBottom: 24 }} />

      {/* Score header */}
      <div style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: 32, padding: "2rem 0" }}>
        <div style={{ textAlign: "center" }}>
          <Skeleton style={{ width: 64, height: 64, borderRadius: "50%", margin: "0 auto 8px" }} />
          <Skeleton style={{ width: 100, height: 20 }} />
        </div>
        <div style={{ textAlign: "center" }}>
          <Skeleton style={{ width: 120, height: 48, borderRadius: 8 }} />
          <Skeleton style={{ width: 80, height: 16, marginTop: 8 }} />
        </div>
        <div style={{ textAlign: "center" }}>
          <Skeleton style={{ width: 64, height: 64, borderRadius: "50%", margin: "0 auto 8px" }} />
          <Skeleton style={{ width: 100, height: 20 }} />
        </div>
      </div>

      {/* Prediction card */}
      <Skeleton style={{ width: "100%", height: 120, borderRadius: 12, marginBottom: 24 }} />

      {/* Tabs */}
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} style={{ width: 80, height: 36, borderRadius: 8 }} />
        ))}
      </div>

      {/* Table skeleton */}
      <div style={{ display: "grid", gap: 8 }}>
        {Array.from({ length: 8 }).map((_, i) => (
          <SkeletonText key={i} lines={1} />
        ))}
      </div>
    </main>
  );
}
