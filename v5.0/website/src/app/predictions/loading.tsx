import { Skeleton, SkeletonText } from "@/components/ui";

export default function PredictionsLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 200, height: 32, marginBottom: 16 }} />
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} style={{ width: 80, height: 36, borderRadius: 18 }} />
        ))}
      </div>
      <SkeletonText lines={1} />
      {Array.from({ length: 6 }).map((_, i) => (
        <Skeleton key={i} style={{ height: 72, marginBottom: 12, borderRadius: 8 }} />
      ))}
    </main>
  );
}
