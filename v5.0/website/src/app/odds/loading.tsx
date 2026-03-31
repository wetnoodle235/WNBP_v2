import { Skeleton, SkeletonText } from "@/components/ui";

export default function OddsLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 160, height: 32, marginBottom: 16 }} />
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} style={{ width: 90, height: 36, borderRadius: 18 }} />
        ))}
      </div>
      <SkeletonText lines={1} />
      {Array.from({ length: 8 }).map((_, i) => (
        <Skeleton key={i} style={{ height: 64, marginBottom: 10, borderRadius: 8 }} />
      ))}
    </main>
  );
}
