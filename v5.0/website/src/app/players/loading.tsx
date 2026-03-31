import { Skeleton, SkeletonText } from "@/components/ui";

export default function PlayersLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <Skeleton style={{ width: 160, height: 32, marginBottom: 16 }} />
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        <Skeleton style={{ width: 220, height: 40, borderRadius: 8 }} />
        <Skeleton style={{ width: 120, height: 40, borderRadius: 8 }} />
      </div>
      <div style={{ display: "grid", gap: 8 }}>
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 16px", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <Skeleton style={{ width: 36, height: 36, borderRadius: "50%" }} />
            <SkeletonText lines={1} />
            <SkeletonText lines={1} />
          </div>
        ))}
      </div>
    </main>
  );
}
