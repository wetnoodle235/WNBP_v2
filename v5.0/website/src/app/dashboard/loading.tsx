import { Skeleton, SkeletonText } from "@/components/ui";

export default function DashboardLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 900, margin: "0 auto" }}>
      <Skeleton style={{ width: 200, height: 32, marginBottom: 24 }} />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 32 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} style={{ padding: 20, borderRadius: 12, border: "1px solid var(--color-border, #e5e7eb)" }}>
            <SkeletonText lines={1} />
            <Skeleton style={{ height: 28, width: "60%" }} />
          </div>
        ))}
      </div>
      <Skeleton style={{ height: 200, borderRadius: 12 }} />
    </main>
  );
}
