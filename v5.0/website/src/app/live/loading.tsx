import { Skeleton } from "@/components/ui";

export default function LiveLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 24 }}>
        <Skeleton style={{ width: 12, height: 12, borderRadius: "50%" }} />
        <Skeleton style={{ width: 160, height: 32 }} />
      </div>
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} style={{ width: 80, height: 36, borderRadius: 18 }} />
        ))}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))", gap: 16 }}>
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} style={{ height: 200, borderRadius: 12 }} />
        ))}
      </div>
    </main>
  );
}
