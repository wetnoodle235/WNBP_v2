import { Skeleton } from "@/components/ui";

export default function RegisterLoading() {
  return (
    <main style={{ display: "flex", justifyContent: "center", alignItems: "center", minHeight: "60vh", padding: "var(--space-6)" }}>
      <div style={{ width: "100%", maxWidth: 440 }}>
        <Skeleton style={{ width: 160, height: 28, margin: "0 auto 24px" }} />
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <Skeleton style={{ height: 44, borderRadius: 6 }} />
          <Skeleton style={{ height: 44, borderRadius: 6 }} />
          <Skeleton style={{ height: 44, borderRadius: 6 }} />
          <Skeleton style={{ height: 44, borderRadius: 8, marginTop: 8 }} />
        </div>
      </div>
    </main>
  );
}
