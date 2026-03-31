import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import DashboardClient from "./DashboardClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Dashboard",
  description: "Your personalized WNBP dashboard with key metrics at a glance.",
  path: "/dashboard",
});

export default function DashboardPage() {
  return (
    <div style={{ padding: "var(--space-6) 0" }}>
      <div style={{ maxWidth: "var(--max-content)", margin: "0 auto", padding: "0 var(--gutter)" }}>
        <h1 style={{ fontSize: "var(--text-2xl)", fontWeight: 900, marginBottom: "var(--space-6)" }}>
          Dashboard
        </h1>
        <DashboardClient />
      </div>
    </div>
  );
}
