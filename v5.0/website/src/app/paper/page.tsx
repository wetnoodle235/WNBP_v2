import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { PaperClient } from "./PaperClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Paper Trading",
  description: "Practice your sports betting strategy with paper trades.",
  path: "/paper",
  keywords: ["paper trading", "practice betting", "sports betting simulator", "virtual betting"],
});

export default function PaperTradingPage() {
  return <PaperClient />;
}
