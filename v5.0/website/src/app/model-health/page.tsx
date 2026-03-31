import type { Metadata } from "next";
import { buildPageMetadata } from "@/lib/seo";
import { ModelHealthClient } from "./ModelHealthClient";

export const dynamic = "auto";
export const revalidate = 90;

export const metadata: Metadata = buildPageMetadata({
  title: "Model Health",
  description: "Calibration and probability quality metrics across sports.",
  path: "/model-health",
});

export default function ModelHealthPage() {
  return <ModelHealthClient />;
}
