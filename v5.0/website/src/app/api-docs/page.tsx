import type { Metadata } from "next";
import ApiDocsClient from "./ApiDocsClient";
import { buildPageMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPageMetadata({
  title: "API Documentation",
  description:
    "Interactive API reference for the WNBP sports data API. Explore endpoints, parameters, and try live requests.",
  path: "/api-docs",
  keywords: ["WNBP API", "sports data API", "predictions API", "odds API", "sports analytics"],
});

export default function ApiDocsPage() {
  return <ApiDocsClient />;
}
