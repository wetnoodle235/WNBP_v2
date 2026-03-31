import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { FavoritesClient } from "./FavoritesClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Favorites",
  description: "Your bookmarked games and predictions.",
  path: "/favorites",
});

export default function FavoritesPage() {
  return <FavoritesClient />;
}
