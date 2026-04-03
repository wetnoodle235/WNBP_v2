// ──────────────────────────────────────────────────────────
// RSS / Atom Feed Parser Utility
// ──────────────────────────────────────────────────────────
// Lightweight regex-based parser shared by all RSS-based providers.
// Handles CDATA sections, HTML entity decoding, and media extensions.

export interface RssItem {
  title: string;
  link: string;
  description: string;
  pubDate: string | null;
  source: string | null;
  imageUrl: string | null;
  guid: string | null;
  category?: string;
}

function decodeEntities(str: string): string {
  return str
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)));
}

function extractCdataOrTag(block: string, tag: string): string {
  const escaped = tag.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(
    `<${escaped}[^>]*>\\s*(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([\\s\\S]*?))\\s*</${escaped}>`,
    "i",
  );
  const m = regex.exec(block);
  if (!m) return "";
  const raw = (m[1] ?? m[2] ?? "").trim();
  return decodeEntities(raw);
}

function extractAttr(block: string, tag: string, attr: string): string | null {
  const escapedTag = tag.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const escapedAttr = attr.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(`<${escapedTag}[^>]+${escapedAttr}="([^"]+)"`, "i");
  const m = regex.exec(block);
  return m ? m[1]! : null;
}

/** Strip XSSI protection prefix (e.g., `)]}'` or `)]}',\n`) from API responses */
export function stripXssi(text: string): string {
  const idx = text.indexOf("{");
  return idx > 0 ? text.slice(idx) : text;
}

/** Parse an RSS/Atom XML string into a list of items */
export function parseRss(xml: string): RssItem[] {
  const items: RssItem[] = [];
  const itemRegex = /<item>([\s\S]*?)<\/item>/gi;
  let match: RegExpExecArray | null;

  while ((match = itemRegex.exec(xml)) !== null) {
    const block = match[1]!;

    const linkFromTag = extractCdataOrTag(block, "link");
    const linkFromHref = extractAttr(block, "link", "href");
    const imageUrl =
      extractAttr(block, "media:thumbnail", "url") ??
      extractAttr(block, "media:content", "url") ??
      extractAttr(block, "enclosure", "url") ??
      null;

    items.push({
      title: extractCdataOrTag(block, "title"),
      link: linkFromHref ?? linkFromTag ?? "",
      description: extractCdataOrTag(block, "description"),
      pubDate: extractCdataOrTag(block, "pubDate") || null,
      source: extractCdataOrTag(block, "source") || null,
      imageUrl,
      guid: extractCdataOrTag(block, "guid") || null,
    });
  }

  return items.filter((i) => i.title || i.link);
}
