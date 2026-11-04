import { getLLMText, source } from "@/lib/source";
import {
  DEFAULT_LIMIT,
  MAX_FULL_PAGES,
  MAX_LIMIT,
  groupByPage,
  parseLimit,
  renderMarkdown,
  wantsMarkdown,
  type SearchHit,
} from "@/lib/agent-search";
import { searchEvent } from "@/lib/search-analytics";
import { track } from "@vercel/analytics/server";
import { createFromSource } from "fumadocs-core/search/server";
import { after } from "next/server";

const server = createFromSource(source, {
  // https://docs.orama.com/docs/orama-js/supported-languages
  language: "english",
});

let headings: Map<string, Map<string, string>> | undefined;

/** Heading text for `path#anchor`, from the same structured data the index is built from. */
function headingFor(path: string, anchor: string): string | undefined {
  if (!headings) {
    headings = new Map();
    for (const page of source.getPages()) {
      const byAnchor = new Map<string, string>();
      for (const heading of page.data.structuredData.headings) {
        byAnchor.set(heading.id, heading.content);
      }
      headings.set(page.url, byAnchor);
    }
  }
  return headings.get(path)?.get(anchor);
}

/**
 * `GET /api/search?query=…` returns fumadocs JSON for the site's search box.
 *
 * Agents add `format=md` (or send `Accept: text/markdown`) and may use `q`
 * instead of `query`. They get Markdown: matching pages with their sections
 * and links to each page as Markdown. `limit` caps the pages (default 8, max
 * 20), and `full=1` inlines the full Markdown of the top 3 pages.
 */
export async function GET(request: Request): Promise<Response> {
  const url = new URL(request.url);
  if (!wantsMarkdown(url, request.headers.get("accept"))) {
    if (!url.searchParams.has("query") && url.searchParams.has("q")) {
      url.searchParams.set("query", url.searchParams.get("q")!);
      return server.GET(new Request(url, request));
    }
    return server.GET(request);
  }

  const query = (url.searchParams.get("q") ?? url.searchParams.get("query") ?? "").trim();
  const headers = {
    "Content-Type": "text/markdown; charset=utf-8",
    "Cache-Control": "public, max-age=300, s-maxage=3600",
    Vary: "Accept",
  };

  if (!query) {
    return new Response(
      [
        "# Jazz docs search",
        "",
        "Pass a query: `GET /api/search?q=<words>&format=md`.",
        "Optional: `limit=<1-20>` pages (default 8), `full=1` to inline the top 3 pages.",
        `Index of all pages: ${url.origin}/llms.txt`,
        "",
      ].join("\n"),
      { status: 400, headers },
    );
  }

  const limit = parseLimit(url.searchParams.get("limit"), DEFAULT_LIMIT, MAX_LIMIT);
  const hits = (await server.search(query)) as SearchHit[];
  const pages = groupByPage(hits, limit, headingFor);

  // The site's search box records its own (settled) searches client-side; the
  // JSON path runs on every keystroke, so only the Markdown path tracks here.
  const event = searchEvent(query, "api", pages.length);
  if (event) {
    after(() => track(event.name, event.properties, { request }).catch(() => {}));
  }

  let fullText: Map<string, string> | undefined;
  const full = url.searchParams.get("full");
  if (full === "1" || full === "true") {
    fullText = new Map();
    for (const match of pages.slice(0, MAX_FULL_PAGES)) {
      const slugs = match.path
        .replace(/^\/docs\/?/, "")
        .split("/")
        .filter(Boolean);
      const page = source.getPage(slugs);
      if (page) fullText.set(match.path, await getLLMText(page));
    }
  }

  return new Response(renderMarkdown({ query, origin: url.origin, pages, fullText }), {
    headers,
  });
}
