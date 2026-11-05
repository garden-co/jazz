// Markdown rendering of docs search results for coding agents.
//
// The site's command palette calls `/api/search?query=…` and gets fumadocs'
// JSON. Agents call the same route with `format=md` (or `Accept: text/markdown`)
// and get this Markdown instead: one entry per page, its matching sections, and
// the URL of the page as Markdown so the agent can read the whole thing.

export interface SearchHit {
  id: string;
  url: string;
  type: "page" | "heading" | "text";
  content: string;
  breadcrumbs?: string[];
}

export interface PageMatch {
  /** Page path without anchor, e.g. `/docs/schemas/policies`. */
  path: string;
  title: string;
  breadcrumbs: string[];
  sections: { anchor: string | null; heading: string | null; snippet: string | null }[];
}

export const DEFAULT_LIMIT = 8;
export const MAX_LIMIT = 20;
export const MAX_FULL_PAGES = 3;
const MAX_SECTIONS_PER_PAGE = 3;
const MAX_SNIPPET_CHARS = 280;

/** Wants Markdown: `format=md`, or an Accept header that prefers Markdown or plain text over JSON. */
export function wantsMarkdown(url: URL, accept: string | null): boolean {
  const format = url.searchParams.get("format");
  if (format) return format === "md" || format === "markdown";
  if (!accept) return false;
  const a = accept.toLowerCase();
  if (a.includes("application/json")) return false;
  return a.includes("text/markdown") || a.includes("text/plain");
}

export function parseLimit(raw: string | null, fallback: number, max: number): number {
  if (raw === null) return fallback;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(n, max);
}

/** Strip fumadocs' `<mark>` highlights (and nothing else: docs text mentions `<JazzProvider>`); collapse whitespace. */
function cleanContent(text: string): string {
  return text
    .replace(/<\/?mark>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text;
  const cut = text.slice(0, max);
  const lastSpace = cut.lastIndexOf(" ");
  return `${cut.slice(0, lastSpace > max * 0.6 ? lastSpace : max).trimEnd()}…`;
}

function splitUrl(url: string): { path: string; anchor: string | null } {
  const hash = url.indexOf("#");
  if (hash === -1) return { path: url, anchor: null };
  return { path: url.slice(0, hash), anchor: url.slice(hash + 1) || null };
}

/**
 * Group fumadocs' flat, ranked result list (page, heading and text hits) into
 * one entry per page with up to three sections each, keyed by anchor.
 * `headingFor` names sections that only matched in body text.
 */
export function groupByPage(
  hits: SearchHit[],
  limit: number,
  headingFor: (path: string, anchor: string) => string | undefined = () => undefined,
): PageMatch[] {
  const pages = new Map<string, PageMatch>();

  for (const hit of hits) {
    const { path, anchor } = splitUrl(hit.url);
    let page = pages.get(path);
    if (!page) {
      if (pages.size >= limit) continue;
      page = { path, title: path, breadcrumbs: [], sections: [] };
      pages.set(path, page);
    }

    if (hit.type === "page") {
      page.title = cleanContent(hit.content);
      page.breadcrumbs = (hit.breadcrumbs ?? []).map(cleanContent);
      continue;
    }

    let section = page.sections.find((s) => s.anchor === anchor);
    if (!section) {
      if (page.sections.length >= MAX_SECTIONS_PER_PAGE) continue;
      section = {
        anchor,
        heading: anchor ? (headingFor(path, anchor) ?? null) : null,
        snippet: null,
      };
      page.sections.push(section);
    }
    if (hit.type === "heading") section.heading = cleanContent(hit.content);
    else section.snippet ??= truncate(cleanContent(hit.content), MAX_SNIPPET_CHARS);
  }

  return [...pages.values()];
}

/** `/docs/a/b` → `/llms.mdx/docs/a/b`, the route that serves a page as Markdown. */
export function markdownPath(path: string): string {
  return path.startsWith("/docs") ? `/llms.mdx${path}` : path;
}

export const MAX_QUERY_CHARS = 200;

/**
 * The query as it is echoed back: one line, capped, inside an inline code span
 * that can't be closed early, so a crafted `q=` can't inject headings, links
 * or instructions into a page served from jazz.tools.
 */
export function displayQuery(query: string): string {
  const oneLine = query.replace(/\s+/g, " ").trim();
  const capped =
    oneLine.length > MAX_QUERY_CHARS ? `${oneLine.slice(0, MAX_QUERY_CHARS)}…` : oneLine;
  return `\`${capped.replace(/`/g, "'")}\``;
}

export function renderMarkdown(opts: {
  query: string;
  origin: string;
  pages: PageMatch[];
  fullText?: Map<string, string>;
}): string {
  const { query, origin, pages, fullText } = opts;
  const lines: string[] = [];
  lines.push(`# Jazz docs search: ${displayQuery(query)}`);
  lines.push("");

  if (pages.length === 0) {
    lines.push("No matching pages. Try fewer or different words, or browse the index:");
    lines.push(`${origin}/llms.txt`);
    lines.push("");
    lines.push(
      "If the docs should cover this, please tell us: https://github.com/garden-co/jazz/issues/new",
    );
    return `${lines.join("\n")}\n`;
  }

  lines.push(
    `${pages.length} matching page${pages.length === 1 ? "" : "s"}, best first. ` +
      `Each page is also available as Markdown at the "Markdown" URL. ` +
      `Add \`&full=1\` to inline the top ${MAX_FULL_PAGES} pages.`,
  );

  pages.forEach((page, i) => {
    lines.push("");
    const crumbs = page.breadcrumbs.length ? `${page.breadcrumbs.join(" › ")} › ` : "";
    lines.push(`## ${i + 1}. ${crumbs}${page.title}`);
    lines.push("");
    lines.push(`- Page: ${origin}${page.path}`);
    lines.push(`- Markdown: ${origin}${markdownPath(page.path)}`);
    for (const section of page.sections) {
      const link = section.anchor ? `${origin}${page.path}#${section.anchor}` : null;
      const label = section.heading ?? (section.anchor ? `#${section.anchor}` : "Top of page");
      const head = link ? `[${label}](${link})` : label;
      lines.push(section.snippet ? `- ${head}: ${section.snippet}` : `- ${head}`);
    }

    const full = fullText?.get(page.path);
    if (full) {
      lines.push("");
      lines.push(`<page url="${origin}${page.path}">`);
      lines.push("");
      lines.push(full.trim());
      lines.push("");
      lines.push("</page>");
    }
  });

  return `${lines.join("\n")}\n`;
}
