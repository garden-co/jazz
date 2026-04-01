import { readFile } from "node:fs/promises";

import { splitIntoSections, type Section } from "./build-index.js";
import type { DocResult, DocsBackend, PageInfo, SearchResult } from "./backend-sqlite.js";

export type { DocResult, DocsBackend, PageInfo, SearchResult };

// ---------------------------------------------------------------------------
// Warning — emitted when the naive backend is created.
// The server only ever creates one backend instance per process, so
// deduplication is not needed here.
// ---------------------------------------------------------------------------

function emitWarning(): void {
  process.stderr.write(
    "node:sqlite not available — using basic text search. " +
      "Upgrade to Node >=22.13 (current LTS) for better results.\n",
  );
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

interface ParsedPage {
  slug: string;
  title: string;
  description: string;
  body: string;
  sections: Section[];
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

function parseTxt(content: string): ParsedPage[] {
  // Split on page boundaries; each chunk starts with ===PAGE:slug===
  const chunks = content.split(/(?=^===PAGE:)/m).filter((c) => c.trim());
  const pages: ParsedPage[] = [];

  for (const chunk of chunks) {
    const lines = chunk.split("\n");
    const firstLine = lines[0];
    if (!firstLine) continue;
    const slugMatch = firstLine.match(/^===PAGE:(.+)===$/);
    if (!slugMatch) continue;
    const slug = slugMatch[1];
    if (!slug) continue;

    const title = lines[1]?.replace(/^TITLE:/, "").trim() ?? slug;
    const description = lines[2]?.replace(/^DESCRIPTION:/, "").trim() ?? "";
    // lines[3] is blank, body starts at lines[4]
    const body = lines.slice(4).join("\n").trim();

    pages.push({ slug, title, description, body, sections: splitIntoSections(body) });
  }

  return pages;
}

// ---------------------------------------------------------------------------
// Search helpers
// ---------------------------------------------------------------------------

function termFrequency(text: string, terms: string[]): number {
  const lower = text.toLowerCase();
  return terms.reduce((count, term) => {
    let pos = 0;
    while ((pos = lower.indexOf(term, pos)) !== -1) {
      count++;
      pos += term.length;
    }
    return count;
  }, 0);
}

function buildSnippet(body: string, terms: string[]): string {
  const lower = body.toLowerCase();
  let best = 0;
  for (const term of terms) {
    const idx = lower.indexOf(term);
    if (idx !== -1) {
      best = idx;
      break;
    }
  }
  const start = Math.max(0, best - 60);
  const end = Math.min(body.length, best + 100);
  const snip = body.slice(start, end).replace(/\s+/g, " ").trim();
  return start > 0 ? "…" + snip : snip;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export async function createNaiveBackend(txtPath: string): Promise<DocsBackend> {
  emitWarning();

  const content = await readFile(txtPath, "utf8");
  const pages = parseTxt(content);
  const bySlug = new Map(pages.map((p) => [p.slug, p]));

  function search(query: string, limit: number): SearchResult[] {
    const terms = query
      .toLowerCase()
      .split(/\s+/)
      .filter((t) => t.length > 0);

    const results: Array<SearchResult & { _freq: number }> = [];

    for (const page of pages) {
      for (const section of page.sections) {
        const combined = (section.heading + " " + section.body).toLowerCase();
        if (!terms.every((t) => combined.includes(t))) continue;

        const freq = termFrequency(combined, terms);
        results.push({
          title: page.title,
          slug: page.slug,
          section: section.heading,
          snippet: buildSnippet(section.body || section.heading, terms),
          _freq: freq,
        });
      }
    }

    results.sort((a, b) => b._freq - a._freq);
    return results.slice(0, limit).map(({ _freq: _, ...r }) => r);
  }

  function getDoc(slug: string): DocResult | null {
    const page = bySlug.get(slug);
    if (!page) return null;
    return {
      title: page.title,
      slug: page.slug,
      description: page.description,
      body: page.body,
      related: [],
    };
  }

  function listPages(): PageInfo[] {
    return pages.map(({ slug, title, description }) => ({
      slug,
      title,
      description,
    }));
  }

  return { search, getDoc, listPages };
}
