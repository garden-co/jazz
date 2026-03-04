// NOTE: no top-level `import … from "node:sqlite"` — this module must be safe
// to parse/import on Node <22.  DatabaseSync is obtained via dynamic import
// inside createSqliteBackend().

export interface SearchResult {
  title: string;
  slug: string;
  section: string;
  snippet: string;
}

export interface DocResult {
  body: string;
  related: string[];
}

export interface PageInfo {
  title: string;
  slug: string;
  description: string;
}

export interface DocsBackend {
  search(query: string, limit: number): SearchResult[];
  getDoc(slug: string): DocResult | null;
  listPages(): PageInfo[];
}

export async function createSqliteBackend(dbPath: string): Promise<DocsBackend> {
  // Dynamic import keeps node:sqlite off the top-level parse graph
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(dbPath, { readOnly: true });

  // ------------------------------------------------------------------
  // Prepared statements
  // ------------------------------------------------------------------

  const stmtSearch = db.prepare(`
    SELECT
      title,
      slug,
      section_heading,
      snippet(sections_fts, 3, '', '', '…', 32) AS snip,
      bm25(sections_fts)                         AS score
    FROM sections_fts
    WHERE sections_fts MATCH ?
    ORDER BY bm25(sections_fts)
    LIMIT ?
  `);

  const stmtGetPage = db.prepare("SELECT body FROM pages WHERE slug = ?");

  const stmtGetTopHeadings = db.prepare(`
    SELECT section_heading
    FROM sections_fts
    WHERE slug = ? AND section_heading != ''
    LIMIT 3
  `);

  // Fetch up to 15 rows ordered by bm25; deduplicate by slug in code (max 5 kept).
  // We avoid sum(bm25()) in a GROUP BY because bm25() is only valid in
  // WHERE / ORDER BY clauses of FTS5 queries.
  const stmtRelated = db.prepare(`
    SELECT slug, bm25(sections_fts) AS score
    FROM sections_fts
    WHERE sections_fts MATCH ? AND slug != ?
    ORDER BY bm25(sections_fts)
    LIMIT 15
  `);

  const stmtListPages = db.prepare("SELECT title, slug, description FROM pages ORDER BY slug");

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  function buildRelatedQuery(title: string, slug: string): string | null {
    const headingRows = stmtGetTopHeadings.all(slug) as Array<{
      section_heading: string;
    }>;

    const rawTerms = [title, ...headingRows.map((r) => r.section_heading)]
      .join(" ")
      .split(/\s+/)
      .map((w) => w.replace(/[^a-zA-Z0-9]/g, ""))
      .filter((w) => w.length >= 3);

    if (rawTerms.length === 0) return null;

    // Deduplicate and use OR so we get broader matches
    const unique = [...new Set(rawTerms.map((w) => w.toLowerCase()))];
    return unique.join(" OR ");
  }

  // ------------------------------------------------------------------
  // Backend methods
  // ------------------------------------------------------------------

  function search(query: string, limit: number): SearchResult[] {
    try {
      const rows = stmtSearch.all(query, limit) as Array<{
        title: string;
        slug: string;
        section_heading: string;
        snip: string;
        score: number;
      }>;
      return rows.map((r) => ({
        title: r.title,
        slug: r.slug,
        section: r.section_heading,
        snippet: r.snip,
      }));
    } catch {
      return [];
    }
  }

  function getDoc(slug: string): DocResult | null {
    const row = stmtGetPage.get(slug) as { body: string } | undefined;
    if (!row) return null;

    let related: string[] = [];
    try {
      const relQuery = buildRelatedQuery(row.title, slug);
      if (relQuery) {
        const relRows = stmtRelated.all(relQuery, slug) as Array<{
          slug: string;
        }>;
        // Deduplicate while preserving bm25 order (first occurrence = best)
        const seen = new Set<string>();
        for (const r of relRows) {
          if (!seen.has(r.slug)) {
            seen.add(r.slug);
            related.push(r.slug);
            if (related.length >= 5) break;
          }
        }
      }
    } catch {
      // related stays []
    }

    return { body: row.body, related };
  }

  function listPages(): PageInfo[] {
    const rows = stmtListPages.all() as Array<{
      title: string;
      slug: string;
      description: string;
    }>;
    return rows;
  }

  return { search, getDoc, listPages };
}
