import type { DocsBackend } from "./backend-sqlite.js";

// ---------------------------------------------------------------------------
// Tool definitions (JSON Schema)
// ---------------------------------------------------------------------------

export const toolDefinitions = [
  {
    name: "search_docs",
    description: "Search the Jazz documentation.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Search query (FTS5 syntax when available, plain keywords in fallback)",
        },
        limit: {
          type: "number",
          description: "Max results (default 10)",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "get_doc",
    description: "Retrieve the full content of a documentation page.",
    inputSchema: {
      type: "object",
      properties: {
        slug: {
          type: "string",
          description: 'Page slug (e.g. "reading-data", "quickstarts/react")',
        },
      },
      required: ["slug"],
    },
  },
  {
    name: "list_pages",
    description: "List all available documentation pages.",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
];

// ---------------------------------------------------------------------------
// Tool call dispatcher
// ---------------------------------------------------------------------------

interface RpcError extends Error {
  code: number;
}

function rpcError(code: number, message: string): RpcError {
  return Object.assign(new Error(message), { code });
}

/**
 * Thrown for tool-execution failures (e.g. page not found).
 * The server returns these as a successful JSON-RPC result with isError: true
 * so the model can read the message and recover (e.g. retry with a corrected slug).
 */
export class ToolError extends Error {
  readonly isToolError = true;
  constructor(message: string) {
    super(message);
    this.name = "ToolError";
  }
}

// ---------------------------------------------------------------------------
// ANSI helpers (no dependencies)
// ---------------------------------------------------------------------------

const B = "\x1b[1m"; // bold
const D = "\x1b[2m"; // dim
const R = "\x1b[0m"; // reset
const CYAN = "\x1b[36m";
const HR = `${D}${"─".repeat(60)}${R}`;
const fenceRe = /^[ \t]*```([^\n]*)\n([\s\S]*?)^[ \t]*```[ \t]*$/gm;

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------

function formatSearchResults(
  results: Array<{ title: string; slug: string; section: string; snippet: string }>,
): string {
  if (results.length === 0) return "No results found.";
  return results
    .map(({ title, slug, section, snippet }) => {
      const heading = section ? `${title} › ${section}` : title;
      return `${B}${CYAN}${heading}${R}\n${D}${slug}${R}\n\n${renderInline(snippet)}`;
    })
    .join(`\n\n${HR}\n\n`);
}

function renderInline(text: string): string {
  return text.replace(/\*\*([^*\n]+)\*\*/g, `${B}$1${R}`).replace(/`([^`\n]+)`/g, `${CYAN}$1${R}`);
}

function dedent(text: string): string {
  const lines = text.split("\n");
  const nonEmpty = lines.filter((l) => l.trim().length > 0);
  if (nonEmpty.length === 0) return text;
  const minIndent = Math.min(...nonEmpty.map((l) => (l.match(/^[ \t]*/)?.[0] ?? "").length));
  if (minIndent === 0) return text;
  return lines.map((l) => (l.length >= minIndent ? l.slice(minIndent) : l)).join("\n");
}

function renderProse(text: string): string {
  const withHeadings = text.replace(/^(#{1,6}) (.+)$/gm, (_, hashes: string, heading: string) => {
    if (hashes.length === 1) return `${B}${CYAN}${heading}${R}`;
    if (hashes.length === 2) return `${B}${heading}${R}`;
    return `${D}${heading}${R}`;
  });
  return renderInline(withHeadings);
}

function renderBody(body: string): string {
  // Alternate between prose and code blocks so heading/inline rendering
  // never touches code block content.
  const segments: string[] = [];
  let lastIndex = 0;
  fenceRe.lastIndex = 0;
  let match: RegExpExecArray | null;
  // eslint-disable-next-line no-cond-assign
  while ((match = fenceRe.exec(body)) !== null) {
    const prose = body.slice(lastIndex, match.index);
    if (prose) segments.push(renderProse(prose.trimEnd()));
    const lang = match[1].trim();
    const content = dedent(match[2].trimEnd());
    segments.push(`\`\`\`${lang}\n${content}\n\`\`\``);
    lastIndex = match.index + match[0].length;
  }
  const remaining = body.slice(lastIndex);
  if (remaining) segments.push(renderProse(remaining));
  return segments.join("\n");
}

function formatDoc(doc: {
  title: string;
  slug: string;
  description: string;
  body: string;
  related: string[];
}): string {
  const parts: string[] = [`${B}${doc.title}${R}`];
  if (doc.description) parts.push(`${D}${renderInline(doc.description)}${R}`);
  parts.push(renderBody(doc.body));
  if (doc.related.length > 0) {
    parts.push(`${HR}\n${D}Related:${R} ${doc.related.join("  ")}`);
  }
  return parts.join("\n\n");
}

function formatPageList(
  pages: Array<{ title: string; slug: string; description: string }>,
): string {
  return pages
    .map(
      ({ title, slug, description }) =>
        `${B}${title}${R}  ${D}${slug}${R}\n  ${renderInline(description)}`,
    )
    .join("\n\n");
}

// ---------------------------------------------------------------------------
// Tool call dispatcher
// ---------------------------------------------------------------------------

export function callTool(
  backend: DocsBackend,
  name: string,
  args: Record<string, unknown>,
): string {
  switch (name) {
    case "search_docs": {
      const query = args.query;
      if (typeof query !== "string" || !query) {
        throw rpcError(-32602, "search_docs: query (string) is required");
      }
      const limit = typeof args.limit === "number" ? Math.floor(args.limit) : 10;
      return formatSearchResults(backend.search(query, limit));
    }

    case "get_doc": {
      const slug = args.slug;
      if (typeof slug !== "string" || !slug) {
        throw rpcError(-32602, "get_doc: slug (string) is required");
      }
      const result = backend.getDoc(slug);
      if (!result) {
        throw new ToolError(
          `get_doc: page not found: "${slug}". Use list_pages to find valid slugs.`,
        );
      }
      return formatDoc(result);
    }

    case "list_pages":
      return formatPageList(backend.listPages());

    default:
      throw new ToolError(`Unknown tool: "${name}". Use tools/list to see available tools.`);
  }
}
