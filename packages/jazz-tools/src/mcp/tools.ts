import type { SqliteBackend } from "./backend-sqlite.js";

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

export function callTool(
  backend: SqliteBackend,
  name: string,
  args: Record<string, unknown>,
): unknown {
  switch (name) {
    case "search_docs": {
      const query = args.query;
      if (typeof query !== "string" || !query) {
        throw rpcError(-32602, "search_docs: query (string) is required");
      }
      const limit = typeof args.limit === "number" ? Math.floor(args.limit) : 10;
      return backend.search(query, limit);
    }

    case "get_doc": {
      const slug = args.slug;
      if (typeof slug !== "string" || !slug) {
        throw rpcError(-32602, "get_doc: slug (string) is required");
      }
      const result = backend.getDoc(slug);
      if (!result) {
        throw rpcError(-32602, `get_doc: page not found: ${slug}`);
      }
      return result;
    }

    case "list_pages":
      return backend.listPages();

    default:
      throw rpcError(-32601, `Unknown tool: ${name}`);
  }
}
