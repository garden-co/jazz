import { createInterface } from "node:readline";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { DocsBackend } from "./backend-sqlite.js";
import { callTool, toolDefinitions } from "./tools.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface RunServerOptions {
  input?: NodeJS.ReadableStream;
  output?: NodeJS.WritableStream;
  /** Path to docs-index.db. Defaults to <package-root>/bin/docs-index.db */
  dbPath?: string;
  /** Path to docs-index.txt. Defaults to <package-root>/bin/docs-index.txt */
  txtPath?: string;
}

// ---------------------------------------------------------------------------
// Backend selection
// ---------------------------------------------------------------------------

async function selectBackend(dbPath: string, txtPath: string): Promise<DocsBackend> {
  try {
    // Step 1: confirm node:sqlite is importable
    const { DatabaseSync } = await import("node:sqlite");

    // Step 2: FTS5 probe on an in-memory DB
    const probe = new DatabaseSync(":memory:");
    probe.exec("CREATE VIRTUAL TABLE _probe USING fts5(x)");
    probe.close();

    // Both passed — use SQLite backend
    const { createSqliteBackend } = await import("./backend-sqlite.js");
    return createSqliteBackend(dbPath);
  } catch {
    // Fall back to naive backend (it emits its own stderr warning)
    const { createNaiveBackend } = await import("./backend-naive.js");
    return createNaiveBackend(txtPath);
  }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

export async function runServer(opts: RunServerOptions = {}): Promise<void> {
  const input = opts.input ?? process.stdin;
  const output = opts.output ?? process.stdout;

  // Resolve default index paths relative to this module's package bin/ dir
  const here = dirname(fileURLToPath(import.meta.url));
  const binDir = resolve(here, "../../bin");
  const dbPath = opts.dbPath ?? join(binDir, "docs-index.db");
  const txtPath = opts.txtPath ?? join(binDir, "docs-index.txt");

  const backend = await selectBackend(dbPath, txtPath);

  function write(obj: unknown): void {
    (output as NodeJS.WritableStream).write(JSON.stringify(obj) + "\n");
  }

  function handleLine(line: string): void {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let msg: any;
    try {
      msg = JSON.parse(line);
    } catch {
      write({
        jsonrpc: "2.0",
        id: null,
        error: { code: -32700, message: "Parse error" },
      });
      return;
    }

    const { id, method, params } = msg;

    // JSON-RPC notifications have no id — do not respond
    if (id === undefined || id === null) {
      return;
    }

    switch (method as string) {
      case "initialize":
        write({
          jsonrpc: "2.0",
          id,
          result: {
            protocolVersion: "2024-11-05",
            capabilities: { tools: {} },
            serverInfo: { name: "jazz-docs", version: "1.0.0" },
          },
        });
        break;

      case "ping":
        write({ jsonrpc: "2.0", id, result: {} });
        break;

      case "tools/list":
        write({ jsonrpc: "2.0", id, result: { tools: toolDefinitions } });
        break;

      case "tools/call": {
        const name = (params as any)?.name as string;
        const args = (params as any)?.arguments ?? {};
        try {
          const result = callTool(backend, name, args as Record<string, unknown>);
          write({
            jsonrpc: "2.0",
            id,
            result: {
              content: [{ type: "text", text: result }],
            },
          });
        } catch (err: unknown) {
          const e = err as { code?: number; message?: string };
          write({
            jsonrpc: "2.0",
            id,
            error: {
              code: e.code ?? -32603,
              message: e.message ?? "Internal error",
            },
          });
        }
        break;
      }

      default:
        write({
          jsonrpc: "2.0",
          id,
          error: { code: -32601, message: `Method not found: ${method}` },
        });
    }
  }

  await new Promise<void>((res) => {
    const rl = createInterface({ input, terminal: false });
    rl.on("line", handleLine);
    rl.on("close", res);
  });
}

// ---------------------------------------------------------------------------
// Script entry point
// ---------------------------------------------------------------------------

const isMain = typeof process !== "undefined" && process.argv[1] === fileURLToPath(import.meta.url);

if (isMain) {
  runServer().catch((err: unknown) => {
    console.error("jazz-docs MCP server error:", err);
    process.exit(1);
  });
}
