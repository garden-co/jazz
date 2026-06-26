import { createInterface } from "node:readline";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { createSqliteBackend } from "./backend-sqlite.js";
import { callTool, toolDefinitions, ToolError } from "./tools.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface RunServerOptions {
  input?: NodeJS.ReadableStream;
  output?: NodeJS.WritableStream;
  /** Path to docs-index.db. Defaults to <package-root>/bin/docs-index.db */
  dbPath?: string;
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

  const backend = await createSqliteBackend(dbPath);

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
          if (err instanceof ToolError) {
            // Tool-execution failure: surface as a successful result so the
            // model can read the message and recover (MCP spec §tool-errors).
            write({
              jsonrpc: "2.0",
              id,
              result: {
                content: [{ type: "text", text: err.message }],
                isError: true,
              },
            });
          } else {
            // Protocol-level failure (e.g. malformed params).
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
