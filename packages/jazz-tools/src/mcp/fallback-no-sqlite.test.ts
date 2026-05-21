/**
 * Reproduces a user report: installing the MCP server under Node.js < 22
 * (no `node:sqlite`) crashed with ERR_UNKNOWN_BUILTIN_MODULE instead of
 * falling back to text search.
 *
 * Spawns the real server via the shim with a module hook that makes
 * `node:sqlite` unavailable (see __fixtures__/block-node-sqlite.*), then
 * drives it over stdio like a real MCP client.
 *
 * Requires: `pnpm build` before running (dist/mcp/server.js must exist).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const shimPath = join(here, "../../bin/jazz-tools.js");
const registerHook = join(here, "__fixtures__/block-node-sqlite.register.mjs");

interface Server {
  send(msg: object): void;
  recv(): Promise<Record<string, unknown>>;
  stderr(): string;
  close(): Promise<number>;
}

function spawnMcpWithoutSqlite(): {
  server: Server;
  proc: ChildProcessWithoutNullStreams;
} {
  const proc = spawn(process.execPath, ["--import", registerHook, shimPath, "mcp"], {
    stdio: ["pipe", "pipe", "pipe"],
  });

  let stderrBuf = "";
  proc.stderr.on("data", (c: Buffer) => {
    stderrBuf += c.toString("utf8");
  });

  const queued: string[] = [];
  const waiters: Array<(line: string) => void> = [];
  let buf = "";
  proc.stdout.on("data", (chunk: Buffer) => {
    buf += chunk.toString("utf8");
    const parts = buf.split("\n");
    buf = parts.pop() ?? "";
    for (const line of parts) {
      if (!line.trim()) continue;
      const waiter = waiters.shift();
      if (waiter) {
        waiter(line);
      } else {
        queued.push(line);
      }
    }
  });

  let exitCode: number | null = null;
  const exited = new Promise<number>((resolve) => {
    proc.on("close", (code) => {
      exitCode = code ?? 0;
      resolve(exitCode);
    });
  });

  const server: Server = {
    send(msg: object) {
      proc.stdin.write(JSON.stringify(msg) + "\n");
    },
    recv(): Promise<Record<string, unknown>> {
      return new Promise((resolve, reject) => {
        const line = queued.shift();
        if (line !== undefined) {
          resolve(JSON.parse(line) as Record<string, unknown>);
          return;
        }
        let settled = false;
        waiters.push((l) => {
          if (settled) return;
          settled = true;
          resolve(JSON.parse(l) as Record<string, unknown>);
        });
        // If the process dies before answering, fail loudly with its stderr
        // rather than hanging until the test timeout.
        void exited.then((code) => {
          if (settled) return;
          settled = true;
          reject(
            new Error(
              `MCP server exited (code ${code}) before responding.\n--- stderr ---\n${stderrBuf}`,
            ),
          );
        });
      });
    },
    stderr: () => stderrBuf,
    close(): Promise<number> {
      proc.stdin.end();
      return exited;
    },
  };

  return { server, proc };
}

let server: Server;
let proc: ChildProcessWithoutNullStreams;

beforeEach(() => {
  ({ server, proc } = spawnMcpWithoutSqlite());
});

afterEach(async () => {
  if (proc.exitCode === null && !proc.killed) {
    proc.stdin.end();
    await new Promise<void>((res) => proc.on("close", () => res()));
  }
});

describe("MCP server without node:sqlite (Node < 22)", () => {
  it("falls back to text search and answers search_docs instead of crashing", async () => {
    server.send({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} });
    const init = await server.recv();
    expect(init.id).toBe(1);

    server.send({
      jsonrpc: "2.0",
      id: 2,
      method: "tools/call",
      params: { name: "search_docs", arguments: { query: "schema" } },
    });
    const res = await server.recv();

    expect(res.error).toBeUndefined();
    const content = (
      (res.result as Record<string, unknown>).content as Array<{ type: string; text: string }>
    )[0];
    expect(content).toBeDefined();
    expect(content!.type).toBe("text");
    expect(content!.text).not.toBe("No results found.");
    expect(content!.text.length).toBeGreaterThan(0);
  });

  it("exits cleanly (code 0) on stdin EOF", async () => {
    server.send({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} });
    await server.recv();
    const code = await server.close();
    expect(code).toBe(0);
  });

  it("emits a loud deprecation warning to stderr", async () => {
    server.send({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} });
    await server.recv();
    await server.close();

    const err = server.stderr();
    expect(err).toMatch(/DEPRECATED/);
    expect(err).toMatch(/REMOVED/);
    expect(err).toMatch(/Node\.js 22/);
  });
});
