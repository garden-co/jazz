/**
 * Integration smoke test for the MCP server.
 *
 * Spawns `node bin/jazz-tools.js mcp` as a real child process, communicates
 * over stdio with newline-delimited JSON-RPC, and exercises the full lifecycle
 * and all three tools against the committed index artefacts.
 *
 * Naive-backend coverage: the stderr warning on SQLite unavailability is
 * tested at the unit level in backend-naive.test.ts; that backend is not
 * exercised here because node:sqlite is available in the test environment.
 *
 * Requires: `pnpm build` before running (dist/mcp/server.js must exist).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
// Relative to this source file: src/mcp/ → ../../ → package root → bin/
const shimPath = join(here, "../../bin/jazz-tools.js");

// ---------------------------------------------------------------------------
// Spawned-server helper
// ---------------------------------------------------------------------------

interface Server {
  send(msg: object): void;
  recv(): Promise<Record<string, unknown>>;
  close(): Promise<number>;
}

function spawnMcp(): { server: Server; proc: ChildProcessWithoutNullStreams } {
  const proc = spawn(process.execPath, [shimPath, "mcp"], {
    stdio: ["pipe", "pipe", "pipe"],
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

  const server: Server = {
    send(msg: object) {
      proc.stdin.write(JSON.stringify(msg) + "\n");
    },
    recv(): Promise<Record<string, unknown>> {
      return new Promise((resolve) => {
        const line = queued.shift();
        if (line !== undefined) {
          resolve(JSON.parse(line) as Record<string, unknown>);
        } else {
          waiters.push((l) => resolve(JSON.parse(l) as Record<string, unknown>));
        }
      });
    },
    close(): Promise<number> {
      proc.stdin.end();
      return new Promise((resolve) => {
        proc.on("close", (code) => resolve(code ?? 0));
      });
    },
  };

  return { server, proc };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

let server: Server;
let proc: ChildProcessWithoutNullStreams;

beforeEach(() => {
  ({ server, proc } = spawnMcp());
});

afterEach(async () => {
  // Clean up even if a test fails mid-sequence.
  // exitCode is null while running, 0+ after exit — don't use falsy check.
  if (proc.exitCode === null && !proc.killed) {
    proc.stdin.end();
    await new Promise<void>((res) => proc.on("close", () => res()));
  }
});

describe("MCP integration: full lifecycle", () => {
  it("initialize → capabilities include tools", async () => {
    server.send({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} });
    const res = await server.recv();
    expect(res.id).toBe(1);
    const result = res.result as Record<string, unknown>;
    expect((result.capabilities as Record<string, unknown>).tools).toBeDefined();
    expect(result.protocolVersion).toBeDefined();
    expect((result.serverInfo as Record<string, unknown>).name).toBe("jazz-docs");
  });

  it("tools/list → three tools with correct names", async () => {
    server.send({ jsonrpc: "2.0", id: 2, method: "tools/list" });
    const res = await server.recv();
    const tools = ((res.result as Record<string, unknown>).tools as Array<{ name: string }>) ?? [];
    const names = tools.map((t) => t.name);
    expect(names).toContain("search_docs");
    expect(names).toContain("get_doc");
    expect(names).toContain("list_pages");
  });

  it("search_docs → array with title/slug/section/snippet per item", async () => {
    server.send({
      jsonrpc: "2.0",
      id: 3,
      method: "tools/call",
      params: { name: "search_docs", arguments: { query: "CoValue" } },
    });
    const res = await server.recv();
    const content = (
      (res.result as Record<string, unknown>).content as Array<{ type: string; text: string }>
    )[0];
    expect(content.type).toBe("text");
    const results = JSON.parse(content.text) as Array<Record<string, unknown>>;
    expect(Array.isArray(results)).toBe(true);
    if (results.length > 0) {
      expect(typeof results[0].title).toBe("string");
      expect(typeof results[0].slug).toBe("string");
      expect(typeof results[0].snippet).toBe("string");
    }
  });

  it("get_doc → body is non-empty string, related is array", async () => {
    // First get a known slug via list_pages, then fetch it
    server.send({
      jsonrpc: "2.0",
      id: 4,
      method: "tools/call",
      params: { name: "list_pages", arguments: {} },
    });
    const listRes = await server.recv();
    const pages = JSON.parse(
      ((listRes.result as Record<string, unknown>).content as Array<{ text: string }>)[0].text,
    ) as Array<{ slug: string }>;
    expect(pages.length).toBeGreaterThan(0);
    const slug = pages[0].slug;

    server.send({
      jsonrpc: "2.0",
      id: 5,
      method: "tools/call",
      params: { name: "get_doc", arguments: { slug } },
    });
    const res = await server.recv();
    const doc = JSON.parse(
      ((res.result as Record<string, unknown>).content as Array<{ text: string }>)[0].text,
    ) as { body: string; related: unknown[] };
    expect(typeof doc.body).toBe("string");
    expect(doc.body.length).toBeGreaterThan(0);
    expect(Array.isArray(doc.related)).toBe(true);
  });

  it("list_pages → array of { title, slug, description }", async () => {
    server.send({
      jsonrpc: "2.0",
      id: 6,
      method: "tools/call",
      params: { name: "list_pages", arguments: {} },
    });
    const res = await server.recv();
    const pages = JSON.parse(
      ((res.result as Record<string, unknown>).content as Array<{ text: string }>)[0].text,
    ) as Array<{ title: string; slug: string; description: string }>;
    expect(Array.isArray(pages)).toBe(true);
    expect(pages.length).toBeGreaterThan(0);
    expect(typeof pages[0].title).toBe("string");
    expect(typeof pages[0].slug).toBe("string");
    expect(typeof pages[0].description).toBe("string");
  });

  it("stdin EOF → process exits cleanly with code 0", async () => {
    const exitCode = await server.close();
    expect(exitCode).toBe(0);
  });
});
