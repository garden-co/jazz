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

  it("search_docs → returns results with title and slug for a known term", async () => {
    server.send({
      jsonrpc: "2.0",
      id: 3,
      method: "tools/call",
      params: { name: "search_docs", arguments: { query: "schema table" } },
    });
    const res = await server.recv();
    const content = (
      (res.result as Record<string, unknown>).content as Array<{ type: string; text: string }>
    )[0];
    expect(content).toBeDefined();
    expect(content!.type).toBe("text");
    // Should find real results — "schema" and "table" appear throughout the docs
    expect(content!.text).not.toBe("No results found.");
    // eslint-disable-next-line no-control-regex
    expect(content!.text).toMatch(/\u001b\[1m\u001b\[36m/); // bold cyan heading present
  });

  it("get_doc → markdown with title heading and body content", async () => {
    // Get a slug from list_pages first
    server.send({
      jsonrpc: "2.0",
      id: 4,
      method: "tools/call",
      params: { name: "list_pages", arguments: {} },
    });
    const listRes = await server.recv();
    const listContent = (
      (listRes.result as Record<string, unknown>).content as Array<{ text: string }>
    )[0];
    expect(listContent).toBeDefined();
    const listText = listContent!.text;
    // Slug appears dim-wrapped: \u001b[2m{slug}\u001b[0m
    // eslint-disable-next-line no-control-regex
    const slugMatch = listText.match(/\u001b\[2m([^\u001b\n]+)\u001b\[0m/);
    expect(slugMatch).not.toBeNull();
    const slug = slugMatch?.[1];
    expect(slug).toBeDefined();

    server.send({
      jsonrpc: "2.0",
      id: 5,
      method: "tools/call",
      params: { name: "get_doc", arguments: { slug: slug!.trim() } },
    });
    const res = await server.recv();
    const content = ((res.result as Record<string, unknown>).content as Array<{ text: string }>)[0];
    expect(content).toBeDefined();
    const text = content!.text;
    // Title appears bold-wrapped at the start
    // eslint-disable-next-line no-control-regex
    expect(text).toMatch(/\u001b\[1m\S/);
    expect(text.length).toBeGreaterThan(0);
  });

  it("list_pages → markdown list with title, slug, description per page", async () => {
    server.send({
      jsonrpc: "2.0",
      id: 6,
      method: "tools/call",
      params: { name: "list_pages", arguments: {} },
    });
    const res = await server.recv();
    const content = ((res.result as Record<string, unknown>).content as Array<{ text: string }>)[0];
    expect(content).toBeDefined();
    const text = content!.text;
    expect(typeof text).toBe("string");
    expect(text.length).toBeGreaterThan(0);
    // Each page's title and slug appear in the output
    expect(text).toContain("Authentication");
  });

  it("get_doc with unknown slug → isError result so the model can recover", async () => {
    server.send({
      jsonrpc: "2.0",
      id: 7,
      method: "tools/call",
      params: { name: "get_doc", arguments: { slug: "no-such-page" } },
    });
    const res = await server.recv();
    // Must be a successful JSON-RPC result (not a JSON-RPC error) so the
    // model can read the message and retry with a corrected slug.
    expect(res.error).toBeUndefined();
    const result = res.result as Record<string, unknown>;
    expect(result.isError).toBe(true);
    const content = (result.content as Array<{ text: string }>)[0];
    expect(content).toBeDefined();
    const text = content!.text;
    expect(text).toContain("no-such-page");
  });

  it("unknown tool name → isError result so the model can recover", async () => {
    server.send({
      jsonrpc: "2.0",
      id: 8,
      method: "tools/call",
      params: { name: "nonexistent_tool", arguments: {} },
    });
    const res = await server.recv();
    expect(res.error).toBeUndefined();
    expect((res.result as Record<string, unknown>).isError).toBe(true);
  });

  it("stdin EOF → process exits cleanly with code 0", async () => {
    const exitCode = await server.close();
    expect(exitCode).toBe(0);
  });
});
