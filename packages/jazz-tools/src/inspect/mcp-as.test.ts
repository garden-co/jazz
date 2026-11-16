import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { MCP_TOOLS, runMcpServer } from "./mcp.js";

// .62 gate: MCP `as` given as an object ({ userId, claims }, the HTTP API's
// shape) was dropped, so the call ran with the backend's authority and
// answered every row. A call that names a user must run as that user, or
// be refused.

const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!();
});

async function callTools(calls: [name: string, args: Record<string, unknown>][]) {
  const bodies: any[] = [];
  const fetch = (async (_url: string, init?: RequestInit) => {
    bodies.push(JSON.parse(String(init?.body)));
    return Response.json({ rows: [], truncated: false, inspectorUrl: "http://x/jazz/inspect/" });
  }) as typeof globalThis.fetch;
  const config = await mkdtemp(join(tmpdir(), "jazz-inspect-mcp-as-"));
  cleanups.push(() => rm(config, { recursive: true, force: true }));
  const input = new PassThrough();
  const lines: any[] = [];
  const served = runMcpServer({
    app: "http://localhost:3000",
    env: { JAZZ_CONFIG_DIR: config },
    input,
    output: (line) => lines.push(JSON.parse(line)),
    fetch,
  });
  calls.forEach(([name, args], index) =>
    input.write(
      `${JSON.stringify({ jsonrpc: "2.0", id: index + 1, method: "tools/call", params: { name, arguments: args } })}\n`,
    ),
  );
  input.end();
  await served;
  const results = calls.map((_, index) => lines.find((line) => line.id === index + 1).result);
  return { bodies, results };
}

const query = { table: "expenses" };

describe("MCP `as`", () => {
  it("takes { userId, claims } as well as a user id, for every tool that runs as a user", async () => {
    const { bodies, results } = await callTools([
      ["query", { query, as: { userId: "mona" } }],
      ["query", { query, as: { userId: "mona", claims: { role: "manager" } } }],
      ["get", { table: "expenses", id: "e1", as: { userId: "mona" }, claims: { role: "manager" } }],
      ["write", { ops: [{ kind: "delete", table: "expenses", id: "e1" }], as: { userId: "mona" } }],
      ["explain", { query, as: { userId: "mona", claims: { role: "manager" } } }],
      ["inspector_link", { query, as: { userId: "mona" } }],
    ]);
    expect(results.every((result) => !result.isError)).toBe(true);
    // The server answers calls concurrently, so requests reach the backend
    // in any order: compare them as a set.
    const sent = bodies.map((body) => JSON.stringify([body.op, body.as])).sort();
    expect(sent).toEqual(
      [
        ["query", "mona"],
        ["query", { userId: "mona", claims: { role: "manager" } }],
        ["get", { userId: "mona", claims: { role: "manager" } }],
        ["write", "mona"],
        ["explain", { userId: "mona", claims: { role: "manager" } }],
        ["link", "mona"],
      ]
        .map((entry) => JSON.stringify(entry))
        .sort(),
    );
  });

  it("refuses any other `as` instead of running with the backend's authority", async () => {
    const refused: Record<string, unknown>[] = [
      { as: {} },
      { as: { user: "mona" } },
      { as: { userId: "" } },
      { as: { userId: "mona", role: "manager" } },
      { as: { userId: "mona", claims: { a: 1 } }, claims: { b: 2 } },
      { as: ["mona"] },
      { as: 42 },
      { as: null },
      { as: "" },
    ];
    const { bodies, results } = await callTools(
      refused.map((args) => ["query", { query, ...args }]),
    );
    expect(bodies).toEqual([]);
    for (const [index, result] of results.entries()) {
      expect(result.isError, JSON.stringify(refused[index])).toBe(true);
    }
  });

  it("declares both shapes in the tools' schemas", () => {
    const as = (MCP_TOOLS.find((tool) => tool.name === "query")!.inputSchema as any).properties.as;
    expect(as.oneOf.map((shape: any) => shape.type)).toEqual(["string", "object"]);
    expect(as.oneOf[1]).toMatchObject({ required: ["userId"], additionalProperties: false });
  });
});
