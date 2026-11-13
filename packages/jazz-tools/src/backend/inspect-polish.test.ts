import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { createInspector, type InspectEvent, type InspectorDeps } from "./inspect.js";
import { requestApproval, tokenStore } from "../inspect/client.js";
import { runInspectCommand } from "../inspect/cli.js";
import { MCP_TOOLS, runMcpServer } from "../inspect/mcp.js";

// alpha.62 polish on the Inspector API, CLI and MCP server:
// #5181 the token's label in whoami and the audit log, #5179 a token's
// lifetime checked before anyone approves it, #5183 and #5168 the MCP
// server's write and query schemas.

const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  while (cleanups.length) await cleanups.pop()!();
});

function inspector(options: Record<string, unknown> = {}) {
  const db = { all: async () => [] };
  const handle = createInspector({
    appId: "app",
    basePath: "/jazz",
    publicUrls: [],
    options,
    schema: () => ({
      todos: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
    }),
    permissions: {},
    db: () => db,
    dbAs: async () => db,
    sessions: () => [],
    hook: () => ({ by: "ops@example.com", write: true }),
    tokenSecret: async () => "a test secret that is long enough to sign tokens with",
    remoteAddress: () => undefined,
    serveUi: async () => null,
    env: { NODE_ENV: "production" },
  } as unknown as InspectorDeps);
  return async (body: unknown, token?: string) => {
    const response = await handle.handle(
      new Request("https://app.example.com/jazz/inspect/api", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(token ? { authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(body),
      }),
      "/inspect/api",
    );
    return { status: response.status, body: (await response.json()) as any };
  };
}

describe("the agent token's label (#5181)", () => {
  it("shows in whoami and every audit event of the token", async () => {
    const events: InspectEvent[] = [];
    const call = inspector({ audit: (event: InspectEvent) => events.push(event) });
    const minted = await call({ op: "mint_token", label: "debugging #123" });
    const whoami = await call({ op: "whoami" }, minted.body.token);
    expect(whoami.body).toMatchObject({ via: "token", label: "debugging #123" });
    expect(events.map((event) => [event.op, event.label])).toEqual([
      ["mint_token", "debugging #123"],
      ["whoami", "debugging #123"],
    ]);
    // A person signed in through the hook has no label.
    expect((await call({ op: "whoami" })).body.label).toBeUndefined();
  });

  it("is kept without control, separator or bidi characters, at most 200 long", async () => {
    const events: InspectEvent[] = [];
    const call = inspector({ audit: (event: InspectEvent) => events.push(event) });
    const label = `fix\u2028jazz inspect: admin\u0085(hook)\u202e write\u0007 ${"x".repeat(300)}`;
    const minted = await call({ op: "mint_token", label });
    const kept = (await call({ op: "whoami" }, minted.body.token)).body.label;
    expect(kept).toHaveLength(200);
    expect(kept.startsWith("fix jazz inspect: admin (hook) write x")).toBe(true);
    expect(kept).not.toMatch(/[\u0000-\u001f\u007f-\u009f\u2028\u2029\u202a-\u202e\u2066-\u2069]/);
    expect(events.every((event) => event.label === undefined || event.label === kept)).toBe(true);
  });

  it("is quoted in the default audit line, so it can't forge one", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const call = inspector();
    const minted = await call({ op: "mint_token", label: 'fix "it" \\ now' });
    await call({ op: "whoami" }, minted.body.token);
    const line = info.mock.calls.at(-1)![0] as string;
    expect(line).toBe('jazz inspect: ops@example.com (token "fix \\"it\\" \\\\ now") whoami');
  });
});

describe("a token's lifetime, checked before anyone approves (#5179)", () => {
  it("whoami tells the approval page the longest this app issues", async () => {
    expect((await inspector()({ op: "whoami" })).body.maxTokenTtlSeconds).toBe(86400);
    expect(
      (await inspector({ maxTokenTtlSeconds: 7200 })({ op: "whoami" })).body.maxTokenTtlSeconds,
    ).toBe(7200);
  });

  it("requestApproval and the CLI refuse less than a minute up front", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-inspect-ttl-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    const store = tokenStore({ JAZZ_CONFIG_DIR: dir });
    await expect(
      requestApproval("http://localhost:3000/jazz", { ttlSeconds: 30 }, { store }),
    ).rejects.toMatchObject({
      code: "bad_request",
      message: expect.stringMatching(/at least a minute/),
    });
    let out = "";
    const code = await runInspectCommand(
      ["login", "--ttl", "30s", "--no-browser", "--app", "http://localhost:3000/jazz"],
      { stdout: (text) => (out += text), stderr: () => {}, env: { JAZZ_CONFIG_DIR: dir } },
    );
    expect(code).toBe(1);
    expect(out).toMatch(/at least a minute/);
    expect(out).not.toMatch(/"step":"approve"/);
  });
});

describe("the MCP server's schemas (#5183, #5168)", () => {
  const schema = (name: string) => MCP_TOOLS.find((tool) => tool.name === name)!.inputSchema as any;

  it("types each write op, with kind naming insert, update or delete", () => {
    const items = schema("write").properties.ops.items;
    expect(items.oneOf.map((op: any) => [op.properties.kind.const, op.required])).toEqual([
      ["insert", ["kind", "table", "values"]],
      ["update", ["kind", "table", "id", "values"]],
      ["delete", ["kind", "table", "id"]],
    ]);
  });

  it("documents the query's where operators, select, orderBy and limit", () => {
    const query = schema("query").properties.query;
    expect(query.properties.where.description).toMatch(
      /eq, ne, gt, gte, lt, lte, contains, in, notIn, isNull/,
    );
    expect(query.properties.select.description).toMatch(/\$createdBy/);
    expect(query.properties.orderBy.items.prefixItems[1].enum).toEqual(["asc", "desc"]);
    expect(query.properties.limit).toMatchObject({ minimum: 1, maximum: 1000 });
    expect(query.additionalProperties).toBe(false);
  });

  it("a write op without a known kind is refused naming the kind field", async () => {
    const call = inspector();
    const answer = await call({
      op: "write",
      ops: [{ type: "insert", table: "todos", values: {} }],
    });
    expect(answer).toMatchObject({
      status: 400,
      body: { message: 'each op needs kind: "insert", "update" or "delete" (got null)' },
    });
    const wrong = await call({ op: "write", ops: [{ kind: "upsert", table: "todos" }] });
    expect(wrong.body.message).toMatch(/needs kind: .* \(got "upsert"\)/);
  });

  it("MCP lists tools whose schemas JSON-encode", async () => {
    const input = new PassThrough();
    const lines: any[] = [];
    const served = runMcpServer({ input, output: (line) => lines.push(JSON.parse(line)), env: {} });
    input.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" })}\n`);
    input.end();
    await served;
    expect(lines[0].result.tools.find((tool: any) => tool.name === "write").inputSchema).toEqual(
      schema("write"),
    );
  });
});
