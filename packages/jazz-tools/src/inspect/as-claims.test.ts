import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { schema as s } from "../schema-namespace.js";
import { runInspectCommand } from "./cli.js";
import { runMcpServer } from "./mcp.js";
import { inspectorLink, parseInspectorLink, readsClaims } from "./protocol.js";

// "View as user" used to run as a session with no claims, and nothing in the
// CLI, MCP or Inspector could pass any: policies that read claims then gave
// confidently wrong answers (E10, alpha.61).

const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!();
});

/** A fetch that records each request body sent to the Inspector API. */
function recording() {
  const bodies: any[] = [];
  const fetch = (async (_url: string, init?: RequestInit) => {
    bodies.push(JSON.parse(String(init?.body)));
    return Response.json({ rows: [], truncated: false, inspectorUrl: "http://x/jazz/inspect/" });
  }) as typeof globalThis.fetch;
  return { bodies, fetch };
}

describe("claims with `as`", () => {
  it("the CLI sends --claims with --as, and refuses --claims alone", async () => {
    const { bodies, fetch } = recording();
    let out = "";
    const io = { stdout: (text: string) => (out += text), stderr: () => {}, env: {}, fetch };
    const app = ["--app", "http://localhost:3000/jazz"];
    expect(
      await runInspectCommand(
        ["query", '{"table":"expenses"}', "--as", "m", "--claims", '{"role":"manager"}', ...app],
        io,
      ),
    ).toBe(0);
    expect(
      await runInspectCommand(["query", '{"table":"expenses"}', "--as", "m", ...app], io),
    ).toBe(0);
    expect(bodies.map((body) => body.as)).toEqual([
      { userId: "m", claims: { role: "manager" } },
      "m",
    ]);
    expect(
      await runInspectCommand(["query", '{"table":"expenses"}', "--claims", "{}", ...app], io),
    ).not.toBe(0);
    expect(out).toMatch(/--claims goes with --as/);
    // Not an object: refused here, whatever the backend's version.
    const sent = bodies.length;
    expect(
      await runInspectCommand(
        ["query", '{"table":"expenses"}', "--as", "m", "--claims", '"manager"', ...app],
        io,
      ),
    ).not.toBe(0);
    expect(out).toMatch(/--claims must be a JSON object/);
    expect(bodies).toHaveLength(sent);
  });

  it("MCP tools take claims next to as", async () => {
    const { bodies, fetch } = recording();
    const config = await mkdtemp(join(tmpdir(), "jazz-inspect-mcp-"));
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
    const call = (id: number, name: string, args: Record<string, unknown>) =>
      input.write(
        `${JSON.stringify({ jsonrpc: "2.0", id, method: "tools/call", params: { name, arguments: args } })}\n`,
      );
    input.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" })}\n`);
    call(2, "query", { query: { table: "expenses" }, as: "m", claims: { role: "manager" } });
    call(3, "write", { ops: [], as: "m", claims: { role: "manager" } });
    call(4, "query", { query: { table: "expenses" }, claims: { role: "manager" } });
    // Claims that aren't an object reach the backend as given, which refuses them.
    call(5, "query", { query: { table: "expenses" }, as: "m", claims: "manager" });
    input.end();
    await served;
    const tools = lines.find((line) => line.id === 1).result.tools;
    for (const name of ["query", "get", "write", "inspector_link"]) {
      expect(
        tools.find((tool: any) => tool.name === name).inputSchema.properties,
        name,
      ).toHaveProperty("claims");
    }
    // The server answers calls concurrently, so the requests reach the
    // backend in any order: compare them as a set.
    const sent = bodies.map((body) => JSON.stringify([body.op, body.as])).sort();
    expect(sent).toEqual(
      [
        ["query", { userId: "m", claims: { role: "manager" } }],
        ["write", { userId: "m", claims: { role: "manager" } }],
        ["query", { userId: "m", claims: "manager" }],
      ]
        .map((entry) => JSON.stringify(entry))
        .sort(),
    );
    expect(lines.find((line) => line.id === 4).result.isError).toBe(true);
  });

  it("deep links carry the claims, so the Inspector opens with them", () => {
    const as = { userId: "m", claims: { role: "manager", teams: ["a"] } };
    const link = inspectorLink("http://x/jazz/inspect/", { table: "expenses", as });
    expect(parseInspectorLink(new URL(link).hash).as).toEqual(as);
    expect(parseInspectorLink(new URL(inspectorLink("http://x/", { as: "m" })).hash).as).toBe("m");
    // A damaged claims parameter still views as the user.
    expect(parseInspectorLink("#as=m&claims=%%%").as).toBe("m");
  });

  it("readsClaims finds claims read anywhere in compiled permissions", () => {
    const app = s.defineApp(
      { expenses: s.table({ title: s.string(), owner: s.uuid() }, {}) },
      { migrations: "none" },
    );
    const byOwner = definePermissions(app, ({ policy, session }) => {
      policy.expenses.allowRead.where({ owner: session.user });
    });
    const byClaim = definePermissions(app, ({ policy, anyOf, session }) => {
      policy.expenses.allowRead.where(
        anyOf([{ owner: session.user }, session.where({ "claims.role": "manager" })]),
      );
    });
    const byClaimValue = definePermissions(app, ({ policy, session }) => {
      policy.expenses.allowUpdate.whereOld({ owner: session.claims.team as never });
    });
    const byClaimList = definePermissions(app, ({ policy, session }) => {
      policy.expenses.allowRead.where({ owner: { in: session.claims.owners as never } });
    });
    const sessionForms = [
      { "claims.role": { in: ["manager", "admin"] } },
      { "claims.teams": { contains: "finance" } },
      { "claims.suspended": { isNull: true } },
      { "claims.suspended": { isNull: false } },
    ];
    expect(readsClaims(byOwner)).toBe(false);
    expect(readsClaims(byClaim)).toBe(true);
    expect(readsClaims(byClaimValue)).toBe(true);
    expect(readsClaims(byClaimList)).toBe(true);
    for (const form of sessionForms) {
      const permissions = definePermissions(app, ({ policy, session }) => {
        policy.expenses.allowRead.where(session.where(form));
      });
      expect(readsClaims(permissions), JSON.stringify(form)).toBe(true);
    }
    expect(readsClaims(undefined)).toBe(false);
  });
});
