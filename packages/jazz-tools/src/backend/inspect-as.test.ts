import { describe, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { schema as s } from "../schema-namespace.js";
import { createInspector, type InspectEvent, type InspectorDeps } from "./inspect.js";

// E10 (alpha.61): viewing as a user ran as a session with no claims, so
// policies that read claims answered as if the user had none, silently.
// (inspect.integration.test.ts checks the rows against the real runtime.)

const app = s.defineApp(
  { expenses: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const readsClaims = definePermissions(app, ({ policy, anyOf, session }) => {
  policy.expenses.allowRead.where(
    anyOf([{ owner: session.user }, session.where({ "claims.role": "manager" })]),
  );
});
const readsNoClaims = definePermissions(app, ({ policy, session }) => {
  policy.expenses.allowRead.where({ owner: session.user });
});

function inspector(permissions: unknown) {
  const sessions: unknown[] = [];
  const events: InspectEvent[] = [];
  const explained: { ids: string[] }[] = [];
  const db = {
    all: async () => [
      { id: "11111111-1111-4111-8111-111111111111", title: "a" },
      { id: "22222222-2222-4222-8222-222222222222", title: "b" },
    ],
    explainReadClauses: (_table: unknown, ids: string[]) => {
      explained.push({ ids });
      // The second row went away between the two reads.
      return [{ id: ids[0]!, holds: [false, true] }];
    },
  };
  const deps = {
    appId: "app",
    basePath: "/jazz",
    publicUrls: [],
    options: { audit: (event: InspectEvent) => events.push(event) },
    schema: () => ({
      expenses: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
    }),
    permissions,
    db: () => db,
    dbAs: async (user: unknown) => {
      sessions.push(user);
      return db;
    },
    sessions: () => [],
    hook: () => ({ by: "ops@example.com", write: true }),
    tokenSecret: async () => null,
    remoteAddress: () => undefined,
    serveUi: async () => null,
    env: { NODE_ENV: "production" },
  } as unknown as InspectorDeps;
  const handle = createInspector(deps);
  const call = async (body: unknown) => {
    const response = await handle.handle(
      new Request("https://app.example.com/jazz/inspect/api", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
      "/inspect/api",
    );
    return { status: response.status, body: (await response.json()) as any };
  };
  return { call, sessions, events, explained };
}

describe("viewing as a user when the permissions read claims", () => {
  const query = { table: "expenses" };

  it("warns when no claims were given, and runs with the claims that were", async () => {
    const { call, sessions, events } = inspector(readsClaims);
    const bare = await call({ op: "query", query, as: "manager" });
    expect(bare.status).toBe(200);
    expect(bare.body.warning).toMatch(/read session claims.*none were given for manager/);

    const as = { userId: "manager", claims: { role: "manager" } };
    const withClaims = await call({ op: "query", query, as });
    expect(withClaims.body.warning).toBeUndefined();
    expect(sessions).toEqual([{ userId: "manager" }, as]);
    // The audit log tells the two apart by the claims' names (never their values).
    expect(events.slice(0, 2).map((event) => [event.as, event.asClaims])).toEqual([
      ["manager", undefined],
      ["manager", ["role"]],
    ]);

    expect((await call({ op: "query", query })).body.warning).toBeUndefined();
    expect((await call({ op: "query", query, as: { userId: "m", claims: [1] } })).status).toBe(400);
  });

  it("doesn't warn when they read none", async () => {
    const { call } = inspector(readsNoClaims);
    expect((await call({ op: "query", query, as: "alice" })).body.warning).toBeUndefined();
  });

  it("explains every row the query matches, by clause, for the user", async () => {
    const { call, explained } = inspector(readsClaims);
    const as = { userId: "manager", claims: { role: "manager" } };
    const answer = await call({ op: "explain", query, as });
    expect(answer.status).toBe(200);
    expect(answer.body.clauses).toEqual([
      { index: 0, text: "owner = session.user" },
      { index: 1, text: 'session.claims.role = "manager"' },
    ]);
    expect(explained).toEqual([
      {
        ids: ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"],
      },
    ]);
    expect(
      answer.body.rows.map((row: any) => [row.id, row.allowed, row.by, row.row.title]),
    ).toEqual([["11111111-1111-4111-8111-111111111111", true, [1], "a"]]);
    expect(answer.body.warning).toBeUndefined();
    expect(answer.body.inspectorUrl).toMatch(/#q=.*as=manager&claims=/);

    expect((await call({ op: "explain", query })).status).toBe(400);
  });
});
