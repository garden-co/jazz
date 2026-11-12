import { describe, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { schema as s } from "../schema-namespace.js";
import { createInspector, type InspectEvent, type InspectorDeps } from "./inspect.js";

// Viewing as a user, and explaining why they may read each row, with stub
// dependencies (inspect.integration.test.ts runs both on the real runtime).
// E10 (alpha.61): viewing as a user ran as a session with no claims, so
// policies that read claims answered as if the user had none, silently.

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

const FIRST = "11111111-1111-4111-8111-111111111111";
const SECOND = "22222222-2222-4222-8222-222222222222";

function inspector(
  permissions: unknown,
  overrides: { clauses?: number; branchBy?: string[]; redact?: Record<string, string[]> } = {},
) {
  const sessions: unknown[] = [];
  const events: InspectEvent[] = [];
  const explained: { ids: string[] }[] = [];
  let reads = 0;
  const db = {
    all: async () => {
      reads += 1;
      return [
        { id: FIRST, title: "a" },
        { id: SECOND, title: "b" },
      ];
    },
    explainReadClauses: (_table: unknown, ids: string[]) => {
      explained.push({ ids });
      // The second row went away between the two reads.
      return { clauses: overrides.clauses ?? 2, rows: [{ id: ids[0]!, holds: [false, true] }] };
    },
  };
  const deps = {
    appId: "app",
    basePath: "/jazz",
    publicUrls: [],
    options: {
      audit: (event: InspectEvent) => events.push(event),
      ...(overrides.redact ? { redact: overrides.redact } : {}),
    },
    schema: () => ({
      expenses: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "owner", column_type: { type: "Uuid" }, nullable: false },
        ],
        ...(overrides.branchBy ? { branchBy: overrides.branchBy } : {}),
      },
    }),
    permissions,
    db: () => db,
    dbAs: async (user: unknown) => {
      sessions.push(user);
      return db;
    },
    sessions: () => [],
    hook: () => ({ by: "ops@example.com", write: true }),
    tokenSecret: async () => "a test secret that is long enough to sign tokens with",
    remoteAddress: () => undefined,
    serveUi: async () => null,
    env: { NODE_ENV: "production" },
  } as unknown as InspectorDeps;
  const handle = createInspector(deps);
  const call = async (body: unknown, token?: string) => {
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
  return { call, sessions, events, explained, reads: () => reads };
}

const query = { table: "expenses" };

describe("viewing as a user when the permissions read claims", () => {
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
});

describe("explain", () => {
  it("explains every row the query matches, by clause, for the user", async () => {
    const { call, explained } = inspector(readsClaims);
    const as = { userId: "manager", claims: { role: "manager" } };
    const answer = await call({ op: "explain", query, as });
    expect(answer.status).toBe(200);
    expect(answer.body.clauses).toEqual([
      { index: 0, text: "owner = session.user" },
      { index: 1, text: 'session.claims.role = "manager"' },
    ]);
    expect(explained).toEqual([{ ids: [FIRST, SECOND] }]);
    expect(
      answer.body.rows.map((row: any) => [row.id, row.allowed, row.by, row.row.title]),
    ).toEqual([[FIRST, true, [1], "a"]]);
    expect(answer.body.warning).toBeUndefined();
    expect(answer.body.inspectorUrl).toMatch(/#q=.*as=manager&claims=/);

    expect((await call({ op: "explain", query })).status).toBe(400);
  });

  it("refuses when its clauses and the store's don't line up", async () => {
    const { call } = inspector(readsClaims, { clauses: 3 });
    const answer = await call({ op: "explain", query, as: "manager" });
    expect(answer.status).toBe(500);
    expect(answer.body.message).toMatch(
      /has 2 clause\(s\), but the store judges reads by one with 3/,
    );
  });

  it("says a table with branches isn't supported yet, before reading it", async () => {
    const { call, reads } = inspector(readsClaims, { branchBy: ["workspace"] });
    const answer = await call({ op: "explain", query, as: "manager" });
    expect(answer).toMatchObject({ status: 501, body: { error: "not_supported" } });
    expect(reads()).toBe(0);
  });

  it("doesn't tell a limited agent token which clause let a row through", async () => {
    const cases = [
      { scope: { tables: ["expenses"] }, redact: undefined, showsText: false },
      { scope: {}, redact: { expenses: ["owner"] }, showsText: true },
    ];
    for (const { scope, redact, showsText } of cases) {
      const { call } = inspector(readsClaims, redact ? { redact } : {});
      const minted = await call({ op: "mint_token", ...scope });
      expect(minted.status, JSON.stringify(minted.body)).toBe(200);
      const answer = await call({ op: "explain", query, as: "manager" }, minted.body.token);
      expect(answer.status, JSON.stringify(answer.body)).toBe(200);
      expect(answer.body.rows.map((row: any) => [row.allowed, "by" in row])).toEqual([
        [true, false],
      ]);
      // A table-scoped token isn't shown the policy's text either.
      expect(answer.body.clauses.every((clause: any) => "text" in clause)).toBe(showsText);
    }
  });
});
