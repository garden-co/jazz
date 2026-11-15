import { describe, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { schema as s } from "../schema-namespace.js";
import { createInspector, type InspectorDeps } from "./inspect.js";

// explain through an inherited clause ("the row projectId points to allows
// read"): each row names its parent and that parent's own verdict, one level.

const app = s.defineApp(
  {
    projects: s.table({ owner: s.uuid() }, {}),
    tasks: s.table(
      { title: s.string(), projectId: s.uuid().optional() },
      { project: s.rel("projects", "projectId") },
    ),
  },
  { migrations: "none" },
);
const permissions = definePermissions(app, ({ policy, anyOf, session, allowedTo }) => {
  policy.projects.allowRead.where({ owner: session.user });
  policy.tasks.allowRead.where(anyOf([allowedTo.read("project"), { title: "public" }]));
});

const MINE = "11111111-1111-4111-8111-111111111111";
const THEIRS = "22222222-2222-4222-8222-222222222222";
const tasks = [
  { id: "a0000000-0000-4000-8000-000000000001", title: "mine", projectId: MINE },
  { id: "a0000000-0000-4000-8000-000000000002", title: "theirs", projectId: THEIRS },
  { id: "a0000000-0000-4000-8000-000000000003", title: "public", projectId: null },
];

function inspector(
  options: {
    redact?: Record<string, string[]>;
    permissions?: unknown;
    parents?: "fail" | { clauses: number };
  } = {},
) {
  const asked: Record<string, string[][]> = {};
  const db = {
    all: async () => tasks,
    explainReadClauses: (table: { _table: string }, ids: string[]) => {
      (asked[table._table] ??= []).push(ids);
      if (table._table === "projects") {
        if (options.parents === "fail") throw new Error("the store can't explain projects");
        return {
          clauses: options.parents?.clauses ?? 1,
          rows: ids.map((id) => ({ id, holds: [id === MINE] })),
        };
      }
      return {
        clauses: 2,
        rows: ids.map((id) => {
          const task = tasks.find((row) => row.id === id)!;
          return { id, holds: [task.projectId === MINE, task.title === "public"] };
        }),
      };
    },
  };
  const handle = createInspector({
    appId: "app",
    basePath: "/jazz",
    publicUrls: [],
    options: options.redact ? { redact: options.redact } : {},
    schema: () => app.wasmSchema,
    permissions: options.permissions ?? permissions,
    db: () => db,
    dbAs: async () => db,
    sessions: () => [],
    hook: () => ({ by: "ops@example.com", write: false }),
    tokenSecret: async () => "a test secret that is long enough to sign tokens with",
    remoteAddress: () => undefined,
    serveUi: async () => null,
    env: { NODE_ENV: "production" },
  } as unknown as InspectorDeps);
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
  return { call, asked };
}

describe("explain through an inherited clause", () => {
  it("names each row's parent and the parent's own verdict", async () => {
    const { call, asked } = inspector();
    const answer = await call({ op: "explain", query: { table: "tasks" }, as: "alice" });
    expect(answer.status).toBe(200);
    expect(answer.body.clauses.map((clause: any) => clause.text)).toEqual([
      "the row projectId points to allows read",
      'title = "public"',
    ]);
    expect(answer.body.inherited).toEqual({
      0: {
        table: "projects",
        column: "projectId",
        clauses: [{ index: 0, text: "owner = session.user" }],
      },
    });
    expect(
      answer.body.rows.map((row: any) => [row.row.title, row.allowed, row.by, row.via]),
    ).toEqual([
      ["mine", true, [0], { 0: { id: MINE, allowed: true, by: [0] } }],
      ["theirs", false, [], { 0: { id: THEIRS, allowed: false, by: [] } }],
      ["public", true, [1], { 0: { id: null, allowed: false } }],
    ]);
    // The parents, once each, in one call.
    expect(asked.projects).toEqual([[MINE, THEIRS]]);
  });

  it("leaves parents out for a token with redacted columns in their table", async () => {
    // Which parent clause held tells about what it reads: projects.owner.
    const { call, asked } = inspector({ redact: { projects: ["owner"] } });
    const minted = await call({ op: "mint_token" });
    const answer = await call(
      { op: "explain", query: { table: "tasks" }, as: "alice" },
      minted.body.token,
    );
    expect(answer.status, JSON.stringify(answer.body)).toBe(200);
    expect(answer.body.rows.map((row: any) => [row.row.title, row.allowed, row.by])).toEqual([
      ["mine", true, [0]],
      ["theirs", false, []],
      ["public", true, [1]],
    ]);
    expect(answer.body.inherited).toBeUndefined();
    expect(answer.body.rows.every((row: any) => row.via === undefined)).toBe(true);
    expect(asked.projects).toBeUndefined();
  });

  it("asks once for two clauses through the same column", async () => {
    const twice = definePermissions(app, ({ policy, anyOf, session, allowedTo }) => {
      policy.projects.allowRead.where({ owner: session.user });
      policy.tasks.allowRead.where(anyOf([allowedTo.read("project"), allowedTo.read("project")]));
    });
    const { call, asked } = inspector({ permissions: twice });
    const answer = await call({ op: "explain", query: { table: "tasks" }, as: "alice" });
    expect(Object.keys(answer.body.inherited)).toEqual(["0", "1"]);
    expect(asked.projects).toHaveLength(1);
  });

  it("answers the rows' own verdicts when a parent can't be explained", async () => {
    for (const parents of ["fail", { clauses: 2 }] as const) {
      const { call } = inspector({ parents });
      const answer = await call({ op: "explain", query: { table: "tasks" }, as: "alice" });
      expect(answer.status, JSON.stringify(parents)).toBe(200);
      expect(answer.body.inherited).toBeUndefined();
      expect(answer.body.rows.map((row: any) => [row.allowed, row.via])).toEqual([
        [true, undefined],
        [false, undefined],
        [true, undefined],
      ]);
    }
  });

  it("leaves parents out for a token that may not see their table", async () => {
    const { call, asked } = inspector();
    const minted = await call({ op: "mint_token", tables: ["tasks"] });
    const answer = await call(
      { op: "explain", query: { table: "tasks" }, as: "alice" },
      minted.body.token,
    );
    expect(answer.status, JSON.stringify(answer.body)).toBe(200);
    expect(answer.body.inherited).toBeUndefined();
    expect(answer.body.rows.every((row: any) => row.via === undefined)).toBe(true);
    expect(asked.projects).toBeUndefined();
  });
});
