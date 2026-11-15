/**
 * What Jazz's Rust loader refuses in a jazz.app.json, and the values it
 * keeps exact, as a TS app sees them through `testApp` (the same loader
 * `jazz-tools generate` and `createJazz` run).
 */
import { describe, expect, it } from "vitest";
import {
  compileTestApp,
  createPolicyTestApp,
  testApp,
  testPermissions,
} from "../../src/testing/index.js";

const HASH = "[0-9a-f]{64}";

describe("relations in jazz.app.json", () => {
  const users = { columns: { name: "string" } };

  it("refuses a relation named like a column of its table", () => {
    expect(() =>
      testApp({
        tables: {
          users,
          todos: {
            columns: { owner: "uuid" },
            relations: { owner: { rel: "users", column: "owner" } },
          },
        },
      }),
    ).toThrow(
      "jazz.app.json: tables.todos.relations.owner: table todos already has a column owner; a relation needs its own name, e.g. `ownerRow`",
    );
  });

  it("refuses a relation to an unknown table", () => {
    expect(() =>
      testApp({
        tables: {
          users,
          todos: {
            columns: { owner: "uuid" },
            relations: { ownerRow: { rel: "accounts", column: "owner" } },
          },
        },
      }),
    ).toThrow(
      new RegExp(
        `^jazz\\.app\\.json: migrations: invalid schema ${HASH}: \\$\\.todos\\.owner: reference target table 'accounts' does not exist$`,
      ),
    );
  });

  it("refuses a relation over a column that isn't UUID or UUID[]", () => {
    expect(() =>
      testApp({
        tables: {
          users,
          todos: {
            columns: { owner: "string" },
            relations: { ownerRow: { rel: "users", column: "owner" } },
          },
        },
      }),
    ).toThrow(
      new RegExp(
        `^jazz\\.app\\.json: migrations: invalid schema ${HASH}: \\$\\.todos\\.owner: reference source column must use UUID or UUID\\[\\] type$`,
      ),
    );
  });

  it("refuses a relation that leads elsewhere than its column references", () => {
    expect(() =>
      testApp({
        tables: {
          users,
          projects: { columns: { name: "string" } },
          todos: {
            columns: { owner: { type: "uuid", references: "projects" } },
            relations: { ownerRow: { rel: "users", column: "owner" } },
          },
        },
      }),
    ).toThrow(
      "jazz.app.json: tables.todos.relations.ownerRow: column owner references projects, so a relation over it leads to projects, not users",
    );
    // Where the column references the relation's table, it loads.
    expect(() =>
      testApp({
        tables: {
          users,
          todos: {
            columns: { owner: { type: "uuid", references: "users" } },
            relations: { ownerRow: { rel: "users", column: "owner" } },
          },
        },
      }),
    ).not.toThrow();
  });

  it("refuses a reverse relation that names no forward relation back", () => {
    expect(() =>
      testApp({
        tables: {
          users: {
            columns: { name: "string" },
            relations: { todos: { reverse: "todos", relation: "authorRow" } },
          },
          todos: {
            columns: { owner: "uuid" },
            relations: { ownerRow: { rel: "users", column: "owner" } },
          },
        },
      }),
    ).toThrow(
      "jazz.app.json: tables.users.relations.todos: todos has no forward relation authorRow to users",
    );
  });
});

describe("merge strategies in jazz.app.json", () => {
  it("refuses g-set on a column that isn't an array", () => {
    expect(() =>
      testApp({ tables: { notes: { columns: { tags: { type: "string", merge: "g-set" } } } } }),
    ).toThrow(
      new RegExp(
        `^jazz\\.app\\.json: migrations: invalid schema ${HASH}: \\$\\.notes\\.tags: g-set merge strategy is only supported on non-nullable ARRAY columns, got tags \\(Text, nullable=false\\)$`,
      ),
    );
  });

  it("refuses markdown on a column that isn't text", () => {
    expect(() =>
      testApp({ tables: { notes: { columns: { body: { type: "int", merge: "markdown" } } } } }),
    ).toThrow(
      new RegExp(
        `^jazz\\.app\\.json: migrations: invalid schema ${HASH}: \\$\\.notes\\.body: Markdown merge strategy is only supported on TEXT columns, got body \\(Integer\\)$`,
      ),
    );
  });

  it("accepts g-set on an array and markdown on text", () => {
    const columns = compileTestApp({
      tables: {
        notes: {
          columns: {
            tags: { type: { array: "string" }, merge: "g-set" },
            body: { type: "string", merge: "markdown" },
          },
        },
      },
    }).schema.schemas[0]!.schema as { tables: { notes: { columns: unknown[] } } };
    expect(columns.tables.notes.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "tags", merge_strategy: "GSet" }),
        expect.objectContaining({ name: "body", merge_strategy: "Markdown" }),
      ]),
    );
  });

  it("refuses an unknown merge strategy", () => {
    expect(() =>
      testApp({ tables: { notes: { columns: { body: { type: "string", merge: "crdt" } } } } }),
    ).toThrow(
      'jazz.app.json: tables.notes.columns.body.merge: must be "counter", "g-set", "text", "markdown" or "html"',
    );
  });
});

describe("bigint defaults", () => {
  const MAX = "9223372036854775807";
  const MIN = "-9223372036854775808";
  const app = testApp({
    tables: {
      counters: {
        columns: {
          label: "string",
          big: { type: "bigint", default: MAX },
          nested: { type: { array: "bigint" }, default: [MIN, "9007199254740993"] },
        },
      },
    },
  });

  it("keeps a decimal-string default exact, past 2^53 and at the i64 bounds", () => {
    const schema = compileTestApp({
      tables: { counters: { columns: { big: { type: "bigint", default: MAX } } } },
    }).schema.schemas[0]!.schema as { tables: { counters: { columns: unknown[] } } };
    expect(schema.tables.counters.columns).toEqual([
      expect.objectContaining({ name: "big", default: { type: "BigInt", value: MAX } }),
    ]);
  });

  it("fills omitted bigint columns with their exact defaults", async () => {
    const policyApp = await createPolicyTestApp(
      app,
      { tables: { counters: { read: "true", insert: "true" } } },
      expect,
    );
    try {
      const db = policyApp.as({
        issuer: "https://issuer.example",
        user_id: "bigint-defaults",
        claims: {},
        authMode: "external",
      });
      const row = await db.insert(app.counters, { label: "defaults" }).wait({ tier: "global" });
      expect(row.big).toBe(9223372036854775807n);
      expect(row.nested).toEqual([-9223372036854775808n, 9007199254740993n]);
      const [read] = await db.all(app.counters.where({ id: row.id }));
      expect(read).toMatchObject({
        big: 9223372036854775807n,
        nested: [-9223372036854775808n, 9007199254740993n],
      });
    } finally {
      await policyApp.shutdown();
    }
  }, 20_000);

  it.each([
    ["a JSON number", 42, "a number"],
    ["a fraction", "1.5", "a string"],
    ["a value past i64", "9223372036854775808", "a string"],
    ["a word", "lots", "a string"],
  ])("refuses %s as a bigint default", (_, value, kind) => {
    expect(() =>
      testApp({ tables: { counters: { columns: { big: { type: "bigint", default: value } } } } }),
    ).toThrow(
      `jazz.app.json: tables.counters.columns.big.default: ${kind} doesn't fit this column's type`,
    );
  });
});

// BC-U04b: the TS DSL compiled a relation's literals as bare JS values
// (`Literal: "individual"`), and createJazzContext normalized them into the
// engine's tagged values. jazz.permissions.json is compiled by the Rust
// loader, which emits tagged values itself; this checks that it does, and
// that the engine evaluates them.
describe("literals in relation policies", () => {
  const app = testApp({
    tables: {
      resources: {
        columns: { name: "string" },
        relations: { edges: { reverse: "edges", relation: "resourceRow" } },
      },
      edges: {
        columns: { resource: "uuid", kind: "string", role: "string" },
        relations: { resourceRow: { rel: "resources", column: "resource" } },
      },
    },
  });
  const permissions = {
    tables: {
      resources: {
        read: "exists edges(resource = this.id and kind = 'individual' and role = 'viewer')",
        insert: "true",
      },
      edges: { read: "true", insert: "true" },
    },
  };

  it("compiles them to the engine's tagged values", () => {
    const compiled = JSON.stringify(testPermissions(app, permissions).resources);
    expect(compiled).toContain('{"type":"Literal","value":{"type":"Text","value":"individual"}}');
    expect(compiled).toContain('{"type":"Literal","value":{"type":"Text","value":"viewer"}}');
    expect(compiled).not.toMatch(/"Literal":"/);
  });

  it("grants a read only where every literal matches", async () => {
    const policyApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = policyApp.as({
        issuer: "https://issuer.example",
        user_id: "literal-reader",
        claims: {},
        authMode: "external",
      });
      const grant = async (name: string, kind: string, role: string) => {
        const resource = await db.insert(app.resources, { name }).wait({ tier: "global" });
        await db.insert(app.edges, { resource: resource.id, kind, role }).wait({ tier: "global" });
      };
      await grant("granted", "individual", "viewer");
      await grant("group", "group", "viewer");
      await grant("editor", "individual", "editor");
      const visible = await db.all(app.resources, { tier: "remote" });
      expect(visible.map((resource) => resource.name)).toEqual(["granted"]);
    } finally {
      await policyApp.shutdown();
    }
  }, 20_000);
});
