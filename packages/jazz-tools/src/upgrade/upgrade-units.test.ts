import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { entriesOf, parseAppJson } from "./app-json.js";
import { identityDifferences, isRustApp, renameValidate } from "./index.js";
import { dslAlpha } from "./fresh.js";
import { codemodSchema, codemodSpreadModule } from "./schema-ts.js";
import { loadTypeScript } from "./project.js";
import { payloadEnumColumns } from "./payload-enums.js";
import { readGeneratedNames, rowTypeName } from "./names.js";
import { FIXTURES } from "./upgrade-test-helpers.js";

const ts = loadTypeScript(join(FIXTURES, "..", "..", ".."));

describe("generated names", () => {
  it("match jazz-codegen's (crates/jazz-codegen/src/names.rs)", () => {
    for (const [table, name] of [
      ["chats", "Chat"],
      ["chatMembers", "ChatMember"],
      ["chat_members", "ChatMember"],
      ["canvases", "Canvas"],
      ["categories", "Category"],
      ["statuses", "Status"],
      ["boxes", "Box"],
      ["matches", "Match"],
      ["addresses", "Address"],
      ["people", "Person"],
      ["teamPeople", "TeamPerson"],
      ["cases", "Case"],
      ["todo", "Todo"],
      ["news", "New"],
      ["status", "Status"],
      ["analysis", "Analysis"],
      ["glass", "Glass"],
      ["series", "Sery"],
      ["profiles", "Profile"],
      ["strokes", "Stroke"],
    ]) {
      expect(rowTypeName(table!), table).toBe(name);
    }
  });

  it("are read from a generated module's Types", () => {
    const names = readGeneratedNames(
      [
        "export interface ChatMember { readonly id: string }",
        'export type TaskStatus = "a" | "b";',
        "interface Types extends AppTypes {",
        "  chatMembers: {",
        "    row: ChatMember;",
        "    insert: ChatMemberInsert;",
        "  };",
        "}",
        "export const app = bindApp<Types>({});",
      ].join("\n"),
    );
    expect(names.tables).toEqual({
      chatMembers: { row: "ChatMember", insert: "ChatMemberInsert" },
    });
    expect([...names.exports].sort()).toEqual(["ChatMember", "TaskStatus", "app"]);
  });
});

describe("jazz.app.json reading", () => {
  it("keeps integer-like keys in file order", () => {
    const text = '{ "history": { "900000000002": { "tables": {} }, "5": {}, "1": {} } }\n';
    const app = parseAppJson(text);
    expect(entriesOf(app.history as never).map(([k]) => k)).toEqual(["900000000002", "5", "1"]);
  });
});

describe("scripts", () => {
  it("call `generate` where they called `validate`", () => {
    expect(renameValidate("jazz-tools validate && vite build")).toBe(
      "jazz-tools generate && vite build",
    );
    expect(renameValidate("node ../../packages/jazz-tools/dist/cli.js validate")).toBe(
      "node ../../packages/jazz-tools/dist/cli.js generate",
    );
    expect(renameValidate("sh -c 'npx jazz-tools validate --strict-provenance'")).toBe(
      "sh -c 'npx jazz-tools generate --strict-provenance'",
    );
    expect(renameValidate("echo validate")).toBe("echo validate");
  });

  it("know which versions have the DSL", () => {
    expect(dslAlpha("2.0.0-alpha.64")).toBe(64);
    expect(dslAlpha("2.0.0-alpha.65")).toBe(65);
    expect(dslAlpha("2.0.0-alpha.66")).toBeNull();
    expect(dslAlpha("2.0.0")).toBeNull();
  });
});

describe("enum columns with cases", () => {
  it("are found in the current tables, as array elements too, optional or not; history and migrations pass", () => {
    const cases = { bug: { severity: "int" }, feature: { area: "string" } };
    const app = parseAppJson(
      JSON.stringify({
        format: 1,
        tables: {
          tickets: {
            columns: {
              title: "string",
              kind: { enum: cases, optional: true },
              status: { enum: ["a", "b"] },
            },
          },
          notes: { columns: { kinds: { array: { enum: cases } } } },
        },
        history: { "0123456789ab": { tables: { old: { columns: { kind: { enum: cases } } } } } },
        migrations: [
          { from: "0123456789ab", migrate: { old: { kind: { drop: { enum: cases } } } } },
        ],
      }),
    );
    expect(payloadEnumColumns(app)).toEqual(["tickets.kind", "notes.kinds"]);
  });

  it("in older versions only don't stop an app that has moved off them", () => {
    const cases = { bug: { severity: "int" }, feature: { area: "string" } };
    const app = parseAppJson(
      JSON.stringify({
        format: 1,
        tables: {
          tickets: {
            columns: { kind: { enum: ["bug", "feature"] }, severity: "int?", area: "string?" },
          },
        },
        history: {
          "0123456789ab": { tables: { tickets: { columns: { kind: { enum: cases } } } } },
        },
        migrations: [
          { from: "0123456789ab", migrate: { tickets: { kind: { drop: { enum: cases } } } } },
        ],
      }),
    );
    expect(payloadEnumColumns(app)).toEqual([]);
  });
});

describe("the schema.ts codemod", () => {
  const codemod = (text: string, exports = new Map()) =>
    codemodSchema({
      ts,
      file: "/app/schema.ts",
      text,
      exports,
      extrasSpecifier: "./app-extras",
      resolve: (specifier) =>
        specifier.startsWith("./migrations")
          ? "/app/migrations/index.ts"
          : `/app/${specifier.slice(2)}.ts`,
      root: "/app",
    });

  it("turns `defineApp(schema, …)` into `export const schema = s.defineSchema({…})`, keeping comments", () => {
    const out = codemod(
      `import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

// The tables.
const schema = {
  // Every todo.
  todos: s.table({ title: s.string() }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema, { migrations });
export type Todo = s.RowOf<typeof app.todos>;
export const LIMIT = 10;
`,
      new Map([
        ["app", { module: "generated", name: "app", typeOnly: false }],
        ["Todo", { module: "generated", name: "Todo", typeOnly: true }],
        ["LIMIT", { module: "extras", name: "LIMIT", typeOnly: false }],
      ]),
    );
    expect(out.text).toBe(`import { schema as s } from "jazz-tools";

// The tables.
export const schema = s.defineSchema({
  // Every todo.
  todos: s.table({ title: s.string() }),
});
`);
  });

  it("renames another tables name and imports moved constants back", () => {
    const out = codemod(
      `import { schema as s } from "jazz-tools";

export const STATUSES = ["open", "done"] as const;
const tables = { todos: s.table({ status: s.enum(...STATUSES) }) };
export const app = s.defineApp(tables, { migrations: "none" });
`,
      new Map([
        ["app", { module: "generated", name: "app", typeOnly: false }],
        ["STATUSES", { module: "extras", name: "STATUSES", typeOnly: false }],
      ]),
    );
    expect(out.text).toBe(`import { schema as s } from "jazz-tools";
import { STATUSES } from "./app-extras";

export const schema = s.defineSchema({ todos: s.table({ status: s.enum(...STATUSES) }) });
`);
  });

  it("puts the app-extras import after the last import that stays, not the removed migrations one", () => {
    const exports = new Map([
      ["app", { module: "generated", name: "app", typeOnly: false }],
      ["STATUSES", { module: "extras", name: "STATUSES", typeOnly: false }],
    ]);
    const out = codemod(
      `import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

export const STATUSES = ["open", "done"] as const;
const schema = { todos: s.table({ status: s.enum(...STATUSES) }) };
export const app = s.defineApp(schema, { migrations });
`,
      exports,
    );
    expect(out.text).toBe(`import { schema as s } from "jazz-tools";
import { STATUSES } from "./app-extras";

export const schema = s.defineSchema({ todos: s.table({ status: s.enum(...STATUSES) }) });
`);
  });

  it("finds spread tables modules and imports their `tables`", () => {
    const out = codemod(
      `import { schema as s } from "jazz-tools";
import { schema as authSchema } from "./auth";

const schema = { ...authSchema, todos: s.table({ title: s.string() }) };
export const app = s.defineApp(schema);
`,
      new Map([["app", { module: "generated", name: "app", typeOnly: false }]]),
    );
    expect(out.spreads).toEqual([
      { file: "/app/auth.ts", specifier: "./auth", local: "authSchema", exported: "schema" },
    ]);
    expect(out.text).toContain(`import { tables as authSchema } from "./auth";`);
  });

  it("rewrites a spread module to export `tables` and plain permissions", () => {
    const out = codemodSpreadModule({
      ts,
      file: "/app/auth.ts",
      exported: "schema",
      text: `import { schema as s } from "jazz-tools";

export const schema = { users: s.table({ name: s.string() }) };

const authApp = s.defineApp(schema, { migrations: "none" });

export const permissions = s.definePermissions(authApp, ({ policy }) => {
  policy.users.allowRead.never();
});
`,
      permissionsText: "export const permissions = { users: { read: false } };",
    });
    expect(out.hadPermissions).toBe(true);
    expect(out.text).toBe(`import { schema as s } from "jazz-tools";

export const tables = { users: s.table({ name: s.string() }) };

export const permissions = { users: { read: false } };
`);
  });
});

describe("spread tables modules", () => {
  it("are found by what the schema spreads, whatever they call their tables and permissions", () => {
    const out = codemodSpreadModule({
      ts,
      file: "/app/auth-schema.ts",
      exported: "betterAuthSchema",
      text: `import { schema as s } from "jazz-tools";

export const betterAuthSchema = { users: s.table({ name: s.string() }) };

type BetterAuthSchema = s.Schema<typeof betterAuthSchema>;
const betterAuthApp: s.App<BetterAuthSchema> = s.defineApp(betterAuthSchema, { migrations: "none" });
export const wasmSchema = betterAuthApp.wasmSchema;

export const betterAuthPermissions = s.definePermissions(betterAuthApp, ({ policy }) => {
  policy.users.allowRead.never();
});
`,
      permissionsText: "export const permissions = { users: { read: false } };",
    });
    expect(out.dropped).toEqual(["wasmSchema"]);
    expect(out.text).toBe(`import { schema as s } from "jazz-tools";

export const tables = { users: s.table({ name: s.string() }) };

export const permissions = { users: { read: false } };
`);
  });

  it("import as `tables` from the schema", () => {
    const out = codemodSchema({
      ts,
      file: "/app/schema.ts",
      text: `import { schema as s } from "jazz-tools";
import { betterAuthSchema } from "./auth-schema";

const schema = { ...betterAuthSchema, todos: s.table({ title: s.string() }) };
export const app = s.defineApp(schema);
`,
      exports: new Map([["app", { module: "generated", name: "app", typeOnly: false }]]),
      extrasSpecifier: "./app-extras",
      resolve: (specifier) => `/app/${specifier.slice(2)}.ts`,
      root: "/app",
    });
    expect(out.spreads[0]!.exported).toBe("betterAuthSchema");
    expect(out.text).toContain('import { tables as betterAuthSchema } from "./auth-schema";');
  });
});

describe("what used the app", () => {
  it("leaves schema.ts with it (a query moved to app-extras.ts with its row type)", () => {
    const out = codemodSchema({
      ts,
      file: "/app/schema.ts",
      text: `import { schema as s } from "jazz-tools";

const schema = { stops: s.table({ venueId: s.uuid() }) };
export const app = s.defineApp(schema);

const stopWithVenueQuery = app.stops.include({ venue: true });

export type StopWithVenue = s.RowOf<typeof stopWithVenueQuery>;
`,
      exports: new Map([
        ["app", { module: "generated", name: "app", typeOnly: false }],
        ["StopWithVenue", { module: "extras", name: "StopWithVenue", typeOnly: true }],
      ]),
      extrasSpecifier: "./app-extras",
      resolve: (specifier) => `/app/${specifier.slice(2)}.ts`,
      root: "/app",
    });
    expect(out.text).toBe(`import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({ stops: s.table({ venueId: s.uuid() }) });
`);
  });
});

describe("the deployment check", () => {
  it("names what changed", () => {
    const before = { bundle: "b1", current: "h2", schemas: ["h1", "h2"] };
    expect(identityDifferences(before, { ...before, schemas: ["h2", "h1"] })).toEqual([]);
    expect(
      identityDifferences(before, { bundle: "b2", current: "h3", schemas: ["h1", "h3"] }),
    ).toEqual([
      "current schema hash h2 → h3",
      "schema versions [h1, h2] → [h1, h3]",
      "bundle id b1 → b2",
    ]);
  });
});

describe("Rust apps", () => {
  it("are crates that depend on jazz-app-support or commit src/jazz_app.rs, as `jazz-tools generate` tells", () => {
    const dir = mkdtempSync(join(tmpdir(), "jazz-upgrade-rust-"));
    try {
      expect(isRustApp(dir)).toBe(false);
      writeFileSync(join(dir, "Cargo.toml"), '[workspace]\nmembers = ["a"]\n');
      expect(isRustApp(dir), "a workspace root").toBe(false);
      writeFileSync(
        join(dir, "Cargo.toml"),
        '[package]\nname = "x"\n\n[dependencies]\nserde = "1"\n',
      );
      expect(isRustApp(dir), "an unrelated crate").toBe(false);
      writeFileSync(
        join(dir, "Cargo.toml"),
        '[package]\nname = "x"\n\n[dependencies]\njazz-app-support = { path = "../jazz-app-support" }\n',
      );
      expect(isRustApp(dir)).toBe(true);
      writeFileSync(join(dir, "Cargo.toml"), '[package]\nname = "x"\n');
      mkdirSync(join(dir, "src"));
      writeFileSync(join(dir, "src", "jazz_app.rs"), "// generated\n");
      expect(isRustApp(dir), "committed bindings").toBe(true);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
