import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { DataError, classifyError } from "./errors.js";
import { compileSql, describePlan, parseSql, resolveColumn, type Statement } from "./sql.js";
import { parseDataArgs, resolveDataOptions, type DataCommandMode } from "./command.js";
import { formatError, formatData, resolveFormat } from "./output.js";
import { capabilities } from "./capabilities.js";
import { idFromSeed, schemaHashOf } from "./schema.js";
import { dataHelp } from "./help.js";

const app = s.defineApp({
  notes: s.table({
    text: s.string(),
    rank: s.int(),
    count: s.bigint().optional(),
    done: s.boolean().default(false),
    due: s.timestamp().optional(),
    status: s.enum("open", "closed").optional(),
    ownerId: s.ref("owners").optional(),
  }),
  owners: s.table({ name: s.string() }),
});
const context = { schemaHash: "h", schemaSource: "local:test" };
const compile = (sql: string, write = false) =>
  compileSql(parseSql(sql, { write }), app.wasmSchema, context);
const defaults = { appId: "test-app", serverUrl: "http://localhost:1625" };

function codeOf(run: () => unknown): string {
  try {
    run();
  } catch (error) {
    if (error instanceof DataError) return error.code;
    throw error;
  }
  throw new Error("expected a DataError");
}

function hintOf(run: () => unknown): string | undefined {
  try {
    run();
  } catch (error) {
    if (error instanceof DataError) return error.hint;
    throw error;
  }
  throw new Error("expected a DataError");
}

/** Mirrors the real flow: flags, then the statement they selected, then policy. */
function resolved(
  args: string[],
  mode: DataCommandMode = "sql",
  env: Record<string, string | undefined> = {},
) {
  const flags = parseDataArgs(args, defaults, env, mode);
  const statement =
    flags.sql === undefined
      ? undefined
      : parseSql(flags.sql, { write: flags.write || flags.explain, idSeed: flags.idSeed });
  return resolveDataOptions(flags, statement);
}

describe("Jazz SQL validation", () => {
  it.each([
    ["SELECT * FROM absent", "UNKNOWN_TABLE"],
    ["SELECT typo FROM notes", "UNKNOWN_COLUMN"],
    ["SELECT text, text FROM notes", "SQL_SYNTAX"],
    ["SELECT * FROM notes WHERE done = 'false'", "TYPE_MISMATCH"],
    ["SELECT * FROM notes WHERE rank = 2147483648", "TYPE_MISMATCH"],
    ["SELECT * FROM notes WHERE count = 9223372036854775808", "TYPE_MISMATCH"],
    ["SELECT * FROM notes WHERE status = 'missing'", "TYPE_MISMATCH"],
    ["SELECT * FROM notes WHERE text > 'a'", "UNSUPPORTED"],
    ["SELECT * FROM notes WHERE due = '2026-02-30T00:00:00Z'", "TYPE_MISMATCH"],
    ["SELECT * FROM notes WHERE rank = NULL", "SQL_SYNTAX"],
    ["SELECT * FROM notes WHERE rank IS NULL", "TYPE_MISMATCH"],
    ["SELECT * FROM notes ORDER BY typo", "UNKNOWN_COLUMN"],
    ["SELECT * FROM notes LIMIT -1", "SQL_SYNTAX"],
    ["SELECT * FROM notes LIMIT 9007199254740992", "SQL_SYNTAX"],
    ["SELECT text AS title FROM notes", "SQL_SYNTAX"],
    ["SELECT * FROM notes; DELETE FROM notes", "SQL_SYNTAX"],
    ["SELECT * FROM notes WHERE done = TRUE OR done = FALSE", "SQL_SYNTAX"],
    ["SELECT * FROM notes JOIN owners ON notes.id = owners.id", "UNSUPPORTED"],
    ["SELECT * FROM notes /* unfinished", "SQL_SYNTAX"],
    ["SELECT * FROM notes WHERE text = 'unfinished", "SQL_SYNTAX"],
  ])("rejects %s with %s", (sql, code) => {
    expect(codeOf(() => compile(sql))).toBe(code);
  });

  it("suggests the nearest table or column", () => {
    expect(hintOf(() => compile("SELECT * FROM note"))).toMatch(/Did you mean "notes"/);
    expect(hintOf(() => compile("SELECT txet FROM notes"))).toMatch(/Did you mean "text"/);
  });

  it.each([
    "INSERT INTO notes (text, rank) VALUES ('new', 1)",
    "UPDATE notes SET text = 'changed' WHERE id = '00000000-0000-0000-0000-000000000001'",
    "DELETE FROM notes WHERE id = '00000000-0000-0000-0000-000000000001'",
  ])("requires --write for %s while --explain can still inspect it", (sql) => {
    expect(codeOf(() => compile(sql))).toBe("READ_ONLY");
    // `--explain` compiles with the write guard relaxed because nothing runs.
    expect(compileSql(parseSql(sql, { write: true }), app.wasmSchema, context).kind).not.toBe(
      "schema",
    );
  });

  it.each([
    ["INSERT INTO notes (text) VALUES ('new')", "SQL_SYNTAX"],
    ["INSERT INTO notes (text, text, rank) VALUES ('a', 'b', 1)", "SQL_SYNTAX"],
    [
      "INSERT INTO notes (id, text, rank) VALUES ('00000000-0000-0000-0000-000000000001', 'a', 1)",
      "UNSUPPORTED",
    ],
    ["INSERT INTO notes (text, rank) VALUES ('a')", "SQL_SYNTAX"],
    ["INSERT INTO notes (text, rank) VALUES ('a', 1), ('b', 2)", "SQL_SYNTAX"],
    ["UPDATE notes SET text = 'all'", "SQL_SYNTAX"],
    ["UPDATE notes SET text = 'all' WHERE rank = 1", "SQL_SYNTAX"],
    ["UPDATE notes SET id = 'x' WHERE id = '00000000-0000-0000-0000-000000000001'", "UNSUPPORTED"],
    ["DELETE FROM notes WHERE id = 'bad-id'", "TYPE_MISMATCH"],
    [
      "DELETE FROM notes WHERE id = '00000000-0000-0000-0000-000000000001' AND rank = 1",
      "SQL_SYNTAX",
    ],
  ])("rejects invalid write %s with %s before execution", (sql, code) => {
    expect(codeOf(() => compile(sql, true))).toBe(code);
  });

  it("accepts quoted names, escaped strings, comments, and exact BIGINT values", () => {
    const result = compile(
      `-- setup\ninsert INTO "notes" ("text", rank, count, due) VALUES ('O''Brien; -- text', -2, 9223372036854775807, '2026-09-11T00:00:00Z'); /* end */`,
      true,
    );
    expect(result.kind).toBe("insert");
    if (result.kind !== "insert") throw new Error("Expected insert");
    expect(result.values).toMatchObject({
      text: "O'Brien; -- text",
      rank: -2,
      count: 9223372036854775807n,
      due: Date.parse("2026-09-11T00:00:00Z"),
    });
    expect(result.idSource).toBe("server");
  });
});

describe("id strategy", () => {
  it("derives a stable UUID from a seed so a retry cannot duplicate the row", () => {
    const seeded = compileSql(
      parseSql("INSERT INTO notes (text, rank) VALUES ('a', 1) WITH ID SEED 'retry-key'", {
        write: true,
      }),
      app.wasmSchema,
      context,
    );
    expect(seeded).toMatchObject({
      kind: "insert",
      idSource: "client",
      id: idFromSeed("retry-key"),
    });
    expect(idFromSeed("retry-key")).toBe(idFromSeed("retry-key"));
    expect(idFromSeed("retry-key")).not.toBe(idFromSeed("other-key"));
    expect(idFromSeed("retry-key")).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  });

  it("rejects --id-seed for statements that cannot use it", () => {
    expect(
      codeOf(() =>
        resolveDataOptions(
          parseDataArgs(["--id-seed", "x"], defaults, {}, "sql"),
          parseSql("SELECT * FROM notes"),
        ),
      ),
    ).toBe("USAGE");
  });
});

describe("explain plan", () => {
  const planContext = {
    schemaSource: "local:test",
    schemaHash: "h",
    auth: "backend-secret",
    appId: "a",
    serverUrl: "u",
  };

  it("reports the resolved projection and operator types", () => {
    const statement = parseSql(
      "SELECT rank, text FROM notes WHERE rank >= 1 AND text = 'x' ORDER BY rank DESC LIMIT 5",
    );
    const plan = describePlan(
      statement,
      compileSql(statement, app.wasmSchema, context),
      app.wasmSchema,
      planContext,
    );
    expect(plan).toMatchObject({
      statement: "select",
      table: "notes",
      projection: ["rank", "text"],
      limit: 5,
      executes: false,
      connects: false,
      atomic: false,
      schemaHash: "h",
      readTier: "remote",
      where: [
        { column: "rank", operator: "gte", resolvedType: "Integer" },
        { column: "text", operator: "eq", resolvedType: "Text" },
      ],
    });
  });

  it("resolves a mutation plan without executing it", () => {
    const statement = parseSql("INSERT INTO notes (text, rank) VALUES ('a', 1) WITH ID SEED 'k'", {
      write: true,
    });
    const plan = describePlan(
      statement,
      compileSql(statement, app.wasmSchema, context),
      app.wasmSchema,
      planContext,
    );
    expect(plan).toMatchObject({
      statement: "insert",
      table: "notes",
      idSource: "client",
      safeToRetry: true,
      executes: false,
      atomic: false,
      generatedId: idFromSeed("k"),
    });
  });
});

describe("capabilities", () => {
  const report = capabilities();

  it("describes the dialect from the same data the compiler uses", () => {
    expect(report).toMatchObject({ version: 1 });
    expect(report.whereOperators).toMatchObject({
      parsed: ["eq", "ne", "gt", "gte", "lt", "lte"],
      unsupportedSyntax: ["contains", "in", "notIn"],
    });
    expect(report.unsupported).toContain("joins");
    expect(report.unsupported).toContain("multi-row VALUES");
    expect(report.limits).toMatchObject({ maxStatementCount: 1 });
    expect(report.guarantees).toMatchObject({ atomicWrites: false, truncatedInTableFormat: true });
    expect((report.guarantees as { retries: string }).retries).toMatch(/ALREADY_EXISTS/);
    expect(report.exitCodes).toMatchObject({ "5": "timeout (a write may already have committed)" });
  });

  it("orders operators per column type consistently with compilation", () => {
    const types = report.columnTypes as Record<string, { operators: string[] }>;
    expect(types.Integer.operators).toEqual(["eq", "ne", "gt", "gte", "lt", "lte"]);
    expect(types.Text.operators).toEqual(["eq", "ne"]);
    expect(types.Array.operators).toEqual([]);
    expect(types.Row.operators).toEqual([]);
  });
});

describe("output formatting", () => {
  const result = {
    kind: "rows" as const,
    columns: ["text", "count", "bytes"],
    rows: [
      { text: "line\n\u001b[31m", count: 9223372036854775807n, bytes: new Uint8Array([0, 255]) },
    ],
  };

  it("defaults to machine-readable output when stdout is not a terminal", () => {
    expect(resolveFormat(undefined, false)).toBe("json");
    expect(resolveFormat(undefined, true)).toBe("table");
    expect(resolveFormat("jsonl", true)).toBe("jsonl");
  });

  it("preserves full machine-readable values and exact integers", () => {
    expect(JSON.parse(formatData(result, "json"))).toEqual([
      { text: "line\n\u001b[31m", count: "9223372036854775807", bytes: [0, 255] },
    ]);
    expect(JSON.parse(formatData(result, "jsonl"))).toEqual(
      JSON.parse(formatData(result, "json"))[0],
    );
    expect(formatData({ kind: "rows", columns: ["id"], rows: [] }, "jsonl")).toBe("");
    expect(formatData({ kind: "rows", columns: ["id"], rows: [] }, "json")).toBe("[]\n");
  });

  it("escapes controls, honours the cell budget, and prints structured results", () => {
    const text = formatData(result, "table");
    expect(text).not.toContain("\u001b");
    expect(text).toContain("line\\u000a\\u001b[31m");
    expect(formatData({ kind: "rows", columns: ["id", "text"], rows: [] }, "table")).toContain(
      "(0 rows)",
    );
    expect(formatData(result, "table", { maxCellWidth: 6 })).toContain("lin...");
    expect(formatData(result, "table", { maxCellWidth: 0 })).toContain("9223372036854775807");
    expect(text).toContain("(1 row)");
    expect(JSON.parse(formatData({ kind: "object", value: { ok: true } }, "json"))).toEqual({
      ok: true,
    });
  });
});

describe("error contract", () => {
  it("prints a stable code in text mode and structured JSON otherwise", () => {
    const error = new DataError("UNKNOWN_TABLE", 'Unknown table "note"', { hint: "Did you mean?" });
    expect(formatError(error, "table")).toBe(
      'UNKNOWN_TABLE: Unknown table "note"\nhint: Did you mean?\n',
    );
    expect(JSON.parse(formatError(error, "json"))).toEqual({
      error: {
        code: "UNKNOWN_TABLE",
        message: 'Unknown table "note"',
        hint: "Did you mean?",
        exitCode: 2,
      },
    });
  });

  it("classifies a repeated seeded INSERT as a conflict, not an internal failure", () => {
    const failure = classifyError(
      new Error('Insert failed: WriteError("encoding error: object already exists: 0-0")'),
    );
    expect(failure.code).toBe("ALREADY_EXISTS");
    expect(failure.exitCode).toBe(6);
    expect(failure.hint).toMatch(/committed/);
    expect(classifyError(new Error("Permissions head fetch failed: 401 Unauthorized")).code).toBe(
      "AUTH_REQUIRED",
    );
    expect(classifyError(new Error("denied by policy")).code).toBe("DENIED");
    expect(classifyError(new Error("socket hang up")).code).toBe("INTERNAL");
  });

  it("documents the dialect, the preview flags, and every exit code", () => {
    const help = dataHelp("sql");
    expect(help).toContain("--capabilities");
    expect(help).toContain("--explain");
    expect(help).toContain("Exit codes");
    expect(help).toContain("5  timeout");
    expect(help).toContain("6  the row already exists");
    expect(help).toContain("--id-seed");
    expect(help).toContain("--schema-dir");
    // The retired compatibility form is gone.
    expect(help).not.toContain("data query");
    expect(dataHelp("tables")).toContain("schema tables");
    expect(dataHelp("sql", true)).toContain("--capabilities");
  });
});

describe("data command arguments", () => {
  it("never treats an admin secret as a data credential", () => {
    expect(
      codeOf(() => resolved(["SELECT * FROM notes"], "sql", { JAZZ_ADMIN_SECRET: "admin-only" })),
    ).toBe("AUTH_REQUIRED");
  });

  it("selects JWT permissions even when a backend secret is inherited", () => {
    const local = ["--schema-dir", "/tmp/app"];
    expect(
      resolved(["SELECT * FROM notes", ...local, "--jwt", "user-token"], "sql", {
        JAZZ_BACKEND_SECRET: "backend",
      }).auth,
    ).toEqual({ jwt: "user-token" });
    expect(
      resolved(["SELECT * FROM notes", ...local], "sql", { JAZZ_BACKEND_SECRET: "backend" }).auth,
    ).toEqual({ backendSecret: "backend" });
    expect(
      codeOf(() =>
        resolved(["SELECT * FROM notes", ...local], "sql", {
          JAZZ_JWT_TOKEN: "user-token",
          JAZZ_BACKEND_SECRET: "backend",
        }),
      ),
    ).toBe("AUTH_CONFLICT");
    expect(
      codeOf(() => parseDataArgs(["--jwt", "u", "--backend-secret", "b"], defaults, {}, "sql")),
    ).toBe("USAGE");
  });

  it.each([
    [["--writ"], "USAGE"],
    [["--format=csv"], "USAGE"],
    [["--timeout=0"], "USAGE"],
    [["--max-cell-width=-1"], "USAGE"],
    [["--schema-hash=short"], "USAGE"],
    [["--schema-dir=x", "--schema-hash=current"], "USAGE"],
  ])("fails closed for %s with %s", (extra, code) => {
    expect(
      codeOf(() =>
        parseDataArgs(["SELECT 1", ...extra], defaults, { JAZZ_BACKEND_SECRET: "b" }, "sql"),
      ),
    ).toBe(code);
  });

  it("takes the app id from flags only, so a stray positional is SQL", () => {
    expect(
      codeOf(() => parseDataArgs(["one", "two"], defaults, { JAZZ_BACKEND_SECRET: "b" }, "sql")),
    ).toBe("USAGE");
    expect(
      codeOf(() =>
        parseDataArgs(
          ["--sql", "SELECT 1", "--file", "-"],
          defaults,
          { JAZZ_BACKEND_SECRET: "b" },
          "sql",
        ),
      ),
    ).toBe("USAGE");
  });

  it("refuses discovery-only flags on a statement and vice versa", () => {
    // In `describe` mode the table is the only positional; --sql is not a source.
    expect(codeOf(() => parseDataArgs(["--sql", "SELECT 1"], defaults, {}, "describe"))).toBe(
      "USAGE",
    );
    expect(codeOf(() => parseDataArgs([], defaults, {}, "describe"))).toBe("USAGE");
    expect(codeOf(() => parseDataArgs(["--write"], defaults, {}, "tables"))).toBe("USAGE");
    expect(codeOf(() => parseDataArgs(["--capabilities"], defaults, {}, "tables"))).toBe("USAGE");
    expect(codeOf(() => parseDataArgs(["--capabilities"], defaults, {}, "describe"))).toBe("USAGE");
  });

  it("rejects capabilities combined with a schema or a statement", () => {
    for (const extra of [
      ["SELECT 1", "--capabilities"],
      ["--capabilities", "--schema-dir", "."],
      ["--capabilities", "--write"],
    ]) {
      expect(codeOf(() => parseDataArgs(extra, defaults, {}, "sql"))).toBe("USAGE");
    }
    const only = parseDataArgs(["--capabilities"], defaults, {}, "sql");
    expect(only.capabilities).toBe(true);
    expect(only.sql).toBeUndefined();
  });
});

describe("schema source resolution", () => {
  it("reads a local schema by default for schema discovery", () => {
    expect(resolved([], "tables").schemaSource).toMatchObject({ kind: "local" });
    expect(resolved(["notes"], "describe").schemaSource).toMatchObject({ kind: "local" });
    expect(resolved(["--schema-dir", "/tmp/app"], "tables").schemaSource).toEqual({
      kind: "local",
      dir: "/tmp/app",
    });
  });

  it("needs no app, server, or credentials for local schema inspection", () => {
    for (const [args, mode] of [
      [["--schema-dir", "/tmp/app"], "tables"],
      [["notes", "--schema-dir", "/tmp/app"], "describe"],
      [["SHOW TABLES", "--schema-dir", "/tmp/app"], "sql"],
      [["DESCRIBE notes", "--schema-dir", "/tmp/app"], "sql"],
    ] as const) {
      const flags = parseDataArgs([...args], {}, {}, mode);
      const options = resolveDataOptions(flags, parseSql(flags.sql!));
      expect(options.auth).toBeUndefined();
      expect(options.adminSecret).toBeUndefined();
      expect(options.appId).toBeUndefined();
      expect(options.schemaSource).toMatchObject({ kind: "local" });
    }
  });

  it("requires an app, a server, and an admin secret for stored schema versions", () => {
    // Local discovery is the default, so a hash without a server is a usage error.
    expect(
      codeOf(() =>
        resolveDataOptions(
          parseDataArgs(["--schema-hash", "current"], {}, {}, "tables"),
          parseSql("SHOW TABLES"),
        ),
      ),
    ).toBe("USAGE");
    expect(
      codeOf(() =>
        resolveDataOptions(
          parseDataArgs(["--schema-hash", "current"], defaults, {}, "tables"),
          parseSql("SHOW TABLES"),
        ),
      ),
    ).toBe("AUTH_REQUIRED");
    expect(
      resolved(["--schema-hash", "current"], "tables", { JAZZ_ADMIN_SECRET: "a" }).schemaSource,
    ).toEqual({ kind: "remote-head" });
    // `schema describe` is local by default too, so it never demands credentials.
    expect(resolved(["notes"], "describe", { JAZZ_ADMIN_SECRET: "a" }).schemaSource).toMatchObject({
      kind: "local",
    });
  });

  it("reads the deployed schema for `sql` and never needs data credentials for discovery", () => {
    expect(resolved(["SHOW TABLES"], "sql", { JAZZ_ADMIN_SECRET: "admin" }).schemaSource).toEqual({
      kind: "remote-head",
    });
    expect(resolved(["SHOW TABLES"], "sql", { JAZZ_ADMIN_SECRET: "admin" }).auth).toBeUndefined();
    expect(codeOf(() => resolved(["SHOW TABLES"], "sql"))).toBe("AUTH_REQUIRED");
  });

  it("lets local sql reads resolve their schema offline but still requires a data credential", () => {
    const options = resolved(["SELECT * FROM notes", "--schema-dir", "/tmp/app"], "sql", {
      JAZZ_BACKEND_SECRET: "backend",
    });
    expect(options.schemaSource).toEqual({ kind: "local", dir: "/tmp/app" });
    expect(options.adminSecret).toBeUndefined();
    expect(codeOf(() => resolved(["SELECT * FROM notes", "--schema-dir", "/tmp/app"], "sql"))).toBe(
      "AUTH_REQUIRED",
    );
  });

  it("pins an explicit stored hash and proves local schema discovery offline", () => {
    const hash = "a".repeat(64);
    expect(
      resolved(["SELECT * FROM notes", "--schema-hash", hash], "sql", {
        JAZZ_BACKEND_SECRET: "b",
        JAZZ_ADMIN_SECRET: "a",
      }).schemaSource,
    ).toEqual({ kind: "remote-hash", hash });
    expect(resolved(["SHOW TABLES", "--schema-dir", "/tmp/app"], "sql").schemaSource).toEqual({
      kind: "local",
      dir: "/tmp/app",
    });
  });
});

describe("schema identity", () => {
  it("hashes structurally, independent of key order", () => {
    expect(schemaHashOf(app.wasmSchema)).toBe(schemaHashOf(app.wasmSchema));
    expect(schemaHashOf(app.wasmSchema)).toMatch(/^[a-f0-9]{64}$/);
    expect(schemaHashOf({})).not.toBe(schemaHashOf(app.wasmSchema));
  });

  it("resolves the implicit id column and refuses runtime path syntax", () => {
    expect(resolveColumn(app.wasmSchema, "notes", "id")).toMatchObject({
      name: "id",
      nullable: false,
    });
    expect(codeOf(() => resolveColumn(app.wasmSchema, "notes", "a.b"))).toBe("UNKNOWN_COLUMN");
    expect(codeOf(() => resolveColumn(app.wasmSchema, "notes", "$path"))).toBe("UNKNOWN_COLUMN");
  });

  it("declares every emitted describe key and keeps machine detail in `meta`", () => {
    const described = compile("DESCRIBE notes");
    expect(described.kind).toBe("schema");
    if (described.kind !== "schema") throw new Error("expected schema result");
    const rows = described.result.rows;
    const declared = new Set(described.result.columns);
    for (const key of Object.keys(rows[0]!)) expect(declared.has(key)).toBe(true);
    expect(rows.map((row) => row.column)).toEqual([
      "id",
      "text",
      "rank",
      "count",
      "done",
      "due",
      "status",
      "ownerId",
    ]);
    expect(rows[4]).toMatchObject({
      column: "done",
      type: "BOOLEAN",
      default: false,
      meta: { hasDefault: true, generated: false },
    });
    expect(rows[0]).toMatchObject({ column: "id", meta: { generated: true } });
    expect(rows[7]).toMatchObject({ column: "ownerId", references: "owners" });
  });

  it("keeps parsed statements narrowed for callers", () => {
    const statement: Statement = parseSql("SELECT text FROM notes");
    expect(statement.kind).toBe("select");
  });
});
