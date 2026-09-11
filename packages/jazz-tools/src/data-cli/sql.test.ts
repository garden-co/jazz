import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { compileSql, parseSql } from "./sql.js";
import { parseDataArgs } from "./command.js";
import { formatData } from "./output.js";

const app = s.defineApp({
  notes: s.table({
    text: s.string(),
    rank: s.int(),
    count: s.bigint().optional(),
    done: s.boolean().default(false),
    due: s.timestamp().optional(),
    status: s.enum("open", "closed").optional(),
  }),
});
const compile = (sql: string, write = false) => compileSql(parseSql(sql, write), app.wasmSchema);
const defaults = { appId: "test-app", serverUrl: "http://localhost:1625" };
const args = ["--sql", "SELECT * FROM notes"];

describe("Jazz SQL validation", () => {
  it.each([
    ["SELECT * FROM absent", "Unknown table"],
    ["SELECT typo FROM notes", "column"],
    ["SELECT text, text FROM notes", "Duplicate"],
    ["SELECT * FROM notes WHERE done = 'false'", "Boolean"],
    ["SELECT * FROM notes WHERE rank = 2147483648", "Integer"],
    ["SELECT * FROM notes WHERE rank = 1.00000000000001", "Integer"],
    ["SELECT * FROM notes WHERE count = 9223372036854775808", "BigInt"],
    ["SELECT * FROM notes WHERE status = 'missing'", "one of"],
    ["SELECT * FROM notes WHERE text > 'a'", "Operator"],
    ["SELECT * FROM notes WHERE due = '2026-02-30T00:00:00Z'", "Timestamp"],
    ["SELECT * FROM notes WHERE rank = NULL", "IS NULL"],
    ["SELECT * FROM notes WHERE rank IS NULL", "not nullable"],
    ["SELECT * FROM notes ORDER BY typo", "column"],
    ["SELECT * FROM notes LIMIT -1", "nonnegative"],
    ["SELECT * FROM notes LIMIT 9007199254740992", "safe integer"],
    ["SELECT text AS title FROM notes", "expected FROM"],
    ["SELECT * FROM notes; DELETE FROM notes", "multiple statements"],
    ["SELECT * FROM notes WHERE done = TRUE OR done = FALSE", "unsupported clause"],
    ["SELECT * FROM notes JOIN other ON notes.id = other.id", "unsupported character"],
    ["SELECT * FROM notes /* unfinished", "unterminated"],
    ["SELECT * FROM notes WHERE text = 'unfinished", "unterminated"],
  ])("rejects %s", (sql, message) => expect(() => compile(sql)).toThrow(message));

  it.each([
    "INSERT INTO notes (text, rank) VALUES ('new', 1)",
    "UPDATE notes SET text = 'changed' WHERE id = '00000000-0000-0000-0000-000000000001'",
    "DELETE FROM notes WHERE id = '00000000-0000-0000-0000-000000000001'",
  ])("requires --write for %s", (sql) => expect(() => compile(sql)).toThrow("--write"));

  it.each([
    ["INSERT INTO notes (text) VALUES ('new')", "required column"],
    ["INSERT INTO notes (text, text) VALUES ('a', 'b')", "Duplicate"],
    [
      "INSERT INTO notes (id) VALUES ('00000000-0000-0000-0000-000000000001')",
      "cannot be assigned",
    ],
    ["INSERT INTO notes (text, rank) VALUES ('a')", "counts differ"],
    ["INSERT INTO notes (text, rank) VALUES ('a', 1), ('b', 2)", "unsupported clause"],
    ["UPDATE notes SET text = 'all'", "WHERE"],
    ["UPDATE notes SET text = 'all' WHERE rank = 1", "WHERE id"],
    [
      "UPDATE notes SET id = 'x' WHERE id = '00000000-0000-0000-0000-000000000001'",
      "cannot be assigned",
    ],
    ["DELETE FROM notes WHERE id = 'bad-id'", "Uuid"],
    [
      "DELETE FROM notes WHERE id = '00000000-0000-0000-0000-000000000001' AND rank = 1",
      "unsupported clause",
    ],
  ])("rejects invalid write %s before execution", (sql, message) =>
    expect(() => compile(sql, true)).toThrow(message),
  );

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
  });
});

describe("data command arguments", () => {
  it("never treats an admin secret as a data credential", () => {
    expect(() => parseDataArgs(args, defaults, { JAZZ_ADMIN_SECRET: "admin-only" })).toThrow(
      "catalogue access only",
    );
  });
  it("selects JWT permissions even when a backend secret is inherited", () => {
    expect(
      parseDataArgs([...args, "--jwt", "user-token"], defaults, { JAZZ_BACKEND_SECRET: "backend" })
        .auth,
    ).toEqual({ jwt: "user-token" });
    expect(() =>
      parseDataArgs(args, defaults, {
        JAZZ_JWT_TOKEN: "user-token",
        JAZZ_BACKEND_SECRET: "backend",
      }),
    ).toThrow("explicitly choose");
    expect(() =>
      parseDataArgs([...args, "--jwt", "user", "--backend-secret", "backend"], defaults, {}),
    ).toThrow("not both");
  });
  it.each([
    ["--writ", "Unknown option"],
    ["--format=csv", "--format"],
    ["--timeout=0", "--timeout"],
    ["--schema-hash=short", "full 64-character"],
  ])("fails closed for %s", (flag, error) => {
    expect(() =>
      parseDataArgs([...args, flag], defaults, { JAZZ_BACKEND_SECRET: "backend" }),
    ).toThrow(error);
  });
  it("requires exactly one SQL source", () => {
    expect(() =>
      parseDataArgs([...args, "--file", "-"], defaults, { JAZZ_BACKEND_SECRET: "backend" }),
    ).toThrow("exactly one");
  });
});

describe("data output", () => {
  const result = {
    columns: ["text", "count", "bytes"],
    rows: [
      { text: "line\n\u001b[31m", count: 9223372036854775807n, bytes: new Uint8Array([0, 255]) },
    ],
  };
  it("preserves full machine-readable values and exact integers", () => {
    expect(JSON.parse(formatData(result, "json"))).toEqual([
      { text: "line\n\u001b[31m", count: "9223372036854775807", bytes: [0, 255] },
    ]);
    expect(JSON.parse(formatData(result, "jsonl"))).toEqual(
      JSON.parse(formatData(result, "json"))[0],
    );
    expect(formatData({ columns: ["id"], rows: [] }, "jsonl")).toBe("");
    expect(formatData({ columns: ["id"], rows: [] }, "json")).toBe("[]\n");
  });
  it("escapes controls and shows columns even for empty tables", () => {
    const text = formatData(result, "table");
    expect(text).not.toContain("\u001b");
    expect(text).toContain("line\\u000a\\u001b[31m");
    expect(formatData({ columns: ["id", "text"], rows: [] }, "table")).toContain("(0 rows)");
  });
});
