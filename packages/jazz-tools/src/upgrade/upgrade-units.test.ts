import {
  readdirSync,
  readFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { editAppJson, entriesOf, parseAppJson, printAppJson } from "./app-json.js";
import { isRustApp, renameValidate } from "./index.js";
import { dslAlpha } from "./legacy.js";
import { checkMigrations } from "./migrations.js";
import { payloadEnumColumns } from "./payload-enums.js";
import { readGeneratedNames, rowTypeName } from "./names.js";
import { FIXTURES } from "./upgrade-test-helpers.js";

const REPO = join(FIXTURES, "..", "..", "..", "..", "..");

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

describe("jazz.app.json editing", () => {
  it("prints every example's file exactly as Jazz's writer did", () => {
    const files: string[] = [];
    const walk = (dir: string, depth: number) => {
      if (depth > 4 || !existsSync(dir)) return;
      for (const entry of readdirSync(dir, { withFileTypes: true })) {
        if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
        if (entry.isDirectory()) walk(join(dir, entry.name), depth + 1);
        else if (entry.name === "jazz.app.json") files.push(join(dir, entry.name));
      }
    };
    walk(join(REPO, "examples"), 0);
    walk(FIXTURES, 0);
    expect(files.length).toBeGreaterThan(5);
    for (const file of files) {
      const text = readFileSync(file, "utf8");
      expect(printAppJson(parseAppJson(text)), file).toBe(text);
    }
  });

  it("keeps integer-like keys in file order", () => {
    const text = '{ "history": { "900000000002": { "tables": {} }, "5": {}, "1": {} } }\n';
    const app = parseAppJson(text);
    expect(entriesOf(app.history as never).map(([k]) => k)).toEqual(["900000000002", "5", "1"]);
    expect(printAppJson(app)).toBe(text);
  });

  it("drops slices and tsOnly, adding computed in the writer's key order", () => {
    const app = parseAppJson(
      JSON.stringify({
        format: 1,
        tables: { t: { columns: { a: "int" }, history: "latest", tsOnly: { b: "computed" } } },
        slices: { s: ["t"] },
      }),
    );
    const edited = editAppJson(app, {
      computed: { t: { b: { type: "int", from: ["a"] } } },
      transforms: {},
      columnTransforms: {},
    });
    expect(edited.droppedSlices).toEqual(["s"]);
    expect(printAppJson(edited.app)).toBe(
      '{\n  "format": 1,\n  "tables": {\n    "t": {\n      "columns": { "a": "int" },\n      "history": "latest",\n      "computed": { "b": { "type": "int", "from": ["a"] } }\n    }\n  }\n}\n',
    );
  });
});

describe("migrations check", () => {
  it("refuses when jazz.app.json lacks a version migrations/ holds", () => {
    const dir = join(FIXTURES, "backend-history");
    const app = parseAppJson(readFileSync(join(dir, "jazz.app.json"), "utf8"));
    expect(checkMigrations(app, join(dir, "migrations"))).toEqual({
      versions: 2,
      migrations: 1,
      unshipped: [],
    });
    delete app.history;
    delete app.migrations;
    expect(() => checkMigrations(app, join(dir, "migrations"))).toThrow(
      /is not in jazz\.app\.json's migrations/,
    );
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
    expect(dslAlpha("2.0.0-alpha.63")).toBe(63);
    expect(dslAlpha("2.0.0-alpha.64")).toBe(64);
    expect(dslAlpha("2.0.0-alpha.65")).toBeNull();
    expect(dslAlpha("2.0.0")).toBeNull();
  });
});

describe("Rust apps", () => {
  it("are crates that depend on jazz-app or commit src/jazz_app.rs, as `jazz-tools generate` tells", () => {
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
        '[package]\nname = "x"\n\n[dependencies]\njazz-app = { path = "../jazz-app" }\n',
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
