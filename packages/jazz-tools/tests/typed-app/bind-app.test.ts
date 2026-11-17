import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { bindApp } from "../../src/typed-app.js";
import { compileTestApp, testApp, testPermissions } from "../../src/testing/test-app.js";

const tasksApp = readFileSync(
  new URL("../../../../crates/jazz-codegen/tests/apps/tasks/jazz.app.json", import.meta.url),
  "utf8",
);

describe("bindApp", () => {
  it("binds the compiled schema with its relations, history and computed fields", () => {
    const app = testApp(tasksApp, {
      computed: {
        tasks: {
          priority: (row: { score: number }) => (row.score >= 8 ? "high" : "low"),
          label: (row: { title: string }) => row.title,
        },
      },
      transforms: {
        isoDate: { from: (v: Date) => v.toISOString(), to: (v: string) => new Date(v) },
        lowercase: { from: (v: string) => v, to: (v: string) => v.toLowerCase() },
      },
    });
    expect(Object.keys(app.wasmSchema).sort()).toEqual(["people", "projects", "tasks"]);
    expect(app.wasmSchema.tasks!.relations?.project).toEqual({
      kind: "forward",
      table: "projects",
      column: "projectId",
    });
    expect(app.tasks._columnTransforms?.priority?.computed).toBe(true);
  });

  it("refuses a missing computed implementation at runtime", () => {
    expect(() => testApp(tasksApp, { transforms: {} as never })).toThrow(
      /declares the computed field tasks\.priority, but jazz\.computed\.ts doesn't implement it/,
    );
  });

  it("refuses an implementation the file doesn't declare", () => {
    const app = { tables: { notes: { columns: { body: "string" } } } };
    expect(() => testApp(app, { computed: { notes: { len: () => 1 } } })).toThrow(
      /implements the computed field notes\.len, which jazz\.app\.json doesn't declare/,
    );
  });

  it("refuses a malformed source and a missing current version", () => {
    const options = compileTestApp({ tables: { notes: { columns: { body: "string" } } } });
    expect(() => bindApp({ ...options, source: "nope" } as never)).toThrow(/sha256/);
    expect(() =>
      bindApp({ ...options, schema: { ...options.schema, current: "0".repeat(64) } } as never),
    ).toThrow(/ships no schema for its current version/);
  });

  it("compiles inline permissions against the bound app", () => {
    const app = testApp({ tables: { notes: { columns: { body: "string", ownerId: "uuid" } } } });
    const permissions = testPermissions(app, {
      tables: { notes: { read: "ownerId = session.user", insert: "true" } },
    });
    expect(Object.keys(permissions)).toEqual(["notes"]);
    expect(() => testPermissions(app, { tables: { nope: { read: "true" } } })).toThrow();
  });
});

import { columnTypeSignature } from "../../src/runtime/schema-metadata.js";
describe("column type signatures", () => {
  it("ignore key order, as the engine's serde form and the TS form differ in it", () => {
    expect(columnTypeSignature({ element: { type: "Text" }, type: "Array" } as never)).toBe(
      columnTypeSignature({ type: "Array", element: { type: "Text" } }),
    );
  });
});
