import { afterEach, describe, expect, it } from "vitest";
import { bindApp } from "../../src/index.js";
import { createBrowserTestDb, TestCleanup } from "./support.js";

// The browser WASM build has no Rust JSON Schema validator: the WASM loader
// installs the JS one with `setJsonSchemaValidator`, and the WASM runtime
// checks a JSON column's declared default with it when it admits the schema.
// `jazz-tools generate` (and `testApp` in Node) refuse these apps before any
// bindings exist (tests/typed-app/json-schema-refusals.test.ts), so bindings
// that reach a browser with such a column were not made by `generate`: these
// pass the compiled schema to `bindApp` by hand, as stale or edited bindings
// would, to reach the runtime's own check.

/** Bindings for one table `jobs` with one JSON column `meta`. */
function jobsBindings(meta: Record<string, unknown>) {
  return bindApp({
    // Never checked in a browser: only backends refuse stale bindings.
    source: `sha256:${"0".repeat(64)}`,
    schema: {
      current: HASH,
      schemas: [
        {
          hash: HASH,
          schema: { tables: { jobs: { columns: [{ name: "meta", nullable: false, ...meta }] } } },
        },
      ],
      migrations: [],
    },
    relations: { jobs: {} },
  } as never);
}
const HASH = "5668".repeat(16);

function appId(label: string): string {
  return `browser-json-schema-refusal-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("browser JSON Schema refusals through WASM", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  it("refuses a default that does not match the column's schema", async () => {
    const jobs = jobsBindings({
      column_type: {
        type: "Json",
        schema: { type: "object", properties: { count: { multipleOf: 2 } } },
      },
      default: { type: "Text", value: JSON.stringify({ count: 3 }) },
    });
    await expect(async () => {
      const db = ctx.track(await createBrowserTestDb({ appId: appId("default") }));
      db.insert(jobs.jobs, {});
    }).rejects.toThrow(
      "JSON schema validation failed for column `meta`: 3 is not a multiple of 2. (at /count)",
    );
  });

  it("refuses a declared schema that is not valid", async () => {
    const jobs = jobsBindings({
      column_type: { type: "Json", schema: { type: "string", pattern: "[z-a]" } },
    });
    await expect(async () => {
      const db = ctx.track(await createBrowserTestDb({ appId: appId("schema") }));
      db.insert(jobs.jobs, { meta: "a" });
    }).rejects.toThrow(/"\[z-a\]" is not a valid regular expression/);
  });
});
