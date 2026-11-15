import { afterEach, describe, expect, it } from "vitest";
import { createBrowserTestDb, TestCleanup } from "./support.js";
import { app as jobs } from "./apps/json-schema-jobs/jazz.generated.js";

// The browser WASM build has no Rust JSON Schema validator: the WASM loader
// installs the JS one with `setJsonSchemaValidator`. These run through that
// real path; the parity fixture pins the verdicts themselves. The WASM runtime
// checks JSON values when it admits a schema's declared defaults; cell writes
// are only checked by the TS pre-check in `value-converter.ts` (#3917).

// apps/json-schema-jobs/jazz.app.json: `jobs.meta` is
// { "json": { "type": "object", "properties": { "code": { "type": "string",
// "pattern": "^[a-z]+\\-[0-9]+$" }, "count": { "type": "integer", "minimum": 0 } },
// "required": ["code"] } }
// (`\-` is valid in the native regex syntax and not in a JS `u` regex.)

function appId(label: string): string {
  return `browser-json-schema-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("browser JSON Schema validation through WASM", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  it("accepts a value that matches the column's schema", async () => {
    const db = ctx.track(await createBrowserTestDb({ appId: appId("valid") }));

    const { value: job } = db.insert(jobs.jobs, {
      meta: { code: "build-7", count: 2 },
    });

    expect(await db.one(jobs.jobs.where({ id: job.id }), { tier: "local-first" })).toMatchObject({
      meta: { code: "build-7", count: 2 },
    });
  });

  // Schemas that are refused (an invalid default, an invalid regex): tests/typed-app/json-schema-refusals.test.ts.
});
