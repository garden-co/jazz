import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createBrowserTestDb, TestCleanup } from "./support.js";

// The browser WASM build has no Rust JSON Schema validator: the WASM loader
// installs the JS one with `setJsonSchemaValidator`. These run through that
// real path; the parity fixture pins the verdicts themselves. The invalid
// values break keywords (`multipleOf`, `uniqueItems`) the TS write pre-check
// in `value-converter.ts` does not look at, so only the WASM path rejects them.

const jobs = s.defineApp({
  jobs: s.table(
    {
      meta: s.json({
        type: "object",
        properties: {
          // `\-` is valid in the native regex syntax and not in a JS `u` regex.
          code: { type: "string", pattern: "^[a-z]+\\-[0-9]+$" },
          count: { type: "integer", minimum: 0, multipleOf: 2 },
          tags: { type: "array", uniqueItems: true },
        },
        required: ["code"],
      }),
    },
    {},
  ),
});

const invalidJobs = s.defineApp({
  jobs: s.table(
    {
      meta: s.json({ type: "string", pattern: "[z-a]" }),
    },
    {},
  ),
});

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

    expect(await db.one(jobs.jobs.where({ id: job.id }), { tier: "local" })).toMatchObject({
      meta: { code: "build-7", count: 2 },
    });
  });

  it("rejects a value that does not match the column's schema", async () => {
    const db = ctx.track(await createBrowserTestDb({ appId: appId("invalid") }));

    expect(() => db.insert(jobs.jobs, { meta: { code: "build-7", count: 3 } })).toThrow(
      "JSON schema validation failed for column `meta`: 3 is not a multiple of 2. (at /count)",
    );
    expect(() => db.insert(jobs.jobs, { meta: { code: "build-7", tags: ["a", "a"] } })).toThrow(
      "JSON schema validation failed for column `meta`: Duplicate items at indexes 0 and 1. (at /tags)",
    );
  });

  it("rejects a declared schema that is not valid", async () => {
    await expect(async () => {
      const db = ctx.track(await createBrowserTestDb({ appId: appId("invalid-schema") }));
      db.insert(invalidJobs.jobs, { meta: "a" });
    }).rejects.toThrow(/"\[z-a\]" is not a valid regular expression/);
  });
});
