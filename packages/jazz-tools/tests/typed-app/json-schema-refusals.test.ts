import { describe, expect, it } from "vitest";
import { testApp } from "../../src/testing/test-app.js";

// Loading an app checks its JSON Schema columns: a declared default that its
// schema refuses, or a schema that isn't valid, refuses the app itself. (These
// were browser tests in tests/browser/db.json-schema.test.ts, which reached the
// same refusals through the WASM runtime.)
describe("JSON Schema columns an app is refused for", () => {
  it("refuses a default that does not match the column's schema", () => {
    expect(() =>
      testApp({
        tables: {
          jobs: {
            columns: {
              meta: {
                json: { type: "object", properties: { count: { multipleOf: 2 } } },
                default: { count: 3 },
              },
            },
          },
        },
      }),
    ).toThrow("JSON schema validation failed for column `meta`: 3 is not a multiple of 2");
  });

  it("refuses a declared schema that is not valid", () => {
    expect(() =>
      testApp({
        tables: {
          jobs: { columns: { meta: { json: { type: "string", pattern: "[z-a]" } } } },
        },
      }),
    ).toThrow('invalid JSON schema for column `meta`: "[z-a]" is not a "regex"');
  });
});
