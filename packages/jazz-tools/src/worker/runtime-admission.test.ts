import { describe, expect, it } from "vitest";
import { compareRuntimeAdmission } from "./runtime-admission.js";
import type { WasmSchema } from "../drivers/types.js";

const tasks = {
  tasks: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
} as unknown as WasmSchema;

function fingerprint(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    protocolVersion: "jazz-shared-runtime-v3",
    dbName: "store",
    schemaHash: "tasks-schema",
    authClass: 'local-first:["account","urn:jazz:local-first","guest"]',
    workerUrl: "worker.js",
    ...overrides,
  });
}
const user = { authClass: 'external:["account","jazz:backend","erin"]' };
const noSchema = { schemaHash: "empty-schema" };

describe("compareRuntimeAdmission", () => {
  it("admits the same configuration", () => {
    expect(compareRuntimeAdmission(fingerprint(), fingerprint(), tasks)).toBe("admit");
  });

  it("makes a tab of another sign-in to the account wait for the store", () => {
    expect(compareRuntimeAdmission(fingerprint(), fingerprint(user), tasks)).toBe(
      "held-by-another-sign-in",
    );
  });

  it("makes a tab of another build (another schema) wait for the store, whoever is signed in", () => {
    const other = { schemaHash: "older-deploy" };
    expect(compareRuntimeAdmission(fingerprint(), fingerprint(other), tasks)).toBe(
      "held-by-another-build",
    );
    expect(compareRuntimeAdmission(fingerprint(), fingerprint({ ...other, ...user }), tasks)).toBe(
      "held-by-another-build",
    );
  });

  it("refuses another configuration besides the schema and sign-in", () => {
    expect(
      compareRuntimeAdmission(fingerprint(), fingerprint({ workerUrl: "other.js" }), tasks),
    ).toBe("incompatible");
    expect(compareRuntimeAdmission(fingerprint(), "not json", tasks)).toBe("incompatible");
  });

  it("refuses another configuration that also has another schema, rather than wait for it", () => {
    // Waiting would never end: no reload of the other tab makes this one fit.
    expect(
      compareRuntimeAdmission(
        fingerprint(),
        fingerprint({ schemaHash: "older-deploy", workerUrl: "other.js" }),
        tasks,
      ),
    ).toBe("incompatible");
  });

  // #5139: a Db that hasn't read or written opens the store with no schema,
  // only to delete it (a clearOnSignOut sign-out right after a sign-up).
  it("admits a tab with no schema to a store open under the app's schema", () => {
    expect(compareRuntimeAdmission(fingerprint(), fingerprint(noSchema), {})).toBe("admit");
  });

  it("makes a tab with no schema wait while another sign-in holds the store", () => {
    expect(compareRuntimeAdmission(fingerprint(), fingerprint({ ...noSchema, ...user }), {})).toBe(
      "held-by-another-sign-in",
    );
  });

  it("still refuses a tab with no schema whose other configuration differs", () => {
    expect(
      compareRuntimeAdmission(
        fingerprint(),
        fingerprint({ ...noSchema, workerUrl: "other.js" }),
        {},
      ),
    ).toBe("incompatible");
  });
});
