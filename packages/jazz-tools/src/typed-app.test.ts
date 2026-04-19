import { describe, expect, it } from "vitest";
import { col } from "./dsl.js";
import { defineApp, defineTable } from "./typed-app.js";

describe("typed app table transactionality", () => {
  it("emits transaction-required metadata into the wasm schema", () => {
    const app = defineApp({
      audit_logs: defineTable({
        message: col.string(),
      }).requireTransaction(),
      comments: defineTable({
        body: col.string(),
      }),
    });

    expect(app.wasmSchema.audit_logs?.requiresTransaction).toBe(true);
    expect(app.wasmSchema.comments?.requiresTransaction).toBeUndefined();
  });
});
