import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import { loadWasmModuleForTest } from "../runtime/testing/wasm-runtime-test-utils.js";

describe.each(["native", "wasm"])("%s schema validation binding", (runtime) => {
  it("accepts a matching policy and rejects the same policy after its column is removed", async () => {
    const binding =
      runtime === "native" ? await import("jazz-napi") : await loadWasmModuleForTest();
    const app = s.defineApp({ notes: s.table({ owner: s.string() }, {}) });
    const permissions = s.definePermissions(app, ({ policy, session }) => {
      policy.notes.allowRead.where({ owner: session.user.identity.subject });
    });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, permissions)),
      ),
    ).not.toThrow();

    const wrongType = s.definePermissions(app, ({ policy, session }) => {
      policy.notes.allowRead.where({ owner: session.user });
    });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, wrongType)),
      ),
    ).toThrow("ClaimTypeMismatch");

    const changed = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(changed.wasmSchema, permissions)),
      ),
    ).toThrow("owner");
  });
});
