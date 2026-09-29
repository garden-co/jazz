import { NapiDb } from "jazz-napi";
import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import { nativeCoreErrorCode, nativeErrorCode } from "./native-runtime/native-error-code.js";
import { encodeSchema } from "./native-runtime/schema-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

// A core error crossing a native binding keeps its Rust display text as the
// message and carries the stable core `ErrorCode` string as `code` (#3691).

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });

function config(label: string): Uint8Array {
  return openConfig(new Uint8Array(16).fill(31), testAuthorBytes(label), 1, true);
}

/** Read a missing table and return whatever the binding threw or rejected with. */
async function missingTableReadError(read: (query: Uint8Array) => unknown): Promise<unknown> {
  try {
    let result = read(queryFromTable("missing"));
    while (result && typeof (result as { poll?: unknown }).poll === "function") {
      const polled = (result as { poll(): unknown }).poll();
      if (polled !== null) {
        result = polled;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    await result;
  } catch (error) {
    return error;
  }
  throw new Error("reading an undeclared table unexpectedly succeeded");
}

describe("native core error codes", () => {
  it("NAPI throws a core error as an Error with its message and stable code", async () => {
    const db = NapiDb.openMemoryAsBackend(encodeSchema(app.wasmSchema), config("napi-error-code"));
    try {
      const error = await missingTableReadError((query) => db.all(query, { tier: "local" }));

      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toBe("Query: unknown table missing");
      expect(nativeErrorCode(error)).toBe("query");
      expect(nativeCoreErrorCode(error)).toBe("query");
    } finally {
      await db.close();
    }
  });

  it("NAPI keeps the historical code of an error that is not a core error", () => {
    const db = NapiDb.openMemoryAsBackend(encodeSchema(app.wasmSchema), config("napi-non-core"));
    let error: unknown;
    try {
      db.rollbackTransaction("not-a-transaction-id");
    } catch (thrown) {
      error = thrown;
    }

    expect(error).toBeInstanceOf(Error);
    expect(nativeErrorCode(error)).toBe("GenericFailure");
    expect(nativeCoreErrorCode(error)).toBeUndefined();
  });

  it.skipIf(!hasJazzWasmBuild())(
    "WASM throws a core error as an Error, not a string, with its message and stable code",
    async () => {
      const wasm = await loadWasmModuleForTest();
      const db = wasm.WasmDb.openMemory(encodeSchema(app.wasmSchema), config("wasm-error-code"));
      try {
        const error = await missingTableReadError((query) =>
          db.all(query, { tier: "local" }, undefined, undefined, undefined),
        );

        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toBe("Query: unknown table missing");
        expect(String(error)).toContain("Query: unknown table missing");
        expect(nativeCoreErrorCode(error)).toBe("query");
      } finally {
        db.free?.();
      }
    },
  );
});
