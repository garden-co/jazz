import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });

describe.skipIf(!hasJazzWasmBuild())("SQLite browser storage configuration", () => {
  for (const operation of ["open", "destroy"] as const) {
    it.each(["metadata", "readPage", "commitPages"])(
      `${operation} rejects a page-store %s callback before accessing storage`,
      async (callback) => {
        const wasm = await loadWasmModuleForTest();
        let callbackReads = 0;
        const callbacks = Object.defineProperty({}, callback, {
          get() {
            callbackReads += 1;
            throw new Error("obsolete callback must not be evaluated");
          },
        });
        // Class/prototype methods must be refused too, even when the object
        // also supplies a valid-looking SQLite descriptor.
        const storage = Object.assign(Object.create(callbacks), {
          backend: "indexeddb",
          path: "obsolete-callbacks.sqlite",
        });
        const previous = Object.getOwnPropertyDescriptor(globalThis, "indexedDB");
        let storageReads = 0;
        Object.defineProperty(globalThis, "indexedDB", {
          configurable: true,
          get() {
            storageReads += 1;
            throw new Error("IndexedDB must not be accessed");
          },
        });
        try {
          const result =
            operation === "open"
              ? wasm.WasmDb.openBrowser(
                  storage,
                  encodeSchema(app.wasmSchema),
                  openConfig(new Uint8Array(16).fill(1), testAuthorBytes("storage-config")),
                  "storage-config-owner",
                )
              : wasm.WasmDb.destroyBrowserStorage(storage, "storage-config-owner");
          await expect(result).rejects.toMatchObject({
            message: expect.stringContaining("does not provide durable admission for SQLite"),
            code: "query",
          });
          expect(callbackReads).toBe(0);
          expect(storageReads).toBe(0);
        } finally {
          if (previous) Object.defineProperty(globalThis, "indexedDB", previous);
          else Reflect.deleteProperty(globalThis, "indexedDB");
        }
      },
    );
  }
});
