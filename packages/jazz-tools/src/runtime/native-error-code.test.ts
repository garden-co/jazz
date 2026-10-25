import { NapiDb } from "jazz-napi";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import { nativeCoreErrorCode, nativeErrorCode } from "./native-runtime/native-error-code.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "./native-runtime/schema-codec.js";
import {
  UnsupportedStorageCodecsError,
  isUnsupportedStorageCodecsError,
} from "./unsupported-storage-codecs-error.js";
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
      const error = await missingTableReadError((query) => db.all(query, { tier: "local-first" }));

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
          db.all(query, { tier: "local-first" }, undefined, undefined, undefined),
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

// A store written before SQLite (the Groove engine's RocksDB directory) is
// refused at open with the stable code `unsupported_storage_codecs`; the
// SQLite engine has no codec lists, so they are empty.

/** The published alpha.54 native client root, unpacked into a fresh directory. */
function unpackPublishedAlpha54Root(): { dir: string; root: string } {
  const base64 = readFileSync(
    new URL(
      "../../../../crates/jazz/fixtures/pre-sqlite/published-alpha54-native-rocksdb.tar.gz.base64",
      import.meta.url,
    ),
    "utf8",
  ).replace(/\s+/g, "");
  const dir = mkdtempSync(join(tmpdir(), "jazz-old-store-"));
  const archive = join(dir, "root.tar.gz");
  writeFileSync(archive, Buffer.from(base64, "base64"));
  execFileSync("tar", ["-xzf", archive, "-C", dir]);
  return { dir, root: join(dir, "rocksdb-epoch-1") };
}

describe("storage-format refusal", () => {
  it("NAPI throws a Groove store as an Error with the stable code", () => {
    const { dir, root } = unpackPublishedAlpha54Root();
    try {
      let error: unknown;
      try {
        NapiDb.openPersistent(root, encodeSchema(app.wasmSchema), config("napi-old-store"));
      } catch (thrown) {
        error = thrown;
      }

      expect(error).toBeInstanceOf(Error);
      expect(nativeCoreErrorCode(error)).toBe("unsupported_storage_codecs");
      expect(isUnsupportedStorageCodecsError(error)).toBe(true);
      expect((error as Error).message).toContain("Groove engine (RocksDB)");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("the native runtime rethrows the refusal as UnsupportedStorageCodecsError", () => {
    const { dir, root } = unpackPublishedAlpha54Root();
    try {
      let error: unknown;
      try {
        new NativeRuntimeAdapter(
          {
            openMemory: (schema, configBytes) => NapiDb.openMemory(schema, configBytes) as never,
            openPersistent: (path, schema, configBytes) =>
              NapiDb.openPersistent(path, schema, configBytes) as never,
          },
          app.wasmSchema,
          new Uint8Array(16).fill(32),
          testAuthorBytes("runtime-old-store"),
          1,
          false,
          { persistentPath: root },
        );
      } catch (thrown) {
        error = thrown;
      }

      expect(error).toBeInstanceOf(UnsupportedStorageCodecsError);
      expect(error).toMatchObject({
        name: "UnsupportedStorageCodecsError",
        code: "unsupported_storage_codecs",
        missing: [],
        unknown: [],
      });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
