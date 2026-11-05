import { NapiDb } from "jazz-napi";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import {
  NATIVE_CORE_ERROR_REASONS,
  nativeCoreErrorCode,
  nativeCoreErrorReason,
  nativeErrorCode,
} from "./native-runtime/native-error-code.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "./native-runtime/schema-codec.js";
import {
  UnsupportedStorageCodecsError,
  isUnsupportedStorageCodecsError,
} from "./unsupported-storage-codecs-error.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
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

describe("native core error reasons", () => {
  it("reads a core reason name and ignores anything else", () => {
    const denied = Object.assign(new Error("WriteRejected: authorization_denied: x"), {
      code: "write_rejected",
      reason: "authorization_denied",
    });
    expect(nativeCoreErrorReason(denied)).toBe("authorization_denied");
    expect(nativeCoreErrorReason(new Error("no reason"))).toBeUndefined();
    expect(nativeCoreErrorReason({ reason: "a_reason_from_a_newer_engine" })).toBeUndefined();
    expect(nativeCoreErrorReason("authorization_denied")).toBeUndefined();
  });

  it("knows exactly the reasons the engine has", async () => {
    // A test-only export (`skip_typescript`), absent from the declarations.
    const napi = (await loadNapiModule()) as unknown as {
      __testCoreErrorReasonNames(): string[];
    };
    expect([...NATIVE_CORE_ERROR_REASONS].sort()).toEqual(napi.__testCoreErrorReasonNames().sort());
  });
});

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

// A store written by an older Jazz storage format is refused at open with the
// stable code `unsupported_storage_codecs` and the required SQLite root format.

/** The published alpha.54 native client root, unpacked into a fresh directory. */
function unpackPublishedAlpha54Root(): {
  dir: string;
  root: string;
  originalFiles: Record<string, string>;
} {
  const base64 = readFileSync(
    new URL(
      "../../../../crates/jazz/fixtures/pre-sqlite/published-alpha54-native-rocksdb.tar.gz.base64",
      import.meta.url,
    ),
    "utf8",
  ).replace(/\s+/g, "");
  const bytes = Buffer.from(base64, "base64");
  // This is the immutable published receipt, not a newly generated old store.
  expect(createHash("sha256").update(bytes).digest("hex")).toBe(
    "10d139b12fd21530fd553ee148e975d4e2f55d11b43bf3bd90d00179f5703575",
  );
  const dir = mkdtempSync(join(tmpdir(), "jazz-old-store-"));
  const archive = join(dir, "root.tar.gz");
  writeFileSync(archive, bytes);
  execFileSync("tar", ["-xzf", archive, "-C", dir]);
  const root = join(dir, "rocksdb-epoch-1");
  return { dir, root, originalFiles: fileHashes(root) };
}

const SQLITE_ROOT_FORMAT = "jazz.sqlite.native-root.v1";

function fileHashes(root: string): Record<string, string> {
  return Object.fromEntries(
    readdirSync(root)
      .sort()
      .map((name) => [
        name,
        createHash("sha256")
          .update(readFileSync(join(root, name)))
          .digest("hex"),
      ]),
  );
}

function expectUnchangedOldRoot(root: string, originalFiles: Record<string, string>) {
  const after = fileHashes(root);
  // Admission may create its process lock, but must not create a SQLite file
  // or alter/remove any of the published root's original physical files.
  delete after[".jazz-native-owner.lock"];
  expect(after).toEqual(originalFiles);
}

describe("storage-format refusal", () => {
  it("NAPI refuses an old store with the stable code and required SQLite format", () => {
    const { dir, root, originalFiles } = unpackPublishedAlpha54Root();
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
      expect(error).toMatchObject({ format: SQLITE_ROOT_FORMAT });
      expect(error).not.toHaveProperty("epoch");
      expect(error).not.toHaveProperty("missing");
      expect(error).not.toHaveProperty("unknown");
      expectUnchangedOldRoot(root, originalFiles);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("the native runtime rethrows the refusal as UnsupportedStorageCodecsError", () => {
    const { dir, root, originalFiles } = unpackPublishedAlpha54Root();
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
        format: SQLITE_ROOT_FORMAT,
        missing: [],
        unknown: [],
        cause: expect.objectContaining({
          code: "unsupported_storage_codecs",
          format: SQLITE_ROOT_FORMAT,
        }),
      });
      expectUnchangedOldRoot(root, originalFiles);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
