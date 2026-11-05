import { describe, expect, it } from "vitest";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { schema as s } from "../schema-namespace.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import {
  UnsupportedStorageCodecsError,
  asUnsupportedStorageCodecsError,
  isUnsupportedStorageCodecsError,
} from "./unsupported-storage-codecs-error.js";

const format = "jazz.sqlite.native-root.v1";
const message = `native database root uses an unsupported storage layout; expected ${format}`;
const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
function refusal<T extends Record<string, unknown>>(fields: T) {
  return Object.assign(new Error(message), { code: "unsupported_storage_codecs" }, fields);
}

describe("SQLite storage refusal metadata", () => {
  it("rebuilds a format-only refusal with its message, code and original cause", () => {
    const native = refusal({ format });
    const error = asUnsupportedStorageCodecsError(native);
    expect(error).toBeInstanceOf(UnsupportedStorageCodecsError);
    expect(isUnsupportedStorageCodecsError(error)).toBe(true);
    expect(error).toMatchObject({ format, message, cause: native, missing: [], unknown: [] });
    expect(asUnsupportedStorageCodecsError(error)).toBe(error);
  });

  it("preserves format and independent codec lists when both are supplied", () => {
    const native = refusal({ format, missing: ["required.v1"], unknown: ["future.v2"] });
    const error = asUnsupportedStorageCodecsError(native);
    native.missing[0] = "mutated";
    native.unknown.length = 0;
    expect(error).toMatchObject({ format, missing: ["required.v1"], unknown: ["future.v2"] });
  });

  it.each([
    { format: "" },
    { format: 1 },
    { format: null },
    { format, missing: [] },
    { format, unknown: [] },
    { format, missing: [1], unknown: [] },
    { format, missing: [], unknown: [null] },
  ])("keeps an incomplete or malformed refusal unchanged: %j", (fields) => {
    const native = refusal(fields);
    expect(asUnsupportedStorageCodecsError(native)).toBe(native);
  });

  it("normalizes the error at the public persistent runtime boundary", () => {
    const native = refusal({ format });
    let thrown: unknown;
    try {
      new NativeRuntimeAdapter(
        {
          openMemory() {
            throw new Error("memory open must not run");
          },
          openPersistent() {
            throw native;
          },
        },
        app.wasmSchema,
        new Uint8Array(16).fill(73),
        testAuthorBytes("sqlite-storage-refusal"),
        1,
        false,
        { persistentPath: "refused-sqlite-root" },
      );
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(UnsupportedStorageCodecsError);
    expect(thrown).toMatchObject({ format, message, cause: native });
  });

  it("preserves a real SQLite root refusal without opening or changing its data", async () => {
    const { NapiDb } = await import("jazz-napi");
    const root = await mkdtemp(join(tmpdir(), "jazz-sqlite-refusal-"));
    const file = join(root, "foreign.data");
    const bytes = new Uint8Array([0x13, 0x37, 0, 0xff]);
    await writeFile(file, bytes);
    try {
      let thrown: unknown;
      try {
        new NativeRuntimeAdapter(
          {
            openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never,
            openPersistent: (path, schema, config) =>
              NapiDb.openPersistent(path, schema, config) as never,
          },
          app.wasmSchema,
          new Uint8Array(16).fill(74),
          testAuthorBytes("sqlite-root-refusal"),
          1,
          false,
          { persistentPath: root },
        );
      } catch (error) {
        thrown = error;
      }
      expect(thrown).toBeInstanceOf(UnsupportedStorageCodecsError);
      expect(thrown).toMatchObject({
        format,
        code: "unsupported_storage_codecs",
        cause: expect.objectContaining({ format, code: "unsupported_storage_codecs" }),
      });
      expect(new Uint8Array(await readFile(file))).toEqual(bytes);
      expect((await readdir(root)).sort()).toEqual([".jazz-native-owner.lock", "foreign.data"]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
