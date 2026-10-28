import { describe, expect, it } from "vitest";
import {
  UnsupportedStorageCodecsError,
  asUnsupportedStorageCodecsError,
  isUnsupportedStorageCodecsError,
} from "./unsupported-storage-codecs-error.js";

const format = "jazz.sqlite.native-root.v1";
const message = `native database root uses an unsupported storage layout; expected ${format}`;
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
});
