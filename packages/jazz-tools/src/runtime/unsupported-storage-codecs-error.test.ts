import { describe, expect, it } from "vitest";
import {
  UnsupportedStorageCodecsError,
  asUnsupportedStorageCodecsError,
  isUnsupportedStorageCodecsError,
} from "./unsupported-storage-codecs-error.js";

function coded(message: string, code: string, fields: Record<string, unknown> = {}): Error {
  return Object.assign(new Error(message), { code }, fields);
}

describe("UnsupportedStorageCodecsError", () => {
  it("is detected by its stable code, whatever class carried it", () => {
    expect(isUnsupportedStorageCodecsError(new UnsupportedStorageCodecsError(["a.v2"], []))).toBe(
      true,
    );
    expect(isUnsupportedStorageCodecsError(coded("refused", "unsupported_storage_codecs"))).toBe(
      true,
    );
    expect(isUnsupportedStorageCodecsError(coded("other", "storage"))).toBe(false);
    expect(isUnsupportedStorageCodecsError("unsupported_storage_codecs")).toBe(false);
    expect(isUnsupportedStorageCodecsError(null)).toBe(false);
  });

  it("rebuilds a binding refusal with codec lists as the class, keeping its message", () => {
    const binding = coded("UnsupportedStorageCodecs: refused", "unsupported_storage_codecs", {
      missing: ["groove.durable-index.v2"],
      unknown: ["jazz.future.v2"],
    });

    const rebuilt = asUnsupportedStorageCodecsError(binding);

    expect(rebuilt).toBeInstanceOf(UnsupportedStorageCodecsError);
    expect(rebuilt).toMatchObject({
      name: "UnsupportedStorageCodecsError",
      code: "unsupported_storage_codecs",
      message: "UnsupportedStorageCodecs: refused",
      missing: ["groove.durable-index.v2"],
      unknown: ["jazz.future.v2"],
      cause: binding,
    });
  });

  it("leaves other errors and refusals without codec lists unchanged", () => {
    const other = coded("other", "storage");
    const withoutLists = coded("refused", "unsupported_storage_codecs");
    expect(asUnsupportedStorageCodecsError(other)).toBe(other);
    expect(asUnsupportedStorageCodecsError(withoutLists)).toBe(withoutLists);
  });
});
