import { describe, expect, it } from "vitest";
import {
  AUTHORITY_CHANGED_ERROR_CODE,
  AuthorityChangedError,
  isAuthorityChangedError,
  upstreamRefusalError,
} from "./authority-changed-error.js";

describe("upstreamRefusalError", () => {
  it("is null while the link stands", () => {
    expect(upstreamRefusalError(null)).toBeNull();
    expect(upstreamRefusalError(undefined)).toBeNull();
  });

  it("rebuilds a changed authority as an AuthorityChangedError, keeping its message", () => {
    // NAPI reports a plain object, WASM an Error with `code`.
    for (const refusal of [
      { code: "authority_changed", message: "AuthorityChanged: reset the local store" },
      Object.assign(new Error("AuthorityChanged: reset the local store"), {
        code: "authority_changed",
      }),
    ]) {
      const error = upstreamRefusalError(refusal);
      expect(error).toBeInstanceOf(AuthorityChangedError);
      expect(error).toMatchObject({
        name: "AuthorityChangedError",
        code: AUTHORITY_CHANGED_ERROR_CODE,
        message: "AuthorityChanged: reset the local store",
      });
      expect(isAuthorityChangedError(error)).toBe(true);
    }
  });

  it("keeps the core code of any other refusal", () => {
    const error = upstreamRefusalError({ code: "protocol", message: "Protocol: stranded" });
    expect(error).not.toBeInstanceOf(AuthorityChangedError);
    expect(error).toMatchObject({ code: "protocol", message: "Protocol: stranded" });
    expect(isAuthorityChangedError(error)).toBe(false);
  });
});
