import { describe, expect, it } from "vitest";
import {
  CORE_TICK_FAILED_ERROR_CODE,
  CoreTickFailedError,
  isCoreTickFailedError,
} from "./core-tick-failed-error.js";

describe("CoreTickFailedError", () => {
  it("keeps the native error as its cause, with its core code", () => {
    // NAPI and WASM throw an Error with `code`; some hosts reject with a plain object.
    for (const cause of [
      Object.assign(new Error("Storage: disk I/O error"), { code: "storage" }),
      { code: "storage", message: "Storage: disk I/O error" },
    ]) {
      const error = new CoreTickFailedError(cause);
      expect(error).toBeInstanceOf(Error);
      expect(error).toMatchObject({
        name: "CoreTickFailedError",
        code: CORE_TICK_FAILED_ERROR_CODE,
        message: "core tick failed: Storage: disk I/O error",
        cause,
      });
      expect(isCoreTickFailedError(error)).toBe(true);
    }
    expect(new CoreTickFailedError("boom").message).toBe("core tick failed: boom");
  });

  it("is detected by its stable code, which survives bindings and worker messages", () => {
    expect(CORE_TICK_FAILED_ERROR_CODE).toBe("core_tick_failed");
    expect(isCoreTickFailedError({ code: "core_tick_failed", message: "relayed" })).toBe(true);
    expect(isCoreTickFailedError(Object.assign(new Error("x"), { code: "storage" }))).toBe(false);
    expect(isCoreTickFailedError(null)).toBe(false);
    expect(isCoreTickFailedError("core_tick_failed")).toBe(false);
  });
});
