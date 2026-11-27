import { describe, expect, it, vi } from "vitest";
import { retryAfterKey } from "./key-retry.js";

const space = { scopeId: "scope", identifier: "project" };
const pending = (reason = "key-pending", address = space) =>
  Object.assign(new Error("waiting"), { reason, space: address });

describe("retryAfterKey", () => {
  it("runs a write that needs no key once", () => {
    const write = vi.fn(() => 1);
    const decide = vi.fn();
    expect(retryAfterKey(write, decide)).toBe(1);
    expect(write).toHaveBeenCalledTimes(1);
    expect(decide).not.toHaveBeenCalled();
  });

  it("waits for each named key in the same window, then succeeds", async () => {
    const second = { ...space, identifier: "another" };
    const write = vi
      .fn()
      .mockImplementationOnce(() => {
        throw pending();
      })
      .mockImplementationOnce(() => {
        throw pending("space-pending", second);
      })
      .mockReturnValue("written");
    const decide = vi.fn<Parameters<typeof retryAfterKey>[1]>(async () => "retry");
    await expect(retryAfterKey(write, decide)).resolves.toBe("written");
    const start = decide.mock.calls[0]?.[1];
    expect(decide.mock.calls).toEqual([
      [space, expect.any(Number), false],
      [second, start, true],
    ]);
  });

  it("never retries an unrelated error even when its text names a pending key", () => {
    const error = new Error("WriteRejected: key-pending: space aaaa/bbbb");
    const decide = vi.fn();
    expect(() =>
      retryAfterKey(() => {
        throw error;
      }, decide),
    ).toThrow(error);
    expect(decide).not.toHaveBeenCalled();
  });

  it("returns the last key refusal when the runtime stops waiting", async () => {
    const last = pending("space-pending");
    const write = vi
      .fn()
      .mockImplementationOnce(() => {
        throw pending();
      })
      .mockImplementation(() => {
        throw last;
      });
    const decide = vi.fn().mockResolvedValueOnce("retry").mockResolvedValueOnce("stop");
    await expect(retryAfterKey(write, decide)).rejects.toBe(last);
    expect(write).toHaveBeenCalledTimes(2);
  });
});
