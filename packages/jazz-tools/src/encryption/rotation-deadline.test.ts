import { afterEach, expect, it, vi } from "vitest";
import { retryAfterKey } from "./key-retry.js";

afterEach(() => vi.useRealTimers());

it("retries the write at its deadline when rotation has not answered", async () => {
  vi.useFakeTimers();
  const started = Date.now();
  const space = { scopeId: "aa", identifier: "bb" };
  const write = vi
    .fn()
    .mockImplementationOnce(() => {
      throw new Error("key-pending: space aa/bb");
    })
    .mockReturnValue("written");
  const useKey = vi.fn(async () => true);
  const prepare = vi.fn(() => new Promise<boolean>(() => {}));
  const written = retryAfterKey(write, prepare, 500, useKey);
  await vi.advanceTimersByTimeAsync(499);
  expect(write).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1);
  await expect(written).resolves.toBe("written");
  expect(prepare).toHaveBeenCalledExactlyOnceWith(space, started + 500);
  expect(useKey).toHaveBeenCalledExactlyOnceWith(space, expect.stringContaining("deadline"));
  expect(write).toHaveBeenCalledTimes(2);
});
