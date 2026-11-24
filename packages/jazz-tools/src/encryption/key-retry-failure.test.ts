import { expect, it, vi } from "vitest";
import { retryAfterKey } from "./key-retry.js";

it("preserves the private store failure after an encrypted write asks for its key", async () => {
  const keyPending = Object.assign(new Error("waiting"), {
    reason: "key-pending",
    space: { scopeId: "aaaa-1", identifier: "bbbb-2" },
  });
  const storeFailure = new Error("store: keychain unavailable");
  const write = vi.fn(() => {
    throw keyPending;
  });
  const prepare = vi.fn(async () => {
    throw storeFailure;
  });

  await expect(retryAfterKey(write, prepare)).rejects.toBe(storeFailure);
  expect(write).toHaveBeenCalledTimes(1);
  expect(prepare).toHaveBeenCalledExactlyOnceWith(
    { scopeId: "aaaa-1", identifier: "bbbb-2" },
    expect.any(Number),
    false,
  );
});
