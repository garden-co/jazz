import { expect, it, vi } from "vitest";
import { retryAfterKey } from "./key-retry.js";

it("makes one final write with the current key when the runtime grants it", async () => {
  const pending = Object.assign(new Error("waiting"), {
    reason: "key-pending",
    space: { scopeId: "aa", identifier: "bb" },
  });
  const write = vi.fn(() => {
    throw pending;
  });
  const decide = vi.fn(async () => "last" as const);
  await expect(retryAfterKey(write, decide)).rejects.toBe(pending);
  expect(decide).toHaveBeenCalledTimes(1);
  expect(write).toHaveBeenCalledTimes(2);
});
