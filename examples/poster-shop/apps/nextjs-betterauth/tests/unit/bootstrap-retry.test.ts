import { expect, it } from "vitest";
import { BootstrapConflictError, withBoundedConflictRetry } from "../../src/lib/retry.js";

const noSleep = async () => undefined;

it("retries a transient first-open conflict and then succeeds", async () => {
  let calls = 0;
  const result = await withBoundedConflictRetry(
    async () => {
      calls += 1;
      if (calls < 3) throw new Error("exclusive_conflict: canvasMembers changed");
      return "canvas";
    },
    { sleep: noSleep },
  );
  expect(result).toBe("canvas");
  expect(calls).toBe(3);
});

it("terminates a persistent conflict with a recoverable error (#2615)", async () => {
  let calls = 0;
  const delays: number[] = [];
  await expect(
    withBoundedConflictRetry(
      async () => {
        calls += 1;
        throw new Error("transaction_conflict");
      },
      { attempts: 4, baseDelayMs: 10, sleep: async (ms) => void delays.push(ms) },
    ),
  ).rejects.toBeInstanceOf(BootstrapConflictError);
  expect(calls).toBe(4);
  expect(delays).toEqual([10, 20, 40]);
});

it("does not retry unrelated failures", async () => {
  let calls = 0;
  await expect(
    withBoundedConflictRetry(
      async () => {
        calls += 1;
        throw new Error("permission_denied");
      },
      { sleep: noSleep },
    ),
  ).rejects.toThrow("permission_denied");
  expect(calls).toBe(1);
});
