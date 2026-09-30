import { PersistedWriteRejectedError } from "jazz-tools";
import { expect, it } from "vitest";
import { BootstrapConflictError, withBoundedConflictRetry } from "../../src/lib/retry.js";

const noSleep = async () => undefined;
const rejected = (code: string) =>
  new PersistedWriteRejectedError("tx" as never, code, "another transaction won");

it("retries a transient first-open conflict and then succeeds", async () => {
  let calls = 0;
  const result = await withBoundedConflictRetry(
    async () => {
      calls += 1;
      if (calls < 3) throw rejected("exclusive_conflict");
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
        throw rejected("transaction_conflict");
      },
      { attempts: 4, baseDelayMs: 10, sleep: async (ms) => void delays.push(ms) },
    ),
  ).rejects.toBeInstanceOf(BootstrapConflictError);
  expect(calls).toBe(4);
  expect(delays).toEqual([10, 20, 40]);
});

it.each(["transaction_conflict", "exclusive_conflict"])("retries a %s rejection", async (code) => {
  let calls = 0;
  await withBoundedConflictRetry(
    async () => {
      calls += 1;
      if (calls === 1) throw rejected(code);
    },
    { sleep: noSleep },
  );
  expect(calls).toBe(2);
});

it("does not retry other rejections or errors that only mention a conflict", async () => {
  for (const error of [
    rejected("permission_denied"),
    new Error("exclusive_conflict mentioned in an unrelated message"),
    // An untyped error with a conflict `code` is no longer accepted (#3753).
    Object.assign(new Error("TransactionConflict"), { code: "transaction_conflict" }),
  ]) {
    let calls = 0;
    await expect(
      withBoundedConflictRetry(
        async () => {
          calls += 1;
          throw error;
        },
        { sleep: noSleep },
      ),
    ).rejects.toBe(error);
    expect(calls).toBe(1);
  }
});
