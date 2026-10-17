import { describe, expect, it } from "vitest";
import { PersistedWriteRejectedError } from "jazz-tools";
import { retryOnConflict } from "../src/lib/retry";
import { isExclusiveConflict, isPermissionDenied, writeErrorCode } from "../src/lib/write-errors";

type TxId = ConstructorParameters<typeof PersistedWriteRejectedError>[0];
const rejected = (code: string) =>
  new PersistedWriteRejectedError("tx-1" as unknown as TxId, code, "test");

describe("write errors", () => {
  it("retries local and authority conflicts, and nothing else", () => {
    expect(isExclusiveConflict(rejected("transaction_conflict"))).toBe(true);
    expect(isExclusiveConflict(rejected("exclusive_conflict"))).toBe(true);
    expect(isExclusiveConflict(rejected("cascade_rejected"))).toBe(true);
    expect(isExclusiveConflict(rejected("permission_denied"))).toBe(false);
  });

  it("a plain Error carrying a code is not retried", () => {
    const plain = Object.assign(new Error("(transaction_conflict): not a rejection"), {
      code: "transaction_conflict",
    });
    expect(writeErrorCode(plain)).toBeUndefined();
    expect(isExclusiveConflict(plain)).toBe(false);
  });

  it("tells a permission denial apart from a conflict", () => {
    expect(isPermissionDenied(rejected("permission_denied"))).toBe(true);
    expect(isPermissionDenied(rejected("transaction_conflict"))).toBe(false);
  });

  it("reads the code from another copy of the error class", () => {
    const copy = Object.assign(new Error("rejected"), {
      name: "PersistedWriteRejectedError",
      code: "exclusive_conflict",
    });
    expect(writeErrorCode(copy)).toBe("exclusive_conflict");
    expect(writeErrorCode("exclusive_conflict")).toBeUndefined();
  });
});

describe("retryOnConflict", () => {
  it("retries a conflict a bounded number of times, then rethrows it", async () => {
    let calls = 0;
    const conflict = rejected("exclusive_conflict");
    await expect(
      retryOnConflict(async () => {
        calls++;
        throw conflict;
      }, 3),
    ).rejects.toBe(conflict);
    expect(calls).toBe(3);
  });

  it("returns once an attempt succeeds, and never retries other errors", async () => {
    let calls = 0;
    expect(
      await retryOnConflict(async () => {
        if (++calls < 2) throw rejected("transaction_conflict");
        return "done";
      }),
    ).toBe("done");
    expect(calls).toBe(2);

    const denied = rejected("permission_denied");
    calls = 0;
    await expect(
      retryOnConflict(async () => {
        calls++;
        throw denied;
      }),
    ).rejects.toBe(denied);
    expect(calls).toBe(1);
  });
});
