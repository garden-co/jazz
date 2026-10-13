import { describe, expect, it } from "vitest";
import { PersistedWriteRejectedError } from "jazz-tools";
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
    expect(isExclusiveConflict(new Error("(transaction_conflict): not a rejection"))).toBe(false);
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
