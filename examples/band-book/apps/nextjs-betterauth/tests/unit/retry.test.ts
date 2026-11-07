import { describe, expect, it, vi } from "vitest";
import { PersistedWriteRejectedError } from "jazz-tools";
import { isSerializationConflict, retryOnConflict } from "../../src/lib/retry.js";

type TxId = ConstructorParameters<typeof PersistedWriteRejectedError>[0];
const rejected = (code: string) =>
  new PersistedWriteRejectedError("tx-1" as unknown as TxId, code, "test");

/** A rejection from another copy of jazz-tools, as the Next server routes see it. */
function foreignRejection(code: string) {
  return Object.assign(new Error(`rejected (${code})`), {
    name: "PersistedWriteRejectedError",
    code,
  });
}

describe("conflict retries", () => {
  it("retries local and authority conflicts, and nothing else", () => {
    expect(isSerializationConflict(rejected("transaction_conflict"))).toBe(true);
    expect(isSerializationConflict(rejected("serialization_conflict"))).toBe(true);
    expect(isSerializationConflict(rejected("permission_denied"))).toBe(false);
    expect(isSerializationConflict(new Error("(transaction_conflict): not a rejection"))).toBe(
      false,
    );
  });

  it("recognises a conflict thrown by another copy of jazz-tools", () => {
    expect(isSerializationConflict(foreignRejection("transaction_conflict"))).toBe(true);
    expect(isSerializationConflict(foreignRejection("permission_denied"))).toBe(false);
  });

  it("runs the transaction again after a conflict, and gives up after a bound", async () => {
    const attempt = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(rejected("transaction_conflict"))
      .mockResolvedValueOnce("done");
    await expect(retryOnConflict(attempt)).resolves.toBe("done");
    expect(attempt).toHaveBeenCalledTimes(2);

    const forever = vi.fn(async () => {
      throw rejected("transaction_conflict");
    });
    await expect(retryOnConflict(forever, 3)).rejects.toThrow("transaction_conflict");
    expect(forever).toHaveBeenCalledTimes(3);
  });
});
