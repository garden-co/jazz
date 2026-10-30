import { describe, expect, it } from "vitest";
import { PersistedWriteRejectedError } from "jazz-tools";
import { writeRejectionReason } from "../src/lib/write-rejection";

type TxId = ConstructorParameters<typeof PersistedWriteRejectedError>[0];

describe("writeRejectionReason", () => {
  it("returns the reason of a rejected write, without its transaction id", () => {
    const rejected = new PersistedWriteRejectedError(
      "tx-1" as unknown as TxId,
      "permission_denied",
      "not a member of this room",
    );
    expect(writeRejectionReason(rejected)).toBe("not a member of this room");
  });

  it("matches a rejection from another copy of jazz-tools by name", () => {
    const foreign = Object.assign(new Error("Persisted transaction tx-2 was rejected"), {
      name: "PersistedWriteRejectedError",
      reason: "not a member of this room",
    });
    expect(writeRejectionReason(foreign)).toBe("not a member of this room");
  });

  it("ignores failures that leave the write committed locally", () => {
    expect(writeRejectionReason(new Error("database is shutting down"))).toBeUndefined();
    expect(writeRejectionReason("transport closed")).toBeUndefined();
  });
});
