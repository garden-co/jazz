import { expect, it } from "vitest";
import { Encryption, EncryptionError, type EncryptionNative } from "./index.js";

it("exposes the records from a failed reset publication on the rejected promise", async () => {
  const publication = {
    code: "authorization_denied",
    message: "the authority refused the write",
    stage: "account-reset",
    outcome: "refused",
    recordIds: [
      "00000000-0000-4000-8000-000000000001",
      "00000000-0000-4000-8000-000000000002",
      "00000000-0000-4000-8000-000000000003",
    ],
  };
  const native: EncryptionNative = {
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status: "active" }),
    encryptionCall: async (op: string) => {
      expect(op).toBe("resetAccount");
      throw new Error(`publication: ${JSON.stringify(publication)}`);
    },
  };
  const encryption = new Encryption(native);

  const error = await encryption.resetAccount().catch((error: unknown) => error);
  expect(error).toBeInstanceOf(EncryptionError);
  expect(error).toMatchObject(publication);
});
