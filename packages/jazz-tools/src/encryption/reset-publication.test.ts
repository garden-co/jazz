import { expect, expectTypeOf, it } from "vitest";
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
    encryptionCall: async (op: string) => {
      expect(op).toBe("resetAccount");
      throw new Error(`publication: ${JSON.stringify(publication)}`);
    },
    takeEncryptionReports: () => [],
    encryptionState: () => "{}",
  };
  const encryption = new Encryption(native);

  const error = await encryption.resetAccount().catch((error: unknown) => error);
  expect(error).toBeInstanceOf(EncryptionError);
  if (error instanceof EncryptionError) {
    expectTypeOf(error.stage).toEqualTypeOf<"account-reset" | undefined>();
    expectTypeOf(error.outcome).toEqualTypeOf<"refused" | "unknown" | undefined>();
    expectTypeOf(error.recordIds).toEqualTypeOf<readonly [string, string, string] | undefined>();
    expect(error).toMatchObject(publication);
  }
});
