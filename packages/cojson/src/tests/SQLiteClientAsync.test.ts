import { beforeEach, describe, expect, test } from "vitest";
import { getDbPath } from "./testStorage.js";
import { setupTestNode } from "./testUtils.js";
import { DBClientInterfaceAsync } from "../exports.js";

describe("SQLiteClientAsync", () => {
  describe("transaction", () => {
    let dbClient: DBClientInterfaceAsync;

    beforeEach(async () => {
      const node = setupTestNode();
      const { storage } = await node.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: getDbPath(),
      });
      // @ts-expect-error - dbClient is private
      dbClient = storage.dbClient;
    });

    test("serializes concurrent transactions to avoid SQLITE_BUSY errors", async () => {
      const times = Array.from({ length: 10 });
      await Promise.all(
        times.map(async (_, i) => {
          return dbClient.transaction(async (tx) => {
            // Sleep between 0 and 100ms to force interleaving
            await new Promise((r) => setTimeout(r, Math.random() * 100));
            return tx.addSignatureAfter({
              sessionRowID: 0,
              idx: i,
              signature: `signature_z${i}`,
            });
          });
        }),
      );

      const signatures = await dbClient.getSignatures(0, 0);
      expect(signatures.length).toBe(10);
      signatures.forEach(({ signature }, i) => {
        expect(signature).toBe(`signature_z${i}`);
      });
    });

    test("continues to serialize transactions even if one fails", async () => {
      // First transaction succeeds
      await dbClient.transaction(async (tx) => {
        return tx.addSignatureAfter({
          sessionRowID: 0,
          idx: 0,
          signature: `signature_z0`,
        });
      });
      // Second transaction fails (duplicate primary key)
      await expect(
        dbClient.transaction(async (tx) => {
          return tx.addSignatureAfter({
            sessionRowID: 0,
            idx: 0,
            signature: `signature_z0`,
          });
        }),
      ).rejects.toThrow(
        /UNIQUE constraint failed: signatureAfter\.ses, signatureAfter\.idx/,
      );
      // Third transaction succeeds
      await dbClient.transaction(async (tx) => {
        return tx.addSignatureAfter({
          sessionRowID: 0,
          idx: 1,
          signature: `signature_z1`,
        });
      });
    });
  });
});
