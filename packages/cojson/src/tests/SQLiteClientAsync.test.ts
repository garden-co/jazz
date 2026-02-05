import { describe, expect, test } from "vitest";
import { getDbPath } from "./testStorage.js";
import { setupTestNode } from "./testUtils.js";

describe("SQLiteClientAsync", () => {
  describe("transaction", () => {
    test("serializes concurrent transactions to avoid SQLITE_BUSY errors", async () => {
      const node = setupTestNode();
      const { storage } = await node.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: getDbPath(),
      });

      // @ts-expect-error - dbClient is private
      const dbClient = storage.dbClient;

      const times = Array.from({ length: 10 });
      await Promise.all(
        times.map(async (_, i) => {
          return dbClient.transaction(async (tx) => {
            // Sleep between 0 and 100ms to force interleaving
            await new Promise((r) => setTimeout(r, Math.random() * 100));
            tx.addSignatureAfter({
              sessionRowID: 0,
              idx: i,
              signature: `signature_z${i}`,
            });
          });
        }),
      );

      const signatures = await dbClient.getSignatures(0, 0);
      expect(signatures.length).toBe(10);
      signatures.forEach(async ({ signature }, i) => {
        expect(signature).toBe(`signature_z${i}`);
      });
    });
  });
});
