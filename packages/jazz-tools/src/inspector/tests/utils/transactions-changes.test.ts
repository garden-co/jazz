import { assert, describe, expect, it } from "vitest";
import { setActiveAccount, setupJazzTestSync } from "jazz-tools/testing";
import { co, z } from "jazz-tools";
import * as TransactionsChanges from "../../utils/transactions-changes";

describe("transactions changes", async () => {
  const account = await setupJazzTestSync();
  setActiveAccount(account);

  describe("ambiguous values in Group's transactions", () => {
    it("isGroupExtension should return false for a CoMap", () => {
      const value = co.map({ test: z.string() }).create({ test: "extend" })
        .$jazz.raw;

      const transactions = value.core.verifiedTransactions;
      expect(
        TransactionsChanges.isGroupExtension(
          value,
          transactions[0]?.changes?.[0],
        ),
      ).toBe(false);
    });

    it("isGroupExtendRevocation should return false for a CoMap", () => {
      const value = co.map({ test: z.string() }).create({ test: "revoked" })
        .$jazz.raw;

      const transactions = value.core.verifiedTransactions;
      expect(
        TransactionsChanges.isGroupExtendRevocation(
          value,
          transactions[0]?.changes?.[0],
        ),
      ).toBe(false);
    });

    it("isGroupPromotion should return false for a CoMap", () => {
      const value = co
        .map({ parent_co_test: z.string() })
        .create({ parent_co_test: "foo" }).$jazz.raw;

      const transactions = value.core.verifiedTransactions;
      expect(
        TransactionsChanges.isGroupPromotion(
          value,
          transactions[0]?.changes?.[0],
        ),
      ).toBe(false);
    });

    it("isUserPromotion should return false for a CoMap", () => {
      const value = co.map({ everyone: z.string() }).create({ everyone: "foo" })
        .$jazz.raw;

      const transactions = value.core.verifiedTransactions;
      expect(
        TransactionsChanges.isUserPromotion(
          value,
          transactions[0]?.changes?.[0],
        ),
      ).toBe(false);
    });

    it("isUserPromotion should return false for a CoMap", () => {
      const value = co.map({ everyone: z.string() }).create({ everyone: "foo" })
        .$jazz.raw;

      const transactions = value.core.verifiedTransactions;
      expect(
        TransactionsChanges.isUserPromotion(
          value,
          transactions[0]?.changes?.[0],
        ),
      ).toBe(false);

      const value2 = co.map({ co_z123: z.string() }).create({ co_z123: "foo" })
        .$jazz.raw;

      const transactions2 = value2.core.verifiedTransactions;
      expect(
        TransactionsChanges.isUserPromotion(
          value2,
          transactions2[0]?.changes?.[0],
        ),
      ).toBe(false);
    });

    it("isKeyRevelation should return false for a CoMap", () => {
      const value = co
        .map({ "123_for_test": z.string() })
        .create({ "123_for_test": "foo" }).$jazz.raw;

      const transactions = value.core.verifiedTransactions;
      expect(
        TransactionsChanges.isKeyRevelation(
          value,
          transactions[0]?.changes?.[0],
        ),
      ).toBe(false);
    });
  });
});
