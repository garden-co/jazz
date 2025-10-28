import { SessionID } from "cojson";
import {
  getTransactionsToTry,
  isTxSuccessful,
  markSuccessful,
  SuccessMap,
} from "../successMap";
import { beforeEach, describe, expect, it } from "vitest";
import { createJazzTestAccount } from "jazz-tools/testing";
import { Account } from "jazz-tools";

describe("successMap", () => {
  beforeEach(async () => {
    await createJazzTestAccount({
      AccountSchema: Account,
      isCurrentActiveAccount: true,
    });
  });

  it("should mark a first transaction as successful", async () => {
    const successMap = SuccessMap.create({});
    const transactionID = {
      sessionID: "co_z123_session_z123" as SessionID,
      txIndex: 0,
    };
    markSuccessful(successMap, transactionID);
    expect(successMap).toEqual({
      "co_z123_session_z123:0": true,
    });
    expect(isTxSuccessful(successMap, transactionID)).toBe(true);
  });

  it("should mark a second, consecutive transaction as successful", async () => {
    const successMap = SuccessMap.create({});
    const transactionID0 = {
      sessionID: "co_z123_session_z123" as SessionID,
      txIndex: 0,
    };
    const transactionID1 = {
      sessionID: "co_z123_session_z123" as SessionID,
      txIndex: 1,
    };
    markSuccessful(successMap, transactionID0);
    markSuccessful(successMap, transactionID1);
    expect(successMap).toEqual({
      "co_z123_session_z123:0": true,
      "co_z123_session_z123:1": true,
    });
    expect(isTxSuccessful(successMap, transactionID0)).toBe(true);
    expect(isTxSuccessful(successMap, transactionID1)).toBe(true);
    expect(
      isTxSuccessful(successMap, {
        sessionID: transactionID0.sessionID,
        txIndex: 3,
      }),
    ).toBe(false);
  });

  it("should mark a second, non-consecutive transaction as successful", async () => {
    const successMap = SuccessMap.create({});
    const transactionID0 = {
      sessionID: "co_z123_session_z123" as SessionID,
      txIndex: 0,
    };
    const transactionID2 = {
      sessionID: "co_z123_session_z123" as SessionID,
      txIndex: 2,
    };
    markSuccessful(successMap, transactionID0);
    markSuccessful(successMap, transactionID2);
    expect(successMap).toEqual({
      "co_z123_session_z123:0": true,
      "co_z123_session_z123:2": true,
    });
    expect(isTxSuccessful(successMap, transactionID0)).toBe(true);
    expect(
      isTxSuccessful(successMap, {
        sessionID: transactionID0.sessionID,
        txIndex: 1,
      }),
    ).toBe(false);
    expect(isTxSuccessful(successMap, transactionID2)).toBe(true);
    expect(
      isTxSuccessful(successMap, {
        sessionID: transactionID0.sessionID,
        txIndex: 3,
      }),
    ).toBe(false);
  });

  it("should mark gap-filling transactions as successful", async () => {
    const successMap = SuccessMap.create({});
    const transactionID0 = {
      sessionID: "co_z123_session_z123" as SessionID,
      txIndex: 0,
    };
    const transactionID2 = {
      sessionID: "co_z123_session_z123" as SessionID,
      txIndex: 2,
    };
    markSuccessful(successMap, transactionID0);
    markSuccessful(successMap, transactionID2);
    expect(successMap).toEqual({
      "co_z123_session_z123:0": true,
      "co_z123_session_z123:2": true,
    });
    expect(isTxSuccessful(successMap, transactionID0)).toBe(true);
    expect(isTxSuccessful(successMap, transactionID2)).toBe(true);
    expect(
      isTxSuccessful(successMap, {
        sessionID: transactionID0.sessionID,
        txIndex: 1,
      }),
    ).toBe(false);
    expect(
      isTxSuccessful(successMap, {
        sessionID: transactionID0.sessionID,
        txIndex: 3,
      }),
    ).toBe(false);

    markSuccessful(successMap, {
      sessionID: transactionID0.sessionID,
      txIndex: 1,
    });
    expect(successMap).toEqual({
      "co_z123_session_z123:0": true,
      "co_z123_session_z123:1": true,
      "co_z123_session_z123:2": true,
    });
    expect(isTxSuccessful(successMap, transactionID0)).toBe(true);
    expect(isTxSuccessful(successMap, transactionID2)).toBe(true);
    expect(
      isTxSuccessful(successMap, {
        sessionID: transactionID0.sessionID,
        txIndex: 1,
      }),
    ).toBe(true);
    expect(
      isTxSuccessful(successMap, {
        sessionID: transactionID0.sessionID,
        txIndex: 3,
      }),
    ).toBe(false);
  });

  it("should get transactions to retry", async () => {
    const map1 = SuccessMap.create({});

    expect(
      Array.from(
        getTransactionsToTry(map1, {
          id: "co_z123",
          header: false,
          sessions: {
            ["co_z123_session_z123" as SessionID]: 3,
          },
        }),
      ),
    ).toEqual([
      {
        sessionID: "co_z123_session_z123",
        txIndex: 0,
      },
      {
        sessionID: "co_z123_session_z123",
        txIndex: 1,
      },
      {
        sessionID: "co_z123_session_z123",
        txIndex: 2,
      },
    ]);

    const map2 = SuccessMap.create({
      "co_z123_session_z123:0": true,
      "co_z123_session_z123:2": true,
    });

    expect(
      Array.from(
        getTransactionsToTry(map2, {
          id: "co_z123",
          header: false,
          sessions: {
            ["co_z123_session_z123" as SessionID]: 3,
          },
        }),
      ),
    ).toEqual([
      {
        sessionID: "co_z123_session_z123",
        txIndex: 1,
      },
    ]);

    const map3 = SuccessMap.create({
      "co_z123_session_z123:0": true,
      "co_z123_session_z123:1": true,
      "co_z123_session_z123:2": true,
    });

    expect(
      Array.from(
        getTransactionsToTry(map3, {
          id: "co_z123",
          header: false,
          sessions: {
            ["co_z123_session_z123" as SessionID]: 3,
          },
        }),
      ),
    ).toEqual([]);
  });
});
