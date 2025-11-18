import { describe, expect, test } from "vitest";
import {
  getNewTransactionsFromContentMessage,
  knownStateFromContent,
} from "../coValueContentMessage.js";
import { emptyKnownState } from "../knownState.js";
import { NewContentMessage, SessionNewContent } from "../sync.js";
import type { RawCoID, SessionID } from "../ids.js";
import { stableStringify } from "../jsonStringify.js";
import { CO_VALUE_PRIORITY } from "../priority.js";
import { CoValueKnownState } from "../knownState.js";
import { Transaction } from "../coValueCore/verifiedState.js";

describe("knownStateFromContent", () => {
  const mockCoID: RawCoID = "co_z1234567890abcdef";
  const mockSessionID1: SessionID = "sealer_z123/signer_z456_session_z789";
  const mockSessionID2: SessionID = "sealer_zabc/signer_zdef_session_zghi";

  test("returns empty known state for content with no header and no sessions", () => {
    const content: NewContentMessage = {
      action: "content",
      id: mockCoID,
      header: undefined,
      priority: CO_VALUE_PRIORITY.HIGH,
      new: {},
    };

    const result = knownStateFromContent(content);
    const expected = emptyKnownState(mockCoID);

    expect(result).toEqual(expected);
    expect(result.id).toBe(mockCoID);
    expect(result.header).toBe(false);
    expect(result.sessions).toEqual({});
  });

  test("sets header to true when content has header", () => {
    const content: NewContentMessage = {
      action: "content",
      id: mockCoID,
      header: {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        uniqueness: null,
        createdAt: null,
      },
      priority: CO_VALUE_PRIORITY.HIGH,
      new: {},
    };

    const result = knownStateFromContent(content);

    expect(result.header).toBe(true);
    expect(result.id).toBe(mockCoID);
    expect(result.sessions).toEqual({});
  });

  test("sets header to false when content has no header", () => {
    const content: NewContentMessage = {
      action: "content",
      id: mockCoID,
      priority: CO_VALUE_PRIORITY.HIGH,
      new: {},
    };

    const result = knownStateFromContent(content);

    expect(result.header).toBe(false);
    expect(result.id).toBe(mockCoID);
    expect(result.sessions).toEqual({});
  });

  test("calculates session states correctly for single session", () => {
    const content: NewContentMessage = {
      action: "content",
      id: mockCoID,
      header: {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        uniqueness: null,
        createdAt: null,
      },
      priority: CO_VALUE_PRIORITY.HIGH,
      new: {
        [mockSessionID1]: {
          after: 5,
          newTransactions: [
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
          ],
          lastSignature: "signature_z1234",
        },
      },
    };

    const result = knownStateFromContent(content);

    expect(result.header).toBe(true);
    expect(result.sessions[mockSessionID1]).toBe(8); // 5 + 3
    expect(Object.keys(result.sessions)).toHaveLength(1);
  });

  test("calculates session states correctly for multiple sessions", () => {
    const content: NewContentMessage = {
      action: "content",
      id: mockCoID,
      priority: CO_VALUE_PRIORITY.HIGH,
      new: {
        [mockSessionID1]: {
          after: 3,
          newTransactions: [
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
          ],
          lastSignature: "signature_z1234",
        },
        [mockSessionID2]: {
          after: 7,
          newTransactions: [
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
          ],
          lastSignature: "signature_z1234",
        },
      },
    };

    const result = knownStateFromContent(content);

    expect(result.header).toBe(false);
    expect(result.sessions[mockSessionID1]).toBe(5); // 3 + 2
    expect(result.sessions[mockSessionID2]).toBe(11); // 7 + 4
    expect(Object.keys(result.sessions)).toHaveLength(2);
  });

  test("handles session with no transactions", () => {
    const content: NewContentMessage = {
      action: "content",
      id: mockCoID,
      priority: CO_VALUE_PRIORITY.HIGH,
      new: {
        [mockSessionID1]: {
          after: 10,
          newTransactions: [],
          lastSignature: "signature_z1234",
        },
      },
    };

    const result = knownStateFromContent(content);

    expect(result.sessions[mockSessionID1]).toBe(10); // 10 + 0
  });

  test("handles session with after index 0", () => {
    const content: NewContentMessage = {
      action: "content",
      id: mockCoID,
      priority: CO_VALUE_PRIORITY.HIGH,
      new: {
        [mockSessionID1]: {
          after: 0,
          newTransactions: [
            {
              privacy: "trusting",
              madeAt: Date.now(),
              changes: stableStringify([]),
            },
          ],
          lastSignature: "signature_z1234",
        },
      },
    };

    const result = knownStateFromContent(content);

    expect(result.sessions[mockSessionID1]).toBe(1); // 0 + 1
  });
});

describe("getNewTransactionsFromContentMessage", () => {
  const mockCoID: RawCoID = "co_z1234567890abcdef";
  const mockSessionID: SessionID = "sealer_z123/signer_z456_session_z789";

  function createTransaction(): Transaction {
    return {
      privacy: "trusting",
      madeAt: Date.now(),
      changes: stableStringify([{ op: "set", key: "test", value: "value" }]),
    };
  }

  function createKnownState(
    sessionTxIdx: number | undefined,
  ): CoValueKnownState {
    const knownState = emptyKnownState(mockCoID);
    knownState.header = true;
    if (sessionTxIdx !== undefined) {
      knownState.sessions[mockSessionID] = sessionTxIdx;
    }
    return knownState;
  }

  test("returns all transactions when we know none (ourKnownTxIdx = 0, theirFirstNewTxIdx = 0)", () => {
    const transactions = [createTransaction(), createTransaction()];
    const content: SessionNewContent = {
      after: 0,
      newTransactions: transactions,
      lastSignature: "signature_z1234",
    };
    const knownState = createKnownState(undefined); // defaults to 0

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    expect(result).toEqual(transactions);
    expect(result).toHaveLength(2);
  });

  test("returns subset of transactions when we know some (ourKnownTxIdx = 3, theirFirstNewTxIdx = 0)", () => {
    const transactions = [
      createTransaction(),
      createTransaction(),
      createTransaction(),
      createTransaction(),
      createTransaction(),
    ];
    const content: SessionNewContent = {
      after: 0,
      newTransactions: transactions,
      lastSignature: "signature_z1234",
    };
    const knownState = createKnownState(3); // we already know txs 0, 1, 2

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    expect(result).toEqual(transactions.slice(3));
    expect(result).toHaveLength(2);
  });

  test("returns undefined when we're missing transactions (ourKnownTxIdx = 2, theirFirstNewTxIdx = 5)", () => {
    const transactions = [createTransaction(), createTransaction()];
    const content: SessionNewContent = {
      after: 5, // they're sending txs starting from idx 5
      newTransactions: transactions,
      lastSignature: "signature_z1234",
    };
    const knownState = createKnownState(2); // but we only know up to idx 2

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    expect(result).toBeUndefined();
  });

  test("returns empty array when we know all transactions (ourKnownTxIdx = 5, theirFirstNewTxIdx = 2)", () => {
    const transactions = [
      createTransaction(),
      createTransaction(),
      createTransaction(),
    ];
    const content: SessionNewContent = {
      after: 2, // they're sending txs 2, 3, 4
      newTransactions: transactions,
      lastSignature: "signature_z1234",
    };
    const knownState = createKnownState(5); // we already know txs up to idx 5

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    expect(result).toEqual([]);
    expect(result).toHaveLength(0);
  });

  test("returns all transactions when ourKnownTxIdx equals theirFirstNewTxIdx", () => {
    const transactions = [createTransaction(), createTransaction()];
    const content: SessionNewContent = {
      after: 3,
      newTransactions: transactions,
      lastSignature: "signature_z1234",
    };
    const knownState = createKnownState(3); // we know up to idx 3

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    expect(result).toEqual(transactions);
    expect(result).toHaveLength(2);
  });

  test("handles session not in knownState (defaults to 0)", () => {
    const transactions = [createTransaction()];
    const content: SessionNewContent = {
      after: 1,
      newTransactions: transactions,
      lastSignature: "signature_z1234",
    };
    const knownState = emptyKnownState(mockCoID);
    knownState.header = true;
    // no sessions defined, so mockSessionID defaults to 0

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    // ourKnownTxIdx = 0, theirFirstNewTxIdx = 1, so 0 < 1 -> undefined
    expect(result).toBeUndefined();
  });

  test("returns single transaction when offset is length - 1", () => {
    const transactions = [
      createTransaction(),
      createTransaction(),
      createTransaction(),
    ];
    const content: SessionNewContent = {
      after: 5,
      newTransactions: transactions,
      lastSignature: "signature_z1234",
    };
    const knownState = createKnownState(7); // we know up to idx 7 (5 + 2 transactions)

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    expect(result).toEqual([transactions[2]]);
    expect(result).toHaveLength(1);
  });

  test("handles empty newTransactions array", () => {
    const content: SessionNewContent = {
      after: 5,
      newTransactions: [],
      lastSignature: "signature_z1234",
    };
    const knownState = createKnownState(5);

    const result = getNewTransactionsFromContentMessage(
      content,
      knownState,
      mockSessionID,
    );

    expect(result).toEqual([]);
    expect(result).toHaveLength(0);
  });
});
