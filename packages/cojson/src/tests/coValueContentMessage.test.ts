import { describe, expect, test } from "vitest";
import { knownStateFromContent } from "../coValueContentMessage.js";
import { emptyKnownState } from "../knownState.js";
import { NewContentMessage } from "../sync.js";
import type { RawCoID, SessionID } from "../ids.js";
import { stableStringify } from "../jsonStringify.js";
import { CO_VALUE_PRIORITY } from "../priority.js";

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
