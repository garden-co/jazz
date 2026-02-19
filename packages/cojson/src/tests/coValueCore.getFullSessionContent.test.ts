import { assert, beforeEach, describe, expect, test } from "vitest";
import { fillCoMapWithLargeData, setupTestNode } from "./testUtils.js";

beforeEach(() => {
  setupTestNode({ isSyncServer: true });
});

function simplifySessionContent(
  content: ReturnType<
    NonNullable<
      ReturnType<
        ReturnType<typeof setupTestNode>["node"]["getCoValue"]
      >["verified"]
    >["getFullSessionContent"]
  >,
) {
  return content.map((piece) => ({
    after: piece.after,
    transactions: piece.newTransactions.length,
    hasSignature: Boolean(piece.lastSignature),
  }));
}

describe("VerifiedState.getFullSessionContent", () => {
  test("should return empty array when session has no transactions", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    expect(
      map.core.verified?.getFullSessionContent(client.node.currentSessionID),
    ).toEqual([]);
  });

  test("should return one chunk for a small session log", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const content =
      map.core.verified?.getFullSessionContent(client.node.currentSessionID) ??
      [];

    expect(simplifySessionContent(content)).toEqual([
      {
        after: 0,
        transactions: 1,
        hasSignature: true,
      },
    ]);
  });

  test("should include intermediate signatures for large session logs", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    fillCoMapWithLargeData(map);

    const verified = map.core.verified;
    assert(verified);

    const sessionID = client.node.currentSessionID;
    const fullContent = verified.getFullSessionContent(sessionID);
    const session = verified.getSession(sessionID);
    assert(session);
    assert(session.lastSignature);

    expect(fullContent.length).toBeGreaterThan(1);

    const signatureCheckpoints = Object.entries(session.signatureAfter)
      .map(([txIdx, signature]) => [Number(txIdx), signature] as const)
      .filter((entry) => entry[1] !== undefined)
      .sort((a, b) => a[0] - b[0]);

    expect(fullContent.length).toBe(signatureCheckpoints.length + 1);

    let processedTransactions = 0;

    for (let i = 0; i < fullContent.length; i++) {
      const piece = fullContent[i]!;

      expect(piece.after).toBe(processedTransactions);
      processedTransactions += piece.newTransactions.length;

      if (i < fullContent.length - 1) {
        const checkpointAtEnd = signatureCheckpoints[i];
        assert(checkpointAtEnd);
        const [txIdx, signature] = checkpointAtEnd;

        expect(txIdx).toBe(processedTransactions - 1);
        expect(piece.lastSignature).toBe(signature);
      } else {
        expect(piece.lastSignature).toBe(session.lastSignature);
      }
    }

    expect(processedTransactions).toBe(session.transactions.length);
  });
});
