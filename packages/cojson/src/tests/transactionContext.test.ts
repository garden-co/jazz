import { describe, expect, test } from "vitest";
import { TransactionContext } from "../transactionContext.js";
import type { NewContentMessage } from "../sync.js";
import type { RawCoID, SessionID } from "../ids.js";
import { CO_VALUE_PRIORITY } from "../priority.js";

function createTestMessage(id: RawCoID): NewContentMessage {
  return {
    action: "content",
    id,
    header: {
      type: "comap",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      createdAt: new Date().toISOString() as `2${string}`,
      uniqueness: "test",
    },
    priority: CO_VALUE_PRIORITY.MEDIUM,
    new: {
      ["test-session" as SessionID]: {
        after: 0,
        newTransactions: [],
        lastSignature: "test-signature" as any,
      },
    },
  };
}

describe("TransactionContext", () => {
  test("bufferMessage adds messages to pending list", () => {
    const context = new TransactionContext();
    const msg1 = createTestMessage("co_test1" as RawCoID);
    const msg2 = createTestMessage("co_test2" as RawCoID);

    context.bufferMessage(msg1);
    context.bufferMessage(msg2);

    const messages = context.getPendingMessages();
    expect(messages).toHaveLength(2);
    expect(messages[0]).toBe(msg1);
    expect(messages[1]).toBe(msg2);
  });

  test("getPendingMessages returns messages in order", () => {
    const context = new TransactionContext();
    const msg1 = createTestMessage("co_test1" as RawCoID);
    const msg2 = createTestMessage("co_test2" as RawCoID);
    const msg3 = createTestMessage("co_test3" as RawCoID);

    context.bufferMessage(msg1);
    context.bufferMessage(msg2);
    context.bufferMessage(msg3);

    const messages = context.getPendingMessages();
    expect(messages).toEqual([msg1, msg2, msg3]);
  });

  test("getCoValueIds returns unique CoValue IDs", () => {
    const context = new TransactionContext();
    const id1 = "co_test1" as RawCoID;
    const id2 = "co_test2" as RawCoID;

    context.bufferMessage(createTestMessage(id1));
    context.bufferMessage(createTestMessage(id1)); // Duplicate
    context.bufferMessage(createTestMessage(id2));

    const ids = context.getCoValueIds();
    expect(ids.size).toBe(2);
    expect(ids.has(id1)).toBe(true);
    expect(ids.has(id2)).toBe(true);
  });

  test("isActive returns true for active context", () => {
    const context = new TransactionContext();
    expect(context.isActive()).toBe(true);
  });

  test("clear empties the buffer", () => {
    const context = new TransactionContext();
    context.bufferMessage(createTestMessage("co_test1" as RawCoID));
    context.bufferMessage(createTestMessage("co_test2" as RawCoID));

    expect(context.getPendingMessages()).toHaveLength(2);
    expect(context.getCoValueIds().size).toBe(2);

    context.clear();

    expect(context.getPendingMessages()).toHaveLength(0);
    expect(context.getCoValueIds().size).toBe(0);
  });

  test("messageCount returns correct count", () => {
    const context = new TransactionContext();
    expect(context.messageCount).toBe(0);

    context.bufferMessage(createTestMessage("co_test1" as RawCoID));
    expect(context.messageCount).toBe(1);

    context.bufferMessage(createTestMessage("co_test2" as RawCoID));
    expect(context.messageCount).toBe(2);
  });

  test("isEmpty returns true for empty context", () => {
    const context = new TransactionContext();
    expect(context.isEmpty).toBe(true);

    context.bufferMessage(createTestMessage("co_test1" as RawCoID));
    expect(context.isEmpty).toBe(false);

    context.clear();
    expect(context.isEmpty).toBe(true);
  });

  test("multiple messages for same CoValue are preserved in order", () => {
    const context = new TransactionContext();
    const id = "co_test1" as RawCoID;

    const msg1 = createTestMessage(id);
    const msg2 = createTestMessage(id);
    const msg3 = createTestMessage(id);

    context.bufferMessage(msg1);
    context.bufferMessage(msg2);
    context.bufferMessage(msg3);

    const messages = context.getPendingMessages();
    expect(messages).toHaveLength(3);
    expect(messages[0]).toBe(msg1);
    expect(messages[1]).toBe(msg2);
    expect(messages[2]).toBe(msg3);

    // But only one unique CoValue ID
    expect(context.getCoValueIds().size).toBe(1);
  });
});
