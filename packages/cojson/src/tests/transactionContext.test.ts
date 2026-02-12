import { describe, expect, test } from "vitest";
import { TransactionContext } from "../transactionContext.js";
import type { VerifiedState } from "../coValueCore/verifiedState.js";
import type { CoValueKnownState } from "../knownState.js";

const createTestCoValue = (id: string) => ({ id }) as unknown as VerifiedState;

const createTestKnownState = (label: string) =>
  ({ label }) as unknown as CoValueKnownState;

describe("TransactionContext", () => {
  test("bufferMessage adds messages to pending list", () => {
    const context = new TransactionContext();
    const coValue1 = createTestCoValue("co_test1");
    const state1 = createTestKnownState("state1");
    const coValue2 = createTestCoValue("co_test2");
    const state2 = createTestKnownState("state2");

    context.bufferMessage(coValue1, state1);
    context.bufferMessage(coValue2, state2);

    const messages = context.getPendingMessages();
    expect(messages).toHaveLength(2);
    expect(messages[0]).toEqual([coValue1, state1]);
    expect(messages[1]).toEqual([coValue2, state2]);
  });

  test("getPendingMessages returns messages in order", () => {
    const context = new TransactionContext();
    const coValue1 = createTestCoValue("co_test1");
    const state1 = createTestKnownState("state1");
    const coValue2 = createTestCoValue("co_test2");
    const state2 = createTestKnownState("state2");
    const coValue3 = createTestCoValue("co_test3");
    const state3 = createTestKnownState("state3");

    context.bufferMessage(coValue1, state1);
    context.bufferMessage(coValue2, state2);
    context.bufferMessage(coValue3, state3);

    const messages = context.getPendingMessages();
    expect(messages).toEqual([
      [coValue1, state1],
      [coValue2, state2],
      [coValue3, state3],
    ]);
  });

  test("isActive returns true for active context", () => {
    const context = new TransactionContext();
    expect(context.isActive()).toBe(true);
  });

  test("clear empties the buffer", () => {
    const context = new TransactionContext();
    context.bufferMessage(
      createTestCoValue("co_test1"),
      createTestKnownState("state1"),
    );
    context.bufferMessage(
      createTestCoValue("co_test2"),
      createTestKnownState("state2"),
    );

    expect(context.getPendingMessages()).toHaveLength(2);

    context.clear();

    expect(context.getPendingMessages()).toHaveLength(0);
  });

  test("messageCount returns correct count", () => {
    const context = new TransactionContext();
    expect(context.messageCount).toBe(0);

    context.bufferMessage(
      createTestCoValue("co_test1"),
      createTestKnownState("state1"),
    );
    expect(context.messageCount).toBe(1);

    context.bufferMessage(
      createTestCoValue("co_test2"),
      createTestKnownState("state2"),
    );
    expect(context.messageCount).toBe(2);
  });

  test("isEmpty returns true for empty context", () => {
    const context = new TransactionContext();
    expect(context.isEmpty).toBe(true);

    context.bufferMessage(
      createTestCoValue("co_test1"),
      createTestKnownState("state1"),
    );
    expect(context.isEmpty).toBe(false);

    context.clear();
    expect(context.isEmpty).toBe(true);
  });

  test("multiple messages for same CoValue are preserved in order", () => {
    const context = new TransactionContext();
    const id = "co_test1";

    const coValue1 = createTestCoValue(id);
    const state1 = createTestKnownState("state1");
    const coValue2 = createTestCoValue(id);
    const state2 = createTestKnownState("state2");
    const coValue3 = createTestCoValue(id);
    const state3 = createTestKnownState("state3");

    context.bufferMessage(coValue1, state1);
    context.bufferMessage(coValue2, state2);
    context.bufferMessage(coValue3, state3);

    const messages = context.getPendingMessages();
    expect(messages).toHaveLength(3);
    expect(messages[0]).toEqual([coValue1, state1]);
    expect(messages[1]).toEqual([coValue2, state2]);
    expect(messages[2]).toEqual([coValue3, state3]);
  });
});
