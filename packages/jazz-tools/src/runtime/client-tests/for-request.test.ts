import { describe, expect, it, vi } from "vitest";
import {
  JazzClient,
  flushMicrotasks,
  makeClient,
  makeClientWithContext,
  mockMutation,
  mockRow,
  runtimeTransactionRecordStubs,
  type AppContext,
  type Runtime,
} from "./support.js";

describe("JazzClient runtime helpers", () => {
  it("enables backend mode when backend secret + server URL are configured", () => {
    const { client } = makeClient();
    expect(client.asBackend()).toBe(client);
  });

  it("throws when backend mode is requested without backend secret", () => {
    const client = makeClientWithContext({
      appId: "test-app",
      schema: {},
      serverUrl: "http://localhost:1625",
    });
    expect(() => client.asBackend()).toThrow("backendSecret required for backend mode");
  });

  it("throws when backend mode is requested without server URL", () => {
    const client = makeClientWithContext({
      appId: "test-app",
      schema: {},
      backendSecret: "test-backend-secret",
    });
    expect(() => client.asBackend()).toThrow("serverUrl required for backend mode");
  });

  it("accepts runtime query JSON strings for subscribe calls", async () => {
    const { client, createSubscriptionCalls, executeSubscriptionCalls } = makeClient();
    const queryJson = '{"relation_ir":{"table":"todos"}}';

    client.subscribe(queryJson, () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    expect(createSubscriptionCalls[0]![0]).toBe(queryJson);
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("forwards structured runtime deltas to subscription callbacks", async () => {
    const { client, executeSubscriptionCalls } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    const onUpdate = executeSubscriptionCalls[0]![1];
    const delta = {
      added: [
        {
          sourceId: "row-a",
          occurrenceKey: Uint8Array.of(1),
          index: 0,
          row: { id: "row-a", values: [] },
        },
      ],
      removed: [],
      updated: [],
    };
    onUpdate(delta);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith(delta);
  });

  it("passes query propagation options to runtime query", async () => {
    const { client, queryCalls } = makeClient();
    await client.queryInternal('{"table":"todos"}', { propagation: "local-only" });
    expect(queryCalls[0]![3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  it("passes transaction overlay options to runtime query for transaction reads", async () => {
    const queryCalls: Array<[string, string | undefined, string | undefined, string | undefined]> =
      [];
    let writeContextJson: string | null | undefined;

    const runtime: Runtime = {
      ...runtimeTransactionRecordStubs,
      insert: (_table, _values, contextJson) => {
        writeContextJson = contextJson;
        return mockRow("00000000-0000-0000-0000-000000000001");
      },
      restore: (_table, _objectId, _values, contextJson) => {
        writeContextJson = contextJson;
        return mockRow("00000000-0000-0000-0000-000000000001");
      },
      update: () => mockMutation(),
      delete: () => mockMutation(),
      query: async (
        queryJson: string,
        sessionJson?: string | null,
        tier?: string | null,
        optionsJson?: string | null,
      ) => {
        queryCalls.push([
          queryJson,
          sessionJson ?? undefined,
          tier ?? undefined,
          optionsJson ?? undefined,
        ]);
        return [];
      },
      createSubscription: () => 0,
      executeSubscription: () => {},
      unsubscribe: () => {},
    };

    const JazzClientCtor = JazzClient as unknown as {
      new (
        runtime: Runtime,
        context: AppContext,
        defaultDurabilityTier: "local" | "edge" | "global",
      ): JazzClient;
    };
    const client = new JazzClientCtor(
      runtime,
      {
        appId: "test-app",
        schema: {},
        serverUrl: "http://localhost:1625",
        backendSecret: "test-backend-secret",
      },
      "edge",
    );

    const transactionId = client.beginTransaction("exclusive");
    client.insertInternal(
      "todos",
      { done: { type: "Boolean", value: false } },
      undefined,
      undefined,
      undefined,
      transactionId,
    );
    await client.queryInternal(
      '{"table":"todos"}',
      {
        localUpdates: "deferred",
        openTransactionId: transactionId,
      },
      undefined,
    );

    const writeContext = JSON.parse(writeContextJson ?? "{}");
    expect(queryCalls[0]![3]).toBe(
      JSON.stringify({
        local_updates: "deferred",
        transaction_id: writeContext.transaction_id,
      }),
    );
  });

  it("lowers the internal local-only tier to local-only propagation", () => {
    const { client, createSubscriptionCalls } = makeClient();
    client.subscribeInternal('{"table":"todos"}', () => {}, {
      tier: "local-only",
    });
    expect(createSubscriptionCalls[0]![3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  // =========================================================================
  // 2-phase subscribe lifecycle
  // =========================================================================

  it("createSubscription and executeSubscription are called synchronously", () => {
    const { client, createSubscriptionCalls, executeSubscriptionCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("returns the handle from runtime.createSubscription", () => {
    const { client } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    expect(subId).toBe(0);
    const subId2 = client.subscribe('{"table":"todos"}', () => {});
    expect(subId2).toBe(1);
  });

  it("unsubscribe calls runtime.unsubscribe with the handle", () => {
    const { client, executeSubscriptionCalls, unsubscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    client.unsubscribe(subId);

    expect(unsubscribeCalls).toEqual([0]);
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("unsubscribe after execute calls runtime.unsubscribe", async () => {
    const { client, unsubscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    await flushMicrotasks();
    client.unsubscribe(subId);
    expect(unsubscribeCalls).toEqual([0]);
  });

  it("unsubscribe unknown handle is a no-op", () => {
    const { client } = makeClient();
    expect(() => client.unsubscribe(123_456)).not.toThrow();
  });
});
