import { describe, expect, it, vi } from "vitest";
import {
  JazzClient,
  flushMicrotasks,
  makeClient,
  makeClientWithContext,
  mockMutation,
  mockRow,
  runtimeBatchRecordStubs,
  schemaWithTodos,
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

  it("accepts query builders for subscribe calls", async () => {
    const { client, createSubscriptionCalls, executeSubscriptionCalls } = makeClient();

    const builder = {
      _build() {
        return '{"table":"todos"}';
      },
    };

    client.subscribe(builder, () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    expect(createSubscriptionCalls[0]![0]).toBe(builder._build());
    expect(executeSubscriptionCalls).toHaveLength(0);
    await flushMicrotasks();
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("translates schema-aware query builders for subscribe calls", async () => {
    const { client, createSubscriptionCalls } = makeClient();

    const builder = {
      _schema: schemaWithTodos,
      _build() {
        return JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        });
      },
    };

    client.subscribe(builder, () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    const parsed = JSON.parse(createSubscriptionCalls[0]![0]) as Record<string, unknown>;
    expect(parsed.table).toBe("todos");
    expect(parsed).toHaveProperty("relation_ir");
  });

  it("forwards structured RN delta payloads to subscription callbacks", async () => {
    const { client, executeSubscriptionCalls } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    const onUpdate = executeSubscriptionCalls[0]![1];
    onUpdate(
      JSON.stringify({
        added: [{ row: { id: "row-a", values: [] }, index: 0 }],
        removed: [{ row: { id: "row-r", values: [] }, index: 1 }],
        updated: [
          {
            old_row: { id: "row-u", values: [] },
            new_row: { id: "row-u", values: [] },
            old_index: 0,
            new_index: 0,
          },
        ],
        pending: false,
      }),
    );

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith({
      added: [{ row: { id: "row-a", values: [] }, index: 0 }],
      removed: [{ row: { id: "row-r", values: [] }, index: 1 }],
      updated: [
        {
          old_row: { id: "row-u", values: [] },
          new_row: { id: "row-u", values: [] },
          old_index: 0,
          new_index: 0,
        },
      ],
      pending: false,
    });
  });

  it("forwards NAPI error-first delta payloads to subscription callbacks", async () => {
    const { client, executeSubscriptionCalls } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    const onUpdate = executeSubscriptionCalls[0]![1];
    onUpdate(null, [
      {
        kind: 0,
        id: "row-a",
        index: 0,
        row: { id: "row-a", values: [] },
      },
    ]);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith([
      {
        kind: 0,
        id: "row-a",
        index: 0,
        row: { id: "row-a", values: [] },
      },
    ]);
  });

  it("forwards partial structured deltas without throwing", async () => {
    const { client, executeSubscriptionCalls } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    const onUpdate = executeSubscriptionCalls[0]![1];
    expect(() =>
      onUpdate(
        JSON.stringify({
          pending: true,
        }),
      ),
    ).not.toThrow();

    expect(callback).toHaveBeenCalledWith({
      pending: true,
    });
  });

  it("passes query propagation options to runtime query", async () => {
    const { client, queryCalls } = makeClient();
    await client.query('{"table":"todos"}', { propagation: "local-only" });
    expect(queryCalls[0]![3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  it("passes transaction overlay options to runtime query for transaction reads", async () => {
    const queryCalls: Array<[string, string | undefined, string | undefined, string | undefined]> =
      [];
    let writeContextJson: string | null | undefined;

    const runtime: Runtime = {
      ...runtimeBatchRecordStubs,
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
      getSchema: () => ({}),
      getSchemaHash: () => "schema-hash",
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

    const batchId = client.beginBatch("transactional");
    client.insertInternal(
      "todos",
      { done: { type: "Boolean", value: false } },
      undefined,
      undefined,
      undefined,
      batchId,
    );
    await client.query(
      '{"table":"todos"}',
      {
        localUpdates: "deferred",
        transactionBatchId: batchId,
      },
      undefined,
    );

    const writeContext = JSON.parse(writeContextJson ?? "{}");
    expect(queryCalls[0]![3]).toBe(
      JSON.stringify({
        local_updates: "deferred",
        transaction_batch_id: writeContext.batch_id,
      }),
    );
  });

  it("passes query propagation options to runtime createSubscription", () => {
    const { client, createSubscriptionCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {}, {
      propagation: "local-only",
    });
    expect(createSubscriptionCalls[0]![3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  // =========================================================================
  // 2-phase subscribe lifecycle
  // =========================================================================

  it("createSubscription is called synchronously, executeSubscription is deferred", async () => {
    const { client, createSubscriptionCalls, executeSubscriptionCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    expect(executeSubscriptionCalls).toHaveLength(0);

    await flushMicrotasks();
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("returns the handle from runtime.createSubscription", () => {
    const { client } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    expect(subId).toBe(0);
    const subId2 = client.subscribe('{"table":"todos"}', () => {});
    expect(subId2).toBe(1);
  });

  it("unsubscribe before execute calls runtime.unsubscribe with the handle", async () => {
    const { client, executeSubscriptionCalls, unsubscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    client.unsubscribe(subId);

    expect(unsubscribeCalls).toEqual([0]);

    await flushMicrotasks();
    // executeSubscription still fires (the runtime no-ops since handle was already unsubscribed)
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
