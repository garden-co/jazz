import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Row, type Runtime } from "./client.js";
import type { AppContext } from "./context.js";
import { ObjectOutcomeMirror } from "./object-outcomes.js";

vi.mock("jazz-wasm", () => ({
  default: async () => {},
  initSync: () => {},
  WasmRuntime: class {},
}));

function makeClient() {
  let subscriptionCallback: ((delta: unknown) => void) | null = null;

  const runtime: Runtime = {
    insert: () => ({ id: "row-1", values: [] }),
    insertDurable: async () => ({ id: "row-1", values: [] }),
    update: () => {},
    updateDurable: async () => {},
    delete: () => {},
    deleteDurable: async () => {},
    query: async () => [{ id: "row-1", values: [] }],
    subscribe: () => 1,
    createSubscription: () => 1,
    executeSubscription: (_handle: number, onUpdate: Function) => {
      subscriptionCallback = onUpdate as (delta: unknown) => void;
    },
    unsubscribe: () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "client-id",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  const context: AppContext = {
    appId: "test-app",
    schema: {},
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "worker" | "edge" | "global",
    ): JazzClient;
  };

  return {
    client: new JazzClientCtor(runtime, context, "worker"),
    emitSubscriptionDelta(delta: unknown) {
      if (!subscriptionCallback) {
        throw new Error("subscription callback not registered");
      }
      subscriptionCallback(delta);
    },
  };
}

describe("JazzClient object outcomes", () => {
  it("enriches queried rows with the current object outcome", async () => {
    const { client } = makeClient();
    const mirror = new ObjectOutcomeMirror();
    mirror.replaceSnapshot([
      {
        objectId: "row-1",
        outcome: {
          type: "pending",
          mutationId: "mutation-1",
        },
      },
    ]);
    client.setObjectOutcomeSource(mirror);

    const rows = await client.queryInternal('{"table":"todos"}');

    expect(rows).toEqual([
      {
        id: "row-1",
        values: [],
        $outcome: {
          type: "pending",
          mutationId: "mutation-1",
        },
      },
    ] satisfies Row[]);
  });

  it("emits subscription updates when only the object outcome changes", async () => {
    const { client, emitSubscriptionDelta } = makeClient();
    const mirror = new ObjectOutcomeMirror();
    client.setObjectOutcomeSource(mirror);

    const seen: Row[][] = [];
    client.subscribeInternal('{"table":"todos"}', (delta) => {
      seen.push(
        delta.flatMap((change) => (change.kind === 1 || !change.row ? [] : [change.row as Row])),
      );
    });

    await Promise.resolve();

    emitSubscriptionDelta([
      {
        kind: 0,
        id: "row-1",
        index: 0,
        row: {
          id: "row-1",
          values: [],
        },
      },
    ]);

    mirror.applyEvents([
      {
        objectId: "row-1",
        outcome: {
          type: "errored",
          mutationId: "mutation-1",
          code: "permission_denied",
          reason: "blocked",
        },
      },
    ]);

    expect(seen).toHaveLength(2);
    expect(seen[0]).toEqual([
      {
        id: "row-1",
        values: [],
      },
    ]);
    expect(seen[1][0]).toMatchObject({
      id: "row-1",
      values: [],
      $outcome: {
        type: "errored",
        mutationId: "mutation-1",
        code: "permission_denied",
        reason: "blocked",
      },
    });
    expect(seen[1][0]?.$outcome?.type).toBe("errored");
    if (seen[1][0]?.$outcome?.type === "errored") {
      expect(typeof seen[1][0].$outcome.acknowledge).toBe("function");
    }
  });
});
