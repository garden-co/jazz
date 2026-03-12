import { describe, expect, it } from "vitest";
import { JazzClient, PersistedMutationError, type Runtime } from "./client.js";
import type { AppContext } from "./context.js";
import { ObjectOutcomeMirror } from "./object-outcomes.js";

const PERSISTED_MUTATION_ERROR_CAUSE_PREFIX = "__jazzPersistedMutationError__:";

function makeClient() {
  const updateCalls: Array<[string, Record<string, unknown>]> = [];
  const updateDurableCalls: Array<[string, Record<string, unknown>, string]> = [];
  const deleteCalls: string[] = [];
  const deleteDurableCalls: Array<[string, string]> = [];

  const runtime: Runtime = {
    insert: () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    insertDurable: async () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    update: (objectId: string, updates: Record<string, unknown>) => {
      updateCalls.push([objectId, updates]);
    },
    updateDurable: async (objectId: string, updates: Record<string, unknown>, tier: string) => {
      updateDurableCalls.push([objectId, updates, tier]);
    },
    delete: (objectId: string) => {
      deleteCalls.push(objectId);
    },
    deleteDurable: async (objectId: string, tier: string) => {
      deleteDurableCalls.push([objectId, tier]);
    },
    query: async () => [],
    subscribe: () => 0,
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  const context: AppContext = {
    appId: "test-app",
    schema: {},
    serverUrl: "http://localhost:1625",
    backendSecret: "test-backend-secret",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "worker" | "edge" | "global",
    ): JazzClient;
  };

  return {
    client: new JazzClientCtor(runtime, context, "edge"),
    updateCalls,
    updateDurableCalls,
    deleteCalls,
    deleteDurableCalls,
  };
}

describe("JazzClient mutation durability split", () => {
  it("routes update/delete through the synchronous runtime methods", () => {
    const { client, updateCalls, deleteCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(client.update("row-1", updates)).toBeUndefined();
    expect(client.delete("row-1")).toBeUndefined();

    expect(updateCalls).toEqual([["row-1", updates]]);
    expect(deleteCalls).toEqual(["row-1"]);
  });

  it("routes updateDurable/deleteDurable through durability-aware runtime methods", async () => {
    const { client, updateDurableCalls, deleteDurableCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    const updatePending = client.updateDurable("row-1", updates);
    const deletePending = client.deleteDurable("row-1", { tier: "global" });

    expect(updatePending).toBeInstanceOf(Promise);
    expect(deletePending).toBeInstanceOf(Promise);

    await updatePending;
    await deletePending;

    expect(updateDurableCalls).toEqual([["row-1", updates, "edge"]]);
    expect(deleteDurableCalls).toEqual([["row-1", "global"]]);
  });

  it("wraps structured durable mutation rejections with acknowledge()", async () => {
    const { client } = makeClient();
    const acknowledgeCalls: string[] = [];
    client.setObjectOutcomeSource(
      new ObjectOutcomeMirror(async (mutationId) => {
        acknowledgeCalls.push(mutationId);
      }),
    );

    const runtime = (client as unknown as { runtime: Runtime }).runtime;
    runtime.updateDurable = async () => {
      const error = new Error("mutation rejected");
      Object.assign(error, {
        name: "PersistedMutationError",
        jazzPersistedMutationError: {
          mutationId: "mutation-2",
          rootMutationId: "mutation-1",
          objectId: "row-1",
          branchName: "main",
          operation: "update",
          commitIds: ["commit-1"],
          previousCommitIds: ["commit-0"],
          code: "permission_denied",
          reason: "blocked",
          rejectedAtMicros: 123,
        },
      });
      throw error;
    };

    const rejection = await client
      .updateDurable("row-1", { done: { type: "Boolean", value: true } })
      .catch((error) => error);

    expect(rejection).toBeInstanceOf(PersistedMutationError);
    expect(rejection).toMatchObject({
      mutationId: "mutation-2",
      rootMutationId: "mutation-1",
      objectId: "row-1",
      branchName: "main",
      operation: "update",
      code: "permission_denied",
      reason: "blocked",
      rejectedAtMicros: 123,
    });

    await rejection.acknowledge();
    expect(acknowledgeCalls).toEqual(["mutation-2"]);
  });

  it("parses napi-style durable mutation rejection causes", async () => {
    const { client } = makeClient();
    const runtime = (client as unknown as { runtime: Runtime }).runtime;
    runtime.deleteDurable = async () => {
      const error = new Error("mutation mutation-3 rejected: blocked");
      (error as { cause?: unknown }).cause = new Error(
        `${PERSISTED_MUTATION_ERROR_CAUSE_PREFIX}${JSON.stringify({
          mutationId: "mutation-3",
          rootMutationId: "mutation-1",
          objectId: "row-1",
          branchName: "main",
          operation: "delete",
          commitIds: ["commit-3"],
          previousCommitIds: ["commit-2"],
          code: "permission_denied",
          reason: "blocked",
          rejectedAtMicros: 456,
        })}`,
      );
      throw error;
    };

    const rejection = await client.deleteDurable("row-1").catch((error) => error);

    expect(rejection).toBeInstanceOf(PersistedMutationError);
    expect(rejection).toMatchObject({
      mutationId: "mutation-3",
      rootMutationId: "mutation-1",
      operation: "delete",
      code: "permission_denied",
      reason: "blocked",
    });
  });
});
