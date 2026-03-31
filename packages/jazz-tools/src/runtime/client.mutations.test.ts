import { describe, expect, it } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { AppContext, Session } from "./context.js";

function makeClient(runtimeOverrides: Partial<Runtime> = {}) {
  const insertWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const insertDurableWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string]
  > = [];
  const updateWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const updateCalls: Array<[string, Record<string, unknown>]> = [];
  const deleteWithSessionCalls: Array<[string, string | undefined]> = [];
  const updateDurableCalls: Array<[string, Record<string, unknown>, string]> = [];
  const updateDurableWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string]
  > = [];
  const deleteCalls: string[] = [];
  const deleteDurableCalls: Array<[string, string]> = [];
  const deleteDurableWithSessionCalls: Array<[string, string | undefined, string]> = [];

  const runtimeBase: Runtime = {
    insert: () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    insertWithSession: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      insertWithSessionCalls.push([table, values, writeContextJson ?? undefined]);
      return { id: "00000000-0000-0000-0000-000000000001", values: [] };
    },
    insertDurable: async () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    insertDurableWithSession: async (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      insertDurableWithSessionCalls.push([table, values, writeContextJson ?? undefined, tier]);
      return { id: "00000000-0000-0000-0000-000000000001", values: [] };
    },
    update: (objectId: string, updates: Record<string, unknown>) => {
      updateCalls.push([objectId, updates]);
    },
    updateWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined]);
    },
    updateDurable: async (objectId: string, updates: Record<string, unknown>, tier: string) => {
      updateDurableCalls.push([objectId, updates, tier]);
    },
    updateDurableWithSession: async (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      updateDurableWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined, tier]);
    },
    delete: (objectId: string) => {
      deleteCalls.push(objectId);
    },
    deleteWithSession: (objectId: string, writeContextJson?: string | null) => {
      deleteWithSessionCalls.push([objectId, writeContextJson ?? undefined]);
    },
    deleteDurable: async (objectId: string, tier: string) => {
      deleteDurableCalls.push([objectId, tier]);
    },
    deleteDurableWithSession: async (
      objectId: string,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      deleteDurableWithSessionCalls.push([objectId, writeContextJson ?? undefined, tier]);
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
  const runtime: Runtime = { ...runtimeBase, ...runtimeOverrides };

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
    insertWithSessionCalls,
    insertDurableWithSessionCalls,
    updateCalls,
    updateWithSessionCalls,
    updateDurableCalls,
    updateDurableWithSessionCalls,
    deleteCalls,
    deleteDurableCalls,
    deleteWithSessionCalls,
    deleteDurableWithSessionCalls,
  };
}

describe("JazzClient mutation durability split", () => {
  it("rethrows synchronous runtime mutation errors", () => {
    const insertError = new Error("Insert failed: indexed value too large");
    const updateError = new Error("Update failed: indexed value too large");
    const { client } = makeClient({
      insert: () => {
        throw insertError;
      },
      update: () => {
        throw updateError;
      },
    });

    expect(() => client.create("todos", {})).toThrow(insertError);
    expect(() =>
      client.update("row-1", { done: { type: "Boolean" as const, value: true } }),
    ).toThrow(updateError);
  });

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

  it("routes attributed writes through session-aware runtime methods", async () => {
    const {
      client,
      insertWithSessionCalls,
      insertDurableWithSessionCalls,
      updateWithSessionCalls,
      updateDurableWithSessionCalls,
      deleteWithSessionCalls,
      deleteDurableWithSessionCalls,
    } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    client.createInternal("todos", insertValues, undefined, "alice");
    await client.createDurableInternal("todos", insertValues, undefined, "alice");
    client.updateInternal("row-1", updates, undefined, "alice");
    await client.updateDurableInternal("row-1", updates, undefined, "alice");
    client.deleteInternal("row-1", undefined, "alice");
    await client.deleteDurableInternal("row-1", undefined, "alice", { tier: "global" });

    expect(insertWithSessionCalls).toEqual([["todos", insertValues, attributedContext]]);
    expect(insertDurableWithSessionCalls).toEqual([
      ["todos", insertValues, attributedContext, "edge"],
    ]);
    expect(updateWithSessionCalls).toEqual([["row-1", updates, attributedContext]]);
    expect(updateDurableWithSessionCalls).toEqual([["row-1", updates, attributedContext, "edge"]]);
    expect(deleteWithSessionCalls).toEqual([["row-1", attributedContext]]);
    expect(deleteDurableWithSessionCalls).toEqual([["row-1", attributedContext, "global"]]);
  });

  it("encodes session and attribution together when both are provided", () => {
    const { client, insertWithSessionCalls } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
    };
    const insertValues = { title: { type: "Text" as const, value: "Attributed" } };

    client.createInternal("todos", insertValues, session, "alice");

    expect(insertWithSessionCalls).toEqual([
      [
        "todos",
        insertValues,
        JSON.stringify({
          session,
          attribution: "alice",
        }),
      ],
    ]);
  });
});
