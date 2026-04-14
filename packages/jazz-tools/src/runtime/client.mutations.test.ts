import { afterEach, describe, expect, it, vi } from "vitest";
import { JazzClient, type LocalBatchRecord, type Runtime } from "./client.js";
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
  const insertPersistedCalls: Array<[string, Record<string, unknown>, string]> = [];
  const insertPersistedWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string]
  > = [];
  const updatePersistedCalls: Array<[string, Record<string, unknown>, string]> = [];
  const updatePersistedWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string]
  > = [];
  const deletePersistedCalls: Array<[string, string]> = [];
  const deletePersistedWithSessionCalls: Array<[string, string | undefined, string]> = [];
  const localBatchRecordCalls: string[] = [];
  const localBatchRecordsCalls: string[] = [];
  const acknowledgeRejectedBatchCalls: string[] = [];
  const sealBatchCalls: string[] = [];
  const deleteCalls: string[] = [];
  const deleteDurableCalls: Array<[string, string]> = [];
  const deleteDurableWithSessionCalls: Array<[string, string | undefined, string]> = [];

  const localBatchRecord = {
    batchId: "00000000-0000-0000-0000-000000000041",
    mode: "direct" as const,
    requestedTier: "edge" as const,
    sealed: true,
    latestSettlement: {
      kind: "durable_direct" as const,
      batchId: "00000000-0000-0000-0000-000000000041",
      confirmedTier: "edge" as const,
      visibleMembers: [
        {
          objectId: "00000000-0000-0000-0000-000000000001",
          branchName: "main",
          batchId: "00000000-0000-0000-0000-000000000041",
        },
      ],
    },
  };

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
    insertDurable: async () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
    }),
    insertDurableWithSession: async (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      insertDurableWithSessionCalls.push([table, values, writeContextJson ?? undefined, tier]);
      return { id: "00000000-0000-0000-0000-000000000001", values: [] };
    },
    insertPersisted: (table: string, values: Record<string, unknown>, tier: string) => {
      insertPersistedCalls.push([table, values, tier]);
      return {
        batchId: localBatchRecord.batchId,
        row: { id: "00000000-0000-0000-0000-000000000001", values: [] },
      };
    },
    insertPersistedWithSession: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      insertPersistedWithSessionCalls.push([table, values, writeContextJson ?? undefined, tier]);
      return {
        batchId: localBatchRecord.batchId,
        row: { id: "00000000-0000-0000-0000-000000000001", values: [] },
      };
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
    updatePersisted: (objectId: string, updates: Record<string, unknown>, tier: string) => {
      updatePersistedCalls.push([objectId, updates, tier]);
      return { batchId: localBatchRecord.batchId };
    },
    updatePersistedWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      updatePersistedWithSessionCalls.push([
        objectId,
        updates,
        writeContextJson ?? undefined,
        tier,
      ]);
      return { batchId: localBatchRecord.batchId };
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
    deletePersisted: (objectId: string, tier: string) => {
      deletePersistedCalls.push([objectId, tier]);
      return { batchId: localBatchRecord.batchId };
    },
    deletePersistedWithSession: (
      objectId: string,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      deletePersistedWithSessionCalls.push([objectId, writeContextJson ?? undefined, tier]);
      return { batchId: localBatchRecord.batchId };
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
    loadLocalBatchRecord: (batchId: string) => {
      localBatchRecordCalls.push(batchId);
      return batchId === localBatchRecord.batchId ? localBatchRecord : null;
    },
    loadLocalBatchRecords: () => {
      localBatchRecordsCalls.push("scan");
      return [localBatchRecord];
    },
    acknowledgeRejectedBatch: (batchId: string) => {
      acknowledgeRejectedBatchCalls.push(batchId);
      return batchId === localBatchRecord.batchId;
    },
    sealBatch: (batchId: string) => {
      sealBatchCalls.push(batchId);
    },
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
    insertPersistedCalls,
    insertPersistedWithSessionCalls,
    updateCalls,
    updateWithSessionCalls,
    updateDurableCalls,
    updateDurableWithSessionCalls,
    updatePersistedCalls,
    updatePersistedWithSessionCalls,
    deleteCalls,
    deleteDurableCalls,
    deleteWithSessionCalls,
    deleteDurableWithSessionCalls,
    deletePersistedCalls,
    deletePersistedWithSessionCalls,
    localBatchRecordCalls,
    localBatchRecordsCalls,
    acknowledgeRejectedBatchCalls,
    sealBatchCalls,
    localBatchRecord,
  };
}

describe("JazzClient mutation durability split", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

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
      client.update("row-1", {
        done: { type: "Boolean" as const, value: true },
      }),
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
    await client.deleteDurableInternal("row-1", undefined, "alice", {
      tier: "global",
    });

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
    const insertValues = {
      title: { type: "Text" as const, value: "Attributed" },
    };

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

  it("reuses one transactional batch id across create, update, and delete", () => {
    const { client, insertWithSessionCalls, updateWithSessionCalls, deleteWithSessionCalls } =
      makeClient();
    const transaction = client.beginTransaction();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(typeof transaction.batchId()).toBe("string");

    transaction.create("todos", insertValues);
    transaction.update("row-1", updates);
    transaction.delete("row-1");

    const insertContext = JSON.parse(insertWithSessionCalls[0]![2]!);
    const updateContext = JSON.parse(updateWithSessionCalls[0]![2]!);
    const deleteContext = JSON.parse(deleteWithSessionCalls[0]![1]!);

    expect(insertContext).toMatchObject({
      batch_mode: "transactional",
      batch_id: transaction.batchId(),
    });
    expect(updateContext).toMatchObject({
      batch_mode: "transactional",
      batch_id: transaction.batchId(),
    });
    expect(deleteContext).toMatchObject({
      batch_mode: "transactional",
      batch_id: transaction.batchId(),
    });
  });

  it("commits a transactional batch by sealing its batch id", () => {
    const { client, sealBatchCalls } = makeClient();
    const transaction = client.beginTransaction();

    expect(transaction.commit()).toBe(transaction.batchId());
    expect(sealBatchCalls).toEqual([transaction.batchId()]);
  });

  it("rejects transactional writes after commit", () => {
    const { client, insertWithSessionCalls } = makeClient();
    const transaction = client.beginTransaction();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    transaction.commit();

    expect(() => transaction.create("todos", insertValues)).toThrow(/committed/i);
    expect(insertWithSessionCalls).toEqual([]);
  });

  it("reuses one direct batch id across create, update, and delete without sealing", () => {
    const {
      client,
      insertWithSessionCalls,
      updateWithSessionCalls,
      deleteWithSessionCalls,
      sealBatchCalls,
    } = makeClient();
    const batch = client.beginDirectBatch();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(typeof batch.batchId()).toBe("string");

    batch.create("todos", insertValues);
    batch.update("row-1", updates);
    batch.delete("row-1");

    const insertContext = JSON.parse(insertWithSessionCalls[0]![2]!);
    const updateContext = JSON.parse(updateWithSessionCalls[0]![2]!);
    const deleteContext = JSON.parse(deleteWithSessionCalls[0]![1]!);

    expect(insertContext).toMatchObject({
      batch_mode: "direct",
      batch_id: batch.batchId(),
    });
    expect(updateContext).toMatchObject({
      batch_mode: "direct",
      batch_id: batch.batchId(),
    });
    expect(deleteContext).toMatchObject({
      batch_mode: "direct",
      batch_id: batch.batchId(),
    });
    expect(sealBatchCalls).toEqual([]);
  });

  it("returns persisted writes with an immediate batch id and local wait handle", async () => {
    const { client, insertPersistedCalls, localBatchRecordCalls, localBatchRecord } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    const persisted = client.createPersisted("todos", insertValues);

    expect(insertPersistedCalls).toEqual([["todos", insertValues, "edge"]]);
    expect(persisted.batchId()).toBe(localBatchRecord.batchId);
    expect(persisted.value()).toEqual({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
    });
    await expect(persisted.wait()).resolves.toEqual({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
    });
    expect(localBatchRecordCalls).toContain(localBatchRecord.batchId);
  });

  it("routes persisted transactional writes through session-aware runtime methods", () => {
    const {
      client,
      insertPersistedWithSessionCalls,
      updatePersistedWithSessionCalls,
      deletePersistedWithSessionCalls,
    } = makeClient();
    const transaction = client.beginTransaction();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };

    transaction.createPersisted("todos", insertValues, { tier: "global" });
    transaction.updatePersisted("row-1", updates, { tier: "global" });
    transaction.deletePersisted("row-1", { tier: "global" });

    const insertContext = JSON.parse(insertPersistedWithSessionCalls[0]![2]!);
    const updateContext = JSON.parse(updatePersistedWithSessionCalls[0]![2]!);
    const deleteContext = JSON.parse(deletePersistedWithSessionCalls[0]![1]!);

    expect(insertPersistedWithSessionCalls[0]![3]).toBe("global");
    expect(updatePersistedWithSessionCalls[0]![3]).toBe("global");
    expect(deletePersistedWithSessionCalls[0]![2]).toBe("global");
    expect(insertContext.batch_id).toBe(transaction.batchId());
    expect(updateContext.batch_id).toBe(transaction.batchId());
    expect(deleteContext.batch_id).toBe(transaction.batchId());
  });

  it("binds a transaction to the target composed prefix at begin time", () => {
    let currentSchemaHash = "1111111111111111111111111111111111111111111111111111111111111111";
    const { client, insertWithSessionCalls, updateWithSessionCalls } = makeClient({
      getSchemaHash: () => currentSchemaHash,
    });
    const transaction = client.beginTransaction();

    currentSchemaHash = "2222222222222222222222222222222222222222222222222222222222222222";

    transaction.create("todos", {
      title: { type: "Text" as const, value: "Bound prefix" },
    });
    transaction.update("row-1", {
      done: { type: "Boolean" as const, value: true },
    });

    const insertContext = JSON.parse(insertWithSessionCalls[0]![2]!);
    const updateContext = JSON.parse(updateWithSessionCalls[0]![2]!);

    expect(insertContext).toMatchObject({
      batch_mode: "transactional",
      batch_id: transaction.batchId(),
      target_branch_name: "dev-111111111111-main",
    });
    expect(updateContext).toMatchObject({
      batch_mode: "transactional",
      batch_id: transaction.batchId(),
      target_branch_name: "dev-111111111111-main",
    });
  });

  it("delegates local batch record inspection and rejection acknowledgement", () => {
    const {
      client,
      localBatchRecordCalls,
      localBatchRecordsCalls,
      acknowledgeRejectedBatchCalls,
      localBatchRecord,
    } = makeClient();

    expect(client.localBatchRecord(localBatchRecord.batchId)).toEqual(localBatchRecord);
    expect(client.localBatchRecords()).toEqual([localBatchRecord]);
    expect(client.acknowledgeRejectedBatch(localBatchRecord.batchId)).toBe(true);

    expect(localBatchRecordCalls).toEqual([localBatchRecord.batchId, localBatchRecord.batchId]);
    expect(localBatchRecordsCalls).toEqual(["scan"]);
    expect(acknowledgeRejectedBatchCalls).toEqual([localBatchRecord.batchId]);
  });

  it("rejects persisted waits on rejected settlements without polling storage", async () => {
    let currentLocalBatchRecord: LocalBatchRecord | null = {
      batchId: "00000000-0000-0000-0000-000000000041",
      mode: "transactional",
      requestedTier: "edge",
      sealed: true,
      latestSettlement: null,
    };
    const runtimeOnSyncMessageReceived = vi.fn(() => {
      currentLocalBatchRecord = {
        ...currentLocalBatchRecord!,
        latestSettlement: {
          kind: "rejected",
          batchId: currentLocalBatchRecord!.batchId,
          code: "permission_denied",
          reason: "writer lacks publish rights",
        },
      };
    });
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
    const { client } = makeClient({
      loadLocalBatchRecord: (batchId: string) =>
        batchId === currentLocalBatchRecord?.batchId ? currentLocalBatchRecord : null,
      onSyncMessageReceived: runtimeOnSyncMessageReceived,
    });

    const persisted = client.createPersisted("todos", {
      title: { type: "Text" as const, value: "Draft" },
    });
    const waitPromise = persisted.wait();

    await Promise.resolve();
    expect(setTimeoutSpy).not.toHaveBeenCalled();

    client.getRuntime().onSyncMessageReceived("{}");

    await expect(waitPromise).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      batchId: currentLocalBatchRecord!.batchId,
      code: "permission_denied",
      reason: "writer lacks publish rights",
    });
    expect(runtimeOnSyncMessageReceived).toHaveBeenCalledWith("{}", undefined);
    expect(setTimeoutSpy).not.toHaveBeenCalled();
  });
});
