import { describe, expect, it, vi } from "vitest";
import { JazzClient, type LocalBatchRecord, type Runtime } from "./client.js";
import type { AppContext, Session } from "./context.js";

function makeClient(runtimeOverrides: Partial<Runtime> = {}) {
  const insertCalls: Array<[string, Record<string, unknown>]> = [];
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
    sealed: true,
    latestSettlement: {
      kind: "durableDirect" as const,
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
    insert: (table: string, values: Record<string, unknown>) => {
      insertCalls.push([table, values]);
      return {
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "plain-insert-batch",
      };
    },
    insertWithSession: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      insertWithSessionCalls.push([table, values, writeContextJson ?? undefined]);
      return {
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "plain-insert-session-batch",
      };
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
      return { batchId: localBatchRecord.batchId };
    },
    updateWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined]);
      return { batchId: localBatchRecord.batchId };
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
      return { batchId: localBatchRecord.batchId };
    },
    deleteWithSession: (objectId: string, writeContextJson?: string | null) => {
      deleteWithSessionCalls.push([objectId, writeContextJson ?? undefined]);
      return { batchId: localBatchRecord.batchId };
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
      defaultDurabilityTier: "local" | "edge" | "global",
    ): JazzClient;
  };

  return {
    client: new JazzClientCtor(runtime, context, "edge"),
    insertCalls,
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
    localBatchRecord,
    localBatchRecordCalls,
    localBatchRecordsCalls,
    acknowledgeRejectedBatchCalls,
    sealBatchCalls,
  };
}

describe("JazzClient mutation durability split", () => {
  it("keeps Bytea mutations as Uint8Array at the runtime boundary", () => {
    const { client, insertCalls, updateCalls } = makeClient();
    const payload = new Uint8Array([1, 2, 3]);
    const insertValues = {
      payload: { type: "Bytea" as const, value: payload },
    };
    const updateValues = {
      payload: { type: "Bytea" as const, value: payload },
    };

    client.create("todos", insertValues);
    client.update("row-1", updateValues);

    expect(insertCalls).toHaveLength(1);
    expect(updateCalls).toHaveLength(1);
    expect(insertCalls[0]?.[1]).toBe(insertValues);
    expect(updateCalls[0]?.[1]).toBe(updateValues);

    const insertPayload = insertCalls[0]?.[1].payload as
      | { type: "Bytea"; value: Uint8Array }
      | undefined;
    const updatePayload = updateCalls[0]?.[1].payload as
      | { type: "Bytea"; value: Uint8Array }
      | undefined;

    expect(insertPayload?.type).toBe("Bytea");
    expect(updatePayload?.type).toBe("Bytea");
    expect(insertPayload?.value).toBeInstanceOf(Uint8Array);
    expect(updatePayload?.value).toBeInstanceOf(Uint8Array);
    expect(insertPayload?.value).toBe(payload);
    expect(updatePayload?.value).toBe(payload);
    expect(Array.from(insertPayload?.value ?? [])).toEqual([1, 2, 3]);
    expect(Array.from(updatePayload?.value ?? [])).toEqual([1, 2, 3]);
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
      client.update("row-1", { done: { type: "Boolean" as const, value: true } }),
    ).toThrow(updateError);
  });

  it("routes update/delete through the synchronous runtime methods", () => {
    const { client, updateCalls, deleteCalls, localBatchRecord } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(client.update("row-1", updates)).toEqual({ batchId: localBatchRecord.batchId });
    expect(client.delete("row-1")).toEqual({ batchId: localBatchRecord.batchId });

    expect(updateCalls).toEqual([["row-1", updates]]);
    expect(deleteCalls).toEqual(["row-1"]);
  });

  it("returns direct-write batch ids from plain runtime mutations when available", () => {
    const { client } = makeClient({
      insert: () => ({
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "batch-insert",
      }),
      update: () => ({ batchId: "batch-update" }),
      delete: () => ({ batchId: "batch-delete" }),
    });

    const created = client.createInternal("todos", {});
    const updated = client.updateInternal("row-1", {
      done: { type: "Boolean" as const, value: true },
    });
    const deleted = client.deleteInternal("row-1");

    expect(created).toMatchObject({ batchId: "batch-insert" });
    expect(updated).toEqual({ batchId: "batch-update" });
    expect(deleted).toEqual({ batchId: "batch-delete" });
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

  it("forwards caller-supplied create ids to runtime insert methods", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insert = vi.fn(
      (table: string, values: Record<string, unknown>, objectId?: string | null) => {
        return { id: objectId ?? "generated-id", values: [], batchId: "batch-1" };
      },
    );
    const insertDurable = vi.fn(
      async (
        table: string,
        values: Record<string, unknown>,
        tier: string,
        objectId?: string | null,
      ) => {
        return { id: objectId ?? "generated-id", values: [] };
      },
    );
    const { client } = makeClient({ insert, insertDurable });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    const created = client.create("todos", insertValues, { id: externalId });
    const createdDurable = await client.createDurable("todos", insertValues, { id: externalId });

    expect(insert).toHaveBeenCalledWith("todos", insertValues, externalId);
    expect(insertDurable).toHaveBeenCalledWith("todos", insertValues, "edge", externalId);
    expect(created.id).toBe(externalId);
    expect(createdDurable.id).toBe(externalId);
  });

  it("falls back to update when upsert sees an existing object id", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insert = vi.fn(() => {
      throw insertError;
    });
    const insertDurable = vi.fn(async () => {
      throw insertError;
    });
    const update = vi.fn();
    const updateDurable = vi.fn(async () => {});
    const { client } = makeClient({ insert, insertDurable, update, updateDurable });
    const values = { title: { type: "Text" as const, value: "Updated title" } };

    expect(client.upsert("todos", values, { id: externalId })).toBeUndefined();
    await expect(
      client.upsertDurable("todos", values, { id: externalId }),
    ).resolves.toBeUndefined();

    expect(insert).toHaveBeenCalledWith("todos", values, externalId);
    expect(insertDurable).toHaveBeenCalledWith("todos", values, "edge", externalId);
    expect(update).toHaveBeenCalledWith(externalId, values);
    expect(updateDurable).toHaveBeenCalledWith(externalId, values, "edge");
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

  it("encodes custom updated_at overrides for create and update mutation options", async () => {
    const insertWithSession = vi.fn(
      (
        table: string,
        values: Record<string, unknown>,
        _writeContextJson?: string | null,
        objectId?: string | null,
      ) => ({
        id: objectId ?? "generated-id",
        values: [],
        batchId: "generated-batch-id",
      }),
    );
    const insertDurableWithSession = vi.fn(
      async (
        table: string,
        values: Record<string, unknown>,
        _writeContextJson?: string | null,
        _tier = "edge",
        objectId?: string | null,
      ) => ({
        id: objectId ?? "generated-id",
        values: [],
      }),
    );
    const updateWithSession = vi.fn();
    const updateDurableWithSession = vi.fn(async () => {});
    const { client } = makeClient({
      insertWithSession,
      insertDurableWithSession,
      updateWithSession,
      updateDurableWithSession,
    });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    client.create("todos", insertValues, { updatedAt });
    await client.createDurable("todos", insertValues, {
      id: "todo-1",
      tier: "global",
      updatedAt,
    });
    client.update("row-1", updates, { updatedAt });
    await client.updateDurable("row-1", updates, { tier: "global", updatedAt });

    expect(insertWithSession).toHaveBeenCalledWith("todos", insertValues, updatedAtContext);
    expect(insertDurableWithSession).toHaveBeenCalledWith(
      "todos",
      insertValues,
      updatedAtContext,
      "global",
      "todo-1",
    );
    expect(updateWithSession).toHaveBeenCalledWith("row-1", updates, updatedAtContext);
    expect(updateDurableWithSession).toHaveBeenCalledWith(
      "row-1",
      updates,
      updatedAtContext,
      "global",
    );
  });

  it("preserves custom updated_at overrides when upsert falls back to update", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insertWithSession = vi.fn(() => {
      throw insertError;
    });
    const insertDurableWithSession = vi.fn(async () => {
      throw insertError;
    });
    const updateWithSession = vi.fn();
    const updateDurableWithSession = vi.fn(async () => {});
    const { client } = makeClient({
      insertWithSession,
      insertDurableWithSession,
      updateWithSession,
      updateDurableWithSession,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    expect(client.upsert("todos", values, { id: externalId, updatedAt })).toBeUndefined();
    await expect(
      client.upsertDurable("todos", values, { id: externalId, updatedAt }),
    ).resolves.toBeUndefined();

    expect(insertWithSession).toHaveBeenCalledWith("todos", values, updatedAtContext, externalId);
    expect(insertDurableWithSession).toHaveBeenCalledWith(
      "todos",
      values,
      updatedAtContext,
      "edge",
      externalId,
    );
    expect(updateWithSession).toHaveBeenCalledWith(externalId, values, updatedAtContext);
    expect(updateDurableWithSession).toHaveBeenCalledWith(
      externalId,
      values,
      updatedAtContext,
      "edge",
    );
  });

  it("resolves lower-tier waits without resolving stricter waits for the same batch", async () => {
    let syncMessageCount = 0;
    let currentLocalBatchRecord: LocalBatchRecord | null = {
      batchId: "00000000-0000-0000-0000-000000000041",
      mode: "direct",
      sealed: true,
      latestSettlement: {
        kind: "durableDirect",
        batchId: "00000000-0000-0000-0000-000000000041",
        confirmedTier: "local",
        visibleMembers: [
          {
            objectId: "00000000-0000-0000-0000-000000000001",
            branchName: "main",
            batchId: "00000000-0000-0000-0000-000000000041",
          },
        ],
      },
    };
    const runtimeOnSyncMessageReceived = vi.fn(() => {
      syncMessageCount += 1;
      currentLocalBatchRecord = {
        ...currentLocalBatchRecord!,
        latestSettlement: {
          kind: "durableDirect",
          batchId: currentLocalBatchRecord!.batchId,
          confirmedTier: syncMessageCount === 1 ? "edge" : "global",
          visibleMembers: [
            {
              objectId: "00000000-0000-0000-0000-000000000001",
              branchName: "main",
              batchId: currentLocalBatchRecord!.batchId,
            },
          ],
        },
      };
    });
    const { client } = makeClient({
      loadLocalBatchRecord: (batchId: string) =>
        batchId === currentLocalBatchRecord?.batchId ? currentLocalBatchRecord : null,
      onSyncMessageReceived: runtimeOnSyncMessageReceived,
    });

    const edgeWait = client.waitForPersistedBatch(currentLocalBatchRecord.batchId, "edge");
    const globalWait = client.waitForPersistedBatch(currentLocalBatchRecord.batchId, "global");
    let globalResolved = false;
    void globalWait.then(() => {
      globalResolved = true;
    });

    client.getRuntime().onSyncMessageReceived("{}");

    await expect(edgeWait).resolves.toBeUndefined();
    await Promise.resolve();
    expect(globalResolved).toBe(false);

    client.getRuntime().onSyncMessageReceived("{}");

    await expect(globalWait).resolves.toBeUndefined();
  });

  it("allows a stricter wait to attach after an earlier lower-tier wait resolved", async () => {
    let currentLocalBatchRecord: LocalBatchRecord | null = {
      batchId: "00000000-0000-0000-0000-000000000041",
      mode: "direct",
      sealed: true,
      latestSettlement: {
        kind: "durableDirect",
        batchId: "00000000-0000-0000-0000-000000000041",
        confirmedTier: "edge",
        visibleMembers: [
          {
            objectId: "00000000-0000-0000-0000-000000000001",
            branchName: "main",
            batchId: "00000000-0000-0000-0000-000000000041",
          },
        ],
      },
    };
    const runtimeOnSyncMessageReceived = vi.fn(() => {
      currentLocalBatchRecord = {
        ...currentLocalBatchRecord!,
        latestSettlement: {
          kind: "durableDirect",
          batchId: currentLocalBatchRecord!.batchId,
          confirmedTier: "global",
          visibleMembers: [
            {
              objectId: "00000000-0000-0000-0000-000000000001",
              branchName: "main",
              batchId: currentLocalBatchRecord!.batchId,
            },
          ],
        },
      };
    });
    const { client } = makeClient({
      loadLocalBatchRecord: (batchId: string) =>
        batchId === currentLocalBatchRecord?.batchId ? currentLocalBatchRecord : null,
      onSyncMessageReceived: runtimeOnSyncMessageReceived,
    });

    await expect(
      client.waitForPersistedBatch(currentLocalBatchRecord.batchId, "edge"),
    ).resolves.toBeUndefined();

    const globalWait = client.waitForPersistedBatch(currentLocalBatchRecord.batchId, "global");
    let globalResolved = false;
    void globalWait.then(() => {
      globalResolved = true;
    });

    await Promise.resolve();
    expect(globalResolved).toBe(false);

    client.getRuntime().onSyncMessageReceived("{}");

    await expect(globalWait).resolves.toBeUndefined();
  });
});
