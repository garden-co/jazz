import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { AppContext, Session } from "./context.js";

function response(ok: boolean, statusText: string, body: unknown = {}): Response {
  return {
    ok,
    statusText,
    json: async () => body,
  } as Response;
}

function makeClient(runtimeOverrides: Partial<Runtime> = {}) {
  const insertCalls: Array<[string, Record<string, unknown>]> = [];
  const insertWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const insertPersistedCalls: Array<[string, Record<string, unknown>, string, string | undefined]> =
    [];
  const insertPersistedWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string, string | undefined]
  > = [];
  const updateWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const updatePersistedCalls: Array<[string, Record<string, unknown>, string]> = [];
  const updatePersistedWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string]
  > = [];
  const updateCalls: Array<[string, Record<string, unknown>]> = [];
  const deleteWithSessionCalls: Array<[string, string | undefined]> = [];
  const deletePersistedCalls: Array<[string, string]> = [];
  const deletePersistedWithSessionCalls: Array<[string, string | undefined, string]> = [];
  const deleteCalls: string[] = [];

  const runtimeBase: Runtime = {
    loadLocalBatchRecord: () => null,
    loadLocalBatchRecords: () => [],
    insert: (table: string, values: Record<string, unknown>) => {
      insertCalls.push([table, values]);
      return {
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "insert-batch-id",
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
        batchId: "insert-with-session-batch-id",
      };
    },
    insertPersisted: (
      table: string,
      values: Record<string, unknown>,
      tier: string,
      objectId?: string | null,
    ) => {
      insertPersistedCalls.push([table, values, tier, objectId ?? undefined]);
      return {
        batchId: "insert-persisted-batch-id",
        row: {
          id: objectId ?? "00000000-0000-0000-0000-000000000001",
          values: [],
        },
      };
    },
    insertPersistedWithSession: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson: string | null | undefined,
      tier: string,
      objectId?: string | null,
    ) => {
      insertPersistedWithSessionCalls.push([
        table,
        values,
        writeContextJson ?? undefined,
        tier,
        objectId ?? undefined,
      ]);
      return {
        batchId: "insert-persisted-with-session-batch-id",
        row: {
          id: objectId ?? "00000000-0000-0000-0000-000000000001",
          values: [],
        },
      };
    },
    update: (objectId: string, updates: Record<string, unknown>) => {
      updateCalls.push([objectId, updates]);
      return { batchId: "update-batch-id" };
    },
    updateWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined]);
      return { batchId: "update-with-session-batch-id" };
    },
    updatePersisted: (objectId: string, updates: Record<string, unknown>, tier: string) => {
      updatePersistedCalls.push([objectId, updates, tier]);
      return { batchId: "update-persisted-batch-id" };
    },
    updatePersistedWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson: string | null | undefined,
      tier: string,
    ) => {
      updatePersistedWithSessionCalls.push([
        objectId,
        updates,
        writeContextJson ?? undefined,
        tier,
      ]);
      return { batchId: "update-persisted-with-session-batch-id" };
    },
    delete: (objectId: string) => {
      deleteCalls.push(objectId);
      return { batchId: "delete-batch-id" };
    },
    deleteWithSession: (objectId: string, writeContextJson?: string | null) => {
      deleteWithSessionCalls.push([objectId, writeContextJson ?? undefined]);
      return { batchId: "delete-with-session-batch-id" };
    },
    deletePersisted: (objectId: string, tier: string) => {
      deletePersistedCalls.push([objectId, tier]);
      return { batchId: "delete-persisted-batch-id" };
    },
    deletePersistedWithSession: (
      objectId: string,
      writeContextJson: string | null | undefined,
      tier: string,
    ) => {
      deletePersistedWithSessionCalls.push([objectId, writeContextJson ?? undefined, tier]);
      return { batchId: "delete-persisted-with-session-batch-id" };
    },
    query: async () => [],
    subscribe: () => 0,
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    sealBatch: vi.fn(),
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
      defaultDurabilityTier: "local" | "edge" | "global",
    ): JazzClient;
  };

  return {
    client: new JazzClientCtor(runtime, context, "edge"),
    runtime,
    insertCalls,
    insertWithSessionCalls,
    insertPersistedCalls,
    insertPersistedWithSessionCalls,
    updateCalls,
    updateWithSessionCalls,
    updatePersistedCalls,
    updatePersistedWithSessionCalls,
    deleteCalls,
    deleteWithSessionCalls,
    deletePersistedCalls,
    deletePersistedWithSessionCalls,
  };
}

describe("JazzClient mutation durability split", () => {
  it("keeps Bytea mutations as Uint8Array at the native sealed write boundary", () => {
    const { client, insertPersistedCalls, updatePersistedCalls } = makeClient();
    const payload = new Uint8Array([1, 2, 3]);
    const insertValues = {
      payload: { type: "Bytea" as const, value: payload },
    };
    const updateValues = {
      payload: { type: "Bytea" as const, value: payload },
    };

    client.create("todos", insertValues);
    client.update("row-1", updateValues);

    expect(insertPersistedCalls).toHaveLength(1);
    expect(updatePersistedCalls).toHaveLength(1);
    expect(insertPersistedCalls[0]).toEqual(["todos", insertValues, "local", undefined]);
    expect(updatePersistedCalls[0]).toEqual(["row-1", updateValues, "local"]);

    const insertPayload = insertPersistedCalls[0]?.[1].payload as
      | { type: "Bytea"; value: Uint8Array }
      | undefined;
    const updatePayload = updatePersistedCalls[0]?.[1].payload as
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
      insertPersisted: () => {
        throw insertError;
      },
      updatePersisted: () => {
        throw updateError;
      },
    });

    expect(() => client.create("todos", {})).toThrow(insertError);
    expect(() =>
      client.update("row-1", { done: { type: "Boolean" as const, value: true } }),
    ).toThrow(updateError);
  });

  it("routes standalone update/delete through native sealed runtime methods", () => {
    const { client, runtime, updatePersistedCalls, deletePersistedCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(client.update("row-1", updates)).toEqual({
      batchId: "update-persisted-batch-id",
    });
    expect(client.delete("row-1")).toEqual({
      batchId: "delete-persisted-batch-id",
    });

    expect(updatePersistedCalls).toEqual([["row-1", updates, "local"]]);
    expect(deletePersistedCalls).toEqual([["row-1", "local"]]);
    expect(runtime.sealBatch).not.toHaveBeenCalled();
  });

  it("routes attributed standalone writes through native sealed session-aware methods", async () => {
    const {
      client,
      insertPersistedWithSessionCalls,
      updatePersistedWithSessionCalls,
      deletePersistedWithSessionCalls,
      runtime,
    } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    client.createInternal("todos", insertValues, undefined, "alice");
    client.updateInternal("row-1", updates, undefined, "alice");
    client.deleteInternal("row-1", undefined, "alice");

    expect(insertPersistedWithSessionCalls).toEqual([
      ["todos", insertValues, attributedContext, "local", undefined],
    ]);
    expect(updatePersistedWithSessionCalls).toEqual([
      ["row-1", updates, attributedContext, "local"],
    ]);
    expect(deletePersistedWithSessionCalls).toEqual([["row-1", attributedContext, "local"]]);
    expect(runtime.sealBatch).not.toHaveBeenCalled();
  });

  it("forwards caller-supplied create ids to native sealed insert methods", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertPersisted = vi.fn(
      (table: string, values: Record<string, unknown>, tier: string, objectId?: string | null) => {
        return {
          batchId: "batch-1",
          row: { id: objectId ?? "generated-id", values: [] },
        };
      },
    );
    const { client, runtime } = makeClient({ insertPersisted });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    const created = client.create("todos", insertValues, { id: externalId });

    expect(insertPersisted).toHaveBeenCalledWith("todos", insertValues, "local", externalId);
    expect(created.value.id).toBe(externalId);
    expect(runtime.sealBatch).not.toHaveBeenCalled();
  });

  it("falls back to native sealed update when upsert sees an existing object id", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insertPersisted = vi.fn(() => {
      throw insertError;
    });
    const updatePersisted = vi.fn(() => ({ batchId: "fallback-update-batch" }));
    const { client } = makeClient({
      insertPersisted,
      updatePersisted,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };

    expect(client.upsert("todos", values, { id: externalId })).toEqual({
      batchId: "fallback-update-batch",
    });

    expect(insertPersisted).toHaveBeenCalledWith("todos", values, "local", externalId);
    expect(updatePersisted).toHaveBeenCalledWith(externalId, values, "local");
  });

  it("returns the inserted batch id when upsert creates a new row", () => {
    const { client } = makeClient({
      insertPersisted: () => ({
        batchId: "batch-created-via-upsert",
        row: {
          id: "00000000-0000-0000-0000-000000000001",
          values: [],
        },
      }),
    });
    const values = { title: { type: "Text" as const, value: "New todo" } };

    expect(client.upsert("todos", values, { id: "row-1" })).toEqual({
      batchId: "batch-created-via-upsert",
    });
  });

  it("does not fall back to update when upsert insert shape validation fails", () => {
    const validationError = new Error("encoding error: missing required column title");
    const insertPersisted = vi.fn(() => {
      throw validationError;
    });
    const updatePersisted = vi.fn(() => ({ batchId: "should-not-update" }));
    const { client } = makeClient({ insertPersisted, updatePersisted });

    expect(() =>
      client.upsert(
        "todos",
        { done: { type: "Boolean" as const, value: true } },
        { id: "todo-missing-title" },
      ),
    ).toThrow(validationError);

    expect(updatePersisted).not.toHaveBeenCalled();
  });

  it("encodes session and attribution together when both are provided", () => {
    const { client, insertPersistedWithSessionCalls } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
    };
    const insertValues = { title: { type: "Text" as const, value: "Attributed" } };

    client.createInternal("todos", insertValues, session, "alice");

    expect(insertPersistedWithSessionCalls).toEqual([
      [
        "todos",
        insertValues,
        JSON.stringify({
          session,
          attribution: "alice",
        }),
        "local",
        undefined,
      ],
    ]);
  });

  it("encodes custom updated_at overrides for create and update mutation options", async () => {
    const insertPersistedWithSession = vi.fn(
      (
        table: string,
        values: Record<string, unknown>,
        _writeContextJson?: string | null,
        tier?: string,
        objectId?: string | null,
      ) => ({
        batchId: "generated-batch-id",
        row: {
          id: objectId ?? "generated-id",
          values: [],
        },
      }),
    );
    const updatePersistedWithSession = vi.fn(() => ({
      batchId: "generated-update-batch-id",
    }));
    const { client } = makeClient({
      insertPersistedWithSession,
      updatePersistedWithSession,
    });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    client.create("todos", insertValues, { updatedAt });
    client.update("row-1", updates, { updatedAt });

    expect(insertPersistedWithSession).toHaveBeenCalledWith(
      "todos",
      insertValues,
      updatedAtContext,
      "local",
      undefined,
    );
    expect(updatePersistedWithSession).toHaveBeenCalledWith(
      "row-1",
      updates,
      updatedAtContext,
      "local",
    );
  });

  it("preserves custom updated_at overrides when upsert falls back to update", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insertPersistedWithSession = vi.fn(() => {
      throw insertError;
    });
    const updatePersistedWithSession = vi.fn(() => ({
      batchId: "fallback-update-session-batch",
    }));
    const { client } = makeClient({
      insertPersistedWithSession,
      updatePersistedWithSession,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    expect(client.upsert("todos", values, { id: externalId, updatedAt })).toEqual({
      batchId: "fallback-update-session-batch",
    });

    expect(insertPersistedWithSession).toHaveBeenCalledWith(
      "todos",
      values,
      updatedAtContext,
      "local",
      externalId,
    );
    expect(updatePersistedWithSession).toHaveBeenCalledWith(
      externalId,
      values,
      updatedAtContext,
      "local",
    );
  });

  it("uses the same conflict-only upsert fallback in transactions and direct batches", () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insertWithSession = vi.fn(() => {
      throw insertError;
    });
    const updateWithSession = vi.fn(
      (
        _objectId: string,
        _updates: Record<string, unknown>,
        _writeContextJson?: string | null,
      ) => ({
        batchId: "fallback-update-batch",
      }),
    );
    const { client } = makeClient({ insertWithSession, updateWithSession });

    client.beginTransactionInternal().upsert("todos", values, { id: externalId });
    client.beginBatchInternal().upsert("todos", values, { id: externalId });

    expect(updateWithSession).toHaveBeenCalledTimes(2);
    expect(updateWithSession.mock.calls[0]?.[0]).toBe(externalId);
    expect(updateWithSession.mock.calls[0]?.[1]).toBe(values);
    expect(JSON.parse(updateWithSession.mock.calls[0]?.[2] ?? "{}")).toMatchObject({
      batch_mode: "transactional",
    });
    expect(updateWithSession.mock.calls[1]?.[0]).toBe(externalId);
    expect(updateWithSession.mock.calls[1]?.[1]).toBe(values);
    expect(JSON.parse(updateWithSession.mock.calls[1]?.[2] ?? "{}")).toMatchObject({
      batch_mode: "direct",
    });
  });

  it("does not fall back to update in transactions or direct batches when insert validation fails", () => {
    const validationError = new Error("encoding error: missing required column title");
    const insertWithSession = vi.fn(() => {
      throw validationError;
    });
    const updateWithSession = vi.fn(() => ({ batchId: "should-not-update" }));
    const { client } = makeClient({ insertWithSession, updateWithSession });
    const values = { done: { type: "Boolean" as const, value: true } };

    expect(() =>
      client.beginTransactionInternal().upsert("todos", values, { id: "todo-missing-title" }),
    ).toThrow(validationError);
    expect(() =>
      client.beginBatchInternal().upsert("todos", values, { id: "todo-missing-title" }),
    ).toThrow(validationError);

    expect(updateWithSession).not.toHaveBeenCalled();
  });

  it("uses create then update-on-conflict for SessionClient upsert", async () => {
    const { client } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
    };
    const values = { title: { type: "Text" as const, value: "Backend todo" } };
    const sendRequest = vi
      .spyOn(client, "sendRequest")
      .mockResolvedValueOnce(response(true, "Created", { object_id: "todo-session" }))
      .mockResolvedValueOnce(response(false, "Conflict"))
      .mockResolvedValueOnce(response(true, "OK"));
    const scoped = client.forSession(session);

    await expect(scoped.upsert("todos", values, { id: "todo-session-create" })).resolves.toBe(
      undefined,
    );
    await expect(scoped.upsert("todos", values, { id: "todo-session-existing" })).resolves.toBe(
      undefined,
    );

    expect(sendRequest.mock.calls.map((call) => call[1])).toEqual(["POST", "POST", "PUT"]);
    expect(sendRequest.mock.calls[0]?.[2]).toMatchObject({
      table: "todos",
      values,
      object_id: "todo-session-create",
    });
    expect(sendRequest.mock.calls[2]?.[2]).toMatchObject({
      object_id: "todo-session-existing",
      updates: Object.entries(values),
    });
  });

  it("does not fall back to SessionClient update when insert shape validation fails", async () => {
    const { client } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
    };
    const sendRequest = vi
      .spyOn(client, "sendRequest")
      .mockResolvedValueOnce(response(false, "Bad Request"));

    await expect(
      client
        .forSession(session)
        .upsert(
          "todos",
          { done: { type: "Boolean" as const, value: true } },
          { id: "todo-missing-title" },
        ),
    ).rejects.toThrow("Create failed: Bad Request");

    expect(sendRequest).toHaveBeenCalledTimes(1);
    expect(sendRequest.mock.calls[0]?.[1]).toBe("POST");
  });
});
