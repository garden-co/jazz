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
  const insertSealedCalls: Array<
    [string, Record<string, unknown>, string | undefined, string | undefined]
  > = [];
  const updateWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const updateCalls: Array<[string, Record<string, unknown>]> = [];
  const updateSealedCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const deleteWithSessionCalls: Array<[string, string | undefined]> = [];
  const deleteCalls: string[] = [];
  const deleteSealedCalls: Array<[string, string | undefined]> = [];

  const runtimeBase: Runtime = {
    loadLocalBatchRecord: () => null,
    loadLocalBatchRecords: () => [],
    createWriteBatchContext: (batchMode) => ({
      batchMode,
      batchId: `${batchMode}-batch-id`,
      targetBranchName: `dev-schema-hash-main`,
    }),
    insert: (table: string, values: Record<string, unknown>) => {
      insertCalls.push([table, values]);
      return {
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "insert-batch-id",
      };
    },
    insertSealed: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
      objectId?: string | null,
    ) => {
      insertSealedCalls.push([table, values, writeContextJson ?? undefined, objectId ?? undefined]);
      return {
        id: objectId ?? "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "insert-sealed-batch-id",
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
    update: (objectId: string, updates: Record<string, unknown>) => {
      updateCalls.push([objectId, updates]);
      return { batchId: "update-batch-id" };
    },
    updateSealed: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateSealedCalls.push([objectId, updates, writeContextJson ?? undefined]);
      return { batchId: "update-sealed-batch-id" };
    },
    updateWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined]);
      return { batchId: "update-with-session-batch-id" };
    },
    delete: (objectId: string) => {
      deleteCalls.push(objectId);
      return { batchId: "delete-batch-id" };
    },
    deleteSealed: (objectId: string, writeContextJson?: string | null) => {
      deleteSealedCalls.push([objectId, writeContextJson ?? undefined]);
      return { batchId: "delete-sealed-batch-id" };
    },
    deleteWithSession: (objectId: string, writeContextJson?: string | null) => {
      deleteWithSessionCalls.push([objectId, writeContextJson ?? undefined]);
      return { batchId: "delete-with-session-batch-id" };
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
    insertSealedCalls,
    updateCalls,
    updateWithSessionCalls,
    updateSealedCalls,
    deleteCalls,
    deleteWithSessionCalls,
    deleteSealedCalls,
  };
}

describe("JazzClient mutation durability split", () => {
  it("keeps Bytea mutations as Uint8Array at the runtime boundary", () => {
    const { client, insertSealedCalls, updateSealedCalls } = makeClient();
    const payload = new Uint8Array([1, 2, 3]);
    const insertValues = {
      payload: { type: "Bytea" as const, value: payload },
    };
    const updateValues = {
      payload: { type: "Bytea" as const, value: payload },
    };

    client.create("todos", insertValues);
    client.update("row-1", updateValues);

    expect(insertSealedCalls).toHaveLength(1);
    expect(updateSealedCalls).toHaveLength(1);
    expect(insertSealedCalls[0]?.[1]).toBe(insertValues);
    expect(updateSealedCalls[0]?.[1]).toBe(updateValues);

    const insertPayload = insertSealedCalls[0]?.[1].payload as
      | { type: "Bytea"; value: Uint8Array }
      | undefined;
    const updatePayload = updateSealedCalls[0]?.[1].payload as
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
      insertSealed: () => {
        throw insertError;
      },
      updateSealed: () => {
        throw updateError;
      },
    });

    expect(() => client.create("todos", {})).toThrow(insertError);
    expect(() =>
      client.update("row-1", { done: { type: "Boolean" as const, value: true } }),
    ).toThrow(updateError);
  });

  it("routes update/delete through sealed runtime methods without TS-side sealing", () => {
    const { client, runtime, updateSealedCalls, deleteSealedCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(client.update("row-1", updates)).toEqual({
      batchId: "update-sealed-batch-id",
    });
    expect(client.delete("row-1")).toEqual({
      batchId: "delete-sealed-batch-id",
    });

    expect(updateSealedCalls).toEqual([["row-1", updates, undefined]]);
    expect(deleteSealedCalls).toEqual([["row-1", undefined]]);
    expect(runtime.sealBatch).not.toHaveBeenCalled();
  });

  it("routes attributed writes through session-aware runtime methods", async () => {
    const { client, insertSealedCalls, updateSealedCalls, deleteSealedCalls } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    client.createHandleInternal("todos", insertValues, undefined, "alice");
    client.updateHandleInternal("row-1", updates, undefined, "alice");
    client.deleteHandleInternal("row-1", undefined, "alice");

    expect(insertSealedCalls).toEqual([["todos", insertValues, attributedContext, undefined]]);
    expect(updateSealedCalls).toEqual([["row-1", updates, attributedContext]]);
    expect(deleteSealedCalls).toEqual([["row-1", attributedContext]]);
  });

  it("forwards caller-supplied create ids to runtime insert methods", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertSealed = vi.fn(
      (
        table: string,
        values: Record<string, unknown>,
        _writeContextJson?: string | null,
        objectId?: string | null,
      ) => {
        return { id: objectId ?? "generated-id", values: [], batchId: "batch-1" };
      },
    );
    const { client, runtime } = makeClient({ insertSealed });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    const created = client.create("todos", insertValues, { id: externalId });

    expect(insertSealed).toHaveBeenCalledWith("todos", insertValues, undefined, externalId);
    expect(created.value.id).toBe(externalId);
    expect(runtime.sealBatch).not.toHaveBeenCalled();
  });

  it("falls back to update when upsert sees an existing object id", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insert = vi.fn(() => {
      throw insertError;
    });
    const update = vi.fn(() => ({ batchId: "fallback-update-batch" }));
    const { client } = makeClient({
      insert,
      update,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };

    expect(client.upsert("todos", values, { id: externalId })).toEqual({
      batchId: "fallback-update-batch",
    });

    expect(insert).toHaveBeenCalledWith("todos", values, externalId);
    expect(update).toHaveBeenCalledWith(externalId, values);
  });

  it("returns the inserted batch id when upsert creates a new row", () => {
    const { client } = makeClient({
      insert: () => ({
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "batch-created-via-upsert",
      }),
    });
    const values = { title: { type: "Text" as const, value: "New todo" } };

    expect(client.upsert("todos", values, { id: "row-1" })).toEqual({
      batchId: "batch-created-via-upsert",
    });
  });

  it("does not fall back to update when upsert insert shape validation fails", () => {
    const validationError = new Error("encoding error: missing required column title");
    const insert = vi.fn(() => {
      throw validationError;
    });
    const update = vi.fn(() => ({ batchId: "should-not-update" }));
    const { client } = makeClient({ insert, update });

    expect(() =>
      client.upsert(
        "todos",
        { done: { type: "Boolean" as const, value: true } },
        { id: "todo-missing-title" },
      ),
    ).toThrow(validationError);

    expect(update).not.toHaveBeenCalled();
  });

  it("encodes session and attribution together when both are provided", () => {
    const { client, insertWithSessionCalls } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
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

  it("encodes custom updated_at overrides for create and update mutation options", async () => {
    const insertSealed = vi.fn(
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
    const updateSealed = vi.fn(() => ({ batchId: "generated-update-batch-id" }));
    const { client } = makeClient({
      insertSealed,
      updateSealed,
    });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    client.create("todos", insertValues, { updatedAt });
    client.update("row-1", updates, { updatedAt });

    expect(insertSealed).toHaveBeenCalledWith("todos", insertValues, updatedAtContext, undefined);
    expect(updateSealed).toHaveBeenCalledWith("row-1", updates, updatedAtContext);
  });

  it("preserves custom updated_at overrides when upsert falls back to update", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insertWithSession = vi.fn(() => {
      throw insertError;
    });
    const updateWithSession = vi.fn(() => ({ batchId: "fallback-update-session-batch" }));
    const { client } = makeClient({
      insertWithSession,
      updateWithSession,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    expect(client.upsert("todos", values, { id: externalId, updatedAt })).toEqual({
      batchId: "fallback-update-session-batch",
    });

    expect(insertWithSession).toHaveBeenCalledWith("todos", values, updatedAtContext, externalId);
    expect(updateWithSession).toHaveBeenCalledWith(externalId, values, updatedAtContext);
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
