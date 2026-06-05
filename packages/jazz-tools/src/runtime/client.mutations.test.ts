import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { AppContext, Session } from "./context.js";

function makeClient(runtimeOverrides: Partial<Runtime> = {}) {
  const insertCalls: Array<
    [string, Record<string, unknown>, string | undefined, string | undefined]
  > = [];
  const restoreCalls: Array<[string, string, Record<string, unknown>, string | undefined]> = [];
  const updateCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const deleteCalls: Array<[string, string | undefined]> = [];

  const runtimeBase: Runtime = {
    insert: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
      objectId?: string | null,
    ) => {
      insertCalls.push([table, values, writeContextJson ?? undefined, objectId ?? undefined]);
      return {
        id: objectId ?? "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: writeContextJson ? "insert-with-context-batch-id" : "insert-batch-id",
      };
    },
    restore: (
      table: string,
      objectId: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      restoreCalls.push([table, objectId, values, writeContextJson ?? undefined]);
      return {
        id: objectId,
        values: [],
        batchId: writeContextJson ? "restore-with-context-batch-id" : "restore-batch-id",
      };
    },
    update: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateCalls.push([objectId, updates, writeContextJson ?? undefined]);
      return { batchId: writeContextJson ? "update-with-context-batch-id" : "update-batch-id" };
    },
    delete: (objectId: string, writeContextJson?: string | null) => {
      deleteCalls.push([objectId, writeContextJson ?? undefined]);
      return { batchId: writeContextJson ? "delete-with-context-batch-id" : "delete-batch-id" };
    },
    query: async () => [],
    waitForBatch: async () => {},
    onMutationError: () => {},
    subscribe: () => 0,
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    sealBatch: vi.fn(),
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
    restoreCalls,
    updateCalls,
    deleteCalls,
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
    const { client, runtime, updateCalls, deleteCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(client.update("row-1", updates)).toEqual({
      batchId: "update-batch-id",
    });
    expect(client.delete("row-1")).toEqual({
      batchId: "delete-batch-id",
    });

    expect(updateCalls).toEqual([["row-1", updates, undefined]]);
    expect(deleteCalls).toEqual([["row-1", undefined]]);
  });

  it("routes attributed writes through runtime methods with write context", async () => {
    const { client, insertCalls, updateCalls, deleteCalls } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    client.createInternal("todos", insertValues, undefined, "alice");
    client.updateInternal("row-1", updates, undefined, "alice");
    client.deleteInternal("row-1", undefined, "alice");

    expect(insertCalls).toEqual([["todos", insertValues, attributedContext, undefined]]);
    expect(updateCalls).toEqual([["row-1", updates, attributedContext]]);
    expect(deleteCalls).toEqual([["row-1", attributedContext]]);
  });

  it("forwards caller-supplied create ids to runtime insert methods", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insert = vi.fn(
      (
        table: string,
        values: Record<string, unknown>,
        _writeContextJson?: string | null,
        objectId?: string | null,
      ) => {
        return { id: objectId ?? "generated-id", values: [], batchId: "batch-1" };
      },
    );
    const { client } = makeClient({ insert });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    const created = client.create("todos", insertValues, { id: externalId });

    expect(insert).toHaveBeenCalledWith("todos", insertValues, undefined, externalId);
    expect(created.value.id).toBe(externalId);
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

    expect(insert).toHaveBeenCalledWith("todos", values, undefined, externalId);
    expect(update).toHaveBeenCalledWith(externalId, values, undefined);
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
    const { client, insertCalls } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
    };
    const insertValues = { title: { type: "Text" as const, value: "Attributed" } };

    client.createInternal("todos", insertValues, session, "alice");

    expect(insertCalls).toEqual([
      [
        "todos",
        insertValues,
        JSON.stringify({
          session,
          attribution: "alice",
        }),
        undefined,
      ],
    ]);
  });

  it("encodes custom updated_at overrides for create and update mutation options", async () => {
    const insert = vi.fn(
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
    const update = vi.fn(() => ({ batchId: "generated-update-batch-id" }));
    const { client } = makeClient({
      insert,
      update,
    });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    client.create("todos", insertValues, { updatedAt });
    client.update("row-1", updates, { updatedAt });

    expect(insert).toHaveBeenCalledWith("todos", insertValues, updatedAtContext, undefined);
    expect(update).toHaveBeenCalledWith("row-1", updates, updatedAtContext);
  });

  it("preserves custom updated_at overrides when upsert falls back to update", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insert = vi.fn(() => {
      throw insertError;
    });
    const update = vi.fn(() => ({ batchId: "fallback-update-session-batch" }));
    const { client } = makeClient({
      insert,
      update,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    expect(client.upsert("todos", values, { id: externalId, updatedAt })).toEqual({
      batchId: "fallback-update-session-batch",
    });

    expect(insert).toHaveBeenCalledWith("todos", values, updatedAtContext, externalId);
    expect(update).toHaveBeenCalledWith(externalId, values, updatedAtContext);
  });

  it("uses the same conflict-only upsert fallback in transactions and direct batches", () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insert = vi.fn(() => {
      throw insertError;
    });
    const update = vi.fn(
      (
        _objectId: string,
        _updates: Record<string, unknown>,
        _writeContextJson?: string | null,
      ) => ({
        batchId: "fallback-update-batch",
      }),
    );
    const { client } = makeClient({ insert, update });

    client.beginTransaction().upsert("todos", values, { id: externalId });
    client.beginBatch().upsert("todos", values, { id: externalId });

    expect(update).toHaveBeenCalledTimes(2);
    expect(update.mock.calls[0]?.[0]).toBe(externalId);
    expect(update.mock.calls[0]?.[1]).toBe(values);
    expect(JSON.parse(update.mock.calls[0]?.[2] ?? "{}")).toMatchObject({
      batch_mode: "transactional",
    });
    expect(update.mock.calls[1]?.[0]).toBe(externalId);
    expect(update.mock.calls[1]?.[1]).toBe(values);
    expect(JSON.parse(update.mock.calls[1]?.[2] ?? "{}")).toMatchObject({
      batch_mode: "direct",
    });
  });

  it("does not fall back to update in transactions or direct batches when insert validation fails", () => {
    const validationError = new Error("encoding error: missing required column title");
    const insert = vi.fn(() => {
      throw validationError;
    });
    const update = vi.fn(() => ({ batchId: "should-not-update" }));
    const { client } = makeClient({ insert, update });
    const values = { done: { type: "Boolean" as const, value: true } };

    expect(() =>
      client.beginTransaction().upsert("todos", values, { id: "todo-missing-title" }),
    ).toThrow(validationError);
    expect(() => client.beginBatch().upsert("todos", values, { id: "todo-missing-title" })).toThrow(
      validationError,
    );

    expect(update).not.toHaveBeenCalled();
  });
});
