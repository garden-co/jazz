import { describe, expect, it, vi } from "vitest";
import {
  JazzClient,
  type BatchId,
  type OpenBatchId,
  type Runtime,
  type TransactionalRuntime,
  type WriteReceipt,
} from "./client.js";
import type { AppContext, Session } from "./context.js";

function makeClient(runtimeOverrides: Partial<TransactionalRuntime> = {}) {
  const receipt = (
    writeContextJson: string | null | undefined,
    committedId: string,
  ): WriteReceipt => {
    const openBatchId = writeContextJson
      ? (JSON.parse(writeContextJson).batch_id as OpenBatchId | undefined)
      : undefined;
    return openBatchId
      ? { kind: "staged", openBatchId }
      : { kind: "committed", batchId: committedId as BatchId };
  };
  const insertCalls: Array<
    [string, Record<string, unknown>, string | undefined, string | undefined]
  > = [];
  const restoreCalls: Array<[string, string, Record<string, unknown>, string | undefined]> = [];
  const updateCalls: Array<[string, string, Record<string, unknown>, string | undefined]> = [];
  const upsertCalls: Array<[string, string, Record<string, unknown>, string | undefined]> = [];
  const deleteCalls: Array<[string, string, string | undefined]> = [];
  const dryRunCalls: Array<[string, ...unknown[]]> = [];

  const runtimeBase: TransactionalRuntime = {
    beginTransaction: (_mode, id) => id,
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
        ...receipt(writeContextJson, "insert-transaction-id"),
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
        ...receipt(writeContextJson, "restore-transaction-id"),
      };
    },
    update: (
      table: string,
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateCalls.push([table, objectId, updates, writeContextJson ?? undefined]);
      return receipt(writeContextJson, "update-transaction-id");
    },
    upsert: (
      table: string,
      objectId: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      upsertCalls.push([table, objectId, values, writeContextJson ?? undefined]);
      return receipt(writeContextJson, "upsert-transaction-id");
    },
    delete: (table: string, objectId: string, writeContextJson?: string | null) => {
      deleteCalls.push([table, objectId, writeContextJson ?? undefined]);
      return receipt(writeContextJson, "delete-transaction-id");
    },
    canInsertLocally: (table, values, session) => {
      dryRunCalls.push(["canInsertLocally", table, values, session]);
      return "allowed";
    },
    canReadLocally: (table, objectId, session) => {
      dryRunCalls.push(["canReadLocally", table, objectId, session]);
      return "allowed";
    },
    canUpdateLocally: (table, objectId, values, session) => {
      dryRunCalls.push(["canUpdateLocally", table, objectId, values, session]);
      return "allowed";
    },
    canDeleteLocally: (table, objectId, session) => {
      dryRunCalls.push(["canDeleteLocally", table, objectId, session]);
      return "allowed";
    },
    query: async () => [],
    waitForTransaction: async () => {},
    connect: () => {},
    disconnect: async () => {},
    updateAuth: () => {},
    onAuthFailure: () => {},
    onMutationError: () => {},
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    commitTransaction: vi.fn(async () => "committed-batch" as BatchId),
    rollbackTransaction: async () => false,
  };
  const runtime: TransactionalRuntime = { ...runtimeBase, ...runtimeOverrides };

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
    upsertCalls,
    deleteCalls,
    dryRunCalls,
  };
}

describe("JazzClient write attribution", () => {
  it("routes dry-run permission checks through runtime methods", () => {
    const { client, dryRunCalls } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
    };

    expect(client.canInsertLocally("todos", insertValues, session)).toBe("allowed");
    expect(client.canReadLocally("todos", "row-1", session)).toBe("allowed");
    expect(client.canUpdateLocally("todos", "row-1", updates, session)).toBe("allowed");
    expect(client.canDeleteLocally("todos", "row-1", session)).toBe("allowed");

    expect(dryRunCalls).toEqual([
      ["canInsertLocally", "todos", insertValues, session],
      ["canReadLocally", "todos", "row-1", session],
      ["canUpdateLocally", "todos", "row-1", updates, session],
      ["canDeleteLocally", "todos", "row-1", session],
    ]);
  });

  it("routes attributed writes through runtime methods with write context", async () => {
    const { client, insertCalls, updateCalls, deleteCalls } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    client.insert("todos", insertValues, undefined, undefined, "alice");
    client.update("todos", "row-1", updates, undefined, undefined, "alice");
    client.delete("todos", "row-1", undefined, undefined, "alice");

    expect(insertCalls).toEqual([["todos", insertValues, attributedContext, undefined]]);
    expect(updateCalls).toEqual([["todos", "row-1", updates, attributedContext]]);
    expect(deleteCalls).toEqual([["todos", "row-1", attributedContext]]);
  });

  it("encodes session and attribution together when both are provided", () => {
    const { client, insertCalls } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
    };
    const insertValues = { title: { type: "Text" as const, value: "Attributed" } };

    client.insert("todos", insertValues, undefined, session, "alice");

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
});
