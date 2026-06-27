import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Runtime, type TransactionalRuntime } from "./client.js";
import type { AppContext, Session } from "./context.js";

function makeClient(runtimeOverrides: Partial<TransactionalRuntime> = {}) {
  const insertCalls: Array<
    [string, Record<string, unknown>, string | undefined, string | undefined]
  > = [];
  const restoreCalls: Array<[string, string, Record<string, unknown>, string | undefined]> = [];
  const updateCalls: Array<[string, string, Record<string, unknown>, string | undefined]> = [];
  const upsertCalls: Array<[string, string, Record<string, unknown>, string | undefined]> = [];
  const deleteCalls: Array<[string, string, string | undefined]> = [];

  const runtimeBase: TransactionalRuntime = {
    beginTransaction: (mode) => `transaction-${mode}`,
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
        transactionId: writeContextJson
          ? "insert-with-context-transaction-id"
          : "insert-transaction-id",
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
        transactionId: writeContextJson
          ? "restore-with-context-transaction-id"
          : "restore-transaction-id",
      };
    },
    update: (
      table: string,
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateCalls.push([table, objectId, updates, writeContextJson ?? undefined]);
      return {
        transactionId: writeContextJson
          ? "update-with-context-transaction-id"
          : "update-transaction-id",
      };
    },
    upsert: (
      table: string,
      objectId: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      upsertCalls.push([table, objectId, values, writeContextJson ?? undefined]);
      return {
        transactionId: writeContextJson
          ? "upsert-with-context-transaction-id"
          : "upsert-transaction-id",
      };
    },
    delete: (table: string, objectId: string, writeContextJson?: string | null) => {
      deleteCalls.push([table, objectId, writeContextJson ?? undefined]);
      return {
        transactionId: writeContextJson
          ? "delete-with-context-transaction-id"
          : "delete-transaction-id",
      };
    },
    query: async () => [],
    waitForTransaction: async () => {},
    connect: () => {},
    disconnect: () => {},
    updateAuth: () => {},
    onAuthFailure: () => {},
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    commitTransaction: vi.fn(),
    rollbackTransaction: () => false,
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
  };
}

describe("JazzClient write attribution", () => {
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
