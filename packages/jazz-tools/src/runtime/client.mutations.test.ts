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
import {
  LOCAL_FIRST_JWT_ISSUER,
  TRUSTED_RESERVED_SESSION_TOKEN_FIELD,
  internalSessionFromVerifiedReservedJwtPayload,
} from "./client-session.js";

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
    commitTransaction: vi.fn(() => "committed-batch" as BatchId),
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
  it("keeps public author out of serialized writes while retaining the trusted token", () => {
    const { client, insertCalls } = makeClient();
    const session = internalSessionFromVerifiedReservedJwtPayload(
      { iss: LOCAL_FIRST_JWT_ISSUER, sub: "alice", claims: { role: "owner" } },
      "local-first",
    );
    expect(session).toMatchObject({ issuer: LOCAL_FIRST_JWT_ISSUER, user_id: "alice" });

    client.insert(
      "todos",
      { title: { type: "Text", value: "Private boundary" } },
      undefined,
      session ?? undefined,
    );

    const serialized = JSON.parse(insertCalls[0]?.[2] ?? "null");
    expect(serialized).toEqual({
      issuer: LOCAL_FIRST_JWT_ISSUER,
      user_id: "alice",
      claims: { role: "owner" },
      authMode: "local-first",
      [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: expect.any(String),
    });
    expect(serialized).not.toHaveProperty("author");
  });

  it("routes dry-run permission checks through runtime methods", () => {
    const { client, dryRunCalls } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      issuer: "https://issuer.example",
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
      issuer: "https://issuer.example",
      authMode: "external",
    };
    const insertValues = { title: { type: "Text" as const, value: "Attributed" } };

    client.insert("todos", insertValues, undefined, session, "alice");

    expect(insertCalls).toHaveLength(1);
    expect(insertCalls[0]?.[0]).toBe("todos");
    expect(insertCalls[0]?.[1]).toEqual(insertValues);
    expect(JSON.parse(insertCalls[0]?.[2] ?? "null")).toEqual({
      session,
      attribution: "alice",
    });
    expect(insertCalls[0]?.[3]).toBeUndefined();
  });
});
