import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { runInTransaction, Transaction } from "./db.js";
import { ExclusiveWriteResult } from "./client.js";
import {
  JazzClient,
  type TxId,
  type OpenTransactionId,
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
    const openTransactionId = writeContextJson
      ? (JSON.parse(writeContextJson).transaction_id as OpenTransactionId | undefined)
      : undefined;
    return openTransactionId
      ? { kind: "staged", openTransactionId }
      : { kind: "committed", txId: committedId as TxId };
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
    commitTransaction: vi.fn(() => "committed-batch" as TxId),
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
      defaultDurabilityTier: "local" | "global",
    ): JazzClient;
  };

  return {
    client: new JazzClientCtor(runtime, context, "global"),
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
  it.each(["mergeable", "exclusive"] as const)(
    "preserves registered %s preparation failure without submitting the transaction",
    async (kind) => {
      const failure = new Error("Key lookup failed");
      let submitted = false;
      let continued = false;
      let rolledBack = false;
      const { client } = makeClient({
        waitForTransaction: async (id) => {
          await id;
        },
        commitTransaction: () => {
          submitted = true;
          return "unexpected" as TxId;
        },
        rollbackTransaction: async () => {
          rolledBack = true;
          return true;
        },
      });
      const tx = new Transaction(kind, () => client, undefined, undefined, client);
      const id = tx.openTransactionId();
      const result = await runInTransaction(
        tx,
        () => {
          client.prepareTransaction(id, async () => {
            throw failure;
          });
          client.prepareTransaction(id, async () => {
            continued = true;
          });
        },
        client,
      );
      await expect(result.wait({ tier: "global" })).rejects.toBe(failure);
      expect(submitted).toBe(false);
      expect(continued).toBe(false);
      expect(rolledBack).toBe(true);
      expect(() => client.prepareTransaction(id, async () => {})).toThrow("closed");
    },
  );

  it("does not start queued preparation after rollback", async () => {
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started!: () => void;
    const running = new Promise<void>((resolve) => {
      started = resolve;
    });
    let continued = false;
    const { client } = makeClient();
    const tx = new Transaction("exclusive", () => client, undefined, undefined, client);
    const id = tx.openTransactionId();
    client.prepareTransaction(id, async () => {
      started();
      await ready;
    });
    client.prepareTransaction(id, async () => {
      continued = true;
    });
    try {
      await running;
      await tx.rollback();
    } finally {
      release();
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(continued).toBe(false);
    expect(() => client.prepareTransaction(id, async () => {})).toThrow("closed");
  });

  it.each(["mergeable", "exclusive"] as const)(
    "drains registered %s preparation before a dependent read and commit",
    async (kind) => {
      let release!: () => void;
      const ready = new Promise<void>((resolve) => {
        release = resolve;
      });
      let prepared = "not started";
      const { client } = makeClient({
        waitForTransaction: async (id) => {
          await id;
        },
        query: async () => {
          if (prepared !== "complete") throw new Error("Read overtook preparation");
          return [];
        },
        commitTransaction: () => {
          if (prepared !== "complete") throw new Error("Commit overtook preparation");
          return "prepared-transaction" as TxId;
        },
      });
      const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) });
      const tx = new Transaction(kind, () => client, undefined, undefined, client);
      client.prepareTransaction(tx.openTransactionId(), async () => {
        await ready;
        prepared = "first";
      });
      client.prepareTransaction(tx.openTransactionId(), async () => {
        if (prepared !== "first") throw new Error("Preparation ran out of order");
        prepared = "complete";
      });
      const reading = tx.all(app.todos);
      const committed = tx.commit();
      expect(committed).not.toBeInstanceOf(Promise);
      let finished = false;
      const waiting = committed.wait({ tier: "global" }).then(() => {
        finished = true;
      });
      try {
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(finished).toBe(false);
        expect(prepared).toBe("not started");
      } finally {
        release();
        await Promise.allSettled([reading, waiting]);
      }
      await expect(reading).resolves.toEqual([]);
      await waiting;
      expect(finished).toBe(true);
    },
  );

  it.each(["mergeable", "exclusive"] as const)(
    "preserves a %s read failure when rollback closes another in-flight read",
    async (kind) => {
      const failure = new Error("Read preparation failed");
      let failRead!: (error: Error) => void;
      const failedRead = new Promise<unknown[]>((_, reject) => {
        failRead = reject;
      });
      let finishRead!: (rows: unknown[]) => void;
      const delayedRead = new Promise<unknown[]>((resolve) => {
        finishRead = resolve;
      });
      let open = true;
      let queryNumber = 0;
      const { client } = makeClient({
        query: async () => {
          const rows = await (queryNumber++ === 0 ? failedRead : delayedRead);
          if (!open) throw new Error("Read view closed by rollback");
          return rows;
        },
        rollbackTransaction: async () => {
          const wasOpen = open;
          open = false;
          return wasOpen;
        },
        waitForTransaction: async (id) => {
          await id;
        },
      });
      const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) });
      const tx = new Transaction(kind, () => client, undefined, undefined, client);
      const first = tx.all(app.todos, { tier: "local" });
      const second = tx.all(app.todos, { tier: "local" });
      try {
        const result = await runInTransaction(tx, () => "callback value", client);
        const waiting =
          result instanceof ExclusiveWriteResult ? result.wait() : result.wait({ tier: "local" });
        failRead(failure);
        await expect(waiting).rejects.toBe(failure);
        await expect(first).rejects.toBe(failure);
        finishRead([]);
        await expect(second).rejects.toThrow("Read view closed by rollback");
      } finally {
        failRead(failure);
        finishRead([]);
        await Promise.allSettled([first, second]);
      }
    },
  );

  it.each(["mergeable", "exclusive"] as const)(
    "rejects further %s operations while a no-read commit is deferred",
    async (kind) => {
      let complete!: (id: TxId) => void;
      const pending = new Promise<TxId>((resolve) => {
        complete = resolve;
      });
      const { client } = makeClient({ commitTransaction: () => pending });
      const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) });
      const tx = new Transaction(kind, () => client, undefined, undefined, client);
      const committed = tx.commit();
      try {
        expect(committed).not.toBeInstanceOf(Promise);
        expect(() => tx.commit()).toThrow("after commit has been requested");
        expect(() => tx.insert(app.todos, { title: "too late" })).toThrow(
          "after commit has been requested",
        );
        expect(() => tx.rollback()).toThrow("after commit has been requested");
        await expect(tx.all(app.todos, { tier: "local" })).rejects.toThrow(
          "after commit has been requested",
        );
      } finally {
        complete("prepared-transaction" as TxId);
        await committed.txId;
      }
    },
  );

  it.each(["mergeable", "exclusive"] as const)(
    "closes a %s callback transaction after deferred commit fails",
    async (kind) => {
      const failure = new Error("Preparation failed before submission");
      let open = true;
      const { client } = makeClient({
        commitTransaction: () => Promise.reject(failure),
        rollbackTransaction: async () => {
          const wasOpen = open;
          open = false;
          return wasOpen;
        },
        waitForTransaction: async (id) => {
          await id;
        },
      });
      const tx = new Transaction(kind, () => client, undefined, undefined, client);
      const result = await runInTransaction(tx, () => "callback value", client);
      const waiting =
        result instanceof ExclusiveWriteResult ? result.wait() : result.wait({ tier: "local" });
      await expect(waiting).rejects.toBe(failure);
      // The callback helper owns cleanup; no open transaction remains for its caller.
      await expect(tx.rollback()).resolves.toBe(false);
    },
  );

  it.each(["mergeable", "exclusive"] as const)(
    "retains a deferred %s failure for a later wait without an unhandled rejection",
    async (kind) => {
      const failure = new Error("Preparation failed");
      const unhandled = vi.fn();
      process.on("unhandledRejection", unhandled);
      try {
        const { client } = makeClient({
          commitTransaction: () => Promise.reject(failure),
          waitForTransaction: async (id) => {
            await id;
          },
        });
        const tx = new Transaction(kind, () => client, undefined, undefined, client);
        const result = await runInTransaction(tx, () => "callback value", client);
        // Applications can attach their wait after the asynchronous failure arrives.
        await new Promise((resolve) => setTimeout(resolve, 0));
        const waiting =
          result instanceof ExclusiveWriteResult ? result.wait() : result.wait({ tier: "local" });
        await expect(waiting).rejects.toBe(failure);
        expect(unhandled).not.toHaveBeenCalled();
      } finally {
        process.off("unhandledRejection", unhandled);
      }
    },
  );

  it.each(["mergeable", "exclusive"] as const)(
    "returns a %s callback result before deferred commit, but waits for persistence",
    async (kind) => {
      let prepare!: (id: TxId) => void;
      const prepared = new Promise<TxId>((resolve) => {
        prepare = resolve;
      });
      let persist!: () => void;
      const persisted = new Promise<void>((resolve) => {
        persist = resolve;
      });
      const { client } = makeClient({
        commitTransaction: () => prepared,
        waitForTransaction: async (id) => {
          await id;
          await persisted;
        },
      });
      const tx = new Transaction(kind, () => client, undefined, undefined, client);
      const resultPromise = runInTransaction(tx, () => "callback value", client);
      try {
        // Runtime I/O is controlled; no transaction or write-handle method is mocked.
        const result = await Promise.race([
          resultPromise,
          new Promise<undefined>((resolve) => setTimeout(() => resolve(undefined), 0)),
        ]);
        expect(result, "callback result must not await deferred commit").toBeDefined();
        if (!result) throw new Error("callback result was withheld");
        expect(result.value).toBe("callback value");
        let completed = false;
        const waiting =
          result instanceof ExclusiveWriteResult ? result.wait() : result.wait({ tier: "local" });
        waiting.then(() => {
          completed = true;
        });
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(completed).toBe(false);
        prepare("prepared-transaction" as TxId);
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(completed).toBe(false);
        persist();
        await expect(waiting).resolves.toBe("callback value");
      } finally {
        prepare("prepared-transaction" as TxId);
        persist();
        await resultPromise;
      }
    },
  );

  it("keeps public author out of serialized writes while retaining the trusted token", () => {
    const { client, insertCalls } = makeClient();
    const session = internalSessionFromVerifiedReservedJwtPayload(
      { iss: LOCAL_FIRST_JWT_ISSUER, sub: "alice", role: "owner" },
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
