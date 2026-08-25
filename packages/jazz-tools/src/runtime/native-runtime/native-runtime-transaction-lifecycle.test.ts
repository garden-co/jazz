import { expect, it, vi } from "vitest";
import { PostcardWriter, createRecord, writeDescriptor } from "./native-codec.js";
import type { WasmSchema } from "../../drivers/types.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import {
  createOpenBatchId,
  type BatchId,
  type MutationErrorEvent,
  type OpenBatchId,
} from "../client.js";

function beginTestBatch(runtime: NativeRuntimeAdapter, userId?: string): OpenBatchId {
  const id = createOpenBatchId();
  runtime.beginTransaction(
    "mergeable",
    id,
    userId === undefined
      ? undefined
      : JSON.stringify({ issuer: "https://issuer.example", user_id: userId }),
  );
  return id;
}

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;
const TEST_RUNTIME_AUTHOR = new TextEncoder().encode('["urn:jazz:test","runtime"]');

type EncodedTestRow = {
  table: string;
  rowId: Uint8Array;
  title: string;
};

function encodeRows(rows: EncodedTestRow[]): Uint8Array {
  const writer = new PostcardWriter();
  const byTable = new Map<string, EncodedTestRow[]>();
  for (const row of rows) {
    const tableRows = byTable.get(row.table) ?? [];
    tableRows.push(row);
    byTable.set(row.table, tableRows);
  }
  writer.vec((batch, batchIndex) => {
    const [table, tableRows] = Array.from(byTable.entries())[batchIndex]!;
    const descriptor = [{ name: "title", valueType: { tag: 8 } }];
    batch.string(table);
    writeDescriptor(batch, descriptor);
    batch.vec((row, index) => {
      const source = tableRows[index]!;
      row.bytes(source.rowId);
      row.bool(false);
      row.bytes(
        createRecord(descriptor, [Uint8Array.from([2, ...new TextEncoder().encode(source.title)])]),
      );
    }, tableRows.length);
  }, byTable.size);
  return writer.finish();
}

function fakeDb<T extends object>(
  db: T,
): T & { setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void } {
  type FakeOpenBatch = {
    kind: "mergeable" | "exclusive";
    author?: Uint8Array;
    tx?: TxForTest;
  };
  const implementation = db as T & {
    mergeableTx?(openBatchId: string): TxForTest;
    mergeableTxForIdentity?(openBatchId: string, author: Uint8Array): TxForTest;
    exclusiveTx?(openBatchId: string): TxForTest;
  };
  const openBatches = new Map<string, FakeOpenBatch>();
  const attach = (openBatchId: string, kind: FakeOpenBatch["kind"]): TxForTest => {
    const batch = openBatches.get(openBatchId);
    if (!batch || batch.kind !== kind) throw new Error(`unknown ${kind} batch ${openBatchId}`);
    batch.tx ??=
      kind === "exclusive"
        ? (implementation.exclusiveTx?.(openBatchId) ?? fakeTx())
        : batch.author && implementation.mergeableTxForIdentity
          ? implementation.mergeableTxForIdentity(openBatchId, batch.author)
          : (implementation.mergeableTx?.(openBatchId) ?? fakeTx());
    return batch.tx;
  };
  return {
    setTickScheduler: () => undefined,
    onMutationError: () => undefined,
    beginTransaction: (openBatchId: string, kind: FakeOpenBatch["kind"], author?: Uint8Array) => {
      openBatches.set(openBatchId, { kind, author });
    },
    attachMergeableTx: (openBatchId: string) => attach(openBatchId, "mergeable"),
    attachExclusiveTx: (openBatchId: string) => attach(openBatchId, "exclusive"),
    commitTransaction: (openBatchId: string) => {
      const batch = openBatches.get(openBatchId);
      if (!batch) throw new Error(`unknown batch ${openBatchId}`);
      openBatches.delete(openBatchId);
      return batch.tx?.commit() ?? fakeWrite();
    },
    rollbackTransaction: (openBatchId: string) => {
      const batch = openBatches.get(openBatchId);
      if (!batch) throw new Error(`unknown batch ${openBatchId}`);
      batch.tx?.rollback();
      openBatches.delete(openBatchId);
    },
    ...db,
  };
}

function fakeTx(overrides: Partial<TxForTest> = {}): TxForTest {
  return {
    commit: () => fakeWrite(),
    rollback: () => undefined,
    insertEncoded: (_table, _cells, options) => options?.rowId ?? new Uint8Array(16),
    restoreEncoded: () => undefined,
    updateEncoded: () => undefined,
    upsertEncoded: () => undefined,
    deleteEncoded: () => undefined,
    ...overrides,
  };
}

function fakeWrite() {
  return {
    batchId: "00000000000070008000000000000001",
    payload: new Uint8Array(0),
    wait: async () => undefined,
    writeState: () => ({}),
  };
}

type TxForTest = {
  commit(): ReturnType<typeof fakeWrite>;
  rollback(): void;
  insertEncoded(
    table: string,
    cells: Uint8Array,
    options?: { rowId?: Uint8Array; branch?: unknown; updatedAtMs?: number },
  ): Uint8Array;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: { branch?: unknown; updatedAtMs?: number },
  ): void;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    options?: { head?: unknown; base?: unknown; updatedAtMs?: number },
  ): void;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: { branch?: unknown; updatedAtMs?: number },
  ): void;
  deleteEncoded(
    table: string,
    rowId: Uint8Array,
    options?: { head?: unknown; base?: unknown; updatedAtMs?: number },
  ): void;
};

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

it("stages authenticated client mutations through the optimistic local core path", () => {
  const staged: string[] = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          allForIdentity: () => encodeRows([]),
          insertEncoded: (table: string, _cells: Uint8Array, options?: { rowId?: Uint8Array }) => {
            staged.push(table);
            return { ...fakeWrite(), rowId: options?.rowId ?? new Uint8Array(16) };
          },
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );

  runtime.insert(
    "todos",
    { title: { type: "Text", value: "optimistic" } },
    JSON.stringify({
      session: {
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000a1",
      },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(staged).toEqual(["todos"]);
});

it("preserves logical user columns that share names with native storage metadata", () => {
  const collisionSchema = {
    records: {
      columns: ["schema_version", "parents", "authored_columns"].map((name) => ({
        name,
        column_type: { type: "Text" } as const,
        nullable: false,
      })),
    },
  } satisfies WasmSchema;
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          insertEncoded: (
            _table: string,
            _cells: Uint8Array,
            options?: { rowId?: Uint8Array },
          ) => ({
            ...fakeWrite(),
            rowId: options?.rowId ?? new Uint8Array(16),
          }),
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    collisionSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );

  const inserted = runtime.insert("records", {
    schema_version: { type: "Text", value: "v1" },
    parents: { type: "Text", value: "root" },
    authored_columns: { type: "Text", value: "all" },
  });

  expect(inserted.values).toEqual([
    { type: "Text", value: "v1" },
    { type: "Text", value: "root" },
    { type: "Text", value: "all" },
  ]);
});

it("uses identity-aware core txs only on an explicit trusted-serving host", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const authors: string[] = [];
  const staged: string[] = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          allForIdentity: () => encodeRows([]),
          mergeableTxForIdentity: (_openBatchId: string, author: Uint8Array) => {
            authors.push(new TextDecoder().decode(author));
            return fakeTx({
              insertEncoded: (table, _cells, options) => {
                staged.push(table);
                return options?.rowId ?? new Uint8Array(16);
              },
            });
          },
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );

  const tx = beginTestBatch(runtime, alice);
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "session tx" } },
    JSON.stringify({
      batch_id: tx,
      session: { issuer: "https://issuer.example", user_id: alice },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(authors).toEqual(['["https://issuer.example","00000000-0000-0000-0000-0000000000a1"]']);
  expect(staged).toEqual(["todos"]);
});

it("binds a trusted-serving exclusive transaction to its opening identity", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const issuer = "https://issuer.example";
  const beganAs: string[] = [];
  const policySchema = {
    todos: {
      ...testSchema.todos,
      policies: {},
    },
  } satisfies WasmSchema;
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () => {
        const db = fakeDb({
          all: () => encodeRows([]),
          allForIdentity: () => encodeRows([]),
          exclusiveTx: () => fakeTx(),
          prepareQuery: () => ({}),
          tick: () => undefined,
        }) as unknown as {
          beginTransaction(
            openBatchId: string,
            kind: "mergeable" | "exclusive",
            author?: Uint8Array,
          ): void;
        };
        const begin = db.beginTransaction.bind(db);
        db.beginTransaction = (openBatchId, kind, author) => {
          beganAs.push(author === undefined ? "none" : new TextDecoder().decode(author));
          begin(openBatchId, kind, author);
        };
        return db;
      },
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    policySchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );

  const tx = createOpenBatchId();
  runtime.beginTransaction("exclusive", tx, JSON.stringify({ issuer, user_id: alice }));
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "session-scoped exclusive write" } },
    JSON.stringify({ batch_id: tx, session: { issuer, user_id: alice } }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(beganAs).toEqual([`["${issuer}","${alice}"]`]);
  expect(() =>
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "wrong subject" } },
      JSON.stringify({
        batch_id: tx,
        session: { issuer, user_id: "00000000-0000-0000-0000-0000000000b2" },
      }),
      "00000000-0000-0000-0000-000000000002",
    ),
  ).toThrow("Native runtime exclusive transaction cannot mix write identities");
});

it("uses the opening identity for trusted-serving transaction reads", async () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const issuer = "https://issuer.example";
  const tx = fakeTx();
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          allForIdentity: () => encodeRows([]),
          allInTransaction: (_query: object, receivedTx: TxForTest) => {
            expect(receivedTx).toBe(tx);
            return encodeRows([
              {
                table: "todos",
                rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                title: "Alice's pending row",
              },
            ]);
          },
          exclusiveTx: () => tx,
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );

  const transactionId = createOpenBatchId();
  runtime.beginTransaction("exclusive", transactionId, JSON.stringify({ issuer, user_id: alice }));

  await expect(
    runtime.query(
      JSON.stringify({ table: "todos" }),
      JSON.stringify({ issuer, user_id: "00000000-0000-0000-0000-0000000000b2" }),
      "local",
      JSON.stringify({ transaction_batch_id: transactionId }),
    ),
  ).resolves.toEqual([
    {
      table: "todos",
      id: "00000000-0000-0000-0000-000000000001",
      values: [{ type: "Text", value: "Alice's pending row" }],
    },
  ]);
});

it("rejects a duplicate live OpenBatchId without replacing its staged transaction", () => {
  const stagedTransactions: string[][] = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          mergeableTx: () => {
            const staged: string[] = [];
            stagedTransactions.push(staged);
            return fakeTx({
              insertEncoded: (table, _cells, options) => {
                staged.push(table);
                return options?.rowId ?? new Uint8Array(16);
              },
            });
          },
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const id = createOpenBatchId();
  runtime.beginTransaction("mergeable", id);
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "first" } },
    JSON.stringify({ batch_id: id }),
  );

  expect(() => runtime.beginTransaction("mergeable", id)).toThrow(
    `Begin transaction failed: transaction ${id} has already been opened`,
  );
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "second" } },
    JSON.stringify({ batch_id: id }),
  );

  expect(stagedTransactions).toEqual([["todos", "todos"]]);
});

it("commits empty exclusive transactions, rejects empty mergeable transactions, and rejects unknown waits", async () => {
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () => fakeDb({ exclusiveTx: () => fakeTx() }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const emptyMergeable = createOpenBatchId();
  runtime.beginTransaction("mergeable", emptyMergeable);
  expect(() => runtime.commitTransaction(emptyMergeable)).toThrow(
    "empty mergeable transaction has no committed unit; roll it back instead",
  );
  await runtime.rollbackTransaction(emptyMergeable);

  const openBatchId = createOpenBatchId();
  runtime.beginTransaction("exclusive", openBatchId);
  const committed = await runtime.commitTransaction(openBatchId);
  expect(committed).toBe("00000000000070008000000000000001");
  await expect(
    runtime.waitForTransaction("00000000000070008000000000000002" as BatchId, "local"),
  ).rejects.toThrow(
    "Wait for transaction failed: unknown transaction 00000000000070008000000000000002",
  );

  const reopened = new NativeRuntimeAdapter(
    {
      openMemory: () => fakeDb({ exclusiveTx: () => fakeTx() }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  await expect(reopened.waitForTransaction(committed, "local")).rejects.toThrow(
    `Wait for transaction failed: unknown transaction ${committed}`,
  );
});

it("binds the trusted-serving identity when an exclusive transaction begins", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const observed: Array<{ phase: "begin" | "commit"; author?: string }> = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          beginTransaction: (
            _openBatchId: string,
            _kind: "mergeable" | "exclusive",
            author?: Uint8Array,
          ) =>
            observed.push({
              phase: "begin",
              author: author && new TextDecoder().decode(author),
            }),
          attachExclusiveTx: () => fakeTx(),
          commitTransaction: (
            _openBatchId: string,
            _kind?: "mergeable" | "exclusive",
            author?: Uint8Array,
          ) => {
            observed.push({ phase: "commit", author: author && new TextDecoder().decode(author) });
            return fakeWrite();
          },
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    TEST_RUNTIME_AUTHOR,
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );

  const openBatchId = createOpenBatchId();
  runtime.beginTransaction(
    "exclusive",
    openBatchId,
    JSON.stringify({ issuer: "https://issuer.example", user_id: alice }),
  );
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "exclusive" } },
    JSON.stringify({
      batch_id: openBatchId,
      session: { issuer: "https://issuer.example", user_id: alice },
    }),
  );
  runtime.commitTransaction(openBatchId);

  expect(observed).toEqual([
    { phase: "begin", author: `["https://issuer.example","${alice}"]` },
    // The native core persists the bound subject; commit must not accept a replacement.
    { phase: "commit", author: undefined },
  ]);
});

it("emits an onMutationError event for an unawaited rejected write", async () => {
  const batchId = "00000000000070008000000000000042" as BatchId;
  let mutationErrorCallback: ((event: MutationErrorEvent) => void) | undefined;
  const write = {
    batchId,
    payload: new Uint8Array(),
    wait: async () => undefined,
    writeState: () => ({}),
  };
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          insertEncoded: () => write,
          onMutationError: (callback: (event: MutationErrorEvent) => void) => {
            mutationErrorCallback = callback;
          },
        }),
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const listener = vi.fn();
  runtime.onMutationError(listener);

  runtime.insert(
    "todos",
    { title: { type: "Text", value: "rejected" } },
    null,
    "00000000-0000-0000-0000-000000000042",
  );
  mutationErrorCallback?.({
    code: "permission_denied",
    reason: "Write rejected by server authorization",
    transaction: {
      transactionId: batchId,
      kind: "mergeable",
      sealed: true,
      latestSettlement: {
        kind: "rejected",
        transactionId: batchId,
        code: "permission_denied",
        reason: "Write rejected by server authorization",
      },
    },
  });

  await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1));
  expect(listener).toHaveBeenCalledWith({
    code: "permission_denied",
    reason: "Write rejected by server authorization",
    transaction: {
      transactionId: batchId,
      kind: "mergeable",
      sealed: true,
      latestSettlement: {
        kind: "rejected",
        transactionId: batchId,
        code: "permission_denied",
        reason: "Write rejected by server authorization",
      },
    },
  });
});

it("does not emit onMutationError when an active wait handles the rejection", async () => {
  const batchId = "00000000000070008000000000000043" as BatchId;
  let rejected = false;
  const stateChangeWaiters: Array<() => void> = [];
  const nextWriteStateChange = () =>
    new Promise<void>((resolve) => {
      stateChangeWaiters.push(resolve);
    });
  const write = {
    batchId,
    payload: new Uint8Array(),
    wait: async () => {
      await nextWriteStateChange();
      if (rejected) throw new Error("WriteRejected: AuthorizationDenied");
    },
    writeState: () => ({}),
  };
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          insertEncoded: () => write,
          onMutationError: () => undefined,
        }),
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const listener = vi.fn();
  runtime.onMutationError(listener);

  runtime.insert(
    "todos",
    { title: { type: "Text", value: "rejected" } },
    null,
    "00000000-0000-0000-0000-000000000043",
  );
  const wait = runtime.waitForTransaction(batchId, "edge");
  await Promise.resolve();
  rejected = true;
  stateChangeWaiters.splice(0).forEach((resolve) => resolve());

  await expect(wait).rejects.toMatchObject({
    kind: "rejected",
    transactionId: batchId,
    code: "permission_denied",
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(listener).not.toHaveBeenCalled();
});

it("passes caller-supplied updatedAt into staged mergeable transaction writes", () => {
  const updatedAt = 1_704_067_200_123;
  const expectedUpdatedAtMs = updatedAt;
  const staged: Array<{ op: string; updatedAtMs: number | null | undefined }> = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          mergeableTx: () =>
            fakeTx({
              insertEncoded: (_table, _cells, options) => {
                staged.push({ op: "insert", updatedAtMs: options?.updatedAtMs });
                return options?.rowId ?? new Uint8Array(16);
              },
              updateEncoded: (_table, _rowId, _patch, options) =>
                staged.push({ op: "update", updatedAtMs: options?.updatedAtMs }),
              upsertEncoded: (_table, _rowId, _cells, options) =>
                staged.push({ op: "upsert", updatedAtMs: options?.updatedAtMs }),
              restoreEncoded: (_table, _rowId, _cells, options) =>
                staged.push({ op: "restore", updatedAtMs: options?.updatedAtMs }),
              deleteEncoded: (_table, _rowId, options) =>
                staged.push({ op: "delete", updatedAtMs: options?.updatedAtMs }),
            }),
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );

  const tx = beginTestBatch(runtime);
  const context = JSON.stringify({ batch_id: tx, updated_at: updatedAt });
  const rowId = "00000000-0000-0000-0000-000000000001";
  runtime.insert("todos", { title: { type: "Text", value: "inserted" } }, context, rowId);
  runtime.update("todos", rowId, { title: { type: "Text", value: "updated" } }, context);
  runtime.upsert("todos", rowId, { title: { type: "Text", value: "upserted" } }, context);
  runtime.restore("todos", rowId, { title: { type: "Text", value: "restored" } }, context);
  runtime.delete("todos", rowId, context);

  expect(staged).toEqual([
    { op: "insert", updatedAtMs: expectedUpdatedAtMs },
    { op: "update", updatedAtMs: expectedUpdatedAtMs },
    { op: "upsert", updatedAtMs: expectedUpdatedAtMs },
    { op: "restore", updatedAtMs: expectedUpdatedAtMs },
    { op: "delete", updatedAtMs: expectedUpdatedAtMs },
  ]);
});

it("rejects mixed identities within one trusted-serving mergeable transaction", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          allForIdentity: () => encodeRows([]),
          mergeableTxForIdentity: () => fakeTx(),
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );

  const tx = beginTestBatch(runtime, alice);
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "one" } },
    JSON.stringify({
      batch_id: tx,
      session: { issuer: "https://issuer.example", user_id: alice },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(() =>
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "two" } },
      JSON.stringify({
        batch_id: tx,
        session: {
          issuer: "https://issuer.example",
          user_id: "00000000-0000-0000-0000-0000000000b2",
        },
      }),
      "00000000-0000-0000-0000-000000000002",
    ),
  ).toThrow("Native runtime mergeable transaction cannot mix write identities");
});

it("keeps session-scoped transaction reads on the client-local native method", async () => {
  const tx = fakeTx();
  let transactionReads = 0;
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          allForIdentity: () => encodeRows([]),
          allInTransaction: (_query: object, receivedTx: TxForTest) => {
            expect(receivedTx).toBe(tx);
            transactionReads += 1;
            return encodeRows([
              {
                table: "todos",
                rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                title: "alice pending",
              },
            ]);
          },
          allInTransactionForIdentity: () => {
            throw new Error("ordinary client transaction reads must not use trusted serving");
          },
          mergeableTx: () => tx,
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );

  const transactionId = beginTestBatch(runtime, "00000000-0000-0000-0000-0000000000a1");
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "alice pending" } },
    JSON.stringify({
      batch_id: transactionId,
      session: {
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000a1",
      },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  await expect(
    runtime.query(
      JSON.stringify({ table: "todos" }),
      JSON.stringify({
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000b2",
      }),
      "local",
      JSON.stringify({ transaction_batch_id: transactionId }),
    ),
  ).resolves.toEqual([
    {
      table: "todos",
      id: "00000000-0000-0000-0000-000000000001",
      values: [{ type: "Text", value: "alice pending" }],
    },
  ]);
  await expect(
    runtime.query(
      JSON.stringify({ table: "todos" }),
      JSON.stringify({
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000a1",
      }),
      "local",
      JSON.stringify({ transaction_batch_id: transactionId }),
    ),
  ).resolves.toEqual([
    {
      table: "todos",
      id: "00000000-0000-0000-0000-000000000001",
      values: [{ type: "Text", value: "alice pending" }],
    },
  ]);
  expect(transactionReads).toBe(2);
});
