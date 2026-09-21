import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createAccountDbWithRuntimeSource } from "../accounts/context.js";
import { createAccountManager } from "../accounts/create-account-manager.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { RuntimeClientContext } from "./runtime-source.js";
import type { JazzClient, TransactionalRuntime } from "./client.js";
import type { Db } from "./db.js";
import { beginDbTransactionAfter } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { translateQuery } from "./query-adapter.js";

// Use the real database and handles with a controlled asynchronous preparation callback.
class PreparationSource extends DefaultRuntimeSource {
  client!: JazzClient;
  override createClient(context: RuntimeClientContext): JazzClient {
    this.client = super.createClient(context);
    return this.client;
  }
}

const app = s.defineApp({
  documents: s.table({ payload: s.bytes(), label: s.string().default("untitled") }, {}),
});
let db: Db;
let source: PreparationSource;
beforeEach(async () => {
  source = new PreparationSource();
  db = await createAccountDbWithRuntimeSource(
    await localAccountConfig(`transaction-preparation-${crypto.randomUUID()}`),
    source,
  );
  await db.all(app.documents, { tier: "local" });
});
afterEach(async () => {
  vi.restoreAllMocks();
  await db.shutdown();
});

it.each(["ready", "rejected", "cancelled"])(
  "owns deferred snapshot admission through wait (%s)",
  async (outcome) => {
    const opening = vi.spyOn(
      source.client.getRuntime() as TransactionalRuntime,
      "beginTransaction",
    );
    let release!: () => void;
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tx = beginDbTransactionAfter(db, async () => {
      entered();
      await ready;
      if (outcome === "rejected") throw new Error("Admission prerequisite failed");
    });
    const inserted = tx.insert(app.documents, { payload: new Uint8Array([6]) });
    expect(inserted).not.toBeInstanceOf(Promise);
    const reading = tx.all(app.documents, { tier: "local" });
    reading.catch(() => {});
    const earlyWait = outcome === "cancelled" ? undefined : tx.commit().wait();
    earlyWait?.catch(() => {});
    await started;
    expect(opening).not.toHaveBeenCalled();
    expect(await db.all(app.documents, { tier: "local" })).toEqual([]);
    if (outcome === "cancelled") await tx.rollback();
    const wait = earlyWait ?? tx.commit().wait();
    wait.catch(() => {});
    release();
    if (outcome === "ready") {
      await wait;
      expect(await reading).toEqual([inserted]);
      expect(await db.all(app.documents, { tier: "local" })).toEqual([inserted]);
      expect(opening).toHaveBeenCalledTimes(1);
    } else {
      const message =
        outcome === "rejected" ? "Admission prerequisite failed" : "rolled back before admission";
      await expect(wait).rejects.toThrow(message);
      await expect(reading).rejects.toThrow(message);
      if (outcome === "rejected") await tx.rollback();
      expect(await db.all(app.documents, { tier: "local" })).toEqual([]);
      expect(opening).not.toHaveBeenCalled();
    }
  },
);

it("does not enter the runtime after shutdown during admission", async () => {
  const opening = vi.spyOn(source.client.getRuntime() as TransactionalRuntime, "beginTransaction");
  let release!: () => void;
  let entered!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  const tx = beginDbTransactionAfter(db, async () => {
    entered();
    await ready;
  });
  tx.insert(app.documents, { payload: new Uint8Array([4]) });
  const wait = tx.commit().wait();
  wait.catch(() => {});
  await started;
  await db.shutdown();
  release();
  await expect(wait).rejects.toThrow();
  expect(opening).not.toHaveBeenCalled();
});

it("does not enter a discarded runtime after suspended admission", async () => {
  const opening = vi.spyOn(source.client.getRuntime() as TransactionalRuntime, "beginTransaction");
  let release!: () => void;
  let entered!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  const tx = beginDbTransactionAfter(db, async () => {
    entered();
    await ready;
  });
  tx.insert(app.documents, { payload: new Uint8Array([5]) });
  const reading = tx.all(app.documents, { tier: "local" });
  reading.catch(() => {});
  const wait = tx.commit().wait();
  wait.catch(() => {});
  await started;
  source.client.discard();
  release();
  await expect(wait).rejects.toThrow();
  await expect(reading).rejects.toThrow();
  expect(opening).not.toHaveBeenCalled();
});

it.each(["selection", "logout"])(
  "preserves account isolation during suspended admission (%s)",
  async (change) => {
    await db.shutdown();
    const appId = `account-admission-${crypto.randomUUID()}`;
    let saved: string | null = null;
    const accounts = await createAccountManager({
      appId,
      serverUrl: "http://127.0.0.1:1",
      store: {
        async read() {
          return saved;
        },
        async update(transform) {
          saved = transform(saved);
        },
      },
    });
    const first = accounts.createLocalFirst();
    db = await createAccountDbWithRuntimeSource(
      { appId, account: first, driver: { type: "memory" } },
      source,
    );
    await db.all(app.documents, { tier: "local" });
    const opening = vi.spyOn(
      source.client.getRuntime() as TransactionalRuntime,
      "beginTransaction",
    );
    let release!: () => void;
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tx = beginDbTransactionAfter(db, async () => {
      entered();
      await ready;
    });
    const oldRow = tx.insert(app.documents, {
      payload: new Uint8Array([1]),
      label: "first account",
    });
    const wait = tx.commit().wait();
    wait.catch(() => {});
    await started;
    if (change === "logout") accounts.logout();
    const second = accounts.createLocalFirst();
    expect(second.id).not.toBe(first.id);
    expect(accounts.getLoggedIn()!.id).toBe(second.id);
    const replacement = await createAccountDbWithRuntimeSource(
      { appId, account: second, driver: { type: "memory" } },
      new PreparationSource(),
    );
    try {
      const newRow = await replacement
        .insert(app.documents, { payload: new Uint8Array([2]), label: "second account" })
        .wait({ tier: "local" });
      release();
      if (change === "logout") {
        await expect(wait).rejects.toThrow();
        expect(opening).not.toHaveBeenCalled();
      } else {
        await wait;
        expect(opening).toHaveBeenCalledTimes(1);
        expect(JSON.parse(opening.mock.calls[0]![2]!).account_id).toBe(first.id);
        expect(await db.all(app.documents, { tier: "local" })).toEqual([oldRow]);
      }
      expect(await replacement.all(app.documents, { tier: "local" })).toEqual([newRow]);
    } finally {
      release();
      await replacement.shutdown();
      accounts.logout();
    }
  },
);

it("does not delay an ordinary transaction beside suspended admission", async () => {
  let release!: () => void;
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  const deferred = beginDbTransactionAfter(db, () => ready);
  deferred.insert(app.documents, { payload: new Uint8Array([1]) });
  const pending = deferred.commit().wait();
  pending.catch(() => {});
  try {
    const ordinary = db.beginExclusiveTransaction();
    const row = ordinary.insert(app.documents, { payload: new Uint8Array([2]) });
    await ordinary.commit().wait();
    expect(await db.all(app.documents, { tier: "local" })).toEqual([row]);
  } finally {
    release();
  }
  await pending;
  expect(await db.all(app.documents, { tier: "local" })).toHaveLength(2);
});

it("returns an inserted row immediately without overtaking earlier preparation", async () => {
  const tx = db.beginExclusiveTransaction();
  const id = tx.openTransactionId();
  let documentId: string;
  source.client.prepareTransaction(id, async () => {
    const query = app.documents.where({ id: documentId });
    const earlier = await source.client
      .getRuntime()
      .query(
        translateQuery(query._build(), query._schema),
        undefined,
        "local",
        JSON.stringify({ transaction_id: id }),
      );
    expect(earlier).toEqual([]);
  });
  const inserted = tx.insert(app.documents, { payload: new Uint8Array([3]) });
  documentId = inserted.id;
  expect(inserted).not.toBeInstanceOf(Promise);
  expect(inserted.payload).toEqual(new Uint8Array([3]));
  const reading = tx.all(app.documents, { tier: "local" });
  // Keep a failed preparation visible through both read and commit handles.
  const result = await Promise.allSettled([reading, tx.commit().wait()]);
  expect(result).toEqual([
    { status: "fulfilled", value: [inserted] },
    { status: "fulfilled", value: undefined },
  ]);
});

it.each(["mergeable", "exclusive"] as const)(
  "terminalizes a %s transaction when a pending read fails",
  async (kind) => {
    const failure = new Error(`controlled ${kind} read failure`);
    const runtime = source.client.getRuntime() as TransactionalRuntime;
    const query = vi.spyOn(runtime, "query").mockRejectedValueOnce(failure);
    const tx = kind === "exclusive" ? db.beginExclusiveTransaction() : db.beginTransaction();
    const inserted = tx.insert(app.documents, { payload: new Uint8Array([7]) });
    const reading = tx.all(app.documents, { tier: "local" });
    const committed = tx.commit();

    await expect(reading).rejects.toBe(failure);
    const waiting = kind === "exclusive" ? committed.wait() : committed.wait({ tier: "local" });
    await expect(waiting).rejects.toBe(failure);
    await expect(db.all(app.documents, { tier: "local" })).resolves.toEqual([]);
    expect(() => tx.commit()).toThrow("after a pending read failed");
    expect(() => tx.insert(app.documents, { payload: new Uint8Array([8]) })).toThrow(
      "after a pending read failed",
    );
    await expect(tx.all(app.documents, { tier: "local" })).rejects.toThrow(
      "after a pending read failed",
    );
    await expect(tx.rollback()).resolves.toBe(false);
    expect(inserted.payload).toEqual(new Uint8Array([7]));
    expect(query).toHaveBeenCalledOnce();
  },
);

it.each(["mergeable", "exclusive"] as const)(
  "preserves the pending read error when failed-read rollback rejects in a %s transaction",
  async (kind) => {
    const failure = new Error(`controlled ${kind} read failure`);
    const cleanupFailure = new Error(`controlled ${kind} rollback failure`);
    const runtime = source.client.getRuntime() as TransactionalRuntime;
    vi.spyOn(runtime, "query").mockRejectedValueOnce(failure);
    const rollback = vi.spyOn(runtime, "rollbackTransaction").mockRejectedValueOnce(cleanupFailure);
    const tx = kind === "exclusive" ? db.beginExclusiveTransaction() : db.beginTransaction();
    tx.insert(app.documents, { payload: new Uint8Array([9]) });
    const reading = tx.all(app.documents, { tier: "local" });
    const committed = tx.commit();

    await expect(reading).rejects.toBe(failure);
    const waiting = kind === "exclusive" ? committed.wait() : committed.wait({ tier: "local" });
    await expect(waiting).rejects.toBe(failure);
    await expect(db.all(app.documents, { tier: "local" })).resolves.toEqual([]);
    expect(() => tx.commit()).toThrow("after a pending read failed");
    expect(() => tx.insert(app.documents, { payload: new Uint8Array([10]) })).toThrow(
      "after a pending read failed",
    );
    await expect(tx.all(app.documents, { tier: "local" })).rejects.toThrow(
      "after a pending read failed",
    );

    rollback.mockRestore();
    await expect(tx.rollback()).resolves.toBe(true);
  },
);

it.each(
  ["update", "upsert", "delete"].flatMap((operation) =>
    ["mergeable", "exclusive"].map((kind) => ({ operation, kind })),
  ),
)(
  "preserves an ordinary $operation after deferred preparation in a $kind transaction",
  async ({ operation, kind }) => {
    const inserted = db.insert(app.documents, { payload: new Uint8Array([0]) });
    await inserted.wait({ tier: "local" });
    const tx = kind === "exclusive" ? db.beginExclusiveTransaction() : db.beginTransaction();
    // Bind to the table before registering work at the logical-operation seam.
    await tx.all(app.documents, { tier: "local" });
    const id = tx.openTransactionId();
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    source.client.prepareTransaction(id, async () => {
      await ready;
      source.client
        .getRuntime()
        .update(
          "documents",
          inserted.value.id,
          { payload: { type: "Bytea", value: new Uint8Array([1]) } },
          JSON.stringify({ transaction_id: id }),
        );
    });
    const payload = new Uint8Array([2]);
    if (operation === "delete") tx.delete(app.documents, inserted.value.id);
    else if (operation === "upsert") tx.upsert(app.documents, inserted.value.id, { payload });
    else tx.update(app.documents, inserted.value.id, { payload });
    payload[0] = 9;
    const reading = tx.all(app.documents, { tier: "local" });
    const committed = tx.commit();
    release();
    await committed.wait({ tier: "local" });
    const expected = operation === "delete" ? [] : [{ payload: new Uint8Array([2]) }];
    await expect(reading).resolves.toMatchObject(expected);
    await expect(db.all(app.documents, { tier: "local" })).resolves.toMatchObject(expected);
  },
);

it("queues restoration with defaults and isolates returned and input bytes", async () => {
  const original = await db
    .insert(app.documents, { payload: new Uint8Array([0]) })
    .wait({ tier: "local" });
  await db.delete(app.documents, original.id).wait({ tier: "local" });
  const tx = db.beginExclusiveTransaction();
  source.client.prepareTransaction(tx.openTransactionId(), async () => {});
  const payload = new Uint8Array([5]);
  const restored = tx.restore(app.documents, original.id, { payload });
  expect(restored.label).toBe("untitled");
  payload[0] = 8;
  restored.payload[0] = 7;
  await tx.commit().wait();
  await expect(
    db.one(app.documents.where({ id: original.id }), { tier: "local" }),
  ).resolves.toMatchObject({
    payload: new Uint8Array([5]),
    label: "untitled",
  });
});
