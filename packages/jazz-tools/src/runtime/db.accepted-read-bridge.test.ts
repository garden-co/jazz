import { expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import {
  beginDbTransactionAfter,
  readAcceptedTransactionRows,
  prepareDbTransaction,
} from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz } from "../testing/index.js";
import { createAccountDbWithRuntimeSource } from "../accounts/context.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { RuntimeClientContext } from "./runtime-source.js";
import type { JazzClient } from "./client.js";

const app = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
const permissions = definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
  policy.notes.allowDelete.always();
});
class PreparationSource extends DefaultRuntimeSource {
  client!: JazzClient;
  override createClient(context: RuntimeClientContext): JazzClient {
    this.client = super.createClient(context);
    return this.client;
  }
}

it("opts in before opening, drains an immediate SDK commit and preserves paired typed evidence", async () => {
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let release!: () => void;
  try {
    // startTestJazz publishes the app and admits clients through its normal backend.
    const source = new PreparationSource();
    db = await createAccountDbWithRuntimeSource(
      await localAccountConfig(server.appId, server.url),
      source,
    );
    const inserted = db.insert(app.notes, { body: "accepted content" });
    await inserted.wait({ tier: "global" });
    const opening = vi.spyOn(source.client.getRuntime(), "beginTransaction");
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tx = beginDbTransactionAfter(db, () => ready, "accepted-evidence");
    const reading = readAcceptedTransactionRows(tx, app.notes, 5000);
    const committed = tx.commit();
    const global = committed.wait({ tier: "global", timeoutMs: 10000 });
    global.catch(() => {});
    reading.catch(() => {});
    expect(opening).not.toHaveBeenCalled();
    release();
    const [result] = await Promise.all([reading, global]);
    expect(opening).toHaveBeenCalledTimes(1);
    expect(opening.mock.calls[0]?.[3]).toBe("accepted-evidence");
    expect(opening.mock.calls[0]?.[2]).toBeUndefined();
    expect(result.rows).toEqual([inserted.value]);
    expect(result.evidence).toHaveLength(1);
    expect(result.evidence[0]?.rowId).toBe(inserted.value.id);
    expect(result.evidence[0]?.content.transactionId).toBe(await inserted.txId);
    expect(result.evidence[0]?.state).toEqual(result.evidence[0]?.content);
    expect(BigInt(result.through)).toBeGreaterThanOrEqual(
      BigInt(result.evidence[0]!.state.position),
    );
    opening.mockRestore();

    const deletion = db.delete(app.notes, inserted.value.id);
    await deletion.wait({ tier: "global" });
    const deleted = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    const snapshot = await readAcceptedTransactionRows(deleted, app.notes.includeDeleted(), 5000);
    await deleted.commit().wait({ tier: "global" });
    expect(snapshot.rows).toEqual([inserted.value]);
    expect(snapshot.evidence[0]?.content).toEqual(result.evidence[0]?.content);
    expect(snapshot.evidence[0]?.state.transactionId).toBe(await deletion.txId);
    expect(BigInt(snapshot.evidence[0]!.state.position)).toBeGreaterThan(
      BigInt(snapshot.evidence[0]!.content.position),
    );

    const ordinary = db.beginSerializableTransaction();
    await expect(readAcceptedTransactionRows(ordinary, app.notes, 1000)).rejects.toThrow(
      /before transaction opening/,
    );
    expect(await ordinary.all(app.notes, { tier: "local-first" })).toEqual([]);
    await ordinary.rollback();
  } finally {
    release?.();
    vi.restoreAllMocks();
    await db?.shutdown();
    await server.stop();
  }
}, 60000);

it("records an empty accepted predicate and refuses a phantom at Global commit", async () => {
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    // startTestJazz publishes the app and admits clients through its normal backend.
    db = await createDb(await localAccountConfig(server.appId, server.url));
    await db.all(app.notes, { tier: "remote" });
    const tx = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    const snapshot = await readAcceptedTransactionRows(
      tx,
      app.notes.where({ body: "phantom" }),
      5000,
    );
    expect(snapshot.rows).toEqual([]);
    expect(snapshot.evidence).toEqual([]);
    const concurrent = await db.insert(app.notes, { body: "phantom" }).wait({ tier: "global" });
    tx.insert(app.notes, { body: "must not publish" });
    await expect(tx.commit().wait({ tier: "global" })).rejects.toThrow();
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([concurrent]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60000);

it("uses current policies after a retained accepted read and never publishes its staged write", async () => {
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    // The same app backend is redeployed below; clients retain their origin.
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const initial = await db
      .insert(app.notes, { body: "before revocation" })
      .wait({ tier: "global" });
    const tx = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    expect((await readAcceptedTransactionRows(tx, app.notes, 5000)).rows).toEqual([initial]);
    tx.insert(app.notes, { body: "must not publish" });
    const denied = definePermissions(app, ({ policy }) => {
      policy.notes.allowInsert.always();
    });
    await server.redeploy({ app, permissions: denied });
    await expect(tx.commit().wait({ tier: "global" })).rejects.toThrow();
    await server.redeploy({ app, permissions });
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([initial]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60000);

it("keeps typed accepted preparation inside an immediate commit and preserves the evidence envelope", async () => {
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let release!: () => void;
  try {
    // startTestJazz publishes the app and admits clients through its normal backend.
    const source = new PreparationSource();
    db = await createAccountDbWithRuntimeSource(
      await localAccountConfig(server.appId, server.url),
      source,
    );
    const original = db.insert(app.notes, { body: "covered authority content" });
    await original.wait({ tier: "global" });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const tx = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    const nativeCommit = vi.spyOn(source.client.getRuntime(), "commitTransaction");
    let observed: Awaited<ReturnType<typeof readAcceptedTransactionRows>> | undefined;
    const preparing = prepareDbTransaction(tx, async (scope) => {
      observed = await scope.readAccepted(app.notes);
      entered();
      await gate;
      scope.insert(app.notes, { body: "prepared in the same transaction" });
    });
    const committed = tx.commit().wait({ tier: "global", timeoutMs: 10000 });
    preparing.catch(() => {});
    committed.catch(() => {});
    await started;
    expect(nativeCommit).not.toHaveBeenCalled();
    expect(() => prepareDbTransaction(tx, async () => {})).toThrow("commit has been requested");
    expect(observed?.rows).toEqual([original.value]);
    expect(observed?.evidence[0]?.content.transactionId).toBe(await original.txId);
    expect(observed?.evidence[0]?.state).toEqual(observed?.evidence[0]?.content);
    expect(BigInt(observed!.through)).toBeGreaterThanOrEqual(
      BigInt(observed!.evidence[0]!.state.position),
    );
    release();
    await Promise.all([preparing, committed]);
    expect(nativeCommit).toHaveBeenCalledOnce();
    expect((await db.all(app.notes, { tier: "remote" })).map((row) => row.body).sort()).toEqual([
      "covered authority content",
      "prepared in the same transaction",
    ]);
  } finally {
    release?.();
    vi.restoreAllMocks();
    await db?.shutdown();
    await server.stop();
  }
}, 60000);

it("does not upgrade ordinary transactions through a registered accepted preparation", async () => {
  const source = new PreparationSource();
  const db = await createAccountDbWithRuntimeSource(
    await localAccountConfig(`prepared-capability-${crypto.randomUUID()}`),
    source,
  );
  try {
    await db.all(app.notes, { tier: "local-first" });
    const tx = db.beginSerializableTransaction();
    const preparing = prepareDbTransaction(tx, async (scope) => {
      await scope.readAccepted(app.notes);
    });
    preparing.catch(() => {});
    await expect(preparing).rejects.toThrow("before transaction opening");
    await expect(tx.commit().txId).rejects.toThrow("before transaction opening");
    await tx.rollback();
  } finally {
    await db.shutdown();
  }
});
