import { expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import { beginDbTransactionAfter, readAcceptedTransactionRows } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer, startTestJazz } from "../testing/index.js";
import { createAccountDbWithRuntimeSource } from "../accounts/context.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { RuntimeClientContext } from "./runtime-source.js";
import { PersistedWriteRejectedError, type JazzClient } from "./client.js";

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
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let release!: () => void;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
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
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
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

it("revalidates live grant support under one fixed bundle and never publishes its staged write", async () => {
  // This replaces the old global policy-definition activation scenario.
  // The deployment, session and INSERT policy stay fixed; only grant data changes.
  const app = s.defineApp(
    {
      notes: s.table({ body: s.string() }, {}),
      grants: s.table({ enabled: s.boolean() }, {}),
    },
    { migrations: "none" },
  );
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.where(() => policy.grants.exists.where({ enabled: true }));
    policy.notes.allowInsert.always();
    policy.notes.allowDelete.always();
    policy.grants.allowRead.always();
    policy.grants.allowInsert.always();
    policy.grants.allowUpdate.always();
  });
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  const closed = vi.spyOn(server.jazz, "close");
  const redeployed = vi.spyOn(server, "redeploy");
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const grant = await server.jazz.db
      .insert(app.grants, { enabled: true })
      .wait({ tier: "global" });
    const initial = await db
      .insert(app.notes, { body: "before revocation" })
      .wait({ tier: "global" });
    const session = structuredClone(db.getAuthState());

    // The same admitted accepted-read/write path succeeds while support is live.
    const control = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    expect((await readAcceptedTransactionRows(control, app.notes, 5000)).rows).toEqual([initial]);
    const controlRow = control.insert(app.notes, { body: "stable support control" });
    await control.commit().wait({ tier: "global", timeoutMs: 10000 });
    await db.delete(app.notes, controlRow.id).wait({ tier: "global" });

    const tx = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    const accepted = await readAcceptedTransactionRows(tx, app.notes, 5000);
    expect(accepted.rows).toEqual([initial]);
    expect(accepted.evidence).toHaveLength(1);
    expect(accepted.evidence[0]?.rowId).toBe(initial.id);
    expect(accepted.evidence[0]?.state).toEqual(accepted.evidence[0]?.content);
    tx.insert(app.notes, { body: "must not publish" });

    // A separate normal owner changes only permission support, at Global.
    await server.jazz.db.update(app.grants, grant.id, { enabled: false }).wait({ tier: "global" });
    const committed = tx.commit().wait({ tier: "global", timeoutMs: 10000 });
    await expect(committed).rejects.toThrow();
    const error: unknown = await committed.catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(PersistedWriteRejectedError);
    expect(error).toMatchObject({ code: "serialization_conflict" });

    // A denied SELECT cannot hide a published marker from the real backend.
    expect(await server.jazz.db.all(app.notes)).toEqual([initial]);
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([]);

    // The same client still has a live accepted-read and Global-write service.
    await db.insert(app.grants, { enabled: false }).wait({ tier: "global" });
    const alive = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    const liveGrant = await readAcceptedTransactionRows(
      alive,
      app.grants.where({ id: grant.id }),
      5000,
    );
    expect(liveGrant.rows).toEqual([{ ...grant, enabled: false }]);
    await alive.commit().wait({ tier: "global", timeoutMs: 10000 });

    await server.jazz.db.update(app.grants, grant.id, { enabled: true }).wait({ tier: "global" });
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([initial]);
    const restored = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
    const unchanged = await readAcceptedTransactionRows(restored, app.notes, 5000);
    expect(unchanged.rows).toEqual([initial]);
    expect(unchanged.evidence).toEqual(accepted.evidence);
    await restored.commit().wait({ tier: "global", timeoutMs: 10000 });
    expect(db.getAuthState()).toEqual(session);
    expect(closed).not.toHaveBeenCalled();
    expect(redeployed).not.toHaveBeenCalled();
  } finally {
    closed.mockRestore();
    redeployed.mockRestore();
    await db?.shutdown();
    await server.stop();
  }
}, 60000);
