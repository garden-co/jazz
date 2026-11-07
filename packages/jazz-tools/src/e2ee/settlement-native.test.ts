import { frontE2eeCore } from "./fixtures/app.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import {
  beginDbTransactionAfter,
  readAcceptedTransactionRows,
  type Db,
  type QueryBuilder,
} from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer, startTestJazz } from "../testing/index.js";
import { nativeCoreErrorCode } from "../runtime/native-runtime/native-error-code.js";

async function fixture(ownOnly = false) {
  const app = s.defineApp(
    { proposals: s.table({ value: s.string() }, {}) },
    { migrations: "none" },
  );
  const permissions = definePermissions(app, ({ policy, session }) => {
    if (ownOnly) {
      policy.proposals.allowRead.where({ "$createdBy.account": session.user.account });
      policy.proposals.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.proposals.allowUpdate.never();
      policy.proposals.allowDelete.never();
    } else {
      policy.proposals.allowRead.always();
      policy.proposals.allowInsert.always();
      policy.proposals.allowUpdate.always();
      policy.proposals.allowDelete.always();
    }
  });
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  const config = await localAccountConfig(server.appId, server.url);
  const db = await createDb(config);
  return { app, server, config, db };
}

async function accepted<T>(db: Db, query: QueryBuilder<T>) {
  const tx = beginDbTransactionAfter(db, async () => {}, "client-accepted-evidence");
  let commitAttempted = false;
  try {
    const snapshot = await readAcceptedTransactionRows(tx, query, 10_000);
    commitAttempted = true;
    await tx.commit().wait({ tier: "global", timeoutMs: 10_000 });
    return snapshot;
  } finally {
    if (!commitAttempted) await tx.rollback();
  }
}

it("reads authority-ordered E2EE proposals through the public native SDK", async () => {
  const { app, server, config, db: reader } = await fixture(true);
  let writer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    writer = await createDb(config);
    const first = writer.insert(app.proposals, { value: "first" });
    await first.wait({ tier: "global" });
    const second = writer.insert(app.proposals, { value: "second" });
    await second.wait({ tier: "global" });
    const ordinary = await reader.all(app.proposals, { tier: "remote" });
    expect(ordinary).toHaveLength(2);
    const snapshot = await accepted(reader, app.proposals);
    expect(snapshot.rows).toEqual(ordinary);
    const byId = new Map(snapshot.evidence.map((entry) => [entry.rowId, entry]));
    expect(byId.size).toBe(2);
    const earlier = byId.get(first.value.id)!;
    const later = byId.get(second.value.id)!;
    expect(earlier.content.transactionId).toBe(await first.txId);
    expect(later.content.transactionId).toBe(await second.txId);
    expect(earlier.state).toEqual(earlier.content);
    expect(later.state).toEqual(later.content);
    expect(later.content.transactionId).not.toBe(earlier.content.transactionId);
    expect(BigInt(later.content.position)).toBeGreaterThan(BigInt(earlier.content.position));
  } finally {
    await writer?.shutdown();
    await reader.shutdown();
    await server.stop();
  }
}, 60_000);

it.each(["update", "upsert", "delete"] as const)(
  "keeps accepted content origin separate from a later %s",
  async (operation) => {
    const { app, server, db } = await fixture();
    try {
      const original = db.insert(app.proposals, { value: "accepted content" });
      await original.wait({ tier: "global" });
      const baseline = await accepted(db, app.proposals);
      expect(baseline.rows).toEqual([original.value]);
      expect(baseline.evidence[0]!.content.transactionId).toBe(await original.txId);
      if (operation === "delete") {
        const deletion = db.delete(app.proposals, original.value.id);
        await deletion.wait({ tier: "global" });
        const snapshot = await accepted(db, app.proposals.includeDeleted());
        expect(snapshot.rows).toEqual([original.value]);
        expect(snapshot.evidence[0]!.content).toEqual(baseline.evidence[0]!.content);
        expect(snapshot.evidence[0]!.state.transactionId).toBe(await deletion.txId);
        expect(snapshot.evidence[0]!.content.transactionId).not.toBe(await deletion.txId);
      } else {
        const tx = beginDbTransactionAfter(db, async () => {}, "client-accepted-evidence");
        try {
          if (operation === "update")
            tx.update(app.proposals, original.value.id, { value: "provisional content" });
          else tx.upsert(app.proposals, original.value.id, { value: "provisional content" });
          expect(await tx.all(app.proposals, { tier: "local-first" })).toEqual([
            { id: original.value.id, value: "provisional content" },
          ]);
          const failure: unknown = await readAcceptedTransactionRows(
            tx,
            app.proposals,
            10_000,
          ).then(
            () => undefined,
            (error: unknown) => error,
          );
          expect(nativeCoreErrorCode(failure)).toBe("not_observed");
          expect(String(failure)).toContain(
            "accepted reads cannot substitute already staged content",
          );
        } finally {
          await tx.rollback();
        }
        const unchanged = await accepted(db, app.proposals);
        expect(unchanged.rows).toEqual(baseline.rows);
        expect(unchanged.evidence).toEqual(baseline.evidence);
      }
    } finally {
      await db.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("observes cold accepted native metadata before any ordinary read", async () => {
  let server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const directory = await mkdtemp(join(tmpdir(), "jazz-native-observation-"));
  const app = s.defineApp(
    {
      __e2ee_snapshot_roots: s.table({ title: s.string() }, {}),
      __e2ee_snapshot_refs: s.table(
        { rootId: s.uuid() },
        { root: s.rel("__e2ee_snapshot_roots", "rootId") },
      ),
    },
    { migrations: "none" },
  );
  const permissions = definePermissions(app, ({ policy }) => {
    policy.__e2ee_snapshot_roots.allowRead.always();
    policy.__e2ee_snapshot_roots.allowInsert.always();
    policy.__e2ee_snapshot_roots.allowUpdate.always();
    policy.__e2ee_snapshot_refs.allowRead.always();
    policy.__e2ee_snapshot_refs.allowInsert.always();
    policy.__e2ee_snapshot_refs.allowDelete.always();
  });
  let saved: string | null = null;
  const config = {
    appId: server.appId,
    serverUrl: server.url,
    app,
    permissions,
    driver: { type: "persistent" as const, dataPath: join(directory, "database") },
    initial: "local-first" as const,
    store: {
      async read() {
        return saved;
      },
      async update(transform: (value: string | null) => string) {
        saved = transform(saved);
      },
    },
  };
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    server = await frontE2eeCore(server, { schema: app, permissions });
    config.serverUrl = server.url;
    owner = await createJazzSession(config);
    const before = owner.getSnapshot().client!.db;
    await before.all(app.__e2ee_snapshot_roots, { tier: "local-first" });
    const tx = before.beginSerializableTransaction();
    const root = tx.insert(app.__e2ee_snapshot_roots, { title: "Accepted" });
    const ref = tx.insert(app.__e2ee_snapshot_refs, { rootId: root.id });
    const commit = tx.commit();
    await commit.wait({ tier: "global" });
    const transactionId = await commit.txId;
    await owner.close();
    owner = undefined;
    await server.stop();
    owner = await createJazzSession(config);
    const reopened = owner.getSnapshot().client!.db;
    await reopened.disconnect();
    const roots = app.__e2ee_snapshot_roots.where({});
    const refs = app.__e2ee_snapshot_refs.where({ rootId: root.id });
    // First application query after reopening: no ordinary read warms the inputs.
    const observed = await reopened.observeE2eeHistory([roots, refs]);
    expect(observed.map(({ rows }) => rows)).toEqual([[root], [ref]]);
    expect(observed.map(({ settlements }) => settlements)).toEqual([
      [
        {
          rowId: root.id,
          state: { transactionId, position: expect.any(String) },
          content: { transactionId, position: expect.any(String) },
        },
      ],
      [
        {
          rowId: ref.id,
          state: { transactionId, position: expect.any(String) },
          content: { transactionId, position: expect.any(String) },
        },
      ],
    ]);
    expect(observed[0]!.settlements[0]!.content.position).toBe(
      observed[1]!.settlements[0]!.content.position,
    );

    const otherApp = s.defineApp(
      { __e2ee_other_roots: s.table({ title: s.string() }, {}) },
      { migrations: "none" },
    );
    await expect(reopened.observeE2eeHistory([roots, otherApp.__e2ee_other_roots])).rejects.toThrow(
      "Db is already initialized with a different schema",
    );

    const pending = reopened.beginSerializableTransaction();
    pending.update(app.__e2ee_snapshot_roots, root.id, { title: "Pending" });
    const added = pending.insert(app.__e2ee_snapshot_roots, { title: "Not accepted" });
    pending.delete(app.__e2ee_snapshot_refs, ref.id);
    await pending.commit().wait({ tier: "local" });
    expect(await reopened.all(roots, { tier: "local-first" })).toEqual(
      expect.arrayContaining([{ ...root, title: "Pending" }, added]),
    );
    expect(await reopened.all(refs, { tier: "local-first" })).toEqual([]);
    expect(await reopened.observeE2eeHistory([roots, refs])).toEqual(observed);
  } finally {
    await owner?.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
}, 30_000);
