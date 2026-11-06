import { expect, it } from "vitest";
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
import { startTestJazz } from "../testing/index.js";
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
