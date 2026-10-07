import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession, type JazzClient } from "../backend/create-jazz-session.js";
import { createDb } from "../runtime/default-create-db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import type { JazzSession } from "../session/state.js";

it("reads authority-ordered WASM proposals through the native Node binding", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const app = s.defineApp({ proposals: s.table({ value: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.proposals.allowRead.where({ "$createdBy.account": session.user.account });
    policy.proposals.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.proposals.allowUpdate.never();
    policy.proposals.allowDelete.never();
  });
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let writer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    owner = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: "local-first",
    });
    const reader = owner.getSnapshot().client!.db;
    writer = await createDb({
      appId: server.appId,
      serverUrl: server.url,
      account: owner.getSnapshot().account!,
      driver: { type: "memory" },
    });
    const first = writer.insert(app.proposals, { value: "first" });
    await first.wait({ tier: "global" });
    const second = writer.insert(app.proposals, { value: "second" });
    await second.wait({ tier: "global" });
    const ordinary = await reader.all(app.proposals, { tier: "remote" });
    expect(ordinary).toHaveLength(2);
    const read = await reader.exclusiveTransaction((tx) => tx.allSettledForE2ee(app.proposals));
    const snapshot = await read.wait({ tier: "global" });
    expect(snapshot.rows).toEqual(ordinary);
    const byId = new Map(snapshot.settlements.map((entry) => [entry.rowId, entry]));
    expect(byId.size).toBe(2);
    const earlier = byId.get(first.value.id)!;
    const later = byId.get(second.value.id)!;
    expect(earlier.transactionId).not.toBe(later.transactionId);
    expect(BigInt(later.position)).toBeGreaterThan(BigInt(earlier.position));
  } finally {
    await writer?.shutdown();
    await owner?.close();
    await server.stop();
  }
}, 30_000);

it.each(["update", "upsert", "delete"] as const)(
  "keeps content settlement separate from a later %s",
  async (operation) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const app = s.defineApp({ proposals: s.table({ value: s.string() }, {}) });
    const permissions = definePermissions(app, ({ policy }) => {
      policy.proposals.allowRead.always();
      policy.proposals.allowInsert.always();
      policy.proposals.allowUpdate.always();
      policy.proposals.allowDelete.always();
    });
    let owner: JazzSession<JazzClient> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      owner = await createJazzSession({
        appId: server.appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: "local-first",
      });
      const db = owner.getSnapshot().client!.db;
      const original = db.insert(app.proposals, { value: "accepted content" });
      await original.wait({ tier: "global" });
      const baseline = await (
        await db.exclusiveTransaction((tx) => tx.allSettledForE2ee(app.proposals))
      ).wait({ tier: "global" });
      expect(baseline.rows).toEqual([original.value]);
      expect(baseline.settlements[0]!.transactionId).toBe(await original.txId);
      if (operation === "delete") {
        const deletion = db.delete(app.proposals, original.value.id);
        await deletion.wait({ tier: "global" });
        const snapshot = await (
          await db.exclusiveTransaction((tx) =>
            tx.allSettledForE2ee(app.proposals.includeDeleted()),
          )
        ).wait({ tier: "global" });
        expect(snapshot.rows).toEqual([original.value]);
        expect(snapshot.settlements).toEqual(baseline.settlements);
        expect(snapshot.settlements[0]!.transactionId).not.toBe(await deletion.txId);
      } else {
        const tx = db.beginExclusiveTransaction();
        try {
          tx[operation](app.proposals, original.value.id, { value: "provisional content" });
          expect(await tx.all(app.proposals, { tier: "local" })).toEqual([
            { id: original.value.id, value: "provisional content" },
          ]);
          await expect(tx.allSettledForE2ee(app.proposals)).rejects.toThrow();
        } finally {
          await tx.rollback();
        }
        const unchanged = await (
          await db.exclusiveTransaction((read) => read.allSettledForE2ee(app.proposals))
        ).wait({ tier: "global" });
        expect(unchanged).toEqual(baseline);
      }
    } finally {
      await owner?.close();
      await server.stop();
    }
  },
  30_000,
);
