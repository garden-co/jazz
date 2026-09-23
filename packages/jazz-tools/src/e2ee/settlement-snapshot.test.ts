import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { PersistedWriteRejectedError } from "../runtime/client.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

const app = s.defineApp({ proposals: s.table({ value: s.string() }, {}) });

// Alice publishes two rows atomically, then a later proposal. Bob must recover
// authority transaction order, not row-ID order, from an accepted snapshot.
it.each(["warm", "cold", "concurrent", "immediate commit"])(
  "checks %s exclusive E2EE snapshot coverage and authority order",
  async (scenario) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: definePermissions(app, ({ policy, session }) => {
          policy.proposals.allowRead.where({ "$createdBy.account": session.user.account });
          policy.proposals.allowInsert.where({ "$createdBy.account": session.user.account });
          policy.proposals.allowUpdate.never();
          policy.proposals.allowDelete.never();
        }),
      });
      const account = await localAccountConfig(server.appId, server.url);
      const alice = await createDb(account);
      const bob = await createDb(account);
      clients.push(alice, bob);
      const firstId = "ffffffff-ffff-4fff-8fff-ffffffffffff";
      const siblingId = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
      const laterId = "00000000-0000-4000-8000-000000000001";
      const first = alice.beginTransaction();
      first.insert(app.proposals, { value: "first" }, { id: firstId });
      first.insert(app.proposals, { value: "same transaction" }, { id: siblingId });
      await first.commit().wait({ tier: "global" });
      await alice
        .insert(app.proposals, { value: "later" }, { id: laterId })
        .wait({ tier: "global" });
      // Initialise Bob's runtime without hydrating the proposal history.
      if (scenario === "cold")
        expect(
          await bob.all(app.proposals.where({ id: "00000000-0000-4000-8000-000000000002" }), {
            tier: "edge",
          }),
        ).toEqual([]);
      const ordinary =
        scenario === "cold" ? undefined : await bob.all(app.proposals, { tier: "edge" });
      if (scenario === "immediate commit") {
        const tx = bob.beginExclusiveTransaction();
        const reading = tx.allSettledForE2ee(app.proposals);
        const committed = tx.commit();
        const [snapshot] = await Promise.all([reading, committed.wait({ tier: "global" })]);
        expect(snapshot.rows).toEqual(ordinary);
        expect(snapshot.settlements).toHaveLength(3);
        return;
      }
      const read = await bob.exclusiveTransaction(async (tx) => {
        const snapshot = await tx.allSettledForE2ee(app.proposals);
        if (scenario === "concurrent")
          await alice.insert(app.proposals, { value: "after snapshot" }).wait({ tier: "global" });
        return snapshot;
      });
      if (scenario === "concurrent") {
        await expect(read.wait({ tier: "global" })).rejects.toMatchObject({
          code: "exclusive_conflict",
        });
        return;
      }
      let snapshot;
      try {
        snapshot = await read.wait({ tier: "global" });
      } catch (error) {
        // A cold client may lack authority coverage. Refusal is safe; accepting
        // an empty or partial history would let a valid approval be omitted.
        if (scenario !== "cold") throw error;
        expect(error).toBeInstanceOf(PersistedWriteRejectedError);
        expect(error).toMatchObject({ code: "exclusive_conflict" });
        return;
      }
      expect(snapshot.rows).toHaveLength(3);
      if (ordinary) expect(snapshot.rows).toEqual(ordinary);
      expect(snapshot.settlements).toHaveLength(3);
      const byId = new Map(snapshot.settlements.map((entry) => [entry.rowId, entry]));
      const firstSettlement = byId.get(firstId)!;
      const sibling = byId.get(siblingId)!;
      const later = byId.get(laterId)!;
      expect(firstSettlement.transactionId).toBe(sibling.transactionId);
      expect(firstSettlement.position).toBe(sibling.position);
      expect(later.transactionId).not.toBe(firstSettlement.transactionId);
      expect(BigInt(later.position)).toBeGreaterThan(BigInt(firstSettlement.position));
      expect(await bob.all(app.proposals, { tier: "edge" })).toEqual(snapshot.rows);
    } finally {
      await Promise.all(clients.map((db) => db.shutdown()));
      await server.stop();
    }
  },
  30_000,
);
