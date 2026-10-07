import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";
import type { Db } from "../../src/runtime/db.js";

it("preserves authority transaction positions through browser snapshot coverage", async () => {
  const server = await getJazzServerInfo(`e2ee-settlement-${crypto.randomUUID()}`);
  const app = s.defineApp({ proposals: s.table({ value: s.string() }, {}) });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      ...server,
      schema: app,
      permissions: definePermissions(app, ({ policy, session }) => {
        policy.proposals.allowRead.where({ "$createdBy.account": session.user.account });
        policy.proposals.allowInsert.where({ "$createdBy.account": session.user.account });
        policy.proposals.allowUpdate.never();
        policy.proposals.allowDelete.never();
      }),
    });
    const account = await acquireBrowserTestAccount(server);
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account,
      driver: { type: "memory" as const },
    };
    const alice = await createDb(config);
    const bob = await createDb(config);
    clients.push(alice, bob);
    const earlier = alice.insert(app.proposals, { value: "earlier" });
    await earlier.wait({ tier: "global" });
    const later = alice.insert(app.proposals, { value: "later" });
    await later.wait({ tier: "global" });
    const ordinary = await bob.all(app.proposals, { tier: "remote" });
    const read = await bob.exclusiveTransaction((tx) => tx.allSettledForE2ee(app.proposals));
    const { rows, settlements } = await read.wait({ tier: "global" });
    expect(rows).toEqual(ordinary);
    expect(settlements).toHaveLength(2);
    const first = settlements.find((item) => item.rowId === earlier.value.id)!;
    const second = settlements.find((item) => item.rowId === later.value.id)!;
    expect(first.transactionId).not.toBe(second.transactionId);
    expect(BigInt(second.position)).toBeGreaterThan(BigInt(first.position));
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await stopJazzServer(server.serverUrl);
  }
}, 30_000);

it.each(["upsert", "delete"] as const)(
  "keeps browser content settlement separate from a later %s",
  async (operation) => {
    const server = await getJazzServerInfo(`e2ee-content-${crypto.randomUUID()}`);
    const app = s.defineApp({ proposals: s.table({ value: s.string() }, {}) });
    let db: Db | undefined;
    try {
      await deploy({
        ...server,
        schema: app,
        permissions: definePermissions(app, ({ policy }) => {
          policy.proposals.allowRead.always();
          policy.proposals.allowInsert.always();
          policy.proposals.allowUpdate.always();
          policy.proposals.allowDelete.always();
        }),
      });
      db = await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        account: await acquireBrowserTestAccount(server),
        driver: { type: "memory" },
      });
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
      } else {
        const tx = db.beginExclusiveTransaction();
        try {
          tx.upsert(app.proposals, original.value.id, { value: "provisional content" });
          expect(await tx.all(app.proposals, { tier: "local" })).toEqual([
            { id: original.value.id, value: "provisional content" },
          ]);
          await expect(tx.allSettledForE2ee(app.proposals)).rejects.toThrow();
        } finally {
          await tx.rollback();
        }
      }
    } finally {
      await db?.shutdown();
      await stopJazzServer(server.serverUrl);
    }
  },
  30_000,
);
