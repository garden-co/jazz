import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

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
    const ordinary = await bob.all(app.proposals, { tier: "edge" });
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
