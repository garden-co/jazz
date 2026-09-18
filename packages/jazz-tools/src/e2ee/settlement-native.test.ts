import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { createDb } from "../runtime/default-create-db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

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
    const ordinary = await reader.all(app.proposals, { tier: "edge" });
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
