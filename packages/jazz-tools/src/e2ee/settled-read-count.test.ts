import { expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

// Count actual adapter calls without replacing their behaviour. This internal
// performance contract is not observable in the product's returned row values.
it("captures non-empty E2EE history with one covered query", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const app = s.defineApp({ proposals: s.table({ value: s.string() }, {}) });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
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
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const inserted = db.insert(app.proposals, { value: "accepted" });
    await inserted.wait({ tier: "global" });
    const queries = vi.spyOn(NativeRuntimeAdapter.prototype, "query");
    try {
      const read = await db.exclusiveTransaction((tx) => tx.allSettledForE2ee(app.proposals));
      const snapshot = await read.wait({ tier: "global" });
      expect(snapshot.rows).toEqual([{ id: inserted.value.id, value: "accepted" }]);
      expect(snapshot.settlements).toEqual([
        {
          rowId: inserted.value.id,
          transactionId: expect.any(String),
          position: expect.stringMatching(/^\d+$/),
        },
      ]);
      expect(queries).toHaveBeenCalledTimes(1);
      expect(queries.mock.calls[0]![2]).toBe("global");
    } finally {
      queries.mockRestore();
    }
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);
