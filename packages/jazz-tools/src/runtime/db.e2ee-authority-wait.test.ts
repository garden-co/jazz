import { expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

const app = s.defineApp({ epochs: s.table({ generation: s.int() }, {}) });

it("confirms exclusive writes at the global authority and preserves callback results", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: definePermissions(app, ({ policy, session }) => {
        policy.epochs.allowRead.where({ "$createdBy.account": session.user.account });
        policy.epochs.allowInsert.where({ "$createdBy.account": session.user.account });
        policy.epochs.allowUpdate.never();
        policy.epochs.allowDelete.never();
      }),
    });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    await db.all(app.epochs, { tier: "edge" });
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.epochs, { generation: 1 });
    await expect(tx.commit().wait({ tier: "global" })).resolves.toBeUndefined();
    const result = await db.exclusiveTransaction((scope) =>
      scope.insert(app.epochs, { generation: 2 }),
    );
    await expect(result.wait({ tier: "global" })).resolves.toMatchObject({ generation: 2 });
    await expect(result.mapValue((row) => row.generation).wait({ tier: "global" })).resolves.toBe(
      2,
    );
    await expect(db.all(app.epochs, { tier: "edge" })).resolves.toHaveLength(2);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
});

it.each(["explicit", "callback", "mapped"] as const)(
  "%s exclusive handle can require global confirmation instead of local durability",
  async (kind) => {
    const db = await createDb(await localAccountConfig(`e2ee-wait-${crypto.randomUUID()}`));
    try {
      await db.all(app.epochs, { tier: "local" });
      const handle = await (async () => {
        if (kind === "explicit") {
          const tx = db.beginExclusiveTransaction();
          tx.insert(app.epochs, { generation: 1 });
          return tx.commit();
        }
        const result = await db.exclusiveTransaction((tx) =>
          tx.insert(app.epochs, { generation: 1 }),
        );
        return kind === "mapped" ? result.mapValue((row) => row.generation) : result;
      })();
      let outcome = "pending";
      handle.wait({ tier: "global" }).then(
        () => {
          outcome = "confirmed";
        },
        () => {
          outcome = "rejected";
        },
      );
      // Preserve the existing offline wait behaviour; it must not satisfy the
      // separate global wait used before E2EE releases a new epoch.
      await handle.wait();
      await expect(db.all(app.epochs, { tier: "local" })).resolves.toHaveLength(1);
      expect(outcome).toBe("pending");
    } finally {
      await db.shutdown();
    }
  },
);
