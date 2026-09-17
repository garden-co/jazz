import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("keeps local reads available while another transaction waits for remote coverage", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const app = s.defineApp({ entries: s.table({ title: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.entries.allowRead.always();
    policy.entries.allowInsert.always();
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const initial = db.insert(app.entries, { title: "Available offline" });
    await initial.wait({ tier: "global" });
    expect(await db.all(app.entries, { tier: "global" })).toEqual([initial.value]);
    await db.disconnect();
    const tx = db.beginExclusiveTransaction();
    let remoteState = "pending";
    const remoteRead = tx.all(app.entries.where({ title: "Not cached" }), { tier: "global" }).then(
      () => {
        remoteState = "ready";
      },
      () => {
        remoteState = "rejected";
      },
    );
    await new Promise((resolve) => setTimeout(resolve, 20));
    const rows = await Promise.race([
      db.all(app.entries, { tier: "local" }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Local read waited for remote coverage")), 5_000);
      }),
    ]);
    clearTimeout(timer);
    expect(rows).toEqual([initial.value]);
    expect(remoteState).toBe("pending");
    await db.reconnect();
    await remoteRead;
    expect(remoteState).toBe("ready");
    await tx.rollback();
  } finally {
    clearTimeout(timer);
    await db?.reconnect();
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
