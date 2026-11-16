import { expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz } from "../testing/index.js";

it("keeps local reads available while another transaction waits for remote coverage", async () => {
  const app = testApp({ tables: { entries: { columns: { title: "string" } } } });
  const permissions = { format: 1, tables: { entries: { read: "true", insert: "true" } } };
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const initial = db.insert(app.entries, { title: "Available offline" });
    await initial.wait({ tier: "global" });
    expect(await db.all(app.entries, { tier: "remote" })).toEqual([initial.value]);
    await db.disconnect();
    const tx = db.beginSerializableTransaction();
    let remoteState = "pending";
    const remoteRead = tx.all(app.entries.where({ title: "Not cached" }), { tier: "remote" }).then(
      () => {
        remoteState = "ready";
      },
      () => {
        remoteState = "rejected";
      },
    );
    await new Promise((resolve) => setTimeout(resolve, 20));
    const rows = await Promise.race([
      db.all(app.entries, { tier: "local-first" }),
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
