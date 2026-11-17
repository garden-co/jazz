import { expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { createDb } from "./default-create-db.js";
import type { Db } from "./db.js";
import { DbClosedError, GracefulShutdownSyncError } from "./graceful-shutdown-error.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz } from "../testing/index.js";

const app = testApp({ tables: { notes: { columns: { title: "string" } } } });

const permissions = { format: 1, tables: { notes: { read: "true", insert: "true" } } };

async function withDb(run: (db: Db) => Promise<void>): Promise<void> {
  const server = await startTestJazz({ app, permissions, coreOptions: { inMemory: true } });
  let db: Db | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    await run(db);
  } finally {
    await db?.shutdown().catch(() => undefined);
    await server.stop();
  }
}

it("a failed graceful shutdown leaves the Db open, and a later shutdown closes it for good", async () => {
  await withDb(async (db) => {
    await db.disconnect();
    await db.insert(app.notes, { title: "offline" }).wait({ tier: "local" });

    // An offline sign-out: the drain waits for an upload that can't happen.
    const draining = db.shutdown({ waitForSync: true });
    // Draining takes no new work, but the Db may still reconnect to upload.
    expect(() => db.insert(app.notes, { title: "while draining" })).toThrow(DbClosedError);
    expect(db.isUsable()).toBe(false);
    // Another graceful shutdown asked for meanwhile shares the drain.
    expect(db.shutdown({ waitForSync: true })).toBe(draining);
    db.abortGracefulShutdown();
    await expect(draining).rejects.toBeInstanceOf(GracefulShutdownSyncError);

    // The shutdown never started: the Db is open, as it was.
    expect(db.isUsable()).toBe(true);
    await db.insert(app.notes, { title: "after the failed sign-out" }).wait({ tier: "local" });
    await db.reconnect();
    await db.waitForSync({ timeoutMs: 10_000 });

    // Now the shutdown goes through, and closed is final.
    await db.shutdown({ waitForSync: true });
    expect(db.isUsable()).toBe(false);
    expect(() => db.insert(app.notes, { title: "after close" })).toThrow(
      "Cannot operate on a Db that is shutting down or closed: it is closed.",
    );
    await expect(db.all(app.notes)).rejects.toBeInstanceOf(DbClosedError);
    await expect(db.waitForSync()).rejects.toBeInstanceOf(DbClosedError);
    await expect(db.reconnect()).rejects.toThrow("Cannot reconnect a Db that is shutting down.");
    await expect(db.disconnect()).rejects.toThrow("Cannot disconnect a Db that is shutting down.");
    // Shutting a closed Db down again is the same, settled shutdown.
    await expect(db.shutdown({ waitForSync: true })).resolves.toBeUndefined();
  });
});

it("reports an operation during teardown as closing, and the shutdown it shares", async () => {
  await withDb(async (db) => {
    const shutdown = db.shutdown();
    expect(() => db.insert(app.notes, { title: "during teardown" })).toThrow(
      "Cannot operate on a Db that is shutting down or closed: it is closing.",
    );
    // Teardown started: a graceful shutdown asked for now can't drain any more.
    expect(db.shutdown({ waitForSync: true })).toBe(shutdown);
    db.abortGracefulShutdown();
    await expect(shutdown).resolves.toBeUndefined();
  });
});

it("a plain shutdown during a drain that fails still ends closed", async () => {
  await withDb(async (db) => {
    await db.disconnect();
    await db.insert(app.notes, { title: "offline" }).wait({ tier: "local" });
    let observed = 0;

    // An offline sign-out drains; a provider unmounts meanwhile.
    const signOut = db.shutdown({ waitForSync: true });
    db.onShutdown(() => observed++);
    // Registered during the drain, it waits for the teardown.
    expect(observed).toBe(0);
    const unmount = db.shutdown();
    // A second graceful shutdown still shares the drain itself.
    expect(db.shutdown({ waitForSync: true })).toBe(signOut);
    db.abortGracefulShutdown();

    await expect(signOut).rejects.toBeInstanceOf(GracefulShutdownSyncError);
    // The drain's failure didn't leave the Db open: the plain shutdown closed it.
    expect(db.isUsable()).toBe(false);
    await expect(unmount).resolves.toBeUndefined();
    expect(observed).toBe(1);
    expect(() => db.insert(app.notes, { title: "after close" })).toThrow(
      "Cannot operate on a Db that is shutting down or closed: it is closed.",
    );
    await expect(db.reconnect()).rejects.toThrow("Cannot reconnect a Db that is shutting down.");
  });
});

it("a listener registered during a drain that fails stays armed for the later teardown", async () => {
  await withDb(async (db) => {
    await db.disconnect();
    await db.insert(app.notes, { title: "offline" }).wait({ tier: "local" });
    let observed = 0;
    const signOut = db.shutdown({ waitForSync: true });
    db.onShutdown(() => observed++);
    db.abortGracefulShutdown();
    await expect(signOut).rejects.toBeInstanceOf(GracefulShutdownSyncError);
    expect(observed).toBe(0);
    expect(db.isUsable()).toBe(true);
    await db.shutdown();
    expect(observed).toBe(1);
  });
});
