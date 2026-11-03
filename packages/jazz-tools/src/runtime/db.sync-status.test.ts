import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import { WriteWaitTimeoutError } from "./client.js";
import type { Db } from "./db.js";
import type { SyncStatus } from "./sync-status.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });

/** Resolve with the first status `matches` accepts, now or after a change. */
function statusWhere(db: Db, matches: (status: SyncStatus) => boolean): Promise<SyncStatus> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`status never matched; last ${JSON.stringify(db.getSyncStatus())}`)),
      15_000,
    );
    const check = (status: SyncStatus) => {
      if (!matches(status)) return;
      clearTimeout(timer);
      unsubscribe();
      resolve(status);
    };
    const unsubscribe = db.onSyncStatusChange(check);
    check(db.getSyncStatus());
  });
}

it("reports the link and pending uploads through a disconnect", async () => {
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
  });
  const server = await startLocalJazzServer({
    allowLocalFirstAuth: true,
    inMemory: true,
  });
  let db: Db | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    await db.insert(app.notes, { title: "online" }).wait({ tier: "global" });
    expect(await statusWhere(db, (status) => status.synced)).toEqual({
      link: { state: "connected" },
      pendingUploads: 0,
      synced: true,
    });
    // An unchanged status is the same object, as useSyncExternalStore needs.
    expect(db.getSyncStatus()).toBe(db.getSyncStatus());

    const seen: SyncStatus[] = [];
    const unsubscribe = db.onSyncStatusChange((status) => seen.push(status));

    await db.disconnect();
    await db.insert(app.notes, { title: "offline one" }).wait({ tier: "local" });
    await db.insert(app.notes, { title: "offline two" }).wait({ tier: "local" });
    expect(await statusWhere(db, (status) => status.pendingUploads === 2)).toEqual({
      link: { state: "offline", reason: "disconnected", retrying: false },
      pendingUploads: 2,
      synced: false,
    });
    await expect(db.waitForSync({ timeoutMs: 100 })).rejects.toBeInstanceOf(WriteWaitTimeoutError);
    let synced = false;
    const sync = db.waitForSync().then(() => {
      synced = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(synced).toBe(false);

    await db.reconnect();
    await sync;
    expect(await statusWhere(db, (status) => status.synced)).toEqual({
      link: { state: "connected" },
      pendingUploads: 0,
      synced: true,
    });
    unsubscribe();

    expect(seen.length).toBeGreaterThan(0);
    expect(seen.at(-1)).toEqual(expect.objectContaining({ synced: true }));
    expect(seen).toContainEqual(
      expect.objectContaining({
        link: { state: "offline", reason: "disconnected", retrying: false },
      }),
    );
    for (let index = 1; index < seen.length; index += 1) {
      expect(seen[index]).not.toEqual(seen[index - 1]);
    }
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it("is local-only and synced without a server", async () => {
  const db = await createDb(await localAccountConfig("sync-status-local"));
  try {
    await db.insert(app.notes, { title: "here" }).wait({ tier: "local" });
    expect(db.getSyncStatus()).toEqual({
      link: { state: "local-only" },
      pendingUploads: 0,
      synced: true,
    });
    await db.waitForSync();
  } finally {
    await db.shutdown();
  }
});

it("rejects an invalid timeout before waiting", async () => {
  const db = await createDb(await localAccountConfig("sync-status-timeout"));
  try {
    await expect(db.waitForSync({ timeoutMs: -1 })).rejects.toThrow(TypeError);
    const aborted = new AbortController();
    aborted.abort(new Error("gave up"));
    await expect(db.waitForSync({ signal: aborted.signal })).rejects.toThrow("gave up");
  } finally {
    await db.shutdown();
  }
});

it("ends a pending wait when the Db shuts down", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const db = await createDb(await localAccountConfig(server.appId, server.url));
  try {
    await db.disconnect();
    await db.insert(app.notes, { title: "never sent" }).wait({ tier: "local" });
    const wait = db.waitForSync();
    wait.catch(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await db.shutdown();
    await expect(wait).rejects.toThrow();
  } finally {
    await server.stop();
  }
}, 60_000);
