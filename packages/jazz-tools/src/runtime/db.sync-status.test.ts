import { connect, createServer, type Socket } from "node:net";
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import { WriteWaitTimeoutError } from "./client.js";
import type { Db } from "./db.js";
import type { SyncStatus } from "./sync-status.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startLocalJazzServer, startTestJazz } from "../testing/index.js";

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });

/** Resolve with the first status `matches` accepts, now or after a change. */
function statusWhere(
  db: Db,
  matches: (status: SyncStatus) => boolean,
  timeoutMs = 15_000,
): Promise<SyncStatus> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`status never matched; last ${JSON.stringify(db.getSyncStatus())}`)),
      timeoutMs,
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
  const server = await startTestJazz({
    app: app,
    permissions: permissions,
    coreOptions: {
      inMemory: true,
    },
  });
  let db: Db | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    await db.insert(app.notes, { title: "online" }).wait({ tier: "global" });
    expect(await statusWhere(db, (status) => status.synced)).toEqual({
      link: { state: "connected" },
      pendingUploads: 0,
      oldestPendingAt: null,
      rejected: 0,
      lastError: null,
      synced: true,
    });
    // An unchanged status is the same object, as useSyncExternalStore needs.
    expect(db.getSyncStatus()).toBe(db.getSyncStatus());

    const seen: SyncStatus[] = [];
    const unsubscribe = db.onSyncStatusChange((status) => seen.push(status));

    await db.disconnect();
    await db.insert(app.notes, { title: "offline one" }).wait({ tier: "local" });
    await db.insert(app.notes, { title: "offline two" }).wait({ tier: "local" });
    const offline = await statusWhere(db, (status) => status.pendingUploads === 2);
    expect(offline).toEqual({
      link: { state: "offline", reason: "disconnected", retrying: false },
      pendingUploads: 2,
      oldestPendingAt: expect.any(Number),
      rejected: 0,
      // Disconnecting on purpose is not an error.
      lastError: null,
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
      oldestPendingAt: null,
      rejected: 0,
      lastError: null,
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

it("says since when a write waits, what the server rejected, and when the link failed", async () => {
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.where({ title: "allowed" });
  });
  const server = await startTestJazz({
    app: app,
    permissions: permissions,
    coreOptions: { inMemory: true },
  });
  let db: Db | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    await db.insert(app.notes, { title: "allowed" }).wait({ tier: "global" });

    await db.disconnect();
    const before = Date.now();
    await db.insert(app.notes, { title: "forbidden" }).wait({ tier: "local" });
    const waiting = await statusWhere(db, (status) => status.pendingUploads === 1);
    expect(waiting.oldestPendingAt).toBeGreaterThanOrEqual(before - 5);
    expect(waiting.oldestPendingAt).toBeLessThanOrEqual(Date.now());
    expect(waiting.rejected).toBe(0);

    // The status counts a rejection whether or not a wait or a
    // mutation-error listener took it.
    await db.reconnect();
    const refused = await statusWhere(db, (status) => status.rejected === 1);
    expect(refused).toMatchObject({
      link: { state: "connected" },
      pendingUploads: 0,
      oldestPendingAt: null,
      rejected: 1,
      lastError: { kind: "rejected", code: "permission_denied" },
      synced: true,
    });
    const rejectedAt = (refused.lastError as { at: number }).at;
    expect(rejectedAt).toBeGreaterThanOrEqual(waiting.oldestPendingAt!);

    // A write the server accepts leaves the count and the last error alone.
    await db.insert(app.notes, { title: "allowed" }).wait({ tier: "global" });
    expect(db.getSyncStatus()).toMatchObject({ rejected: 1, lastError: { kind: "rejected" } });

    // The server going away is the newest error, and stays after the outage is noticed.
    await server.stop();
    const down = await statusWhere(db, (status) => status.lastError?.kind === "network");
    expect(down.link).toMatchObject({ state: "offline", reason: "network" });
    expect(down.lastError!.at).toBeGreaterThanOrEqual(rejectedAt);
    expect(down.rejected).toBe(1);
  } finally {
    await db?.shutdown();
    await server.stop().catch(() => undefined);
  }
}, 90_000);

// #5225: a server that takes the connection and never answers (a dev proxy
// holding the upgrade for a backend that is down) used to leave the link
// "connecting" for as long as it hung: no attempt failed, so no outage was
// published. Each attempt now gives up without a hello, so it reads offline.
it("reads offline when the server accepts connections but never answers", async () => {
  const sockets: Socket[] = [];
  const hole = createServer((socket) => void sockets.push(socket));
  await new Promise<void>((resolve) => hole.listen(0, "127.0.0.1", resolve));
  const { port } = hole.address() as { port: number };
  let db: Db | undefined;
  try {
    db = await createDb(
      await localAccountConfig("00000000-0000-0000-0000-0000000005e5", `http://127.0.0.1:${port}`),
    );
    const unsubscribe = db.subscribe(app.notes, () => undefined);
    const offline = await statusWhere(db, (status) => status.link.state === "offline", 45_000);
    expect(offline.link).toEqual({ state: "offline", reason: "network", retrying: true });
    expect(offline.lastError).toMatchObject({ kind: "network" });
    expect(sockets.length).toBeGreaterThan(1);
    unsubscribe();
  } finally {
    await db?.shutdown();
    for (const socket of sockets) socket.destroy();
    hole.close();
  }
}, 60_000);

// #3581: after a live link drops, a reconnect attempt that the server takes
// but never answers used to keep the link "connecting" with no outage
// published. Each attempt now ends without a hello (#5382), so the hung
// reconnect reads offline too. A Global wait stays pending through the outage
// (#3923) and settles once the server answers again.
it("reads offline when a reconnect after a live link is accepted but never answered", async () => {
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
  });
  const server = await startTestJazz({
    app: app,
    permissions: permissions,
    coreOptions: { inMemory: true },
  });
  const upstream = new URL(server.url);
  // Forwards to the server until `answering` is false; then it takes each
  // connection and never writes, like a proxy holding the upgrade.
  let answering = true;
  const sockets: Socket[] = [];
  const proxy = createServer((socket) => {
    sockets.push(socket);
    if (!answering) return;
    const target = connect(Number(upstream.port), upstream.hostname);
    sockets.push(target);
    socket.pipe(target).pipe(socket);
    socket.on("error", () => target.destroy());
    target.on("error", () => socket.destroy());
    socket.on("close", () => target.destroy());
    target.on("close", () => socket.destroy());
  });
  await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  const { port } = proxy.address() as { port: number };
  let db: Db | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, `http://127.0.0.1:${port}`));
    await db.insert(app.notes, { title: "online" }).wait({ tier: "global" });
    expect(db.getSyncStatus().link).toEqual({ state: "connected" });

    answering = false;
    const live = sockets.length;
    for (const socket of sockets) socket.destroy();
    let settled = false;
    const pending = db
      .insert(app.notes, { title: "during the hang" })
      .wait({ tier: "global" })
      .then((row) => {
        settled = true;
        return row;
      });

    const offline = await statusWhere(db, (status) => status.link.state === "offline", 45_000);
    expect(offline.link).toEqual({ state: "offline", reason: "network", retrying: true });
    expect(offline.lastError).toMatchObject({ kind: "network" });
    // The client did reach the proxy again: the attempts hung, not refused.
    expect(sockets.length).toBeGreaterThan(live);
    expect(settled).toBe(false);

    answering = true;
    for (const socket of sockets) socket.destroy();
    expect(await pending).toEqual(expect.objectContaining({ title: "during the hang" }));
    expect((await statusWhere(db, (status) => status.synced, 45_000)).link).toEqual({
      state: "connected",
    });
  } finally {
    await db?.shutdown();
    for (const socket of sockets) socket.destroy();
    proxy.close();
    await server.stop();
  }
}, 120_000);

it("is local-only and synced without a server", async () => {
  const db = await createDb(await localAccountConfig("sync-status-local"));
  try {
    await db.insert(app.notes, { title: "here" }).wait({ tier: "local" });
    expect(db.getSyncStatus()).toEqual({
      link: { state: "local-only" },
      pendingUploads: 0,
      oldestPendingAt: null,
      rejected: 0,
      lastError: null,
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
  const server = await startLocalJazzServer({ inMemory: true });
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
