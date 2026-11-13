import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import type { Db } from "./db.js";
import type { SyncStatus } from "./sync-status.js";
import { isAuthorityChangedError } from "./authority-changed-error.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz, type TestJazz } from "../testing/index.js";

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
});

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

// A client that synced with a server whose store is then recreated (here: an
// in-memory dev server restarted on the same port) is told the authority
// changed: its link is offline with that reason, its waits for the server
// fail with an AuthorityChangedError naming the recovery instead of hanging,
// and nothing resets on its own. A client on a fresh store syncs with the new
// server.
it("reports a server whose store was recreated as authority-changed", async () => {
  const coreOptions = {
    appId: randomUUID(),
    adminSecret: randomUUID(),
    backendSecret: randomUUID(),
    inMemory: true,
  };
  const start = (port?: number) =>
    startTestJazz({ app, permissions, coreOptions, ...(port === undefined ? {} : { port }) });
  let server: TestJazz | undefined = await start();
  const url = server.url;
  const port = Number(new URL(url).port);
  let alice: Db | undefined;
  let reset: Db | undefined;
  try {
    alice = await createDb(await localAccountConfig(coreOptions.appId, url));
    await alice.insert(app.notes, { title: "first history" }).wait({ tier: "global" });
    await statusWhere(alice, (status) => status.synced);

    // The server comes back on the same URL with a new in-memory store.
    await alice.disconnect();
    await server.stop();
    server = undefined;
    server = await start(port);

    const offline = alice.insert(app.notes, { title: "written while away" });
    await offline.wait({ tier: "local" });
    await alice.reconnect();
    expect(await statusWhere(alice, (status) => status.link.state === "offline")).toEqual({
      link: { state: "offline", reason: "authority-changed", retrying: false },
      pendingUploads: 1,
      oldestPendingAt: expect.any(Number),
      rejected: 0,
      lastError: expect.objectContaining({ kind: "authority-changed" }),
      synced: false,
    });

    const failure = await offline.wait({ tier: "global" }).then(
      () => null,
      (error: unknown) => error,
    );
    expect(isAuthorityChangedError(failure)).toBe(true);
    expect((failure as Error).message).toContain("deleteClientStorage()");
    await expect(alice.waitForSync({ timeoutMs: 5_000 })).rejects.toSatisfy(
      isAuthorityChangedError,
    );
    // Local data stays: nothing is reset behind the app's back.
    expect(
      (await alice.all(app.notes, { tier: "local-first" })).map((note) => note.title).sort(),
    ).toEqual(["first history", "written while away"]);
    // The old history's write never reached the new server.
    expect(await server.jazz.db.all(app.notes)).toEqual([]);

    // The recovery: a fresh local store syncs with the new server.
    reset = await createDb(await localAccountConfig(coreOptions.appId, url));
    await reset.insert(app.notes, { title: "second history" }).wait({ tier: "global" });
    expect(await statusWhere(reset, (status) => status.synced)).toMatchObject({
      link: { state: "connected" },
    });
    expect((await reset.all(app.notes, { tier: "remote" })).map((note) => note.title)).toEqual([
      "second history",
    ]);
  } finally {
    await alice?.shutdown();
    await reset?.shutdown();
    await server?.stop();
  }
}, 60_000);
