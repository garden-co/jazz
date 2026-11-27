import { afterEach, expect, it, vi } from "vitest";
import { commands } from "vitest/browser";
import { createJazzClient, openBrowserSessionCore } from "../../src/session/create-jazz-session.js";
import { unreadableRows } from "../../src/encryption/unreadable.js";
import { browserPrivateStore } from "../../src/encryption/browser-store.js";
import { encryptionStoreScope } from "../../src/encryption/store-scope.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { EncryptionNeedsLocalFirstError } from "../../src/runtime/encryption-needs-local-first.js";
import { isThinDb, type LocalFirstLoader } from "../../src/runtime/local-first-upgrade.js";
import { uniqueDbName, waitForCondition } from "./support.js";
import {
  createJazzServerTransportControl,
  deploy,
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppUserUrl,
  jazzAppVisitor,
  jazzServerStoreHolds,
  signOutOfJazzApp,
  startJazzApp,
} from "./testing-server.js";

// End-to-end encryption in a real browser: a device account whose keys live in
// the browser's own private store (IndexedDB, a non-extractable WebCrypto key),
// the encrypted rows kept by the shared worker as sealed bytes, a real server
// relaying them. The account's store is localStorage; reloading is closing the
// session and opening another.

import { app } from "../fixtures/encrypted/jazz.generated.js";
import permissions from "../fixtures/encrypted/jazz.permissions.json";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
  localStorage.clear();
});

async function serverAt(
  name: string,
  options?: { onDisk?: boolean; permissions?: typeof permissions; user?: string },
) {
  const core = await getJazzServerInfo(uniqueDbName(name), options);
  const { appId, serverUrl: coreUrl, adminSecret } = core;
  const served = options?.permissions ?? permissions;
  await deploy({ appId, serverUrl: coreUrl, adminSecret, app, permissions: served });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app, permissions: served },
  );
  const url = options?.user
    ? await jazzAppUserUrl(backend, options.user)
    : await jazzAppVisitor(backend);
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const target = input instanceof Request ? input.url : String(input);
    return target.startsWith(new URL(url).origin)
      ? jazzAppFetch(input, init)
      : pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = pageFetch;
  });
  return { url, appId, core };
}

/** A page load: a session with a device account and the app's encryption. */
async function load(
  server: { url: string; appId: string },
  dbName: string,
  { clearOnSignOut = false } = {},
) {
  const session = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,
    clearOnSignOut,
    driver: { type: "persistent", dbName },
    fetch: jazzAppFetch,
    initial: "device",
    client: async () => ({ createClient: createLocalFirstDb }),
    encryption: { app },
  } as never);
  cleanups.push(() => session.close());
  await waitForCondition(
    async () => {
      const client = session.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "The session should upgrade to local-first",
  );
  return session;
}

/**
 * Whether this origin's IndexedDB databases (the shared worker's store, the
 * private store, every other) hold each of `needles` as UTF-8 bytes in any
 * stored value: what is on this browser's disk.
 */
async function indexedDbHolds(needles: string[]): Promise<boolean[]> {
  const encoder = new TextEncoder();
  const found = needles.map(() => false);
  const look = (bytes: Uint8Array) => {
    needles.forEach((needle, index) => {
      if (found[index]) return;
      const wanted = encoder.encode(needle);
      outer: for (let at = 0; at + wanted.length <= bytes.length; at++) {
        for (let i = 0; i < wanted.length; i++) if (bytes[at + i] !== wanted[i]) continue outer;
        found[index] = true;
        return;
      }
    });
  };
  const visit = async (value: unknown, seen: Set<unknown>): Promise<void> => {
    if (value === null || typeof value !== "object") {
      if (typeof value === "string") look(encoder.encode(value));
      return;
    }
    if (seen.has(value)) return;
    seen.add(value);
    if (value instanceof ArrayBuffer) return look(new Uint8Array(value));
    if (ArrayBuffer.isView(value))
      return look(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    if (value instanceof Blob) return look(new Uint8Array(await value.arrayBuffer()));
    for (const inner of Object.values(value)) await visit(inner, seen);
  };
  for (const { name } of await indexedDB.databases()) {
    if (!name) continue;
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(name);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    try {
      for (const store of Array.from(db.objectStoreNames)) {
        const values = await new Promise<unknown[]>((resolve, reject) => {
          const request = db.transaction(store).objectStore(store).getAll();
          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });
        await visit(values, new Set());
      }
    } finally {
      db.close();
    }
  }
  return found;
}

/** Names the step that does not finish, instead of a bare test timeout. */
function step<T>(name: string, work: Promise<T>, ms = 15_000): Promise<T> {
  return Promise.race([
    work,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`step "${name}" did not finish in ${ms} ms`)), ms),
    ),
  ]);
}

it("encrypts in a real browser: a device's keys in the browser's own store, a tab behind the shared worker, rows sealed on the server, readable by the device, the same device after a reload", async () => {
  const server = await serverAt("encryption-browser", { onDisk: true });
  const dbName = uniqueDbName("encryption-browser-db");
  const first = await step("load", load(server, dbName, { clearOnSignOut: true }), 40_000);
  const db = first.getSnapshot().client!.db;
  const ready = await step("ready", db.encryption.ready({ timeoutMs: 40_000 }), 45_000);
  expect(ready).toBe("active");
  const device = await step("deviceId", db.encryption.deviceId());
  const account = first.getSnapshot().user!.accountId;

  const id = crypto.randomUUID();
  // A plain value and an encrypted one, each unique: the plain one shows that
  // a store's bytes can be searched at all, the encrypted one must not be found.
  const plainName = `plain-name-${crypto.randomUUID()}`;
  const secretTitle = `secret-title-${crypto.randomUUID()}`;
  const self = await step(
    "member row",
    db.encryption.members.row({ table: "projects", id }, account),
  );
  const written = await db.transaction((tx) => {
    tx.insert(app.projects, { name: plainName, ownerId: account }, { id });
    tx.insert(app.__e2ee_members, self);
  });
  await step("project global", written.wait({ tier: "global" }));
  await step(
    "task global",
    db.insert(app.tasks, { projectId: id, title: secretTitle }).wait({ tier: "global" }),
  );
  const read = async (client: typeof db) =>
    await client.all(app.tasks.where({ projectId: id }), {
      tier: "local-first",
    });
  expect((await step("read", read(db))).map((row) => row.title)).toEqual([secretTitle]);

  // Sealed on the server: its store holds the plain name, and not the title.
  await waitForCondition(
    async () => (await jazzServerStoreHolds(server.core, [plainName]))[0]!,
    30_000,
    "The server should store the plain row",
  );
  expect(await jazzServerStoreHolds(server.core, [plainName, secretTitle])).toEqual([true, false]);
  // And in this browser: the shared worker's store holds the plain name, and
  // no IndexedDB database holds the title.
  await waitForCondition(
    async () => (await indexedDbHolds([plainName]))[0]!,
    30_000,
    "The shared worker should store the plain row",
  );
  expect(await indexedDbHolds([plainName, secretTitle])).toEqual([true, false]);

  // The device's keys are in the browser's store, protected by a key that
  // cannot be exported.
  const names = await indexedDB.databases();
  expect(names.map((entry) => entry.name)).toContain("jazz-private-store");

  // Even a shared device keeps its keys on an ordinary close.
  await first.close();
  const keys = encryptionStoreScope(server.appId, "dev", account);
  expect(await browserPrivateStore().list(keys)).toContain("device");
  const reloaded = await step("reload", load(server, dbName, { clearOnSignOut: true }), 40_000);
  const again = reloaded.getSnapshot().client!.db;
  expect(await step("ready again", again.encryption.ready({ timeoutMs: 150_000 }), 160_000)).toBe(
    "active",
  );
  expect(await again.encryption.deviceId()).toBe(device);
  await waitForCondition(
    async () => (await read(again)).some((row) => row.title === secretTitle),
    30_000,
    "The reloaded device should read its encrypted row",
  );
}, 240_000);

// A tab's encryption runtime acts online only while it is: it rotates a
// space, and evaluates the rotation a removed device makes necessary, before
// it seals. Offline it seals under the current version. So a tab behind the
// shared worker must be online again once the worker reconnects, or every
// write after any lost connection (a network drop, or a server that ends the
// user's connections before a device removal) is sealed under the version a
// removed device still holds.
it("a tab's encryption is online again once its worker reconnects after losing the server", async () => {
  const server = await serverAt("encryption-reconnect");
  const transport = await createJazzServerTransportControl(server.url);
  cleanups.push(() => transport.stop());
  const session = await step(
    "load",
    load({ url: transport.url, appId: server.appId }, uniqueDbName("encryption-reconnect-db")),
    40_000,
  );
  const db = session.getSnapshot().client!.db;
  expect(await step("ready", db.encryption.ready({ timeoutMs: 40_000 }), 45_000)).toBe("active");
  const account = session.getSnapshot().user!.accountId;
  const id = crypto.randomUUID();
  const self = await db.encryption.members.row({ table: "projects", id }, account);
  const written = await db.transaction((tx) => {
    tx.insert(app.projects, { name: "project", ownerId: account }, { id });
    tx.insert(app.__e2ee_members, self);
  });
  await step("project global", written.wait({ tier: "global" }));
  await step(
    "task global",
    db.insert(app.tasks, { projectId: id, title: "before" }).wait({ tier: "global" }),
  );
  const space = { table: "projects", id };
  // Connected: the tab can rotate the space.
  // (It delivers to other member accounts only, so it returns 0 here.)
  await expect(step("rotate before", db.encryption.keys.rotate(space))).resolves.toBe(0);

  // The server goes away until the worker publishes the outage, then comes back.
  await transport.down();
  await waitForCondition(
    async () => db.getSyncStatus().link.state === "offline",
    30_000,
    "the worker's published outage",
  );
  await transport.up();
  await waitForCondition(
    async () => db.getSyncStatus().link.state === "connected",
    30_000,
    "the worker's reconnect",
  );
  await step(
    "task after reconnect",
    db.insert(app.tasks, { projectId: id, title: "after" }).wait({ tier: "global" }),
  );

  // Connected again: the tab rotates as before, instead of failing offline.
  await expect(step("rotate after", db.encryption.keys.rotate(space))).resolves.toBe(0);
}, 120_000);

it("a returning browser subscribes to stored encrypted rows as soon as its session opens", async () => {
  const server = await serverAt("encrypted-message-reload");
  const dbName = uniqueDbName("encrypted-message-reload");
  const first = await load(server, dbName);
  const db = first.getSnapshot().client!.db;
  expect(await db.encryption.ready({ timeoutMs: 40_000 })).toBe("active");
  const projectId = crypto.randomUUID();
  const account = first.getSnapshot().user!.accountId;
  const self = await db.encryption.members.row({ table: "projects", id: projectId }, account);
  await (
    await db.transaction((tx) => {
      tx.insert(app.projects, { name: "project", ownerId: account }, { id: projectId });
      tx.insert(app.__e2ee_members, self);
    })
  ).wait({ tier: "global" });
  const title = "stored title";
  await db.insert(app.tasks, { projectId, title }).wait({ tier: "global" });
  await first.close();

  const returning = await loadThin(server, dbName, async () => ({
    createClient: createLocalFirstDb,
  }));
  const again = returning.getSnapshot().client!.db;
  expect(isThinDb(again)).toBe(false);
  const tasks = app.tasks.where({ projectId }).orderBy("$createdAt", "desc").limit(200);
  const latest: { selected?: string[]; full?: string[] } = {};
  const stopSelected = again.subscribe(tasks.select("projectId", "title", "$createdAt"), (rows) => {
    latest.selected = rows.map((row) => row.title);
  });
  const stopFull = again.subscribe(tasks, (rows) => {
    latest.full = rows.map((row) => row.title);
  });
  try {
    await expect
      .poll(() => latest, { timeout: 30_000 })
      .toEqual({ selected: [title], full: [title] });
  } finally {
    stopSelected();
    stopFull();
  }
}, 120_000);

it("a returning browser reads its stored encrypted rows offline, and its live query goes on once the server is back", async () => {
  const server = await serverAt("encrypted-offline-reload");
  const transport = await createJazzServerTransportControl(server.url);
  cleanups.push(() => transport.stop());
  const through = { url: transport.url, appId: server.appId };
  const dbName = uniqueDbName("encrypted-offline-reload");
  const first = await load(through, dbName);
  const db = first.getSnapshot().client!.db;
  expect(await db.encryption.ready({ timeoutMs: 40_000 })).toBe("active");
  const projectId = crypto.randomUUID();
  const account = first.getSnapshot().user!.accountId;
  const self = await db.encryption.members.row({ table: "projects", id: projectId }, account);
  await (
    await db.transaction((tx) => {
      tx.insert(app.projects, { name: "project", ownerId: account }, { id: projectId });
      tx.insert(app.__e2ee_members, self);
    })
  ).wait({ tier: "global" });
  await db.insert(app.tasks, { projectId, title: "stored" }).wait({ tier: "global" });
  await first.close();

  // The server is out of reach when the page loads again. The browser has
  // synced before: it reads its stored rows with the keys it holds, at once.
  await transport.down();
  const returning = await load(through, dbName);
  const again = returning.getSnapshot().client!.db;
  const seen: string[][] = [];
  const errors: unknown[] = [];
  const stop = again.subscribe(
    app.tasks.where({ projectId }),
    {
      onUpdate: (rows) => seen.push(rows.map((row) => row.title).sort()),
      onError: (error) => errors.push(error),
    },
    { tier: "local-first" },
  );
  cleanups.push(async () => stop());
  await expect.poll(() => seen.at(-1), { timeout: 5_000 }).toEqual(["stored"]);
  expect(errors).toEqual([]);

  // The server comes back. Another page of this device writes a task: the
  // same live query, never ended, picks it up.
  await transport.up();
  const other = await load(through, uniqueDbName("encrypted-offline-other"));
  const otherDb = other.getSnapshot().client!.db;
  expect(await otherDb.encryption.ready({ timeoutMs: 40_000 })).toBe("active");
  await otherDb.insert(app.tasks, { projectId, title: "later" }).wait({ tier: "global" });
  await expect.poll(() => seen.at(-1), { timeout: 30_000 }).toEqual(["later", "stored"]);
  expect(errors).toEqual([]);
}, 120_000);

it("a runtime that cannot say whether encrypted rows are readable fails the subscriber waiting for it", async () => {
  const server = await serverAt("encrypted-subscription-failure");
  const session = await load(server, uniqueDbName("encrypted-subscription-failure"));
  const db = session.getSnapshot().client!.db;
  const failure = new Error("the encryption runtime closed");
  const ready = vi.spyOn(db.encryption, "readable").mockRejectedValueOnce(failure);
  const update = vi.fn();
  const onError = vi.fn();
  const stop = db.subscribe(app.tasks, { onUpdate: update, onError });
  try {
    await expect.poll(() => onError.mock.calls).toEqual([[failure]]);
    expect(update).not.toHaveBeenCalled();
  } finally {
    stop();
    ready.mockRestore();
  }
}, 60_000);

it.each(["select", "equality"] as const)(
  "a keyed tab reads an encrypted column through its worker with %s",
  async (operation) => {
    const server = await serverAt("encrypted-column");
    const session = await load(server, uniqueDbName("encrypted-column-db"));
    const db = session.getSnapshot().client!.db;
    expect(await db.encryption.ready({ timeoutMs: 40_000 })).toBe("active");
    const account = session.getSnapshot().user!.accountId;
    const projectId = crypto.randomUUID();
    const self = await db.encryption.members.row({ table: "projects", id: projectId }, account);
    const project = await db.transaction((tx) => {
      tx.insert(app.projects, { name: "plans", ownerId: account }, { id: projectId });
      tx.insert(app.__e2ee_members, self);
    });
    await project.wait({ tier: "global" });
    const title = `secret-${crypto.randomUUID()}`;
    await db.insert(app.tasks, { projectId, title }).wait({ tier: "global" });
    await db.insert(app.tasks, { projectId, title: "another task" }).wait({ tier: "global" });

    const query = app.tasks
      .where(operation === "select" ? { projectId } : { projectId, title })
      .select("projectId", "title");
    const rows = await db.all(query, { tier: "remote" });
    expect(rows.map((row) => row.title).sort()).toEqual(
      operation === "select" ? ["another task", title] : [title],
    );
  },
  120_000,
);

it("a one-shot read in the browser names the rows it leaves out undecrypted", async () => {
  // Tasks are readable to everyone here, so a device of another account is
  // handed the ciphertext of alice's task: it holds no key for her space.
  const open = structuredClone(permissions) as { tables: Record<string, unknown> };
  open.tables.tasks = { read: "true", insert: "allowedTo.read(project)" };
  const server = await serverAt("encryption-unreadable", {
    permissions: open as typeof permissions,
  });

  const alice = await step("alice load", load(server, uniqueDbName("encryption-alice")), 40_000);
  const aliceDb = alice.getSnapshot().client!.db;
  expect(await step("alice ready", aliceDb.encryption.ready({ timeoutMs: 40_000 }), 45_000)).toBe(
    "active",
  );
  const account = alice.getSnapshot().user!.accountId;
  const project = crypto.randomUUID();
  const self = await step(
    "member row",
    aliceDb.encryption.members.row({ table: "projects", id: project }, account),
  );
  const written = await aliceDb.transaction((tx) => {
    tx.insert(app.projects, { name: "plans", ownerId: account }, { id: project });
    tx.insert(app.__e2ee_members, self);
  });
  await step("project global", written.wait({ tier: "global" }));
  const task = await aliceDb.insert(app.tasks, { projectId: project, title: "alice's secret" });
  await step("task global", task.wait({ tier: "global" }));
  const tasks = app.tasks.where({ projectId: project });
  // Alice holds the key: her result is complete.
  const hers = await aliceDb.all(tasks, { tier: "remote" });
  expect(hers.map((row) => row.title)).toEqual(["alice's secret"]);
  expect(unreadableRows(hers)).toEqual([]);
  await alice.close();

  // Another account's device in this browser (its account is localStorage's).
  localStorage.clear();
  const bob = await step("bob load", load(server, uniqueDbName("encryption-bob")), 40_000);
  const bobDb = bob.getSnapshot().client!.db;
  expect(await step("bob ready", bobDb.encryption.ready({ timeoutMs: 40_000 }), 45_000)).toBe(
    "active",
  );
  expect(bob.getSnapshot().user!.accountId).not.toBe(account);
  let bobSees: readonly unknown[] = [];
  await waitForCondition(
    async () => {
      bobSees = await bobDb.all(tasks, { tier: "remote" });
      return unreadableRows(bobSees).length > 0;
    },
    30_000,
    "Bob's one-shot read should name alice's task as unreadable",
  );
  expect(bobSees).toEqual([]);
  expect(unreadableRows(bobSees)).toEqual([
    { table: "tasks", id: hers[0]!.id, column: "title", code: expect.any(String) },
  ]);
}, 180_000);

/** A thin page load: no wait for the upgrade; `client` loads the local-first client, if given. */
async function loadThin(
  server: { url: string; appId: string },
  dbName: string,
  client?: LocalFirstLoader,
) {
  const session = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,
    driver: { type: "persistent", dbName },
    fetch: jazzAppFetch,
    initial: "device",
    ...(client ? { client } : {}),
    encryption: { app },
  } as never);
  cleanups.push(() => session.close());
  return session;
}

/** Every byte the page sends on a WebSocket or in a fetch body from now on. */
function recordWire() {
  const sent: Uint8Array[] = [];
  const encoder = new TextEncoder();
  const keep = (data: unknown) => {
    if (typeof data === "string") sent.push(encoder.encode(data));
    else if (data instanceof ArrayBuffer) sent.push(new Uint8Array(data));
    else if (ArrayBuffer.isView(data))
      sent.push(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  };
  const send = WebSocket.prototype.send;
  WebSocket.prototype.send = function (data) {
    keep(data);
    return send.call(this, data);
  };
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    keep(init?.body);
    return pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    WebSocket.prototype.send = send;
    globalThis.fetch = pageFetch;
  });
  const holds = (needle: string) => {
    const bytes = encoder.encode(needle);
    return sent.some((frame) => {
      outer: for (let at = 0; at + bytes.length <= frame.length; at++) {
        for (let i = 0; i < bytes.length; i++) if (frame[at + i] !== bytes[i]) continue outer;
        return true;
      }
      return false;
    });
  };
  return { holds, frames: () => sent.length };
}

it("a thin load with a loader writes and subscribes to an encrypted table before the upgrade; both complete after it, a plain read answers at once, and the wire holds no plaintext", async () => {
  const server = await serverAt("encryption-thin");
  const dbName = uniqueDbName("encryption-thin-db");

  // An earlier visit, on the local-first client, makes the device a member of a project.
  const first = await step("load", load(server, dbName), 40_000);
  const firstDb = first.getSnapshot().client!.db;
  expect(await step("ready", firstDb.encryption.ready({ timeoutMs: 40_000 }), 45_000)).toBe(
    "active",
  );
  const account = first.getSnapshot().user!.accountId;
  const project = crypto.randomUUID();
  const self = await firstDb.encryption.members.row({ table: "projects", id: project }, account);
  await step(
    "project global",
    (
      await firstDb.transaction((tx) => {
        tx.insert(app.projects, { name: "plans", ownerId: account }, { id: project });
        tx.insert(app.__e2ee_members, self);
      })
    ).wait({ tier: "global" }),
  );
  await step("close first", first.close());

  // A device that already upgraded opens local-first at once; forget that, so
  // this visit starts thin as a first visit does (the account's keys stay).
  for (const key of Object.keys(localStorage))
    if (key.startsWith("jazz-local-first-v1:")) localStorage.removeItem(key);

  // The next visit starts thin; its loader answers only when released.
  const wire = recordWire();
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  const session = await step(
    "thin load",
    loadThin(server, dbName, async () => {
      await released;
      return { createClient: createLocalFirstDb };
    }),
  );
  const thin = session.getSnapshot().client!.db;
  expect(isThinDb(thin)).toBe(true);

  const marker = `plaintext-${crypto.randomUUID()}`;
  const written = thin.insert(app.tasks, {
    projectId: project,
    title: marker,
  } as never);
  const seen: string[][] = [];
  const stop = thin.subscribe(app.tasks.where({ projectId: project }), (rows) =>
    seen.push(rows.map((row) => row.title)),
  );

  // A plain read answers at once, while the encrypted work waits.
  await step("plain read", thin.all(app.projects.where({ id: project }), { tier: "remote" }));
  expect(isThinDb(thin)).toBe(true);
  expect(seen).toEqual([]);

  release();
  await step("encrypted write", written.wait({ tier: "global" }));
  await waitForCondition(
    async () => seen.some((titles) => titles.includes(marker)),
    30_000,
    "The subscription should deliver the row after the upgrade",
  );
  stop();
  expect(isThinDb(thin)).toBe(false);
  expect(wire.frames()).toBeGreaterThan(0);
  expect(wire.holds(marker)).toBe(false);
}, 240_000);

it("without a loader, a thin client refuses encrypted writes and creates spaces only when automatic membership is disabled", async () => {
  const server = await serverAt("encryption-thin-none");
  const session = await loadThin(server, uniqueDbName("encryption-thin-none-db"));
  const thin = session.getSnapshot().client!.db;
  const account = session.getSnapshot().user!.accountId;

  const refused = await Promise.resolve()
    .then(() =>
      thin.insert(app.tasks, {
        projectId: crypto.randomUUID(),
        title: "secret",
      } as never),
    )
    .then(
      () => undefined,
      (error: unknown) => error,
    );
  expect(refused).toBeInstanceOf(EncryptionNeedsLocalFirstError);
  expect((refused as EncryptionNeedsLocalFirstError).reason).toBe("no-loader");

  const id = crypto.randomUUID();
  const project = { name: "plain", ownerId: account };
  const refusedSpace = await Promise.resolve()
    .then(() => thin.insert(app.projects, project, { id }))
    .then(
      () => undefined,
      (error: unknown) => error,
    );
  expect(refusedSpace).toBeInstanceOf(EncryptionNeedsLocalFirstError);
  const spaceError = refusedSpace as EncryptionNeedsLocalFirstError;
  expect(spaceError.reason).toBe("no-loader");
  expect(spaceError.message).toContain('client: () => import("jazz-tools/local-first")');
  expect(spaceError.message).toContain("addCreatorAsMember: false");
  expect(isThinDb(thin)).toBe(true);

  await step(
    "plain write",
    thin.insert(app.projects, project, { id, addCreatorAsMember: false }).wait({ tier: "global" }),
  );
  expect(isThinDb(thin)).toBe(true);
}, 120_000);

it("with clearOnSignOut, a sign-out wipes the account's keys from the browser's private store", async () => {
  // A shared computer: once alice is signed out, no one using this browser
  // (the next user, a script on the origin) may unprotect her device's keys.
  const name = uniqueDbName("encryption-clear-on-sign-out");
  const { appId, serverUrl: coreUrl, adminSecret } = await getJazzServerInfo(name);
  await deploy({ appId, serverUrl: coreUrl, adminSecret, app, permissions });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app, permissions },
  );
  const aliceUrl = await jazzAppUserUrl(backend, "alice");
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = input instanceof Request ? input.url : String(input);
    return url.startsWith(aliceUrl) ? jazzAppFetch(input, init) : pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = pageFetch;
  });
  // The session core, as the clear-on-sign-out tests open it: it can ask the
  // app's backend again for who is signed in (`reauthenticate`).
  const load = async () => {
    const session = await openBrowserSessionCore({
      scope: appId,
      serverUrl: aliceUrl,
      fetch: jazzAppFetch,
      clearOnSignOut: true,
      client: async () => ({ createClient: createLocalFirstDb }),
      encryption: { app },
    } as never);
    cleanups.push(() => session.close());
    return session;
  };

  const first = await step("load", load(), 40_000);
  if (first.getSnapshot().status !== "ready") await first.reauthenticate();
  await waitForCondition(
    async () => {
      const client = first.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "Alice's page should upgrade to local-first",
  );
  const db = first.getSnapshot().client!.db;
  expect(await step("ready", db.encryption.ready({ timeoutMs: 40_000 }), 45_000)).toBe("active");
  const alice = first.getSnapshot().account!.id;
  const keys = encryptionStoreScope(appId, "dev", alice);
  expect(await browserPrivateStore().list(keys)).toContain("device");

  // Her page goes away and the app's backend signs her out; the next page
  // load wipes what she left, her keys with her replica.
  await first.close();
  await signOutOfJazzApp(backend, aliceUrl);
  const next = await step("next load", load(), 40_000);
  expect(next.getSnapshot().status).toBe("signed-out");
  await waitForCondition(
    async () => (await browserPrivateStore().list(keys)).length === 0,
    30_000,
    "The next load should wipe the signed-out user's keys",
  );
}, 180_000);

it("leaves the second tab able to use its keys when Alice signs out of the first tab", async () => {
  const server = await serverAt("encryption-sign-out-two-tabs", { user: "alice" });
  const first = await openBrowserSessionCore({
    scope: server.appId,
    serverUrl: server.url,
    fetch: jazzAppFetch,
    clearOnSignOut: true,
    localFirst: "now",
    client: async () => ({ createClient: createLocalFirstDb }),
    encryption: { app },
  } as never);
  cleanups.push(() => first.close());
  if (first.getSnapshot().status !== "ready") await first.reauthenticate();
  const { account, client } = first.getSnapshot();
  const db = client!.db;
  expect(await db.encryption.ready({ timeoutMs: 40_000 })).toBe("active");

  const projectId = crypto.randomUUID();
  const member = await db.encryption.members.row({ table: "projects", id: projectId }, account!.id);
  const project = await db.transaction((tx) => {
    tx.insert(app.projects, { name: "shared project", ownerId: account!.id }, { id: projectId });
    tx.insert(app.__e2ee_members, member);
  });
  await project.wait({ tier: "global" });
  await db.insert(app.tasks, { projectId, title: "before sign-out" }).wait({ tier: "global" });

  const id = server.appId;
  cleanups.push(() => commands.encryptedSessionTab({ action: "close", id }));
  await commands.encryptedSessionTab({
    action: "open",
    id,
    appId: server.appId,
    serverUrl: server.url,
  });
  await expect
    .poll(() => commands.encryptedSessionTab({ action: "tasks", id, projectId }), {
      timeout: 15_000,
    })
    .toEqual(["before sign-out"]);

  await first.logout();
  expect(first.getSnapshot().status).toBe("signed-out");
  const scope = encryptionStoreScope(server.appId, "dev", account!.id);
  expect(await browserPrivateStore().list(scope)).toContain("device");
  expect(
    await commands.encryptedSessionTab({
      action: "tasks",
      id,
      projectId,
      insertTitle: "after sign-out",
    }),
  ).toEqual(["after sign-out", "before sign-out"]);
}, 120_000);
