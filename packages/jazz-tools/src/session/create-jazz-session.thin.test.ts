import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import { DbReplacedError } from "../runtime/db-replaced-error.js";
import { Db } from "../runtime/db.js";
import { WriteWaitTimeoutError } from "../runtime/client.js";
import type { AccountStore } from "../accounts/persistence.js";
import { startTestJazz, type TestJazz } from "../testing/index.js";
import { createJazzClient, type BrowserJazzSession } from "./create-jazz-session.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) }, { migrations: "none" });
const permissions = definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ $createdBy: session.user });
  policy.notes.allowInsert.always();
});

let server: TestJazz | undefined;
const sessions: BrowserJazzSession[] = [];

afterEach(async () => {
  for (const session of sessions.splice(0)) await session.close();
  await server?.stop();
  server = undefined;
  vi.unstubAllGlobals();
});

async function startServer(): Promise<TestJazz> {
  server = await startTestJazz({
    app: app,
    permissions: permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  return server;
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

function memoryLocalStorage() {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => void values.set(key, value),
    removeItem: (key: string) => void values.delete(key),
  };
}

const loadLocalFirst: LocalFirstLoader = async () => ({
  createClient: createLocalFirstDb,
});

async function open(
  server: TestJazz,
  store: AccountStore,
  client?: LocalFirstLoader,
): Promise<BrowserJazzSession> {
  const session = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,
    store,
    initial: "local-first",
    driver: { type: "memory" },
    ...(client ? { client } : {}),
  });
  sessions.push(session);
  return session;
}

function db(session: BrowserJazzSession) {
  const client = session.getSnapshot().client;
  if (!client) throw new Error("session has no client");
  return client.db;
}

describe("browser session", () => {
  it("opens the thin client by default", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    expect(session.getSnapshot().status).toBe("ready");
    expect(isThinDb(db(session))).toBe(true);

    await db(session).insert(app.notes, { text: "thin" }).wait({ tier: "global" });
    expect((await db(session).all(app.notes)).map((row) => row.text)).toEqual(["thin"]);
  }, 60_000);

  it("takes a custom sign-in fetch without putting it into the client's config", async () => {
    // rc.3 adopter finding: the JSDoc'd `fetch` hook reached the Db config, so
    // opening the client threw DataCloneError in db.getConfig().
    const server = await startServer();
    const session = await createJazzClient({
      scope: server.appId,
      serverUrl: server.url,
      store: memoryStore(),
      initial: "local-first",
      driver: { type: "memory" },
      fetch: (input, init) => fetch(input, init),
    });
    sessions.push(session);
    expect(session.getSnapshot().status).toBe("ready");
    expect(db(session).getConfig()).not.toHaveProperty("fetch");
    expect(db(session).getConfig()).not.toHaveProperty("store");

    await db(session).upgradeToLocalFirst(loadLocalFirst);
    expect(isThinDb(db(session))).toBe(false);
    expect(db(session).getConfig()).not.toHaveProperty("fetch");
    await db(session).insert(app.notes, { text: "custom fetch" }).wait({ tier: "global" });
  }, 60_000);

  it("upgrades on demand in place: the same Db keeps its live queries and takes writes", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const client = session.getSnapshot().client;
    const kept = db(session);
    await kept.insert(app.notes, { text: "before" }).wait({ tier: "global" });
    const seen: string[][] = [];
    const errors: Error[] = [];
    const states: string[] = [];
    const stopStates = session.subscribe(() => states.push(session.getSnapshot().status));
    const stop = kept.subscribe(app.notes, {
      onUpdate: (rows) => seen.push(rows.map((row) => row.text).sort()),
      onError: (error) => errors.push(error),
    });
    await expect.poll(() => seen.at(-1), { timeout: 10_000 }).toEqual(["before"]);
    const updatesBefore = seen.length;

    await kept.upgradeToLocalFirst(loadLocalFirst);

    // The session kept its client and Db: nothing for an app or provider to redo.
    expect(session.getSnapshot().client).toBe(client);
    expect(db(session)).toBe(kept);
    expect(isThinDb(kept)).toBe(false);
    expect(states).toEqual([]);
    // The rows were read into the local-first client first.
    expect((await kept.all(app.notes, { tier: "local-first" })).map((row) => row.text)).toEqual([
      "before",
    ]);
    await kept.insert(app.notes, { text: "after" }).wait({ tier: "global" });
    await expect.poll(() => seen.at(-1), { timeout: 10_000 }).toEqual(["after", "before"]);
    // The live query never went empty: its first local-first result replaced the thin one.
    expect(seen.slice(updatesBefore).every((rows) => rows.length > 0)).toBe(true);
    expect(errors).toEqual([]);
    // A local-first Db has nothing left to upgrade.
    await expect(kept.upgradeToLocalFirst(loadLocalFirst)).resolves.toBeUndefined();
    stop();
    stopStates();
  }, 60_000);

  it("counts and uploads the thin Db's unsent writes after the upgrade", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const kept = db(session);
    await kept.disconnect();
    kept.insert(app.notes, { text: "unsent" });
    await kept.upgradeToLocalFirst(loadLocalFirst);

    // The thin runtime still holds the write: the kept Db says so.
    expect(kept.getSyncStatus().synced).toBe(false);
    expect(kept.getSyncStatus().pendingUploads).toBeGreaterThan(0);
    await expect(kept.waitForSync({ timeoutMs: 200 })).rejects.toThrow(WriteWaitTimeoutError);

    // Its own runtime reconnects and uploads it; then the kept Db is synced.
    await Db.prototype.reconnect.call(kept);
    await kept.waitForSync({ timeoutMs: 10_000 });
    expect(kept.getSyncStatus().pendingUploads).toBe(0);
    await expect
      .poll(async () => (await kept.all(app.notes)).map((row) => row.text), {
        timeout: 10_000,
      })
      .toEqual(["unsent"]);
  }, 60_000);

  it("closes at once after the upgrade, even with thin writes it can't upload", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const kept = db(session);
    await kept.disconnect();
    kept.insert(app.notes, { text: "unsent" });
    await kept.upgradeToLocalFirst(loadLocalFirst);

    let closed = false;
    void session.close().then(() => (closed = true));
    await expect.poll(() => closed, { timeout: 5_000 }).toBe(true);
    sessions.splice(sessions.indexOf(session), 1);
  }, 60_000);

  it("keeps the thin runtime's own credential renewal on the thin runtime", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const kept = db(session);
    const own = vi.fn();
    const stop = kept.onOwnAuthChanged(own);
    const refreshed: Db[] = [];
    const spy = vi
      .spyOn(Db.prototype, "refreshOwnAccountAuth")
      .mockImplementation(async function (this: Db) {
        refreshed.push(this);
        return "token";
      });
    await kept.upgradeToLocalFirst(loadLocalFirst);
    const account = session.getSnapshot().account!;
    // The app's renewal reaches the local-first Db; the thin runtime's own
    // context renews the thin runtime, which still has writes to upload.
    await kept.refreshAccountAuth(account);
    await Db.prototype.refreshOwnAccountAuth.call(kept, account);
    expect(refreshed[0]).not.toBe(kept);
    expect(refreshed[1]).toBe(kept);
    spy.mockRestore();
    stop();
  }, 60_000);

  it("opens a query subscribed during the upgrade with its rows", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const kept = db(session);
    await kept.insert(app.notes, { text: "before" }).wait({ tier: "global" });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const upgrading = kept.upgradeToLocalFirst(async () => {
      await gate;
      return loadLocalFirst();
    });
    const seen: string[][] = [];
    const stop = kept.subscribe(app.notes, (rows) => seen.push(rows.map((row) => row.text)));
    await expect.poll(() => seen.at(-1), { timeout: 10_000 }).toEqual(["before"]);
    release();
    await upgrading;
    await kept.insert(app.notes, { text: "after" }).wait({ tier: "global" });
    await expect
      .poll(() => seen.at(-1)?.slice().sort(), { timeout: 10_000 })
      .toEqual(["after", "before"]);
    expect(
      seen.slice(seen.findIndex((rows) => rows.length > 0)).every((rows) => rows.length > 0),
    ).toBe(true);
    stop();
  }, 60_000);

  it("ends a kept Db's queries loudly once its user signs out", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const kept = db(session);
    await kept.insert(app.notes, { text: "before" }).wait({ tier: "global" });
    const errors: Error[] = [];
    kept.subscribe(app.notes, {
      onUpdate: () => {},
      onError: (error) => errors.push(error),
    });

    await session.logout();

    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(DbReplacedError);
    expect(errors[0]!.message).toMatch(/signed out or switched accounts/);
    expect(errors[0]!.message).toMatch(
      /session\.getSnapshot\(\)\.client inside session\.subscribe/,
    );
    expect(() => kept.insert(app.notes, { text: "lost" })).toThrow(DbReplacedError);
    expect(() => kept.subscribe(app.notes, () => {})).toThrow(DbReplacedError);
    expect(() => kept.onMutationError(() => {})).toThrow(DbReplacedError);
  }, 60_000);

  it("upgrades in the background with a client loader, and remembers it", async () => {
    vi.stubGlobal("localStorage", memoryLocalStorage());
    const server = await startServer();
    const store = memoryStore();
    const loader = vi.fn(loadLocalFirst);
    const session = await open(server, store, loader);
    expect(isThinDb(db(session))).toBe(true);
    await expect.poll(() => isThinDb(db(session)), { timeout: 10_000 }).toBe(false);
    expect(loader).toHaveBeenCalledTimes(1);
    await db(session).insert(app.notes, { text: "local" }).wait({ tier: "global" });
    await session.close();

    // The same account in this browser opens local-first straight away.
    const again = await open(server, store, loader);
    expect(isThinDb(db(again))).toBe(false);
    expect(loader).toHaveBeenCalledTimes(2);
    expect((await db(again).all(app.notes)).map((row) => row.text)).toEqual(["local"]);
  }, 60_000);

  it("starts thin when the remembered local-first client no longer loads", async () => {
    vi.stubGlobal("localStorage", memoryLocalStorage());
    const server = await startServer();
    const store = memoryStore();
    const first = await open(server, store, loadLocalFirst);
    await expect.poll(() => isThinDb(db(first)), { timeout: 10_000 }).toBe(false);
    await first.close();

    // A deploy renamed the chunk the loader imports: the next visit runs
    // thin instead of failing.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const again = await open(server, store, async () => {
      throw new Error("Failed to fetch dynamically imported module");
    });
    expect(again.getSnapshot().status).toBe("ready");
    expect(isThinDb(db(again))).toBe(true);
    expect(String(warn.mock.calls[0]?.[0])).toMatch(/starting thin/);
    await db(again).insert(app.notes, { text: "thin again" }).wait({ tier: "global" });
    warn.mockRestore();
  }, 60_000);

  it("stays thin when the upgrade fails", async () => {
    const server = await startServer();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const session = await open(server, memoryStore(), async () => {
      throw new Error("offline");
    });
    await expect.poll(() => warn.mock.calls.length, { timeout: 10_000 }).toBe(1);
    expect(isThinDb(db(session))).toBe(true);
    await db(session).insert(app.notes, { text: "still thin" }).wait({ tier: "global" });
    warn.mockRestore();
  }, 60_000);
});
