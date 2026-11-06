import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import { DbReplacedError } from "../runtime/db-replaced-error.js";
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

const loadLocalFirst: LocalFirstLoader = async () => ({ createClient: createLocalFirstDb });

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

  it("ends a kept Db's queries loudly once its user signs out", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const kept = db(session);
    await kept.insert(app.notes, { text: "before" }).wait({ tier: "global" });
    const errors: Error[] = [];
    kept.subscribe(app.notes, { onUpdate: () => {}, onError: (error) => errors.push(error) });

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
