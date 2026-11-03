import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { createJazzSession, type BrowserJazzSession } from "./create-jazz-session.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) }, { migrations: "none" });
const permissions = definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ $createdBy: session.user });
  policy.notes.allowInsert.always();
});

let server: LocalJazzServerHandle | undefined;
const sessions: BrowserJazzSession[] = [];

afterEach(async () => {
  for (const session of sessions.splice(0)) await session.close();
  await server?.stop();
  server = undefined;
  vi.unstubAllGlobals();
});

async function startServer(): Promise<LocalJazzServerHandle> {
  server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schema: app,
    permissions,
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

const loadLocalFirst: LocalFirstLoader = async () => ({ createDb: createLocalFirstDb });

async function open(
  server: LocalJazzServerHandle,
  store: AccountStore,
  client?: LocalFirstLoader,
): Promise<BrowserJazzSession> {
  const session = await createJazzSession({
    appId: server.appId,
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

  it("upgrades on demand, keeping the app's live queries answered", async () => {
    const server = await startServer();
    const session = await open(server, memoryStore());
    const thin = db(session);
    await thin.insert(app.notes, { text: "before" }).wait({ tier: "global" });
    const seen: string[][] = [];
    const stop = thin.subscribe(app.notes, (rows) => seen.push(rows.map((row) => row.text)));
    await expect.poll(() => seen.at(-1), { timeout: 10_000 }).toEqual(["before"]);

    await thin.upgradeToLocalFirst(loadLocalFirst);
    stop();

    const local = db(session);
    expect(local).not.toBe(thin);
    expect(isThinDb(local)).toBe(false);
    // The live query's rows were read into the local-first client first.
    expect((await local.all(app.notes, { tier: "local-first" })).map((row) => row.text)).toEqual([
      "before",
    ]);
    await local.insert(app.notes, { text: "after" }).wait({ tier: "global" });
    expect((await local.all(app.notes)).map((row) => row.text).sort()).toEqual(["after", "before"]);
    // A local-first Db has nothing left to upgrade.
    await expect(local.upgradeToLocalFirst(loadLocalFirst)).resolves.toBeUndefined();
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
