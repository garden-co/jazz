import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import {
  openBrowserSessionCore as createJazzClient,
  type BrowserJazzSession,
} from "./create-jazz-session.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) }, { migrations: "none" });

const sessions: BrowserJazzSession[] = [];

afterEach(async () => {
  for (const session of sessions.splice(0)) await session.close();
});

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

const loadLocalFirst: LocalFirstLoader = async () => ({ createClient: createLocalFirstDb });

async function openDeviceOnly(
  store: AccountStore,
  extra: Partial<Parameters<typeof createJazzClient>[0]> = {},
): Promise<BrowserJazzSession> {
  const session = await createJazzClient({
    serverUrl: null,
    store,
    driver: { type: "memory" },
    client: loadLocalFirst,
    ...extra,
  });
  sessions.push(session);
  return session;
}

describe("device-only browser session (serverUrl: null)", () => {
  it("opens the local-first client on this device's account, with no server", async () => {
    const session = await openDeviceOnly(memoryStore());
    const snapshot = session.getSnapshot();
    expect(snapshot.status).toBe("ready");
    expect(snapshot.account?.identity.issuer).toBe("urn:jazz:local-first");
    const db = snapshot.client!.db;
    expect(isThinDb(db)).toBe(false);
    expect(db.getConfig().serverUrl).toBeUndefined();

    await db.insert(app.notes, { text: "on this device" }).wait({ tier: "local" });
    expect((await db.all(app.notes)).map((row) => row.text)).toEqual(["on this device"]);
  }, 60_000);

  it("keeps account-manager options out of the client's config", async () => {
    const session = await openDeviceOnly(memoryStore(), {
      fetch: (input, init) => fetch(input, init),
      linkDeviceAccountOnSignUp: false,
    });
    const config = session.getSnapshot().client!.db.getConfig();
    expect(config).not.toHaveProperty("fetch");
    expect(config).not.toHaveProperty("store");
    expect(config).not.toHaveProperty("linkDeviceAccountOnSignUp");
  }, 60_000);

  it("reopens the same account from its store", async () => {
    const store = memoryStore();
    const first = await openDeviceOnly(store);
    const account = first.getSnapshot().account?.id;
    await first.close();
    sessions.splice(sessions.indexOf(first), 1);

    const again = await openDeviceOnly(store);
    expect(again.getSnapshot().account?.id).toBe(account);
  }, 60_000);

  it("has nothing to reauthenticate against, and refuses backend sign-in clearly", async () => {
    const session = await openDeviceOnly(memoryStore());
    await expect(session.reauthenticate()).resolves.toBeUndefined();
    await expect(session.loginWithBackend()).rejects.toThrow(/serverUrl: null/);
    await expect(session.linkWithBackend()).rejects.toThrow(/serverUrl: null/);
    expect(session.getSnapshot().status).toBe("ready");
  }, 60_000);

  it("needs the local-first client", async () => {
    await expect(
      createJazzClient({ serverUrl: null, store: memoryStore(), driver: { type: "memory" } }),
    ).rejects.toThrow(/needs the local-first client/);
  });

  it("refuses a signed-out start, which nothing could ever sign in", async () => {
    await expect(openDeviceOnly(memoryStore(), { initial: "signed-out" })).rejects.toThrow(
      /no backend to sign in through/,
    );
  });

  it("refuses an appId: the app id is the backend's", async () => {
    await expect(
      createJazzClient({ appId: "my-app", serverUrl: null, store: memoryStore() } as never),
    ).rejects.toThrow(/takes no appId/);
    await expect(
      createJazzClient({ appId: "my-app", serverUrl: "/jazz", store: memoryStore() } as never),
    ).rejects.toThrow(/remote\(\{ appId \}\)/);
  });
});
