import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { acceptDeviceAccounts, createJazz, memory } from "../backend/index.js";
import type { AccountStore } from "../accounts/persistence.js";
import { DeviceAccountNeedsStoreError } from "../accounts/process-store.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
// The public entry the docs import from.
import { createJazzClient } from "../client/index.js";

// A Node process (a CLI, a backend job) with no browser storage and no
// `store` of its own: the documented path of "In Node, Deno, Bun or a CLI"
// (docs/content/docs/setup/auth/backend.mdx).

const app = s.defineApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user });
  policy.todos.allowInsert.where({ owner: session.user });
});

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.unstubAllGlobals();
});

function authenticate(request: Request) {
  const match = /^Bearer token-(\w+)$/.exec(request.headers.get("authorization") ?? "");
  return match
    ? { userId: match[1]!, expiresAt: Math.floor(Date.now() / 1000) + 3600 }
    : acceptDeviceAccounts(request);
}

async function serve() {
  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate,
  }).ready();
  cleanups.push(() => jazz.close());
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return { jazz, serverUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}/jazz` };
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

describe("createJazzClient on Node without a store", () => {
  it("runs the documented bearer snippet as the user the backend names", async () => {
    const { jazz, serverUrl } = await serve();
    const loadSavedToken = () => "token-alice";

    // The snippet from the docs, as written there.
    const session = await createJazzClient({
      serverUrl,
      bearer: () => loadSavedToken(), // your app's session token, or null when signed out
    });
    cleanups.push(() => session.close());

    const snapshot = session.getSnapshot();
    expect(snapshot.status).toBe("ready");
    expect(snapshot.user?.identity).toEqual({ issuer: "jazz:backend", subject: "alice" });
    const owner = snapshot.user!.accountId;
    const row = await session.db.insert(app.todos, { title: "from the CLI", owner }).wait({
      tier: "global",
    });
    const [written] = await jazz.db.all(app.todos.where({ id: row.id }).select("$createdBy"));
    expect(written?.$createdBy).toMatchObject({ account: owner });

    // Asking the backend again keeps the same user.
    await session.refresh();
    expect(session.getSnapshot().status).toBe("ready");
  });

  it("opens signed out while the bearer names no one", async () => {
    const { serverUrl } = await serve();
    const session = await createJazzClient({ serverUrl, bearer: () => null });
    cleanups.push(() => session.close());
    expect(session.getSnapshot().status).toBe("signed-out");
  });

  it("ignores a localStorage without Web Locks, as on Deno", async () => {
    // Deno has a global localStorage but no navigator.locks: the browser
    // store would refuse, so it keeps the selection in memory like Node.
    vi.stubGlobal("navigator", {});
    const items = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => items.get(key) ?? null,
      setItem: (key: string, value: string) => void items.set(key, value),
      removeItem: (key: string) => void items.delete(key),
      key: (index: number) => [...items.keys()][index] ?? null,
      get length() {
        return items.size;
      },
    });
    const { serverUrl } = await serve();
    const session = await createJazzClient({ serverUrl, bearer: () => "token-bob" });
    cleanups.push(() => session.close());
    expect(session.getSnapshot().user?.identity.subject).toBe("bob");
    expect([...items.keys()].filter((key) => key.startsWith("jazz-account-selection"))).toEqual([]);
  });

  it('refuses initial: "device", naming the store option', async () => {
    const { serverUrl } = await serve();
    const opened = createJazzClient({ serverUrl, bearer: () => null, initial: "device" });
    await expect(opened).rejects.toBeInstanceOf(DeviceAccountNeedsStoreError);
    await expect(opened).rejects.toThrow(
      /^initial: "device" needs a place to keep the device account's key.*pass createJazzClient a `store`/,
    );
  });

  it("refuses createDeviceAccount() and restoreDeviceAccount(), and stays as it was", async () => {
    const { serverUrl } = await serve();
    const session = await createJazzClient({ serverUrl, bearer: () => "token-alice" });
    cleanups.push(() => session.close());
    await expect(session.createDeviceAccount()).rejects.toThrow(
      /^createDeviceAccount\(\) needs a place to keep the device account's key.*`store`/,
    );
    await expect(session.restoreDeviceAccount("not-even-parsed")).rejects.toThrow(
      /^restoreDeviceAccount\(key\) needs .*`store`/,
    );
    await expect(session.reopenDeviceAccount()).rejects.toThrow(
      /^reopenDeviceAccount\(\) needs .*`store`/,
    );
    expect(session.getSnapshot().status).toBe("ready");
    expect(session.getSnapshot().user?.identity.subject).toBe("alice");
  });

  it("refuses a device-only client (serverUrl: null), naming the store option", async () => {
    await expect(
      createJazzClient({
        serverUrl: null,
        driver: { type: "memory" },
        client: async () => ({ createClient: createLocalFirstDb }),
      }),
    ).rejects.toThrow(/^serverUrl: null \(a device-only client\) needs .*`store`/);
  });

  it("keeps a device account in a store the app passes", async () => {
    const { serverUrl } = await serve();
    const store = memoryStore();
    const session = await createJazzClient({
      serverUrl,
      store,
      bearer: () => null,
      initial: "device",
    });
    cleanups.push(() => session.close());
    expect(session.getSnapshot().user?.kind).toBe("device");
    expect(JSON.parse((await store.read())!).roots).toHaveLength(1);
  });
});
