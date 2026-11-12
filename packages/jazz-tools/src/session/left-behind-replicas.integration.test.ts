import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { acceptDeviceAccounts, createJazz, memory } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { openBrowserSessionCore as createJazzClient } from "./create-jazz-session.js";

// `clearOnSignOut` on a shared browser: a user signed out where the page
// didn't see it (on the server, or another user signing in through the app)
// before a reload must not leave their local replica behind.

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

async function setup(options: { guests?: boolean } = {}) {
  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate: (request: Request) => {
      const userId = request.headers.get("x-test-user");
      if (userId) return { userId };
      return options.guests ? acceptDeviceAccounts(request) : null;
    },
  }).ready();
  cleanups.push(() => jazz.close());
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  const sockets = new Set<Socket>();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise<void>((resolve) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => resolve());
      }),
  );
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  // A browser page: the app's cookie names `jar.user`.
  vi.stubGlobal("document", {});
  const storage = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => void storage.set(key, value),
    removeItem: (key: string) => void storage.delete(key),
  });
  const jar = { user: "alice" as string | null };
  const realFetch = globalThis.fetch;
  const RealWebSocket = globalThis.WebSocket;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (jar.user && new URL(request.url).origin === origin)
      request.headers.set("x-test-user", jar.user);
    return realFetch(request);
  }) as typeof fetch;
  globalThis.WebSocket = class extends RealWebSocket {
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      const headers =
        jar.user && new URL(url).host === new URL(origin).host ? { "x-test-user": jar.user } : {};
      super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });

  // The replica a local-first client would keep in this browser's storage:
  // these clients run in memory, and record which user's storage was wiped.
  const wiped: string[] = [];
  // Holds a wipe's delete until released (a slow storage teardown).
  const hold = {
    wipe: undefined as Promise<void> | undefined,
    // Holds a wipe's open until released (storage another tab still closes).
    open: undefined as Promise<void> | undefined,
  };
  const store = memoryStore();
  const open = () =>
    createJazzClient({
      serverUrl: `${origin}/jazz`,
      store,
      clearOnSignOut: true,
      client: async () => ({
        createClient: async (config) => {
          // Only a wipe opens a user the cookie doesn't name.
          if (jar.user !== config.account.identity.subject) await hold.open;
          const db = await createLocalFirstDb({ ...config, driver: { type: "memory" } });
          db.deleteClientStorage = async () => {
            await hold.wipe;
            wiped.push(config.account.identity.subject);
          };
          return db;
        },
      }),
    });
  return { jar, open, wiped, hold, storage };
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

async function signInLocalFirst(open: () => ReturnType<typeof createJazzClient>) {
  const page = await open();
  await page.loginWithBackend();
  await expect.poll(() => isThinDb(page.getSnapshot().client!.db), { timeout: 15_000 }).toBe(false);
  return page;
}

it("wipes the previous user's replica on the next load once another user signed in", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  // Bob signs in through the app; the page reloads before Jazz noticed.
  jar.user = "bob";
  await first.close();
  expect(wiped).toEqual([]);

  const second = await open();
  cleanups.push(() => second.close());
  await expect.poll(() => wiped, { timeout: 15_000 }).toEqual(["alice"]);
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("bob");
  await second.close();
}, 60_000);

it("wipes it on the next load after a sign-out on the server", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  const second = await open();
  cleanups.push(() => second.close());
  await expect.poll(() => wiped, { timeout: 15_000 }).toEqual(["alice"]);
  expect(second.getSnapshot().status).toBe("signed-out");
  await second.close();
}, 60_000);

it("keeps the replica of the user who is still signed in, and wipes once, in the page", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  await first.close();

  const second = await open();
  cleanups.push(() => second.close());
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("alice");
  expect(wiped).toEqual([]);
  // Signing out in the page wipes it there; the next load has nothing left.
  await second.logout();
  expect(wiped).toEqual(["alice"]);
  jar.user = null;
  await second.close();
  const third = await open();
  cleanups.push(() => third.close());
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect(wiped).toEqual(["alice"]);
  await third.close();
}, 60_000);

it("keeps the replica on one signed-out answer the next contradicts (a failed refresh)", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  await first.close();
  // One answer says no one (the app's session refresh failed once); the next names alice again.
  jar.user = null;
  const second = await open();
  cleanups.push(() => second.close());
  jar.user = "alice";
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  expect(wiped).toEqual([]);
  await second.close();
}, 60_000);

it("lets a user who signs back in during the wipe open a fresh replica, still recorded", async () => {
  const { jar, open, wiped, hold, storage } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  let finish!: () => void;
  hold.wipe = new Promise<void>((resolve) => (finish = resolve));
  const second = await open();
  cleanups.push(() => second.close());
  // The wipe has started (after its confirming answer); alice signs in meanwhile.
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  jar.user = "alice";
  const signingIn = second.loginWithBackend();
  await new Promise((resolve) => setTimeout(resolve, 500));
  expect(wiped).toEqual([]);
  finish();
  await signingIn;
  expect(wiped).toEqual(["alice"]);
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("alice");
  // Her new replica is recorded, for a later missed sign-out.
  const recorded = [...storage].find(([key]) => key.startsWith("jazz-clear-on-sign-out-v1:"));
  expect(recorded?.[1]).toContain('"subject":"alice"');
  await second.close();
}, 60_000);

it("lets go of a wipe whose open hangs, so the user can sign back in, and keeps it for a later load", async () => {
  const { jar, open, wiped, hold, storage } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  // The wipe's open never finishes (another tab is still closing the storage).
  let opened!: () => void;
  hold.open = new Promise<void>((resolve) => (opened = resolve));
  const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
  cleanups.push(async () => warned.mockRestore());
  const second = await open();
  cleanups.push(() => second.close());
  await expect
    .poll(() => warned.mock.calls.some(([message]) => /later load/.test(String(message))), {
      timeout: 40_000,
      interval: 500,
    })
    .toBe(true);
  // Given up on: alice signs back in without waiting on it.
  jar.user = "alice";
  await second.loginWithBackend();
  expect(second.getSnapshot().client?.session?.identity.subject).toBe("alice");
  expect(wiped).toEqual([]);
  const recorded = [...storage].find(([key]) => key.startsWith("jazz-clear-on-sign-out-v1:"));
  expect(recorded?.[1]).toContain('"subject":"alice"');
  // Its open finishing late wipes nothing: it was given up on.
  opened();
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect(wiped).toEqual([]);
}, 90_000);

it("holds the user's sign-in until a delete that began finishes, however late", async () => {
  const { jar, open, wiped, hold, storage } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  // The delete starts, then outlasts the wipe's time limit.
  let finish!: () => void;
  hold.wipe = new Promise<void>((resolve) => (finish = resolve));
  const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
  cleanups.push(async () => warned.mockRestore());
  const second = await open();
  cleanups.push(() => second.close());
  await expect
    .poll(() => warned.mock.calls.some(([message]) => /later load/.test(String(message))), {
      timeout: 40_000,
      interval: 500,
    })
    .toBe(true);
  jar.user = "alice";
  let signedIn = false;
  const signingIn = second.loginWithBackend().then(() => (signedIn = true));
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect(signedIn).toBe(false);
  finish();
  await signingIn;
  expect(wiped).toEqual(["alice"]);
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("alice");
  const recorded = [...storage].find(([key]) => key.startsWith("jazz-clear-on-sign-out-v1:"));
  expect(recorded?.[1]).toContain('"subject":"alice"');
}, 90_000);

it("wipes nothing once the session is closed", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  // Closed while it confirms the signed-out answer.
  const second = await open();
  await second.close();
  await new Promise((resolve) => setTimeout(resolve, 4_000));
  expect(wiped).toEqual([]);
}, 60_000);

it("doesn't record a guest's replica", async () => {
  const { jar, open, storage } = await setup({ guests: true });
  jar.user = null;
  const page = await open();
  cleanups.push(() => page.close());
  await page.createLocalFirst();
  await expect.poll(() => isThinDb(page.getSnapshot().client!.db), { timeout: 15_000 }).toBe(false);
  expect([...storage.keys()].filter((key) => key.startsWith("jazz-clear-on-sign-out-v1:"))).toEqual(
    [],
  );
  await page.close();
}, 60_000);

it("keeps the entry while another tab still holds the replica a sign-out here wiped", async () => {
  // Web Locks as a browser has them: both pages share them, and the storage.
  const held = new Map<string, number>();
  vi.stubGlobal("navigator", {
    locks: {
      async request(
        name: string,
        options: { mode: "shared" | "exclusive"; ifAvailable?: boolean },
        callback: (lock: unknown) => Promise<unknown>,
      ) {
        if (options.mode === "exclusive") {
          if (held.get(name)) return callback(null);
          held.set(name, -1);
        } else held.set(name, (held.get(name) ?? 0) + 1);
        try {
          return await callback({ name });
        } finally {
          if (options.mode === "exclusive") held.delete(name);
          else held.set(name, held.get(name)! - 1);
        }
      },
    },
  });
  const { open, storage } = await setup();
  const recorded = () =>
    [...storage].find(([key]) => key.startsWith("jazz-clear-on-sign-out-v1:"))?.[1] ?? "";
  const here = await signInLocalFirst(open);
  cleanups.push(() => here.close());
  const there = await signInLocalFirst(open);
  cleanups.push(() => there.close());

  await here.logout();
  await new Promise((resolve) => setTimeout(resolve, 500));
  expect(there.getSnapshot().status).toBe("ready");
  expect(recorded()).toContain('"subject":"alice"');
  // Once the other tab signs out too, nothing holds it: the entry goes.
  await there.logout();
  await expect.poll(recorded, { timeout: 5_000 }).toBe("");
}, 60_000);
